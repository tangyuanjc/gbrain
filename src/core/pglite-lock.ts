/**
 * PGLite File Lock — prevents concurrent process access to the same data directory.
 *
 * PGLite uses embedded Postgres (WASM) which only supports one connection at a time.
 * When `gbrain embed` (which can take minutes) is running and another process tries
 * to connect, PGLite throws `Aborted()` because it can't handle concurrent access.
 *
 * This module implements a simple advisory lock using a lock file next to the data
 * directory. It uses atomic `mkdir` (which is POSIX-atomic) combined with PID tracking
 * for stale lock detection.
 *
 * Usage:
 *   const lock = await acquireLock(dataDir);
 *   try { ... } finally { await releaseLock(lock); }
 */

import { mkdirSync, existsSync, readFileSync, writeFileSync, rmSync, statSync } from 'fs';
import { join } from 'path';

const LOCK_DIR_NAME = '.gbrain-lock';
const LOCK_FILE = 'lock';

// Refresh the lock's `refreshed_at` while held for operator observability.
// Acquisition never uses heartbeat age to decide whether a holder is dead.
const HEARTBEAT_INTERVAL_MS = 30_000;

export interface LockHandle {
  lockDir: string;
  acquired: boolean;
  /**
   * #2058: heartbeat timer + lock-file path, set when a real (on-disk) lock is
   * held so `releaseLock` can stop refreshing. Absent for the in-memory engine
   * (no lock file, no concurrent access possible).
   */
  heartbeat?: ReturnType<typeof setInterval>;
  lockPath?: string;
  /**
   * Our ownership token (`<pid>:<acquired_at>`). Heartbeat and release verify
   * the on-disk lock is still ours before touching it so a stale handle cannot
   * refresh or delete a replacement owner's lock.
   */
  ownerToken?: string;
}

interface LockRecord {
  pid: number;
  acquired_at: number;
  refreshed_at?: number;
  command?: unknown;
}

/** The on-disk lock identity, used to detect "we were reaped and replaced". */
function tokenOf(lockData: Pick<LockRecord, 'pid' | 'acquired_at'>): string {
  return `${lockData.pid}:${lockData.acquired_at}`;
}

function readLockRecord(lockPath: string): LockRecord | undefined {
  try {
    const parsed: unknown = JSON.parse(readFileSync(lockPath, 'utf-8'));
    if (typeof parsed !== 'object' || parsed === null) return undefined;
    const record = parsed as Record<string, unknown>;
    if (!Number.isSafeInteger(record.pid) || (record.pid as number) <= 0) return undefined;
    if (!Number.isSafeInteger(record.acquired_at) || (record.acquired_at as number) <= 0) return undefined;
    return record as unknown as LockRecord;
  } catch {
    return undefined;
  }
}

/**
 * Keep the held lock's `refreshed_at` current for observability. Acquisition
 * never uses heartbeat age to reap a live PID. Best-effort: if the record is
 * unreadable or no longer ours, stop instead of clobbering another owner.
 * `.unref()` ensures the timer never keeps the process alive on its own.
 */
function startHeartbeat(lockPath: string, ownerToken: string): ReturnType<typeof setInterval> {
  const timer = setInterval(() => {
    try {
      const raw = readLockRecord(lockPath);
      if (!raw || tokenOf(raw) !== ownerToken) {
        // Ownership is no longer certain — do not refresh this lock.
        clearInterval(timer);
        return;
      }
      raw.refreshed_at = Date.now();
      writeFileSync(lockPath, JSON.stringify(raw), { mode: 0o644 });
    } catch { /* best-effort — file removed or transient FS error */ }
  }, HEARTBEAT_INTERVAL_MS);
  (timer as { unref?: () => void }).unref?.();
  return timer;
}

function getLockDir(dataDir: string | undefined): string {
  // Use the parent of the data dir for the lock, or a temp location for in-memory
  if (!dataDir) {
    // In-memory PGLite — no concurrent access possible since it's process-scoped
    // Return a sentinel that we skip
    return '';
  }
  return join(dataDir, LOCK_DIR_NAME);
}

function isProcessAlive(pid: number): boolean {
  try {
    // Sending signal 0 checks existence without actually sending a signal
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // ESRCH confirms the PID is gone. EPERM and unknown probe failures do not
    // prove death, so keep the lock and fail closed.
    return (error as NodeJS.ErrnoException)?.code !== 'ESRCH';
  }
}

/**
 * Attempt to acquire an exclusive lock on the PGLite data directory.
 * Returns { acquired: true } if the lock was obtained, { acquired: false } otherwise.
 * Stale locks (from dead processes) are automatically cleaned up.
 */
export async function acquireLock(dataDir: string | undefined, opts?: { timeoutMs?: number }): Promise<LockHandle> {
  const lockDir = getLockDir(dataDir);

  // In-memory PGLite — no lock needed (process-scoped, can't be shared)
  if (!lockDir) {
    return { lockDir: '', acquired: true };
  }

  // `lockDir` being set implies `dataDir` is set (see getLockDir), but TS
  // can't derive that across helper boundaries.
  mkdirSync(dataDir as string, { recursive: true });

  const timeoutMs = opts?.timeoutMs ?? 30_000; // 30 second default timeout
  const startTime = Date.now();

  while (Date.now() - startTime < timeoutMs) {
    // Check for stale lock first
    if (existsSync(lockDir)) {
      const lockPath = join(lockDir, LOCK_FILE);
      const lockData = readLockRecord(lockPath);
      if (!lockData) {
        // Missing, partially written, or malformed ownership is not evidence
        // that no writer exists. Preserve the directory and fail closed.
        await new Promise(r => setTimeout(r, 1000));
        continue;
      }

      if (!isProcessAlive(lockData.pid)) {
        // A valid record whose holder process is gone is safe to reap.
        try { rmSync(lockDir, { recursive: true, force: true }); } catch { /* race condition, try again */ }
      } else {
        // Heartbeats run on the JS event loop and can stop during synchronous
        // PGLite/WASM work. A live PID is therefore never stolen automatically.
        await new Promise(r => setTimeout(r, 1000));
        continue;
      }
    }

    // Try to acquire lock (atomic mkdir)
    try {
      mkdirSync(lockDir, { recursive: false });
      // We got the lock — write our PID. #2058: seed `refreshed_at` and start
      // the heartbeat so this holder reads as alive-and-working to others.
      const lockPath = join(lockDir, LOCK_FILE);
      const now = Date.now();
      writeFileSync(lockPath, JSON.stringify({
        pid: process.pid,
        acquired_at: now,
        refreshed_at: now,
        command: process.argv.slice(1).join(' '),
      }), { mode: 0o644 });

      const ownerToken = tokenOf({ pid: process.pid, acquired_at: now });
      return { lockDir, acquired: true, lockPath, ownerToken, heartbeat: startHeartbeat(lockPath, ownerToken) };
    } catch (e: unknown) {
      // mkdir failed — someone else grabbed it between our check and mkdir
      // This is fine, we'll retry
      if (Date.now() - startTime >= timeoutMs) {
        // Timeout — report which process holds the lock
        const lockPath = join(lockDir, LOCK_FILE);
        try {
          const lockData = JSON.parse(readFileSync(lockPath, 'utf-8'));
          throw new Error(
            `GBrain: Timed out waiting for PGLite lock. Process ${lockData.pid} has held it since ${new Date(lockData.acquired_at).toISOString()} (command: ${lockData.command}). ` +
            `If that process is dead, remove ${lockDir} and try again.`
          );
        } catch (readErr) {
          if (readErr instanceof Error && readErr.message.startsWith('GBrain')) throw readErr;
          throw new Error(
            `GBrain: Timed out waiting for PGLite lock. Remove ${lockDir} and try again.`
          );
        }
      }
      // Brief wait before retry
      await new Promise(r => setTimeout(r, 500));
    }
  }

  // Should not reach here, but just in case
  throw new Error(`GBrain: Timed out waiting for PGLite lock.`);
}

/**
 * Release a previously acquired lock.
 */
export async function releaseLock(lock: LockHandle): Promise<void> {
  // #2058: stop the heartbeat first so it can't recreate/rewrite the lock file
  // after we remove it.
  if (lock.heartbeat) {
    clearInterval(lock.heartbeat);
    lock.heartbeat = undefined;
  }
  if (!lock.lockDir || !lock.acquired) return;

  // Only remove the lock if it is positively confirmed to still be ours.
  if (lock.ownerToken) {
    const raw = readLockRecord(join(lock.lockDir, LOCK_FILE));
    if (!raw || tokenOf(raw) !== lock.ownerToken) return;
  }

  try {
    rmSync(lock.lockDir, { recursive: true, force: true });
  } catch {
    // Lock file already removed (e.g., by stale cleanup) — that's fine
  }
}
