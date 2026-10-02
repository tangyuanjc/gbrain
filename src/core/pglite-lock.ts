/** Kernel ownership for a persistent PGLite datastore; metadata is diagnostic.
 * Backported from garrytan/gbrain v0.59.20.0 (introduced in d13aa742).
 * Stop all older GBrain processes before migrating. Never unlink the stable
 * sibling .gbrain-owner.lock file, including during datastore maintenance.
 */
import { appendFileSync, closeSync, fsyncSync, mkdirSync, openSync, readFileSync, writeFileSync, rmSync, renameSync, realpathSync, readlinkSync } from 'node:fs';
import { basename, dirname, join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { tryAcquireNativeLock, type NativeLockHandle } from './persistence/native-lock.ts';

const HEARTBEAT_INTERVAL_MS = 30_000;
const LOCK_FILE = 'lock';
// A dropped engine reference must never let GC release a still-open datastore.
const retainedOwners = new Set<LockHandle>();

export class PgliteBusyError extends Error {
  readonly code = 'pglite_busy';
  readonly retryable = true;
  constructor(message: string) {
    super(message); this.name = 'PgliteBusyError';
  }
}
export interface LockHandle {
  /** Legacy metadata directory. It is never the ownership authority. */
  lockDir: string;
  acquired: boolean;
  heartbeat?: ReturnType<typeof setInterval>;
  lockPath?: string;
  ownerToken?: string;
  /** A dead legacy holder was encountered during protocol migration. */
  reaped?: boolean;
  nativeLock?: NativeLockHandle;
  /** Canonical datastore path protected by nativeLock. */
  dataDir?: string;
}

export type LockCloseStatus = 'closed' | 'close_failed' | 'open_failed' | 'not_opened' | 'unknown';

// A local append-only incident trail. Logging must never change lock semantics.
// An unmatched acquisition is evidence of missing orderly release, not by itself
// proof of SIGKILL (disk-full/log-write errors and host failure are also possible).
export function appendLockEvent(lock: LockHandle, event: 'acquired' | 'released' | 'close_failed', closeStatus: LockCloseStatus = 'unknown', releaseStartedAt?: string): void {
  if (!lock.lockDir) return;
  try {
    appendFileSync(join(dirname(lock.lockDir), '.gbrain-lock-events.jsonl'), JSON.stringify({
      at: new Date().toISOString(), event, pid: process.pid,
      command: process.argv.slice(1).join(' '), owner_token: lock.ownerToken,
      data_dir: dirname(lock.lockDir), close_status: closeStatus,
      release_started_at: releaseStartedAt,
      close_ok: closeStatus === 'closed' ? true : closeStatus === 'close_failed' ? false : null,
    }) + '\n', { mode: 0o600 });
  } catch {
    console.warn('[gbrain] could not append PGLite lock audit event');
  }
}

interface LockMetadata {
  pid?: number;
  acquired_at?: number;
  refreshed_at?: number;
  command?: string;
  argv?: string[];
  owner_token?: string;
  protocol?: string;
  pid_ns?: string | null;
  boot_id?: string | null;
}
const protocol = 'kernel-v1';
function tokenOf(metadata: LockMetadata): string {
  return metadata.owner_token ?? `${metadata.pid}:${metadata.acquired_at}`;
}
function readMetadata(lockDir: string): LockMetadata | null {
  try {
    const raw: unknown = JSON.parse(readFileSync(join(lockDir, LOCK_FILE), 'utf8'));
    if (!raw || typeof raw !== 'object') return null;
    const metadata = raw as LockMetadata;
    if (!Number.isSafeInteger(metadata.pid) || metadata.pid! <= 0
      || !Number.isSafeInteger(metadata.acquired_at) || metadata.acquired_at! <= 0) return null;
    return metadata;
  } catch { return null; }
}
function readPidNs(): string | null {
  try { return readlinkSync('/proc/self/ns/pid'); } catch { return null; }
}
function readBootId(): string | null {
  try { return readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim() || null; } catch { return null; }
}

/** Resolve existing ancestors without creating the datastore during inspection. */
function canonicalPath(path: string): string {
  const absolute = resolve(path);
  try { return realpathSync(absolute); } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    const parent = dirname(absolute);
    if (parent === absolute) throw error;
    return join(canonicalPath(parent), basename(absolute));
  }
}
/** Stable sibling survives datastore replacement. Never unlink this file. */
export function getPgliteKernelLockPath(dataDir: string | undefined): string | undefined {
  return dataDir ? `${canonicalPath(dataDir)}.gbrain-owner.lock` : undefined;
}
function getLockDir(dataDir: string | undefined): string {
  return dataDir ? join(dataDir, '.gbrain-lock') : '';
}

/** PID is used for diagnostics and legacy migration, never kernel takeover. */
export function isProcessAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return true;
  try { process.kill(pid, 0); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code !== 'ESRCH'; }
}

function writeMetadata(path: string, metadata: LockMetadata): void {
  const temporary = `${path}.tmp-${metadata.owner_token}`;
  try {
    writeFileSync(temporary, JSON.stringify(metadata), { mode: 0o600 });
    renameSync(temporary, path);
  } finally { rmSync(temporary, { force: true }); }
}
function syncMarkerDirectory(path: string): void {
  // POSIX requires directory fsync to persist rename. Node cannot open a
  // directory for fsync on Windows; file fsync + atomic rename still apply.
  if (process.platform !== 'win32') {
    const parent = openSync(dirname(path), 'r');
    try { fsyncSync(parent); } finally { closeSync(parent); }
  }
}
/** Publish once, before writing diagnostic ownership. Never truncate the marker. */
function writeMigrationMarker(path: string): void {
  const temporary = `${path}.tmp-${randomUUID()}`;
  try {
    const fd = openSync(temporary, 'wx', 0o600);
    try {
      writeFileSync(fd, JSON.stringify({ protocol }));
      fsyncSync(fd);
    } finally { closeSync(fd); }
    renameSync(temporary, path);
    syncMarkerDirectory(path);
  } finally { rmSync(temporary, { force: true }); }
}
function syncMigrationMarker(path: string): void {
  // A predecessor may have exited after rename but before directory fsync.
  // Also make markers from the older truncate/write implementation durable.
  // Sync without rewriting: inode, content and mtime remain unchanged.
  const fd = openSync(path, process.platform === 'win32' ? 'r+' : 'r');
  try { fsyncSync(fd); } finally { closeSync(fd); }
  syncMarkerDirectory(path);
}
function startHeartbeat(path: string, ownerToken: string): ReturnType<typeof setInterval> {
  const timer = setInterval(() => {
    try {
      const metadata = JSON.parse(readFileSync(path, 'utf8')) as LockMetadata;
      if (tokenOf(metadata) !== ownerToken) { clearInterval(timer); return; }
      metadata.refreshed_at = Date.now();
      writeMetadata(path, metadata);
    } catch { /* Metadata failure cannot change kernel ownership. */ }
  }, HEARTBEAT_INTERVAL_MS);
  timer.unref?.();
  return timer;
}
function busy(lockDir: string): PgliteBusyError {
  return new PgliteBusyError(`GBrain: Timed out waiting for PGLite lock at ${lockDir}. Retry after the holder finishes. Stop all older GBrain processes before upgrading this datastore's lock protocol; unreadable legacy ownership is never stolen. Never remove a live holder's lock. This lock is separate from \`gbrain sync --break-lock\`.`);
}

/**
 * First kernel acquisition also claims the legacy mkdir lock. A live or
 * unreadable legacy holder blocks migration. After migration, pid/mtime/
 * metadata corruption cannot authorize or prevent native ownership.
 * All older GBrain processes must be stopped before the protocol upgrade.
 */
export async function acquireLock(dataDir: string | undefined, opts: { timeoutMs?: number; signal?: AbortSignal } = {}): Promise<LockHandle> {
  if (!dataDir) return { lockDir: '', acquired: true };
  const timeoutMs = opts.timeoutMs ?? 30_000;
  if (!Number.isFinite(timeoutMs) || timeoutMs < 0 || timeoutMs > 2 ** 31 - 1) throw new RangeError('Invalid PGLite lock timeout');
  const canonical = canonicalPath(dataDir);
  const kernelPath = getPgliteKernelLockPath(canonical)!;
  const markerPath = `${canonical}.gbrain-owner.json`;
  const lockDir = getLockDir(canonical);
  const deadline = performance.now() + timeoutMs;
  for (;;) {
    opts.signal?.throwIfAborted();
    const nativeLock = await tryAcquireNativeLock(kernelPath);
    if (nativeLock) {
      let accepted = false;
      try {
        let migrated = false;
        try { migrated = JSON.parse(readFileSync(markerPath, 'utf8')).protocol === protocol; } catch { /* first upgrade */ }
        mkdirSync(canonical, { recursive: true });
        let reaped = false;
        try { mkdirSync(lockDir); }
        catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
          const metadata = readMetadata(lockDir);
          if (!migrated || (metadata && metadata.protocol !== protocol)) {
            // Legacy death requires matching namespace evidence on Linux;
            // ESRCH from another container's PID namespace proves nothing.
            const comparable = process.platform !== 'linux' || (metadata?.pid_ns === readPidNs()
              && metadata?.boot_id === readBootId() && metadata?.pid_ns != null && metadata?.boot_id != null);
            if (!metadata || !comparable || isProcessAlive(metadata.pid!)) throw busy(lockDir);
            reaped = true;
          }
        }
        opts.signal?.throwIfAborted();
        if (!migrated) writeMigrationMarker(markerPath);
        else syncMigrationMarker(markerPath);
        const now = Date.now(), ownerToken = randomUUID(), lockPath = join(lockDir, LOCK_FILE);
        writeMetadata(lockPath, { pid: process.pid, acquired_at: now, refreshed_at: now,
          command: process.argv.slice(1).join(' '), argv: process.argv.slice(1),
          owner_token: ownerToken, protocol, pid_ns: readPidNs(), boot_id: readBootId() });
        const result = { lockDir, acquired: true, lockPath, ownerToken, reaped, nativeLock, dataDir: canonical,
          heartbeat: startHeartbeat(lockPath, ownerToken) };
        retainedOwners.add(result);
        accepted = true;
        appendLockEvent(result, 'acquired');
        return result;
      } catch (error) {
        if (!(error instanceof PgliteBusyError) || performance.now() >= deadline) throw error;
      } finally { if (!accepted) await nativeLock.release(); }
    }
    if (performance.now() >= deadline) throw busy(lockDir);
    await delay(Math.min(25, deadline - performance.now()), undefined, { signal: opts.signal });
  }
}

/** Metadata removal is optional; kernel release is mandatory and never unlinks. */
export async function releaseLock(lock: LockHandle, closeStatus: LockCloseStatus = 'unknown'): Promise<void> {
  if (!lock.acquired) return;
  if (lock.heartbeat) { clearInterval(lock.heartbeat); lock.heartbeat = undefined; }
  if (lock.lockDir && lock.ownerToken) {
    const metadata = readMetadata(lock.lockDir);
    if (metadata && tokenOf(metadata) === lock.ownerToken) {
      try { rmSync(lock.lockDir, { recursive: true, force: true }); } catch { /* diagnostic only */ }
    }
  }
  // Success is logged only after the OS handle closes. A successor can log
  // first, so audit consumers use this handoff start only for confirmed releases.
  const releaseStartedAt = new Date().toISOString();
  await lock.nativeLock?.release();
  appendLockEvent(lock, 'released', closeStatus, releaseStartedAt);
  lock.acquired = false;
  retainedOwners.delete(lock);
}
