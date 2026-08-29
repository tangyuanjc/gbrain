import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { mkdirSync, rmSync, existsSync, readFileSync, writeFileSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';
import { acquireLock, releaseLock, type LockHandle } from '../src/core/pglite-lock';

const TEST_DIR = join(tmpdir(), 'gbrain-lock-test-' + process.pid);

describe('pglite-lock', () => {
  beforeEach(() => {
    // Clean up test directory
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
    mkdirSync(TEST_DIR, { recursive: true });
  });

  afterEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
  });

  test('acquires and releases lock', async () => {
    const lock = await acquireLock(TEST_DIR);
    expect(lock.acquired).toBe(true);
    expect(existsSync(join(TEST_DIR, '.gbrain-lock'))).toBe(true);

    await releaseLock(lock);
    expect(existsSync(join(TEST_DIR, '.gbrain-lock'))).toBe(false);
  });

  test('creates missing data directory before acquiring lock', async () => {
    const missingDataDir = join(TEST_DIR, 'missing-data-dir');

    const lock = await acquireLock(missingDataDir);
    expect(lock.acquired).toBe(true);
    expect(existsSync(missingDataDir)).toBe(true);
    expect(existsSync(join(missingDataDir, '.gbrain-lock'))).toBe(true);

    await releaseLock(lock);
    expect(existsSync(join(missingDataDir, '.gbrain-lock'))).toBe(false);
  });

  test('prevents concurrent lock acquisition', async () => {
    const lock1 = await acquireLock(TEST_DIR, { timeoutMs: 2000 });
    expect(lock1.acquired).toBe(true);

    // Second lock attempt should timeout
    await expect(acquireLock(TEST_DIR, { timeoutMs: 1000 })).rejects.toThrow(/Timed out/);

    await releaseLock(lock1);
  });

  test('detects and cleans stale lock from dead process', async () => {
    // Simulate a stale lock from a dead process
    const lockDir = join(TEST_DIR, '.gbrain-lock');
    mkdirSync(lockDir);
    writeFileSync(join(lockDir, 'lock'), JSON.stringify({
      pid: 999999999, // Non-existent PID
      acquired_at: Date.now(),
      command: 'test',
    }));

    // Should clean up the stale lock and acquire
    const lock = await acquireLock(TEST_DIR);
    expect(lock.acquired).toBe(true);

    await releaseLock(lock);
  });

  test('skips lock for in-memory (undefined dataDir)', async () => {
    const lock = await acquireLock(undefined);
    expect(lock.acquired).toBe(true);
    expect(lock.lockDir).toBe('');

    // Release should be a no-op
    await releaseLock(lock);
  });

  test('lock file contains PID and command', async () => {
    const lock = await acquireLock(TEST_DIR);
    const lockData = JSON.parse(readFileSync(join(TEST_DIR, '.gbrain-lock', 'lock'), 'utf-8'));

    expect(lockData.pid).toBe(process.pid);
    expect(lockData.acquired_at).toBeDefined();
    expect(lockData.command).toBeDefined();

    await releaseLock(lock);
  });

  test('releases lock on disconnect even if DB close fails', async () => {
    const lock = await acquireLock(TEST_DIR);
    expect(lock.acquired).toBe(true);

    // Simulate DB already closed
    await releaseLock(lock);
    expect(existsSync(join(TEST_DIR, '.gbrain-lock'))).toBe(false);

    // Second acquisition should work
    const lock2 = await acquireLock(TEST_DIR);
    expect(lock2.acquired).toBe(true);
    await releaseLock(lock2);
  });
});

describe('pglite-lock heartbeat + ownership', () => {
  beforeEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
    mkdirSync(TEST_DIR, { recursive: true });
  });
  afterEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
  });

  function writeHolder(fields: { pid: number; acquiredAgoMs: number; refreshedAgoMs: number }) {
    const lockDir = join(TEST_DIR, '.gbrain-lock');
    mkdirSync(lockDir, { recursive: true });
    const now = Date.now();
    writeFileSync(join(lockDir, 'lock'), JSON.stringify({
      pid: fields.pid,
      acquired_at: now - fields.acquiredAgoMs,
      refreshed_at: now - fields.refreshedAgoMs,
      command: 'test holder',
    }));
  }

  test('[REGRESSION] a LIVE holder with a fresh heartbeat is NOT stolen even when the lock is old', async () => {
    // The WAL-corruption bug: a >5min embed used to get its lock force-removed.
    // Now an alive holder that heartbeated recently is left alone regardless of
    // age. acquired 20min ago, but refreshed just now → must wait, not steal.
    writeHolder({ pid: process.pid, acquiredAgoMs: 20 * 60_000, refreshedAgoMs: 0 });

    await expect(acquireLock(TEST_DIR, { timeoutMs: 1200 })).rejects.toThrow(/Timed out/);
    // Holder's lock still present (was never stolen).
    expect(existsSync(join(TEST_DIR, '.gbrain-lock'))).toBe(true);
  });

  test('[REGRESSION #2348] a LIVE PID with a STALE heartbeat is NOT stolen', async () => {
    // A synchronous PGLite/WASM operation can block the JS heartbeat while the
    // holder is still using the database. PID liveness is the only safe
    // automatic reap signal; a stale heartbeat must never admit a second writer.
    writeHolder({ pid: process.pid, acquiredAgoMs: 25 * 60_000, refreshedAgoMs: 20 * 60_000 });

    await expect(acquireLock(TEST_DIR, { timeoutMs: 1200 })).rejects.toThrow(/Timed out/);
    expect(existsSync(join(TEST_DIR, '.gbrain-lock'))).toBe(true);
  });

  test('[REGRESSION] a lock directory with no ownership record fails closed', async () => {
    const lockDir = join(TEST_DIR, '.gbrain-lock');
    mkdirSync(lockDir);

    await expect(acquireLock(TEST_DIR, { timeoutMs: 1200 })).rejects.toThrow(/Timed out/);
    expect(existsSync(lockDir)).toBe(true);
  });

  test('[REGRESSION] an invalid JSON ownership record fails closed', async () => {
    const lockDir = join(TEST_DIR, '.gbrain-lock');
    mkdirSync(lockDir);
    writeFileSync(join(lockDir, 'lock'), '{not-json');

    await expect(acquireLock(TEST_DIR, { timeoutMs: 1200 })).rejects.toThrow(/Timed out/);
    expect(existsSync(lockDir)).toBe(true);
  });

  test('[REGRESSION] an invalid ownership schema fails closed', async () => {
    const lockDir = join(TEST_DIR, '.gbrain-lock');
    mkdirSync(lockDir);
    writeFileSync(join(lockDir, 'lock'), JSON.stringify({
      pid: 'not-a-positive-integer',
      acquired_at: 0,
      command: 'invalid holder',
    }));

    await expect(acquireLock(TEST_DIR, { timeoutMs: 1200 })).rejects.toThrow(/Timed out/);
    expect(existsSync(lockDir)).toBe(true);
  });

  test('[REGRESSION] EPERM from the PID probe is treated as alive', async () => {
    writeHolder({ pid: process.pid, acquiredAgoMs: 60_000, refreshedAgoMs: 60_000 });

    const originalDescriptor = Object.getOwnPropertyDescriptor(process, 'kill');
    if (!originalDescriptor) throw new Error('process.kill descriptor is unavailable');
    let acquisition: ReturnType<typeof acquireLock>;
    try {
      Object.defineProperty(process, 'kill', {
        ...originalDescriptor,
        value: () => {
          const error = new Error('operation not permitted') as NodeJS.ErrnoException;
          error.code = 'EPERM';
          throw error;
        },
      });
      acquisition = acquireLock(TEST_DIR, { timeoutMs: 1 });
    } finally {
      Object.defineProperty(process, 'kill', originalDescriptor);
    }

    await expect(acquisition!).rejects.toThrow(/Timed out/);
    expect(existsSync(join(TEST_DIR, '.gbrain-lock'))).toBe(true);
  });

  test('[REGRESSION] releaseLock does NOT remove a lock replaced by another process', async () => {
    // We acquire, then simulate replacement with a different pid + acquired_at.
    // Our stale handle must not delete the replacement owner's live lock.
    const lock: LockHandle = await acquireLock(TEST_DIR);
    expect(lock.acquired).toBe(true);
    expect(lock.ownerToken).toBeDefined();
    if (lock.heartbeat) clearInterval(lock.heartbeat); // stop our heartbeat for a deterministic test

    // Overwrite the lock file as if process B re-acquired it.
    const lockFile = join(TEST_DIR, '.gbrain-lock', 'lock');
    const bNow = Date.now() + 1;
    writeFileSync(lockFile, JSON.stringify({ pid: 999999, acquired_at: bNow, refreshed_at: bNow, command: 'process B' }));

    await releaseLock(lock); // our (stale) handle

    // B's lock survives — we did not clobber it.
    expect(existsSync(join(TEST_DIR, '.gbrain-lock'))).toBe(true);
    const after = JSON.parse(readFileSync(lockFile, 'utf-8'));
    expect(after.pid).toBe(999999);

    // Cleanup for afterEach.
    rmSync(join(TEST_DIR, '.gbrain-lock'), { recursive: true, force: true });
  });

  test('[REGRESSION] releaseLock preserves the lock when ownership cannot be read', async () => {
    const lock: LockHandle = await acquireLock(TEST_DIR);
    expect(lock.ownerToken).toBeDefined();
    if (lock.heartbeat) clearInterval(lock.heartbeat);

    const lockDir = join(TEST_DIR, '.gbrain-lock');
    writeFileSync(join(lockDir, 'lock'), '{not-json');

    await releaseLock(lock);

    expect(existsSync(lockDir)).toBe(true);
  });

  test('acquire starts a heartbeat and seeds refreshed_at; release clears it', async () => {
    const lock: LockHandle = await acquireLock(TEST_DIR);
    expect(lock.acquired).toBe(true);
    expect(lock.heartbeat).toBeDefined();
    const data = JSON.parse(readFileSync(join(TEST_DIR, '.gbrain-lock', 'lock'), 'utf-8'));
    expect(data.refreshed_at).toBeDefined();
    expect(typeof data.refreshed_at).toBe('number');

    await releaseLock(lock);
    expect(lock.heartbeat).toBeUndefined();
    expect(existsSync(join(TEST_DIR, '.gbrain-lock'))).toBe(false);
  });
});
