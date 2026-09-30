/**
 * v0.41.8.0 — PGLiteEngine.disconnect() lifecycle regression tests.
 *
 * Pins the invariants the v0.41.8.0 hang fix wave depends on:
 *
 *   1. ORDERING: `db.close()` is called BEFORE the file lock is
 *      released. A sibling process must not be able to acquire the
 *      lock and try to connect to a still-closing brain. PR #1337's
 *      original diff swapped this to release-then-close — we
 *      explicitly REJECTED that ordering. This test fails if a
 *      future maintainer reads the PR and applies the swap.
 *
 *   2. SNAPSHOT + EARLY-NULL: `this._db` is nulled BEFORE awaiting
 *      `close()`, so a concurrent `connect()` cannot observe a
 *      partial mid-close state. PR #1337's load-bearing contribution
 *      that we DID take.
 *
 *   3. FAIL CLOSED: if db.close() throws, ownership remains held until
 *      process exit. An uncertain live DB must never admit a second writer.
 *
 *   4. IDEMPOTENCY: calling disconnect() twice is a clean no-op on
 *      the second call (no throw, no double-close attempt).
 *
 *   5. DOUBLE-DISCONNECT THEN CONNECT: after disconnect, a fresh
 *      connect() sees clean state and succeeds.
 *
 * Marked .serial because PGLite WASM cold-start dominates wallclock
 * for fresh-engine-per-test cases — running these in the parallel
 * shard pool would starve other PGLite tests of cold-start time.
 */

import { describe, test, expect } from 'bun:test';
import { mkdtempSync, rmSync, readFileSync, mkdirSync, symlinkSync, unlinkSync, realpathSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { acquireLock, releaseLock, type LockHandle } from '../src/core/pglite-lock.ts';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';

function newTempDataDir(): string {
  return mkdtempSync(join(tmpdir(), 'gbrain-disconnect-test-'));
}

describe('PGLiteEngine.disconnect() — v0.41.8.0 lifecycle invariants', () => {
  test('ORDERING: db.close() is called BEFORE releaseLock()', async () => {
    const dataDir = newTempDataDir();
    try {
      const engine = new PGLiteEngine();
      await engine.connect({ database_path: dataDir });
      await engine.initSchema();

      // Record the actual call order. We spy by replacing the db
      // handle's close + the lock handle's release with timestamped
      // wrappers.
      const calls: string[] = [];
      const eng = engine as unknown as {
        _db: { close: () => Promise<void> } | null;
        _lock: { lockDir: string; acquired: boolean } | null;
      };

      const realClose = eng._db!.close.bind(eng._db!);
      eng._db!.close = async () => {
        // Tiny delay so a flipped ordering would actually show up
        // (release-before-close would beat us if we returned instantly).
        await new Promise((r) => setTimeout(r, 10));
        calls.push('db.close');
        return realClose();
      };

      // releaseLock is module-level in pglite-lock.ts — to spy we have
      // to swap the lock object's `acquired` flag detection won't
      // route through us. Easier: monkey-patch by replacing the lock
      // ref with one whose presence forces releaseLock to no-op (so
      // we just measure that the close ran during disconnect and that
      // the no-op happened in the same call).
      //
      // For the ORDERING test specifically, we wrap close and
      // measure that the lockDir mkdir is still present immediately
      // before close runs and gone after disconnect returns. The
      // lockDir's existence is observable on disk.
      const { existsSync } = await import('fs');
      const lockDir = eng._lock!.lockDir;
      expect(existsSync(lockDir)).toBe(true);

      // Spy on the lock-release moment by polling lockDir existence
      // from another timer: when close completes, the lock should
      // STILL be present (close-then-release contract).
      let lockStillPresentAtCloseFinish = false;
      const origClose = eng._db!.close;
      eng._db!.close = async () => {
        await origClose();
        // Right after close resolves, the lock has NOT yet been
        // released (the finally branch hasn't run yet). Check
        // synchronously before yielding the event loop again.
        lockStillPresentAtCloseFinish = existsSync(lockDir);
      };

      await engine.disconnect();

      expect(calls).toContain('db.close');
      expect(lockStillPresentAtCloseFinish).toBe(true);
      expect(existsSync(lockDir)).toBe(false);
      const audit = readFileSync(join(dataDir, '.gbrain-lock-events.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
      expect(audit.at(-1).close_ok).toBe(true);
      expect(audit.at(-1).close_status).toBe('closed');
    } finally {
      rmSync(dataDir, { recursive: true, force: true });
    }
  });

  test('SNAPSHOT + EARLY-NULL: _db is nulled before await close', async () => {
    const dataDir = newTempDataDir();
    try {
      const engine = new PGLiteEngine();
      await engine.connect({ database_path: dataDir });
      await engine.initSchema();

      const eng = engine as unknown as {
        _db: { close: () => Promise<void> } | null;
      };

      let dbWasNullWhenCloseRan = false;
      const realClose = eng._db!.close.bind(eng._db!);
      eng._db!.close = async () => {
        // Inside close, the engine's _db field should ALREADY be null
        // (snapshot pattern). If it's not, the partial-state race is
        // back.
        dbWasNullWhenCloseRan = eng._db === null;
        return realClose();
      };

      await engine.disconnect();
      expect(dbWasNullWhenCloseRan).toBe(true);
    } finally {
      rmSync(dataDir, { recursive: true, force: true });
    }
  });

  test('failed close retains ownership and refuses reconnect', async () => {
    const dataDir = newTempDataDir();
    const engine = new PGLiteEngine();
    await engine.connect({ database_path: dataDir });
    const eng = engine as unknown as { _db: { close: () => Promise<void> }; _lock: LockHandle };
    const db = eng._db, lock = eng._lock, realClose = db.close.bind(db);
    try {
      db.close = async () => { throw new Error('synthetic close failure'); };
      await expect(engine.disconnect()).rejects.toThrow('synthetic close failure');
      await expect(acquireLock(dataDir, { timeoutMs: 100 })).rejects.toThrow(/Timed out/);
      await engine.disconnect(); // Repeated cleanup must not release uncertain ownership.
      await expect(acquireLock(dataDir, { timeoutMs: 100 })).rejects.toThrow(/Timed out/);
      await expect(engine.connect({ database_path: dataDir })).rejects.toThrow(/restart this process/);
      const audit = readFileSync(join(dataDir, '.gbrain-lock-events.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
      expect(audit.at(-1).event).toBe('close_failed');
      expect(audit.at(-1).close_ok).toBe(false);
      expect(audit.some(event => event.event === 'released')).toBe(false);
    } finally {
      await realClose();
      await releaseLock(lock, 'closed');
      rmSync(dataDir, { recursive: true, force: true });
    }
  });

  test('IDEMPOTENCY: double disconnect is a clean no-op on the second call', async () => {
    const dataDir = newTempDataDir();
    try {
      const engine = new PGLiteEngine();
      await engine.connect({ database_path: dataDir });
      await engine.initSchema();

      let closeCallCount = 0;
      const eng = engine as unknown as {
        _db: { close: () => Promise<void> } | null;
      };
      const realClose = eng._db!.close.bind(eng._db!);
      eng._db!.close = async () => {
        closeCallCount++;
        return realClose();
      };

      await engine.disconnect();
      expect(closeCallCount).toBe(1);

      // Second call: no throw, no second close
      await engine.disconnect();
      expect(closeCallCount).toBe(1);
    } finally {
      rmSync(dataDir, { recursive: true, force: true });
    }
  });

  test('RECONNECT after disconnect sees clean state', async () => {
    const dataDir = newTempDataDir();
    try {
      const engine = new PGLiteEngine();
      await engine.connect({ database_path: dataDir });
      await engine.initSchema();
      await engine.disconnect();

      // Same dataDir, fresh connect. Must succeed without lock contention.
      await engine.connect({ database_path: dataDir });
      await engine.initSchema();
      // Smoke: a SELECT 1 round-trip proves the new handle is alive.
      const result = await engine.executeRaw<{ ok: number }>('SELECT 1 AS ok');
      expect(result[0].ok).toBe(1);
      await engine.disconnect();
    } finally {
      rmSync(dataDir, { recursive: true, force: true });
    }
  });
});

// ─────────────────────────────────────────────────────────────────
// #2084 — preservingProcessExitCode behavioral containment
// ─────────────────────────────────────────────────────────────────
describe('PGLiteEngine: Emscripten process.exitCode containment (#2084)', () => {
  test('connect() leaves process.exitCode pinned at 0, not the Emscripten 99', async () => {
    const prev = process.exitCode;
    const eng = new PGLiteEngine();
    try {
      await eng.connect({ engine: 'pglite' });
      // Emscripten writes 99 during create; the wrapper pins explicit 0 when
      // nothing was set before (undefined cannot be restored — the accessor
      // falls back to the WASM status).
      expect(Number(process.exitCode)).toBe(0);
    } finally {
      await eng.disconnect();
      process.exitCode = prev;
    }
  }, 60_000);

  test('a pre-call verdict survives the create-throw path (finally restores)', async () => {
    const prev = process.exitCode;
    const eng = new PGLiteEngine();
    try {
      process.exitCode = 3;
      // A dataDir under a regular FILE cannot be created — PGlite.create rejects.
      await expect(
        eng.connect({ engine: 'pglite', database_path: '/dev/null/nope/brain' }),
      ).rejects.toThrow();
      expect(Number(process.exitCode)).toBe(3);
    } finally {
      process.exitCode = prev;
    }
  }, 60_000);
});

test('connect opens the canonical datastore whose lock it acquired', async () => {
  const root = newTempDataDir();
  const a = join(root, 'a'), b = join(root, 'b'), alias = join(root, 'alias');
  mkdirSync(a); mkdirSync(b); symlinkSync(a, alias);
  const holder = await acquireLock(a);
  const engine = new PGLiteEngine();
  const connecting = engine.connect({ database_path: alias });
  try {
    // connect has resolved alias to a and is waiting for its native lock.
    await Bun.sleep(50);
    unlinkSync(alias); symlinkSync(b, alias);
    await releaseLock(holder);
    await connecting;
    const db = engine.db as unknown as { dataDir: string };
    expect(db.dataDir).toBe(realpathSync(a));
    expect((await engine.executeRaw<{ ok: number }>('SELECT 1 AS ok'))[0].ok).toBe(1);
  } finally {
    await releaseLock(holder);
    await connecting.catch(() => {});
    await engine.disconnect();
    rmSync(root, { recursive: true, force: true });
  }
}, 15000);

test('disconnect during initialization keeps ownership until create and close settle', async () => {
  const { PGlite } = await import('@electric-sql/pglite');
  const realCreate = PGlite.create;
  const root = newTempDataDir(), dir = join(root, 'db');
  const engine = new PGLiteEngine();
  let resume!: () => void, entered!: () => void;
  const barrier = new Promise<void>(resolve => { resume = resolve; });
  const creating = new Promise<void>(resolve => { entered = resolve; });
  PGlite.create = (async (...args: any[]) => {
    entered(); await barrier;
    return (realCreate as any).apply(PGlite, args);
  }) as typeof PGlite.create;
  const connecting = engine.connect({ database_path: dir });
  try {
    await creating;
    let disconnected = false;
    const disconnecting = engine.disconnect().then(() => { disconnected = true; });
    await expect(acquireLock(dir, { timeoutMs: 50 })).rejects.toThrow(/Timed out/);
    expect(disconnected).toBe(false);
    resume();
    await Promise.all([connecting, disconnecting]);
    expect(disconnected).toBe(true);
    const next = await acquireLock(dir, { timeoutMs: 100 });
    await releaseLock(next);
  } finally {
    resume(); PGlite.create = realCreate;
    await connecting.catch(() => {});
    await engine.disconnect();
    rmSync(root, { recursive: true, force: true });
  }
}, 15000);
