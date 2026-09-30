import { test, expect } from 'bun:test';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, readlinkSync, existsSync, rmSync, symlinkSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { acquireLock, releaseLock, getPgliteKernelLockPath } from '../src/core/pglite-lock.ts';

const fixture = join(import.meta.dir, 'fixtures/pglite-lock/contender.ts');
export function stale(dir: string): void {
  mkdirSync(join(dir, '.gbrain-lock'), { recursive: true });
  writeFileSync(join(dir, '.gbrain-lock', 'lock'), JSON.stringify({ pid: 999999999, acquired_at: 1,
    ...(process.platform === 'linux' ? { pid_ns: readlinkSync('/proc/self/ns/pid'), boot_id: readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim() } : {}),
  }));
}
async function wait(dir: string, file: string): Promise<void> {
  const deadline = Date.now() + 10000;
  while (!existsSync(join(dir, file))) {
    if (Date.now() > deadline) throw new Error(`barrier timeout: ${file}`);
    await Bun.sleep(5);
  }
}
function spawn(dir: string, id: string, mode: string) {
  return Bun.spawn([process.execPath, fixture, dir, id, mode], { stdout: 'ignore', stderr: 'inherit' });
}

test('controlled stale-read interleaving admits one owner, not two', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
  const dir = join(root, 'db'); stale(dir);
  const children = [spawn(dir, 'A', 'controlled'), spawn(dir, 'B', 'controlled')];
  try {
    await Promise.all(['A-ready', 'B-ready'].map(file => wait(dir, file)));
    writeFileSync(join(dir, 'start'), '1');
    const deadline = Date.now() + 10000;
    while (!['A', 'B'].some(id => existsSync(join(dir, `${id}-stale-observed`)))) {
      if (Date.now() > deadline) throw new Error('no stale observation');
      await Bun.sleep(5);
    }
    const winner = existsSync(join(dir, 'A-stale-observed')) ? 'A' : 'B';
    const loser = winner === 'A' ? 'B' : 'A';
    await Bun.sleep(100);
    // The second process cannot even observe stale metadata while the first
    // is paused before takeover: the kernel serializes that old race window.
    expect(existsSync(join(dir, `${loser}-stale-observed`))).toBe(false);
    writeFileSync(join(dir, `${winner}-continue`), '1');
    await wait(dir, `${winner}-acquired`);
    await wait(dir, `${loser}-timeout`);
    expect(existsSync(join(dir, `${loser}-acquired`))).toBe(false);
    const owner = JSON.parse(readFileSync(join(dir, '.gbrain-lock', 'lock'), 'utf8'));
    expect(owner.pid).toBe(JSON.parse(readFileSync(join(dir, `${winner}-acquired`), 'utf8')).pid);
    console.log(JSON.stringify({ probe: 'controlled-stale-lock', winner, loser, acquired: 1, loserTimedOut: true, onDiskOwner: owner.pid, productionDbOpened: false }));
    writeFileSync(join(dir, 'release'), '1');
    expect(await Promise.all(children.map(child => child.exited))).toEqual([0, 0]);
  } finally {
    for (const child of children) if (child.exitCode === null) child.kill();
    await Promise.all(children.map(child => child.exited));
    rmSync(root, { recursive: true, force: true });
  }
}, 15000);

test('SIGKILL releases native ownership and a successor recovers', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
  const dir = join(root, 'db'); mkdirSync(dir);
  const child = spawn(dir, 'A', 'hold');
  try {
    await wait(dir, 'A-ready'); writeFileSync(join(dir, 'start'), '1');
    await wait(dir, 'A-acquired');
    await expect(acquireLock(dir, { timeoutMs: 50 })).rejects.toThrow(/Timed out/);
    child.kill('SIGKILL'); await child.exited;
    const successor = await acquireLock(dir, { timeoutMs: 1000 });
    expect(successor.acquired).toBe(true);
    await releaseLock(successor);
    expect(existsSync(getPgliteKernelLockPath(dir)!)).toBe(true);
  } finally {
    if (child.exitCode === null) child.kill();
    await child.exited;
    rmSync(root, { recursive: true, force: true });
  }
}, 15000);

test('metadata removal and symlink aliases cannot bypass a live kernel owner', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
  const dir = join(root, 'db');
  const lock = await acquireLock(dir);
  try {
    symlinkSync(dir, join(root, 'alias'));
    rmSync(join(dir, '.gbrain-lock'), { recursive: true });
    await expect(acquireLock(join(root, 'alias'), { timeoutMs: 50 })).rejects.toThrow(/Timed out/);
    writeFileSync(join(dir, '.gbrain-lock'), 'bad metadata');
    await expect(acquireLock(dir, { timeoutMs: 50 })).rejects.toThrow(/Timed out/);
  } finally { await releaseLock(lock); rmSync(root, { recursive: true, force: true }); }
});

test('corrupted migrated metadata does not strand the native lock', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
  const dir = join(root, 'db');
  try {
    const first = await acquireLock(dir);
    writeFileSync(join(dir, '.gbrain-lock', 'lock'), '{broken');
    await releaseLock(first);
    const second = await acquireLock(dir, { timeoutMs: 100 });
    await releaseLock(second);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('failed native release does not report a successful release', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
  const dir = join(root, 'db');
  const lock = await acquireLock(dir);
  const release = lock.nativeLock!.release;
  try {
    lock.nativeLock!.release = async () => { throw new Error('synthetic native close failure'); };
    await expect(releaseLock(lock)).rejects.toThrow('synthetic native close failure');
    const events = readFileSync(join(dir, '.gbrain-lock-events.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
    expect(events.map(event => event.event)).toEqual(['acquired']);
    await expect(acquireLock(dir, { timeoutMs: 50 })).rejects.toThrow(/Timed out/);
  } finally {
    lock.nativeLock!.release = release;
    await releaseLock(lock);
    rmSync(root, { recursive: true, force: true });
  }
});

test('native close errors remain failures on repeated release without retrying close', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
  const dir = join(root, 'db');
  const child = Bun.spawn([process.execPath, join(import.meta.dir, 'fixtures/pglite-lock/native-close-failure.ts'), dir], { stdout: 'pipe', stderr: 'inherit' });
  try {
    expect(await child.exited).toBe(0);
    expect(JSON.parse(await new Response(child.stdout).text())).toEqual({ calls: 1, failures: 2 });
    const events = readFileSync(join(dir, '.gbrain-lock-events.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
    expect(events.map(event => event.event)).toEqual(['acquired']);
    const next = await acquireLock(dir, { timeoutMs: 100 });
    await releaseLock(next);
  } finally {
    if (child.exitCode === null) child.kill();
    await child.exited;
    rmSync(root, { recursive: true, force: true });
  }
});
