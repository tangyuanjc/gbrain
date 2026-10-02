import { test, expect } from 'bun:test';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, readlinkSync, existsSync, rmSync, symlinkSync, statSync } from 'node:fs';
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

const publicationFixture = join(import.meta.dir, 'fixtures/pglite-lock/marker-publication.ts');
test('migration claims legacy directory, then syncs and atomically publishes marker before diagnostic ownership', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
  const dir = join(root, 'db');
  const child = Bun.spawn([process.execPath, publicationFixture, dir, 'trace'], { stdout: 'pipe', stderr: 'inherit' });
  try {
    expect(await child.exited).toBe(0);
    expect(JSON.parse(await new Response(child.stdout).text()).events).toEqual([
      'lockdir-created', 'temp-written', 'file-synced', 'marker-renamed',
      ...(process.platform === 'win32' ? [] : ['directory-synced']),
      'metadata-renamed',
    ]);
  } finally { await child.exited; rmSync(root, { recursive: true, force: true }); }
});

test('steady acquisitions leave the migration marker inode, content and mtime unchanged', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
  const dir = join(root, 'db');
  try {
    await releaseLock(await acquireLock(dir));
    const marker = `${dir}.gbrain-owner.json`;
    const before = statSync(marker);
    const content = readFileSync(marker, 'utf8');
    const child = Bun.spawn([process.execPath, publicationFixture, dir, 'steady'], { stdout: 'pipe', stderr: 'inherit' });
    expect(await child.exited).toBe(0);
    const events = JSON.parse(await new Response(child.stdout).text()).events;
    expect(events.filter((event: string) => event.startsWith('marker') || event.startsWith('temp'))).toEqual([]);
    expect(statSync(marker).ino).toBe(before.ino);
    expect(statSync(marker).mtimeMs).toBe(before.mtimeMs);
    expect(readFileSync(marker, 'utf8')).toBe(content);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

for (const phase of ['temp-written', 'file-synced', 'marker-renamed',
  ...(process.platform === 'win32' ? [] : ['directory-synced']), 'metadata-renamed']) {
  test(`dead-legacy migration SIGKILL after ${phase} leaves a recoverable state`, async () => {
    const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
    const dir = join(root, 'db');
    stale(dir);
    const child = Bun.spawn([process.execPath, publicationFixture, dir, phase], { stdout: 'ignore', stderr: 'inherit' });
    try {
      await wait(root, 'paused');
      const marker = `${dir}.gbrain-owner.json`;
      if (existsSync(marker)) expect(JSON.parse(readFileSync(marker, 'utf8')).protocol).toBe('kernel-v1');
      child.kill('SIGKILL'); await child.exited;
      const successor = await acquireLock(dir, { timeoutMs: 1000 });
      await releaseLock(successor);
    } finally {
      if (child.exitCode === null) child.kill('SIGKILL');
      await child.exited;
      rmSync(root, { recursive: true, force: true });
    }
  }, 15000);
}

test('failed marker write leaves the validated legacy record intact and recoverable', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
  const dir = join(root, 'db');
  stale(dir);
  const legacy = readFileSync(join(dir, '.gbrain-lock', 'lock'), 'utf8');
  const child = Bun.spawn([process.execPath, publicationFixture, dir, 'fail-temp-written'], { stdout: 'pipe', stderr: 'inherit' });
  try {
    expect(await child.exited).toBe(0);
    expect(JSON.parse(await new Response(child.stdout).text()).failure).toContain('synthetic temp-written failure');
    expect(readFileSync(join(dir, '.gbrain-lock', 'lock'), 'utf8')).toBe(legacy);
    expect(existsSync(`${dir}.gbrain-owner.json`)).toBe(false);
    await releaseLock(await acquireLock(dir, { timeoutMs: 1000 }));
  } finally { await child.exited; rmSync(root, { recursive: true, force: true }); }
});

test.skipIf(process.platform === 'win32')('successor syncs a marker left by failed directory fsync before diagnostic ownership', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
  const dir = join(root, 'db');
  stale(dir);
  try {
    const failed = Bun.spawn([process.execPath, publicationFixture, dir, 'fail-directory-sync'], { stdout: 'pipe', stderr: 'inherit' });
    expect(await failed.exited).toBe(0);
    expect(JSON.parse(await new Response(failed.stdout).text()).failure).toContain('synthetic directory fsync failure');
    const before = statSync(`${dir}.gbrain-owner.json`);
    const successor = Bun.spawn([process.execPath, publicationFixture, dir, 'trace'], { stdout: 'pipe', stderr: 'inherit' });
    expect(await successor.exited).toBe(0);
    expect(JSON.parse(await new Response(successor.stdout).text()).events).toEqual(['file-synced', 'directory-synced', 'metadata-renamed']);
    expect(statSync(`${dir}.gbrain-owner.json`).ino).toBe(before.ino);
    expect(statSync(`${dir}.gbrain-owner.json`).mtimeMs).toBe(before.mtimeMs);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

for (const phase of ['lockdir-created', 'metadata-renamed']) {
  test(`migrated owner SIGKILL after ${phase} preserves the marker and recovers`, async () => {
    const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
    const dir = join(root, 'db');
    await releaseLock(await acquireLock(dir));
    const before = statSync(`${dir}.gbrain-owner.json`);
    const child = Bun.spawn([process.execPath, publicationFixture, dir, phase], { stdout: 'ignore', stderr: 'inherit' });
    try {
      await wait(root, 'paused');
      child.kill('SIGKILL'); await child.exited;
      expect(JSON.parse(readFileSync(`${dir}.gbrain-owner.json`, 'utf8')).protocol).toBe('kernel-v1');
      expect(statSync(`${dir}.gbrain-owner.json`).ino).toBe(before.ino);
      expect(statSync(`${dir}.gbrain-owner.json`).mtimeMs).toBe(before.mtimeMs);
      // Live PID reuse in diagnostic kernel metadata must not block native ownership.
      if (phase === 'metadata-renamed') {
        const path = join(dir, '.gbrain-lock', 'lock');
        const metadata = JSON.parse(readFileSync(path, 'utf8'));
        writeFileSync(path, JSON.stringify({ ...metadata, pid: process.pid }));
      }
      await releaseLock(await acquireLock(dir, { timeoutMs: 1000 }));
    } finally {
      if (child.exitCode === null) child.kill('SIGKILL');
      await child.exited;
      rmSync(root, { recursive: true, force: true });
    }
  }, 15000);
}

for (const marker of [undefined, '', JSON.stringify({ protocol: 'kernel-v1' })]) {
  test(`legacy live and unreadable owners stay protected (marker=${marker ?? 'absent'})`, async () => {
    const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
    try {
      for (const content of [JSON.stringify({ pid: process.pid, acquired_at: 1 }), '{broken']) {
        const dir = join(root, content.startsWith('{broken') ? 'unreadable' : 'live');
        mkdirSync(join(dir, '.gbrain-lock'), { recursive: true });
        writeFileSync(join(dir, '.gbrain-lock', 'lock'), content);
        // A valid marker alone cannot identify an unreadable record as legacy.
        // Match the established protocol: unreadable metadata after migration
        // is diagnostic, while unreadable first-migration owners fail closed.
        if (marker !== undefined) writeFileSync(`${dir}.gbrain-owner.json`, marker);
        if (content === '{broken' && marker?.includes('kernel-v1')) continue;
        await expect(acquireLock(dir, { timeoutMs: 50 })).rejects.toThrow(/Timed out/);
        expect(readFileSync(join(dir, '.gbrain-lock', 'lock'), 'utf8')).toBe(content);
      }
    } finally { rmSync(root, { recursive: true, force: true }); }
  });
}

// Child environments keep configuration probes isolated from the test runner.
test('batch lock budget waits for a live holder and still fails at its deadline', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-budget-'));
  const dir = join(root, 'db');
  const code = `import { acquireLock, releaseLock } from ${JSON.stringify(join(import.meta.dir, '../src/core/pglite-lock.ts'))};
    console.log('ready');
    const started = performance.now();
    try { const lock = await acquireLock(process.argv[1]);
      console.log(JSON.stringify({acquired: true, waitedMs: performance.now() - started}));
      await releaseLock(lock);
    } catch (error) { console.log(JSON.stringify({acquired: false, waitedMs: performance.now() - started}));
      console.error(error.message); process.exitCode = 1; }`;
  const spawnBudget = (budget: string) => Bun.spawn([process.execPath, '-e', code, dir], {
    env: { ...process.env, GBRAIN_PGLITE_LOCK_TIMEOUT_MS: budget }, stdout: 'pipe', stderr: 'pipe',
  });
  const holder = await acquireLock(dir);
  const children: ReturnType<typeof spawnBudget>[] = [];
  try {
    const blocked = spawnBudget('100'); children.push(blocked);
    expect(await blocked.exited).toBe(1);
    expect(await new Response(blocked.stderr).text()).toContain('Timed out waiting');
    const timeout = JSON.parse((await new Response(blocked.stdout).text()).trim().split('\n').at(-1)!);
    expect(timeout.waitedMs).toBeGreaterThanOrEqual(80);
    expect(timeout.waitedMs).toBeLessThan(1000);
    expect(JSON.parse(readFileSync(join(dir, '.gbrain-lock/lock'), 'utf8')).owner_token).toBe(holder.ownerToken);
    const waiting = spawnBudget('3000'); children.push(waiting);
    const reader = waiting.stdout.getReader();
    const first = await reader.read();
    expect(new TextDecoder().decode(first.value)).toContain('ready');
    await Bun.sleep(250);
    expect(waiting.exitCode).toBeNull();
    await releaseLock(holder);
    let output = '';
    for (;;) { const part = await reader.read(); if (part.done) break; output += new TextDecoder().decode(part.value); }
    expect(await waiting.exited).toBe(0);
    const result = JSON.parse(output.trim());
    expect(result.acquired).toBe(true);
    expect(result.waitedMs).toBeGreaterThanOrEqual(200);
    expect(existsSync(getPgliteKernelLockPath(dir)!)).toBe(true);
  } finally {
    await releaseLock(holder);
    await Promise.all(children.map(child => child.exited));
    rmSync(root, { recursive: true, force: true });
  }
}, 10000);

test('invalid batch budgets fail closed and explicit timeout takes precedence', async () => {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-budget-validation-'));
  const dir = join(root, 'db');
  const modulePath = JSON.stringify(join(import.meta.dir, '../src/core/pglite-lock.ts'));
  try {
    for (const budget of ['', '-1', '1.5', 'NaN', 'Infinity', '2147483648']) {
      const child = Bun.spawn([process.execPath, '-e', `import {acquireLock} from ${modulePath}; await acquireLock(process.argv[1]);`, dir], {
        env: { ...process.env, GBRAIN_PGLITE_LOCK_TIMEOUT_MS: budget }, stdout: 'ignore', stderr: 'pipe',
      });
      expect(await child.exited).not.toBe(0);
      expect(await new Response(child.stderr).text()).toContain('Invalid');
    }
    expect(existsSync(getPgliteKernelLockPath(dir)!)).toBe(false);
    const child = Bun.spawn([process.execPath, '-e', `import {acquireLock,releaseLock} from ${modulePath}; await releaseLock(await acquireLock(process.argv[1], {timeoutMs: 100}));`, dir], {
      env: { ...process.env, GBRAIN_PGLITE_LOCK_TIMEOUT_MS: 'invalid' }, stdout: 'ignore', stderr: 'pipe',
    });
    expect(await child.exited).toBe(0);
  } finally { rmSync(root, { recursive: true, force: true }); }
});
