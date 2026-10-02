// Fault injection runs in a fresh process; no module mocks reach other tests.
import { mock } from 'bun:test';
import * as fs from 'node:fs';
import { basename, dirname, join, resolve } from 'node:path';
const real = { ...fs };
const [input, mode] = process.argv.slice(2);
if (!input.includes('gbrain-kernel-test-')) throw new Error('isolated lock tests only');
const dir = join(real.realpathSync(dirname(input)), basename(input));
const marker = `${dir}.gbrain-owner.json`;
// Bun intrinsifies several node:fs calls, bypassing built-in module mocks.
// Compile the actual source against a local forwarding module for injection.
const shim = join(dirname(dir), 'instrumented-fs.ts');
const copy = join(dirname(dir), 'lock-impl.ts');
real.writeFileSync(shim, "export * from 'node:fs';\n");
const source = resolve(import.meta.dir, '../../../src/core/pglite-lock.ts');
real.writeFileSync(copy, real.readFileSync(source, 'utf8')
  .replace("'node:fs'", JSON.stringify(shim))
  .replace("'./persistence/native-lock.ts'", JSON.stringify(resolve(dirname(source), 'persistence/native-lock.ts'))));
let markerFd: number | undefined;
let parentFd: number | undefined;
const events: string[] = [];
function step(name: string) {
  events.push(name);
  if (mode === `fail-${name}`) throw new Error(`synthetic ${name} failure`);
  if (mode !== name) return;
  real.writeFileSync(join(dirname(dir), 'paused'), name);
  // Parent kills this exact child, simulating a crash at a deterministic point.
  for (;;) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
}
mock.module(shim, () => ({ ...real,
  openSync: (...args: Parameters<typeof fs.openSync>) => {
    const fd = real.openSync(...args);
    if (String(args[0]).startsWith(`${marker}.tmp-`)) {
      if (mode === 'steady') throw new Error('steady acquisition rewrote the marker');
      markerFd = fd;
    } else if (String(args[0]) === dirname(marker)) parentFd = fd;
    return fd;
  },
  writeFileSync: (...args: Parameters<typeof fs.writeFileSync>) => {
    if (args[0] === marker) throw new Error('marker must never be truncated');
    real.writeFileSync(...args);
    if (args[0] === markerFd) step('temp-written');
  },
  fsyncSync: (fd: number) => {
    if (mode === 'fail-directory-sync' && fd === parentFd) throw new Error('synthetic directory fsync failure');
    real.fsyncSync(fd);
    step(fd === parentFd ? 'directory-synced' : 'file-synced');
  },
  renameSync: (...args: Parameters<typeof fs.renameSync>) => {
    real.renameSync(...args);
    if (args[1] === marker) step('marker-renamed');
    else if (String(args[1]) === join(dir, '.gbrain-lock', 'lock')) step('metadata-renamed');
  },
  mkdirSync: (...args: Parameters<typeof fs.mkdirSync>) => {
    const result = real.mkdirSync(...args);
    if (String(args[0]) === join(dir, '.gbrain-lock')) step('lockdir-created');
    return result;
  },
}));
const { acquireLock, releaseLock } = await import(copy);
try {
  for (let i = 0; i < (mode === 'steady' ? 3 : 1); i++) {
    const lock = await acquireLock(dir, { timeoutMs: 1000 });
    await releaseLock(lock, 'not_opened');
  }
  console.log(JSON.stringify({ events }));
} catch (error) {
  if (!mode.startsWith('fail-') || !String(error).includes('synthetic')) throw error;
  console.log(JSON.stringify({ events, failure: String(error) }));
}
