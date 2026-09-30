import { mock } from 'bun:test';
import { resolve } from 'node:path';
import { family } from 'detect-libc';
const dir = process.argv[2];
if (!dir.includes('gbrain-kernel-test-')) throw new Error('isolated lock tests only');
const target = `${process.platform}-${process.arch}${process.platform === 'linux' ? `-${await family()}` : ''}`;
const path = resolve(import.meta.dir, `../../../native/locks/prebuilds/${target}.node`);
const binding = require(path);
let calls = 0, failures = 0;
const failingBinding = { target: binding.target, openLock: binding.openLock, tryLock: binding.tryLock,
  openIpcMutex: binding.openIpcMutex, removeWindowsUnixSocket: binding.removeWindowsUnixSocket,
  close: () => { calls++; throw new Error('synthetic binding close failure'); } };
mock.module(path, () => failingBinding);
const { acquireLock, releaseLock } = await import('../../../src/core/pglite-lock.ts');
const lock = await acquireLock(dir);
for (let i = 0; i < 2; i++) {
  try { await releaseLock(lock); }
  catch (error) { if (String(error).includes('Cannot close the writer lock handle')) failures++; else throw error; }
}
console.log(JSON.stringify({ calls, failures }));
if (calls !== 1 || failures !== 2) throw new Error('close failure was retried or forgotten');
// No close retry: process teardown releases retained native handles.
