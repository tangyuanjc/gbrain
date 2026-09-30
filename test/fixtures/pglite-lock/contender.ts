// Isolated subprocess helper: never opens PGLite or a user's configuration.
import * as fs from 'node:fs';
import { join } from 'node:path';
const [dir, id, mode = 'stress'] = process.argv.slice(2);
if (!dir.includes('gbrain-kernel-test-')) throw new Error('isolated lock tests only');
const wait = async (name: string) => {
  const deadline = Date.now() + 15000;
  while (!fs.existsSync(join(dir, name))) {
    if (Date.now() > deadline) throw new Error(`barrier timeout: ${name}`);
    await Bun.sleep(2);
  }
};
if (mode === 'controlled') {
  const kill = process.kill.bind(process);
  process.kill = ((pid: number, signal?: string | number) => {
    if (pid === 999999999) {
      fs.writeFileSync(join(dir, `${id}-stale-observed`), '1');
      const deadline = Date.now() + 15000;
      while (!fs.existsSync(join(dir, `${id}-continue`))) {
        if (Date.now() > deadline) throw new Error('stale-read barrier timeout');
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2);
      }
    }
    return kill(pid, signal as any);
  }) as typeof process.kill;
}
const { acquireLock, releaseLock } = await import('../../../src/core/pglite-lock.ts');
fs.writeFileSync(join(dir, `${id}-ready`), String(process.pid));
await wait('start');
let lock;
try { lock = await acquireLock(dir, { timeoutMs: mode === 'controlled' ? 1500 : 10000 }); }
catch (error) {
  if (mode === 'controlled' && String(error).includes('Timed out waiting for PGLite lock')) {
    fs.writeFileSync(join(dir, `${id}-timeout`), String(error));
    process.exit(0);
  }
  throw error;
}
try {
  fs.writeFileSync(join(dir, `${id}-acquired`), JSON.stringify({ pid: process.pid, token: lock.ownerToken }));
  if (mode === 'stress') {
    const critical = join(dir, 'critical-section');
    const fd = fs.openSync(critical, 'wx'); // An independent exclusivity oracle.
    try { await Bun.sleep(3); } finally { fs.closeSync(fd); fs.unlinkSync(critical); }
  } else await wait('release');
} finally { await releaseLock(lock, 'not_opened'); }
