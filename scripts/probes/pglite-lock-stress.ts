/** Eight real contenders per dead legacy owner, 100 fresh rounds by default. */
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, readlinkSync, existsSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
const rounds = Number(process.argv[2] ?? 100), contenders = 8;
if (!Number.isSafeInteger(rounds) || rounds < 1) throw new Error('positive integer rounds required');
const root = mkdtempSync(join(tmpdir(), 'gbrain-kernel-test-'));
const fixture = join(import.meta.dir, '../../test/fixtures/pglite-lock/contender.ts');
let acquired = 0, overlaps = 0, maxConcurrent = 0;
try {
  for (let round = 0; round < rounds; round++) {
    const dir = join(root, String(round));
    mkdirSync(join(dir, '.gbrain-lock'), { recursive: true });
    writeFileSync(join(dir, '.gbrain-lock', 'lock'), JSON.stringify({ pid: 999999999, acquired_at: 1,
      ...(process.platform === 'linux' ? { pid_ns: readlinkSync('/proc/self/ns/pid'), boot_id: readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim() } : {}),
    }));
    const children = Array.from({ length: contenders }, (_, i) => Bun.spawn([process.execPath, fixture, dir, String(i)], { stdout: 'ignore', stderr: 'inherit' }));
    try {
      const deadline = Date.now() + 15000;
      while (!children.every((_, i) => existsSync(join(dir, `${i}-ready`)))) {
        if (Date.now() > deadline) throw new Error(`round ${round}: ready timeout`);
        await Bun.sleep(5);
      }
      writeFileSync(join(dir, 'start'), '1');
      if ((await Promise.all(children.map(child => child.exited))).some(code => code !== 0)) throw new Error(`round ${round}: contender failed`);
      const live = new Set<string>();
      for (const line of readFileSync(join(dir, '.gbrain-lock-events.jsonl'), 'utf8').trim().split('\n')) {
        const event = JSON.parse(line);
        if (event.event === 'acquired') { if (live.size) overlaps++; live.add(event.owner_token); acquired++; maxConcurrent = Math.max(maxConcurrent, live.size); }
        else if (!live.delete(event.owner_token)) throw new Error('unmatched release');
      }
      if (live.size) throw new Error('unreleased owner');
      console.log(JSON.stringify({ round: round + 1, contenders, overlaps, acquired }));
    } finally {
      for (const child of children) if (child.exitCode === null) child.kill();
      await Promise.all(children.map(child => child.exited));
    }
  }
  console.log(JSON.stringify({ rounds, contenders, acquired, overlaps, maxConcurrent, productionDbOpened: false }));
  if (overlaps !== 0 || acquired !== rounds * contenders) throw new Error('exclusivity violated');
} finally { rmSync(root, { recursive: true, force: true }); }
