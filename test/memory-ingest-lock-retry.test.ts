import { test, expect } from 'bun:test';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { importWithLockRetry, runMemoryIngest, type ImportCommandResult } from '../scripts/local/memory-ingest.ts';

const busy = { exitCode: 1, stdout: '', stderr: 'Timed out waiting for PGLite lock' };
const success = { exitCode: 0, stderr: '', stdout: JSON.stringify({ status: 'success', imported: 1, skipped: 0, errors: 0, total_files: 1 }) };

test('lock retries share one deadline; success is retried without inflating import budget', () => {
  let now = 0, calls = 0;
  const budgets: number[] = [];
  const outcome = importWithLockRetry((_args, opts) => {
    budgets.push(opts.timeoutMs); now += 30000;
    return ++calls < 3 ? busy : success;
  }, [], { cwd: '.', timeoutMs: 300000 }, () => now, ms => { now += ms; });
  expect(outcome).toEqual({ result: success, retries: 2 });
  expect(budgets[1]).toBeLessThan(270000);
  expect(budgets[2]).toBeLessThan(240000);
});

test('ordinary failures and actual process timeouts are never retried', () => {
  for (const result of [{ ...busy, stderr: 'query timeout' }, { ...busy, timedOut: true }]) {
    let calls = 0;
    const outcome = importWithLockRetry(() => { calls++; return result; }, [], { cwd: '.', timeoutMs: 300000 });
    expect(calls).toBe(1);
    expect(outcome.retries).toBe(0);
  }
});

test('changed-file ingest distinguishes lock failure from process timeout and keeps failed digest retryable', () => {
  for (const [result, field] of [[busy, 'lock_timeout_files'], [{ ...busy, timedOut: true }, 'timed_out_files']] as const) {
    const root = mkdtempSync(join(tmpdir(), 'gbrain-ingest-retry-'));
    try {
      const source = join(root, 'source'); mkdirSync(source);
      writeFileSync(join(source, 'note.md'), '# Updated note\n');
      let now = 0;
      const options = {
        home: root, sources: [{ root: source, slugPrefix: 'memory' }],
        monotonicNow: () => now, sleep: (ms: number) => { now += ms; },
        runImport: (): ImportCommandResult => { now += 30000; return result; },
      };
      const summary = runMemoryIngest(options);
      expect(summary.status).toBe('failed');
      expect(summary.changed_files).toBe(1);
      expect(summary.failed_files).toBe(1);
      expect(summary[field]).toBe(1);
      if (field === 'lock_timeout_files') expect(summary.timed_out_files).toBeUndefined();
      const state = JSON.parse(readFileSync(join(root, '.gbrain/memory-ingest-state.json'), 'utf8'));
      expect(state.files).toEqual({});
      expect(runMemoryIngest({ ...options, runImport: () => success })).toMatchObject({ status: 'success', changed_files: 1, imported_pages: 1 });
    } finally { rmSync(root, { recursive: true, force: true }); }
  }
});
