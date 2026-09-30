import { test, expect } from 'bun:test';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { withEnv } from './helpers/with-env.ts';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { runEmbedCore } from '../src/commands/embed.ts';
import { configureGateway, resetGateway, __setEmbedTransportForTests } from '../src/core/ai/gateway.ts';

for (const mode of ['http', 'backoff'] as const) {
  test(`stale CLI budget cancels ${mode}, restores and closes database within 5s`, async () => {
    const root = mkdtempSync(join(tmpdir(), 'gbrain-embed-budget-'));
    const db = join(root, 'db');
    const engine = new PGLiteEngine();
    let calls = 0, requestAborted = false;
    const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: () => new Promise<Response>(() => {}) });
    try {
      await withEnv({ GBRAIN_HOME: root, GBRAIN_EMBED_TIME_BUDGET_MS: '400', GBRAIN_EMBED_CONCURRENCY: '1' }, async () => {
        configureGateway({ embedding_model: 'openai:text-embedding-3-small', embedding_dimensions: 1536, env: { OPENAI_API_KEY: 'test-only' } });
        await engine.connect({ database_path: db });
        await engine.initSchema();
        await engine.putPage('note', { title: 'note', type: 'note', compiled_truth: 'text' });
        await engine.upsertChunks('note', [{ chunk_index: 0, chunk_text: 'text', chunk_source: 'compiled_truth' }]);
        __setEmbedTransportForTests(async (opts) => {
          calls++;
          if (mode === 'backoff') throw Object.assign(new Error('429 try again in 60s'), { status: 429 });
          try { await fetch(server.url, { signal: opts.abortSignal }); }
          catch (error) { requestAborted = opts.abortSignal?.aborted === true; throw error; }
          throw new Error('unreachable');
        });
        const started = performance.now();
        const result = await runEmbedCore(engine, { stale: true, exclusivePglite: true });
        expect(result.embedded).toBe(0);
        expect(calls).toBe(1);
        if (mode === 'http') expect(requestAborted).toBe(true);
        expect(await engine.countStaleChunks()).toBe(1);
        await engine.disconnect();
        const elapsed = performance.now() - started;
        expect(elapsed).toBeLessThan(5000);
        const events = readFileSync(join(db, '.gbrain-lock-events.jsonl'), 'utf8').trim().split('\n').map(row => JSON.parse(row));
        expect(events.filter(e => e.event === 'acquired').length).toBe(2);
        expect(events.filter(e => e.event === 'released').length).toBe(2);
        console.log(JSON.stringify({ mode, elapsed_ms: elapsed, calls, requestAborted }));
      });
    } finally {
      __setEmbedTransportForTests(null); resetGateway();
      server.stop(true);
      await engine.disconnect();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30000);
}
