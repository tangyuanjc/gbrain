import { test, expect } from 'bun:test';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { embedDetachedBatch } from '../src/core/embed-detached.ts';

async function fixture(run: (engine: PGLiteEngine, path: string) => Promise<void>) {
  const root = mkdtempSync(join(tmpdir(), 'gbrain-detached-'));
  const path = join(root, 'db');
  const engine = new PGLiteEngine();
  try {
    await engine.connect({ database_path: path });
    await engine.initSchema();
    await run(engine, path);
  } finally {
    await engine.disconnect();
    rmSync(root, { recursive: true, force: true });
  }
}
async function seed(engine: PGLiteEngine, slug: string) {
  await engine.putPage(slug, { title: slug, type: 'note', compiled_truth: 'original' });
  await engine.upsertChunks(slug, [{ chunk_index: 0, chunk_text: 'original', chunk_source: 'compiled_truth', language: 'markdown' }]);
}
const vector = () => { const v = new Float32Array(1536); v[0] = 1; return v; };
const defaults = () => ({ receipts: new Map(), signature: 'test:model:1536', model: 'test-model', concurrency: 4, signal: new AbortController().signal });

test('waiting writer opens the real datastore during model wait; guarded writes reject mutations', async () => {
  await fixture(async (engine, path) => {
    const slugs = ['same', 'edited', 'deleted', 'recreated', 'signature', 'completed', 'skip'];
    for (const slug of slugs) await seed(engine, slug);
    const batch = await engine.listStaleChunks();
    // Separate process queues behind our live owner before compute starts.
    const child = Bun.spawn([process.execPath, new URL('./fixtures/embed-detached-writer.ts', import.meta.url).pathname, path], { stdout: 'pipe', stderr: 'pipe' });
    const stderr = new Response(child.stderr).text();
    const reader = child.stdout.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toContain('waiting');
    const stdout = (async () => {
      let output = '';
      for (;;) { const part = await reader.read(); if (part.done) break; output += new TextDecoder().decode(part.value); }
      return output;
    })();
    const writerDone = (async () => {
      const exit = await child.exited;
      const errors = await stderr;
      const output = await stdout;
      if (exit !== 0) throw new Error(`Writer exited ${exit}: ${errors}`);
      expect(output).toContain('writer committed');
    })();
    const outcome = await embedDetachedBatch(engine, batch, {
      ...defaults(), embed: async texts => {
        await writerDone; // Deadlocks/timeouts if compute still holds the DB.
        return texts.map(vector);
      },
    });
    await writerDone;
    expect(outcome).toEqual({ embedded: 1, skipped: 6, failures: [] });
    expect((await engine.getChunks('same'))[0]).toMatchObject({ model: 'test-model', language: 'markdown' });
    expect(await engine.executeRaw("SELECT embedding IS NOT NULL AS present FROM content_chunks cc JOIN pages p ON p.id = cc.page_id WHERE p.slug = 'same'")).toEqual([{ present: true }]);
    expect((await engine.getChunks('edited'))[0]).toMatchObject({ chunk_text: 'new text', embedding: null });
    expect(await engine.getChunks('deleted')).toEqual([]);
    expect(await engine.executeRaw("SELECT p.slug FROM pages p JOIN content_chunks cc ON cc.page_id = p.id WHERE cc.embedding IS NOT NULL ORDER BY p.slug")).toEqual([{ slug: 'completed' }, { slug: 'same' }]);
    expect((await engine.getChunks('completed'))[0].model).toBe('other');
    const events = readFileSync(join(path, '.gbrain-lock-events.jsonl'), 'utf8').trim().split('\n').map(l => JSON.parse(l));
    expect(events.filter(e => e.event === 'acquired').length).toBeGreaterThanOrEqual(3);
  });
}, 60000);

test('abort cancels model wait, restores connection, and never writes partial vectors', async () => {
  await fixture(async (engine) => {
    await seed(engine, 'cancel');
    const controller = new AbortController();
    const start = performance.now();
    const outcome = await embedDetachedBatch(engine, await engine.listStaleChunks(), {
      ...defaults(), signal: controller.signal,
      embed: async () => {
        const timer = setTimeout(() => controller.abort(), 20);
        try { await new Promise((_resolve, reject) => controller.signal.addEventListener('abort', () => reject(new Error('cancelled')), { once: true })); }
        finally { clearTimeout(timer); }
        return [vector()];
      },
    });
    expect(outcome.embedded).toBe(0);
    expect(outcome.failures).toEqual([]);
    expect(await engine.countStaleChunks()).toBe(1);
    expect(performance.now() - start).toBeLessThan(5000);
  });
}, 30000);

test('close failure does not begin model calls or reopen uncertain ownership', async () => {
  let called = false;
  let reopened = false;
  const engine = {
    kind: 'pglite', executeRaw: async () => [{ id: 1, embedding_signature: null }],
    disconnect: async () => { throw new Error('close failed'); },
    reconnect: async () => { reopened = true; },
  } as unknown as PGLiteEngine;
  await expect(embedDetachedBatch(engine, [{ page_id: 1, chunk_index: 0, chunk_text: 'x', chunk_source: 'compiled_truth', slug: 'a', source_id: 'default', model: null, token_count: 1 }], {
    ...defaults(), embed: async () => { called = true; return [vector()]; },
  })).rejects.toThrow('close failed');
  expect(called).toBe(false);
  expect(reopened).toBe(false);
});

test('65-chunk pages converge across batches; concurrent rewrite invalidates earlier receipts', async () => {
  await fixture(async (engine, path) => {
    for (const slug of ['large', 'changed-between-batches']) {
      await seed(engine, slug);
      await engine.upsertChunks(slug, Array.from({ length: 65 }, (_, i) => ({
        chunk_index: i, chunk_text: `chunk ${i}`, chunk_source: 'compiled_truth' as const,
      })));
      await engine.setPageEmbeddingSignature(slug, { signature: 'old' });
      const opts = defaults();
      const rows = (await engine.listStaleChunks()).filter(row => row.slug === slug);
      const first = await embedDetachedBatch(engine, rows.slice(0, 64), { ...opts, embed: async texts => texts.map(vector) });
      expect(first.embedded).toBe(64);
      expect(await engine.executeRaw('SELECT embedding_signature FROM pages WHERE slug = $1', [slug])).toEqual([{ embedding_signature: 'old' }]);
      const last = await embedDetachedBatch(engine, rows.slice(64), {
        ...opts, embed: async texts => {
          if (slug === 'changed-between-batches') {
            const writer = new PGLiteEngine();
            await writer.connect({ database_path: path });
            try { await writer.executeRaw("UPDATE content_chunks SET chunk_text = 'changed', embedding = NULL, embedded_at = NULL WHERE page_id = $1 AND chunk_index = 0", [rows[0].page_id]); }
            finally { await writer.disconnect(); }
          }
          return texts.map(vector);
        },
      });
      expect(last.embedded).toBe(1);
      expect(await engine.executeRaw('SELECT embedding_signature FROM pages WHERE slug = $1', [slug])).toEqual([{ embedding_signature: slug === 'large' ? opts.signature : 'old' }]);
    }
  });
}, 30000);
