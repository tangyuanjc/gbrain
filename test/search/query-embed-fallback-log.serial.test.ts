import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { PGLiteEngine } from '../../src/core/pglite-engine.ts';
import { hybridSearch } from '../../src/core/search/hybrid.ts';
import {
  configureGateway,
  resetGateway,
  __setEmbedTransportForTests,
} from '../../src/core/ai/gateway.ts';

let engine: PGLiteEngine;

beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
  await engine.putPage('notes/keyword-fallback', {
    type: 'note',
    title: 'Keyword fallback fixture',
    compiled_truth: 'needle appears in this fixture',
  });
  configureGateway({
    embedding_model: 'openai:text-embedding-3-large',
    embedding_dimensions: 1536,
    env: { OPENAI_API_KEY: 'sk-test' },
  });
}, 120_000);

afterAll(async () => {
  __setEmbedTransportForTests(null);
  resetGateway();
  await engine.disconnect();
}, 120_000);

describe('hybridSearch query-embedding fallback observability', () => {
  test('logs a bounded query summary, elapsed time, and reason before keyword fallback', async () => {
    const privateTail = 'PRIVATE-TAIL-MUST-NOT-BE-LOGGED';
    const query = `needle ${'x'.repeat(160)} ${privateTail}`;
    __setEmbedTransportForTests(() => {
      throw new Error('synthetic embedding outage');
    });

    const originalError = console.error;
    const stderr: string[] = [];
    console.error = (...args: unknown[]) => { stderr.push(args.map(String).join(' ')); };
    try {
      await hybridSearch(engine, query, { expansion: false, limit: 5 });
    } finally {
      console.error = originalError;
    }

    const warning = stderr.find((line) => line.includes('[gbrain search] query embedding failed'));
    expect(warning).toBeDefined();
    expect(warning).toContain('query="needle ');
    expect(warning).toMatch(/elapsed_ms=\d+/);
    expect(warning).toContain('reason="');
    expect(warning).toContain('synthetic embedding outage');
    expect(warning).not.toContain(privateTail);
    expect(warning!.length).toBeLessThan(320);
  }, 120_000);
});
