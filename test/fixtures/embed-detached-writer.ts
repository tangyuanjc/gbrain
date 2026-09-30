import { PGLiteEngine } from '../../src/core/pglite-engine.ts';

const engine = new PGLiteEngine();
console.log('waiting');
await engine.connect({ database_path: process.argv[2] });
try {
  await engine.upsertChunks('edited', [{ chunk_index: 0, chunk_text: 'new text', chunk_source: 'compiled_truth' }]);
  await engine.executeRaw("DELETE FROM pages WHERE slug IN ('deleted', 'recreated')");
  await engine.putPage('recreated', { title: 'recreated', type: 'note', compiled_truth: 'original' });
  await engine.upsertChunks('recreated', [{ chunk_index: 0, chunk_text: 'original', chunk_source: 'compiled_truth' }]);
  await engine.setPageEmbeddingSignature('signature', { signature: 'other:model:1536' });
  const vector = new Float32Array(1536); vector[0] = 1;
  await engine.upsertChunks('completed', [{ chunk_index: 0, chunk_text: 'original', chunk_source: 'compiled_truth', embedding: vector, model: 'other' }]);
  await engine.executeRaw("UPDATE pages SET frontmatter = '{\"embed_skip\":true}' WHERE slug = 'skip'");
} finally { await engine.disconnect(); }
console.log('writer committed');
