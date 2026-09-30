import type { BrainEngine } from './engine.ts';
import type { StaleChunkRow } from './types.ts';
import { runSlidingPool } from './worker-pool.ts';
import { EMBED_SKIP_FILTER_FRAGMENT } from './embed-skip.ts';

export type EmbedReceipts = Map<number, Map<number, { id: number; embedded_at: string; chunk_text: string; chunk_source: string }>>;

type Snapshot = StaleChunkRow & { id: number; embedding_signature: string | null };

/** Only for an exclusively owned, file-backed PGLite CLI connection.
 * No engine access (including progress callbacks) is allowed during compute.
 * Callers bound rows to 64 so read/write/close phases remain small.
 */
export async function embedDetachedBatch(
  engine: BrainEngine,
  rows: StaleChunkRow[],
  opts: {
    receipts: EmbedReceipts;
    signature: string;
    model: string;
    concurrency: number;
    signal: AbortSignal;
    embed: (texts: string[], signal: AbortSignal) => Promise<Float32Array[]>;
  },
): Promise<{ embedded: number; skipped: number; failures: string[] }> {
  if (engine.kind !== 'pglite' || rows.length > 64) throw new Error('Detached embedding requires PGLite and at most 64 chunks');
  const snapshots: Snapshot[] = [];
  for (const row of rows) {
    const [snapshot] = await engine.executeRaw<Snapshot>(
      `SELECT cc.id, p.embedding_signature FROM content_chunks cc
       JOIN pages p ON p.id = cc.page_id
       WHERE cc.page_id = $1 AND cc.chunk_index = $2 AND cc.chunk_text = $3
         AND cc.chunk_source = $4 AND cc.embedding IS NULL AND p.deleted_at IS NULL
         AND ${EMBED_SKIP_FILTER_FRAGMENT}`,
      [row.page_id, row.chunk_index, row.chunk_text, row.chunk_source],
    );
    if (snapshot) snapshots.push({ ...row, ...snapshot });
  }
  const groups = Map.groupBy(snapshots, row => row.page_id);
  const vectors = new Map<number, Float32Array>();
  const failures: string[] = [];

  // disconnect resolves only after DB close AND native ownership release.
  // Never release a lock directly or race close against a timer.
  await engine.disconnect();
  const releasedAt = performance.now();
  try {
    await runSlidingPool({
      items: [...groups.values()],
      workers: opts.concurrency,
      signal: opts.signal,
      onItem: async group => {
        try {
          const embedded = await opts.embed(group.map(row => row.chunk_text), opts.signal);
          if (opts.signal.aborted) return;
          if (embedded.length !== group.length) throw new Error('Embedding response count mismatch');
          group.forEach((row, i) => vectors.set(row.id, embedded[i]));
        } catch (error) {
          if (!opts.signal.aborted) failures.push(`${group[0].slug}: ${String(error)}`);
        }
      },
    });
  } finally {
    // Even an instant local model yields across several lock polling cycles.
    // Restore before returning: caller teardown/progress may use the engine.
    await new Promise(resolve => setTimeout(resolve, Math.max(0, 500 - (performance.now() - releasedAt))));
    await engine.reconnect();
  }

  let embedded = 0;
  for (const group of groups.values()) {
    let written = 0;
    for (const row of group) {
      const vector = vectors.get(row.id);
      if (!vector || opts.signal.aborted) continue;
      // Compare-and-set re-reads identity, exact text, source, signature and
      // eligibility under the reacquired lock. Never upsert old chunk text or
      // overwrite another embedder's vector/metadata on a changed page.
      const updated = await engine.executeRaw<{ id: number; embedded_at: string }>(
        `UPDATE content_chunks cc SET embedding = $1::vector, model = $2, embedded_at = now()
         FROM pages p WHERE p.id = cc.page_id AND cc.id = $3 AND cc.page_id = $4
           AND cc.chunk_index = $5 AND cc.chunk_text = $6 AND cc.chunk_source = $7
           AND cc.embedding IS NULL AND p.embedding_signature IS NOT DISTINCT FROM $8::text
           AND p.deleted_at IS NULL AND ${EMBED_SKIP_FILTER_FRAGMENT}
         RETURNING cc.id, cc.embedded_at::text AS embedded_at`,
        ['[' + Array.from(vector).join(',') + ']', opts.model, row.id, row.page_id,
          row.chunk_index, row.chunk_text, row.chunk_source, row.embedding_signature],
      );
      written += updated.length;
      for (const receipt of updated) {
        let pageReceipts = opts.receipts.get(row.page_id);
        if (!pageReceipts) opts.receipts.set(row.page_id, pageReceipts = new Map());
        pageReceipts.set(row.id, { ...receipt, chunk_text: row.chunk_text, chunk_source: row.chunk_source });
      }
    }
    embedded += written;
    if (written > 0) {
      // Keep exact write receipts across split pages. A later editor/embedder
      // changes text, identity, or embedded_at and invalidates that receipt.
      const receipts = [...opts.receipts.get(group[0].page_id)!.values()];
      const stamped = await engine.executeRaw<{ id: number }>(
        `UPDATE pages p SET embedding_signature = $1 WHERE p.id = $2
         AND p.embedding_signature IS NOT DISTINCT FROM $3::text
         AND p.deleted_at IS NULL AND ${EMBED_SKIP_FILTER_FRAGMENT}
         AND NOT EXISTS (
           SELECT 1 FROM content_chunks cc WHERE cc.page_id = p.id AND NOT EXISTS (
             SELECT 1 FROM jsonb_to_recordset($4::jsonb)
               AS r(id int, embedded_at text, chunk_text text, chunk_source text)
             WHERE r.id = cc.id AND r.embedded_at = cc.embedded_at::text
               AND r.chunk_text = cc.chunk_text AND r.chunk_source = cc.chunk_source
               AND cc.embedding IS NOT NULL))
         RETURNING p.id`,
        [opts.signature, group[0].page_id, group[0].embedding_signature, JSON.stringify(receipts)],
      );
      if (stamped.length) opts.receipts.delete(group[0].page_id);
    }
  }
  return { embedded, skipped: rows.length - embedded, failures };
}
