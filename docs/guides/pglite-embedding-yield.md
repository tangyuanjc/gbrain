# PGLite stale embedding and writer contention

The standalone file-backed `gbrain embed --stale` CLI releases its database
connection while the embedding service is working or backing off. Each batch
contains at most 64 chunks, even with a larger `--batch-size`. `disconnect()`
must finish closing PGLite before the native lock is released. Fast providers
also leave a minimum 500 ms acquisition opportunity for queued writers.

After reopening the same datastore, updates compare chunk ID, page ID, index,
exact text, chunk source, page embedding signature, and deletion/skip state.
Existing vectors are never overwritten. Only vector/model/embedded-at fields
change; unrelated chunk metadata is preserved. Whole-page signatures require
validated write receipts across batches, including exact embedded-at timestamps.
Concurrent edits may leave chunks stale for the next run.

This path is explicitly enabled by the standalone CLI. Shared workers, sync,
MCP, in-memory engines, `--all`, and single-page embedding retain their existing
connection lifecycle. A total embedding time budget still bounds HTTP requests
and rate-limit sleep. It is not a hard deadline for filesystem I/O or database
close; deployment acceptance must measure actual lock intervals.

`scripts/local/memory-ingest.ts` is a versioned copy of the local launchd ingest
runner. Deploy it to the path invoked by that installation's launchd wrapper.
It retries only the specific PGLite acquisition failure, with jitter and a
single per-part wall-clock budget covering all attempts and sleeps. It reports
`lock_timeout_files` separately from actual process `timed_out_files`. Both
also count as `failed_files`, produce `status: failed`, and exit nonzero; failed
file hashes remain eligible for retry. Health checks must reject any positive
failure counter, even if total page count grows. A no-change run does not prove
that an import succeeded.

For rollout validation, record the UTC start and log offsets after smoke tests.
Observe the same complete 24-hour window in both scheduler/ingest logs and
`.gbrain-lock-events.jsonl`. Pair acquisitions with successful releases by
owner token and use `release_started_at` as the interval end. The released
record is appended after native close, so JSONL ordering alone cannot establish
overlap. A live current owner is not a dead unreleased owner. Require no overlaps,
no dead unreleased owners, no close failures, maximum hold strictly below 300
seconds, and no lock-timeout task failures. Include a changed-file ingest run;
boot-time coordination additionally needs evidence from the next natural reboot.

Never delete or replace the permanent sibling `.gbrain-owner.lock` inode,
including during datastore repair. Never kill active writers to shorten a hold.
