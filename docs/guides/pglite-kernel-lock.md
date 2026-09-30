# PGLite kernel lock backport

This fork backports datastore ownership from upstream `v0.59.20.0`
(`a9c46ffbf68447cf0b157cac6843e994e61fc4b8`, originally introduced in
`d13aa742fd68b71bfd6c98be3dda5813791f1d6c`). It does not enable the newer
resident writer, schema migrations, or publication coordinator.

The native addon, source, prebuild manifest and build tooling are vendored
unchanged. The TypeScript adapter remembers native close failures so repeated
cleanup cannot turn an uncertain close into success. The PGLite wrapper keeps
strict legacy metadata validation and the fork's local lock audit.

## Deploy

1. Stop scheduling datastore clients and allow active clients to finish.
2. Confirm that **all older clients, including waiters**, have exited. Old
   clients do not obey the new kernel protocol. Do not mix versions.
3. Update the source and install the pinned dependencies with lifecycle scripts
   disabled. A successful open records the `kernel-v1` migration marker.
4. Verify one successful open/close, then restore the prior schedules.

Ownership uses a canonical, permanent sibling `<dataDir>.gbrain-owner.lock`.
Never unlink or replace this file, including during database replacement,
backup restoration, or repair. Keep datastore path topology stable while open.
`.gbrain-lock/lock` is diagnostic metadata. Removing it cannot release a native
owner. Missing native support fails closed; it never falls back to mkdir.

The initial migration refuses live, malformed or unreadable legacy owners.
Linux also requires matching PID namespace and boot ID before interpreting a
legacy PID as dead. Handle an uncertain legacy record only in a quiescent
maintenance window. The migration marker does not make mixed versions safe.

A failed database close keeps ownership until process exit and refuses reconnect
on that engine. Disconnect waits for pending initialization before closing and
releasing. The database opens the same canonical path whose lock was acquired.

## Audit

`.gbrain-lock-events.jsonl` records `acquired`, `released` and `close_failed`.
Pair records by `owner_token`. A `close_failed` event does not release ownership.
`released` is written only after native close succeeds; its `release_started_at`
marks the handoff boundary. A successor can append before the confirmed release,
so compute intervals using this boundary for successfully paired releases,
instead of treating JSONL line order as an ownership oracle. Unpaired acquisitions
need investigation; a currently live holder is still in progress.

## Reproduce

```sh
bun test test/pglite-lock.test.ts test/pglite-lock-kernel.test.ts
bun test test/pglite-engine-disconnect.serial.test.ts test/pglite-reconnect.serial.test.ts
bun scripts/probes/pglite-lock-stress.ts 100
bun scripts/native/verify.ts
bun scripts/native/compiled-smoke.ts
bun run typecheck
```

The contention probe uses eight fresh processes and a dead legacy owner in each
round. A separate exclusive file in the critical section checks actual overlap;
the audit also checks acquired/released pairs. No probe opens a production DB.
