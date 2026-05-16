# Leyla — review findings tracker

Snapshot of the gemini-cli code-review findings against the v0.1.0 codebase,
recorded so they survive across sessions and so contributors can pick them up
without re-running the review. Each item lists the file, the symptom, the
recommended fix, and the risk level the reviewer assigned.

The actual code changes are intentionally deferred to dedicated PRs — each
finding here touches load-bearing crate internals (engine loop, store query
shape, dispatcher trait) and should land with its own focused test pass.

## High

### H1 — `run trigger` bypasses the engine
- **File:** `crates/leyla-cli/src/main.rs` (the `run trigger` subcommand)
- **Symptom:** the CLI inserts manual runs directly into the store, skipping
  `LeylaEngine::trigger`. The engine therefore cannot apply pre-trigger logic
  (rate caps, dedup) or wake immediately on the new run.
- **Fix:** route the CLI through the same `LeylaEngine` API exposed to the
  daemon. The store insertion should be an implementation detail of the
  engine, not the CLI.

## Medium

### M1 — `DueRunMaterializer` does an O(N) job scan every tick
- **File:** `crates/leyla-core/src/engine/materialize.rs`
- **Symptom:** on every 1s tick the materialiser loads every job and filters
  in Rust. At a few dozen jobs this is fine; at a few thousand it dominates
  the engine's CPU.
- **Fix:** add a `due_jobs(now)` method to `LeylaStore` that issues a
  `SELECT … WHERE next_run_at <= ?1 ORDER BY next_run_at LIMIT N` against an
  index on `next_run_at`. The materialiser becomes a thin wrapper around that
  query.

### M2 — Engine has fixed 1s polling latency
- **File:** `crates/leyla-core/src/engine/mod.rs`
- **Symptom:** even when a manual trigger or a freshly-scheduled job lands,
  the engine sleeps up to 1s before noticing because the tick is a hard
  `tokio::time::interval`.
- **Fix:** wrap the tick in `tokio::select!` with a `tokio::sync::Notify`
  that `LeylaEngine::trigger` and the materialiser call. Keep the 1s tick as
  a backstop.

## Low

### L1 — Flaky `engine_creates_and_stops` test
- **File:** `crates/leyla-core/tests/engine.rs`
- **Symptom:** the test sleeps `50ms` to "wait for the engine to start". CI
  schedulers under load sometimes need more than that.
- **Fix:** poll the engine's `started_at()` (or a one-shot
  `Notify`/`oneshot::Receiver` returned by `start()`) instead of sleeping.

### L2 — Brittle PID handling in the daemon
- **File:** `crates/leyla-cli/src/daemon.rs`
- **Symptom:** the daemon uses `libc::kill(pid, 0)` for liveness checks,
  which does not cross-compile to Windows and is vulnerable to PID reuse.
- **Fix:** persist a per-process boot id (e.g. from `/proc/sys/kernel/random/boot_id`
  or `sysctl kern.boottime`) alongside the PID and verify the boot id when
  re-attaching.

---

_Source: `gemini-cli` review run on 2026-05-17 against the `main` branch._
