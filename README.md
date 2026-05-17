# Leyla

> _"Zaman zaman beni dusunup agliyormussun Leyla"_

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)]()
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)]()
[![Tests](https://img.shields.io/badge/tests-75%20passing-green.svg)]()

Leyla is a durable, session-aware task scheduler and resumable job orchestrator built in Rust for Claude Code workflows. It survives Claude Code session disconnects, self-heals stale leases, and retries failed jobs — so the work you scheduled keeps running even when your session doesn't.

**Key Features**

- **Session-resilient** — jobs survive Claude Code disconnects and daemon restarts
- **Durable state** — all job and run state persisted to SQLite (`~/.leyla/leyla.db`)
- **Three-loop engine** — dedicated loops for due-generation, claim/dispatch, and recovery
- **Rich policies** — retry (Fixed/Linear/Exponential), misfire (RunImmediately/Skip/Coalesce/ReplayAll), concurrency (Allow/ForbidOverlap/QueueOne/ReplaceRunning)
- **Multiple executors** — Shell, LocalHandler, ClaudeTask
- **Claude Code plugin** — native `/leyla` commands inside your Claude Code session
- **Zero config** — single SQLite file, no external dependencies

---

## Table of Contents

- [Why Leyla?](#why-leyla)
- [Comparison](#comparison-vs-alternatives)
- [Architecture](#architecture)
- [Run Status State Machine](#run-status-state-machine)
- [Execution Flow — The Three Loops](#execution-flow--the-three-loops)
- [Install](#install)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
- [Schedule Types](#schedule-types)
- [Policies](#policies)
- [Recovery and Session Gap](#recovery-and-session-gap)
- [Configuration Defaults](#configuration-defaults)
- [Crate Structure](#crate-structure)
- [Tech Stack](#tech-stack)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [FAQ](#faq)
- [Thanks](#thanks)
- [License](#license)

---

## Why Leyla?

<!-- sen gidince her sey durdu leyla -->

Claude Code sessions are ephemeral. When a session ends — whether due to a timeout, a network drop, or a deliberate `/exit` — everything that lived only in memory disappears with it. Most schedulers are in-memory by design. They exist for the duration of the process that created them, and when that process dies, so do all scheduled jobs.

This is a fundamental mismatch with the way Claude Code workflows operate.

**Before Leyla:**

```
Session starts -> Schedule cron job -> Session dies -> Job lost forever
```

You come back the next morning. Nothing ran. No output. No error. Just silence.

**After Leyla:**

```
Session starts -> /leyla schedule -> Session dies -> Daemon keeps running
    -> Job executes on time -> Results waiting when you return
```

Leyla separates the concern of _scheduling_ from the concern of _session_. The daemon process is independent of any Claude Code session. It outlives sessions, recovers from machine sleep, handles missed fires when the machine was offline, and re-queues orphaned runs automatically. You interact with it through a CLI or through the Claude Code plugin, but it does not depend on either to keep running.

The result: scheduled work happens reliably, regardless of what your session does.

### What Leyla Survives

```
┌─────────────────┬─────────────────┬─────────────────┐
│  Session Close   │  Process Crash  │  Machine Sleep  │
│  ✅ Jobs persist │  ✅ Lease expiry │  ✅ Misfire      │
│  ✅ Daemon lives │  ✅ Auto-recover │  ✅ Coalesce     │
└─────────────────┴─────────────────┴─────────────────┘
```

---

## Comparison vs Alternatives

| Feature               | node-cron | agenda       | bull       | Quartz    | **Leyla**              |
| --------------------- | --------- | ------------ | ---------- | --------- | ---------------------- |
| Session-resilient     | No        | No           | Partial    | Yes       | **Yes**                |
| Zero config           | Yes       | No (MongoDB) | No (Redis) | No (JDBC) | **Yes (SQLite)**       |
| Single binary         | No        | No           | No         | No        | **Yes (Rust)**         |
| Lease-based claiming  | No        | No           | Yes        | Yes       | **Yes**                |
| Dead letter queue     | No        | No           | Yes        | No        | **Yes**                |
| Misfire policies      | No        | Partial      | No         | Yes       | **Yes (4 modes)**      |
| Concurrency policies  | No        | No           | Partial    | No        | **Yes (4 modes)**      |
| Retry with backoff    | No        | Partial      | Yes        | No        | **Yes (3 strategies)** |
| Claude Code native    | No        | No           | No         | No        | **Yes**                |
| Deterministic testing | No        | No           | No         | No        | **Yes (FakeClock)**    |

---

## Architecture

```
┌─────────────────────────────────────────┐
│     Claude Code Plugin (/leyla …)       │
│         MCP server wrapper              │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│         Leyla CLI  (leyla-cli)          │
│  daemon · job · run · doctor · recover  │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│        LeylaEngine  (leyla-core)        │
│                                         │
│  ┌─────────────┐  ┌──────────────────┐  │
│  │  Loop A     │  │  Loop B          │  │
│  │  Due Gen    │  │  Claim & Dispatch│  │
│  └─────────────┘  └──────────────────┘  │
│           ┌──────────────────┐          │
│           │  Loop C          │          │
│           │  Recovery        │          │
│           └──────────────────┘          │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│       SQLite Store  (leyla-core)        │
│  jobs · job_runs · dead_letters tables  │
│  ~/.leyla/leyla.db                      │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│    Execution Adapters  (leyla-executors)│
│  ShellExecutor · LocalHandlerExecutor  │
│  ClaudeTaskExecutor                     │
└─────────────────────────────────────────┘
```

### How It Works

When you register a job, Leyla stores its definition in SQLite. The daemon runs three concurrent async loops: Loop A scans for jobs whose `next_run_at` has passed and materializes `JobRun` rows; Loop B atomically claims those rows using a `UPDATE … RETURNING` lease and dispatches them to the appropriate executor; Loop C periodically scans for leased or running jobs whose lease has expired (orphaned runs) and re-queues or dead-letters them according to the retry budget. Because all state lives in SQLite, every loop can be interrupted and resumed safely — the database is the source of truth, not RAM.

---

## Run Status State Machine

<!-- ne kadar beklesem de gelmedin — timeout state lives here -->

```
                     ┌───────────┐
              ┌─────►│ scheduled │◄────────────────────┐
              │      └─────┬─────┘                     │
              │            │ claim_due_runs             │
              │      ┌─────▼─────┐                     │
              │      │  leased   │                     │
              │      └─────┬─────┘                     │
              │            │ executor.dispatch          │
              │      ┌─────▼──────┐                    │
              │      │ dispatched │                    │
              │      └─────┬──────┘                    │
              │            │ heartbeat starts           │
              │      ┌─────▼─────┐                     │
              │      │  running  │                     │
              │      └──┬─┬──┬───┘                     │
              │         │ │  │                          │
              │         │ │  └──── succeeded ──────────►│(terminal — job
              │         │ │                             │ advances next_run_at)
              │         │ └─────── timed_out ─────────►│
              │         │              │                │  ┌────────────┐
              │         │              └────────────────┼─►│ dead_letter│(terminal)
              │         │                               │  └────────────┘
              │         └───────── failed ──────────────┤
              │                        │                │
              │               ┌────────▼──────────┐    │
              └───────────────┤    retry_wait      │    │
                              └────────┬──────────┘    │
                 max_attempts          │ delay elapsed  │
                 exceeded              └────────────────┘
                     │
                     ▼
              ┌────────────┐
              │ dead_letter│  (terminal)
              └────────────┘

  orphaned ──────────────────────────────► retry_wait or dead_letter
  cancelled ─────────────────────────────► (terminal)
```

**State definitions**

| State         | Meaning                                                       |
| ------------- | ------------------------------------------------------------- |
| `scheduled`   | Run materialized, waiting for a worker to claim it            |
| `leased`      | Atomically claimed by a worker instance, not yet dispatched   |
| `dispatched`  | Handed to executor, awaiting first heartbeat                  |
| `running`     | Executor confirmed start, heartbeating                        |
| `succeeded`   | Executor returned success                                     |
| `failed`      | Executor returned failure; retry policy evaluated             |
| `retry_wait`  | Waiting for backoff delay before re-scheduling                |
| `timed_out`   | No completion before `timeout_ms` elapsed                     |
| `orphaned`    | Lease expired with no heartbeat — session likely disconnected |
| `dead_letter` | Max attempts exhausted or unretryable — terminal              |
| `cancelled`   | Cancelled via `leyla run cancel` — terminal                   |

---

## Execution Flow — The Three Loops

<!-- zaman zaman beni dusunup agliyormussun — the engine never forgets -->

### Loop A — Due Generation _(every `poll_interval_ms`)_

```
1. Scan jobs WHERE enabled = true AND next_run_at <= now
2. For each job:
   a. INSERT JobRun { status: Scheduled, scheduled_for: next_run_at }
   b. Advance job.next_run_at to next occurrence (cron/interval)
      Once jobs: disable after materializing
      Manual jobs: skip (never auto-materialize)
```

### Loop B — Claim & Dispatch _(every `poll_interval_ms`)_

```
1. claim_due_runs(now, instance_id, batch_size)
   → Atomic UPDATE … RETURNING (prevents double-dispatch)
2. For each claimed run:
   a. Evaluate ConcurrencyPolicy
      ForbidOverlap  → skip if another run is active for this job
      QueueOne       → allow at most one queued + one running
      ReplaceRunning → cancel running, dispatch new
      Allow          → dispatch unconditionally
   b. executor.dispatch(RunContext) → DispatchResult
   c. Succeeded  → status = Succeeded, advance next_run_at
      Failed      → evaluate RetryPolicy → retry_wait or dead_letter
      TimedOut    → status = TimedOut   → retry_wait or dead_letter
```

### Loop C — Recovery _(every `5 × poll_interval_ms`)_

```
1. Find stale runs:
   status IN ('leased','running') AND lease_expires_at < now
                                        ──────────────────────
                             // sahipsiz kaldın mı leyla — orphan check
2. For each stale run:
   a. No recent heartbeat → mark Orphaned
   b. Evaluate retry budget:
      attempts < max_attempts → RetryWait (exponential backoff)
      attempts >= max_attempts → DeadLetter
3. RetryWait runs with elapsed delay → back to Scheduled
```

---

## Install

```bash
cargo install --path crates/leyla-cli
```

Or once published to crates.io:

```bash
cargo install leyla-cli
```

Leyla stores all data in `~/.leyla/`:

```
~/.leyla/
├── leyla.db      # SQLite — all jobs, runs, dead letters
└── leyla.pid     # daemon process ID
```

---

## Quick Start

Follow these steps to go from zero to a running scheduled job in under two minutes.

**Step 1 — Start the daemon**

```bash
leyla daemon start
```

The daemon spawns in the background and writes its PID to `~/.leyla/leyla.pid`. It will survive your session.

**Step 2 — Register a recurring cron job**

```bash
leyla job add \
  --id daily-report \
  --name "Daily Report" \
  --cron "0 9 * * *" \
  --timezone "Europe/Istanbul" \
  --command node \
  --args scripts/report.js
```

**Step 3 — Register a one-shot job**

```bash
leyla job add \
  --id db-migrate \
  --name "Database Migration" \
  --at "2026-04-11T09:00:00Z" \
  --command cargo \
  --args "run --bin migrate"
```

**Step 4 — Register an interval job (every 30 minutes)**

```bash
leyla job add \
  --id health-ping \
  --name "Health Ping" \
  --interval 1800000 \
  --command curl \
  --args "-sf https://example.com/health"
```

**Step 5 — Trigger a job immediately (bypass the schedule)**

```bash
leyla run trigger daily-report
```

**Step 6 — Watch job status**

```bash
leyla job list
```

**Step 7 — Inspect a specific run**

```bash
leyla run list --job daily-report
leyla run inspect <run_id>
```

**Step 8 — Pause and resume a job**

```bash
leyla job pause daily-report
leyla job resume daily-report
```

**Step 9 — Run a health check**

```bash
leyla doctor
```

**Step 10 — Force recovery after a hard disconnect**

```bash
leyla recover
```

---

## CLI Reference

### Daemon Management

| Command               | Description                                          |
| --------------------- | ---------------------------------------------------- |
| `leyla daemon start`  | Spawn the background daemon (hybrid on-demand spawn) |
| `leyla daemon stop`   | Send graceful shutdown signal to the running daemon  |
| `leyla daemon status` | Show daemon PID, uptime, and health summary          |

### Job Management

| Command                  | Description                                      |
| ------------------------ | ------------------------------------------------ |
| `leyla job add [flags]`  | Register a new scheduled job                     |
| `leyla job list`         | List all jobs with status, next run, last result |
| `leyla job inspect <id>` | Full job definition including policies           |
| `leyla job pause <id>`   | Disable job (stops new runs from materializing)  |
| `leyla job resume <id>`  | Re-enable a paused job                           |
| `leyla job remove <id>`  | Delete job and all associated runs               |

**`leyla job add` flags**

| Flag             | Type     | Description                                         |
| ---------------- | -------- | --------------------------------------------------- |
| `--id`           | string   | Unique job identifier                               |
| `--name`         | string   | Human-readable name                                 |
| `--cron`         | string   | Cron expression (5-field)                           |
| `--interval`     | u64      | Repeat interval in milliseconds                     |
| `--at`           | RFC3339  | One-shot scheduled time                             |
| `--timezone`     | string   | IANA timezone (default UTC)                         |
| `--command`      | string   | Executable to run                                   |
| `--args`         | string   | Arguments string                                    |
| `--timeout`      | u64      | Timeout in milliseconds                             |
| `--max-attempts` | u32      | Retry budget (default 5)                            |
| `--concurrency`  | string   | `allow`, `forbid`, `queue_one`, `replace`           |
| `--misfire`      | string   | `run_immediately`, `skip`, `coalesce`, `replay_all` |
| `--tag`          | string[] | Tags for grouping                                   |
| `--json`         | flag     | Output machine-readable JSON                        |

### Run Management

| Command                      | Description                                        |
| ---------------------------- | -------------------------------------------------- |
| `leyla run trigger <job_id>` | Materialize a run immediately (ignores schedule)   |
| `leyla run inspect <run_id>` | Full run details: status, timing, output, error    |
| `leyla run retry <run_id>`   | Re-queue a failed or dead-letter run               |
| `leyla run cancel <run_id>`  | Cancel a scheduled or running run                  |
| `leyla run list`             | List recent runs (filter with `--job`, `--status`) |

### Diagnostics

| Command         | Description                                                 |
| --------------- | ----------------------------------------------------------- |
| `leyla doctor`  | Full health report: daemon, DB, stale runs, dead letters    |
| `leyla recover` | Force Loop C to run immediately — reschedules orphaned runs |

All commands support `--json` for machine-readable output (powered by the `tabled` crate for human mode).

---

## Schedule Types

<!-- arayıp gerçeği bulamadın mı — Manual means you decide when -->

### Cron

Standard 5-field cron expression with optional IANA timezone:

```bash
leyla job add --id nightly-backup \
  --cron "0 2 * * *" \
  --timezone "Europe/Istanbul" \
  --command ./scripts/backup.sh
```

| Field        | Values                 |
| ------------ | ---------------------- |
| Minute       | 0–59                   |
| Hour         | 0–23                   |
| Day of month | 1–31                   |
| Month        | 1–12                   |
| Day of week  | 0–7 (0 and 7 = Sunday) |

### Interval

Repeat every N milliseconds, anchored to first run time:

```bash
leyla job add --id metrics-flush \
  --interval 60000 \
  --command ./flush-metrics
```

### Once

Fire exactly once at a specific UTC timestamp:

```bash
leyla job add --id feature-flag-release \
  --at "2026-05-01T10:00:00Z" \
  --command ./enable-feature.sh
```

The job disables itself after the run materializes.

### Manual

No automatic schedule — only fires when explicitly triggered:

```bash
leyla job add --id adhoc-cleanup \
  --name "Ad-hoc Cleanup" \
  --command ./cleanup.sh
  # no --cron / --interval / --at

leyla run trigger adhoc-cleanup
```

---

## Policies

### Retry Policy

<!-- bir kere daha dene leyla -->

Controls what happens when a run returns `Failed` or `TimedOut`.

| Strategy      | Delay formula                                          | Use case                              |
| ------------- | ------------------------------------------------------ | ------------------------------------- |
| `Fixed`       | Always `base_delay_ms`                                 | Simple rate-limited retries           |
| `Linear`      | `base_delay_ms × attempt`                              | Moderate backoff                      |
| `Exponential` | `base_delay_ms × 2^attempt` (capped at `max_delay_ms`) | Aggressive backoff for flaky services |

**Defaults**

| Setting         | Default          |
| --------------- | ---------------- |
| `max_attempts`  | 5                |
| `strategy`      | `Exponential`    |
| `base_delay_ms` | 1 000 ms (1 s)   |
| `max_delay_ms`  | 60 000 ms (60 s) |
| `jitter`        | `true`           |

When `attempt >= max_attempts`, the run moves to **`dead_letter`** (terminal).

### Misfire Policy

A "misfire" occurs when the daemon was offline during a scheduled window and the job is now overdue.

| Policy                 | Behavior                                                      |
| ---------------------- | ------------------------------------------------------------- |
| `RunImmediately`       | Materialize one run right now for the missed slot             |
| `Skip`                 | Discard all missed slots; wait for the next scheduled time    |
| `Coalesce` _(default)_ | Materialize exactly one catch-up run, merge all missed slots  |
| `ReplayAll`            | Materialize a run for every missed slot (up to `max_catchup`) |

**Default:** `Coalesce`

### Concurrency Policy

Controls behavior when a new run is due while a previous run for the same job is still active.

| Policy                      | Behavior                                                                 |
| --------------------------- | ------------------------------------------------------------------------ |
| `Allow`                     | Dispatch without checking — multiple parallel runs permitted             |
| `ForbidOverlap` _(default)_ | Skip the new run if any run is currently `leased`/`dispatched`/`running` |
| `QueueOne`                  | Allow at most one pending + one active; additional triggers are dropped  |
| `ReplaceRunning`            | Cancel the active run and dispatch the new one                           |

**Default:** `ForbidOverlap`

---

## Recovery and Session Gap

<!-- zaman zaman beni dusunup agliyormussun — we remember even when disconnected -->

Leyla is designed around the reality of Claude Code sessions: they disconnect, machines sleep, and networks fail. Here is how Leyla handles each scenario:

### Orphaned Runs (session disconnect mid-execution)

When the Claude Code session or daemon process dies mid-run:

1. The run holds a lease with `lease_expires_at` set to `now + lease_ttl_ms` (default 30 s)
2. The executor is expected to emit heartbeats within the TTL window
3. Loop C (Recovery) scans for `leased` or `running` runs where `lease_expires_at < now`
4. These are marked `Orphaned`
5. Retry policy is evaluated — budget remaining → `RetryWait` → re-`Scheduled`

### Session Gap (daemon was offline for extended period)

When the daemon restarts after being offline:

1. Loop A immediately scans for overdue jobs (`next_run_at <= now`)
2. MisfirePolicy determines how many catch-up runs to materialize
3. `Coalesce` (default) creates exactly one run — the most recent missed slot
4. `ReplayAll` creates runs for every slot missed during the gap (up to `max_catchup`)

### Daemon Respawn

The daemon PID is stored at `~/.leyla/leyla.pid`. Any `/leyla` CLI invocation:

1. Checks if the PID file exists and the process is alive
2. If the PID is stale (process dead), spawns a new daemon
3. New daemon runs recovery immediately on startup

Run `leyla doctor` at any time to see stale runs, orphaned leases, and dead letter queue depth.

---

## Configuration Defaults

All tunables live in `EngineConfig`:

| Setting              | Default   | Description                              |
| -------------------- | --------- | ---------------------------------------- |
| `poll_interval_ms`   | `1 000`   | How often Loop A and B run               |
| `claim_batch_size`   | `50`      | Max runs claimed per Loop B tick         |
| `lease_ttl_ms`       | `30 000`  | Lease expiry window (30 s)               |
| `default_timeout_ms` | `300 000` | Per-run timeout if not specified (5 min) |
| `stale_heartbeat_ms` | `60 000`  | Heartbeat silence threshold for Loop C   |
| `max_output_bytes`   | `65 536`  | Output capture cap per run (64 KiB)      |

Loop C runs every `5 × poll_interval_ms` (default 5 s).

---

## Crate Structure

```
leyla/
├── Cargo.toml              # workspace root
├── crates/
│   ├── leyla-core/         # engine, store, lifecycle, policies
│   │   └── src/
│   │       ├── engine.rs          # LeylaEngine — start/stop/trigger
│   │       ├── store/             # LeylaStore trait, SQLite, in-memory
│   │       ├── scheduler/         # due.rs, claim.rs, recovery.rs
│   │       ├── lifecycle.rs       # state machine
│   │       ├── policies/          # retry, misfire, concurrency
│   │       ├── clock.rs           # Clock trait + FakeClock (testing)
│   │       └── types.rs           # all domain types
│   │
│   ├── leyla-executors/    # executor trait + implementations
│   │   └── src/
│   │       ├── executor.rs        # LeylaExecutor trait
│   │       ├── shell.rs           # tokio::process::Command wrapper
│   │       ├── local_handler.rs   # in-process handler registry
│   │       └── claude_task.rs     # claude -p executor
│   │
│   └── leyla-cli/          # binary crate
│       └── src/
│           ├── main.rs
│           ├── commands/          # daemon, job, run, doctor, recover
│           └── output.rs          # tabled + --json formatting
└── docs/
```

**`leyla-core`** contains the engine, store trait and both SQLite and in-memory implementations, the lifecycle state machine, all three scheduler loops, and all policy logic. It has no I/O dependency outside tokio and rusqlite — easily testable with `FakeClock` and the memory store.

**`leyla-executors`** is a thin adapter crate. `ShellExecutor` wraps `tokio::process::Command` with stdout/stderr capture, exit-code mapping, output truncation, and timeout enforcement. `LocalHandlerExecutor` holds a `HashMap<String, HandlerFn>` for in-process logic. `ClaudeTaskExecutor` calls `claude -p "<prompt>"`.

**`leyla-cli`** is the binary. It wires the engine and executors together, manages the hybrid daemon lifecycle (PID file + respawn), and formats output via `tabled` for humans and `serde_json` for machines.

---

## Tech Stack

| Dependency                       | Role                                              |
| -------------------------------- | ------------------------------------------------- |
| `tokio`                          | Async runtime                                     |
| `clap`                           | CLI argument parsing                              |
| `rusqlite` (bundled)             | SQLite storage — zero external deps               |
| `cron`                           | Cron expression parsing and next-time calculation |
| `chrono`                         | Date/time arithmetic                              |
| `serde` + `serde_json`           | Serialization                                     |
| `tracing`                        | Structured logging                                |
| `tabled`                         | Human-readable table output                       |
| `uuid`                           | Run ID generation                                 |
| `tokio-util` (CancellationToken) | Graceful shutdown                                 |

---

## Roadmap

Leyla is developed in four phases. The current release is V1 (Phase 1).

### Phase 1 — Durable Local Scheduler (current — V1)

The foundation: a single-binary daemon with SQLite-backed state, three-loop engine, full policy set, and Claude Code plugin integration. Everything described in this README is Phase 1.

### Phase 2 — Claude-Aware Resumable Execution

Deep integration with Claude Code task lifecycle. Jobs will be able to emit structured checkpoints, resume interrupted Claude tasks mid-stream, and attach outputs back to the originating session when it reconnects.

### Phase 3 — Shared Deployment (Postgres, multi-instance)

Replace the SQLite backend with Postgres for teams and CI environments. Multiple daemon instances coordinate via the same store — lease claiming already handles concurrent workers by design, so the migration is a store-layer swap. Multi-instance heartbeat coordination and distributed dead-letter management.

### Phase 4 — Productization

Web dashboard for job and run visibility, DAG-style job dependencies (run B after A succeeds), role-based permissions, audit log export, and a marketplace-publishable Claude Code plugin package.

---

## Contributing

<!-- birlikte yapalim leyla -->

Contributions are welcome. Leyla follows standard Rust open source conventions.

### Getting Started

1. Fork the repository and clone your fork
2. Create a feature branch: `git checkout -b feat/your-feature-name`
3. Make your changes in the appropriate crate (`leyla-core`, `leyla-executors`, or `leyla-cli`)
4. Run the full test suite: `cargo test --workspace`
5. Run the linter: `cargo clippy --workspace -- -D warnings`
6. Format your code: `cargo fmt --all`
7. Open a pull request against `main` with a clear description of what changed and why

### Guidelines

- Keep `leyla-core` free of CLI concerns. It must remain independently testable.
- New policy variants belong in `leyla-core/src/policies/`.
- New executors belong in `leyla-executors/src/`.
- All public API changes need accompanying tests. Use `FakeClock` and the in-memory store for unit tests — no SQLite required.
- Commit messages should be imperative, present tense: "Add webhook executor" not "Added webhook executor".

### Running Tests

```bash
# All workspace tests
cargo test --workspace

# Specific crate
cargo test -p leyla-core

# With output (useful for debugging test failures)
cargo test --workspace -- --nocapture
```

### Linting and Formatting

```bash
cargo clippy --workspace -- -D warnings
cargo fmt --all
```

If you are adding a new error path in `leyla-core`, consider adding a Leyla lyrics comment. See the design spec for the full mapping.

---

## FAQ

<!-- sorma bana neden boyle leyla -->

**Is Leyla a cron replacement?**

No. Leyla is a durable scheduler with session awareness and rich operational policies. Standard cron has no concept of leases, retries, dead letters, misfire handling, or concurrency policies. Leyla is closer to a lightweight job queue with a scheduling frontend — think Sidekiq or Bull, but without Redis, and with first-class session-gap recovery built in.

**Does it need a database server?**

No. Leyla uses embedded SQLite via `rusqlite` with the bundled feature enabled. There is no external process to configure or connect to. The database is a single file at `~/.leyla/leyla.db`.

**Can I use Leyla without Claude Code?**

Yes. The `leyla-cli` binary is fully standalone. The Claude Code plugin is a thin MCP server wrapper that delegates to the same CLI commands. Everything works independently of Claude Code — you can use Leyla as a general-purpose durable scheduler for any shell commands.

**What happens when my laptop sleeps?**

The daemon process is suspended by the OS when the machine sleeps. When the machine wakes, the daemon resumes and Loop C runs immediately. Any runs that were in `leased` or `running` state with expired leases are marked `Orphaned` and re-queued according to the retry policy. Loop A then handles the misfire gap — by default, `Coalesce` fires one catch-up run per overdue job.

**What is the dead letter queue?**

When a run exhausts its retry budget (`attempt >= max_attempts`), or when a run fails with `retryable: false`, it is written to the `dead_letters` table. Dead-letter runs are terminal — they will not be retried automatically. Use `leyla run retry <run_id>` to manually re-queue a dead-letter run, or `leyla doctor` to inspect the queue depth.

**How do I run a job from inside a Claude Code session?**

Install the Claude Code plugin (MCP server wrapper). Then use `/leyla job add`, `/leyla run trigger`, `/leyla job list`, etc. directly in your session. The plugin delegates to the same daemon and CLI, so behavior is identical.

**Is there a way to pass input data to a job?**

The `ClaudeTaskExecutor` supports a `payload_template` for structured input. For shell jobs, use environment variables or argument interpolation in your `--args` string. Structured input/output JSON is stored per run and visible via `leyla run inspect <run_id>`.

**How do I debug a job that keeps failing?**

1. Run `leyla run list --job <job_id> --status failed` to see recent failures
2. Run `leyla run inspect <run_id>` to read the captured stdout, stderr, and error JSON
3. Run `leyla doctor` to see overall dead letter queue depth and orphan count
4. Check daemon logs (via `tracing` output) for dispatch-level errors

---

## Thanks

<!-- hoşçakal leyla — but not goodbye, just thank you -->

- **Irem Altiok**
- **Elif Miray Turac**
- **Alper Bayrakli**
- **Baran Kandil**
- **MiMo**

---

## License

Licensed under either of:

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.

---

## Part of [mturac/tools](https://github.com/mturac/tools)

This project is part of an open-source toolkit for AI-augmented engineering — Claude Code plugins, MCP servers, security scanners, schedulers, and dev-productivity utilities. See the [hub](https://github.com/mturac/tools) for the full list.

Install every Claude Code plugin from one place:

```text
/plugin marketplace add mturac/claude-plugin-marketplace
/plugin install leyla-scheduler
```

