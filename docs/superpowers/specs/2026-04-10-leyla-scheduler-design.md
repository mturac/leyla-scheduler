# Leyla Scheduler — Design Document

> **Leyla: Claude Code session kapaninca bile isi unutmayan scheduler.**

## Overview

Rust ile yazilmis, Claude Code CLI icin session-aware, resumable, durable scheduler plugin. Marketplace'de ucretsiz, GitHub'da opensource.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Daemon model | Hybrid — on-demand spawn + recovery | No setup friction, self-healing |
| Crate structure | Workspace with 3 crates | Professional separation, not overkill |
| Storage | SQLite + in-memory (trait-based) | Zero config for users, fast tests |
| Distribution | Standalone CLI + Claude Code plugin wrapper | Marketplace-native AND independent use |
| License | MIT + Apache 2.0 dual | Rust ecosystem standard |
| Build order | Engine-first (A) | Solid core before user-facing layers |

## Architecture

```
leyla/
├── Cargo.toml              (workspace root)
├── LICENSE-MIT
├── LICENSE-APACHE
├── README.md
├── crates/
│   ├── leyla-core/         (engine, store trait, SQLite, lifecycle, policies)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── engine.rs          (LeylaEngine — start/stop/schedule/trigger)
│   │   │   ├── store/
│   │   │   │   ├── mod.rs         (LeylaStore trait)
│   │   │   │   ├── sqlite.rs
│   │   │   │   └── memory.rs
│   │   │   ├── scheduler/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── due.rs         (DueRunMaterializer)
│   │   │   │   ├── claim.rs       (LeaseManager)
│   │   │   │   └── recovery.rs    (RecoveryManager)
│   │   │   ├── lifecycle.rs       (state machine)
│   │   │   ├── policies/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── retry.rs
│   │   │   │   ├── misfire.rs
│   │   │   │   └── concurrency.rs
│   │   │   ├── clock.rs           (Clock trait + FakeClock)
│   │   │   └── types.rs           (domain types)
│   │   └── Cargo.toml
│   ├── leyla-executors/    (executor trait + implementations)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── executor.rs        (LeylaExecutor trait)
│   │   │   ├── shell.rs
│   │   │   ├── local_handler.rs
│   │   │   └── claude_task.rs
│   │   └── Cargo.toml
│   └── leyla-cli/          (binary crate)
│       ├── src/
│       │   ├── main.rs
│       │   ├── commands/
│       │   │   ├── mod.rs
│       │   │   ├── job.rs
│       │   │   ├── run.rs
│       │   │   ├── doctor.rs
│       │   │   ├── recover.rs
│       │   │   └── daemon.rs
│       │   └── output.rs
│       └── Cargo.toml
└── docs/
```

## Core Data Model

### LeylaJob

```rust
pub struct LeylaJob {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub schedule: Schedule,
    pub executor: ExecutorSpec,
    pub concurrency: ConcurrencyPolicy,
    pub retry: RetryPolicy,
    pub timeout_ms: u64,
    pub misfire: MisfirePolicy,
    pub tags: Vec<String>,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: u64,
}
```

### Schedule

```rust
pub enum Schedule {
    Cron { expression: String, timezone: Option<String> },
    Interval { every_ms: u64, anchor: IntervalAnchor },
    Once { at: DateTime<Utc> },
    Manual,
}
```

### RunStatus (State Machine)

```rust
pub enum RunStatus {
    Scheduled,
    Leased,
    Dispatched,
    Running,
    Succeeded,
    Failed,
    RetryWait,
    DeadLetter,
    Cancelled,
    TimedOut,
    Orphaned,
}
```

Transitions:
```
scheduled → leased → dispatched → running → succeeded
                                          → failed → retry_wait → scheduled
                                          → timed_out
                                          → orphaned
                                          → dead_letter
```

### JobRun

```rust
pub struct JobRun {
    pub run_id: String,
    pub job_id: String,
    pub scheduled_for: DateTime<Utc>,
    pub status: RunStatus,
    pub attempt: u32,
    pub max_attempts: u32,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub input_json: Option<serde_json::Value>,
    pub output_json: Option<serde_json::Value>,
    pub error_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

### Policies

```rust
pub enum ConcurrencyPolicy { Allow, ForbidOverlap, QueueOne, ReplaceRunning }
pub enum MisfirePolicy { RunImmediately, Skip, Coalesce, ReplayAll { max_catchup: Option<u32> } }
pub enum RetryStrategy { Fixed, Linear, Exponential }

pub struct RetryPolicy {
    pub max_attempts: u32,
    pub strategy: RetryStrategy,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter: bool,
}
```

### ExecutorSpec

```rust
pub enum ExecutorSpec {
    Shell { command: String, args: Vec<String> },
    LocalHandler { handler_key: String },
    ClaudeTask { task_type: String, payload_template: Option<serde_json::Value> },
    Webhook { url: String, method: String, headers: HashMap<String, String> },
}
```

## Store Trait

```rust
#[async_trait]
pub trait LeylaStore: Send + Sync {
    async fn upsert_job(&self, job: &LeylaJob) -> Result<()>;
    async fn get_job(&self, job_id: &str) -> Result<Option<LeylaJob>>;
    async fn list_jobs(&self) -> Result<Vec<LeylaJob>>;
    async fn delete_job(&self, job_id: &str) -> Result<()>;

    async fn insert_run(&self, run: &JobRun) -> Result<()>;
    async fn claim_due_runs(&self, now: DateTime<Utc>, owner: &str, limit: u32) -> Result<Vec<JobRun>>;
    async fn update_run(&self, run_id: &str, patch: RunPatch) -> Result<()>;
    async fn get_run(&self, run_id: &str) -> Result<Option<JobRun>>;
    async fn list_runs(&self, filter: &RunFilter) -> Result<Vec<JobRun>>;

    async fn get_stale_runs(&self, now: DateTime<Utc>, stale_threshold: Duration) -> Result<Vec<JobRun>>;
    async fn get_dead_letters(&self) -> Result<Vec<JobRun>>;

    async fn migrate(&self) -> Result<()>;
}
```

## SQLite Schema

```sql
CREATE TABLE jobs (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1,
    definition_json TEXT NOT NULL,
    next_run_at     TEXT,
    last_run_at     TEXT,
    last_success_at TEXT,
    last_failure_at TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE job_runs (
    run_id           TEXT PRIMARY KEY,
    job_id           TEXT NOT NULL REFERENCES jobs(id),
    scheduled_for    TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'scheduled',
    attempt          INTEGER NOT NULL DEFAULT 1,
    max_attempts     INTEGER NOT NULL DEFAULT 5,
    lease_owner      TEXT,
    lease_expires_at TEXT,
    dispatched_at    TEXT,
    started_at       TEXT,
    heartbeat_at     TEXT,
    finished_at      TEXT,
    input_json       TEXT,
    output_json      TEXT,
    error_json       TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE INDEX idx_runs_due ON job_runs(status, scheduled_for)
    WHERE status = 'scheduled';
CREATE INDEX idx_runs_stale ON job_runs(status, lease_expires_at)
    WHERE status IN ('leased', 'running');
CREATE INDEX idx_runs_job ON job_runs(job_id, created_at);

CREATE TABLE dead_letters (
    id         TEXT PRIMARY KEY,
    run_id     TEXT NOT NULL REFERENCES job_runs(run_id),
    job_id     TEXT NOT NULL,
    input_json TEXT,
    error_json TEXT,
    attempts   INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
```

`claim_due_runs`: `UPDATE job_runs SET status='leased', lease_owner=?, lease_expires_at=? WHERE status='scheduled' AND scheduled_for <= ? LIMIT ? RETURNING *`

## Engine & Scheduler Loops

### LeylaEngine

```rust
pub struct LeylaEngine {
    store: Arc<dyn LeylaStore>,
    executors: HashMap<String, Arc<dyn LeylaExecutor>>,
    clock: Arc<dyn Clock>,
    instance_id: String,
    config: EngineConfig,
    shutdown: CancellationToken,
}

pub struct EngineConfig {
    pub poll_interval_ms: u64,        // default: 1000
    pub claim_batch_size: u32,        // default: 50
    pub lease_ttl_ms: u64,            // default: 30_000
    pub default_timeout_ms: u64,      // default: 300_000
    pub stale_heartbeat_ms: u64,      // default: 60_000
    pub max_output_bytes: usize,      // default: 65_536
}
```

### Three Loops

**Loop A — Due Generation** (every poll_interval)
1. Scan jobs table (enabled + next_run_at <= now)
2. Create JobRun { status: Scheduled } for each
3. Advance job.next_run_at to next schedule

**Loop B — Claim & Dispatch** (every poll_interval)
1. `claim_due_runs(now, instance_id, batch_size)`
2. For each claimed run: concurrency policy check → executor.dispatch(run) → status update
3. Success → Succeeded, error → evaluate retry policy

**Loop C — Recovery** (every 5 * poll_interval)
1. Find stale leases (leased/running + lease_expires_at < now)
2. Heartbeat check
3. Orphaned → retry_wait or dead_letter
4. retry_wait with elapsed delay → back to scheduled

Graceful shutdown via `CancellationToken`.

## Executor Trait

```rust
#[async_trait]
pub trait LeylaExecutor: Send + Sync {
    fn executor_type(&self) -> &str;
    async fn dispatch(&self, ctx: RunContext) -> Result<DispatchResult>;
    async fn heartbeat(&self, run_id: &str) -> Result<()> { Ok(()) }
    async fn cancel(&self, run_id: &str) -> Result<()> { Ok(()) }
}

pub struct RunContext {
    pub run_id: String,
    pub job_id: String,
    pub attempt: u32,
    pub input: Option<serde_json::Value>,
    pub timeout_ms: u64,
}

pub struct DispatchResult {
    pub status: DispatchStatus,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub duration_ms: u64,
}

pub enum DispatchStatus {
    Succeeded,
    Failed { retryable: bool },
    TimedOut,
}
```

### V1 Executors

- **ShellExecutor** — `tokio::process::Command`, stdout/stderr capture, exit code mapping, output truncation, timeout wrap
- **LocalHandlerExecutor** — `HashMap<String, HandlerFn>` registry
- **ClaudeTaskExecutor** — `claude -p "<prompt>"` via TaskSnapshot → prompt builder

## CLI Commands

```
leyla daemon start|stop|status
leyla job add|list|inspect|pause|resume|delete <id>
leyla run trigger|inspect|retry|cancel|list
leyla doctor                    # health check report
leyla recover                   # force recovery loop
```

Output: `tabled` crate for human-readable, `--json` flag for machine-readable.

Storage: `~/.leyla/leyla.db` (SQLite), `~/.leyla/leyla.pid` (daemon PID).

## Daemon Model (Hybrid)

- First `/leyla` invocation spawns lightweight background process
- Writes PID to `~/.leyla/leyla.pid`
- If daemon dies, next `/leyla` call detects stale PID and respawns
- `leyla doctor` reports daemon health
- `leyla daemon stop` sends graceful shutdown signal

## Defaults

```rust
EngineConfig {
    poll_interval_ms: 1000,
    claim_batch_size: 50,
    lease_ttl_ms: 30_000,
    default_timeout_ms: 300_000,
    stale_heartbeat_ms: 60_000,
    max_output_bytes: 65_536,
}

// Default misfire: Coalesce
// Default concurrency: ForbidOverlap
// Default retry: exponential, 5 attempts, 1s base, 60s max, jitter on
```

## Tech Stack

- **Language:** Rust
- **Async:** tokio
- **CLI:** clap
- **SQLite:** rusqlite (with bundled feature)
- **Cron:** cron crate
- **Time:** chrono
- **Serialization:** serde + serde_json
- **Logging:** tracing
- **Tables:** tabled
- **UUID:** uuid

## Code Personality: Leyla Song Lyrics

Error handling ve failure code path'lerinde Leyla şarkısından Türkçe alıntılar easter egg olarak eklenir:

| Scenario | Comment |
|----------|---------|
| Not found | `// arayıp gerçeği bulamadın mı? sende mi leyla` |
| Recovery | `// zaman zaman beni düşünüp ağlıyormuşsun leyla` |
| Timeout | `// ne kadar beklesem de gelmedin leyla` |
| Dead letter | `// artık çok geç leyla` |
| Retry | `// bir kere daha dene leyla` |
| Orphaned run | `// sahipsiz kaldın mı leyla` |
| Stale lease | `// eskidi bu sevda leyla` |
| Graceful shutdown | `// hoşçakal leyla` |

## Build Phases

1. **leyla-core** — store trait, SQLite/memory impl, types, lifecycle state machine, policies, engine with 3 loops, clock abstraction
2. **leyla-executors** — executor trait, ShellExecutor, LocalHandlerExecutor, ClaudeTaskExecutor
3. **leyla-cli** — clap commands, daemon management, doctor, formatted output
4. **Claude Code plugin** — MCP server wrapper, `/leyla` skill definition
