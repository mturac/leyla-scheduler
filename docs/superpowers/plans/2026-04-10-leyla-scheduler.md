# Leyla Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Rust-based durable scheduler and resumable task orchestrator for Claude Code CLI — opensource, marketplace-ready, best-practice Rust.

**Architecture:** Workspace with 3 crates: `leyla-core` (engine, store, lifecycle, policies), `leyla-executors` (shell, local handler, claude task), `leyla-cli` (clap binary, daemon, doctor). Engine-first build order. SQLite + in-memory store behind trait. TDD throughout. Leyla song lyrics as easter egg comments in error paths.

**Tech Stack:** Rust, tokio, clap, rusqlite (bundled), chrono, cron, serde, serde_json, tracing, tabled, uuid, async-trait, tokio-util (CancellationToken), thiserror, tempfile (test)

---

## File Structure

```
leyla/
├── Cargo.toml                          # workspace root
├── LICENSE-MIT
├── LICENSE-APACHE
├── README.md
├── .gitignore
├── crates/
│   ├── leyla-core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # public API re-exports
│   │       ├── types.rs                # LeylaJob, JobRun, Schedule, ExecutorSpec, policies
│   │       ├── clock.rs                # Clock trait, SystemClock, FakeClock
│   │       ├── lifecycle.rs            # RunStatus state machine, transition validation
│   │       ├── policies/
│   │       │   ├── mod.rs
│   │       │   ├── retry.rs            # RetryPolicy, backoff calculation
│   │       │   ├── misfire.rs          # MisfirePolicy, due run filtering
│   │       │   └── concurrency.rs      # ConcurrencyPolicy, overlap check
│   │       ├── store/
│   │       │   ├── mod.rs              # LeylaStore trait, RunPatch, RunFilter
│   │       │   ├── sqlite.rs           # SqliteLeylaStore
│   │       │   └── memory.rs           # MemoryLeylaStore (for tests)
│   │       ├── scheduler/
│   │       │   ├── mod.rs
│   │       │   ├── due.rs              # DueRunMaterializer
│   │       │   ├── claim.rs            # LeaseManager (claim + dispatch coordination)
│   │       │   └── recovery.rs         # RecoveryManager (stale, orphan, retry advance)
│   │       └── engine.rs               # LeylaEngine, EngineConfig, 3-loop orchestration
│   ├── leyla-executors/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # re-exports
│   │       ├── executor.rs             # LeylaExecutor trait, RunContext, DispatchResult
│   │       ├── shell.rs                # ShellExecutor
│   │       ├── local_handler.rs        # LocalHandlerExecutor
│   │       └── claude_task.rs          # ClaudeTaskExecutor
│   └── leyla-cli/
│       ├── Cargo.toml
│       └── src/
│           ├── main.rs                 # clap App, subcommand dispatch
│           ├── commands/
│           │   ├── mod.rs
│           │   ├── job.rs              # add/list/inspect/pause/resume/remove
│           │   ├── run.rs              # trigger/inspect/retry/cancel/list
│           │   ├── doctor.rs           # health check
│           │   ├── recover.rs          # force recovery
│           │   └── daemon.rs           # start/stop/status
│           └── output.rs              # table formatting, --json support
└── docs/
```

---

### Task 1: Workspace & Crate Scaffolding

**Files:**
- Create: `Cargo.toml`
- Create: `crates/leyla-core/Cargo.toml`
- Create: `crates/leyla-core/src/lib.rs`
- Create: `crates/leyla-executors/Cargo.toml`
- Create: `crates/leyla-executors/src/lib.rs`
- Create: `crates/leyla-cli/Cargo.toml`
- Create: `crates/leyla-cli/src/main.rs`
- Create: `.gitignore`
- Create: `LICENSE-MIT`
- Create: `LICENSE-APACHE`

- [ ] **Step 1: Initialize git repo**

```bash
cd /Users/mehmet.turac/Documents/leylaTheScheduler
git init
```

- [ ] **Step 2: Create workspace root Cargo.toml**

```toml
[workspace]
resolver = "2"
members = [
    "crates/leyla-core",
    "crates/leyla-executors",
    "crates/leyla-cli",
]

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
license = "MIT OR Apache-2.0"
repository = "https://github.com/user/leyla"
description = "Durable scheduler and resumable task orchestrator for Claude Code"

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
chrono = { version = "0.4", features = ["serde"] }
uuid = { version = "1", features = ["v4"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
async-trait = "0.1"
thiserror = "2"
tokio-util = { version = "0.7", features = ["rt"] }

leyla-core = { path = "crates/leyla-core" }
leyla-executors = { path = "crates/leyla-executors" }
```

- [ ] **Step 3: Create leyla-core Cargo.toml**

```toml
[package]
name = "leyla-core"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
description = "Core engine for Leyla durable scheduler"

[dependencies]
tokio.workspace = true
serde.workspace = true
serde_json.workspace = true
chrono.workspace = true
uuid.workspace = true
tracing.workspace = true
async-trait.workspace = true
thiserror.workspace = true
tokio-util.workspace = true
rusqlite = { version = "0.32", features = ["bundled"] }
cron = "0.15"

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
tempfile = "3"
```

- [ ] **Step 4: Create leyla-executors Cargo.toml**

```toml
[package]
name = "leyla-executors"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
description = "Execution adapters for Leyla scheduler"

[dependencies]
leyla-core.workspace = true
tokio.workspace = true
serde.workspace = true
serde_json.workspace = true
tracing.workspace = true
async-trait.workspace = true
thiserror.workspace = true
anyhow = "1"

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
tempfile = "3"
```

- [ ] **Step 5: Create leyla-cli Cargo.toml**

```toml
[package]
name = "leyla"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
description = "CLI for Leyla durable scheduler"

[[bin]]
name = "leyla"
path = "src/main.rs"

[dependencies]
leyla-core.workspace = true
leyla-executors.workspace = true
tokio.workspace = true
serde.workspace = true
serde_json.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
chrono.workspace = true
clap = { version = "4", features = ["derive"] }
tabled = "0.17"
dirs = "6"
anyhow = "1"
libc = "0.2"
```

- [ ] **Step 6: Create minimal lib.rs and main.rs stubs**

```rust
// crates/leyla-core/src/lib.rs
// zaman zaman beni dusunup agliyormussun leyla
```

```rust
// crates/leyla-executors/src/lib.rs
pub use leyla_core;
```

```rust
// crates/leyla-cli/src/main.rs
fn main() {
    println!("leyla: henuz bir sey yok ama olacak");
}
```

- [ ] **Step 7: Create .gitignore**

```gitignore
/target
**/*.rs.bk
.DS_Store
*.db
*.pid
```

- [ ] **Step 8: Create LICENSE-MIT**

Standard MIT license text with `Copyright (c) 2026 Leyla Contributors`.

- [ ] **Step 9: Create LICENSE-APACHE**

Standard Apache 2.0 license text with `Copyright 2026 Leyla Contributors`.

- [ ] **Step 10: Verify workspace compiles**

```bash
cargo check
```

Expected: compiles with no errors.

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "chore: scaffold workspace with 3 crates

leyla-core, leyla-executors, leyla-cli workspace structure.
MIT + Apache 2.0 dual license."
```

---

### Task 2: Domain Types (`leyla-core/src/types.rs`)

**Files:**
- Create: `crates/leyla-core/src/types.rs`
- Modify: `crates/leyla-core/src/lib.rs`

- [ ] **Step 1: Write tests for type serialization roundtrip**

Add to bottom of `types.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_cron_serializes_roundtrip() {
        let s = Schedule::Cron {
            expression: "0 9 * * *".into(),
            timezone: Some("Europe/Istanbul".into()),
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: Schedule = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn schedule_interval_serializes_roundtrip() {
        let s = Schedule::Interval {
            every_ms: 60_000,
            anchor: IntervalAnchor::Start,
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: Schedule = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn schedule_once_serializes_roundtrip() {
        let s = Schedule::Once { at: Utc::now() };
        let json = serde_json::to_string(&s).unwrap();
        let back: Schedule = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn schedule_manual_serializes_roundtrip() {
        let s = Schedule::Manual;
        let json = serde_json::to_string(&s).unwrap();
        let back: Schedule = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn run_status_serializes_all_variants() {
        let variants = vec![
            RunStatus::Scheduled, RunStatus::Leased, RunStatus::Dispatched,
            RunStatus::Running, RunStatus::Succeeded, RunStatus::Failed,
            RunStatus::RetryWait, RunStatus::DeadLetter, RunStatus::Cancelled,
            RunStatus::TimedOut, RunStatus::Orphaned,
        ];
        for v in variants {
            let json = serde_json::to_string(&v).unwrap();
            let back: RunStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(v, back);
        }
    }

    #[test]
    fn retry_policy_default_matches_spec() {
        let d = RetryPolicy::default();
        assert_eq!(d.max_attempts, 5);
        assert_eq!(d.base_delay_ms, 1000);
        assert_eq!(d.max_delay_ms, 60_000);
        assert!(d.jitter);
        assert_eq!(d.strategy, RetryStrategy::Exponential);
    }

    #[test]
    fn misfire_policy_default_is_coalesce() {
        assert_eq!(MisfirePolicy::default(), MisfirePolicy::Coalesce);
    }

    #[test]
    fn concurrency_policy_default_is_forbid_overlap() {
        assert_eq!(ConcurrencyPolicy::default(), ConcurrencyPolicy::ForbidOverlap);
    }

    #[test]
    fn leyla_job_builder_creates_valid_job() {
        let job = LeylaJob::new("test-job", "Test Job", Schedule::Manual,
            ExecutorSpec::Shell { command: "echo".into(), args: vec!["hello".into()] });
        assert_eq!(job.id, "test-job");
        assert!(job.enabled);
        assert_eq!(job.version, 1);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p leyla-core
```

Expected: FAIL — types.rs doesn't exist yet.

- [ ] **Step 3: Implement types.rs**

```rust
// crates/leyla-core/src/types.rs
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Schedule {
    Cron { expression: String, timezone: Option<String> },
    Interval { every_ms: u64, anchor: IntervalAnchor },
    Once { at: DateTime<Utc> },
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntervalAnchor { Start, Finish, WallClock }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Scheduled, Leased, Dispatched, Running, Succeeded, Failed,
    RetryWait, DeadLetter, Cancelled, TimedOut, Orphaned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyPolicy { Allow, ForbidOverlap, QueueOne, ReplaceRunning }

impl Default for ConcurrencyPolicy {
    fn default() -> Self { Self::ForbidOverlap }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MisfirePolicy {
    RunImmediately, Skip, Coalesce, ReplayAll { max_catchup: Option<u32> },
}

impl Default for MisfirePolicy {
    fn default() -> Self { Self::Coalesce }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryStrategy { Fixed, Linear, Exponential }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub strategy: RetryStrategy,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_attempts: 5, strategy: RetryStrategy::Exponential,
            base_delay_ms: 1000, max_delay_ms: 60_000, jitter: true }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutorSpec {
    Shell { command: String, args: Vec<String> },
    LocalHandler { handler_key: String },
    ClaudeTask { task_type: String, payload_template: Option<serde_json::Value> },
    Webhook { url: String, method: String, headers: HashMap<String, String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeylaJob {
    pub id: String, pub name: String, pub enabled: bool,
    pub schedule: Schedule, pub executor: ExecutorSpec,
    pub concurrency: ConcurrencyPolicy, pub retry: RetryPolicy,
    pub timeout_ms: u64, pub misfire: MisfirePolicy,
    pub tags: Vec<String>, pub metadata: serde_json::Value,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>, pub updated_at: DateTime<Utc>,
    pub version: u64,
}

impl LeylaJob {
    pub fn new(id: impl Into<String>, name: impl Into<String>,
        schedule: Schedule, executor: ExecutorSpec) -> Self {
        let now = Utc::now();
        Self { id: id.into(), name: name.into(), enabled: true, schedule, executor,
            concurrency: ConcurrencyPolicy::default(), retry: RetryPolicy::default(),
            timeout_ms: 300_000, misfire: MisfirePolicy::default(),
            tags: Vec::new(), metadata: serde_json::Value::Null,
            next_run_at: None, last_run_at: None,
            last_success_at: None, last_failure_at: None,
            created_at: now, updated_at: now, version: 1 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRun {
    pub run_id: String, pub job_id: String,
    pub scheduled_for: DateTime<Utc>, pub status: RunStatus,
    pub attempt: u32, pub max_attempts: u32,
    pub lease_owner: Option<String>, pub lease_expires_at: Option<DateTime<Utc>>,
    pub dispatched_at: Option<DateTime<Utc>>, pub started_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>, pub finished_at: Option<DateTime<Utc>>,
    pub input_json: Option<serde_json::Value>, pub output_json: Option<serde_json::Value>,
    pub error_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>, pub updated_at: DateTime<Utc>,
}

impl JobRun {
    pub fn new_scheduled(job_id: impl Into<String>, scheduled_for: DateTime<Utc>, max_attempts: u32) -> Self {
        let now = Utc::now();
        Self { run_id: uuid::Uuid::new_v4().to_string(), job_id: job_id.into(),
            scheduled_for, status: RunStatus::Scheduled, attempt: 1, max_attempts,
            lease_owner: None, lease_expires_at: None, dispatched_at: None,
            started_at: None, heartbeat_at: None, finished_at: None,
            input_json: None, output_json: None, error_json: None,
            created_at: now, updated_at: now }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetter {
    pub id: String, pub run_id: String, pub job_id: String,
    pub input_json: Option<serde_json::Value>, pub error_json: Option<serde_json::Value>,
    pub attempts: u32, pub created_at: DateTime<Utc>,
}
```

- [ ] **Step 4: Update lib.rs to export types**

```rust
// crates/leyla-core/src/lib.rs
// zaman zaman beni dusunup agliyormussun leyla
pub mod types;
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p leyla-core
```

Expected: all 9 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/leyla-core/src/types.rs crates/leyla-core/src/lib.rs
git commit -m "feat(core): add domain types

LeylaJob, JobRun, Schedule, ExecutorSpec, RunStatus, policies with spec defaults."
```

---

### Task 3: Clock Abstraction (`leyla-core/src/clock.rs`)

**Files:**
- Create: `crates/leyla-core/src/clock.rs`
- Modify: `crates/leyla-core/src/lib.rs`

- [ ] **Step 1: Write tests for FakeClock**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    #[test]
    fn system_clock_returns_now() {
        let clock = SystemClock;
        let before = Utc::now();
        let now = clock.now();
        let after = Utc::now();
        assert!(now >= before && now <= after);
    }

    #[test]
    fn fake_clock_returns_fixed_time() {
        let fixed = Utc::now();
        let clock = FakeClock::new(fixed);
        assert_eq!(clock.now(), fixed);
    }

    #[test]
    fn fake_clock_advance_moves_time() {
        let fixed = Utc::now();
        let clock = FakeClock::new(fixed);
        clock.advance(TimeDelta::seconds(60));
        assert_eq!(clock.now(), fixed + TimeDelta::seconds(60));
    }

    #[test]
    fn fake_clock_set_changes_time() {
        let t1 = Utc::now();
        let t2 = t1 + TimeDelta::hours(3);
        let clock = FakeClock::new(t1);
        clock.set(t2);
        assert_eq!(clock.now(), t2);
    }
}
```

- [ ] **Step 2: Implement clock.rs**

```rust
// crates/leyla-core/src/clock.rs
use chrono::{DateTime, TimeDelta, Utc};
use std::sync::{Arc, Mutex};

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> { Utc::now() }
}

/// Deterministic clock for tests.
pub struct FakeClock { inner: Arc<Mutex<DateTime<Utc>>> }

impl FakeClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self { inner: Arc::new(Mutex::new(now)) }
    }
    pub fn advance(&self, delta: TimeDelta) {
        let mut t = self.inner.lock().unwrap();
        *t = *t + delta;
    }
    pub fn set(&self, time: DateTime<Utc>) {
        *self.inner.lock().unwrap() = time;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> { *self.inner.lock().unwrap() }
}
```

- [ ] **Step 3: Add to lib.rs, run tests, commit**

```bash
cargo test -p leyla-core clock
git add crates/leyla-core/src/clock.rs crates/leyla-core/src/lib.rs
git commit -m "feat(core): add Clock trait with SystemClock and FakeClock"
```

---

### Task 4: Lifecycle State Machine (`leyla-core/src/lifecycle.rs`)

**Files:**
- Create: `crates/leyla-core/src/lifecycle.rs`
- Modify: `crates/leyla-core/src/lib.rs`

- [ ] **Step 1: Write tests for valid/invalid transitions**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RunStatus::*;

    #[test]
    fn valid_transitions_succeed() {
        let valid = vec![
            (Scheduled, Leased), (Scheduled, Cancelled),
            (Leased, Dispatched), (Leased, Scheduled),
            (Dispatched, Running), (Dispatched, Failed),
            (Running, Succeeded), (Running, Failed),
            (Running, TimedOut), (Running, Orphaned),
            (Failed, RetryWait), (Failed, DeadLetter),
            (RetryWait, Scheduled),
            (TimedOut, RetryWait), (TimedOut, DeadLetter),
            (Orphaned, Scheduled), (Orphaned, DeadLetter),
        ];
        for (from, to) in valid {
            assert!(can_transition(from, to), "{from:?} -> {to:?} should be valid");
        }
    }

    #[test]
    fn invalid_transitions_fail() {
        let invalid = vec![
            (Scheduled, Running), (Running, Scheduled),
            (Succeeded, Failed), (DeadLetter, Scheduled), (Cancelled, Running),
        ];
        for (from, to) in invalid {
            assert!(!can_transition(from, to), "{from:?} -> {to:?} should be invalid");
        }
    }

    #[test]
    fn terminal_states_have_no_outgoing() {
        let terminals = vec![Succeeded, DeadLetter, Cancelled];
        let all = vec![Scheduled, Leased, Dispatched, Running, Succeeded,
            Failed, RetryWait, DeadLetter, Cancelled, TimedOut, Orphaned];
        for t in &terminals {
            for target in &all {
                if t != target { assert!(!can_transition(*t, *target)); }
            }
        }
    }
}
```

- [ ] **Step 2: Implement lifecycle.rs**

```rust
// crates/leyla-core/src/lifecycle.rs
use crate::types::RunStatus;
use crate::types::RunStatus::*;

/// // yanlis yola sapma leyla
pub fn can_transition(from: RunStatus, to: RunStatus) -> bool {
    matches!((from, to),
        (Scheduled, Leased) | (Scheduled, Cancelled)
        | (Leased, Dispatched) | (Leased, Scheduled)
        | (Dispatched, Running) | (Dispatched, Failed)
        | (Running, Succeeded) | (Running, Failed)
        | (Running, TimedOut) | (Running, Orphaned)
        | (Failed, RetryWait) | (Failed, DeadLetter)
        | (RetryWait, Scheduled)
        | (TimedOut, RetryWait) | (TimedOut, DeadLetter)
        | (Orphaned, Scheduled) | (Orphaned, DeadLetter)
    )
}

pub fn transition(from: RunStatus, to: RunStatus) -> Result<RunStatus, LifecycleError> {
    if can_transition(from, to) { Ok(to) }
    else { Err(LifecycleError::InvalidTransition { from, to }) }
}

#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("invalid transition: {from:?} -> {to:?}")]
    InvalidTransition { from: RunStatus, to: RunStatus },
}
```

- [ ] **Step 3: Run tests, commit**

```bash
cargo test -p leyla-core lifecycle
git add crates/leyla-core/src/lifecycle.rs crates/leyla-core/src/lib.rs
git commit -m "feat(core): add RunStatus state machine with transition validation"
```

---

### Task 5: Retry Policy Logic

**Files:**
- Create: `crates/leyla-core/src/policies/mod.rs`
- Create: `crates/leyla-core/src/policies/retry.rs`

- [ ] **Step 1: Write tests for backoff calculation**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RetryPolicy, RetryStrategy};

    #[test]
    fn fixed_returns_base() {
        let p = RetryPolicy { strategy: RetryStrategy::Fixed, base_delay_ms: 1000,
            max_delay_ms: 60_000, jitter: false, max_attempts: 5 };
        assert_eq!(compute_delay(&p, 1), 1000);
        assert_eq!(compute_delay(&p, 3), 1000);
    }

    #[test]
    fn linear_scales() {
        let p = RetryPolicy { strategy: RetryStrategy::Linear, base_delay_ms: 1000,
            max_delay_ms: 60_000, jitter: false, max_attempts: 5 };
        assert_eq!(compute_delay(&p, 2), 2000);
        assert_eq!(compute_delay(&p, 3), 3000);
    }

    #[test]
    fn exponential_doubles() {
        let p = RetryPolicy { strategy: RetryStrategy::Exponential, base_delay_ms: 1000,
            max_delay_ms: 60_000, jitter: false, max_attempts: 5 };
        assert_eq!(compute_delay(&p, 1), 1000);
        assert_eq!(compute_delay(&p, 2), 2000);
        assert_eq!(compute_delay(&p, 3), 4000);
    }

    #[test]
    fn caps_at_max() {
        let p = RetryPolicy { strategy: RetryStrategy::Exponential, base_delay_ms: 10_000,
            max_delay_ms: 30_000, jitter: false, max_attempts: 10 };
        assert_eq!(compute_delay(&p, 5), 30_000);
    }

    #[test]
    fn jitter_within_bounds() {
        let p = RetryPolicy { strategy: RetryStrategy::Fixed, base_delay_ms: 1000,
            max_delay_ms: 60_000, jitter: true, max_attempts: 5 };
        for _ in 0..100 {
            let d = compute_delay(&p, 1);
            assert!(d >= 500 && d <= 1500, "out of range: {d}");
        }
    }

    #[test]
    fn should_retry_within_max() {
        let p = RetryPolicy::default();
        assert!(should_retry(&p, 1));
        assert!(!should_retry(&p, 5));
    }
}
```

- [ ] **Step 2: Implement retry.rs**

```rust
// crates/leyla-core/src/policies/retry.rs
use crate::types::{RetryPolicy, RetryStrategy};

/// // bir kere daha dene leyla
pub fn compute_delay(policy: &RetryPolicy, attempt: u32) -> u64 {
    let raw = match policy.strategy {
        RetryStrategy::Fixed => policy.base_delay_ms,
        RetryStrategy::Linear => policy.base_delay_ms * u64::from(attempt),
        RetryStrategy::Exponential => {
            policy.base_delay_ms * 2u64.saturating_pow(attempt.saturating_sub(1))
        }
    };
    let capped = raw.min(policy.max_delay_ms);
    if policy.jitter { apply_jitter(capped) } else { capped }
}

pub fn should_retry(policy: &RetryPolicy, current_attempt: u32) -> bool {
    current_attempt < policy.max_attempts
}

fn apply_jitter(delay: u64) -> u64 {
    use std::time::SystemTime;
    let seed = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default().subsec_nanos();
    let factor = 0.5 + (seed as f64 % 1000.0) / 1000.0;
    (delay as f64 * factor) as u64
}
```

- [ ] **Step 3: Create policies/mod.rs, update lib.rs, run tests, commit**

```bash
cargo test -p leyla-core retry
git add crates/leyla-core/src/policies/ crates/leyla-core/src/lib.rs
git commit -m "feat(core): add retry policy with fixed/linear/exponential backoff"
```

---

### Task 6: Misfire & Concurrency Policies

**Files:**
- Create: `crates/leyla-core/src/policies/misfire.rs`
- Create: `crates/leyla-core/src/policies/concurrency.rs`

- [ ] **Step 1: Write misfire tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeDelta, Utc};
    use crate::types::MisfirePolicy;

    #[test]
    fn coalesce_returns_only_latest() {
        let now = Utc::now();
        let overdue = vec![now - TimeDelta::hours(3), now - TimeDelta::hours(1)];
        let r = resolve_misfires(&MisfirePolicy::Coalesce, &overdue, now);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn skip_returns_empty() {
        let now = Utc::now();
        let r = resolve_misfires(&MisfirePolicy::Skip, &[now - TimeDelta::hours(1)], now);
        assert!(r.is_empty());
    }

    #[test]
    fn run_immediately_returns_all() {
        let now = Utc::now();
        let overdue = vec![now - TimeDelta::hours(2), now - TimeDelta::hours(1)];
        assert_eq!(resolve_misfires(&MisfirePolicy::RunImmediately, &overdue, now).len(), 2);
    }

    #[test]
    fn replay_all_respects_max_catchup() {
        let now = Utc::now();
        let overdue: Vec<_> = (1..=10).map(|i| now - TimeDelta::hours(i)).collect();
        let r = resolve_misfires(&MisfirePolicy::ReplayAll { max_catchup: Some(3) }, &overdue, now);
        assert_eq!(r.len(), 3);
    }
}
```

- [ ] **Step 2: Write concurrency tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ConcurrencyPolicy;

    #[test]
    fn allow_always_permits() { assert!(can_dispatch(&ConcurrencyPolicy::Allow, 5)); }

    #[test]
    fn forbid_overlap_blocks() {
        assert!(can_dispatch(&ConcurrencyPolicy::ForbidOverlap, 0));
        assert!(!can_dispatch(&ConcurrencyPolicy::ForbidOverlap, 1));
    }
}
```

- [ ] **Step 3: Implement misfire.rs**

```rust
// crates/leyla-core/src/policies/misfire.rs
use chrono::{DateTime, Utc};
use crate::types::MisfirePolicy;

pub fn resolve_misfires(policy: &MisfirePolicy, overdue: &[DateTime<Utc>], _now: DateTime<Utc>) -> Vec<DateTime<Utc>> {
    if overdue.is_empty() { return Vec::new(); }
    match policy {
        MisfirePolicy::Skip => Vec::new(),
        MisfirePolicy::RunImmediately => overdue.to_vec(),
        MisfirePolicy::Coalesce => vec![*overdue.iter().max().unwrap()],
        MisfirePolicy::ReplayAll { max_catchup } => {
            let mut sorted = overdue.to_vec();
            sorted.sort();
            match max_catchup {
                Some(max) => sorted.into_iter().rev().take(*max as usize).rev().collect(),
                None => sorted,
            }
        }
    }
}
```

- [ ] **Step 4: Implement concurrency.rs**

```rust
// crates/leyla-core/src/policies/concurrency.rs
use crate::types::ConcurrencyPolicy;

pub fn can_dispatch(policy: &ConcurrencyPolicy, active_count: usize) -> bool {
    match policy {
        ConcurrencyPolicy::Allow | ConcurrencyPolicy::ReplaceRunning => true,
        ConcurrencyPolicy::ForbidOverlap | ConcurrencyPolicy::QueueOne => active_count == 0,
    }
}
```

- [ ] **Step 5: Run tests, commit**

```bash
cargo test -p leyla-core policies
git add crates/leyla-core/src/policies/
git commit -m "feat(core): add misfire and concurrency policies"
```

---

### Task 7: Store Trait & In-Memory Store

**Files:**
- Create: `crates/leyla-core/src/store/mod.rs`
- Create: `crates/leyla-core/src/store/memory.rs`
- Create: `crates/leyla-core/src/store/sqlite.rs` (stub)

- [ ] **Step 1: Define LeylaStore trait in store/mod.rs**

```rust
// crates/leyla-core/src/store/mod.rs
pub mod memory;
pub mod sqlite;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;
use crate::types::{DeadLetter, JobRun, LeylaJob, RunStatus};

#[derive(Debug, Default)]
pub struct RunPatch {
    pub status: Option<RunStatus>,
    pub lease_owner: Option<Option<String>>,
    pub lease_expires_at: Option<Option<DateTime<Utc>>>,
    pub dispatched_at: Option<Option<DateTime<Utc>>>,
    pub started_at: Option<Option<DateTime<Utc>>>,
    pub heartbeat_at: Option<Option<DateTime<Utc>>>,
    pub finished_at: Option<Option<DateTime<Utc>>>,
    pub attempt: Option<u32>,
    pub output_json: Option<Option<serde_json::Value>>,
    pub error_json: Option<Option<serde_json::Value>>,
}

#[derive(Debug, Default)]
pub struct RunFilter {
    pub job_id: Option<String>,
    pub status: Option<RunStatus>,
    pub limit: Option<u32>,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("storage error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[async_trait]
pub trait LeylaStore: Send + Sync {
    async fn upsert_job(&self, job: &LeylaJob) -> Result<()>;
    async fn get_job(&self, job_id: &str) -> Result<Option<LeylaJob>>;
    async fn list_jobs(&self) -> Result<Vec<LeylaJob>>;
    async fn remove_job(&self, job_id: &str) -> Result<()>;
    async fn insert_run(&self, run: &JobRun) -> Result<()>;
    async fn claim_due_runs(&self, now: DateTime<Utc>, owner: &str, limit: u32) -> Result<Vec<JobRun>>;
    async fn update_run(&self, run_id: &str, patch: RunPatch) -> Result<()>;
    async fn get_run(&self, run_id: &str) -> Result<Option<JobRun>>;
    async fn list_runs(&self, filter: &RunFilter) -> Result<Vec<JobRun>>;
    async fn get_stale_runs(&self, now: DateTime<Utc>, stale_threshold: Duration) -> Result<Vec<JobRun>>;
    async fn get_dead_letters(&self) -> Result<Vec<DeadLetter>>;
    async fn count_active_runs(&self, job_id: &str) -> Result<usize>;
    async fn migrate(&self) -> Result<()>;
}
```

- [ ] **Step 2: Write MemoryStore tests**

```rust
// crates/leyla-core/src/store/memory.rs — tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use chrono::{TimeDelta, Utc};

    fn test_job() -> LeylaJob {
        LeylaJob::new("j1", "Test", Schedule::Manual,
            ExecutorSpec::Shell { command: "echo".into(), args: vec![] })
    }

    #[tokio::test]
    async fn upsert_and_get() {
        let s = MemoryStore::new();
        s.upsert_job(&test_job()).await.unwrap();
        assert!(s.get_job("j1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn remove_job_works() {
        let s = MemoryStore::new();
        s.upsert_job(&test_job()).await.unwrap();
        s.remove_job("j1").await.unwrap();
        assert!(s.get_job("j1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn claim_due_runs_works() {
        let s = MemoryStore::new();
        let now = Utc::now();
        let run = JobRun::new_scheduled("j1", now - TimeDelta::seconds(10), 5);
        s.insert_run(&run).await.unwrap();
        let claimed = s.claim_due_runs(now, "o1", 10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].status, RunStatus::Leased);
        // Second claim yields nothing
        assert!(s.claim_due_runs(now, "o2", 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn update_run_applies_patch() {
        let s = MemoryStore::new();
        let now = Utc::now();
        let run = JobRun::new_scheduled("j1", now, 5);
        let rid = run.run_id.clone();
        s.insert_run(&run).await.unwrap();
        s.update_run(&rid, RunPatch {
            status: Some(RunStatus::Running),
            started_at: Some(Some(now)), ..Default::default()
        }).await.unwrap();
        let got = s.get_run(&rid).await.unwrap().unwrap();
        assert_eq!(got.status, RunStatus::Running);
    }

    #[tokio::test]
    async fn count_active_runs_works() {
        let s = MemoryStore::new();
        let now = Utc::now();
        let mut r1 = JobRun::new_scheduled("j1", now, 5);
        r1.status = RunStatus::Running;
        s.insert_run(&r1).await.unwrap();
        let mut r2 = JobRun::new_scheduled("j1", now, 5);
        r2.status = RunStatus::Succeeded;
        s.insert_run(&r2).await.unwrap();
        assert_eq!(s.count_active_runs("j1").await.unwrap(), 1);
    }
}
```

- [ ] **Step 3: Implement MemoryStore**

Full in-memory implementation using `Mutex<HashMap>` for jobs and runs. `claim_due_runs` atomically flips status to Leased. See design spec for full contract.

- [ ] **Step 4: Stub sqlite.rs**

```rust
// crates/leyla-core/src/store/sqlite.rs
// sqlite impl in Task 8
```

- [ ] **Step 5: Run tests, commit**

```bash
cargo test -p leyla-core store
git add crates/leyla-core/src/store/ crates/leyla-core/src/lib.rs
git commit -m "feat(core): add LeylaStore trait and MemoryStore"
```

---

### Task 8: SQLite Store Implementation

**Files:**
- Modify: `crates/leyla-core/src/store/sqlite.rs`

- [ ] **Step 1: Write SQLite store tests (same contract as MemoryStore)**

Tests for: upsert_and_get, list_and_remove, claim_due_runs_atomic, update_run, stale_runs, count_active. Use `tempfile::TempDir` for isolated DB per test.

- [ ] **Step 2: Implement SqliteStore**

Full implementation with:
- `PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;`
- Schema migration in `migrate()` (CREATE TABLE IF NOT EXISTS for jobs, job_runs, dead_letters + indices)
- `claim_due_runs` via transaction: SELECT + UPDATE atomically
- `remove_job` via SQL
- `update_run` with dynamic field updates
- `row_to_job_run` helper for deserialization
- RFC3339 datetime serialization

- [ ] **Step 3: Run tests, commit**

```bash
cargo test -p leyla-core sqlite
git add crates/leyla-core/src/store/sqlite.rs
git commit -m "feat(core): add SQLite store with WAL mode and atomic claiming"
```

---

### Task 9: DueRunMaterializer

**Files:**
- Create: `crates/leyla-core/src/scheduler/mod.rs`
- Create: `crates/leyla-core/src/scheduler/due.rs`

- [ ] **Step 1: Write tests**

Tests for: manual jobs skipped, once job materializes when due, disabled jobs skipped, interval job advances next_run_at.

- [ ] **Step 2: Implement DueRunMaterializer**

Scans enabled jobs where `next_run_at <= now`, creates `Scheduled` runs, advances `next_run_at` via `compute_next_run()` (cron uses `cron::Schedule`, interval adds `every_ms`, once returns None).

- [ ] **Step 3: Run tests, commit**

```bash
cargo test -p leyla-core due
git add crates/leyla-core/src/scheduler/ crates/leyla-core/src/lib.rs
git commit -m "feat(core): add DueRunMaterializer with cron/interval/once support"
```

---

### Task 10: LeaseManager

**Files:**
- Modify: `crates/leyla-core/src/scheduler/claim.rs`

- [ ] **Step 1: Write tests** — claim batch, respect batch size, concurrency filtering.

- [ ] **Step 2: Implement LeaseManager** — wraps `store.claim_due_runs()` + `filter_by_concurrency()`.

- [ ] **Step 3: Run tests, commit**

---

### Task 11: RecoveryManager

**Files:**
- Modify: `crates/leyla-core/src/scheduler/recovery.rs`

- [ ] **Step 1: Write tests** — stale lease recovery, exhausted to dead letter, retry_wait advance.

- [ ] **Step 2: Implement RecoveryManager** — finds stale runs, transitions orphaned to scheduled or dead_letter based on attempt count. Advances retry_wait to scheduled.

- [ ] **Step 3: Run tests, commit**

---

### Task 12: LeylaEngine (3-Loop Orchestration)

**Files:**
- Create: `crates/leyla-core/src/engine.rs`

- [ ] **Step 1: Write tests** — engine start/stop, schedule + trigger manual job, pause/resume, get/list.

- [ ] **Step 2: Implement LeylaEngine** — `EngineConfig`, `start()` with `tokio::select!` running 3 loops under `CancellationToken`, `stop()`, `schedule()`, `unschedule()`, `pause()`, `resume()`, `trigger()`, `get_job()`, `list_jobs()`, `list_runs()`.

- [ ] **Step 3: Run tests, commit**

---

### Task 13: Executor Trait & ShellExecutor

**Files:**
- Create: `crates/leyla-executors/src/executor.rs`
- Create: `crates/leyla-executors/src/shell.rs`

- [ ] **Step 1: Write tests** — echo succeeds, nonzero exit fails, timeout, output truncation.

- [ ] **Step 2: Implement executor trait** (`RunContext`, `DispatchResult`, `DispatchStatus`, `LeylaExecutor` trait).

- [ ] **Step 3: Implement ShellExecutor** — `tokio::process::Command` with `tokio::time::timeout`, stdout/stderr capture, exit code mapping, output truncation.

- [ ] **Step 4: Run tests, commit**

---

### Task 14: LocalHandlerExecutor

**Files:**
- Modify: `crates/leyla-executors/src/local_handler.rs`

- [ ] **Step 1: Write tests** — registered handler dispatches, unknown returns error, handler failure captured.

- [ ] **Step 2: Implement** — `HashMap<String, HandlerFn>` registry, `register()`, `dispatch_handler()`.

- [ ] **Step 3: Run tests, commit**

---

### Task 15: ClaudeTaskExecutor

**Files:**
- Modify: `crates/leyla-executors/src/claude_task.rs`

- [ ] **Step 1: Write tests** — prompt builds from full snapshot, prompt builds without optional fields.

- [ ] **Step 2: Implement** — `TaskSnapshot` struct, `build_prompt()`, `ClaudeTaskExecutor` that delegates to `ShellExecutor` with `claude -p "<prompt>"`.

- [ ] **Step 3: Run tests, commit**

---

### Task 16: CLI — Clap Skeleton & Job Commands

**Files:**
- Modify: `crates/leyla-cli/src/main.rs`
- Create: `crates/leyla-cli/src/commands/mod.rs`
- Create: `crates/leyla-cli/src/commands/job.rs`
- Create: `crates/leyla-cli/src/output.rs`

- [ ] **Step 1: Implement clap structure** — `Cli` parser with `Commands` enum (Job, Run, Doctor, Recover, Daemon), `--json` global flag.

- [ ] **Step 2: Implement job commands** — add (with --cron/--interval-ms/--at/--command/--args), list, inspect, pause, resume, remove.

- [ ] **Step 3: Implement output.rs** — `print_jobs_table()`, `print_job_detail()`, `print_runs_table()` with column formatting.

- [ ] **Step 4: Verify compiles, commit**

---

### Task 17: CLI — Run, Doctor, Recover, Daemon Commands

**Files:**
- Create: `crates/leyla-cli/src/commands/run.rs`
- Create: `crates/leyla-cli/src/commands/doctor.rs`
- Create: `crates/leyla-cli/src/commands/recover.rs`
- Create: `crates/leyla-cli/src/commands/daemon.rs`

- [ ] **Step 1: Implement run commands** — trigger, inspect, retry, cancel, list (with --job/--status/--limit filters).

- [ ] **Step 2: Implement doctor** — check stale leases, dead letters, overdue runs, daemon PID alive. Output both human and --json format.

- [ ] **Step 3: Implement recover** — instantiate RecoveryManager, run once, print count.

- [ ] **Step 4: Implement daemon** — start (spawn self as background), stop (SIGTERM via PID file), status (check PID alive). PID at `~/.leyla/leyla.pid`.

- [ ] **Step 5: Verify compiles, commit**

---

### Task 18: Integration Tests

**Files:**
- Create: `crates/leyla-core/tests/integration.rs`

- [ ] **Step 1: Write full lifecycle test** — schedule manual job, trigger, verify run created, pause/resume.

- [ ] **Step 2: Write interval materialization test** — create interval job with due next_run_at, materialize, verify run created and next_run_at advanced.

- [ ] **Step 3: Run, commit**

---

### Task 19: Final Verification & README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Run all tests**

```bash
cargo test --workspace
```

- [ ] **Step 2: Run clippy**

```bash
cargo clippy --workspace -- -D warnings
```

- [ ] **Step 3: Create README.md** — install instructions, quick start examples, architecture overview, license.

- [ ] **Step 4: Build release binary**

```bash
cargo build --release -p leyla
./target/release/leyla --help
```

- [ ] **Step 5: Commit**
