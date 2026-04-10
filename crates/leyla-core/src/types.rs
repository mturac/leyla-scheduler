use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

// ── Schedule ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Schedule {
    Cron {
        expression: String,
        timezone: Option<String>,
    },
    Interval {
        every_ms: u64,
        anchor: IntervalAnchor,
    },
    Once {
        at: DateTime<Utc>,
    },
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntervalAnchor {
    Start,
    Finish,
    WallClock,
}

// ── RunStatus ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

// ── ConcurrencyPolicy ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyPolicy {
    Allow,
    ForbidOverlap,
    QueueOne,
    ReplaceRunning,
}

impl Default for ConcurrencyPolicy {
    fn default() -> Self {
        ConcurrencyPolicy::ForbidOverlap
    }
}

// ── MisfirePolicy ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MisfirePolicy {
    RunImmediately,
    Skip,
    Coalesce,
    ReplayAll { max_catchup: Option<u32> },
}

impl Default for MisfirePolicy {
    fn default() -> Self {
        MisfirePolicy::Coalesce
    }
}

// ── RetryStrategy & RetryPolicy ───────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryStrategy {
    Fixed,
    Linear,
    Exponential,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub strategy: RetryStrategy,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 5,
            strategy: RetryStrategy::Exponential,
            base_delay_ms: 1000,
            max_delay_ms: 60000,
            jitter: true,
        }
    }
}

// ── ExecutorSpec ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutorSpec {
    Shell {
        command: String,
        args: Vec<String>,
    },
    LocalHandler {
        handler_key: String,
    },
    ClaudeTask {
        task_type: String,
        payload_template: Option<serde_json::Value>,
    },
    Webhook {
        url: String,
        method: String,
        headers: HashMap<String, String>,
    },
}

// ── LeylaJob ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeylaJob {
    pub id: Uuid,
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
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: u64,
}

impl LeylaJob {
    pub fn new(name: impl Into<String>, schedule: Schedule, executor: ExecutorSpec) -> Self {
        let now = Utc::now();
        LeylaJob {
            id: Uuid::new_v4(),
            name: name.into(),
            enabled: true,
            schedule,
            executor,
            concurrency: ConcurrencyPolicy::default(),
            retry: RetryPolicy::default(),
            timeout_ms: 30_000,
            misfire: MisfirePolicy::default(),
            tags: Vec::new(),
            metadata: serde_json::Value::Null,
            next_run_at: None,
            last_run_at: None,
            last_success_at: None,
            last_failure_at: None,
            created_at: now,
            updated_at: now,
            version: 1,
        }
    }
}

// ── JobRun ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRun {
    pub run_id: Uuid,
    pub job_id: Uuid,
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

impl JobRun {
    pub fn new_scheduled(job_id: Uuid, scheduled_for: DateTime<Utc>, max_attempts: u32) -> Self {
        let now = Utc::now();
        JobRun {
            run_id: Uuid::new_v4(),
            job_id,
            scheduled_for,
            status: RunStatus::Scheduled,
            attempt: 0,
            max_attempts,
            lease_owner: None,
            lease_expires_at: None,
            dispatched_at: None,
            started_at: None,
            heartbeat_at: None,
            finished_at: None,
            input_json: None,
            output_json: None,
            error_json: None,
            created_at: now,
            updated_at: now,
        }
    }
}

// ── DeadLetter ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetter {
    pub id: Uuid,
    pub run_id: Uuid,
    pub job_id: Uuid,
    pub input_json: Option<serde_json::Value>,
    pub error_json: Option<serde_json::Value>,
    pub attempts: u32,
    pub created_at: DateTime<Utc>,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_cron_roundtrip() {
        let s = Schedule::Cron {
            expression: "0 * * * *".to_string(),
            timezone: Some("UTC".to_string()),
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: Schedule = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn schedule_interval_roundtrip() {
        let s = Schedule::Interval {
            every_ms: 5000,
            anchor: IntervalAnchor::WallClock,
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: Schedule = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn schedule_once_roundtrip() {
        let s = Schedule::Once { at: Utc::now() };
        let json = serde_json::to_string(&s).unwrap();
        let back: Schedule = serde_json::from_str(&json).unwrap();
        // Compare via json because DateTime sub-second precision may differ
        assert_eq!(
            serde_json::to_string(&s).unwrap(),
            serde_json::to_string(&back).unwrap()
        );
    }

    #[test]
    fn schedule_manual_roundtrip() {
        let s = Schedule::Manual;
        let json = serde_json::to_string(&s).unwrap();
        let back: Schedule = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn run_status_all_variants_roundtrip() {
        let variants = [
            RunStatus::Scheduled,
            RunStatus::Leased,
            RunStatus::Dispatched,
            RunStatus::Running,
            RunStatus::Succeeded,
            RunStatus::Failed,
            RunStatus::RetryWait,
            RunStatus::DeadLetter,
            RunStatus::Cancelled,
            RunStatus::TimedOut,
            RunStatus::Orphaned,
        ];
        for v in &variants {
            let json = serde_json::to_string(v).unwrap();
            let back: RunStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(v, &back);
        }
    }

    #[test]
    fn retry_policy_default_matches_spec() {
        let rp = RetryPolicy::default();
        assert_eq!(rp.max_attempts, 5);
        assert_eq!(rp.strategy, RetryStrategy::Exponential);
        assert_eq!(rp.base_delay_ms, 1000);
        assert_eq!(rp.max_delay_ms, 60000);
        assert!(rp.jitter);
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
    fn leyla_job_new_creates_valid_job() {
        let executor = ExecutorSpec::LocalHandler {
            handler_key: "my_handler".to_string(),
        };
        let schedule = Schedule::Manual;
        let job = LeylaJob::new("test-job", schedule, executor);

        assert_eq!(job.name, "test-job");
        assert!(job.enabled);
        assert_eq!(job.version, 1);
        assert_eq!(job.concurrency, ConcurrencyPolicy::ForbidOverlap);
        assert_eq!(job.misfire, MisfirePolicy::Coalesce);
        assert_eq!(job.retry.max_attempts, 5);
        assert!(job.next_run_at.is_none());
    }
}
