use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;

use crate::types::{DeadLetter, JobRun, LeylaJob, RunStatus};

pub mod memory;
pub mod sqlite;

// ── RunPatch ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct RunPatch {
    pub status: Option<RunStatus>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub attempt: Option<u32>,
    pub output_json: Option<serde_json::Value>,
    pub error_json: Option<serde_json::Value>,
}

// ── RunFilter ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct RunFilter {
    pub job_id: Option<String>,
    pub status: Option<RunStatus>,
    pub limit: Option<u32>,
}

// ── StoreError ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

// ── LeylaStore trait ──────────────────────────────────────────────────────────

#[async_trait]
pub trait LeylaStore: Send + Sync {
    // Job CRUD
    async fn upsert_job(&self, job: LeylaJob) -> Result<()>;
    async fn get_job(&self, job_id: &str) -> Result<LeylaJob>;
    async fn list_jobs(&self) -> Result<Vec<LeylaJob>>;
    async fn remove_job(&self, job_id: &str) -> Result<()>;

    // Run operations
    async fn insert_run(&self, run: JobRun) -> Result<()>;
    async fn claim_due_runs(
        &self,
        now: DateTime<Utc>,
        owner: &str,
        limit: usize,
    ) -> Result<Vec<JobRun>>;
    async fn update_run(&self, run_id: &str, patch: RunPatch) -> Result<()>;
    async fn get_run(&self, run_id: &str) -> Result<JobRun>;
    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<JobRun>>;

    // Maintenance
    async fn get_stale_runs(
        &self,
        now: DateTime<Utc>,
        stale_threshold: Duration,
    ) -> Result<Vec<JobRun>>;
    async fn get_dead_letters(&self) -> Result<Vec<DeadLetter>>;
    async fn count_active_runs(&self, job_id: &str) -> Result<usize>;

    // Migration / schema setup
    async fn migrate(&self) -> Result<()>;
}
