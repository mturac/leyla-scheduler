use thiserror::Error;

/// Context passed to every executor dispatch call.
#[derive(Debug, Clone)]
pub struct RunContext {
    pub run_id: String,
    pub job_id: String,
    pub attempt: u32,
    pub input: serde_json::Value,
    pub timeout_ms: u64,
}

/// Final status of a dispatch.
#[derive(Debug, Clone, PartialEq)]
pub enum DispatchStatus {
    Succeeded,
    Failed { retryable: bool },
    TimedOut,
}

/// Result returned by every executor.
#[derive(Debug, Clone)]
pub struct DispatchResult {
    pub status: DispatchStatus,
    pub output: Option<String>,
    pub error: Option<String>,
    pub duration_ms: u64,
}

/// Error type for executor failures.
#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("executor failed: {0}")]
    Failed(String),
    #[error("executor timed out after {0}ms")]
    Timeout(u64),
}
