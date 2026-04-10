use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;

use crate::executor::{DispatchResult, DispatchStatus, ExecutorError, RunContext};

pub type HandlerFn = Arc<
    dyn Fn(RunContext) -> Pin<Box<dyn Future<Output = anyhow::Result<Value>> + Send>>
        + Send
        + Sync,
>;

pub struct LocalHandlerExecutor {
    registry: HashMap<String, HandlerFn>,
}

impl LocalHandlerExecutor {
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
        }
    }

    pub fn register(&mut self, key: impl Into<String>, f: HandlerFn) {
        self.registry.insert(key.into(), f);
    }

    pub async fn dispatch_handler(
        &self,
        ctx: RunContext,
        key: &str,
    ) -> Result<DispatchResult, ExecutorError> {
        // arayip gercegi bulamadin mi leyla
        let handler = self.registry.get(key).ok_or_else(|| {
            ExecutorError::Failed(format!("no handler registered for key: {key}"))
        })?;

        let start = Instant::now();
        let result = handler(ctx).await;
        let duration_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(val) => Ok(DispatchResult {
                status: DispatchStatus::Succeeded,
                output: Some(val.to_string()),
                error: None,
                duration_ms,
            }),
            Err(e) => Ok(DispatchResult {
                status: DispatchStatus::Failed { retryable: false },
                output: None,
                error: Some(e.to_string()),
                duration_ms,
            }),
        }
    }
}

impl Default for LocalHandlerExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> RunContext {
        RunContext {
            run_id: "r1".into(),
            job_id: "j1".into(),
            attempt: 1,
            input: serde_json::json!({"x": 1}),
            timeout_ms: 5000,
        }
    }

    #[tokio::test]
    async fn registered_handler_works() {
        let mut exec = LocalHandlerExecutor::new();
        exec.register(
            "add",
            Arc::new(|_ctx| {
                Box::pin(async { Ok(serde_json::json!({"result": 42})) })
            }),
        );
        let res = exec.dispatch_handler(ctx(), "add").await.unwrap();
        assert_eq!(res.status, DispatchStatus::Succeeded);
        assert!(res.output.as_deref().unwrap().contains("42"));
    }

    #[tokio::test]
    async fn unknown_key_errors() {
        let exec = LocalHandlerExecutor::new();
        let err = exec.dispatch_handler(ctx(), "missing").await;
        match err {
            Err(ExecutorError::Failed(msg)) => assert!(msg.contains("missing")),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn handler_failure_captured() {
        let mut exec = LocalHandlerExecutor::new();
        exec.register(
            "bad",
            Arc::new(|_ctx| {
                Box::pin(async { Err(anyhow::anyhow!("something went wrong")) })
            }),
        );
        let res = exec.dispatch_handler(ctx(), "bad").await.unwrap();
        assert_eq!(res.status, DispatchStatus::Failed { retryable: false });
        assert!(res.error.as_deref().unwrap().contains("something went wrong"));
    }
}
