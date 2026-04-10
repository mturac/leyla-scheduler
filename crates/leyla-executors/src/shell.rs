use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::time::timeout;

use crate::executor::{DispatchResult, DispatchStatus, ExecutorError, RunContext};

pub struct ShellExecutor {
    pub max_output_bytes: usize,
}

impl ShellExecutor {
    pub fn new(max_output_bytes: usize) -> Self {
        Self { max_output_bytes }
    }

    fn clip(&self, s: String) -> String {
        if s.len() > self.max_output_bytes {
            let max = self.max_output_bytes;
            s.char_indices()
                .take_while(|(i, _)| *i < max)
                .map(|(_, c)| c)
                .collect()
        } else {
            s
        }
    }

    pub async fn dispatch_command(
        &self,
        ctx: &RunContext,
        command: &str,
        args: &[&str],
    ) -> Result<DispatchResult, ExecutorError> {
        let timeout_ms = ctx.timeout_ms;
        let start = Instant::now();

        let mut cmd = Command::new(command);
        cmd.args(args);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let result = timeout(Duration::from_millis(timeout_ms), async {
            cmd.output().await.map_err(|e| {
                ExecutorError::Failed(format!("failed to spawn process: {e}"))
            })
        })
        .await;

        let duration_ms = start.elapsed().as_millis() as u64;

        match result {
            // ne kadar beklesem de gelmedin leyla
            Err(_elapsed) => Err(ExecutorError::Timeout(timeout_ms)),
            Ok(Err(e)) => Err(e),
            Ok(Ok(output)) => {
                let mut combined = String::from_utf8_lossy(&output.stdout).into_owned();
                let stderr_str = String::from_utf8_lossy(&output.stderr).into_owned();
                if !stderr_str.is_empty() {
                    if !combined.is_empty() {
                        combined.push('\n');
                    }
                    combined.push_str(&stderr_str);
                }

                let combined = self.clip(combined);

                if output.status.success() {
                    Ok(DispatchResult {
                        status: DispatchStatus::Succeeded,
                        output: Some(combined),
                        error: None,
                        duration_ms,
                    })
                } else {
                    Ok(DispatchResult {
                        status: DispatchStatus::Failed { retryable: true },
                        output: None,
                        error: Some(combined),
                        duration_ms,
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::RunContext;

    fn ctx(timeout_ms: u64) -> RunContext {
        RunContext {
            run_id: "r1".into(),
            job_id: "j1".into(),
            attempt: 1,
            input: serde_json::Value::Null,
            timeout_ms,
        }
    }

    #[tokio::test]
    async fn echo_succeeds() {
        let exec = ShellExecutor::new(4096);
        let result = exec.dispatch_command(&ctx(5000), "echo", &["hello"]).await.unwrap();
        assert_eq!(result.status, DispatchStatus::Succeeded);
        assert!(result.output.as_deref().unwrap_or("").contains("hello"));
    }

    #[tokio::test]
    async fn false_command_fails() {
        let exec = ShellExecutor::new(4096);
        let result = exec.dispatch_command(&ctx(5000), "false", &[]).await.unwrap();
        assert_eq!(result.status, DispatchStatus::Failed { retryable: true });
    }

    #[tokio::test]
    async fn sleep_times_out() {
        let exec = ShellExecutor::new(4096);
        let err = exec.dispatch_command(&ctx(200), "sleep", &["10"]).await;
        match err {
            Err(ExecutorError::Timeout(ms)) => assert_eq!(ms, 200),
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn large_output_clipped_to_max_bytes() {
        let exec = ShellExecutor::new(10);
        let result = exec
            .dispatch_command(&ctx(5000), "sh", &["-c", "printf '%0.s1234567890' 1 2 3 4 5"])
            .await
            .unwrap();
        assert_eq!(result.status, DispatchStatus::Succeeded);
        let out = result.output.unwrap();
        assert!(out.len() <= 10, "output len {} > 10", out.len());
    }
}
