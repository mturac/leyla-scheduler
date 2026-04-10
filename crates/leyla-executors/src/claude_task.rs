use crate::executor::{DispatchResult, ExecutorError, RunContext};
use crate::shell::ShellExecutor;

/// A snapshot of a Claude task to be executed.
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub task: String,
    pub cwd: Option<String>,
    pub files: Vec<String>,
    pub notes: Option<String>,
    pub expected_output: Option<String>,
}

/// Build a prompt string from a TaskSnapshot.
pub fn build_prompt(snapshot: &TaskSnapshot) -> String {
    let mut parts = vec![snapshot.task.clone()];

    if let Some(ref cwd) = snapshot.cwd {
        parts.push(format!("Working directory: {cwd}"));
    }

    if !snapshot.files.is_empty() {
        parts.push(format!("Files: {}", snapshot.files.join(", ")));
    }

    if let Some(ref notes) = snapshot.notes {
        parts.push(format!("Notes: {notes}"));
    }

    if let Some(ref expected) = snapshot.expected_output {
        parts.push(format!("Expected output: {expected}"));
    }

    parts.join("\n")
}

pub struct ClaudeTaskExecutor {
    shell: ShellExecutor,
}

impl ClaudeTaskExecutor {
    pub fn new(max_output_bytes: usize) -> Self {
        Self {
            shell: ShellExecutor::new(max_output_bytes),
        }
    }

    pub async fn dispatch_snapshot(
        &self,
        ctx: &RunContext,
        snapshot: &TaskSnapshot,
    ) -> Result<DispatchResult, ExecutorError> {
        let prompt = build_prompt(snapshot);
        self.shell
            .dispatch_command(ctx, "claude", &["-p", &prompt])
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_snapshot() -> TaskSnapshot {
        TaskSnapshot {
            task: "Fix the bug".into(),
            cwd: Some("/workspace".into()),
            files: vec!["src/main.rs".into(), "src/lib.rs".into()],
            notes: Some("check error handling".into()),
            expected_output: Some("tests pass".into()),
        }
    }

    fn minimal_snapshot() -> TaskSnapshot {
        TaskSnapshot {
            task: "Write a function".into(),
            cwd: None,
            files: vec![],
            notes: None,
            expected_output: None,
        }
    }

    #[test]
    fn prompt_builds_with_all_fields() {
        let prompt = build_prompt(&full_snapshot());
        assert!(prompt.contains("Fix the bug"));
        assert!(prompt.contains("/workspace"));
        assert!(prompt.contains("src/main.rs"));
        assert!(prompt.contains("src/lib.rs"));
        assert!(prompt.contains("check error handling"));
        assert!(prompt.contains("tests pass"));
    }

    #[test]
    fn prompt_builds_without_optional_fields() {
        let prompt = build_prompt(&minimal_snapshot());
        assert!(prompt.contains("Write a function"));
        assert!(!prompt.contains("Working directory"));
        assert!(!prompt.contains("Files"));
        assert!(!prompt.contains("Notes"));
        assert!(!prompt.contains("Expected output"));
    }
}
