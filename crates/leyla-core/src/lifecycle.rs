use crate::types::RunStatus;
use crate::types::RunStatus::*;

pub fn can_transition(from: RunStatus, to: RunStatus) -> bool {
    matches!(
        (from, to),
        (Scheduled, Leased)
            | (Scheduled, Cancelled)
            | (Leased, Dispatched)
            | (Leased, Scheduled)
            | (Dispatched, Running)
            | (Dispatched, Failed)
            | (Running, Succeeded)
            | (Running, Failed)
            | (Running, TimedOut)
            | (Running, Orphaned)
            | (Failed, RetryWait)
            | (Failed, DeadLetter)
            | (RetryWait, Scheduled)
            | (TimedOut, RetryWait)
            | (TimedOut, DeadLetter)
            | (Orphaned, Scheduled)
            | (Orphaned, DeadLetter)
    )
}

pub fn transition(from: RunStatus, to: RunStatus) -> Result<RunStatus, LifecycleError> {
    if can_transition(from, to) {
        Ok(to)
    } else {
        Err(LifecycleError::InvalidTransition { from, to })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("invalid transition: {from:?} -> {to:?}")]
    InvalidTransition { from: RunStatus, to: RunStatus },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_transitions_succeed() {
        let valid_pairs = vec![
            (Scheduled, Leased),
            (Scheduled, Cancelled),
            (Leased, Dispatched),
            (Leased, Scheduled),
            (Dispatched, Running),
            (Dispatched, Failed),
            (Running, Succeeded),
            (Running, Failed),
            (Running, TimedOut),
            (Running, Orphaned),
            (Failed, RetryWait),
            (Failed, DeadLetter),
            (RetryWait, Scheduled),
            (TimedOut, RetryWait),
            (TimedOut, DeadLetter),
            (Orphaned, Scheduled),
            (Orphaned, DeadLetter),
        ];

        for (from, to) in valid_pairs {
            assert!(
                can_transition(from, to),
                "Expected valid transition from {:?} to {:?}",
                from,
                to
            );
            let result = transition(from, to);
            assert!(result.is_ok(), "transition failed: {:?}", result);
            assert_eq!(result.unwrap(), to);
        }
    }

    #[test]
    fn invalid_transitions_fail() {
        let invalid_pairs = vec![
            (Scheduled, Running),
            (Running, Scheduled),
            (Succeeded, Failed),
            (DeadLetter, Scheduled),
            (Cancelled, Running),
        ];

        for (from, to) in invalid_pairs {
            assert!(
                !can_transition(from, to),
                "Expected invalid transition from {:?} to {:?}",
                from,
                to
            );
            let result = transition(from, to);
            assert!(
                result.is_err(),
                "transition should have failed for {:?} -> {:?}",
                from,
                to
            );
        }
    }

    #[test]
    fn terminal_states_have_no_outgoing() {
        let terminal_states = vec![Succeeded, DeadLetter, Cancelled];

        for state in terminal_states {
            let all_states = vec![
                Scheduled, Leased, Dispatched, Running, Succeeded, Failed, RetryWait, DeadLetter,
                Cancelled, TimedOut, Orphaned,
            ];

            for target in all_states {
                if target != state {
                    assert!(
                        !can_transition(state, target),
                        "Terminal state {:?} should not transition to {:?}",
                        state,
                        target
                    );
                }
            }
        }
    }
}
