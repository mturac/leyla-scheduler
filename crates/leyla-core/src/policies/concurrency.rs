use crate::types::ConcurrencyPolicy;

pub fn can_dispatch(policy: &ConcurrencyPolicy, active_count: usize) -> bool {
    match policy {
        ConcurrencyPolicy::Allow | ConcurrencyPolicy::ReplaceRunning => true,
        ConcurrencyPolicy::ForbidOverlap | ConcurrencyPolicy::QueueOne => active_count == 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_always_permits() {
        let policy = ConcurrencyPolicy::Allow;
        assert!(can_dispatch(&policy, 0));
        assert!(can_dispatch(&policy, 1));
        assert!(can_dispatch(&policy, 5));
    }

    #[test]
    fn forbid_overlap_blocks_when_active_greater_than_zero() {
        let policy = ConcurrencyPolicy::ForbidOverlap;
        assert!(can_dispatch(&policy, 0));
        assert!(!can_dispatch(&policy, 1));
        assert!(!can_dispatch(&policy, 5));
    }

    #[test]
    fn queue_one_blocks_when_active_greater_than_zero() {
        let policy = ConcurrencyPolicy::QueueOne;
        assert!(can_dispatch(&policy, 0));
        assert!(!can_dispatch(&policy, 1));
        assert!(!can_dispatch(&policy, 5));
    }

    #[test]
    fn replace_running_always_permits() {
        let policy = ConcurrencyPolicy::ReplaceRunning;
        assert!(can_dispatch(&policy, 0));
        assert!(can_dispatch(&policy, 1));
        assert!(can_dispatch(&policy, 5));
    }
}
