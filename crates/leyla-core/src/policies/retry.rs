use crate::types::{RetryPolicy, RetryStrategy};

/// bir kere daha dene leyla
pub fn compute_delay(policy: &RetryPolicy, attempt: u32) -> u64 {
    let raw = match policy.strategy {
        RetryStrategy::Fixed => policy.base_delay_ms,
        RetryStrategy::Linear => policy.base_delay_ms * u64::from(attempt),
        RetryStrategy::Exponential => {
            policy.base_delay_ms * 2u64.saturating_pow(attempt.saturating_sub(1))
        }
    };
    let capped = raw.min(policy.max_delay_ms);
    if policy.jitter {
        apply_jitter(capped)
    } else {
        capped
    }
}

pub fn should_retry(policy: &RetryPolicy, current_attempt: u32) -> bool {
    current_attempt < policy.max_attempts
}

fn apply_jitter(delay: u64) -> u64 {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let factor = 0.5 + (seed as f64 % 1000.0) / 1000.0;
    (delay as f64 * factor) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_returns_base_for_any_attempt() {
        let policy = RetryPolicy {
            max_attempts: 5,
            strategy: RetryStrategy::Fixed,
            base_delay_ms: 1000,
            max_delay_ms: 60000,
            jitter: false,
        };
        assert_eq!(compute_delay(&policy, 1), 1000);
        assert_eq!(compute_delay(&policy, 2), 1000);
        assert_eq!(compute_delay(&policy, 5), 1000);
    }

    #[test]
    fn linear_scales_with_attempt() {
        let policy = RetryPolicy {
            max_attempts: 5,
            strategy: RetryStrategy::Linear,
            base_delay_ms: 1000,
            max_delay_ms: 60000,
            jitter: false,
        };
        assert_eq!(compute_delay(&policy, 1), 1000);
        assert_eq!(compute_delay(&policy, 2), 2000);
        assert_eq!(compute_delay(&policy, 3), 3000);
        assert_eq!(compute_delay(&policy, 5), 5000);
    }

    #[test]
    fn exponential_doubles_each_attempt() {
        let policy = RetryPolicy {
            max_attempts: 5,
            strategy: RetryStrategy::Exponential,
            base_delay_ms: 1000,
            max_delay_ms: 60000,
            jitter: false,
        };
        assert_eq!(compute_delay(&policy, 1), 1000);
        assert_eq!(compute_delay(&policy, 2), 2000);
        assert_eq!(compute_delay(&policy, 3), 4000);
        assert_eq!(compute_delay(&policy, 4), 8000);
        assert_eq!(compute_delay(&policy, 5), 16000);
    }

    #[test]
    fn caps_at_max_delay() {
        let policy = RetryPolicy {
            max_attempts: 10,
            strategy: RetryStrategy::Exponential,
            base_delay_ms: 1000,
            max_delay_ms: 10000,
            jitter: false,
        };
        // 1000 * 2^4 = 16000, capped at 10000
        assert_eq!(compute_delay(&policy, 5), 10000);
        assert_eq!(compute_delay(&policy, 10), 10000);
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let policy = RetryPolicy {
            max_attempts: 5,
            strategy: RetryStrategy::Fixed,
            base_delay_ms: 1000,
            max_delay_ms: 60000,
            jitter: true,
        };
        for _ in 0..100 {
            let delay = compute_delay(&policy, 1);
            assert!(delay >= 500 && delay <= 1500, "delay {} out of bounds", delay);
        }
    }

    #[test]
    fn should_retry_true_for_attempt_less_than_max() {
        let policy = RetryPolicy {
            max_attempts: 5,
            strategy: RetryStrategy::Fixed,
            base_delay_ms: 1000,
            max_delay_ms: 60000,
            jitter: false,
        };
        assert!(should_retry(&policy, 0));
        assert!(should_retry(&policy, 1));
        assert!(should_retry(&policy, 4));
    }

    #[test]
    fn should_retry_false_for_attempt_equal_or_greater_than_max() {
        let policy = RetryPolicy {
            max_attempts: 5,
            strategy: RetryStrategy::Fixed,
            base_delay_ms: 1000,
            max_delay_ms: 60000,
            jitter: false,
        };
        assert!(!should_retry(&policy, 5));
        assert!(!should_retry(&policy, 6));
        assert!(!should_retry(&policy, 100));
    }
}
