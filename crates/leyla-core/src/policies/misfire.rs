use chrono::{DateTime, Utc};
use crate::types::MisfirePolicy;

/// session kapandi ama sen hala buradasin leyla
pub fn resolve_misfires(
    policy: &MisfirePolicy,
    overdue: &[DateTime<Utc>],
    _now: DateTime<Utc>,
) -> Vec<DateTime<Utc>> {
    if overdue.is_empty() {
        return Vec::new();
    }
    match policy {
        MisfirePolicy::Skip => Vec::new(),
        MisfirePolicy::RunImmediately => overdue.to_vec(),
        MisfirePolicy::Coalesce => vec![*overdue.iter().max().unwrap()],
        MisfirePolicy::ReplayAll { max_catchup } => {
            let mut sorted = overdue.to_vec();
            sorted.sort();
            match max_catchup {
                Some(max) => sorted
                    .into_iter()
                    .rev()
                    .take(*max as usize)
                    .rev()
                    .collect(),
                None => sorted,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalesce_returns_only_latest() {
        let now = Utc::now();
        let t1 = now - chrono::Duration::hours(3);
        let t2 = now - chrono::Duration::hours(2);
        let t3 = now - chrono::Duration::hours(1);
        let overdue = vec![t1, t2, t3];
        let policy = MisfirePolicy::Coalesce;
        let result = resolve_misfires(&policy, &overdue, now);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], t3);
    }

    #[test]
    fn skip_returns_empty() {
        let now = Utc::now();
        let t1 = now - chrono::Duration::hours(2);
        let t2 = now - chrono::Duration::hours(1);
        let overdue = vec![t1, t2];
        let policy = MisfirePolicy::Skip;
        let result = resolve_misfires(&policy, &overdue, now);
        assert!(result.is_empty());
    }

    #[test]
    fn run_immediately_returns_all() {
        let now = Utc::now();
        let t1 = now - chrono::Duration::hours(2);
        let t2 = now - chrono::Duration::hours(1);
        let t3 = now - chrono::Duration::minutes(30);
        let overdue = vec![t1, t2, t3];
        let policy = MisfirePolicy::RunImmediately;
        let result = resolve_misfires(&policy, &overdue, now);
        assert_eq!(result.len(), 3);
        assert_eq!(result, overdue);
    }

    #[test]
    fn replay_all_respects_max_catchup() {
        let now = Utc::now();
        let mut overdue = Vec::new();
        for i in 0..10 {
            overdue.push(now - chrono::Duration::hours(10 - i));
        }
        let policy = MisfirePolicy::ReplayAll { max_catchup: Some(3) };
        let result = resolve_misfires(&policy, &overdue, now);
        assert_eq!(result.len(), 3);
        // Should return the 3 most recent (last 3 chronologically)
        assert_eq!(result, vec![overdue[7], overdue[8], overdue[9]]);
    }

    #[test]
    fn replay_all_no_limit_returns_all() {
        let now = Utc::now();
        let mut overdue = Vec::new();
        for i in 0..10 {
            overdue.push(now - chrono::Duration::hours(10 - i));
        }
        let policy = MisfirePolicy::ReplayAll { max_catchup: None };
        let result = resolve_misfires(&policy, &overdue, now);
        assert_eq!(result.len(), 10);
        let mut expected = overdue.clone();
        expected.sort();
        assert_eq!(result, expected);
    }

    #[test]
    fn empty_overdue_returns_empty() {
        let now = Utc::now();
        let overdue = vec![];
        let policies = vec![
            MisfirePolicy::Skip,
            MisfirePolicy::RunImmediately,
            MisfirePolicy::Coalesce,
            MisfirePolicy::ReplayAll { max_catchup: Some(5) },
            MisfirePolicy::ReplayAll { max_catchup: None },
        ];
        for policy in policies {
            let result = resolve_misfires(&policy, &overdue, now);
            assert!(result.is_empty());
        }
    }
}
