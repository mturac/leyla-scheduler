use chrono::{DateTime, Utc};
use std::str::FromStr;
use std::sync::Arc;

use crate::clock::Clock;
use crate::store::LeylaStore;
use crate::types::{JobRun, Schedule};

pub struct DueRunMaterializer {
    store: Arc<dyn LeylaStore>,
    clock: Arc<dyn Clock>,
}

impl DueRunMaterializer {
    pub fn new(store: Arc<dyn LeylaStore>, clock: Arc<dyn Clock>) -> Self {
        Self { store, clock }
    }

    pub async fn materialize(&self) -> crate::store::Result<usize> {
        let now = self.clock.now();
        let jobs = self.store.list_jobs().await?;
        let mut count = 0;

        for mut job in jobs {
            // Skip disabled jobs
            if !job.enabled {
                continue;
            }
            // Skip Manual schedule
            if matches!(job.schedule, Schedule::Manual) {
                continue;
            }
            // Skip if no next_run_at
            let next_run_at = match job.next_run_at {
                Some(t) => t,
                None => continue,
            };
            // Skip if not yet due
            if next_run_at > now {
                continue;
            }

            // Create a scheduled run
            let run = JobRun::new_scheduled(job.id, next_run_at, job.retry.max_attempts);
            self.store.insert_run(run).await?;
            count += 1;

            // Advance next_run_at
            job.next_run_at = compute_next_run(&job.schedule, now);
            job.last_run_at = Some(now);
            job.updated_at = now;
            self.store.upsert_job(job).await?;
        }

        Ok(count)
    }
}

pub fn compute_next_run(schedule: &Schedule, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match schedule {
        Schedule::Manual => None,
        Schedule::Once { .. } => None,
        Schedule::Interval { every_ms, .. } => {
            let delta = chrono::Duration::milliseconds(*every_ms as i64);
            Some(after + delta)
        }
        Schedule::Cron { expression, .. } => {
            let parsed = cron::Schedule::from_str(expression).ok()?;
            parsed.after(&after).next()
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::FakeClock;
    use crate::store::memory::MemoryStore;
    use crate::types::{ExecutorSpec, IntervalAnchor, LeylaJob, Schedule};
    use chrono::TimeZone;

    fn make_store() -> Arc<MemoryStore> {
        Arc::new(MemoryStore::new())
    }

    fn make_clock(ts: DateTime<Utc>) -> Arc<FakeClock> {
        Arc::new(FakeClock::new(ts))
    }

    fn executor() -> ExecutorSpec {
        ExecutorSpec::LocalHandler {
            handler_key: "k".into(),
        }
    }

    #[tokio::test]
    async fn does_not_materialize_manual_jobs() {
        let store = make_store();
        let now = Utc::now();
        let clock = make_clock(now);

        let mut job = LeylaJob::new("manual-job", Schedule::Manual, executor());
        job.next_run_at = Some(now - chrono::Duration::seconds(10));
        store.upsert_job(job).await.unwrap();

        let mat = DueRunMaterializer::new(store.clone(), clock);
        let count = mat.materialize().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn materializes_once_job_when_due() {
        let store = make_store();
        let now = Utc::now();
        let clock = make_clock(now);

        let at = now - chrono::Duration::seconds(5);
        let mut job = LeylaJob::new("once-job", Schedule::Once { at }, executor());
        job.next_run_at = Some(at);
        store.upsert_job(job).await.unwrap();

        let mat = DueRunMaterializer::new(store.clone(), clock);
        let count = mat.materialize().await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn does_not_materialize_disabled_jobs() {
        let store = make_store();
        let now = Utc::now();
        let clock = make_clock(now);

        let mut job = LeylaJob::new(
            "disabled-job",
            Schedule::Interval {
                every_ms: 1000,
                anchor: IntervalAnchor::WallClock,
            },
            executor(),
        );
        job.enabled = false;
        job.next_run_at = Some(now - chrono::Duration::seconds(5));
        store.upsert_job(job).await.unwrap();

        let mat = DueRunMaterializer::new(store.clone(), clock);
        let count = mat.materialize().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn advances_next_run_for_interval_job() {
        let store = make_store();
        let now = Utc::now();
        let clock = make_clock(now);

        let every_ms = 60_000u64;
        let past = now - chrono::Duration::seconds(5);
        let mut job = LeylaJob::new(
            "interval-job",
            Schedule::Interval {
                every_ms,
                anchor: IntervalAnchor::WallClock,
            },
            executor(),
        );
        let job_id = job.id.to_string();
        job.next_run_at = Some(past);
        store.upsert_job(job).await.unwrap();

        let mat = DueRunMaterializer::new(store.clone(), clock);
        let count = mat.materialize().await.unwrap();
        assert_eq!(count, 1);

        let updated = store.get_job(&job_id).await.unwrap();
        let expected_next = now + chrono::Duration::milliseconds(every_ms as i64);
        assert_eq!(updated.next_run_at, Some(expected_next));
    }

    #[test]
    fn compute_next_run_manual_returns_none() {
        let now = Utc::now();
        assert!(compute_next_run(&Schedule::Manual, now).is_none());
    }

    #[test]
    fn compute_next_run_once_returns_none() {
        let now = Utc::now();
        assert!(compute_next_run(&Schedule::Once { at: now }, now).is_none());
    }

    #[test]
    fn compute_next_run_interval_adds_ms() {
        let now = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let result = compute_next_run(
            &Schedule::Interval {
                every_ms: 5000,
                anchor: IntervalAnchor::WallClock,
            },
            now,
        );
        assert_eq!(result, Some(now + chrono::Duration::milliseconds(5000)));
    }

    #[test]
    fn compute_next_run_cron_returns_next() {
        let now = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let result = compute_next_run(
            &Schedule::Cron {
                expression: "0 * * * * *".to_string(), // every minute
                timezone: None,
            },
            now,
        );
        assert!(result.is_some());
        assert!(result.unwrap() > now);
    }
}
