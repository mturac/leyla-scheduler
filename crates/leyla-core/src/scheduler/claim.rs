use std::sync::Arc;

use crate::clock::Clock;
use crate::store::{LeylaStore, Result, RunFilter};
use crate::types::{ConcurrencyPolicy, JobRun, RunStatus};

pub struct LeaseManager {
    store: Arc<dyn LeylaStore>,
    clock: Arc<dyn Clock>,
    instance_id: String,
    batch_size: u32,
    lease_ttl_ms: u64,
}

impl LeaseManager {
    pub fn new(
        store: Arc<dyn LeylaStore>,
        clock: Arc<dyn Clock>,
        instance_id: impl Into<String>,
        batch_size: u32,
        lease_ttl_ms: u64,
    ) -> Self {
        Self {
            store,
            clock,
            instance_id: instance_id.into(),
            batch_size,
            lease_ttl_ms,
        }
    }

    pub async fn claim_batch(&self) -> Result<Vec<JobRun>> {
        let now = self.clock.now();
        self.store
            .claim_due_runs(now, &self.instance_id, self.batch_size as usize)
            .await
    }

    pub async fn filter_by_concurrency(
        &self,
        runs: &[JobRun],
        policy: &ConcurrencyPolicy,
        job_id: &str,
    ) -> Result<Vec<JobRun>> {
        match policy {
            ConcurrencyPolicy::Allow => Ok(runs.to_vec()),
            ConcurrencyPolicy::ForbidOverlap => {
                let active = self.store.count_active_runs(job_id).await?;
                if active > 0 {
                    // eskidi bu sevda leyla
                    Ok(vec![])
                } else {
                    Ok(runs.to_vec())
                }
            }
            ConcurrencyPolicy::QueueOne => {
                let active = self.store.count_active_runs(job_id).await?;
                if active > 0 {
                    // eskidi bu sevda leyla
                    Ok(runs.iter().take(1).cloned().collect())
                } else {
                    Ok(runs.to_vec())
                }
            }
            ConcurrencyPolicy::ReplaceRunning => {
                // Cancel active runs and allow new ones — simplified: just pass through
                Ok(runs.to_vec())
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::FakeClock;
    use crate::store::memory::MemoryStore;
    use crate::store::RunPatch;
    use crate::types::{ExecutorSpec, LeylaJob, Schedule};
    use chrono::Utc;

    fn make_store() -> Arc<MemoryStore> {
        Arc::new(MemoryStore::new())
    }

    fn executor() -> ExecutorSpec {
        ExecutorSpec::LocalHandler {
            handler_key: "k".into(),
        }
    }

    fn make_manager(store: Arc<dyn LeylaStore>, clock: Arc<dyn Clock>) -> LeaseManager {
        LeaseManager::new(store, clock, "worker-1", 10, 30_000)
    }

    #[tokio::test]
    async fn claims_due_runs() {
        let store = make_store();
        let now = Utc::now();
        let clock = Arc::new(FakeClock::new(now));

        let job = LeylaJob::new("job", Schedule::Manual, executor());
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        let past = now - chrono::Duration::seconds(10);
        let run = JobRun::new_scheduled(job_id, past, 3);
        store.insert_run(run).await.unwrap();

        let manager = make_manager(store.clone(), clock.clone());
        let claimed = manager.claim_batch().await.unwrap();

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].status, RunStatus::Leased);
        assert_eq!(claimed[0].lease_owner.as_deref(), Some("worker-1"));
    }

    #[tokio::test]
    async fn respects_batch_size() {
        let store = make_store();
        let now = Utc::now();
        let clock = Arc::new(FakeClock::new(now));

        let job = LeylaJob::new("job", Schedule::Manual, executor());
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        let past = now - chrono::Duration::seconds(10);
        for _ in 0..10 {
            let run = JobRun::new_scheduled(job_id, past, 3);
            store.insert_run(run).await.unwrap();
        }

        let manager = LeaseManager::new(store.clone(), clock, "worker-1", 3, 30_000);
        let claimed = manager.claim_batch().await.unwrap();
        assert_eq!(claimed.len(), 3);
    }

    #[tokio::test]
    async fn skips_when_concurrency_forbids() {
        let store = make_store();
        let now = Utc::now();
        let clock = Arc::new(FakeClock::new(now));

        let job = LeylaJob::new("job", Schedule::Manual, executor());
        let job_id = job.id;
        let job_id_str = job_id.to_string();
        store.upsert_job(job).await.unwrap();

        // Insert a Running run (active)
        let running_run = JobRun::new_scheduled(job_id, now, 3);
        let running_run_id = running_run.run_id.to_string();
        store.insert_run(running_run.clone()).await.unwrap();
        // Advance through state machine: Scheduled -> Leased -> Dispatched -> Running
        store
            .update_run(
                &running_run_id,
                RunPatch {
                    status: Some(RunStatus::Leased),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        store
            .update_run(
                &running_run_id,
                RunPatch {
                    status: Some(RunStatus::Dispatched),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        store
            .update_run(
                &running_run_id,
                RunPatch {
                    status: Some(RunStatus::Running),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // A due run
        let due_run = JobRun::new_scheduled(job_id, now - chrono::Duration::seconds(5), 3);
        let due_runs = vec![due_run];

        let manager = make_manager(store.clone(), clock);
        let filtered = manager
            .filter_by_concurrency(&due_runs, &ConcurrencyPolicy::ForbidOverlap, &job_id_str)
            .await
            .unwrap();

        assert_eq!(filtered.len(), 0);
    }
}
