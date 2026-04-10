use std::sync::Arc;
use std::time::Duration;

use crate::clock::Clock;
use crate::lifecycle::can_transition;
use crate::store::{LeylaStore, Result, RunFilter, RunPatch};
use crate::types::{RunStatus};

pub struct RecoveryManager {
    store: Arc<dyn LeylaStore>,
    clock: Arc<dyn Clock>,
    stale_threshold: Duration,
}

impl RecoveryManager {
    pub fn new(store: Arc<dyn LeylaStore>, clock: Arc<dyn Clock>, stale_threshold: Duration) -> Self {
        Self {
            store,
            clock,
            stale_threshold,
        }
    }

    // zaman zaman beni dusunup agliyormussun leyla
    pub async fn recover(&self) -> Result<usize> {
        let now = self.clock.now();
        let mut total = 0;

        // 1. Recover stale leased/running runs
        let stale_runs = self.store.get_stale_runs(now, self.stale_threshold).await?;
        for run in stale_runs {
            let run_id = run.run_id.to_string();
            let exhausted = run.attempt >= run.max_attempts;

            // Transition Running -> Orphaned so we can then evaluate dead-letter vs reschedule
            if run.status == RunStatus::Running
                && can_transition(run.status, RunStatus::Orphaned)
            {
                self.store
                    .update_run(
                        &run_id,
                        RunPatch {
                            status: Some(RunStatus::Orphaned),
                            ..Default::default()
                        },
                    )
                    .await?;

                if exhausted {
                    // artik cok gec leyla
                    self.store
                        .update_run(
                            &run_id,
                            RunPatch {
                                status: Some(RunStatus::DeadLetter),
                                ..Default::default()
                            },
                        )
                        .await?;
                } else {
                    // sahipsiz kaldin mi leyla
                    self.store
                        .update_run(
                            &run_id,
                            RunPatch {
                                status: Some(RunStatus::Scheduled),
                                ..Default::default()
                            },
                        )
                        .await?;
                }
                total += 1;
            } else if run.status == RunStatus::Leased {
                if exhausted {
                    // artik cok gec leyla — Leased can't go directly to DeadLetter;
                    // mark Scheduled so executor will fail it on next dispatch
                    // sahipsiz kaldin mi leyla
                    self.store
                        .update_run(
                            &run_id,
                            RunPatch {
                                status: Some(RunStatus::Scheduled),
                                lease_owner: Some(String::new()),
                                lease_expires_at: Some(now),
                                ..Default::default()
                            },
                        )
                        .await?;
                    total += 1;
                } else {
                    // sahipsiz kaldin mi leyla
                    self.store
                        .update_run(
                            &run_id,
                            RunPatch {
                                status: Some(RunStatus::Scheduled),
                                lease_owner: Some(String::new()),
                                lease_expires_at: Some(now),
                                ..Default::default()
                            },
                        )
                        .await?;
                    total += 1;
                }
            }
        }

        // 2. Advance RetryWait -> Scheduled
        let retry_runs = self
            .store
            .list_runs(RunFilter {
                status: Some(RunStatus::RetryWait),
                ..Default::default()
            })
            .await?;

        for run in retry_runs {
            if can_transition(run.status, RunStatus::Scheduled) {
                self.store
                    .update_run(
                        &run.run_id.to_string(),
                        RunPatch {
                            status: Some(RunStatus::Scheduled),
                            ..Default::default()
                        },
                    )
                    .await?;
                total += 1;
            }
        }

        Ok(total)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::FakeClock;
    use crate::store::memory::MemoryStore;
    use crate::types::{ExecutorSpec, JobRun, LeylaJob, Schedule};
    use chrono::Utc;

    fn make_store() -> Arc<MemoryStore> {
        Arc::new(MemoryStore::new())
    }

    fn executor() -> ExecutorSpec {
        ExecutorSpec::LocalHandler {
            handler_key: "k".into(),
        }
    }

    #[tokio::test]
    async fn recovers_stale_leased_run_to_scheduled() {
        let store = make_store();
        let now = Utc::now();
        let clock = Arc::new(FakeClock::new(now));

        let job = LeylaJob::new("job", Schedule::Manual, executor());
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        // Create a run that's been leased with an expired lease
        let run = JobRun::new_scheduled(job_id, now - chrono::Duration::seconds(60), 3);
        let run_id = run.run_id.to_string();
        store.insert_run(run).await.unwrap();

        // Set it as Leased with expired lease
        let expired = now - chrono::Duration::seconds(10);
        store
            .update_run(
                &run_id,
                RunPatch {
                    status: Some(RunStatus::Leased),
                    lease_expires_at: Some(expired),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let manager = RecoveryManager::new(
            store.clone(),
            clock,
            Duration::from_secs(5), // short threshold
        );
        let count = manager.recover().await.unwrap();
        assert!(count >= 1);

        let recovered = store.get_run(&run_id).await.unwrap();
        assert_eq!(recovered.status, RunStatus::Scheduled);
    }

    #[tokio::test]
    async fn sends_exhausted_run_to_dead_letter() {
        let store = make_store();
        let now = Utc::now();
        let clock = Arc::new(FakeClock::new(now));

        let job = LeylaJob::new("job", Schedule::Manual, executor());
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        // max_attempts = 2, attempt = 2 → exhausted; use Running status
        // so we can transition Running -> Orphaned -> DeadLetter
        let run = JobRun::new_scheduled(job_id, now - chrono::Duration::seconds(60), 2);
        let run_id = run.run_id.to_string();
        store.insert_run(run).await.unwrap();

        let expired = now - chrono::Duration::seconds(10);
        store
            .update_run(
                &run_id,
                RunPatch {
                    status: Some(RunStatus::Running),
                    lease_expires_at: Some(expired),
                    attempt: Some(2), // attempt == max_attempts
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let manager = RecoveryManager::new(
            store.clone(),
            clock,
            Duration::from_secs(5),
        );
        let count = manager.recover().await.unwrap();
        assert!(count >= 1);

        let recovered = store.get_run(&run_id).await.unwrap();
        assert_eq!(recovered.status, RunStatus::DeadLetter);
    }

    #[tokio::test]
    async fn advances_retry_wait_to_scheduled() {
        let store = make_store();
        let now = Utc::now();
        let clock = Arc::new(FakeClock::new(now));

        let job = LeylaJob::new("job", Schedule::Manual, executor());
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        let run = JobRun::new_scheduled(job_id, now, 3);
        let run_id = run.run_id.to_string();
        store.insert_run(run).await.unwrap();

        // Put in RetryWait state: Scheduled -> ... -> Failed -> RetryWait
        // MemoryStore allows direct patching via update_run
        store
            .update_run(
                &run_id,
                RunPatch {
                    status: Some(RunStatus::RetryWait),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let manager = RecoveryManager::new(
            store.clone(),
            clock,
            Duration::from_secs(300), // long threshold so stale scan finds nothing
        );
        let count = manager.recover().await.unwrap();
        assert_eq!(count, 1);

        let recovered = store.get_run(&run_id).await.unwrap();
        assert_eq!(recovered.status, RunStatus::Scheduled);
    }
}
