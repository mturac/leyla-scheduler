// hafizada tut beni leyla

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use uuid::Uuid;

use crate::types::{DeadLetter, JobRun, LeylaJob, RunStatus};

use super::{LeylaStore, Result, RunFilter, RunPatch, StoreError};

// ── MemoryStore ───────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct Inner {
    jobs: HashMap<String, LeylaJob>,
    runs: HashMap<String, JobRun>,
    dead_letters: HashMap<String, DeadLetter>,
}

#[derive(Debug, Default)]
pub struct MemoryStore {
    inner: Mutex<Inner>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LeylaStore for MemoryStore {
    async fn upsert_job(&self, job: LeylaJob) -> Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.jobs.insert(job.id.to_string(), job);
        Ok(())
    }

    async fn get_job(&self, job_id: &str) -> Result<LeylaJob> {
        let g = self.inner.lock().unwrap();
        g.jobs
            .get(job_id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(job_id.to_string()))
    }

    async fn list_jobs(&self) -> Result<Vec<LeylaJob>> {
        let g = self.inner.lock().unwrap();
        let mut jobs: Vec<LeylaJob> = g.jobs.values().cloned().collect();
        jobs.sort_by_key(|j| j.created_at);
        Ok(jobs)
    }

    async fn remove_job(&self, job_id: &str) -> Result<()> {
        let mut g = self.inner.lock().unwrap();
        if g.jobs.remove(job_id).is_none() {
            return Err(StoreError::NotFound(job_id.to_string()));
        }
        Ok(())
    }

    async fn insert_run(&self, run: JobRun) -> Result<()> {
        let mut g = self.inner.lock().unwrap();
        let key = run.run_id.to_string();
        if g.runs.contains_key(&key) {
            return Err(StoreError::Conflict(key));
        }
        g.runs.insert(key, run);
        Ok(())
    }

    async fn claim_due_runs(
        &self,
        now: DateTime<Utc>,
        owner: &str,
        limit: usize,
    ) -> Result<Vec<JobRun>> {
        let mut g = self.inner.lock().unwrap();
        let lease_expires_at = now + ChronoDuration::seconds(30);

        let due_ids: Vec<String> = g
            .runs
            .values()
            .filter(|r| r.status == RunStatus::Scheduled && r.scheduled_for <= now)
            .take(limit)
            .map(|r| r.run_id.to_string())
            .collect();

        let mut claimed = Vec::with_capacity(due_ids.len());
        for id in due_ids {
            if let Some(run) = g.runs.get_mut(&id) {
                run.status = RunStatus::Leased;
                run.lease_owner = Some(owner.to_string());
                run.lease_expires_at = Some(lease_expires_at);
                run.updated_at = now;
                claimed.push(run.clone());
            }
        }
        Ok(claimed)
    }

    async fn update_run(&self, run_id: &str, patch: RunPatch) -> Result<()> {
        let mut g = self.inner.lock().unwrap();
        let run = g
            .runs
            .get_mut(run_id)
            .ok_or_else(|| StoreError::NotFound(run_id.to_string()))?;

        if let Some(v) = patch.status {
            run.status = v;
        }
        if let Some(v) = patch.lease_owner {
            run.lease_owner = Some(v);
        }
        if let Some(v) = patch.lease_expires_at {
            run.lease_expires_at = Some(v);
        }
        if let Some(v) = patch.dispatched_at {
            run.dispatched_at = Some(v);
        }
        if let Some(v) = patch.started_at {
            run.started_at = Some(v);
        }
        if let Some(v) = patch.heartbeat_at {
            run.heartbeat_at = Some(v);
        }
        if let Some(v) = patch.finished_at {
            run.finished_at = Some(v);
        }
        if let Some(v) = patch.attempt {
            run.attempt = v;
        }
        if let Some(v) = patch.output_json {
            run.output_json = Some(v);
        }
        if let Some(v) = patch.error_json {
            run.error_json = Some(v);
        }
        run.updated_at = Utc::now();
        Ok(())
    }

    async fn get_run(&self, run_id: &str) -> Result<JobRun> {
        let g = self.inner.lock().unwrap();
        g.runs
            .get(run_id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(run_id.to_string()))
    }

    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<JobRun>> {
        let g = self.inner.lock().unwrap();
        let mut runs: Vec<JobRun> = g
            .runs
            .values()
            .filter(|r| {
                if let Some(ref jid) = filter.job_id {
                    if r.job_id.to_string() != *jid {
                        return false;
                    }
                }
                if let Some(ref st) = filter.status {
                    if r.status != *st {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        runs.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        if let Some(limit) = filter.limit {
            let cap = limit as usize;
            if runs.len() > cap {
                runs = runs.into_iter().take(cap).collect();
            }
        }
        Ok(runs)
    }

    async fn get_stale_runs(
        &self,
        now: DateTime<Utc>,
        stale_threshold: Duration,
    ) -> Result<Vec<JobRun>> {
        let g = self.inner.lock().unwrap();
        let threshold = ChronoDuration::from_std(stale_threshold)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let cutoff = now - threshold;

        let stale: Vec<JobRun> = g
            .runs
            .values()
            .filter(|r| {
                let is_active = matches!(r.status, RunStatus::Leased | RunStatus::Running);
                if !is_active {
                    return false;
                }
                // expired lease OR updated_at older than threshold
                r.lease_expires_at.map(|exp| exp < now).unwrap_or(false)
                    || r.updated_at < cutoff
            })
            .cloned()
            .collect();

        Ok(stale)
    }

    async fn get_dead_letters(&self) -> Result<Vec<DeadLetter>> {
        let g = self.inner.lock().unwrap();
        let mut letters: Vec<DeadLetter> = g.dead_letters.values().cloned().collect();
        letters.sort_by_key(|d| d.created_at);
        Ok(letters)
    }

    async fn count_active_runs(&self, job_id: &str) -> Result<usize> {
        let g = self.inner.lock().unwrap();
        let count = g
            .runs
            .values()
            .filter(|r| {
                r.job_id.to_string() == job_id
                    && matches!(
                        r.status,
                        RunStatus::Leased | RunStatus::Dispatched | RunStatus::Running
                    )
            })
            .count();
        Ok(count)
    }

    async fn migrate(&self) -> Result<()> {
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ExecutorSpec, LeylaJob, Schedule};
    use chrono::Duration as D;

    fn make_job() -> LeylaJob {
        LeylaJob::new(
            "test-job",
            Schedule::Manual,
            ExecutorSpec::LocalHandler {
                handler_key: "k".into(),
            },
        )
    }

    fn make_run(job_id: Uuid, scheduled_for: DateTime<Utc>) -> JobRun {
        JobRun::new_scheduled(job_id, scheduled_for, 5)
    }

    #[tokio::test]
    async fn upsert_and_get_job() {
        let store = MemoryStore::new();
        let job = make_job();
        let id = job.id.to_string();
        store.upsert_job(job.clone()).await.unwrap();
        let got = store.get_job(&id).await.unwrap();
        assert_eq!(got.id, job.id);
        assert_eq!(got.name, job.name);
    }

    #[tokio::test]
    async fn remove_job_works() {
        let store = MemoryStore::new();
        let job = make_job();
        let id = job.id.to_string();
        store.upsert_job(job).await.unwrap();
        store.remove_job(&id).await.unwrap();
        let err = store.get_job(&id).await.unwrap_err();
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn claim_due_runs() {
        let store = MemoryStore::new();
        let job = make_job();
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        let past = Utc::now() - D::seconds(10);
        let run = make_run(job_id, past);
        store.insert_run(run).await.unwrap();

        let now = Utc::now();
        let claimed = store.claim_due_runs(now, "worker-1", 10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].status, RunStatus::Leased);
        assert_eq!(claimed[0].lease_owner.as_deref(), Some("worker-1"));

        let claimed2 = store.claim_due_runs(now, "worker-2", 10).await.unwrap();
        assert_eq!(claimed2.len(), 0);
    }

    #[tokio::test]
    async fn update_run_applies_patch() {
        let store = MemoryStore::new();
        let job = make_job();
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        let run = make_run(job_id, Utc::now());
        let run_id = run.run_id.to_string();
        store.insert_run(run).await.unwrap();

        let patch = RunPatch {
            status: Some(RunStatus::Running),
            ..Default::default()
        };
        store.update_run(&run_id, patch).await.unwrap();

        let got = store.get_run(&run_id).await.unwrap();
        assert_eq!(got.status, RunStatus::Running);
    }

    #[tokio::test]
    async fn count_active_runs() {
        let store = MemoryStore::new();
        let job = make_job();
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        let r1 = make_run(job_id, Utc::now());
        let r1_id = r1.run_id.to_string();
        store.insert_run(r1).await.unwrap();
        store
            .update_run(
                &r1_id,
                RunPatch {
                    status: Some(RunStatus::Running),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let r2 = make_run(job_id, Utc::now());
        let r2_id = r2.run_id.to_string();
        store.insert_run(r2).await.unwrap();
        store
            .update_run(
                &r2_id,
                RunPatch {
                    status: Some(RunStatus::Succeeded),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let count = store.count_active_runs(&job_id.to_string()).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn list_runs_with_filter() {
        let store = MemoryStore::new();
        let job = make_job();
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        for _ in 0..3 {
            let r = make_run(job_id, Utc::now());
            store.insert_run(r).await.unwrap();
        }

        let other_job = make_job();
        let other_id = other_job.id;
        store.upsert_job(other_job).await.unwrap();
        let r_other = make_run(other_id, Utc::now());
        store.insert_run(r_other).await.unwrap();

        let filter = RunFilter {
            job_id: Some(job_id.to_string()),
            ..Default::default()
        };
        let runs = store.list_runs(filter).await.unwrap();
        assert_eq!(runs.len(), 3);
        assert!(runs.iter().all(|r| r.job_id == job_id));
    }

    #[tokio::test]
    async fn get_stale_runs() {
        let store = MemoryStore::new();
        let job = make_job();
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        let run = make_run(job_id, Utc::now());
        let run_id = run.run_id.to_string();
        store.insert_run(run).await.unwrap();

        let expired_lease = Utc::now() - D::seconds(5);
        store
            .update_run(
                &run_id,
                RunPatch {
                    status: Some(RunStatus::Leased),
                    lease_expires_at: Some(expired_lease),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let now = Utc::now();
        let stale = store
            .get_stale_runs(now, Duration::from_secs(60))
            .await
            .unwrap();
        assert!(!stale.is_empty());
        assert!(stale.iter().any(|r| r.run_id.to_string() == run_id));
    }
}
