use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::clock::Clock;
use crate::scheduler::claim::LeaseManager;
use crate::scheduler::due::DueRunMaterializer;
use crate::scheduler::recovery::RecoveryManager;
use crate::store::{LeylaStore, RunFilter};
use crate::types::{JobRun, LeylaJob, RunStatus, Schedule};

// ── EngineConfig ──────────────────────────────────────────────────────────────

pub struct EngineConfig {
    pub store: Arc<dyn LeylaStore>,
    pub clock: Arc<dyn Clock>,
    pub instance_id: String,
    pub poll_interval_ms: u64,
    pub claim_batch_size: u32,
    pub lease_ttl_ms: u64,
    pub default_timeout_ms: u64,
    pub stale_heartbeat_ms: u64,
    pub max_output_bytes: usize,
}

impl EngineConfig {
    pub fn new(store: Arc<dyn LeylaStore>, clock: Arc<dyn Clock>, instance_id: impl Into<String>) -> Self {
        Self {
            store,
            clock,
            instance_id: instance_id.into(),
            poll_interval_ms: 1000,
            claim_batch_size: 50,
            lease_ttl_ms: 30_000,
            default_timeout_ms: 300_000,
            stale_heartbeat_ms: 60_000,
            max_output_bytes: 65_536,
        }
    }
}

// ── LeylaEngine ───────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct LeylaEngine {
    store: Arc<dyn LeylaStore>,
    clock: Arc<dyn Clock>,
    instance_id: String,
    poll_interval_ms: u64,
    claim_batch_size: u32,
    lease_ttl_ms: u64,
    stale_heartbeat_ms: u64,
    cancel: CancellationToken,
}

impl LeylaEngine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            store: config.store,
            clock: config.clock,
            instance_id: config.instance_id,
            poll_interval_ms: config.poll_interval_ms,
            claim_batch_size: config.claim_batch_size,
            lease_ttl_ms: config.lease_ttl_ms,
            stale_heartbeat_ms: config.stale_heartbeat_ms,
            cancel: CancellationToken::new(),
        }
    }

    pub async fn start(&self) -> crate::store::Result<()> {
        let poll = Duration::from_millis(self.poll_interval_ms);
        let recovery_poll = Duration::from_millis(self.poll_interval_ms * 5);

        let materializer = DueRunMaterializer::new(self.store.clone(), self.clock.clone());
        let lease_manager = LeaseManager::new(
            self.store.clone(),
            self.clock.clone(),
            self.instance_id.clone(),
            self.claim_batch_size,
            self.lease_ttl_ms,
        );
        let recovery_manager = RecoveryManager::new(
            self.store.clone(),
            self.clock.clone(),
            Duration::from_millis(self.stale_heartbeat_ms),
        );

        let cancel = self.cancel.clone();

        let mut poll_ticker = tokio::time::interval(poll);
        let mut recovery_ticker = tokio::time::interval(recovery_poll);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    // hosçakal leyla
                    info!("LeylaEngine shutting down");
                    break;
                }
                _ = poll_ticker.tick() => {
                    // Loop A: materialize due runs
                    if let Err(e) = materializer.materialize().await {
                        warn!("materialize error: {}", e);
                    }
                    // Loop B: claim batch
                    if let Err(e) = lease_manager.claim_batch().await {
                        warn!("claim_batch error: {}", e);
                    }
                }
                _ = recovery_ticker.tick() => {
                    // Loop C: recovery
                    if let Err(e) = recovery_manager.recover().await {
                        warn!("recovery error: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }

    pub async fn schedule(&self, job: LeylaJob) -> crate::store::Result<()> {
        self.store.upsert_job(job).await
    }

    pub async fn unschedule(&self, job_id: &str) -> crate::store::Result<()> {
        self.store.remove_job(job_id).await
    }

    pub async fn pause(&self, job_id: &str) -> crate::store::Result<()> {
        let mut job = self.store.get_job(job_id).await?;
        job.enabled = false;
        self.store.upsert_job(job).await
    }

    pub async fn resume(&self, job_id: &str) -> crate::store::Result<()> {
        let mut job = self.store.get_job(job_id).await?;
        job.enabled = true;
        self.store.upsert_job(job).await
    }

    pub async fn trigger(
        &self,
        job_id: &str,
        input: Option<serde_json::Value>,
    ) -> crate::store::Result<String> {
        let job = self.store.get_job(job_id).await?;
        let now = self.clock.now();
        let mut run = JobRun::new_scheduled(job.id, now, job.retry.max_attempts);
        run.input_json = input;
        let run_id = run.run_id.to_string();
        self.store.insert_run(run).await?;
        Ok(run_id)
    }

    pub async fn get_job(&self, job_id: &str) -> crate::store::Result<Option<LeylaJob>> {
        match self.store.get_job(job_id).await {
            Ok(job) => Ok(Some(job)),
            Err(crate::store::StoreError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn list_jobs(&self) -> crate::store::Result<Vec<LeylaJob>> {
        self.store.list_jobs().await
    }

    pub async fn list_runs(&self, filter: RunFilter) -> crate::store::Result<Vec<JobRun>> {
        self.store.list_runs(filter).await
    }

    pub async fn get_run(&self, run_id: &str) -> crate::store::Result<Option<JobRun>> {
        match self.store.get_run(run_id).await {
            Ok(run) => Ok(Some(run)),
            Err(crate::store::StoreError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::FakeClock;
    use crate::store::memory::MemoryStore;
    use crate::types::{ExecutorSpec, LeylaJob, Schedule};
    use chrono::Utc;
    use std::sync::Arc;

    fn make_engine() -> LeylaEngine {
        let store = Arc::new(MemoryStore::new());
        let clock = Arc::new(FakeClock::new(Utc::now()));
        let config = EngineConfig::new(store, clock, "test-instance");
        LeylaEngine::new(config)
    }

    fn executor() -> ExecutorSpec {
        ExecutorSpec::LocalHandler {
            handler_key: "k".into(),
        }
    }

    #[tokio::test]
    async fn engine_creates_and_stops() {
        let engine = make_engine();
        let engine2 = engine.clone();

        let handle = tokio::spawn(async move {
            engine2.start().await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        engine.stop();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn schedule_and_trigger_manual_job() {
        let engine = make_engine();
        let job = LeylaJob::new("manual-job", Schedule::Manual, executor());
        let job_id = job.id.to_string();

        engine.schedule(job).await.unwrap();
        let run_id = engine.trigger(&job_id, None).await.unwrap();

        let run = engine.get_run(&run_id).await.unwrap();
        assert!(run.is_some());
        assert_eq!(run.unwrap().status, RunStatus::Scheduled);
    }

    #[tokio::test]
    async fn pause_and_resume_job() {
        let engine = make_engine();
        let job = LeylaJob::new("pausable-job", Schedule::Manual, executor());
        let job_id = job.id.to_string();

        engine.schedule(job).await.unwrap();

        engine.pause(&job_id).await.unwrap();
        let paused = engine.get_job(&job_id).await.unwrap().unwrap();
        assert!(!paused.enabled);

        engine.resume(&job_id).await.unwrap();
        let resumed = engine.get_job(&job_id).await.unwrap().unwrap();
        assert!(resumed.enabled);
    }

    #[tokio::test]
    async fn get_and_list_jobs() {
        let engine = make_engine();
        let job1 = LeylaJob::new("job-1", Schedule::Manual, executor());
        let job2 = LeylaJob::new("job-2", Schedule::Manual, executor());
        let id1 = job1.id.to_string();

        engine.schedule(job1).await.unwrap();
        engine.schedule(job2).await.unwrap();

        let found = engine.get_job(&id1).await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "job-1");

        let all = engine.list_jobs().await.unwrap();
        assert_eq!(all.len(), 2);
    }
}
