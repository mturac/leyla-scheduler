use chrono::Utc;
use std::sync::Arc;

use leyla_core::clock::FakeClock;
use leyla_core::engine::{EngineConfig, LeylaEngine};
use leyla_core::scheduler::due::DueRunMaterializer;
use leyla_core::store::memory::MemoryStore;
use leyla_core::store::LeylaStore;
use leyla_core::types::{ExecutorSpec, IntervalAnchor, LeylaJob, RunStatus, Schedule};

fn make_engine(store: Arc<MemoryStore>, clock: Arc<FakeClock>) -> LeylaEngine {
    let config = EngineConfig::new(store, clock, "test-instance");
    LeylaEngine::new(config)
}

fn executor() -> ExecutorSpec {
    ExecutorSpec::LocalHandler {
        handler_key: "test-handler".into(),
    }
}

#[tokio::test]
async fn full_lifecycle_schedule_trigger_claim() {
    let now = Utc::now();
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(FakeClock::new(now));
    let engine = make_engine(store.clone(), clock.clone());

    // Schedule a manual job
    let job = LeylaJob::new("manual-test-job", Schedule::Manual, executor());
    let job_id = job.id.to_string();
    engine.schedule(job).await.unwrap();

    // Trigger a run manually
    let run_id = engine.trigger(&job_id, None).await.unwrap();

    // Verify run was created with status Scheduled
    let run = engine.get_run(&run_id).await.unwrap().expect("run should exist");
    assert_eq!(run.status, RunStatus::Scheduled, "newly triggered run should be Scheduled");
    assert_eq!(run.job_id.to_string(), job_id);

    // Pause job — enabled should flip to false
    engine.pause(&job_id).await.unwrap();
    let job_after_pause = engine.get_job(&job_id).await.unwrap().expect("job should exist");
    assert!(!job_after_pause.enabled, "job should be disabled after pause");

    // Resume job — enabled should flip back to true
    engine.resume(&job_id).await.unwrap();
    let job_after_resume = engine.get_job(&job_id).await.unwrap().expect("job should exist");
    assert!(job_after_resume.enabled, "job should be enabled after resume");
}

#[tokio::test]
async fn interval_job_materializes_and_advances() {
    let now = Utc::now();
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(FakeClock::new(now));

    let every_ms = 60_000u64;
    // Place next_run_at 10 seconds in the past so it is due
    let past = now - chrono::Duration::seconds(10);

    let mut job = LeylaJob::new(
        "interval-test-job",
        Schedule::Interval {
            every_ms,
            anchor: IntervalAnchor::WallClock,
        },
        executor(),
    );
    let job_id = job.id.to_string();
    job.next_run_at = Some(past);
    store.upsert_job(job).await.unwrap();

    let materializer = DueRunMaterializer::new(store.clone(), clock.clone());
    let count = materializer.materialize().await.unwrap();
    assert_eq!(count, 1, "exactly one run should have been materialized");

    // Verify a run exists for the job
    let runs = store
        .list_runs(leyla_core::store::RunFilter {
            job_id: Some(job_id.clone()),
            status: None,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(runs.len(), 1, "one run should be in the store");
    assert_eq!(runs[0].status, RunStatus::Scheduled);

    // Verify next_run_at was advanced past now
    let updated_job = store.get_job(&job_id).await.unwrap();
    let next = updated_job.next_run_at.expect("next_run_at should be set after materialize");
    assert!(next > now, "next_run_at should be advanced past current time");
    // Should be approximately now + every_ms
    let expected_next = now + chrono::Duration::milliseconds(every_ms as i64);
    assert_eq!(next, expected_next, "next_run_at should equal now + every_ms");
}
