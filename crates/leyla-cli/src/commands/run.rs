use anyhow::Result;
use clap::Subcommand;
use leyla_core::{
    store::{LeylaStore, RunFilter, RunPatch},
    types::{JobRun, RunStatus},
};
use std::sync::Arc;

use crate::output::print_runs_table;

#[derive(Subcommand)]
pub enum RunAction {
    /// Manually trigger a job
    Trigger {
        /// Job ID
        job_id: String,
    },
    /// Inspect a specific run
    Inspect {
        /// Run ID
        run_id: String,
    },
    /// Retry a failed/dead-letter run
    Retry {
        /// Run ID
        run_id: String,
    },
    /// Cancel a scheduled/leased run
    Cancel {
        /// Run ID
        run_id: String,
    },
    /// List runs with optional filters
    List {
        /// Filter by job ID
        #[arg(long)]
        job: Option<String>,
        /// Filter by status (e.g. scheduled, running, succeeded)
        #[arg(long)]
        status: Option<String>,
        /// Maximum number of results
        #[arg(long, default_value = "20")]
        limit: u32,
    },
}

pub async fn handle(action: RunAction, store: Arc<dyn LeylaStore>, json: bool) -> Result<()> {
    match action {
        RunAction::Trigger { job_id } => {
            let job = store
                .get_job(&job_id)
                .await
                .map_err(|_| anyhow::anyhow!("Job not found: {job_id}"))?;

            let run = JobRun::new_scheduled(job.id, chrono::Utc::now(), job.retry.max_attempts);
            let run_id = run.run_id.to_string();
            store.insert_run(run).await?;

            if json {
                println!("{{\"run_id\": \"{run_id}\"}}");
            } else {
                println!("Run triggered: {run_id}");
            }
        }

        RunAction::Inspect { run_id } => {
            let run = store
                .get_run(&run_id)
                .await
                .map_err(|_| anyhow::anyhow!("Run not found: {run_id}"))?;

            if json {
                println!("{}", serde_json::to_string_pretty(&run)?);
            } else {
                println!("Run ID:       {}", run.run_id);
                println!("Job ID:       {}", run.job_id);
                println!("Status:       {:?}", run.status);
                println!("Attempt:      {}/{}", run.attempt, run.max_attempts);
                println!("Scheduled:    {}", run.scheduled_for);
                println!("Started:      {:?}", run.started_at);
                println!("Finished:     {:?}", run.finished_at);
                if let Some(err) = &run.error_json {
                    println!("Error:        {}", err);
                }
            }
        }

        RunAction::Retry { run_id } => {
            let run = store
                .get_run(&run_id)
                .await
                .map_err(|_| anyhow::anyhow!("Run not found: {run_id}"))?;

            // Allow retry from Failed or DeadLetter
            match run.status {
                RunStatus::Failed | RunStatus::DeadLetter | RunStatus::Cancelled => {}
                s => anyhow::bail!("Cannot retry run in status {:?}", s),
            }

            store
                .update_run(
                    &run_id,
                    RunPatch {
                        status: Some(RunStatus::Scheduled),
                        ..Default::default()
                    },
                )
                .await?;

            println!("Run rescheduled: {run_id}");
        }

        RunAction::Cancel { run_id } => {
            let run = store
                .get_run(&run_id)
                .await
                .map_err(|_| anyhow::anyhow!("Run not found: {run_id}"))?;

            match run.status {
                RunStatus::Scheduled | RunStatus::Leased => {}
                s => anyhow::bail!("Cannot cancel run in status {:?}", s),
            }

            store
                .update_run(
                    &run_id,
                    RunPatch {
                        status: Some(RunStatus::Cancelled),
                        finished_at: Some(chrono::Utc::now()),
                        ..Default::default()
                    },
                )
                .await?;

            println!("Run cancelled: {run_id}");
        }

        RunAction::List { job, status, limit } => {
            let status_filter = if let Some(s) = status {
                // deserialize from quoted string e.g. "\"scheduled\""
                let quoted = format!("\"{}\"", s);
                Some(
                    serde_json::from_str::<RunStatus>(&quoted)
                        .map_err(|_| anyhow::anyhow!("Unknown status: {s}"))?,
                )
            } else {
                None
            };

            let filter = RunFilter {
                job_id: job,
                status: status_filter,
                limit: Some(limit),
            };

            let runs = store.list_runs(filter).await?;

            if json {
                println!("{}", serde_json::to_string_pretty(&runs)?);
            } else {
                print_runs_table(&runs);
            }
        }
    }

    Ok(())
}
