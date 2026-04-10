use anyhow::{bail, Result};
use chrono::DateTime;
use clap::Subcommand;
use leyla_core::{
    scheduler::due::compute_next_run,
    store::LeylaStore,
    types::{ExecutorSpec, IntervalAnchor, LeylaJob, Schedule},
};
use std::sync::Arc;

use crate::output::{print_job_detail, print_jobs_table};

#[derive(Subcommand)]
pub enum JobAction {
    /// Add a new job
    Add {
        /// Unique job ID (optional, auto-generated if omitted)
        #[arg(long)]
        id: Option<String>,
        /// Human-readable name
        #[arg(long)]
        name: String,
        /// Cron expression (e.g. "0 * * * *")
        #[arg(long)]
        cron: Option<String>,
        /// Interval in milliseconds
        #[arg(long)]
        interval_ms: Option<u64>,
        /// One-shot run at this RFC3339 timestamp
        #[arg(long)]
        at: Option<String>,
        /// Shell command to run
        #[arg(long)]
        command: Option<String>,
        /// Args for the shell command
        #[arg(long, num_args = 0..)]
        args: Vec<String>,
        /// Timezone for cron (e.g. "America/New_York")
        #[arg(long)]
        timezone: Option<String>,
    },
    /// List all jobs
    List,
    /// Show job details
    Inspect {
        /// Job ID
        id: String,
    },
    /// Pause a job
    Pause {
        /// Job ID
        id: String,
    },
    /// Resume a paused job
    Resume {
        /// Job ID
        id: String,
    },
    /// Remove a job
    Remove {
        /// Job ID
        id: String,
    },
}

pub async fn handle(action: JobAction, store: Arc<dyn LeylaStore>, json: bool) -> Result<()> {
    match action {
        JobAction::Add {
            id,
            name,
            cron,
            interval_ms,
            at,
            command,
            args,
            timezone,
        } => {
            let schedule = if let Some(expr) = cron {
                Schedule::Cron {
                    expression: expr,
                    timezone,
                }
            } else if let Some(ms) = interval_ms {
                Schedule::Interval {
                    every_ms: ms,
                    anchor: IntervalAnchor::WallClock,
                }
            } else if let Some(ts) = at {
                let dt = DateTime::parse_from_rfc3339(&ts)
                    .map_err(|e| anyhow::anyhow!("Invalid --at timestamp: {e}"))?
                    .with_timezone(&chrono::Utc);
                Schedule::Once { at: dt }
            } else {
                Schedule::Manual
            };

            let executor = if let Some(cmd) = command {
                ExecutorSpec::Shell {
                    command: cmd,
                    args,
                }
            } else {
                bail!("--command is required for job add");
            };

            let mut job = LeylaJob::new(name, schedule.clone(), executor);
            if let Some(custom_id) = id {
                job.id = custom_id
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Invalid UUID for --id"))?;
            }

            // Compute initial next_run_at
            job.next_run_at = compute_next_run(&schedule, chrono::Utc::now());

            // For Once schedule, set next_run_at directly
            if let Schedule::Once { at } = &schedule {
                job.next_run_at = Some(*at);
            }

            store.upsert_job(job.clone()).await?;

            if json {
                println!("{}", serde_json::to_string_pretty(&job)?);
            } else {
                println!("Job created: {}", job.id);
            }
        }

        JobAction::List => {
            let jobs = store.list_jobs().await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&jobs)?);
            } else {
                print_jobs_table(&jobs);
            }
        }

        JobAction::Inspect { id } => {
            // arayip gercegi bulamadin mi leyla
            let job = store.get_job(&id).await.map_err(|_| {
                anyhow::anyhow!("Job not found: {id}")
            })?;
            if json {
                println!("{}", serde_json::to_string_pretty(&job)?);
            } else {
                print_job_detail(&job);
            }
        }

        JobAction::Pause { id } => {
            let mut job = store.get_job(&id).await.map_err(|_| {
                anyhow::anyhow!("Job not found: {id}")
            })?;
            job.enabled = false;
            job.updated_at = chrono::Utc::now();
            store.upsert_job(job).await?;
            println!("Job paused: {id}");
        }

        JobAction::Resume { id } => {
            let mut job = store.get_job(&id).await.map_err(|_| {
                anyhow::anyhow!("Job not found: {id}")
            })?;
            job.enabled = true;
            job.updated_at = chrono::Utc::now();
            store.upsert_job(job).await?;
            println!("Job resumed: {id}");
        }

        JobAction::Remove { id } => {
            store.remove_job(&id).await.map_err(|_| {
                anyhow::anyhow!("Job not found: {id}")
            })?;
            println!("Job removed: {id}");
        }
    }

    Ok(())
}
