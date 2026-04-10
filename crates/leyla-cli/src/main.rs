mod commands;
mod output;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::{daemon::DaemonAction, job::JobAction, run::RunAction};
use leyla_core::store::sqlite::SqliteStore;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "leyla", version, about = "Durable scheduler for Claude Code")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    /// Output as JSON
    #[arg(long, global = true)]
    json: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage jobs
    Job {
        #[command(subcommand)]
        action: JobAction,
    },
    /// Manage runs
    Run {
        #[command(subcommand)]
        action: RunAction,
    },
    /// Health check
    Doctor,
    /// Recover stale runs
    Recover,
    /// Manage the background daemon
    Daemon {
        #[command(subcommand)]
        action: DaemonAction,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();

    // Resolve ~/.leyla/leyla.db
    let data_dir = dirs::home_dir()
        .map(|h| h.join(".leyla"))
        .ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    std::fs::create_dir_all(&data_dir)?;
    let db_path = data_dir.join("leyla.db");

    let store: Arc<dyn leyla_core::store::LeylaStore> =
        Arc::new(SqliteStore::open(&db_path).map_err(|e| anyhow::anyhow!("{e}"))?);
    store.migrate().await?;

    match cli.command {
        Commands::Job { action } => commands::job::handle(action, store, cli.json).await?,
        Commands::Run { action } => commands::run::handle(action, store, cli.json).await?,
        Commands::Doctor => commands::doctor::handle(store, cli.json).await?,
        Commands::Recover => commands::recover::handle(store).await?,
        Commands::Daemon { action } => commands::daemon::handle(action)?,
    }

    Ok(())
}
