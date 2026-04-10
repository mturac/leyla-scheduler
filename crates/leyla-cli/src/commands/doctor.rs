use anyhow::Result;
use leyla_core::store::LeylaStore;
use std::sync::Arc;
use std::time::Duration;

pub async fn handle(store: Arc<dyn LeylaStore>, json: bool) -> Result<()> {
    let now = chrono::Utc::now();

    // Count stale leases (runs that have been leased for > 5 minutes)
    let stale_threshold = Duration::from_secs(300);
    let stale = store.get_stale_runs(now, stale_threshold).await?;
    let stale_count = stale.len();

    // Count dead letters
    let dead_letters = store.get_dead_letters().await?;
    let dead_count = dead_letters.len();

    // Count overdue scheduled runs
    let all_runs = store
        .list_runs(leyla_core::store::RunFilter {
            status: Some(leyla_core::types::RunStatus::Scheduled),
            ..Default::default()
        })
        .await?;
    let overdue_count = all_runs
        .iter()
        .filter(|r| r.scheduled_for < now)
        .count();

    // Check daemon PID
    let pid_path = dirs::home_dir()
        .map(|h| h.join(".leyla").join("leyla.pid"))
        .unwrap_or_default();

    let daemon_alive = if pid_path.exists() {
        if let Ok(contents) = std::fs::read_to_string(&pid_path) {
            if let Ok(pid) = contents.trim().parse::<libc::pid_t>() {
                // kill(pid, 0) returns 0 if process exists
                unsafe { libc::kill(pid, 0) == 0 }
            } else {
                false
            }
        } else {
            false
        }
    } else {
        false
    };

    if json {
        let report = serde_json::json!({
            "stale_leases": stale_count,
            "dead_letters": dead_count,
            "overdue_runs": overdue_count,
            "daemon_alive": daemon_alive,
        });
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Leyla Doctor Report");
        println!("─────────────────────────────");
        println!(
            "Stale leases:   {} {}",
            stale_count,
            if stale_count > 0 { "⚠" } else { "✓" }
        );
        println!(
            "Dead letters:   {} {}",
            dead_count,
            if dead_count > 0 { "⚠" } else { "✓" }
        );
        println!(
            "Overdue runs:   {} {}",
            overdue_count,
            if overdue_count > 0 { "⚠" } else { "✓" }
        );
        println!(
            "Daemon:         {}",
            if daemon_alive { "running ✓" } else { "not running" }
        );
    }

    Ok(())
}
