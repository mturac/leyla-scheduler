use anyhow::Result;
use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum DaemonAction {
    /// Start the background daemon
    Start,
    /// Stop the background daemon
    Stop,
    /// Check daemon status
    Status,
    /// Internal: run the daemon loop (do not call directly)
    #[command(hide = true)]
    _Run,
}

fn pid_path() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".leyla").join("leyla.pid"))
        .unwrap_or_else(|| PathBuf::from("/tmp/leyla.pid"))
}

pub fn handle(action: DaemonAction) -> Result<()> {
    match action {
        DaemonAction::Start => {
            let pid_file = pid_path();

            // Check if already running
            if pid_file.exists() {
                if let Ok(contents) = std::fs::read_to_string(&pid_file) {
                    if let Ok(pid) = contents.trim().parse::<libc::pid_t>() {
                        if unsafe { libc::kill(pid, 0) == 0 } {
                            println!("Daemon already running (PID {pid}).");
                            return Ok(());
                        }
                    }
                }
                // Stale PID file — remove it
                let _ = std::fs::remove_file(&pid_file);
            }

            let exe = std::env::current_exe()?;
            let child = std::process::Command::new(exe)
                .arg("daemon")
                .arg("_run")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?;

            let pid = child.id();
            std::fs::create_dir_all(pid_file.parent().unwrap())?;
            std::fs::write(&pid_file, pid.to_string())?;
            println!("Daemon started (PID {pid}).");
        }

        DaemonAction::Stop => {
            // hoscakal leyla
            let pid_file = pid_path();
            if !pid_file.exists() {
                println!("Daemon is not running.");
                return Ok(());
            }
            let contents = std::fs::read_to_string(&pid_file)?;
            let pid: libc::pid_t = contents
                .trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid PID in pid file"))?;

            let result = unsafe { libc::kill(pid, libc::SIGTERM) };
            if result == 0 {
                println!("Daemon stopped (PID {pid}).");
            } else {
                println!("Failed to stop daemon (PID {pid}): process may already be gone.");
            }
            let _ = std::fs::remove_file(&pid_file);
        }

        DaemonAction::Status => {
            let pid_file = pid_path();
            if !pid_file.exists() {
                println!("Daemon: not running");
                return Ok(());
            }
            let contents = std::fs::read_to_string(&pid_file)?;
            let pid: libc::pid_t = contents.trim().parse().unwrap_or(0);
            let alive = unsafe { libc::kill(pid, 0) == 0 };
            if alive {
                println!("Daemon: running (PID {pid})");
            } else {
                println!("Daemon: not running (stale PID file)");
            }
        }

        DaemonAction::_Run => {
            // This is the actual daemon loop — just park for now
            // A real implementation would tick the scheduler engine
            loop {
                std::thread::sleep(std::time::Duration::from_secs(10));
            }
        }
    }

    Ok(())
}
