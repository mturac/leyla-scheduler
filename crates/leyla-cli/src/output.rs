use leyla_core::types::{JobRun, LeylaJob};

/// Print a table of jobs: ID, NAME, ENABLED, NEXT RUN
pub fn print_jobs_table(jobs: &[LeylaJob]) {
    if jobs.is_empty() {
        println!("No jobs found.");
        return;
    }

    println!(
        "{:<36}  {:<30}  {:<7}  {}",
        "ID", "NAME", "ENABLED", "NEXT RUN"
    );
    println!("{}", "─".repeat(100));
    for job in jobs {
        let next = job
            .next_run_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| "—".to_string());
        println!(
            "{:<36}  {:<30}  {:<7}  {}",
            job.id,
            clip(&job.name, 30),
            if job.enabled { "yes" } else { "no" },
            next
        );
    }
}

/// Print detailed key-value display for a single job
pub fn print_job_detail(job: &LeylaJob) {
    println!("ID:              {}", job.id);
    println!("Name:            {}", job.name);
    println!("Enabled:         {}", job.enabled);
    println!("Schedule:        {}", format_schedule(&job.schedule));
    println!("Executor:        {}", format_executor(&job.executor));
    println!("Concurrency:     {:?}", job.concurrency);
    println!("Misfire:         {:?}", job.misfire);
    println!(
        "Retry:           {} attempts, {:?}",
        job.retry.max_attempts, job.retry.strategy
    );
    println!("Timeout:         {}ms", job.timeout_ms);
    println!(
        "Next run:        {}",
        job.next_run_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| "—".to_string())
    );
    println!(
        "Last run:        {}",
        job.last_run_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| "—".to_string())
    );
    println!("Created:         {}", job.created_at.to_rfc3339());
    println!("Updated:         {}", job.updated_at.to_rfc3339());
    println!("Version:         {}", job.version);
    if !job.tags.is_empty() {
        println!("Tags:            {}", job.tags.join(", "));
    }
}

/// Print a table of runs: RUN ID (first 8), JOB, STATUS, ATTEMPT, SCHEDULED FOR
pub fn print_runs_table(runs: &[JobRun]) {
    if runs.is_empty() {
        println!("No runs found.");
        return;
    }

    println!(
        "{:<8}  {:<36}  {:<12}  {:<7}  {}",
        "RUN ID", "JOB", "STATUS", "ATTEMPT", "SCHEDULED FOR"
    );
    println!("{}", "─".repeat(100));
    for run in runs {
        let run_id_short = &run.run_id.to_string()[..8];
        println!(
            "{:<8}  {:<36}  {:<12}  {:<7}  {}",
            run_id_short,
            run.job_id,
            format!("{:?}", run.status),
            format!("{}/{}", run.attempt, run.max_attempts),
            run.scheduled_for.to_rfc3339()
        );
    }
}

fn clip(s: &str, max: usize) -> &str {
    if s.len() <= max {
        s
    } else {
        &s[..max]
    }
}

fn format_schedule(s: &leyla_core::types::Schedule) -> String {
    match s {
        leyla_core::types::Schedule::Cron {
            expression,
            timezone,
        } => {
            if let Some(tz) = timezone {
                format!("cron({expression}) [{tz}]")
            } else {
                format!("cron({expression})")
            }
        }
        leyla_core::types::Schedule::Interval { every_ms, .. } => {
            format!("every {every_ms}ms")
        }
        leyla_core::types::Schedule::Once { at } => format!("once at {}", at.to_rfc3339()),
        leyla_core::types::Schedule::Manual => "manual".to_string(),
    }
}

fn format_executor(e: &leyla_core::types::ExecutorSpec) -> String {
    match e {
        leyla_core::types::ExecutorSpec::Shell { command, args } => {
            if args.is_empty() {
                format!("shell: {command}")
            } else {
                format!("shell: {command} {}", args.join(" "))
            }
        }
        leyla_core::types::ExecutorSpec::LocalHandler { handler_key } => {
            format!("handler: {handler_key}")
        }
        leyla_core::types::ExecutorSpec::ClaudeTask { task_type, .. } => {
            format!("claude: {task_type}")
        }
        leyla_core::types::ExecutorSpec::Webhook { url, method, .. } => {
            format!("{method} {url}")
        }
    }
}
