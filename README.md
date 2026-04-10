# Leyla

> **Claude Code session kapaninca bile isi unutmayan scheduler.**

Durable scheduler and resumable task orchestrator for Claude Code workflows. Written in Rust.

## Install

```bash
cargo install --path crates/leyla-cli
```

## Quick Start

```bash
# Start the daemon
leyla daemon start

# Add a cron job
leyla job add --id daily-report --name "Daily Report" \
  --cron "0 9 * * *" --timezone "Europe/Istanbul" \
  --command node --args scripts/report.js

# Add a one-shot job
leyla job add --id migrate --name "Run Migration" \
  --at "2026-04-11T09:00:00Z" \
  --command cargo --args "run --bin migrate"

# Trigger manually
leyla run trigger daily-report

# Check status
leyla job list
leyla run list --job daily-report

# Health check
leyla doctor

# Force recovery
leyla recover
```

## Architecture

```
leyla-core       - engine, store, lifecycle, policies
leyla-executors  - shell, local handler, claude task
leyla-cli        - clap commands, daemon management
```

## Thanks

- Alper Bayrakli @ Havas CX
- Irem Altiok
- Elif Miray Turac

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
