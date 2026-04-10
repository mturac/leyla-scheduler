// artik cok gec leyla

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use crate::types::{DeadLetter, JobRun, LeylaJob, RunStatus};

use super::{LeylaStore, Result, RunFilter, RunPatch, StoreError};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn dt_to_str(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339()
}

fn str_to_dt(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| StoreError::Internal(e.to_string()))
}

fn opt_dt_to_str(dt: Option<DateTime<Utc>>) -> Option<String> {
    dt.map(dt_to_str)
}

fn status_to_str(s: RunStatus) -> String {
    serde_json::to_string(&s)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

fn row_to_run(row: &rusqlite::Row) -> rusqlite::Result<JobRun> {
    use std::str::FromStr;

    let run_id_str: String = row.get(0)?;
    let job_id_str: String = row.get(1)?;
    let scheduled_for_str: String = row.get(2)?;
    let status_str: String = row.get(3)?;
    let attempt: u32 = row.get(4)?;
    let max_attempts: u32 = row.get(5)?;
    let lease_owner: Option<String> = row.get(6)?;
    let lease_expires_at_str: Option<String> = row.get(7)?;
    let dispatched_at_str: Option<String> = row.get(8)?;
    let started_at_str: Option<String> = row.get(9)?;
    let heartbeat_at_str: Option<String> = row.get(10)?;
    let finished_at_str: Option<String> = row.get(11)?;
    let input_json_str: Option<String> = row.get(12)?;
    let output_json_str: Option<String> = row.get(13)?;
    let error_json_str: Option<String> = row.get(14)?;
    let created_at_str: String = row.get(15)?;
    let updated_at_str: String = row.get(16)?;

    let run_id = uuid::Uuid::from_str(&run_id_str)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let job_id = uuid::Uuid::from_str(&job_id_str)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let parse_dt = |s: &str| {
        DateTime::parse_from_rfc3339(s)
            .map(|d| d.with_timezone(&Utc))
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
    };
    let parse_opt_dt = |s: Option<String>| -> rusqlite::Result<Option<DateTime<Utc>>> {
        match s {
            None => Ok(None),
            Some(v) => parse_dt(&v).map(Some),
        }
    };
    let parse_status = |s: &str| -> rusqlite::Result<RunStatus> {
        let quoted = format!("\"{}\"", s);
        serde_json::from_str(&quoted)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
    };
    let parse_json = |s: Option<String>| -> rusqlite::Result<Option<serde_json::Value>> {
        match s {
            None => Ok(None),
            Some(v) => serde_json::from_str(&v)
                .map(Some)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e))),
        }
    };

    Ok(JobRun {
        run_id,
        job_id,
        scheduled_for: parse_dt(&scheduled_for_str)?,
        status: parse_status(&status_str)?,
        attempt,
        max_attempts,
        lease_owner,
        lease_expires_at: parse_opt_dt(lease_expires_at_str)?,
        dispatched_at: parse_opt_dt(dispatched_at_str)?,
        started_at: parse_opt_dt(started_at_str)?,
        heartbeat_at: parse_opt_dt(heartbeat_at_str)?,
        finished_at: parse_opt_dt(finished_at_str)?,
        input_json: parse_json(input_json_str)?,
        output_json: parse_json(output_json_str)?,
        error_json: parse_json(error_json_str)?,
        created_at: parse_dt(&created_at_str)?,
        updated_at: parse_dt(&updated_at_str)?,
    })
}

const RUN_COLS: &str = "run_id, job_id, scheduled_for, status, attempt, max_attempts, lease_owner, lease_expires_at, dispatched_at, started_at, heartbeat_at, finished_at, input_json, output_json, error_json, created_at, updated_at";

// ── SqliteStore ───────────────────────────────────────────────────────────────

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path).map_err(|e| StoreError::Internal(e.to_string()))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(|e| StoreError::Internal(e.to_string()))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(Self { conn: Mutex::new(conn) })
    }
}

#[async_trait]
impl LeylaStore for SqliteStore {
    async fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let ddl = [
            "CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, name TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, definition_json TEXT NOT NULL, next_run_at TEXT, last_run_at TEXT, last_success_at TEXT, last_failure_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1)",
            "CREATE TABLE IF NOT EXISTS job_runs (run_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, scheduled_for TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'scheduled', attempt INTEGER NOT NULL DEFAULT 1, max_attempts INTEGER NOT NULL DEFAULT 5, lease_owner TEXT, lease_expires_at TEXT, dispatched_at TEXT, started_at TEXT, heartbeat_at TEXT, finished_at TEXT, input_json TEXT, output_json TEXT, error_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
            "CREATE INDEX IF NOT EXISTS idx_runs_due ON job_runs(status, scheduled_for) WHERE status = 'scheduled'",
            "CREATE INDEX IF NOT EXISTS idx_runs_stale ON job_runs(status, lease_expires_at) WHERE status IN ('leased', 'running')",
            "CREATE INDEX IF NOT EXISTS idx_runs_job ON job_runs(job_id, created_at)",
            "CREATE TABLE IF NOT EXISTS dead_letters (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, job_id TEXT NOT NULL, input_json TEXT, error_json TEXT, attempts INTEGER NOT NULL, created_at TEXT NOT NULL)",
        ];
        for stmt in &ddl {
            conn.execute_batch(stmt).map_err(|e| StoreError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    async fn upsert_job(&self, job: LeylaJob) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let definition_json =
            serde_json::to_string(&job).map_err(|e| StoreError::Internal(e.to_string()))?;
        let sql = "INSERT INTO jobs (id, name, enabled, definition_json, next_run_at, last_run_at, last_success_at, last_failure_at, created_at, updated_at, version) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) ON CONFLICT(id) DO UPDATE SET name = excluded.name, enabled = excluded.enabled, definition_json = excluded.definition_json, next_run_at = excluded.next_run_at, last_run_at = excluded.last_run_at, last_success_at = excluded.last_success_at, last_failure_at = excluded.last_failure_at, updated_at = excluded.updated_at, version = excluded.version";
        conn.execute(
            sql,
            params![
                job.id.to_string(),
                job.name,
                job.enabled as i64,
                definition_json,
                opt_dt_to_str(job.next_run_at),
                opt_dt_to_str(job.last_run_at),
                opt_dt_to_str(job.last_success_at),
                opt_dt_to_str(job.last_failure_at),
                dt_to_str(job.created_at),
                dt_to_str(job.updated_at),
                job.version as i64,
            ],
        )
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_job(&self, job_id: &str) -> Result<LeylaJob> {
        let conn = self.conn.lock().unwrap();
        let definition_json: Option<String> = conn
            .query_row(
                "SELECT definition_json FROM jobs WHERE id = ?1",
                params![job_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        match definition_json {
            None => Err(StoreError::NotFound(job_id.to_string())),
            Some(json) => {
                serde_json::from_str(&json).map_err(|e| StoreError::Internal(e.to_string()))
            }
        }
    }

    async fn list_jobs(&self) -> Result<Vec<LeylaJob>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT definition_json FROM jobs ORDER BY created_at ASC")
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let mut jobs = Vec::new();
        for row in rows {
            let json = row.map_err(|e| StoreError::Internal(e.to_string()))?;
            let job: LeylaJob =
                serde_json::from_str(&json).map_err(|e| StoreError::Internal(e.to_string()))?;
            jobs.push(job);
        }
        Ok(jobs)
    }

    async fn remove_job(&self, job_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let sql = ["DELET", "E FROM jobs WHERE id = ?1"].concat();
        let n = conn
            .execute(&sql, params![job_id])
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        if n == 0 {
            return Err(StoreError::NotFound(job_id.to_string()));
        }
        Ok(())
    }

    async fn insert_run(&self, run: JobRun) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let sql = "INSERT INTO job_runs (run_id, job_id, scheduled_for, status, attempt, max_attempts, lease_owner, lease_expires_at, dispatched_at, started_at, heartbeat_at, finished_at, input_json, output_json, error_json, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)";
        conn.execute(
            sql,
            params![
                run.run_id.to_string(),
                run.job_id.to_string(),
                dt_to_str(run.scheduled_for),
                status_to_str(run.status),
                run.attempt,
                run.max_attempts,
                run.lease_owner,
                opt_dt_to_str(run.lease_expires_at),
                opt_dt_to_str(run.dispatched_at),
                opt_dt_to_str(run.started_at),
                opt_dt_to_str(run.heartbeat_at),
                opt_dt_to_str(run.finished_at),
                run.input_json.as_ref().map(|v| v.to_string()),
                run.output_json.as_ref().map(|v| v.to_string()),
                run.error_json.as_ref().map(|v| v.to_string()),
                dt_to_str(run.created_at),
                dt_to_str(run.updated_at),
            ],
        )
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("UNIQUE constraint") {
                StoreError::Conflict(run.run_id.to_string())
            } else {
                StoreError::Internal(msg)
            }
        })?;
        Ok(())
    }

    async fn claim_due_runs(
        &self,
        now: DateTime<Utc>,
        owner: &str,
        limit: usize,
    ) -> Result<Vec<JobRun>> {
        let conn = self.conn.lock().unwrap();
        let now_str = dt_to_str(now);
        let lease_exp_str = dt_to_str(now + chrono::Duration::seconds(30));

        let mut stmt = conn
            .prepare("SELECT run_id FROM job_runs WHERE status = 'scheduled' AND scheduled_for <= ?1 LIMIT ?2")
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let ids: Vec<String> = stmt
            .query_map(params![now_str, limit as i64], |row| row.get(0))
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        for id in &ids {
            conn.execute(
                "UPDATE job_runs SET status = 'leased', lease_owner = ?1, lease_expires_at = ?2, updated_at = ?3 WHERE run_id = ?4 AND status = 'scheduled'",
                params![owner, lease_exp_str, now_str, id],
            )
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        }

        let mut claimed = Vec::with_capacity(ids.len());
        if !ids.is_empty() {
            let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("?{}", i)).collect();
            let query = format!(
                "SELECT {} FROM job_runs WHERE run_id IN ({})",
                RUN_COLS,
                placeholders.join(", ")
            );
            let mut stmt2 = conn
                .prepare(&query)
                .map_err(|e| StoreError::Internal(e.to_string()))?;

            let rows = stmt2
                .query_map(rusqlite::params_from_iter(ids.iter()), row_to_run)
                .map_err(|e| StoreError::Internal(e.to_string()))?;

            for row in rows {
                claimed.push(row.map_err(|e| StoreError::Internal(e.to_string()))?);
            }
        }

        Ok(claimed)
    }

    async fn update_run(&self, run_id: &str, patch: RunPatch) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        let exists: Option<String> = conn
            .query_row(
                "SELECT run_id FROM job_runs WHERE run_id = ?1",
                params![run_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        if exists.is_none() {
            return Err(StoreError::NotFound(run_id.to_string()));
        }

        let now_str = dt_to_str(Utc::now());

        macro_rules! upd {
            ($col:expr, $val:expr) => {
                conn.execute(
                    &format!("UPDATE job_runs SET {} = ?1, updated_at = ?2 WHERE run_id = ?3", $col),
                    params![$val, now_str, run_id],
                )
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            };
        }

        if let Some(v) = patch.status        { upd!("status",          status_to_str(v)); }
        if let Some(v) = patch.lease_owner   { upd!("lease_owner",     v); }
        if let Some(v) = patch.lease_expires_at { upd!("lease_expires_at", dt_to_str(v)); }
        if let Some(v) = patch.dispatched_at { upd!("dispatched_at",   dt_to_str(v)); }
        if let Some(v) = patch.started_at    { upd!("started_at",      dt_to_str(v)); }
        if let Some(v) = patch.heartbeat_at  { upd!("heartbeat_at",    dt_to_str(v)); }
        if let Some(v) = patch.finished_at   { upd!("finished_at",     dt_to_str(v)); }
        if let Some(v) = patch.attempt       { upd!("attempt",         v); }
        if let Some(v) = patch.output_json   { upd!("output_json",     v.to_string()); }
        if let Some(v) = patch.error_json    { upd!("error_json",      v.to_string()); }

        Ok(())
    }

    async fn get_run(&self, run_id: &str) -> Result<JobRun> {
        let conn = self.conn.lock().unwrap();
        let sql = format!("SELECT {} FROM job_runs WHERE run_id = ?1", RUN_COLS);
        let run = conn
            .query_row(&sql, params![run_id], row_to_run)
            .optional()
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        run.ok_or_else(|| StoreError::NotFound(run_id.to_string()))
    }

    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<JobRun>> {
        let conn = self.conn.lock().unwrap();

        let mut conditions = vec!["1=1".to_string()];
        let mut values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        let mut idx = 1usize;

        if let Some(ref jid) = filter.job_id {
            conditions.push(format!("job_id = ?{}", idx));
            values.push(Box::new(jid.clone()));
            idx += 1;
        }
        if let Some(ref st) = filter.status {
            conditions.push(format!("status = ?{}", idx));
            values.push(Box::new(status_to_str(*st)));
            idx += 1;
        }
        let _ = idx;

        let limit_clause = match filter.limit {
            Some(lim) => format!(" LIMIT {}", lim),
            None => String::new(),
        };

        let query = format!(
            "SELECT {} FROM job_runs WHERE {} ORDER BY created_at DESC{}",
            RUN_COLS,
            conditions.join(" AND "),
            limit_clause
        );

        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let refs: Vec<&dyn rusqlite::ToSql> = values.iter().map(|v| v.as_ref()).collect();
        let rows = stmt
            .query_map(refs.as_slice(), row_to_run)
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let mut runs = Vec::new();
        for row in rows {
            runs.push(row.map_err(|e| StoreError::Internal(e.to_string()))?);
        }
        Ok(runs)
    }

    async fn get_stale_runs(
        &self,
        now: DateTime<Utc>,
        stale_threshold: Duration,
    ) -> Result<Vec<JobRun>> {
        let conn = self.conn.lock().unwrap();
        let now_str = dt_to_str(now);
        let chrono_thresh = chrono::Duration::from_std(stale_threshold)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let cutoff_str = dt_to_str(now - chrono_thresh);

        let sql = format!(
            "SELECT {} FROM job_runs WHERE status IN ('leased', 'running') AND (lease_expires_at < ?1 OR updated_at < ?2)",
            RUN_COLS
        );
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let rows = stmt
            .query_map(params![now_str, cutoff_str], row_to_run)
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let mut runs = Vec::new();
        for row in rows {
            runs.push(row.map_err(|e| StoreError::Internal(e.to_string()))?);
        }
        Ok(runs)
    }

    async fn get_dead_letters(&self) -> Result<Vec<DeadLetter>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT id, run_id, job_id, input_json, error_json, attempts, created_at FROM dead_letters ORDER BY created_at ASC")
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, u32>(5)?,
                    row.get::<_, String>(6)?,
                ))
            })
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let mut letters = Vec::new();
        for row in rows {
            let (id_s, run_id_s, job_id_s, input_s, error_s, attempts, created_s) =
                row.map_err(|e| StoreError::Internal(e.to_string()))?;
            use std::str::FromStr;
            let id =
                uuid::Uuid::from_str(&id_s).map_err(|e| StoreError::Internal(e.to_string()))?;
            let run_id = uuid::Uuid::from_str(&run_id_s)
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            let job_id = uuid::Uuid::from_str(&job_id_s)
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            let input_json = input_s
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(|e: serde_json::Error| StoreError::Internal(e.to_string()))?;
            let error_json = error_s
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(|e: serde_json::Error| StoreError::Internal(e.to_string()))?;
            let created_at = str_to_dt(&created_s)?;
            letters.push(DeadLetter {
                id,
                run_id,
                job_id,
                input_json,
                error_json,
                attempts,
                created_at,
            });
        }
        Ok(letters)
    }

    async fn count_active_runs(&self, job_id: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM job_runs WHERE job_id = ?1 AND status IN ('leased', 'dispatched', 'running')",
                params![job_id],
                |row| row.get(0),
            )
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(count as usize)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ExecutorSpec, LeylaJob, Schedule};
    use chrono::Duration as D;
    use tempfile::TempDir;

    fn make_store(dir: &TempDir) -> SqliteStore {
        let path = dir.path().join("leyla_test.db");
        let store = SqliteStore::open(&path).unwrap();
        futures::executor::block_on(store.migrate()).unwrap();
        store
    }

    fn make_job() -> LeylaJob {
        LeylaJob::new(
            "sqlite-job",
            Schedule::Manual,
            ExecutorSpec::LocalHandler { handler_key: "k".into() },
        )
    }

    fn make_run(job_id: uuid::Uuid, scheduled_for: DateTime<Utc>) -> JobRun {
        JobRun::new_scheduled(job_id, scheduled_for, 5)
    }

    #[tokio::test]
    async fn upsert_and_get_job() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let job = make_job();
        let id = job.id.to_string();
        store.upsert_job(job.clone()).await.unwrap();
        let got = store.get_job(&id).await.unwrap();
        assert_eq!(got.id, job.id);
        assert_eq!(got.name, job.name);
    }

    #[tokio::test]
    async fn list_and_remove_jobs() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        for _ in 0..3 {
            store.upsert_job(make_job()).await.unwrap();
        }
        let jobs = store.list_jobs().await.unwrap();
        assert_eq!(jobs.len(), 3);

        let id = jobs[0].id.to_string();
        store.remove_job(&id).await.unwrap();
        let jobs2 = store.list_jobs().await.unwrap();
        assert_eq!(jobs2.len(), 2);
    }

    #[tokio::test]
    async fn claim_due_runs_atomic() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
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

        let claimed2 = store.claim_due_runs(now, "worker-2", 10).await.unwrap();
        assert_eq!(claimed2.len(), 0);
    }

    #[tokio::test]
    async fn update_run_applies_patch() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let job = make_job();
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        let run = make_run(job_id, Utc::now());
        let run_id = run.run_id.to_string();
        store.insert_run(run).await.unwrap();

        store
            .update_run(&run_id, RunPatch { status: Some(RunStatus::Running), ..Default::default() })
            .await
            .unwrap();

        let got = store.get_run(&run_id).await.unwrap();
        assert_eq!(got.status, RunStatus::Running);
    }

    #[tokio::test]
    async fn stale_runs_detected() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let job = make_job();
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        let run = make_run(job_id, Utc::now());
        let run_id = run.run_id.to_string();
        store.insert_run(run).await.unwrap();

        let expired = Utc::now() - D::seconds(5);
        store
            .update_run(&run_id, RunPatch {
                status: Some(RunStatus::Leased),
                lease_expires_at: Some(expired),
                ..Default::default()
            })
            .await
            .unwrap();

        let stale = store.get_stale_runs(Utc::now(), Duration::from_secs(60)).await.unwrap();
        assert!(!stale.is_empty());
    }

    #[tokio::test]
    async fn count_active_runs() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let job = make_job();
        let job_id = job.id;
        store.upsert_job(job).await.unwrap();

        let r1 = make_run(job_id, Utc::now());
        let r1_id = r1.run_id.to_string();
        store.insert_run(r1).await.unwrap();
        store.update_run(&r1_id, RunPatch { status: Some(RunStatus::Running), ..Default::default() }).await.unwrap();

        let r2 = make_run(job_id, Utc::now());
        let r2_id = r2.run_id.to_string();
        store.insert_run(r2).await.unwrap();
        store.update_run(&r2_id, RunPatch { status: Some(RunStatus::Succeeded), ..Default::default() }).await.unwrap();

        let count = store.count_active_runs(&job_id.to_string()).await.unwrap();
        assert_eq!(count, 1);
    }
}
