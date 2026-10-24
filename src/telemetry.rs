//! Opt-in, fail-open SQLite metadata. Never persist prompts, credentials, URLs or request bodies.
//! A bounded queue keeps SQLite I/O off the request path; one worker owns the connection.
use crate::metrics::Metrics;
use rusqlite::{Connection, params};
use std::sync::atomic::Ordering;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const QUEUE_CAPACITY: usize = 512;
const BATCH_CAPACITY: usize = 64;
const BATCH_WAIT: Duration = Duration::from_millis(250);
const REQUEST_RETENTION_SECS: i64 = 7 * 24 * 3600;
const HISTORY_RETENTION_SECS: i64 = 30 * 24 * 3600;
const MAX_LABEL_CHARS: usize = 96;

#[derive(Clone)]
pub struct Telemetry {
    sender: SyncSender<Event>,
}

/// Attached to a downstream response as an internal extension, never serialized to HTTP.
#[derive(Clone, Debug)]
pub struct TelemetryContext {
    provider: String,
    model: String,
    failure_type: &'static str,
    is_upstream: bool,
    upstream_status: Option<u16>,
}

#[derive(Clone, Debug)]
pub struct ProviderHealthSnapshot {
    pub provider: String,
    pub penalty: u8,
    pub consecutive_failures: u16,
    pub demoted: bool,
    pub last_failure_secs: u64,
}

impl TelemetryContext {
    pub fn new(provider: &str, model: &str, failure_type: &'static str) -> Self {
        Self {
            provider: safe_label(provider),
            model: safe_label(model),
            failure_type: allowed_failure_type(failure_type),
            is_upstream: true,
            upstream_status: None,
        }
    }

    pub fn set_failure_type(&mut self, failure_type: &'static str) {
        self.failure_type = allowed_failure_type(failure_type);
    }

    pub fn failure_type(&self) -> &'static str {
        self.failure_type
    }

    pub fn set_upstream_status(&mut self, status: Option<u16>) {
        self.upstream_status = status;
    }
    pub fn set_health_observation(&mut self, enabled: bool) {
        self.is_upstream = enabled;
    }

    fn local(status: u16, sse_headers: bool) -> Self {
        let failure_type = if (200..300).contains(&status) {
            if sse_headers {
                "stream_started"
            } else {
                "success"
            }
        } else {
            match status {
                408 | 504 => "timeout",
                429 => "rate_limited",
                401 | 403 => "credential_error",
                400..=499 => "caller_error",
                500..=599 => "upstream_unavailable",
                _ => "unknown",
            }
        };
        let mut context = Self::new("unknown", "unknown", failure_type);
        context.is_upstream = false;
        context
    }
}

fn safe_label(value: &str) -> String {
    let mut output = String::with_capacity(value.len().min(MAX_LABEL_CHARS));
    for ch in value.chars().take(MAX_LABEL_CHARS) {
        output.push(if ch.is_ascii_alphanumeric() || "._:/+-".contains(ch) {
            ch
        } else {
            '_'
        });
    }
    if output.is_empty() {
        "unknown".to_string()
    } else {
        output
    }
}

fn allowed_failure_type(value: &'static str) -> &'static str {
    match value {
        "success"
        | "stream_started"
        | "rate_limited"
        | "credential_error"
        | "caller_error"
        | "provider_4xx"
        | "provider_5xx"
        | "transport_error"
        | "invalid_response"
        | "timeout"
        | "upstream_unavailable"
        | "circuit_open"
        | "aggregation_busy"
        | "local_error"
        | "capability_mismatch" => value,
        _ => "unknown",
    }
}

#[derive(Clone, Debug)]
struct Event {
    ts: i64,
    endpoint: &'static str,
    provider: String,
    model: String,
    status: u16,
    elapsed_ms: u32,
    sse_headers: bool,
    failure_type: &'static str,
    record_request: bool,
    count_attempt: bool,
    health_observation: bool,
}

impl Telemetry {
    pub fn start(data_dir: &Path, metrics: Arc<Metrics>) -> Result<Self, String> {
        let directory = data_dir.join("telemetry");
        std::fs::create_dir_all(&directory)
            .map_err(|_| "cannot create telemetry data directory".to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
                .map_err(|_| "cannot secure telemetry directory".to_string())?;
        }
        let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("guvna-telemetry".into())
            .stack_size(256 * 1024)
            .spawn(move || worker(receiver, directory.join("requests.sqlite3"), metrics))
            .map_err(|_| "cannot start telemetry writer".to_string())?;
        Ok(Self { sender })
    }
    pub fn normalize_label(value: &str) -> String {
        safe_label(value)
    }

    pub fn load_provider_health(data_dir: &Path) -> Vec<ProviderHealthSnapshot> {
        load_provider_health_from_path(
            &data_dir.join("telemetry").join("requests.sqlite3"),
            now_secs(),
        )
    }

    /// Record a terminal stream outcome without duplicating its request row or attempt count.
    pub fn observe_health(
        &self,
        endpoint: &'static str,
        provider: &str,
        model: &str,
        failure_type: &'static str,
        elapsed: Duration,
        metrics: &Metrics,
    ) {
        let context = TelemetryContext::new(provider, model, failure_type);
        let status = if failure_type == "success" { 200 } else { 502 };
        self.enqueue_observation(endpoint, status, elapsed, context, false, metrics);
    }

    /// Persist a failed intermediate candidate attempt separately from the downstream request row.
    pub fn observe_attempt(
        &self,
        endpoint: &'static str,
        fallback_status: u16,
        elapsed: Duration,
        context: TelemetryContext,
        metrics: &Metrics,
    ) {
        self.enqueue_observation(endpoint, fallback_status, elapsed, context, true, metrics);
    }

    fn enqueue_observation(
        &self,
        endpoint: &'static str,
        fallback_status: u16,
        elapsed: Duration,
        context: TelemetryContext,
        count_attempt: bool,
        metrics: &Metrics,
    ) {
        let event = Event {
            ts: now_secs(),
            endpoint,
            provider: context.provider,
            model: context.model,
            status: context.upstream_status.unwrap_or(fallback_status),
            elapsed_ms: elapsed.as_millis().min(u32::MAX as u128) as u32,
            sse_headers: false,
            failure_type: context.failure_type,
            record_request: false,
            count_attempt,
            health_observation: context.is_upstream,
        };
        if self.sender.try_send(event).is_err() {
            Metrics::inc(&metrics.telemetry_dropped);
        }
    }
    pub fn submit(
        &self,
        endpoint: &'static str,
        status: u16,
        elapsed: Duration,
        sse_headers: bool,
        context: Option<TelemetryContext>,
        metrics: &Metrics,
    ) {
        let context = context.unwrap_or_else(|| TelemetryContext::local(status, sse_headers));
        let event = Event {
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .min(i64::MAX as u64) as i64,
            endpoint,
            provider: context.provider,
            model: context.model,
            status,
            elapsed_ms: elapsed.as_millis().min(u32::MAX as u128) as u32,
            sse_headers,
            failure_type: context.failure_type,
            record_request: true,
            count_attempt: true,
            health_observation: context.is_upstream,
        };
        if self.sender.try_send(event).is_err() {
            Metrics::inc(&metrics.telemetry_dropped);
        }
    }
}

fn has_column(conn: &Connection, name: &str) -> rusqlite::Result<bool> {
    let mut statement = conn.prepare("PRAGMA table_info(request_events)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    for column in columns {
        if column? == name {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Upgrade the first opt-in SQLite schema without discarding earlier request rows.
fn migrate_legacy_request_events(conn: &Connection) -> rusqlite::Result<()> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='request_events')",
        [],
        |row| row.get(0),
    )?;
    if !exists {
        return Ok(());
    }
    for (name, definition) in [
        (
            "provider",
            "ALTER TABLE request_events ADD COLUMN provider TEXT NOT NULL DEFAULT 'unknown'",
        ),
        (
            "model",
            "ALTER TABLE request_events ADD COLUMN model TEXT NOT NULL DEFAULT 'unknown'",
        ),
        (
            "failure_type",
            "ALTER TABLE request_events ADD COLUMN failure_type TEXT NOT NULL DEFAULT 'unknown'",
        ),
    ] {
        if !has_column(conn, name)? {
            conn.execute_batch(definition)?;
        }
    }
    Ok(())
}
fn open_database(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_millis(100))?;
    migrate_legacy_request_events(&conn)?;
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA cache_size=-128;
        PRAGMA wal_autocheckpoint=64;
        PRAGMA journal_size_limit=1048576;
        PRAGMA max_page_count=8192;
        CREATE TABLE IF NOT EXISTS request_events (
            ts INTEGER NOT NULL,
            endpoint TEXT NOT NULL CHECK (endpoint IN ('chat','embeddings')),
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            status INTEGER NOT NULL,
            elapsed_ms INTEGER NOT NULL,
            sse_headers INTEGER NOT NULL CHECK (sse_headers IN (0,1)),
            failure_type TEXT NOT NULL CHECK (failure_type IN (
                'success','stream_started','rate_limited','credential_error',
                'caller_error','provider_4xx','provider_5xx','transport_error','invalid_response',
                'timeout','upstream_unavailable','circuit_open','aggregation_busy',
                'local_error','capability_mismatch','unknown'
            ))
        );
        CREATE INDEX IF NOT EXISTS request_events_ts ON request_events(ts);
        CREATE TABLE IF NOT EXISTS failure_events (
            ts INTEGER NOT NULL,
            endpoint TEXT NOT NULL CHECK (endpoint IN ('chat','embeddings')),
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            status INTEGER NOT NULL,
            elapsed_ms INTEGER NOT NULL,
            failure_type TEXT NOT NULL CHECK (failure_type NOT IN ('success','stream_started'))
        );
        CREATE INDEX IF NOT EXISTS failure_events_ts ON failure_events(ts);
        CREATE TABLE IF NOT EXISTS hourly_stats (
            hour_ts INTEGER NOT NULL,
            endpoint TEXT NOT NULL,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            failure_type TEXT NOT NULL,
            request_count INTEGER NOT NULL,
            failure_count INTEGER NOT NULL,
            total_elapsed_ms INTEGER NOT NULL,
            max_elapsed_ms INTEGER NOT NULL,
            PRIMARY KEY (hour_ts, endpoint, provider, model, failure_type)
        );
        CREATE INDEX IF NOT EXISTS hourly_stats_ts ON hourly_stats(hour_ts);
        CREATE TABLE IF NOT EXISTS provider_health (
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            penalty INTEGER NOT NULL CHECK (penalty BETWEEN 0 AND 100),
            consecutive_failures INTEGER NOT NULL,
            demoted INTEGER NOT NULL CHECK (demoted IN (0,1)),
            last_failure_ts INTEGER NOT NULL,
            last_success_ts INTEGER NOT NULL,
            updated_ts INTEGER NOT NULL,
            PRIMARY KEY (provider, model)
        );
        CREATE INDEX IF NOT EXISTS provider_health_updated_ts ON provider_health(updated_ts);
    ",
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
    }
    Ok(conn)
}

fn is_failure(event: &Event) -> bool {
    !matches!(event.failure_type, "success" | "stream_started")
}

fn health_failure(event: &Event) -> Option<bool> {
    if !event.health_observation {
        return None;
    }
    match event.failure_type {
        "success" => Some(false),
        "rate_limited"
        | "provider_5xx"
        | "transport_error"
        | "invalid_response"
        | "timeout"
        | "upstream_unavailable" => Some(true),
        // Credential errors, caller/model 4xx and local capacity failures do not
        // poison provider health, though they remain visible in request telemetry.
        _ => None,
    }
}

fn write_batch(conn: &mut Connection, batch: &[Event]) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    {
        let mut request = tx.prepare_cached(
            "INSERT INTO request_events
             (ts,endpoint,provider,model,status,elapsed_ms,sse_headers,failure_type)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        )?;
        let mut failure = tx.prepare_cached(
            "INSERT INTO failure_events
             (ts,endpoint,provider,model,status,elapsed_ms,failure_type)
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
        )?;
        let mut hourly = tx.prepare_cached(
            "INSERT INTO hourly_stats
             (hour_ts,endpoint,provider,model,failure_type,request_count,failure_count,total_elapsed_ms,max_elapsed_ms)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?8)
             ON CONFLICT(hour_ts,endpoint,provider,model,failure_type) DO UPDATE SET
             request_count=request_count+excluded.request_count,
             failure_count=failure_count+excluded.failure_count,
             total_elapsed_ms=total_elapsed_ms+excluded.total_elapsed_ms,
             max_elapsed_ms=MAX(max_elapsed_ms,excluded.max_elapsed_ms)",
        )?;
        let mut health = tx.prepare_cached(
            "INSERT INTO provider_health
             (provider,model,penalty,consecutive_failures,demoted,last_failure_ts,last_success_ts,updated_ts)
             VALUES (?1,?2,CASE WHEN ?3=1 THEN 35 ELSE 0 END,
                     CASE WHEN ?3=1 THEN 1 ELSE 0 END,0,
                     CASE WHEN ?3=1 THEN ?4 ELSE 0 END,
                     CASE WHEN ?3=0 THEN ?4 ELSE 0 END,?4)
             ON CONFLICT(provider,model) DO UPDATE SET
             penalty=CASE WHEN ?3=1 THEN MIN(provider_health.penalty+35,100)
                          ELSE MAX(provider_health.penalty-20,0) END,
             consecutive_failures=CASE WHEN ?3=1
                          THEN MIN(provider_health.consecutive_failures+1,65535) ELSE 0 END,
             demoted=CASE WHEN ?3=1 THEN
                          CASE WHEN provider_health.consecutive_failures+1>=3
                                    AND MIN(provider_health.penalty+35,100)>=70
                               THEN 1 ELSE provider_health.demoted END
                          ELSE CASE WHEN MAX(provider_health.penalty-20,0)<=30
                                    THEN 0 ELSE provider_health.demoted END END,
             last_failure_ts=CASE WHEN ?3=1 THEN ?4 ELSE provider_health.last_failure_ts END,
             last_success_ts=CASE WHEN ?3=0 THEN ?4 ELSE provider_health.last_success_ts END,
             updated_ts=?4",
        )?;
        for event in batch {
            if event.record_request {
                request.execute(params![
                    event.ts,
                    event.endpoint,
                    event.provider,
                    event.model,
                    event.status,
                    event.elapsed_ms,
                    i64::from(event.sse_headers),
                    event.failure_type,
                ])?;
                if is_failure(event) {
                    failure.execute(params![
                        event.ts,
                        event.endpoint,
                        event.provider,
                        event.model,
                        event.status,
                        event.elapsed_ms,
                        event.failure_type,
                    ])?;
                }
                let hour = event.ts - event.ts.rem_euclid(3600);
                hourly.execute(params![
                    hour,
                    event.endpoint,
                    event.provider,
                    event.model,
                    event.failure_type,
                    i64::from(event.count_attempt),
                    i64::from(is_failure(event)),
                    event.elapsed_ms,
                ])?;
            } else if is_failure(event) {
                // Streaming starts are counted once at response creation. Record the
                // terminal failure separately without inflating request_count.
                failure.execute(params![
                    event.ts,
                    event.endpoint,
                    event.provider,
                    event.model,
                    event.status,
                    event.elapsed_ms,
                    event.failure_type,
                ])?;
                let hour = event.ts - event.ts.rem_euclid(3600);
                hourly.execute(params![
                    hour,
                    event.endpoint,
                    event.provider,
                    event.model,
                    event.failure_type,
                    i64::from(event.count_attempt),
                    1_i64,
                    event.elapsed_ms,
                ])?;
            }
            if let Some(failed) = health_failure(event) {
                let failed = i64::from(failed);
                // Per-model score plus a provider-wide aggregate used by the
                // existing conservative candidate ranking after restart.
                for model in [event.model.as_str(), "*"] {
                    health.execute(params![event.provider, model, failed, event.ts])?;
                }
            }
        }
    }
    tx.commit()
}
/// Delete request details after seven days and failure/hourly history after 30 days.
fn prune_expired(conn: &mut Connection, now: i64) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM request_events WHERE ts < ?1",
        [now - REQUEST_RETENTION_SECS],
    )?;
    tx.execute(
        "DELETE FROM failure_events WHERE ts < ?1",
        [now - HISTORY_RETENTION_SECS],
    )?;
    tx.execute(
        "DELETE FROM hourly_stats WHERE hour_ts < ?1",
        [now - HISTORY_RETENTION_SECS],
    )?;
    tx.execute(
        "DELETE FROM provider_health WHERE updated_ts < ?1",
        [now - HISTORY_RETENTION_SECS],
    )?;
    tx.commit()
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64
}

fn load_provider_health_from_path(path: &Path, now: i64) -> Vec<ProviderHealthSnapshot> {
    if !path.is_file() {
        return Vec::new();
    }
    let Ok(conn) = Connection::open(path) else {
        return Vec::new();
    };
    if conn.busy_timeout(Duration::from_millis(100)).is_err() {
        return Vec::new();
    }
    let exists: rusqlite::Result<bool> = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='provider_health')",
        [],
        |row| row.get(0),
    );
    if !matches!(exists, Ok(true)) {
        return Vec::new();
    }
    let Ok(mut statement) = conn.prepare(
        "SELECT provider,penalty,consecutive_failures,demoted,last_failure_ts
         FROM provider_health WHERE model='*' AND updated_ts >= ?1",
    ) else {
        return Vec::new();
    };
    let Ok(rows) = statement.query_map([now - HISTORY_RETENTION_SECS], |row| {
        let penalty: i64 = row.get(1)?;
        let failures: i64 = row.get(2)?;
        let demoted: i64 = row.get(3)?;
        let last_failure: i64 = row.get(4)?;
        Ok(ProviderHealthSnapshot {
            provider: row.get(0)?,
            penalty: penalty.clamp(0, 100) as u8,
            consecutive_failures: failures.clamp(0, u16::MAX as i64) as u16,
            demoted: demoted != 0,
            last_failure_secs: last_failure.max(0) as u64,
        })
    }) else {
        return Vec::new();
    };
    rows.filter_map(Result::ok).collect()
}
fn drop_batch(batch: &[Event], metrics: &Metrics) {
    metrics
        .telemetry_dropped
        .fetch_add(batch.len() as u64, Ordering::Relaxed);
    Metrics::inc(&metrics.telemetry_write_failures);
}

fn worker(receiver: Receiver<Event>, path: PathBuf, metrics: Arc<Metrics>) {
    let mut conn = open_database(&path).ok();
    if let Some(database) = conn.as_mut()
        && prune_expired(database, now_secs()).is_err()
    {
        Metrics::inc(&metrics.telemetry_write_failures);
    }
    let mut batches: usize = 0;
    let mut consecutive_failures: u32 = 0;
    while let Ok(first) = receiver.recv() {
        let mut batch = Vec::with_capacity(BATCH_CAPACITY);
        batch.push(first);
        let until = Instant::now() + BATCH_WAIT;
        while batch.len() < BATCH_CAPACITY {
            match receiver.recv_timeout(until.saturating_duration_since(Instant::now())) {
                Ok(event) => batch.push(event),
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
            }
        }
        if conn.is_none() {
            match open_database(&path) {
                Ok(database) => conn = Some(database),
                Err(_) => {
                    drop_batch(&batch, &metrics);
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    let delay_ms = 50_u64
                        .saturating_mul(1_u64 << consecutive_failures.min(7))
                        .min(5000);
                    thread::sleep(Duration::from_millis(delay_ms));
                    continue;
                }
            }
        }
        let write_result = write_batch(conn.as_mut().expect("opened above"), &batch);
        match write_result {
            Ok(()) => {
                consecutive_failures = 0;
                metrics
                    .telemetry_written
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
            }
            Err(_) => {
                drop_batch(&batch, &metrics);
                conn = None; // Reopen after busy, readonly, full-disk or corruption errors.
                consecutive_failures = consecutive_failures.saturating_add(1);
                let delay_ms = 50_u64
                    .saturating_mul(1_u64 << consecutive_failures.min(7))
                    .min(5000);
                thread::sleep(Duration::from_millis(delay_ms));
                continue;
            }
        }
        batches = batches.wrapping_add(1);
        if batches.is_multiple_of(128)
            && let Some(database) = conn.as_mut()
        {
            if prune_expired(database, now_secs()).is_err() {
                Metrics::inc(&metrics.telemetry_write_failures);
            }
            // Checkpoint and truncate WAL from the single writer, never on the request path.
            if database
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .is_err()
            {
                Metrics::inc(&metrics.telemetry_write_failures);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(
        ts: i64,
        provider: &str,
        model: &str,
        status: u16,
        elapsed: u32,
        kind: &'static str,
    ) -> Event {
        Event {
            ts,
            endpoint: "chat",
            provider: safe_label(provider),
            model: safe_label(model),
            status,
            elapsed_ms: elapsed,
            sse_headers: false,
            failure_type: kind,
            record_request: true,
            count_attempt: true,
            health_observation: true,
        }
    }

    #[test]
    fn telemetry_context_restricts_failure_types_and_sanitizes_names() {
        let mut context = TelemetryContext::new("vendor one", "model beta", "untrusted-sql");
        assert_eq!(context.provider, "vendor_one");
        assert_eq!(context.model, "model_beta");
        assert_eq!(context.failure_type, "unknown");
        context.set_failure_type("provider_4xx");
        assert_eq!(context.failure_type, "provider_4xx");
    }
    #[test]
    fn legacy_request_database_migrates_without_losing_existing_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        {
            let old = Connection::open(&path).unwrap();
            old.execute_batch(
                "
                CREATE TABLE request_events (
                    ts INTEGER NOT NULL,
                    endpoint TEXT NOT NULL,
                    status INTEGER NOT NULL,
                    elapsed_ms INTEGER NOT NULL,
                    sse_headers INTEGER NOT NULL
                );
                INSERT INTO request_events VALUES (123,'chat',200,9,0);
            ",
            )
            .unwrap();
        }
        let migrated = open_database(&path).unwrap();
        let row: (String, String, String, i64) = migrated
            .query_row(
                "SELECT provider,model,failure_type,status FROM request_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            ("unknown".into(), "unknown".into(), "unknown".into(), 200)
        );
        let tables: i64 = migrated.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('failure_events','hourly_stats')",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(tables, 2);
    }
    #[test]
    fn queue_overflow_is_nonblocking_and_accounted_for() {
        let (sender, _receiver) = mpsc::sync_channel(1);
        let telemetry = Telemetry { sender };
        let metrics = Metrics::default();
        let start = Instant::now();
        for _ in 0..10000 {
            telemetry.submit("chat", 429, Duration::from_millis(5), false, None, &metrics);
        }
        assert_eq!(metrics.telemetry_dropped.load(Ordering::Relaxed), 9999);
        assert!(start.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn provider_model_failure_and_hourly_metadata_persist() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        let mut conn = open_database(&path).unwrap();
        write_batch(
            &mut conn,
            &[
                event(7200, "provider A", "model/alpha", 200, 42, "success"),
                event(7201, "provider A", "model/alpha", 429, 51, "rate_limited"),
            ],
        )
        .unwrap();
        drop(conn);
        let db = Connection::open(&path).unwrap();
        let row: (String, String, u16, String) = db
            .query_row(
                "SELECT provider,model,status,failure_type FROM request_events WHERE status=429",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (
                "provider_A".into(),
                "model/alpha".into(),
                429,
                "rate_limited".into()
            )
        );
        let failures: i64 = db
            .query_row("SELECT COUNT(*) FROM failure_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(failures, 1);
        let hourly: (i64, i64, i64, i64) = db.query_row(
            "SELECT request_count,failure_count,total_elapsed_ms,max_elapsed_ms FROM hourly_stats WHERE failure_type='rate_limited'",
            [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
        ).unwrap();
        assert_eq!(hourly, (1, 1, 51, 51));
        let health: (i64, i64, i64) = db.query_row(
            "SELECT penalty,consecutive_failures,demoted FROM provider_health WHERE provider='provider_A' AND model='*'",
            [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).unwrap();
        assert_eq!(health, (35, 1, 0));
    }

    #[test]
    fn intermediate_fallback_failure_is_retained_without_a_duplicate_downstream_request() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = open_database(&dir.path().join("fallback.sqlite3")).unwrap();
        let mut failure = event(7200, "down", "down", 503, 50, "provider_5xx");
        failure.record_request = false;
        failure.count_attempt = true;
        let success = event(7201, "live", "fast", 200, 70, "success");
        write_batch(&mut conn, &[failure, success]).unwrap();

        let requests: i64 = conn
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        let failures: i64 = conn
            .query_row("SELECT COUNT(*) FROM failure_events", [], |row| row.get(0))
            .unwrap();
        let fallback: (i64,i64) = conn.query_row(
            "SELECT request_count,failure_count FROM hourly_stats WHERE provider='down' AND failure_type='provider_5xx'",
            [], |row| Ok((row.get(0)?,row.get(1)?)),
        ).unwrap();
        let success_attempts: i64 = conn.query_row(
            "SELECT request_count FROM hourly_stats WHERE provider='live' AND failure_type='success'",
            [], |row| row.get(0),
        ).unwrap();

        assert_eq!(requests, 1);
        assert_eq!(failures, 1);
        assert_eq!(fallback, (1, 1));
        assert_eq!(success_attempts, 1);
    }
    #[test]
    fn terminal_stream_failure_is_recorded_without_counting_a_second_request() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = open_database(&dir.path().join("stream.sqlite3")).unwrap();
        let started = event(7200, "provider A", "model/alpha", 200, 12, "stream_started");
        let mut terminal = event(
            7202,
            "provider A",
            "model/alpha",
            502,
            2000,
            "transport_error",
        );
        terminal.record_request = false;
        terminal.count_attempt = false;
        terminal.sse_headers = true;
        write_batch(&mut conn, &[started, terminal]).unwrap();

        let requests: i64 = conn
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        let failures: i64 = conn
            .query_row("SELECT COUNT(*) FROM failure_events", [], |row| row.get(0))
            .unwrap();
        let aggregate: (i64, i64, i64) = conn.query_row(
            "SELECT request_count,failure_count,max_elapsed_ms FROM hourly_stats WHERE failure_type='transport_error'",
            [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).unwrap();
        assert_eq!(requests, 1);
        assert_eq!(failures, 1);
        assert_eq!(aggregate, (0, 1, 2000));
    }
    #[test]
    fn provider_health_snapshot_restores_only_recent_provider_summary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health.sqlite3");
        let mut conn = open_database(&path).unwrap();
        write_batch(
            &mut conn,
            &[
                event(7200, "provider A", "model/alpha", 503, 1, "provider_5xx"),
                event(7201, "provider A", "model/alpha", 503, 1, "provider_5xx"),
                event(7202, "provider A", "model/alpha", 503, 1, "provider_5xx"),
            ],
        )
        .unwrap();
        drop(conn);

        let snapshots = load_provider_health_from_path(&path, 7203);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].provider, "provider_A");
        assert_eq!(snapshots[0].penalty, 100);
        assert_eq!(snapshots[0].consecutive_failures, 3);
        assert!(snapshots[0].demoted);
        assert_eq!(snapshots[0].last_failure_secs, 7202);

        let stale = load_provider_health_from_path(&path, 7202 + HISTORY_RETENTION_SECS + 1);
        assert!(stale.is_empty());
    }
    #[test]
    fn retention_is_seven_days_for_requests_and_thirty_for_failure_history() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = open_database(&dir.path().join("test.sqlite3")).unwrap();
        let now = 100 * 24 * 3600;
        write_batch(
            &mut conn,
            &[
                event(
                    now - REQUEST_RETENTION_SECS - 1,
                    "p",
                    "m",
                    200,
                    1,
                    "success",
                ),
                event(
                    now - HISTORY_RETENTION_SECS - 1,
                    "p",
                    "m",
                    503,
                    2,
                    "provider_5xx",
                ),
                event(
                    now - REQUEST_RETENTION_SECS - 1,
                    "p",
                    "m",
                    503,
                    3,
                    "provider_5xx",
                ),
                event(now, "p", "m", 200, 4, "success"),
            ],
        )
        .unwrap();
        prune_expired(&mut conn, now).unwrap();
        let requests: i64 = conn
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        let failures: i64 = conn
            .query_row("SELECT COUNT(*) FROM failure_events", [], |row| row.get(0))
            .unwrap();
        let hourly: i64 = conn
            .query_row("SELECT COUNT(*) FROM hourly_stats", [], |row| row.get(0))
            .unwrap();
        assert_eq!(requests, 1);
        assert_eq!(failures, 1);
        assert_eq!(hourly, 3);
    }

    #[test]
    fn labels_are_bounded_and_not_sql_fragments() {
        let label = safe_label(&format!(
            "{}'; DROP TABLE request_events;--",
            "x".repeat(200)
        ));
        assert!(label.len() <= MAX_LABEL_CHARS);
        assert!(!label.contains('\''));
        assert!(!label.contains(';'));
        assert!(label.starts_with("x".repeat(MAX_LABEL_CHARS).as_str()));
    }

    #[test]
    fn corrupt_database_drops_telemetry_without_blocking_or_rewriting_corrupt_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.sqlite3");
        let original = b"not a valid sqlite database";
        std::fs::write(&path, original).unwrap();

        let (sender, receiver) = mpsc::sync_channel(4);
        let metrics = Arc::new(Metrics::default());
        let worker_metrics = Arc::clone(&metrics);
        let worker_path = path.clone();
        let handle = thread::spawn(move || worker(receiver, worker_path, worker_metrics));
        sender
            .try_send(event(123, "provider", "model", 503, 1, "provider_5xx"))
            .unwrap();
        drop(sender);
        handle.join().unwrap();

        assert_eq!(metrics.telemetry_dropped.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.telemetry_write_failures.load(Ordering::Relaxed), 1);
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
    #[test]
    fn locked_database_rejects_batch_without_leaking_partial_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        let mut writer = open_database(&path).unwrap();
        let lock = Connection::open(&path).unwrap();
        lock.execute_batch("BEGIN EXCLUSIVE;").unwrap();
        let events = [event(123, "p", "m", 503, 5, "provider_5xx")];
        let started = Instant::now();
        assert!(write_batch(&mut writer, &events).is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
        lock.execute_batch("ROLLBACK;").unwrap();
        let count: i64 = writer
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn sqlite_schema_excludes_sensitive_request_payload_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        let conn = open_database(&path).unwrap();
        let schema: String = conn
            .query_row(
                "SELECT group_concat(sql, ' ') FROM sqlite_master WHERE type='table'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        for secret in [
            "prompt",
            "token",
            "api_key",
            "authorization",
            "ip_address",
            "request_body",
            "response_body",
            "base_url",
        ] {
            assert!(!schema.contains(secret), "{secret}");
        }
    }
}
