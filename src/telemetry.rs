//! Opt-in, fail-open SQLite metadata. Never persist prompts, credentials, URLs or request bodies.
//! A bounded queue keeps SQLite I/O off the request path; one worker owns the connection.
use crate::metrics::Metrics;
use rusqlite::{Connection, OpenFlags, params};
use std::sync::atomic::Ordering;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError},
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
    sender: SyncSender<TelemetryMessage>,
}

enum TelemetryMessage {
    Record(Event),
    Flush(SyncSender<bool>),
}

/// Attached to a downstream response as an internal extension, never serialized to HTTP.
#[derive(Clone, Debug)]
pub struct TelemetryContext {
    provider: String,
    model: String,
    failure_type: &'static str,
    is_upstream: bool,
    upstream_status: Option<u16>,
    request_id: Option<String>,
    attempt_index: Option<u16>,
    stream_requested: bool,
}

#[derive(Clone, Debug)]
pub struct ProviderHealthSnapshot {
    pub provider: String,
    pub model: String,
    pub penalty: u8,
    pub consecutive_failures: u16,
    pub demoted: bool,
    pub last_failure_secs: u64,
}

impl TelemetryContext {
    pub fn new(provider: &str, model: &str, failure_type: &'static str) -> Self {
        let provider_label = safe_label(provider);
        let model_label = safe_label(model);
        let identity_trusted =
            provider == provider_label && model == model_label && model != "passthrough";
        Self {
            provider: provider_label,
            model: model_label,
            failure_type: allowed_failure_type(failure_type),
            // Lossy labels are fine for diagnostics but never trustworthy
            // as the identity key of persisted provider health.
            // "passthrough" also aliases arbitrary uncatalogued models.
            is_upstream: identity_trusted,
            upstream_status: None,
            request_id: None,
            attempt_index: None,
            stream_requested: false,
        }
    }

    pub fn set_trace(&mut self, request_id: &str, attempt_index: u16, stream: bool) {
        self.request_id = Some(request_id.to_owned());
        self.attempt_index = Some(attempt_index);
        self.stream_requested = stream;
    }

    pub fn ensure_request_id(&mut self, request_id: &str) {
        if self.request_id.is_none() {
            self.request_id = Some(request_id.to_owned());
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

    pub(crate) fn local(status: u16, sse_headers: bool) -> Self {
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
        | "client_cancelled"
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
struct ShadowRecord {
    baseline_provider: String,
    baseline_model: String,
    suggested_provider: Option<String>,
    suggested_model: Option<String>,
    disagreed: bool,
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
    request_id: Option<String>,
    attempt_index: Option<u16>,
    stream_requested: bool,
    shadow: Option<Box<ShadowRecord>>,
}

impl Telemetry {
    /// A bounded FIFO barrier for the final SQLite batch on graceful shutdown.
    pub fn flush(&self, timeout: Duration) -> bool {
        let started = Instant::now();
        let (ack, receipt) = mpsc::sync_channel(1);
        let mut message = TelemetryMessage::Flush(ack);
        loop {
            match self.sender.try_send(message) {
                Ok(()) => break,
                Err(TrySendError::Full(pending)) => {
                    if started.elapsed() >= timeout {
                        return false;
                    }
                    message = pending;
                    thread::sleep(Duration::from_millis(5));
                }
                Err(TrySendError::Disconnected(_)) => return false,
            }
        }
        receipt
            .recv_timeout(timeout.saturating_sub(started.elapsed()))
            .unwrap_or(false)
    }
    pub fn start(data_dir: &Path, metrics: Arc<Metrics>) -> Result<Self, String> {
        let directory = data_dir.join("telemetry");
        if std::fs::symlink_metadata(&directory).is_ok_and(|m| !m.file_type().is_dir()) {
            return Err("telemetry directory must be a real directory".into());
        }
        std::fs::create_dir_all(&directory)
            .map_err(|e| format!("cannot create telemetry data directory: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
                .map_err(|e| format!("cannot secure telemetry directory: {e}"))?;
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
    pub fn observe_stream_health(
        &self,
        provider: &str,
        model: &str,
        failure_type: &'static str,
        elapsed: Duration,
        trace: (&str, u16),
        metrics: &Metrics,
    ) {
        let mut context = TelemetryContext::new(provider, model, failure_type);
        context.set_trace(trace.0, trace.1, true);
        if matches!(failure_type, "timeout" | "local_error") {
            // Stream idle/size policy is local admission control, not proof
            // of supplier degradation. Preserve diagnostics but avoid
            // poisoning persistent provider health and Auto rankings.
            context.set_health_observation(false);
        }
        let status = match failure_type {
            "success" => 200,
            "client_cancelled" => 499,
            _ => 502,
        };
        self.enqueue_observation("chat", status, elapsed, context, false, metrics);
    }

    /// A bounded asynchronous Shadow observation. Never claim the suggested
    /// route was executed; actual outcomes are separate request_events rows.
    pub(crate) fn observe_shadow(
        &self,
        request_id: &str,
        baseline: (&str, &str),
        suggested: Option<(&str, &str)>,
        metrics: &Metrics,
    ) {
        if request_id.is_empty() {
            return;
        }
        let record = ShadowRecord {
            baseline_provider: safe_label(baseline.0),
            baseline_model: safe_label(baseline.1),
            suggested_provider: suggested.map(|s| safe_label(s.0)),
            suggested_model: suggested.map(|s| safe_label(s.1)),
            disagreed: suggested.is_some_and(|s| s != baseline),
        };
        let event = Event {
            ts: now_secs(),
            endpoint: "chat",
            provider: "unknown".into(),
            model: "unknown".into(),
            status: 0,
            elapsed_ms: 0,
            sse_headers: false,
            failure_type: "success",
            record_request: false,
            count_attempt: false,
            health_observation: false,
            request_id: Some(request_id.to_owned()),
            attempt_index: None,
            stream_requested: false,
            shadow: Some(Box::new(record)),
        };
        if self
            .sender
            .try_send(TelemetryMessage::Record(event))
            .is_err()
        {
            Metrics::inc(&metrics.telemetry_dropped);
        }
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
            request_id: context.request_id,
            attempt_index: context.attempt_index,
            stream_requested: context.stream_requested,
            shadow: None,
        };
        if self
            .sender
            .try_send(TelemetryMessage::Record(event))
            .is_err()
        {
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
        let stream_requested = context.stream_requested || sse_headers;
        let request_id = context.request_id.clone();
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
            request_id,
            attempt_index: context.attempt_index,
            stream_requested,
            shadow: None,
        };
        if self
            .sender
            .try_send(TelemetryMessage::Record(event))
            .is_err()
        {
            Metrics::inc(&metrics.telemetry_dropped);
        }
    }
}

fn has_column(conn: &Connection, table: &str, name: &str) -> rusqlite::Result<bool> {
    // Table names are internal fixed constants, not user input.
    let mut statement = conn.prepare(&format!("PRAGMA table_info({table})"))?;
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
        if !has_column(conn, "request_events", name)? {
            conn.execute_batch(definition)?;
        }
    }
    Ok(())
}
/// Existing installations have a three-outcome CHECK constraint. Rebuild
/// only that small, bounded history table in one transaction so a restart
/// retains old trace links while recording local terminal failures precisely.
fn migrate_stream_terminal_outcomes(conn: &mut Connection) -> rusqlite::Result<()> {
    let schema: String = conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='stream_terminals'",
        [],
        |row| row.get(0),
    )?;
    if schema.contains("'local_error'") {
        return Ok(());
    }
    let tx = conn.transaction()?;
    tx.execute_batch(
        "CREATE TABLE stream_terminals_migrated (
            ts INTEGER NOT NULL,
            request_id TEXT NOT NULL,
            attempt_index INTEGER NOT NULL,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            outcome TEXT NOT NULL CHECK (outcome IN
                ('success','transport_error','invalid_response',
                 'timeout','local_error','client_cancelled')),
            elapsed_ms INTEGER NOT NULL,
            PRIMARY KEY (request_id, attempt_index)
         );
         INSERT INTO stream_terminals_migrated
             (ts,request_id,attempt_index,provider,model,outcome,elapsed_ms)
         SELECT ts,request_id,attempt_index,provider,model,outcome,elapsed_ms
         FROM stream_terminals;
         DROP TABLE stream_terminals;
         ALTER TABLE stream_terminals_migrated RENAME TO stream_terminals;
         CREATE INDEX stream_terminals_ts ON stream_terminals(ts);",
    )?;
    tx.commit()
}

/// Responses uses the Chat upstream bridge but needs its own telemetry
/// endpoint label; merging it into Chat would contaminate Auto scoring.
/// SQLite cannot ALTER a CHECK constraint, so rebuild each history table
/// in a transaction, preserving rows and the timestamp index.
fn migrate_responses_endpoint_constraints(conn: &mut Connection) -> rusqlite::Result<()> {
    for table in ["request_events", "failure_events"] {
        let schema: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
            [table],
            |row| row.get(0),
        )?;
        // SQLite stores the original CREATE TABLE statement, preserving
        // arbitrary whitespace and keyword casing. Treat logically
        // equivalent legacy CHECK clauses alike; otherwise a perfectly
        // valid old database may silently reject every new Responses row.
        let normalized = schema
            .chars()
            .filter(|character| !character.is_ascii_whitespace())
            .collect::<String>()
            .to_ascii_lowercase();
        if !normalized.contains("endpointin('chat','embeddings')") {
            continue;
        }
        let tx = conn.transaction()?;
        if table == "request_events" {
            tx.execute_batch(
                "CREATE TABLE request_events_upgraded (
                    ts INTEGER NOT NULL,
                    endpoint TEXT NOT NULL CHECK (endpoint IN ('chat','embeddings','responses')),
                    provider TEXT NOT NULL, model TEXT NOT NULL,
                    status INTEGER NOT NULL, elapsed_ms INTEGER NOT NULL,
                    sse_headers INTEGER NOT NULL CHECK (sse_headers IN (0,1)),
                    identity_verified INTEGER NOT NULL DEFAULT 0 CHECK (identity_verified IN (0,1)),
                    failure_type TEXT NOT NULL CHECK (failure_type IN (
                        'success','stream_started','rate_limited','credential_error',
                        'caller_error','provider_4xx','provider_5xx','transport_error','invalid_response',
                        'timeout','upstream_unavailable','circuit_open','aggregation_busy',
                        'local_error','capability_mismatch','unknown')),
                    request_id TEXT, attempt_index INTEGER,
                    stream_requested INTEGER NOT NULL DEFAULT 0
                );
                INSERT INTO request_events_upgraded
                    (ts,endpoint,provider,model,status,elapsed_ms,sse_headers,
                     identity_verified,failure_type,request_id,attempt_index,stream_requested)
                SELECT ts,endpoint,provider,model,status,elapsed_ms,sse_headers,
                       identity_verified,failure_type,request_id,attempt_index,stream_requested
                  FROM request_events;
                DROP TABLE request_events;
                ALTER TABLE request_events_upgraded RENAME TO request_events;
                CREATE INDEX request_events_ts ON request_events(ts);",
            )?;
        } else {
            tx.execute_batch(
                "CREATE TABLE failure_events_upgraded (
                    ts INTEGER NOT NULL,
                    endpoint TEXT NOT NULL CHECK (endpoint IN ('chat','embeddings','responses')),
                    provider TEXT NOT NULL, model TEXT NOT NULL,
                    status INTEGER NOT NULL, elapsed_ms INTEGER NOT NULL,
                    failure_type TEXT NOT NULL CHECK (failure_type NOT IN ('success','stream_started')),
                    request_id TEXT, attempt_index INTEGER,
                    stream_requested INTEGER NOT NULL DEFAULT 0
                );
                INSERT INTO failure_events_upgraded
                    (ts,endpoint,provider,model,status,elapsed_ms,failure_type,
                     request_id,attempt_index,stream_requested)
                SELECT ts,endpoint,provider,model,status,elapsed_ms,failure_type,
                       request_id,attempt_index,stream_requested
                  FROM failure_events;
                DROP TABLE failure_events;
                ALTER TABLE failure_events_upgraded RENAME TO failure_events;
                CREATE INDEX failure_events_ts ON failure_events(ts);",
            )?;
        }
        tx.commit()?;
    }
    Ok(())
}

fn open_database(path: &Path) -> rusqlite::Result<Connection> {
    // Avoid opening a pre-existing symlink or special file in the service
    // data directory. The normal path is owned by this local daemon.
    if std::fs::symlink_metadata(path).is_ok_and(|meta| !meta.file_type().is_file()) {
        return Err(rusqlite::Error::InvalidPath(path.to_path_buf()));
    }
    // The pre-open symlink_metadata guard is subject to a rename race.
    // Enforce NOFOLLOW at the actual SQLite file-open boundary as well:
    // telemetry is a local writable database, never an arbitrary symlink
    // target supplied by another process.
    let mut conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    conn.busy_timeout(Duration::from_secs(1))?;
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
            endpoint TEXT NOT NULL CHECK (endpoint IN ('chat','embeddings','responses')),
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            status INTEGER NOT NULL,
            elapsed_ms INTEGER NOT NULL,
            sse_headers INTEGER NOT NULL CHECK (sse_headers IN (0,1)),
            identity_verified INTEGER NOT NULL DEFAULT 0 CHECK (identity_verified IN (0,1)),
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
            identity_verified INTEGER NOT NULL DEFAULT 0 CHECK (identity_verified IN (0,1)),
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
            identity_verified INTEGER NOT NULL DEFAULT 0 CHECK (identity_verified IN (0,1)),
            PRIMARY KEY (provider, model)
        );
        CREATE INDEX IF NOT EXISTS provider_health_updated_ts ON provider_health(updated_ts);
        CREATE TABLE IF NOT EXISTS shadow_events (
            ts INTEGER NOT NULL,
            request_id TEXT NOT NULL,
            baseline_provider TEXT NOT NULL,
            baseline_model TEXT NOT NULL,
            suggested_provider TEXT,
            suggested_model TEXT,
            available INTEGER NOT NULL CHECK (available IN (0,1)),
            disagreed INTEGER NOT NULL CHECK (disagreed IN (0,1))
        );
        CREATE INDEX IF NOT EXISTS shadow_events_ts ON shadow_events(ts);
        CREATE TABLE IF NOT EXISTS stream_terminals (
            ts INTEGER NOT NULL,
            request_id TEXT NOT NULL,
            attempt_index INTEGER NOT NULL,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            outcome TEXT NOT NULL CHECK (outcome IN
                ('success','transport_error','invalid_response',
                 'timeout','local_error','client_cancelled')),
            elapsed_ms INTEGER NOT NULL,
            PRIMARY KEY (request_id, attempt_index)
        );
        CREATE INDEX IF NOT EXISTS stream_terminals_ts ON stream_terminals(ts);
    ",
    )?;
    migrate_stream_terminal_outcomes(&mut conn)?;
    for table in ["request_events", "failure_events"] {
        for (column, definition) in [
            ("request_id", "TEXT"),
            ("attempt_index", "INTEGER"),
            ("stream_requested", "INTEGER NOT NULL DEFAULT 0"),
        ] {
            if !has_column(&conn, table, column)? {
                conn.execute_batch(&format!(
                    "ALTER TABLE {table} ADD COLUMN {column} {definition}"
                ))?;
            }
        }
    }
    // Older request and hourly aggregates contain only lossy labels.
    // Fail closed for Auto ranking instead of treating them as exact IDs.
    for table in ["request_events", "hourly_stats"] {
        if !has_column(&conn, table, "identity_verified")? {
            conn.execute_batch(&format!(
                "ALTER TABLE {table} ADD COLUMN identity_verified INTEGER NOT NULL DEFAULT 0"
            ))?;
        }
    }
    // Existing installations still carry two-column endpoint CHECK clauses.
    // Complete all column additions first, then preserve their historical
    // rows during the one-time responses endpoint schema upgrade.
    migrate_responses_endpoint_constraints(&mut conn)?;
    // Old databases stored only lossy labels in provider_health. A migration
    // must not silently treat those potentially collided rows as verified.
    if !has_column(&conn, "provider_health", "identity_verified")? {
        conn.execute_batch(
            "ALTER TABLE provider_health
             ADD COLUMN identity_verified INTEGER NOT NULL DEFAULT 0",
        )?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
    }
    secure_sidecars(path);
    Ok(conn)
}

// SQLite creates WAL and SHM lazily. Protect each new sidecar as soon as the
// writer has created it, not only the main database file.
#[cfg(unix)]
fn secure_sidecars(path: &Path) {
    use std::{
        ffi::OsString,
        fs::{OpenOptions, Permissions},
        os::unix::fs::{OpenOptionsExt, PermissionsExt},
    };
    for suffix in ["-wal", "-shm"] {
        let mut name = OsString::from(path.as_os_str());
        name.push(suffix);
        let sidecar = PathBuf::from(name);
        // Open an FD with O_NOFOLLOW and fchmod THAT FD, so a concurrent
        // rename/symlink replacement can never redirect chmod elsewhere.
        // O_NONBLOCK prevents a substituted FIFO from blocking the writer.
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&sidecar)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => {
                crate::ops_log::warn("telemetry_sidecar_not_regular");
                continue;
            }
        };
        if !file
            .metadata()
            .is_ok_and(|metadata| metadata.file_type().is_file())
        {
            crate::ops_log::warn("telemetry_sidecar_not_regular");
            continue;
        }
        if file.set_permissions(Permissions::from_mode(0o600)).is_err() {
            crate::ops_log::warn("telemetry_permissions_failed");
        }
    }
}

#[cfg(not(unix))]
fn secure_sidecars(_path: &Path) {}

fn is_failure(event: &Event) -> bool {
    !matches!(
        event.failure_type,
        "success" | "stream_started" | "client_cancelled"
    )
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
             (ts,endpoint,provider,model,status,elapsed_ms,sse_headers,failure_type,request_id,attempt_index,stream_requested,identity_verified)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
        )?;
        let mut failure = tx.prepare_cached(
            "INSERT INTO failure_events
             (ts,endpoint,provider,model,status,elapsed_ms,failure_type,request_id,attempt_index,stream_requested)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        )?;
        let mut hourly = tx.prepare_cached(
            "INSERT INTO hourly_stats
             (hour_ts,endpoint,provider,model,failure_type,request_count,failure_count,total_elapsed_ms,max_elapsed_ms,identity_verified)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?8,?9)
             ON CONFLICT(hour_ts,endpoint,provider,model,failure_type) DO UPDATE SET
             request_count=request_count+excluded.request_count,
             failure_count=failure_count+excluded.failure_count,
             total_elapsed_ms=total_elapsed_ms+excluded.total_elapsed_ms,
             max_elapsed_ms=MAX(max_elapsed_ms,excluded.max_elapsed_ms),
             identity_verified=MIN(hourly_stats.identity_verified,excluded.identity_verified)",
        )?;
        let mut shadow = tx.prepare_cached(
            "INSERT INTO shadow_events
             (ts,request_id,baseline_provider,baseline_model,suggested_provider,suggested_model,available,disagreed)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        )?;
        let mut terminal = tx.prepare_cached(
            "INSERT OR IGNORE INTO stream_terminals
             (ts,request_id,attempt_index,provider,model,outcome,elapsed_ms)
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
        )?;
        let mut health = tx.prepare_cached(
            "INSERT INTO provider_health
             (provider,model,penalty,consecutive_failures,demoted,last_failure_ts,last_success_ts,updated_ts,identity_verified)
             VALUES (?1,?2,CASE WHEN ?3=1 THEN 35 ELSE 0 END,
                     CASE WHEN ?3=1 THEN 1 ELSE 0 END,0,
                     CASE WHEN ?3=1 THEN ?4 ELSE 0 END,
                     CASE WHEN ?3=0 THEN ?4 ELSE 0 END,?4,1)
             ON CONFLICT(provider,model) DO UPDATE SET
             penalty=CASE WHEN provider_health.identity_verified=0
                          THEN CASE WHEN ?3=1 THEN 35 ELSE 0 END
                          WHEN ?4 < provider_health.last_failure_ts
                               OR ?4-provider_health.last_failure_ts>300
                          THEN CASE WHEN ?3=1 THEN 35 ELSE 0 END
                          WHEN ?3=1 THEN MIN(provider_health.penalty+35,100)
                          ELSE MAX(provider_health.penalty-20,0) END,
             consecutive_failures=CASE WHEN provider_health.identity_verified=0
                          THEN CASE WHEN ?3=1 THEN 1 ELSE 0 END
                          WHEN ?3=0 THEN 0
                          WHEN ?4 < provider_health.last_failure_ts
                               OR ?4-provider_health.last_failure_ts>300 THEN 1
                          ELSE MIN(provider_health.consecutive_failures+1,65535) END,
             demoted=CASE WHEN provider_health.identity_verified=0 THEN 0
                          WHEN ?4 < provider_health.last_failure_ts
                               OR ?4-provider_health.last_failure_ts>300 THEN 0
                          WHEN ?3=1 THEN
                          CASE WHEN provider_health.consecutive_failures+1>=3
                                    AND MIN(provider_health.penalty+35,100)>=70
                               THEN 1 ELSE provider_health.demoted END
                          ELSE CASE WHEN MAX(provider_health.penalty-20,0)<=30
                                    THEN 0 ELSE provider_health.demoted END END,
             last_failure_ts=CASE WHEN ?3=1 THEN ?4
                                  WHEN provider_health.identity_verified=0 THEN 0
                                  ELSE provider_health.last_failure_ts END,
             last_success_ts=CASE WHEN ?3=0 THEN ?4
                                  WHEN provider_health.identity_verified=0 THEN 0
                                  ELSE provider_health.last_success_ts END,
             updated_ts=?4,identity_verified=1",
        )?;
        for event in batch {
            if let Some(observation) = &event.shadow {
                shadow.execute(params![
                    event.ts,
                    event.request_id,
                    observation.baseline_provider,
                    observation.baseline_model,
                    observation.suggested_provider,
                    observation.suggested_model,
                    i64::from(observation.suggested_provider.is_some()),
                    i64::from(observation.disagreed),
                ])?;
            }
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
                    event.request_id,
                    event.attempt_index,
                    i64::from(event.stream_requested),
                    i64::from(event.health_observation),
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
                        event.request_id,
                        event.attempt_index,
                        i64::from(event.stream_requested),
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
                    i64::from(event.health_observation),
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
                    event.request_id,
                    event.attempt_index,
                    i64::from(event.stream_requested),
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
                    i64::from(event.health_observation),
                ])?;
            }
            // Only actual SSE terminal callbacks enter this table. The initial
            // stream_started request row and failed candidate attempts cannot
            // masquerade as a completed stream or a client cancellation.
            if event.stream_requested
                && !event.record_request
                && !event.count_attempt
                && event.shadow.is_none()
                && let (Some(request_id), Some(attempt)) = (&event.request_id, event.attempt_index)
            {
                terminal.execute(params![
                    event.ts,
                    request_id,
                    attempt,
                    event.provider,
                    event.model,
                    event.failure_type,
                    event.elapsed_ms
                ])?;
            }
            if let Some(failed) = health_failure(event) {
                let failed = i64::from(failed);
                // Restore uses per-provider/model candidate rows, never '*'.
                // Avoid writing redundant provider-wide rows on every event.
                health.execute(params![event.provider, event.model, failed, event.ts])?;
            }
        }
    }
    tx.commit()
}
/// Delete request details after seven days and failure/hourly history after 30 days.
fn prune_expired(conn: &mut Connection, now: i64) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM request_events WHERE ts < ?1 OR ts > ?2",
        params![now - REQUEST_RETENTION_SECS, now.saturating_add(300)],
    )?;
    tx.execute(
        "DELETE FROM failure_events WHERE ts < ?1 OR ts > ?2",
        params![now - HISTORY_RETENTION_SECS, now.saturating_add(300)],
    )?;
    tx.execute(
        "DELETE FROM hourly_stats WHERE hour_ts < ?1 OR hour_ts > ?2",
        params![now - HISTORY_RETENTION_SECS, now.saturating_add(3600)],
    )?;
    tx.execute(
        "DELETE FROM shadow_events WHERE ts < ?1 OR ts > ?2",
        params![now - REQUEST_RETENTION_SECS, now.saturating_add(300)],
    )?;
    tx.execute(
        "DELETE FROM stream_terminals WHERE ts < ?1 OR ts > ?2",
        params![now - REQUEST_RETENTION_SECS, now.saturating_add(300)],
    )?;
    tx.execute(
        "DELETE FROM provider_health WHERE model='*' OR updated_ts < ?1 OR updated_ts > ?2",
        params![now - HISTORY_RETENTION_SECS, now.saturating_add(300)],
    )?;
    tx.commit()
}

/// Prefer availability to losing all future telemetry when the writer
/// reaches its page ceiling; reclaim only a bounded oldest slice.
fn recover_page_capacity(conn: &mut Connection, now: i64) -> rusqlite::Result<bool> {
    prune_expired(conn, now)?;
    let pages: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let ceiling: i64 = conn.query_row("PRAGMA max_page_count", [], |row| row.get(0))?;
    if pages.saturating_add(16) < ceiling {
        return Ok(false);
    }
    let tx = conn.transaction()?;
    let removed_requests = tx.execute(
        "DELETE FROM request_events WHERE rowid IN
         (SELECT rowid FROM request_events ORDER BY ts LIMIT 8192)",
        [],
    )?;
    let removed_failures = tx.execute(
        "DELETE FROM failure_events WHERE rowid IN
         (SELECT rowid FROM failure_events ORDER BY ts LIMIT 2048)",
        [],
    )?;
    let removed_shadows = tx.execute(
        "DELETE FROM shadow_events WHERE rowid IN
         (SELECT rowid FROM shadow_events ORDER BY ts LIMIT 2048)",
        [],
    )?;
    let removed_terminals = tx.execute(
        "DELETE FROM stream_terminals WHERE rowid IN
         (SELECT rowid FROM stream_terminals ORDER BY ts LIMIT 2048)",
        [],
    )?;
    // Aggregates can exhaust the page budget even when all event tables are empty.
    let removed_hourly = tx.execute(
        "DELETE FROM hourly_stats WHERE rowid IN
         (SELECT rowid FROM hourly_stats ORDER BY hour_ts LIMIT 2048)",
        [],
    )?;
    // Preserve Auto's health history when ordinary diagnostics have already
    // freed space. Only evict the oldest advisory penalties if no other
    // rows are reclaimable. Otherwise a busy event table would needlessly
    // erase the very history used to survive provider outages on restart.
    let reclaimed_diagnostics =
        removed_requests + removed_failures + removed_shadows + removed_terminals + removed_hourly;
    let removed_health = if reclaimed_diagnostics == 0 {
        tx.execute(
            "DELETE FROM provider_health WHERE rowid IN
             (SELECT rowid FROM provider_health ORDER BY updated_ts LIMIT 512)",
            [],
        )?
    } else {
        0
    };
    tx.commit()?;
    Ok(reclaimed_diagnostics + removed_health > 0)
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64
}

fn load_provider_health_from_path(path: &Path, now: i64) -> Vec<ProviderHealthSnapshot> {
    // Startup health restore is advisory. Never create, mutate or follow a
    // substituted database file while rebuilding Auto's local health hints.
    // The telemetry writer owns schema migration and writable connections.
    if !std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_file()) {
        return Vec::new();
    }
    let Ok(conn) = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    ) else {
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
        "SELECT provider,model,penalty,consecutive_failures,demoted,last_failure_ts
         FROM provider_health
         WHERE model!='*' AND identity_verified=1
           AND updated_ts BETWEEN ?1 AND ?2
           AND last_failure_ts BETWEEN 0 AND ?2
         ORDER BY updated_ts DESC LIMIT 256",
    ) else {
        return Vec::new();
    };
    // Restore runs before the writer prunes skewed timestamps. If the system
    // clock moved backwards after recording a failure, a future-dated
    // demotion must not remain active through an otherwise healthy restart.
    let Ok(rows) = statement.query_map(
        [
            now.saturating_sub(HISTORY_RETENTION_SECS),
            now.saturating_add(300),
        ],
        |row| {
            let penalty: i64 = row.get(2)?;
            let failures: i64 = row.get(3)?;
            let demoted: i64 = row.get(4)?;
            let last_failure: i64 = row.get(5)?;
            Ok(ProviderHealthSnapshot {
                provider: row.get(0)?,
                model: row.get(1)?,
                penalty: penalty.clamp(0, 100) as u8,
                consecutive_failures: failures.clamp(0, u16::MAX as i64) as u16,
                demoted: demoted != 0,
                last_failure_secs: last_failure.max(0) as u64,
            })
        },
    ) else {
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

/// The background worker is the only writer. Test the FULL -> bounded
/// reclamation -> retry sequence with a capped SQLite connection.
fn write_batch_with_recovery(
    conn: &mut Connection,
    batch: &[Event],
    now: i64,
) -> rusqlite::Result<()> {
    let result = write_batch(conn, batch);
    match result {
        Err(error)
            if matches!(&error, rusqlite::Error::SqliteFailure(sqlite, _)
                if sqlite.code == rusqlite::ErrorCode::DiskFull)
                && recover_page_capacity(conn, now).unwrap_or(false) =>
        {
            crate::ops_log::warn("telemetry_page_capacity_recovered");
            write_batch(conn, batch)
        }
        result => result,
    }
}

fn worker(receiver: Receiver<TelemetryMessage>, path: PathBuf, metrics: Arc<Metrics>) {
    let mut conn = match open_database(&path) {
        Ok(database) => Some(database),
        Err(_) => {
            // The batch drop path below records the failed write exactly
            // once; the initial open failure still emits an explicit warning.
            crate::ops_log::warn("telemetry_open_failed");
            None
        }
    };
    if let Some(database) = conn.as_mut()
        && prune_expired(database, now_secs()).is_err()
    {
        Metrics::inc(&metrics.telemetry_write_failures);
        crate::ops_log::warn("telemetry_prune_failed");
    }
    let mut batches: usize = 0;
    let mut consecutive_failures: u32 = 0;
    loop {
        let first = match receiver.recv_timeout(Duration::from_secs(300)) {
            Ok(TelemetryMessage::Record(event)) => event,
            Ok(TelemetryMessage::Flush(ack)) => {
                // An empty barrier is still a failed persistence check when
                // SQLite never opened or the worker lost its connection.
                let _ = ack.send(conn.is_some());
                continue;
            }
            Err(RecvTimeoutError::Timeout) => {
                if conn
                    .as_mut()
                    .is_some_and(|db| prune_expired(db, now_secs()).is_err())
                {
                    Metrics::inc(&metrics.telemetry_write_failures);
                    crate::ops_log::warn("telemetry_prune_failed");
                }
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        };
        let mut batch = Vec::with_capacity(BATCH_CAPACITY);
        batch.push(first);
        let mut flush_ack = None;
        let until = Instant::now() + BATCH_WAIT;
        while batch.len() < BATCH_CAPACITY {
            match receiver.recv_timeout(until.saturating_duration_since(Instant::now())) {
                Ok(TelemetryMessage::Record(event)) => batch.push(event),
                Ok(TelemetryMessage::Flush(ack)) => {
                    flush_ack = Some(ack);
                    break;
                }
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
            }
        }
        if conn.is_none() {
            match open_database(&path) {
                Ok(database) => conn = Some(database),
                Err(_) => {
                    drop_batch(&batch, &metrics);
                    if let Some(ack) = flush_ack.take() {
                        let _ = ack.send(false);
                    }
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures.is_power_of_two() {
                        crate::ops_log::warn("telemetry_open_failed");
                    }
                    let delay_ms = 50_u64
                        .saturating_mul(1_u64 << consecutive_failures.min(7))
                        .min(5000);
                    thread::sleep(Duration::from_millis(delay_ms));
                    continue;
                }
            }
        }
        let write_result =
            write_batch_with_recovery(conn.as_mut().expect("opened above"), &batch, now_secs());
        match write_result {
            Ok(()) => {
                secure_sidecars(&path);
                if let Some(ack) = flush_ack.take() {
                    let _ = ack.send(true);
                }
                consecutive_failures = 0;
                metrics
                    .telemetry_written
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
            }
            Err(_) => {
                drop_batch(&batch, &metrics);
                if let Some(ack) = flush_ack.take() {
                    let _ = ack.send(false);
                }
                if conn
                    .as_mut()
                    .is_some_and(|database| prune_expired(database, now_secs()).is_err())
                {
                    crate::ops_log::warn("telemetry_prune_failed");
                }
                conn = None; // Reopen after busy, readonly, full-disk or corruption errors.
                consecutive_failures = consecutive_failures.saturating_add(1);
                if consecutive_failures.is_power_of_two() {
                    crate::ops_log::warn("telemetry_write_failed");
                }
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
                crate::ops_log::warn("telemetry_prune_failed");
            }
            // Checkpoint and truncate WAL from the single writer, never on the request path.
            if database
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .is_err()
            {
                Metrics::inc(&metrics.telemetry_write_failures);
                crate::ops_log::warn("telemetry_checkpoint_failed");
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
            request_id: None,
            attempt_index: None,
            stream_requested: false,
            shadow: None,
        }
    }

    #[test]
    fn responses_and_chat_events_commit_in_same_batch_without_losing_failures() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::default());
        let telemetry = Telemetry::start(dir.path(), Arc::clone(&metrics)).unwrap();
        let mut failed = TelemetryContext::new("physical", "model", "provider_5xx");
        failed.set_trace("responses-trace", 1, false);
        telemetry.submit(
            "responses",
            503,
            Duration::from_millis(5),
            false,
            Some(failed),
            &metrics,
        );
        telemetry.submit("chat", 200, Duration::from_millis(2), false, None, &metrics);
        assert!(telemetry.flush(Duration::from_secs(3)));
        let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
        let total: i64 = db
            .query_row("SELECT COUNT(*) FROM request_events", [], |r| r.get(0))
            .unwrap();
        let response_failures: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM failure_events WHERE endpoint='responses' AND status=503",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            total, 2,
            "a Responses event must not roll back a Chat event"
        );
        assert_eq!(response_failures, 1);
        let summary = crate::scoring::read_scoring(dir.path(), 1).unwrap();
        assert_eq!(
            summary["candidate_count"], 0,
            "Responses upstream results must not be mislabeled as Chat quality evidence"
        );
    }

    #[test]
    fn old_endpoint_checks_migrate_without_losing_failure_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old-telemetry.sqlite3");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(
            "CREATE TABLE request_events (
                ts INTEGER NOT NULL,
                endpoint TEXT NOT NULL CHECK (endpoint IN ('chat', 'embeddings')),
                provider TEXT NOT NULL, model TEXT NOT NULL,
                status INTEGER NOT NULL, elapsed_ms INTEGER NOT NULL,
                sse_headers INTEGER NOT NULL,
                identity_verified INTEGER NOT NULL DEFAULT 0,
                failure_type TEXT NOT NULL,
                request_id TEXT, attempt_index INTEGER,
                stream_requested INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX request_events_ts ON request_events(ts);
            CREATE TABLE failure_events (
                ts INTEGER NOT NULL,
                endpoint TEXT NOT NULL check (ENDPOINT in ('chat', 'embeddings')),
                provider TEXT NOT NULL, model TEXT NOT NULL,
                status INTEGER NOT NULL, elapsed_ms INTEGER NOT NULL,
                failure_type TEXT NOT NULL,
                request_id TEXT, attempt_index INTEGER,
                stream_requested INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX failure_events_ts ON failure_events(ts);
            INSERT INTO request_events VALUES (1000,'chat','old','m',503,10,0,1,
                                              'provider_5xx','old-trace',1,0);
            INSERT INTO failure_events VALUES (1000,'chat','old','m',503,10,
                                              'provider_5xx','old-trace',1,0);",
        )
        .unwrap();
        drop(db);
        for _ in 0..2 {
            let db = open_database(&path).unwrap();
            db.execute(
                "INSERT INTO request_events
                 (ts,endpoint,provider,model,status,elapsed_ms,sse_headers,failure_type)
                 VALUES (2000,'responses','new','m',200,3,0,'success')",
                [],
            )
            .unwrap();
            db.execute(
                "INSERT INTO failure_events
                 (ts,endpoint,provider,model,status,elapsed_ms,failure_type)
                 VALUES (2000,'responses','new','m',503,3,'provider_5xx')",
                [],
            )
            .unwrap();
            let preserved: i64 = db
                .query_row(
                    "SELECT COUNT(*) FROM request_events WHERE request_id='old-trace'
                 AND endpoint='chat' AND identity_verified=1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                preserved, 1,
                "upgrading or reopening erased old request evidence"
            );
            let old_failure: i64 = db
                .query_row(
                    "SELECT COUNT(*) FROM failure_events WHERE request_id='old-trace'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(old_failure, 1);
        }
    }

    #[test]
    fn local_stream_timeouts_and_size_limits_do_not_poison_persisted_health() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::default());
        let telemetry = Telemetry::start(dir.path(), Arc::clone(&metrics)).unwrap();
        for (trace, kind) in [
            ("trace-stream-idle", "timeout"),
            ("trace-stream-size", "local_error"),
        ] {
            let mut started = TelemetryContext::new("p", "model", "stream_started");
            started.set_trace(trace, 1, true);
            telemetry.submit(
                "chat",
                200,
                Duration::from_millis(1),
                true,
                Some(started),
                &metrics,
            );
            telemetry.observe_stream_health(
                "p",
                "model",
                kind,
                Duration::from_secs(15),
                (trace, 1),
                &metrics,
            );
        }
        assert!(telemetry.flush(Duration::from_secs(3)));
        let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
        let health: i64 = db
            .query_row("SELECT COUNT(*) FROM provider_health", [], |row| row.get(0))
            .unwrap();
        assert_eq!(health, 0, "local policy must not demote provider health");
        let terminals: i64 = db
            .query_row("SELECT COUNT(*) FROM stream_terminals", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(terminals, 2, "diagnostic terminal outcomes were lost");
        let score = crate::scoring::read_scoring(dir.path(), 1).unwrap();
        assert_eq!(
            score["stream_terminal_metrics"]["local_policy_terminated"],
            2
        );
        assert_eq!(score["stream_terminal_metrics"]["upstream_failed"], 0);
        assert!(
            score["stream_terminal_metrics"]["provider_completion_rate_excluding_local_policy"]
                .is_null(),
            "two local-only failures must not be interpreted as 0% provider reliability"
        );
        let mut started = TelemetryContext::new("p", "model", "stream_started");
        started.set_trace("trace-success", 1, true);
        telemetry.submit(
            "chat",
            200,
            Duration::from_millis(1),
            true,
            Some(started),
            &metrics,
        );
        telemetry.observe_stream_health(
            "p",
            "model",
            "success",
            Duration::from_millis(100),
            ("trace-success", 1),
            &metrics,
        );
        assert!(telemetry.flush(Duration::from_secs(3)));
        let score = crate::scoring::read_scoring(dir.path(), 1).unwrap();
        assert_eq!(
            score["stream_terminal_metrics"]["provider_completion_rate_excluding_local_policy"],
            1.0,
            "local admission guards should not reduce verified provider success rate"
        );
        assert_eq!(
            score["stream_terminal_metrics"]["completion_rate_excluding_client_cancellation"],
            1.0 / 3.0,
            "legacy metric must remain stable for existing dashboards"
        );
    }

    #[test]
    fn older_stream_terminal_schema_migrates_without_losing_trace_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old-terminal-schema.sqlite3");
        let conn = open_database(&path).unwrap();
        let now = now_secs();
        conn.execute_batch(
            "DROP TABLE stream_terminals;
             CREATE TABLE stream_terminals (
                 ts INTEGER NOT NULL,
                 request_id TEXT NOT NULL,
                 attempt_index INTEGER NOT NULL,
                 provider TEXT NOT NULL,
                 model TEXT NOT NULL,
                 outcome TEXT NOT NULL CHECK (outcome IN
                     ('success','transport_error','client_cancelled')),
                 elapsed_ms INTEGER NOT NULL,
                 PRIMARY KEY (request_id, attempt_index)
             );
             CREATE INDEX stream_terminals_ts ON stream_terminals(ts);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO stream_terminals VALUES (?1,'legacy',1,'p','m','transport_error',23)",
            [now],
        )
        .unwrap();
        drop(conn);
        let conn = open_database(&path).unwrap();
        let retained: String = conn
            .query_row(
                "SELECT outcome FROM stream_terminals WHERE request_id='legacy'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained, "transport_error");
        conn.execute(
            "INSERT INTO stream_terminals VALUES (?1,'new',1,'p','m','timeout',42)",
            [now],
        )
        .unwrap();
        drop(conn);
        let reopened = open_database(&path).unwrap();
        let count: i64 = reopened
            .query_row("SELECT COUNT(*) FROM stream_terminals", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 2, "migration must be repeatable and lossless");
    }

    #[test]
    fn sse_terminal_outcomes_are_request_linked_and_cancellations_do_not_poison_health() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::default());
        let telemetry = Telemetry::start(dir.path(), Arc::clone(&metrics)).unwrap();
        for (trace, outcome) in [
            ("trace-completed", "success"),
            ("trace-failed", "transport_error"),
            ("trace-cancelled", "client_cancelled"),
        ] {
            let mut start = TelemetryContext::new("p", "model", "stream_started");
            start.set_trace(trace, 1, true);
            telemetry.submit(
                "chat",
                200,
                Duration::from_millis(12),
                true,
                Some(start),
                &metrics,
            );
            telemetry.observe_stream_health(
                "p",
                "model",
                outcome,
                Duration::from_millis(40),
                (trace, 1),
                &metrics,
            );
        }
        // Duplicate callback must not create a second terminal row.
        telemetry.observe_stream_health(
            "p",
            "model",
            "client_cancelled",
            Duration::from_millis(60),
            ("trace-cancelled", 1),
            &metrics,
        );
        assert!(telemetry.flush(Duration::from_secs(3)));
        let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
        let starts: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM request_events WHERE failure_type='stream_started'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(starts, 3);
        let terminals: Vec<(String, String, i64)> = {
            let mut stmt = db.prepare(
                "SELECT request_id,outcome,attempt_index FROM stream_terminals ORDER BY request_id"
            ).unwrap();
            stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        assert_eq!(
            terminals,
            vec![
                ("trace-cancelled".into(), "client_cancelled".into(), 1),
                ("trace-completed".into(), "success".into(), 1),
                ("trace-failed".into(), "transport_error".into(), 1),
            ]
        );
        let failures: i64 = db
            .query_row("SELECT COUNT(*) FROM failure_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(failures, 1, "client cancellation is not a provider failure");
        let summary = crate::scoring::read_scoring(dir.path(), 1).unwrap();
        assert_eq!(
            summary["stream_terminal_metrics"]["terminal_observations"],
            3
        );
        assert_eq!(summary["stream_terminal_metrics"]["completed"], 1);
        assert_eq!(summary["stream_terminal_metrics"]["upstream_failed"], 1);
        assert_eq!(summary["stream_terminal_metrics"]["client_cancelled"], 1);
        assert_eq!(
            summary["stream_terminal_metrics"]["completion_rate_excluding_client_cancellation"],
            0.5
        );
        let hour_counts: i64 = db
            .query_row("SELECT SUM(request_count) FROM hourly_stats", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            hour_counts, 3,
            "terminal events cannot double count requests"
        );
    }

    #[test]
    fn terminal_only_capacity_recovery_reclaims_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("terminal-capacity.sqlite3");
        let mut conn = open_database(&path).unwrap();
        conn.execute(
            "INSERT INTO stream_terminals VALUES (?1,'trace',1,'p','m','client_cancelled',10)",
            [now_secs()],
        )
        .unwrap();
        conn.execute_batch("PRAGMA max_page_count=1").unwrap();
        assert!(recover_page_capacity(&mut conn, now_secs()).unwrap());
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM stream_terminals", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn hourly_only_capacity_recovery_reclaims_aggregate_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hourly-capacity.sqlite3");
        let mut conn = open_database(&path).unwrap();
        let now = now_secs();
        let hour = now.div_euclid(3600) * 3600;
        for index in 0..8 {
            conn.execute(
                "INSERT INTO hourly_stats
                 (hour_ts, endpoint, provider, model, failure_type,
                  request_count, failure_count, total_elapsed_ms, max_elapsed_ms, identity_verified)
                 VALUES (?1,'chat','p',?2,'success',1,0,10,10,1)",
                rusqlite::params![hour, format!("model-{index}")],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA max_page_count=1").unwrap();
        assert!(recover_page_capacity(&mut conn, now).unwrap());
        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM hourly_stats", [], |row| row.get(0))
            .unwrap();
        assert_eq!(remaining, 0, "aggregate-only pressure must be reclaimable");
    }

    #[test]
    fn provider_health_is_preserved_when_diagnostic_rows_free_page_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health-preserved.sqlite3");
        let mut conn = open_database(&path).unwrap();
        let now = now_secs();
        write_batch(
            &mut conn,
            &[event(now, "p", "recent-model", 503, 25, "provider_5xx")],
        )
        .unwrap();
        let before: i64 = conn
            .query_row("SELECT COUNT(*) FROM provider_health", [], |row| row.get(0))
            .unwrap();
        assert_eq!(before, 1);
        conn.execute_batch("PRAGMA max_page_count=1").unwrap();
        assert!(recover_page_capacity(&mut conn, now).unwrap());
        let after: i64 = conn
            .query_row("SELECT COUNT(*) FROM provider_health", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            after, 1,
            "do not evict live health when event rows are reclaimable"
        );
        let requests: i64 = conn
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(requests, 0);
    }

    #[test]
    fn provider_health_only_page_pressure_reclaims_oldest_advisory_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health-capacity.sqlite3");
        let mut conn = open_database(&path).unwrap();
        let now = now_secs();
        for index in 0..520_i64 {
            conn.execute(
                "INSERT INTO provider_health
                (provider,model,penalty,consecutive_failures,demoted,
                 last_failure_ts,last_success_ts,updated_ts,identity_verified)
                 VALUES('p',?1,35,1,0,?2,0,?2,1)",
                rusqlite::params![format!("model-{index}"), now - 1000 + index],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA max_page_count=1").unwrap();
        assert!(recover_page_capacity(&mut conn, now).unwrap());
        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM provider_health", [], |row| row.get(0))
            .unwrap();
        assert_eq!(remaining, 8);
        let first: String = conn
            .query_row(
                "SELECT model FROM provider_health ORDER BY updated_ts LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(first, "model-512");
        for table in [
            "request_events",
            "failure_events",
            "hourly_stats",
            "shadow_events",
            "stream_terminals",
        ] {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "{table} must not be needed for recovery");
        }
    }

    #[test]
    fn shadow_records_are_bounded_advisory_only_and_never_count_as_real_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::default());
        let telemetry = Telemetry::start(dir.path(), Arc::clone(&metrics)).unwrap();
        telemetry.observe_shadow("trace-a", ("base", "one"), Some(("other", "two")), &metrics);
        telemetry.observe_shadow("trace-b", ("base", "one"), None, &metrics);
        assert!(telemetry.flush(Duration::from_secs(3)));
        let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
        let (total, available, disagreed): (i64, i64, i64) = db
            .query_row(
                "SELECT COUNT(*),SUM(available),SUM(disagreed) FROM shadow_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!((total, available, disagreed), (2, 1, 1));
        let request_count: i64 = db
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        let health_count: i64 = db
            .query_row("SELECT COUNT(*) FROM provider_health", [], |row| row.get(0))
            .unwrap();
        assert_eq!((request_count, health_count), (0, 0));
        let summary = crate::scoring::read_scoring(dir.path(), 1).unwrap();
        assert_eq!(summary["shadow_metrics"]["evaluations"], 2);
        assert_eq!(summary["shadow_metrics"]["valid_rankings"], 1);
        assert_eq!(summary["shadow_metrics"]["top_candidate_disagreements"], 1);
        assert_eq!(summary["shadow_metrics"]["disagreement_rate"], 1.0);
        assert_eq!(summary["request_metrics"]["traced_requests"], 0);
    }

    #[test]
    fn full_disk_recovery_counts_shadow_only_rows_as_reclaimed_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shadow-capacity.sqlite3");
        let mut conn = open_database(&path).unwrap();
        conn.execute(
            "INSERT INTO shadow_events VALUES (?1,'trace','base','one','other','two',1,1)",
            [now_secs()],
        )
        .unwrap();
        let pages: i64 = conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap();
        let ceiling: i64 = conn
            .query_row(&format!("PRAGMA max_page_count={}", pages + 8), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(ceiling, pages + 8);
        assert!(recover_page_capacity(&mut conn, now_secs()).unwrap());
        let left: i64 = conn
            .query_row("SELECT COUNT(*) FROM shadow_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(left, 0);
    }

    #[test]
    fn shadow_trace_links_to_actual_failure_without_inventing_proposal_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::default());
        let telemetry = Telemetry::start(dir.path(), Arc::clone(&metrics)).unwrap();
        telemetry.observe_shadow(
            "trace-joined",
            ("base", "one"),
            Some(("other", "two")),
            &metrics,
        );
        let mut context = TelemetryContext::new("base", "one", "provider_5xx");
        context.set_trace("trace-joined", 1, false);
        telemetry.submit(
            "chat",
            502,
            Duration::from_millis(12),
            false,
            Some(context),
            &metrics,
        );
        assert!(telemetry.flush(Duration::from_secs(3)));
        let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
        let (status, actual, suggestion): (i64, String, String) = db
            .query_row(
                "SELECT r.status,r.model,s.suggested_model FROM shadow_events s
             JOIN request_events r ON s.request_id=r.request_id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            (status, actual, suggestion),
            (502, "one".into(), "two".into())
        );
        let summary = crate::scoring::read_scoring(dir.path(), 1).unwrap();
        assert_eq!(summary["shadow_metrics"]["top_candidate_disagreements"], 1);
        assert_eq!(
            summary["request_metrics"]["first_attempt_successes_nonstream"],
            0
        );
    }

    #[test]
    fn flush_barrier_persists_pending_events_before_acknowledging() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::default());
        let telemetry = Telemetry::start(dir.path(), Arc::clone(&metrics)).unwrap();
        for _ in 0..40 {
            telemetry.submit("chat", 200, Duration::from_millis(1), false, None, &metrics);
        }
        assert!(telemetry.flush(Duration::from_secs(3)));
        let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
        let written: i64 = db
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(written, 40);
        assert_eq!(metrics.telemetry_dropped.load(Ordering::Relaxed), 0);
    }

    #[cfg(unix)]
    #[test]
    fn existing_sqlite_symlink_is_rejected_without_touching_target() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("outside");
        let link = dir.path().join("telemetry.sqlite3");
        std::fs::write(&target, b"keep external bytes").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(matches!(
            open_database(&link),
            Err(rusqlite::Error::InvalidPath(_))
        ));
        assert_eq!(std::fs::read(target).unwrap(), b"keep external bytes");
    }

    #[cfg(unix)]
    #[test]
    fn wal_shm_permission_hardening_never_chmods_a_symlink_target() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("requests.sqlite3");
        let target = dir.path().join("unrelated-file");
        std::fs::write(&target, b"do not touch").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
        for suffix in ["-wal", "-shm"] {
            let link = dir.path().join(format!("requests.sqlite3{suffix}"));
            symlink(&target, &link).unwrap();
        }
        secure_sidecars(&path);
        assert_eq!(std::fs::read(&target).unwrap(), b"do not touch");
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o644,
            "WAL permission protection unexpectedly changed an unrelated file"
        );
    }

    #[cfg(unix)]
    #[test]
    fn wal_shm_permission_hardening_uses_the_opened_regular_inode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("requests.sqlite3");
        for suffix in ["-wal", "-shm"] {
            let sidecar = dir.path().join(format!("requests.sqlite3{suffix}"));
            std::fs::write(&sidecar, b"sqlite sidecar fixture").unwrap();
            std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
        secure_sidecars(&path);
        for suffix in ["-wal", "-shm"] {
            let sidecar = dir.path().join(format!("requests.sqlite3{suffix}"));
            assert_eq!(
                std::fs::metadata(&sidecar).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(std::fs::read(sidecar).unwrap(), b"sqlite sidecar fixture");
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
        for table in ["request_events", "failure_events"] {
            for column in ["request_id", "attempt_index", "stream_requested"] {
                assert!(has_column(&migrated, table, column).unwrap());
            }
        }
        let legacy_trace: (Option<String>, Option<i64>, i64) = migrated
            .query_row(
                "SELECT request_id,attempt_index,stream_requested FROM request_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(legacy_trace, (None, None, 0));
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
            "SELECT penalty,consecutive_failures,demoted FROM provider_health WHERE provider='provider_A' AND model='model/alpha'",
            [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).unwrap();
        assert_eq!(health, (35, 1, 0));
        let redundant: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM provider_health WHERE model='*'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(redundant, 0);
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
        assert_eq!(snapshots[0].model, "model/alpha");
        assert_eq!(snapshots[0].penalty, 100);
        assert_eq!(snapshots[0].consecutive_failures, 3);
        assert!(snapshots[0].demoted);
        assert_eq!(snapshots[0].last_failure_secs, 7202);

        let stale = load_provider_health_from_path(&path, 7202 + HISTORY_RETENTION_SECS + 1);
        assert!(stale.is_empty());
    }

    #[test]
    fn startup_health_restore_rejects_future_clock_skew_without_erasing_valid_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health-skew.sqlite3");
        let mut conn = open_database(&path).unwrap();
        let now = 7201_i64;
        write_batch(
            &mut conn,
            &[
                event(now, "legit", "m", 503, 1, "provider_5xx"),
                event(now + 3600, "future", "m", 503, 1, "provider_5xx"),
            ],
        )
        .unwrap();
        let restored = load_provider_health_from_path(&path, now);
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].provider, "legit");
        // Even a current updated_ts cannot validate a future failure from
        // a damaged or partially migrated health snapshot.
        conn.execute(
            "UPDATE provider_health SET last_failure_ts=?1
             WHERE provider='legit'",
            [now + 3600],
        )
        .unwrap();
        assert!(load_provider_health_from_path(&path, now).is_empty());
        // The writer's eventual expiry cleanup remains independent from
        // the read-only fail-open startup restore.
        let future_before: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM provider_health WHERE provider='future'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(future_before, 1);
    }

    #[test]
    fn old_unverified_health_is_not_restored_or_carried_into_new_observations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-health.sqlite3");
        // Reproduce the pre-identity-verification SQLite layout. These rows
        // may have been written by unrelated names that sanitize identically.
        let db = Connection::open(&path).unwrap();
        db.execute_batch(
            "CREATE TABLE provider_health(
                provider TEXT NOT NULL, model TEXT NOT NULL,
                penalty INTEGER NOT NULL, consecutive_failures INTEGER NOT NULL,
                demoted INTEGER NOT NULL, last_failure_ts INTEGER NOT NULL,
                last_success_ts INTEGER NOT NULL, updated_ts INTEGER NOT NULL,
                PRIMARY KEY(provider,model)
             );
             INSERT INTO provider_health
             VALUES('p','model_a',100,10,1,7200,0,7200);",
        )
        .unwrap();
        drop(db);
        // Before the migration even runs, absence of the proof column must
        // fail closed rather than restoring ambiguous historical penalties.
        assert!(load_provider_health_from_path(&path, 7201).is_empty());
        let mut conn = open_database(&path).unwrap();
        assert!(has_column(&conn, "provider_health", "identity_verified").unwrap());
        assert!(load_provider_health_from_path(&path, 7201).is_empty());

        write_batch(
            &mut conn,
            &[event(7201, "p", "model_a", 503, 10, "provider_5xx")],
        )
        .unwrap();
        let (penalty, failures, demoted, verified): (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT penalty,consecutive_failures,demoted,identity_verified
                 FROM provider_health WHERE provider='p' AND model='model_a'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!((penalty, failures, demoted, verified), (35, 1, 0, 1));
        assert_eq!(load_provider_health_from_path(&path, 7201).len(), 1);
    }

    #[test]
    fn colliding_diagnostic_labels_cannot_write_verified_provider_health() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::default());
        let telemetry = Telemetry::start(dir.path(), Arc::clone(&metrics)).unwrap();
        let oversized = "x".repeat(MAX_LABEL_CHARS + 1);
        for model in ["model a", "model?b", "passthrough", oversized.as_str()] {
            let context = TelemetryContext::new("p", model, "provider_5xx");
            assert!(!context.is_upstream, "lossy identity {model:?}");
            telemetry.observe_attempt("chat", 503, Duration::from_millis(1), context, &metrics);
        }
        let bad_provider = TelemetryContext::new("p a", "model_a", "provider_5xx");
        assert!(!bad_provider.is_upstream);
        telemetry.observe_attempt(
            "chat",
            503,
            Duration::from_millis(1),
            bad_provider,
            &metrics,
        );
        assert!(telemetry.flush(Duration::from_secs(3)));
        let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
        let health: i64 = db
            .query_row("SELECT COUNT(*) FROM provider_health", [], |row| row.get(0))
            .unwrap();
        let failures: i64 = db
            .query_row("SELECT COUNT(*) FROM failure_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(health, 0, "lossy telemetry IDs never poison Auto health");
        assert_eq!(failures, 5, "diagnostic failures remain observable");

        // A genuine exact candidate remains eligible for persistence.
        drop(db);
        let trusted = TelemetryContext::new("p", "model_a", "provider_5xx");
        assert!(trusted.is_upstream);
        telemetry.observe_attempt("chat", 503, Duration::from_millis(1), trusted, &metrics);
        assert!(telemetry.flush(Duration::from_secs(3)));
        let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
        let (penalty, verified): (i64, i64) = db
            .query_row(
                "SELECT penalty,identity_verified FROM provider_health WHERE provider='p' AND model='model_a'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((penalty, verified), (35, 1));
    }

    #[test]
    fn colliding_hourly_scoring_evidence_fails_closed_without_losing_diagnostics() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("requests.sqlite3");
        let mut conn = open_database(&path).unwrap();
        let now = now_secs();
        let mut events = (0..20)
            .map(|_| event(now, "p", "model_a", 200, 10, "success"))
            .collect::<Vec<_>>();
        let mut collision = event(now, "p", "model a", 200, 10, "success");
        collision.health_observation = false;
        events.push(collision);
        write_batch(&mut conn, &events).unwrap();
        let (total, verified): (i64, i64) = conn
            .query_row(
                "SELECT request_count,identity_verified FROM hourly_stats
                 WHERE provider='p' AND model='model_a' AND failure_type='success'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((total, verified), (21, 0));
        let (trusted, untrusted): (i64, i64) = conn
            .query_row(
                "SELECT SUM(identity_verified),SUM(1-identity_verified)
                 FROM request_events WHERE provider='p' AND model='model_a'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((trusted, untrusted), (20, 1));
        // The 20 valid samples cannot override one ambiguous aggregate;
        // the CLI still sees all 21 diagnostic requests.
        drop(conn);
        let data_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(data_dir.path().join("telemetry")).unwrap();
        std::fs::copy(&path, data_dir.path().join("telemetry/requests.sqlite3")).unwrap();
        let scoped = crate::scoring::read_scoring_for_candidates(
            data_dir.path(),
            1,
            &[("p".into(), "model_a".into())],
        )
        .unwrap();
        assert_eq!(scoped["candidate_count"], 0);
        let diagnostic = crate::scoring::read_scoring(data_dir.path(), 1).unwrap();
        assert_eq!(diagnostic["candidates"][0]["attempts"], 21);
    }

    #[test]
    fn old_hourly_evidence_stays_unverified_after_new_exact_observation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-hourly.sqlite3");
        let db = Connection::open(&path).unwrap();
        let now = now_secs();
        let hour = now - now.rem_euclid(3600);
        db.execute_batch(
            "CREATE TABLE hourly_stats(
                hour_ts INTEGER,endpoint TEXT,provider TEXT,model TEXT,
                failure_type TEXT,request_count INTEGER,failure_count INTEGER,
                total_elapsed_ms INTEGER,max_elapsed_ms INTEGER,
                PRIMARY KEY(hour_ts,endpoint,provider,model,failure_type)
            );",
        )
        .unwrap();
        db.execute(
            "INSERT INTO hourly_stats VALUES (?1,'chat','p','model_a','success',200,0,2000,10)",
            [hour],
        )
        .unwrap();
        drop(db);
        let mut conn = open_database(&path).unwrap();
        write_batch(&mut conn, &[event(now, "p", "model_a", 200, 10, "success")]).unwrap();
        let (total, verified): (i64, i64) = conn
            .query_row(
                "SELECT request_count,identity_verified FROM hourly_stats
                 WHERE provider='p' AND model='model_a' AND failure_type='success'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((total, verified), (201, 0));
    }

    #[test]
    fn health_restore_reads_existing_readonly_database_without_modifying_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health-readonly.sqlite3");
        let mut db = open_database(&path).unwrap();
        write_batch(&mut db, &[event(7200, "p", "m", 503, 1, "provider_5xx")]).unwrap();
        drop(db);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();

        let snapshots = load_provider_health_from_path(&path, 7201);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].provider, "p");
        assert_eq!(snapshots[0].penalty, 35);
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o400
        );
    }

    #[test]
    fn health_restore_rejects_symlink_to_valid_database_without_touching_target() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("outside.sqlite3");
        let mut db = open_database(&target).unwrap();
        write_batch(&mut db, &[event(7200, "p", "m", 503, 1, "provider_5xx")]).unwrap();
        drop(db);
        let before = std::fs::read(&target).unwrap();
        let link = dir.path().join("requests.sqlite3");
        symlink(&target, &link).unwrap();

        assert!(load_provider_health_from_path(&link, 7201).is_empty());
        assert_eq!(std::fs::read(&target).unwrap(), before);
        assert_eq!(
            load_provider_health_from_path(&dir.path().join("absent"), 7201).len(),
            0
        );
    }

    #[test]
    fn health_restore_fails_open_on_legacy_database_without_health_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE request_events (ts INTEGER NOT NULL);")
            .unwrap();
        drop(conn);

        let before = std::fs::read(&path).unwrap();
        assert!(load_provider_health_from_path(&path, 7201).is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
    #[test]
    fn future_timestamp_metadata_is_pruned_without_waiting_for_clock_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = open_database(&dir.path().join("skew.sqlite3")).unwrap();
        let now = 100 * 24 * 3600;
        write_batch(
            &mut conn,
            &[
                event(now, "p", "m", 200, 1, "success"),
                event(now + 7200, "p", "m", 200, 1, "success"),
            ],
        )
        .unwrap();
        prune_expired(&mut conn, now).unwrap();
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1);
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
            .try_send(TelemetryMessage::Record(event(
                123,
                "provider",
                "model",
                503,
                1,
                "provider_5xx",
            )))
            .unwrap();
        drop(sender);
        handle.join().unwrap();

        assert_eq!(metrics.telemetry_dropped.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.telemetry_write_failures.load(Ordering::Relaxed), 1);
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
    #[test]
    fn empty_flush_does_not_report_success_when_sqlite_failed_to_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unopenable.sqlite3");
        let original = b"not a valid sqlite database";
        std::fs::write(&path, original).unwrap();
        let (sender, receiver) = mpsc::sync_channel(4);
        let metrics = Arc::new(Metrics::default());
        let worker_path = path.clone();
        let handle = thread::spawn(move || worker(receiver, worker_path, metrics));
        let (ack, receipt) = mpsc::sync_channel(1);
        sender.send(TelemetryMessage::Flush(ack)).unwrap();
        assert!(
            !receipt.recv_timeout(Duration::from_secs(3)).unwrap(),
            "an empty flush must not claim SQLite persistence is healthy"
        );
        drop(sender);
        handle.join().unwrap();
        assert_eq!(std::fs::read(path).unwrap(), original);
    }

    #[test]
    fn readonly_database_rejects_writes_without_changing_existing_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("readonly.sqlite3");
        let mut writer = open_database(&path).unwrap();
        write_batch(&mut writer, &[event(100, "p", "m", 200, 1, "success")]).unwrap();
        drop(writer);

        let mut reader =
            Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert!(write_batch(&mut reader, &[event(101, "p", "m", 503, 1, "provider_5xx")]).is_err());
        let rows: i64 = reader
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn sqlite_full_worker_retry_persists_current_and_subsequent_batches() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = open_database(&dir.path().join("full-then-recover.sqlite3")).unwrap();
        let now = now_secs();
        // Fill the request-event btree with reclaimable recent diagnostic
        // rows, then forbid file growth on this same writer connection.
        let seed = (0..1024)
            .map(|_| event(now - 1, "p", "seed", 200, 1, "success"))
            .collect::<Vec<_>>();
        write_batch(&mut conn, &seed).unwrap();
        let pages: i64 = conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap();
        let max_pages: i64 = conn
            .query_row(&format!("PRAGMA max_page_count={pages}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(max_pages, pages);
        let batch = (0..64)
            .map(|index| event(now, "p", &format!("new-{index}"), 503, 2, "provider_5xx"))
            .collect::<Vec<_>>();
        let first_attempt = write_batch(&mut conn, &batch);
        assert!(
            matches!(first_attempt, Err(rusqlite::Error::SqliteFailure(sqlite, _))
                if sqlite.code == rusqlite::ErrorCode::DiskFull),
            "batch must genuinely encounter SQLITE_FULL before recovery"
        );
        write_batch_with_recovery(&mut conn, &batch, now)
            .expect("worker must reclaim event pages and retry the batch");
        write_batch_with_recovery(
            &mut conn,
            &[event(now, "p", "new-0", 200, 1, "success")],
            now,
        )
        .expect("subsequent batch must remain writable after a recovered SQLITE_FULL");
        let recent: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM request_events WHERE model LIKE 'new-%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(recent, 65);
    }

    #[test]
    fn page_cap_recovery_evicts_bounded_history_and_allows_new_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = open_database(&dir.path().join("recover.sqlite3")).unwrap();
        let now = 10_000_000;
        let rows = (0..256)
            .map(|i| event(now - 60 + i, "p", "m", 200, 1, "success"))
            .collect::<Vec<_>>();
        write_batch(&mut conn, &rows).unwrap();
        let pages: i64 = conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap();
        let limit: i64 = conn
            .query_row(&format!("PRAGMA max_page_count={pages}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(limit, pages);
        assert!(recover_page_capacity(&mut conn, now).unwrap());
        write_batch(&mut conn, &[event(now, "p", "m", 200, 1, "success")]).unwrap();
        let retained: i64 = conn
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(retained, 1);
    }

    #[test]
    fn sqlite_page_limit_rolls_back_whole_batch_without_partial_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full.sqlite3");
        let mut conn = open_database(&path).unwrap();
        let pages: i64 = conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap();
        let limit = pages + 1;
        let effective: i64 = conn
            .query_row(&format!("PRAGMA max_page_count = {limit}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(effective, limit);

        let rows: Vec<_> = (0..256)
            .map(|index| {
                event(
                    7200 + index,
                    "provider",
                    &format!("model-{index}"),
                    503,
                    1,
                    "provider_5xx",
                )
            })
            .collect();
        assert!(
            write_batch(&mut conn, &rows).is_err(),
            "page cap must fail atomic batch"
        );
        for table in [
            "request_events",
            "failure_events",
            "hourly_stats",
            "provider_health",
        ] {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "{table} retained part of a failed transaction");
        }
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
        // Names of columns/values that must never appear in the persisted
        // schema. These are field names from the public request format, not
        // credentials; the assertion reports only that a forbidden column was
        // found, never the value itself.
        for forbidden_column in [
            "prompt",
            "token",
            "api_key",
            "authorization",
            "ip_address",
            "request_body",
            "response_body",
            "base_url",
        ] {
            assert!(
                !schema.contains(forbidden_column),
                "telemetry schema must not persist request payload fields"
            );
        }
    }
}
