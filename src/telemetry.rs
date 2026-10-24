//! Opt-in, fail-open SQLite request metadata, never prompts, keys, IPs or model names.
//! The hot path uses a bounded, nonblocking queue; SQLite is owned by one worker.
use crate::metrics::Metrics;
use rusqlite::{Connection, params};
use std::sync::atomic::Ordering;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const QUEUE_CAPACITY: usize = 512;
const BATCH_CAPACITY: usize = 64;
const BATCH_WAIT: Duration = Duration::from_millis(250);
const RETENTION_SECS: i64 = 7 * 24 * 3600;

#[derive(Clone)]
pub struct Telemetry {
    sender: SyncSender<Event>,
}

#[derive(Clone, Copy, Debug)]
struct Event {
    ts: i64,
    endpoint: &'static str,
    status: u16,
    elapsed_ms: u32,
    sse_headers: bool,
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

    pub fn submit(
        &self,
        endpoint: &'static str,
        status: u16,
        elapsed: Duration,
        sse_headers: bool,
        metrics: &Metrics,
    ) {
        let event = Event {
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .min(i64::MAX as u64) as i64,
            endpoint,
            status,
            elapsed_ms: elapsed.as_millis().min(u32::MAX as u128) as u32,
            sse_headers,
        };
        if self.sender.try_send(event).is_err() {
            Metrics::inc(&metrics.telemetry_dropped);
        }
    }
}

fn open_database(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_millis(100))?;
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
            status INTEGER NOT NULL,
            elapsed_ms INTEGER NOT NULL,
            sse_headers INTEGER NOT NULL CHECK (sse_headers IN (0,1))
        );
        CREATE INDEX IF NOT EXISTS request_events_ts ON request_events(ts);
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

fn write_batch(conn: &mut Connection, batch: &[Event]) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    {
        let mut statement = tx.prepare_cached(
            "INSERT INTO request_events (ts,endpoint,status,elapsed_ms,sse_headers) VALUES (?1,?2,?3,?4,?5)"
        )?;
        for event in batch {
            statement.execute(params![
                event.ts,
                event.endpoint,
                event.status,
                event.elapsed_ms,
                i64::from(event.sse_headers),
            ])?;
        }
    }
    tx.commit()
}

/// Delete expired metadata at startup and periodically, without reading request bodies.
fn prune_expired(conn: &Connection) -> rusqlite::Result<usize> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64;
    conn.execute(
        "DELETE FROM request_events WHERE ts < ?1",
        [now - RETENTION_SECS],
    )
}
fn worker(receiver: Receiver<Event>, path: PathBuf, metrics: Arc<Metrics>) {
    let mut conn = match open_database(&path) {
        Ok(db) => db,
        Err(_) => {
            // Drain to avoid a permanently full sender; never disrupt request dispatch.
            while receiver.recv().is_ok() {
                Metrics::inc(&metrics.telemetry_dropped);
            }
            return;
        }
    };
    if prune_expired(&conn).is_err() {
        Metrics::inc(&metrics.telemetry_write_failures);
    }
    let mut batches: usize = 0;
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
        match write_batch(&mut conn, &batch) {
            Ok(()) => {
                metrics
                    .telemetry_written
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
            }
            Err(_) => {
                metrics
                    .telemetry_dropped
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
                Metrics::inc(&metrics.telemetry_write_failures);
            }
        }
        batches = batches.wrapping_add(1);
        if batches.is_multiple_of(128) && prune_expired(&conn).is_err() {
            Metrics::inc(&metrics.telemetry_write_failures);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_overflow_is_nonblocking_and_accounted_for() {
        let (sender, _receiver) = mpsc::sync_channel(1);
        let telemetry = Telemetry { sender };
        let metrics = Metrics::default();
        let start = Instant::now();
        for _ in 0..10000 {
            telemetry.submit("chat", 429, Duration::from_millis(5), false, &metrics);
        }
        assert_eq!(metrics.telemetry_dropped.load(Ordering::Relaxed), 9999);
        assert!(start.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn sqlite_contains_only_fixed_metadata_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        let mut conn = open_database(&path).unwrap();
        let events = [Event {
            ts: 123,
            endpoint: "chat",
            status: 200,
            elapsed_ms: 42,
            sse_headers: true,
        }];
        write_batch(&mut conn, &events).unwrap();
        drop(conn);
        let db = Connection::open(&path).unwrap();
        let (status, endpoint, elapsed): (u16, String, u32) = db
            .query_row(
                "SELECT status, endpoint, elapsed_ms FROM request_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!((status, endpoint.as_str(), elapsed), (200, "chat", 42));
        let schema: String = db
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='request_events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        for secret in [
            "prompt",
            "token",
            "api_key",
            "authorization",
            "provider",
            "model",
            "ip_address",
        ] {
            assert!(!schema.contains(secret), "{secret}");
        }
    }

    #[test]
    fn expired_rows_are_pruned_but_recent_rows_survive() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = open_database(&dir.path().join("test.sqlite3")).unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let events = [
            Event {
                ts: now - RETENTION_SECS - 1,
                endpoint: "chat",
                status: 200,
                elapsed_ms: 1,
                sse_headers: false,
            },
            Event {
                ts: now,
                endpoint: "embeddings",
                status: 500,
                elapsed_ms: 2,
                sse_headers: false,
            },
        ];
        write_batch(&mut conn, &events).unwrap();
        assert_eq!(prune_expired(&conn).unwrap(), 1);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
    #[test]
    fn locked_database_rejects_batch_without_leaking_partial_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        let mut writer = open_database(&path).unwrap();
        let lock = Connection::open(&path).unwrap();
        lock.execute_batch("BEGIN EXCLUSIVE;").unwrap();
        let events = [Event {
            ts: 123,
            endpoint: "embeddings",
            status: 503,
            elapsed_ms: 5,
            sse_headers: false,
        }];
        let started = Instant::now();
        assert!(write_batch(&mut writer, &events).is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
        lock.execute_batch("ROLLBACK;").unwrap();
        let count: i64 = writer
            .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
}
