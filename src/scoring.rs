//! Offline, read-only SQLite scoring summary for the local Unix control socket.
//! Uses no new management TCP endpoint and no request-path SQL queries.
use rusqlite::{Connection, OpenFlags, params};
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const MAX_CANDIDATES: usize = 32;
const MAX_LATENCY_SAMPLES: usize = 512;
const MIN_SCORE_SAMPLES: i64 = 20;

/// Hourly attempt aggregates have hour precision. A per-model request p95
/// samples up to the last 512 downstream completions, not upstream attempts.
pub fn read_scoring(data_dir: &Path, hours: u32) -> Result<Value, String> {
    if ![1, 6, 24, 168].contains(&hours) {
        return Err("scoring window must be 1h, 6h, 24h or 7d".into());
    }
    let path = data_dir.join("telemetry").join("requests.sqlite3");
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(json!({"enabled":false,"window_hours":hours,"candidates":[]}));
        }
        Err(_) => return Err("cannot inspect telemetry database".into()),
    };
    if !metadata.file_type().is_file() {
        return Err("telemetry database is not a regular file".into());
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|_| "cannot open telemetry database for read-only scoring")?;
    conn.busy_timeout(Duration::from_millis(50))
        .map_err(|_| "cannot configure SQLite reader")?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64;
    let since = now - (hours as i64) * 3600;
    let start_hour = since - since.rem_euclid(3600);
    let mut stmt = conn
        .prepare(
            "SELECT provider,model,COALESCE(SUM(request_count),0),COALESCE(SUM(failure_count),0),
                COALESCE(SUM(total_elapsed_ms),0),COALESCE(MAX(max_elapsed_ms),0)
         FROM hourly_stats
         WHERE hour_ts >= ?1 AND provider != 'unknown'
         GROUP BY provider,model
         ORDER BY COALESCE(SUM(request_count),0) DESC,provider,model
         LIMIT 32",
        )
        .map_err(|_| "cannot prepare telemetry summary")?;
    let rows = stmt
        .query_map(params![start_hour], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(|_| "cannot read telemetry summary")?;
    let mut candidates = Vec::new();
    for record in rows {
        let (provider, model, attempts, failures, elapsed, max_elapsed) =
            record.map_err(|_| "malformed telemetry summary row")?;
        let attempts = attempts.max(0);
        let failures = failures.clamp(0, attempts);
        let eligible = attempts >= MIN_SCORE_SAMPLES;
        let mut sample_stmt = conn
            .prepare(
                "SELECT elapsed_ms FROM request_events
             WHERE provider=?1 AND model=?2 AND ts >= ?3
             ORDER BY ts DESC LIMIT 512",
            )
            .map_err(|_| "cannot prepare latency sample")?;
        let samples = sample_stmt
            .query_map(params![provider, model, since], |row| row.get::<_, i64>(0))
            .map_err(|_| "cannot read latency samples")?;
        let mut latencies = Vec::new();
        for value in samples {
            latencies.push(value.map_err(|_| "malformed latency sample")?.max(0));
        }
        latencies.sort_unstable();
        let p95 = if latencies.is_empty() {
            None
        } else {
            let index = (latencies.len() * 95).div_ceil(100).saturating_sub(1);
            latencies.get(index).copied()
        };
        candidates.push(json!({
            "provider":provider,"model":model,"attempts":attempts,
            "failures":failures,"successes":attempts.saturating_sub(failures),
            "success_rate": if attempts > 0 { (attempts - failures) as f64 / attempts as f64 } else {0.0},
            "average_attempt_ms": if attempts > 0 { elapsed / attempts } else {0},
            "max_attempt_ms": max_elapsed.max(0),
            "downstream_p95_ms": p95,
            "downstream_sample_count":latencies.len().min(MAX_LATENCY_SAMPLES),
            "qualified":eligible,
        }));
        if candidates.len() >= MAX_CANDIDATES {
            break;
        }
    }
    Ok(json!({
        "enabled":true,"source":"local_sqlite_read_only",
        "window_hours":hours,
        "aggregates_start_at_hour":start_hour,
        "minimum_samples":MIN_SCORE_SAMPLES,
        "advisory_only":true,
        "candidate_count":candidates.len(),
        "candidates":candidates,
        "notes":"success rates count upstream candidate attempts; downstream p95 uses up to 512 recent downstream completion samples per candidate, not counterfactual outcomes",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scoring_is_read_only_bounded_and_respects_sample_gate() {
        let dir = tempfile::tempdir().unwrap();
        let no_db = read_scoring(dir.path(), 1).unwrap();
        assert_eq!(no_db["enabled"], false);
        assert!(read_scoring(dir.path(), 2).is_err());
        let directory = dir.path().join("telemetry");
        std::fs::create_dir_all(&directory).unwrap();
        let db = Connection::open(directory.join("requests.sqlite3")).unwrap();
        db.execute_batch(
            "CREATE TABLE hourly_stats(
                hour_ts INTEGER,provider TEXT,model TEXT,request_count INTEGER,
                failure_count INTEGER,total_elapsed_ms INTEGER,max_elapsed_ms INTEGER
             );
             CREATE TABLE request_events(provider TEXT,model TEXT,ts INTEGER,elapsed_ms INTEGER);",
        )
        .unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let hour = now - now.rem_euclid(3600);
        db.execute(
            "INSERT INTO hourly_stats VALUES(?1,'test','alpha',21,1,2100,250)",
            [hour],
        )
        .unwrap();
        for t in 0..21 {
            db.execute(
                "INSERT INTO request_events VALUES('test','alpha',?1,?2)",
                params![now, t * 10],
            )
            .unwrap();
        }
        drop(db);
        let result = read_scoring(dir.path(), 1).unwrap();
        assert_eq!(result["candidate_count"], 1);
        assert_eq!(result["candidates"][0]["qualified"], true);
        assert_eq!(result["candidates"][0]["attempts"], 21);
        assert_eq!(result["candidates"][0]["failures"], 1);
        assert_eq!(result["candidates"][0]["downstream_p95_ms"], 190);
    }
}
