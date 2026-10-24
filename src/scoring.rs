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

/// Only actual upstream observations count toward a model's quality score.
/// Stream starts are not completed streams, and local/caller failures are not
/// evidence of a bad provider. Zero-attempt terminal stream failures must not
/// inflate the denominator a second time.
fn p95(samples: &mut [i64]) -> Option<i64> {
    if samples.is_empty() {
        return None;
    }
    samples.sort_unstable();
    samples.get((samples.len() * 95).div_ceil(100) - 1).copied()
}

/// Hourly attempt aggregates have hour precision. Nonstream completion p95
/// and SSE response-header p95 share a bounded last-512 downstream row sample.
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
                COALESCE(SUM(total_elapsed_ms),0),COALESCE(MAX(max_elapsed_ms),0),
                COALESCE(SUM(CASE WHEN failure_type='success' THEN request_count ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN failure_type IN
                    ('provider_5xx','transport_error','invalid_response','timeout','upstream_unavailable')
                    THEN MIN(request_count,failure_count) ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN failure_type='stream_started' THEN request_count ELSE 0 END),0)
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
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })
        .map_err(|_| "cannot read telemetry summary")?;
    let mut candidates = Vec::new();
    for record in rows {
        let (
            provider,
            model,
            attempts,
            failures,
            elapsed,
            max_elapsed,
            confirmed_successes,
            provider_failures,
            stream_starts,
        ) = record.map_err(|_| "malformed telemetry summary row")?;
        let attempts = attempts.max(0);
        let failures = failures.clamp(0, attempts);
        let confirmed_successes = confirmed_successes.max(0).min(attempts);
        let provider_failures = provider_failures.max(0).min(attempts - confirmed_successes);
        let observed = confirmed_successes + provider_failures;
        let eligible = observed >= MIN_SCORE_SAMPLES;
        let mut sample_stmt = conn
            .prepare(
                "SELECT elapsed_ms,sse_headers FROM request_events
             WHERE provider=?1 AND model=?2 AND ts >= ?3
             ORDER BY ts DESC LIMIT 512",
            )
            .map_err(|_| "cannot prepare latency sample")?;
        let samples = sample_stmt
            .query_map(params![provider, model, since], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(|_| "cannot read latency samples")?;
        let mut latencies = Vec::new();
        let mut stream_headers = Vec::new();
        for value in samples {
            let (elapsed_ms, is_stream) = value.map_err(|_| "malformed latency sample")?;
            if is_stream != 0 {
                stream_headers.push(elapsed_ms.max(0));
            } else {
                latencies.push(elapsed_ms.max(0));
            }
        }
        let downstream_p95 = p95(&mut latencies);
        let stream_header_p95 = p95(&mut stream_headers);
        candidates.push(json!({
            "provider":provider,"model":model,"attempts":attempts,
            "failures":failures,"successes":attempts.saturating_sub(failures),
            "success_rate": if attempts > 0 { (attempts - failures) as f64 / attempts as f64 } else {0.0},
            "average_attempt_ms": if attempts > 0 { elapsed / attempts } else {0},
            "max_attempt_ms": max_elapsed.max(0),
            "downstream_p95_ms": downstream_p95,
            "downstream_sample_count":latencies.len().min(MAX_LATENCY_SAMPLES),
            "stream_header_p95_ms":stream_header_p95,
            "stream_header_sample_count":stream_headers.len().min(MAX_LATENCY_SAMPLES),
            "stream_starts":stream_starts.max(0),
            "observed_samples":observed,
            "observed_successes":confirmed_successes,
            "observed_provider_failures":provider_failures,
            "excluded_attempts":attempts.saturating_sub(observed),
            "observed_success_rate":if observed > 0 {
                Some(confirmed_successes as f64 / observed as f64)
            } else {None},
            "qualified":eligible,
        }));
        if candidates.len() >= MAX_CANDIDATES {
            break;
        }
    }
    // Legacy databases are still readable before the writer migrates them.
    // Only terminal request rows enter the denominator: failed intermediate
    // candidates are recorded separately and cannot inflate request counts.
    let request_metrics = if conn
        .prepare("SELECT request_id,attempt_index,stream_requested FROM request_events LIMIT 0")
        .is_ok()
    {
        let (traced, fallback, first_nonstream, first_success): (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT COUNT(*),
                        COALESCE(SUM(CASE WHEN attempt_index > 1 THEN 1 ELSE 0 END),0),
                        COALESCE(SUM(CASE WHEN stream_requested = 0 THEN 1 ELSE 0 END),0),
                        COALESCE(SUM(CASE WHEN attempt_index = 1 AND stream_requested = 0
                                            AND failure_type = 'success' THEN 1 ELSE 0 END),0)
                 FROM request_events
                 WHERE ts >= ?1 AND request_id IS NOT NULL AND attempt_index IS NOT NULL",
                params![since],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(|_| "cannot read request-level scoring")?;
        Some(json!({
            "traced_requests":traced,
            "requests_with_fallback":fallback,
            "fallback_rate":if traced > 0 {Some(fallback as f64 / traced as f64)} else {None},
            "first_attempt_nonstream":first_nonstream,
            "first_attempt_successes_nonstream":first_success,
            "first_attempt_success_rate_nonstream":if first_nonstream > 0 {
                Some(first_success as f64 / first_nonstream as f64)
            } else {None},
        }))
    } else {
        None
    };
    Ok(json!({
        "enabled":true,"source":"local_sqlite_read_only",
        "window_hours":hours,
        "aggregates_start_at_hour":start_hour,
        "minimum_samples":MIN_SCORE_SAMPLES,
        "advisory_only":true,
        "candidate_count":candidates.len(),
        "candidates":candidates,
        "request_metrics":request_metrics,
         "request_metrics_notes":"Only traced terminal request rows count toward fallback rate; the first-attempt success rate is restricted to nonstream completions, not unverified SSE headers. Old SQLite databases report null until migrated.",
         "notes":"raw success_rate includes all failure classes; observed_success_rate counts only confirmed nonstream successes and attempted upstream provider failures, excluding caller/local/credential errors and unverified stream starts. Qualification requires 20 observed samples. p95 separates nonstream completion from SSE response-header latency, sampled from up to 512 recent downstream rows per candidate; neither is a counterfactual outcome or complete SSE duration.",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_rates_use_terminal_rows_and_exclude_unverified_sse_success() {
        let dir = tempfile::tempdir().unwrap();
        let directory = dir.path().join("telemetry");
        std::fs::create_dir_all(&directory).unwrap();
        let db = Connection::open(directory.join("requests.sqlite3")).unwrap();
        db.execute_batch(
            "CREATE TABLE hourly_stats (
                hour_ts INTEGER, provider TEXT, model TEXT, request_count INTEGER,
                failure_count INTEGER, total_elapsed_ms INTEGER,
                max_elapsed_ms INTEGER, failure_type TEXT
             );
             CREATE TABLE request_events (
                ts INTEGER, provider TEXT, model TEXT, elapsed_ms INTEGER,
                sse_headers INTEGER, request_id TEXT, attempt_index INTEGER,
                stream_requested INTEGER, failure_type TEXT
             );",
        )
        .unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        // One direct success, one fallback success, one direct failure, and
        // one SSE header-only start. Only one of three completed nonstreams
        // succeeded on the first attempt.
        for (id, attempt, stream, outcome) in [
            ("a", 1, 0, "success"),
            ("b", 2, 0, "success"),
            ("c", 1, 0, "provider_5xx"),
            ("d", 1, 1, "stream_started"),
        ] {
            db.execute(
                "INSERT INTO request_events VALUES(?1,'p','m',20,?2,?3,?4,?5,?6)",
                params![now, stream, id, attempt, stream, outcome],
            )
            .unwrap();
        }
        let result = read_scoring(dir.path(), 1).unwrap();
        assert_eq!(result["request_metrics"]["traced_requests"], 4);
        assert_eq!(result["request_metrics"]["requests_with_fallback"], 1);
        assert_eq!(result["request_metrics"]["fallback_rate"], 0.25);
        assert_eq!(result["request_metrics"]["first_attempt_nonstream"], 3);
        assert_eq!(
            result["request_metrics"]["first_attempt_successes_nonstream"],
            1
        );
        let rate = result["request_metrics"]["first_attempt_success_rate_nonstream"]
            .as_f64()
            .unwrap();
        assert!((rate - 1.0 / 3.0).abs() < 1e-9);
    }

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
                failure_count INTEGER,total_elapsed_ms INTEGER,max_elapsed_ms INTEGER,
                failure_type TEXT
             );
             CREATE TABLE request_events(provider TEXT,model TEXT,ts INTEGER,
                 elapsed_ms INTEGER,sse_headers INTEGER);",
        )
        .unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let hour = now - now.rem_euclid(3600);
        db.execute(
            "INSERT INTO hourly_stats VALUES(?1,'test','alpha',20,0,2000,250,'success')",
            [hour],
        )
        .unwrap();
        db.execute(
            "INSERT INTO hourly_stats VALUES(?1,'test','alpha',1,1,100,250,'provider_5xx')",
            [hour],
        )
        .unwrap();
        for t in 0..21 {
            db.execute(
                "INSERT INTO request_events VALUES('test','alpha',?1,?2,0)",
                params![now, t * 10],
            )
            .unwrap();
        }
        for _ in 0..3 {
            db.execute(
                "INSERT INTO request_events VALUES('test','alpha',?1,10000,1)",
                [now],
            )
            .unwrap();
        }
        db.execute(
            "INSERT INTO hourly_stats VALUES(?1,'test','untrusted',60,60,300,100,'caller_error')",
            [hour],
        )
        .unwrap();
        db.execute(
            "INSERT INTO hourly_stats VALUES(?1,'test','untrusted',30,0,100,100,'stream_started')",
            [hour],
        )
        .unwrap();
        // Neither a terminal SSE failure nor key cooldown is an observed
        // model-quality failure in this conservative scoring view.
        db.execute(
            "INSERT INTO hourly_stats VALUES(?1,'test','untrusted',0,1,50,50,'provider_5xx')",
            [hour],
        )
        .unwrap();
        db.execute(
            "INSERT INTO hourly_stats VALUES(?1,'test','untrusted',20,20,100,100,'rate_limited')",
            [hour],
        )
        .unwrap();
        drop(db);
        let result = read_scoring(dir.path(), 1).unwrap();
        assert_eq!(result["candidate_count"], 2);
        assert_eq!(result["candidates"][0]["attempts"], 110);
        assert_eq!(result["candidates"][0]["observed_provider_failures"], 0);
        assert_eq!(result["candidates"][1]["attempts"], 21);
        assert_eq!(result["candidates"][1]["failures"], 1);
        assert_eq!(result["candidates"][0]["qualified"], false);
        assert_eq!(result["candidates"][0]["observed_samples"], 0);
        assert!(result["candidates"][0]["observed_success_rate"].is_null());
        let valid = &result["candidates"][1];
        assert_eq!(valid["qualified"], true);
        assert_eq!(valid["observed_samples"], 21);
        assert_eq!(valid["observed_successes"], 20);
        assert_eq!(valid["observed_provider_failures"], 1);
        assert_eq!(valid["downstream_p95_ms"], 190);
        assert_eq!(valid["stream_header_p95_ms"], 10000);
        assert_eq!(valid["downstream_sample_count"], 21);
        assert_eq!(valid["stream_header_sample_count"], 3);
    }
}
