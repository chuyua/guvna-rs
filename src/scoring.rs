//! Offline, read-only SQLite scoring summary for the local Unix control socket.
//! Uses no new management TCP endpoint and no request-path SQL queries.
use rusqlite::{Connection, OpenFlags, params};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const MAX_CANDIDATES: usize = 32;
const MAX_LATENCY_SAMPLES: usize = 512;
const MAX_SHADOW_SAMPLES: usize = 256;
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

/// Correlate bounded real outcomes with shadow advice; never infer the
/// success of a route that was not actually executed.
fn sampled_shadow_outcomes(conn: &Connection, since: i64) -> Option<Value> {
    // Timestamp-indexed bounded reads avoid unbounded request-id joins.
    let mut requests = HashMap::new();
    let mut query = conn
        .prepare(
            "SELECT request_id,provider,model,attempt_index,stream_requested,failure_type
         FROM request_events WHERE ts >= ?1 AND request_id IS NOT NULL
         ORDER BY ts DESC LIMIT 512",
        )
        .ok()?;
    let rows = query
        .query_map(params![since], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .ok()?;
    for row in rows {
        let (id, provider, model, attempt, stream, outcome) = row.ok()?;
        requests
            .entry(id)
            .or_insert((provider, model, attempt, stream, outcome));
    }
    drop(query);

    let mut terminals = HashMap::new();
    // Old databases may have no terminal table. Unknown is never success.
    if let Ok(mut query) = conn.prepare(
        "SELECT request_id,attempt_index,provider,model,outcome FROM stream_terminals
         WHERE ts >= ?1 ORDER BY ts DESC LIMIT 512",
    ) {
        let rows = query
            .query_map(params![since], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .ok()?;
        for row in rows {
            let (id, attempt, provider, model, outcome) = row.ok()?;
            terminals
                .entry((id, attempt))
                .or_insert((provider, model, outcome));
        }
    }

    let mut query = conn
        .prepare(
            "SELECT request_id,baseline_provider,baseline_model,
                suggested_provider,suggested_model,available,disagreed
         FROM shadow_events WHERE ts >= ?1 ORDER BY ts DESC LIMIT 256",
        )
        .ok()?;
    let rows = query
        .query_map(params![since], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })
        .ok()?;
    let mut sampled = 0_u64;
    let mut linked = 0_u64;
    let mut completed = 0_u64;
    let mut failed = 0_u64;
    let mut cancelled = 0_u64;
    let mut pending_streams = 0_u64;
    let mut fallbacks = 0_u64;
    let mut first_choice_successes = 0_u64;
    let mut proposed_matches_final = 0_u64;
    let mut disagreed_actual_completions = 0_u64;
    for row in rows {
        let (
            id,
            baseline_provider,
            baseline_model,
            suggested_provider,
            suggested_model,
            available,
            disagreed,
        ) = row.ok()?;
        sampled += 1;
        let Some((provider, model, attempt, stream, outcome)) = requests.get(&id) else {
            continue;
        };
        linked += 1;
        if attempt.is_some_and(|value| value > 1) {
            fallbacks += 1;
        }
        if available == 1
            && suggested_provider.as_ref() == Some(provider)
            && suggested_model.as_ref() == Some(model)
        {
            proposed_matches_final += 1;
        }
        let actual = if *stream == 1 && matches!(outcome.as_str(), "stream_started" | "success") {
            // A 2xx response or SSE headers alone cannot certify completion.
            // Terminal for another candidate cannot certify this stream.
            attempt
                .and_then(|index| terminals.get(&(id.clone(), index)))
                .and_then(|(terminal_provider, terminal_model, terminal_outcome)| {
                    (terminal_provider == provider && terminal_model == model)
                        .then_some(terminal_outcome.as_str())
                })
        } else {
            // A stream request rejected before headers has a real failure
            // outcome on its request row, even without an SSE terminal.
            Some(outcome.as_str())
        };
        match actual {
            Some("success") => {
                completed += 1;
                if *attempt == Some(1) && baseline_provider == *provider && baseline_model == *model
                {
                    first_choice_successes += 1;
                }
                if disagreed == 1 {
                    disagreed_actual_completions += 1;
                }
            }
            Some("client_cancelled") => cancelled += 1,
            Some(_) => failed += 1,
            None => {
                if *stream == 1 {
                    pending_streams += 1;
                }
            }
        }
    }
    Some(json!({
        "sampled_shadow_rows":sampled,
        "sample_limit":MAX_SHADOW_SAMPLES,
        "linked_actual_requests":linked,
        "unlinked_actual_requests":sampled.saturating_sub(linked),
        "actual_completed":completed,
        "actual_failed":failed,
        "actual_client_cancelled":cancelled,
        "actual_stream_pending_or_unobserved":pending_streams,
        "actual_fallbacks":fallbacks,
        "baseline_first_choice_successes":first_choice_successes,
        "proposed_top_matches_actual_final_route":proposed_matches_final,
        "disagreed_but_actual_completed":disagreed_actual_completions,
        "observational_only":true,
        "counterfactual_success_inferred":false,
    }))
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
    let shadow_metrics = if conn
        .prepare("SELECT request_id,available,disagreed FROM shadow_events LIMIT 0")
        .is_ok()
    {
        let (total, available, disagreements): (i64, i64, i64) = conn
            .query_row(
                "SELECT COUNT(*),COALESCE(SUM(available),0),COALESCE(SUM(disagreed),0)
             FROM shadow_events WHERE ts >= ?1",
                params![since],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|_| "cannot read shadow comparison summary")?;
        Some(json!({
            "evaluations":total,
            "valid_rankings":available,
            "unavailable_rankings":total.saturating_sub(available),
            "top_candidate_disagreements":disagreements,
            "disagreement_rate":if available > 0 {
                Some(disagreements as f64 / available as f64)
            } else {None},
            "advisory_only":true,
            "observed_actual_sample":sampled_shadow_outcomes(&conn, since),
        }))
    } else {
        None
    };
    // Legacy databases without the terminal table remain readable. These
    // are actual end-of-stream observations, not inferred from SSE headers.
    let stream_terminal_metrics = if conn
        .prepare("SELECT request_id,outcome FROM stream_terminals LIMIT 0")
        .is_ok()
    {
        let (total, completed, failed, cancelled): (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT COUNT(*),
                    COALESCE(SUM(outcome='success'),0),
                    COALESCE(SUM(outcome='transport_error'),0),
                    COALESCE(SUM(outcome='client_cancelled'),0)
                 FROM stream_terminals WHERE ts >= ?1",
                params![since],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(|_| "cannot read stream terminal summary")?;
        Some(json!({
            "terminal_observations":total,
            "completed":completed,
            "upstream_failed":failed,
            "client_cancelled":cancelled,
            "completion_rate_excluding_client_cancellation":
                if total > cancelled {
                    Some(completed as f64 / (total - cancelled) as f64)
                } else {None},
            "actual_terminal_only":true,
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
        "shadow_metrics":shadow_metrics,
        "stream_terminal_metrics":stream_terminal_metrics,
         "shadow_sample_notes":"The bounded recent shadow sample links only real final request outcomes by request ID. SSE success requires a matching terminal observation. A proposed route matching the actual final route is not proof the proposed strategy would have succeeded; missing and pending observations remain unknown.",
         "request_metrics_notes":"Only traced terminal request rows count toward fallback rate; the first-attempt success rate is restricted to nonstream completions, not unverified SSE headers. Old SQLite databases report null until migrated.",
         "notes":"raw success_rate includes all failure classes; observed_success_rate counts only confirmed nonstream successes and attempted upstream provider failures, excluding caller/local/credential errors and unverified stream starts. Qualification requires 20 observed samples. p95 separates nonstream completion from SSE response-header latency, sampled from up to 512 recent downstream rows per candidate; neither is a counterfactual outcome or complete SSE duration.",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shadow_observation_only_counts_real_outcomes_and_correct_sse_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let directory = dir.path().join("telemetry");
        std::fs::create_dir_all(&directory).unwrap();
        let db = Connection::open(directory.join("requests.sqlite3")).unwrap();
        db.execute_batch(
            "CREATE TABLE hourly_stats (
                hour_ts INTEGER,provider TEXT,model TEXT,request_count INTEGER,
                failure_count INTEGER,total_elapsed_ms INTEGER,
                max_elapsed_ms INTEGER,failure_type TEXT
             );
             CREATE TABLE request_events (
                ts INTEGER,provider TEXT,model TEXT,elapsed_ms INTEGER,
                sse_headers INTEGER,request_id TEXT,attempt_index INTEGER,
                stream_requested INTEGER,failure_type TEXT
             );
             CREATE INDEX request_events_ts ON request_events(ts);
             CREATE TABLE shadow_events (
                ts INTEGER,request_id TEXT,baseline_provider TEXT,baseline_model TEXT,
                suggested_provider TEXT,suggested_model TEXT,available INTEGER,disagreed INTEGER
             );
             CREATE INDEX shadow_events_ts ON shadow_events(ts);
             CREATE TABLE stream_terminals (
                ts INTEGER,request_id TEXT,attempt_index INTEGER,
                provider TEXT,model TEXT,outcome TEXT
             );
             CREATE INDEX stream_terminals_ts ON stream_terminals(ts);",
        )
        .unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        for (id, proposed, available, disagreed) in [
            ("direct", Some("b"), 1, 1),
            ("fallback", Some("b"), 1, 1),
            ("cancelled", Some("b"), 1, 1),
            ("missing", None, 0, 0),
            ("stream_ok", Some("a"), 1, 0),
            ("wrong_terminal", Some("b"), 1, 1),
            ("failed", None, 0, 0),
            ("preheader_failure", Some("b"), 1, 1),
            ("unverified_success", Some("b"), 1, 1),
        ] {
            db.execute(
                "INSERT INTO shadow_events VALUES(?1,?2,'p','a','p',?3,?4,?5)",
                params![now, id, proposed, available, disagreed],
            )
            .unwrap();
        }
        for (id, model, attempt, headers, stream, outcome) in [
            ("direct", "a", 1, 0, 0, "success"),
            ("fallback", "b", 2, 0, 0, "success"),
            ("cancelled", "a", 1, 1, 1, "stream_started"),
            ("stream_ok", "a", 1, 1, 1, "stream_started"),
            ("wrong_terminal", "a", 1, 1, 1, "stream_started"),
            ("failed", "a", 1, 1, 1, "stream_started"),
            ("preheader_failure", "a", 1, 0, 1, "provider_5xx"),
            ("unverified_success", "a", 1, 0, 1, "success"),
        ] {
            db.execute(
                "INSERT INTO request_events VALUES(?1,'p',?2,15,?3,?4,?5,?6,?7)",
                params![now, model, headers, id, attempt, stream, outcome],
            )
            .unwrap();
        }
        for (id, attempt, provider, model, outcome) in [
            ("cancelled", 1, "p", "a", "client_cancelled"),
            ("stream_ok", 1, "p", "a", "success"),
            // A wrong model with the same request and attempt is unverified.
            ("wrong_terminal", 1, "p", "other", "success"),
            ("failed", 1, "p", "a", "transport_error"),
        ] {
            db.execute(
                "INSERT INTO stream_terminals VALUES(?1,?2,?3,?4,?5,?6)",
                params![now, id, attempt, provider, model, outcome],
            )
            .unwrap();
        }
        let result = read_scoring(dir.path(), 1).unwrap();
        let observed = &result["shadow_metrics"]["observed_actual_sample"];
        assert_eq!(observed["sampled_shadow_rows"], 9);
        assert_eq!(observed["linked_actual_requests"], 8);
        assert_eq!(observed["unlinked_actual_requests"], 1);
        assert_eq!(observed["actual_completed"], 3);
        assert_eq!(observed["actual_failed"], 2);
        assert_eq!(observed["actual_client_cancelled"], 1);
        assert_eq!(observed["actual_stream_pending_or_unobserved"], 2);
        assert_eq!(observed["actual_fallbacks"], 1);
        assert_eq!(observed["baseline_first_choice_successes"], 2);
        assert_eq!(observed["proposed_top_matches_actual_final_route"], 2);
        assert_eq!(observed["disagreed_but_actual_completed"], 2);
        assert_eq!(observed["counterfactual_success_inferred"], false);
        // Fixed sample size even if the historical window contains many rows.
        for i in 0..300 {
            db.execute(
                "INSERT INTO shadow_events VALUES(?1,?2,'p','a',NULL,NULL,0,0)",
                params![now + 1, format!("unlinked-{i}")],
            )
            .unwrap();
        }
        let bounded = read_scoring(dir.path(), 1).unwrap();
        assert_eq!(bounded["shadow_metrics"]["evaluations"], 309);
        assert_eq!(
            bounded["shadow_metrics"]["observed_actual_sample"]["sampled_shadow_rows"],
            256
        );
        assert_eq!(
            bounded["shadow_metrics"]["observed_actual_sample"]["linked_actual_requests"],
            0
        );
    }

    #[test]
    fn legacy_shadow_table_without_request_ids_remains_readable() {
        let dir = tempfile::tempdir().unwrap();
        let directory = dir.path().join("telemetry");
        std::fs::create_dir_all(&directory).unwrap();
        let db = Connection::open(directory.join("requests.sqlite3")).unwrap();
        db.execute_batch(
            "CREATE TABLE hourly_stats (
                hour_ts INTEGER,provider TEXT,model TEXT,request_count INTEGER,
                failure_count INTEGER,total_elapsed_ms INTEGER,
                max_elapsed_ms INTEGER,failure_type TEXT
             );
             CREATE TABLE request_events (
                ts INTEGER,provider TEXT,model TEXT,elapsed_ms INTEGER,sse_headers INTEGER
             );
             CREATE TABLE shadow_events (
                ts INTEGER,request_id TEXT,baseline_provider TEXT,baseline_model TEXT,
                suggested_provider TEXT,suggested_model TEXT,available INTEGER,disagreed INTEGER
             );",
        )
        .unwrap();
        let result = read_scoring(dir.path(), 1).unwrap();
        assert_eq!(result["shadow_metrics"]["evaluations"], 0);
        assert!(result["shadow_metrics"]["observed_actual_sample"].is_null());
    }

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
        assert!(result["shadow_metrics"].is_null());
        assert!(result["stream_terminal_metrics"].is_null());
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
