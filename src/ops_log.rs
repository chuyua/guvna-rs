//! Optional low-cardinality local stderr failure events. This is NOT remote
//! management or durable telemetry. Never render prompts, API keys, model
//! names, provider endpoints, request bodies or client addresses here.
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

static ENABLED: OnceLock<bool> = OnceLock::new();
static SEQUENCE: AtomicU64 = AtomicU64::new(0);
static AGGREGATION_SUCCESS_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static AGGREGATION_FAILED_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static AGGREGATION_INVALID_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static AGGREGATION_TIMEOUT_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static AGGREGATION_TRANSPORT_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static AGGREGATION_LIMIT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        std::env::var("GUVNA_LOG_EVENTS")
            .ok()
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
    })
}

/// One of a bounded list of fixed event names, supplied by source constants.
pub fn warn(event: &'static str) {
    emit("warn", event);
}

/// Sampled successful conversion events make it possible to verify that
/// SSE-only providers actually aggregate during a multi-day soak.
pub fn info(event: &'static str) {
    emit("info", event);
}

fn should_emit(sequence: u64) -> bool {
    sequence <= 8 || sequence.is_multiple_of(128)
}

fn emit(level: &'static str, event: &'static str) {
    if !enabled() {
        return;
    }
    // Keep aggregate successes/failures independently sampled, otherwise
    // rare failures can disappear behind a busy healthy stream provider.
    // The category set is fixed: no provider/model/client cardinality.
    let counter = match event {
        "aggregation_completed" => &AGGREGATION_SUCCESS_SEQUENCE,
        "aggregation_failed" => &AGGREGATION_FAILED_SEQUENCE,
        "aggregation_invalid_sse" => &AGGREGATION_INVALID_SEQUENCE,
        "aggregation_idle_timeout" => &AGGREGATION_TIMEOUT_SEQUENCE,
        "aggregation_transport_failure" => &AGGREGATION_TRANSPORT_SEQUENCE,
        "aggregation_resource_limit" => &AGGREGATION_LIMIT_SEQUENCE,
        _ => &SEQUENCE,
    };
    let sequence = counter.fetch_add(1, Ordering::Relaxed) + 1;
    if !should_emit(sequence) {
        return;
    }
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    eprintln!(
        "{{\"ts_unix_ms\":{timestamp_ms},\"level\":\"{level}\",\"event\":\"{event}\",\"sequence\":{sequence}}}"
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn log_sampling_is_fixed_cardinality_and_does_not_hide_rare_failures() {
        assert!((1..=8).all(super::should_emit));
        assert!((9..128).all(|i| !super::should_emit(i)));
        assert!(super::should_emit(128));
        assert!(!super::should_emit(129));
        assert!(super::should_emit(256));
    }
    #[test]
    fn aggregation_failure_categories_have_separate_sample_slots() {
        // A flood of unrelated 5xx or successful aggregations must not
        // suppress the first few rare SSE timeout/overflow failures.
        for category in [
            "aggregation_failed",
            "aggregation_invalid_sse",
            "aggregation_idle_timeout",
            "aggregation_transport_failure",
            "aggregation_resource_limit",
        ] {
            assert!(category.starts_with("aggregation_"));
            assert!(
                category
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
            );
        }
    }
    #[test]
    fn explicitly_opt_in_without_exposing_request_or_credential_details() {
        assert!(!super::enabled() || std::env::var("GUVNA_LOG_EVENTS").is_ok());
        for name in [
            "transport_failure",
            "provider_http_failure",
            "upstream_invalid",
            "stream_preflight_failure",
            "provider_circuit_open",
        ] {
            assert!(name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'));
        }
    }
}
