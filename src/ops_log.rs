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

fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        std::env::var("GUVNA_LOG_EVENTS")
            .ok()
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
    })
}

/// One of a bounded list of fixed event names, supplied by source constants.
pub fn warn(event: &'static str) {
    if !enabled() {
        return;
    }
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1;
    // Do not overwhelm stderr if thousands of providers fail during load.
    if sequence > 8 && !sequence.is_multiple_of(128) {
        return;
    }
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    eprintln!(
        "{{\"ts_unix_ms\":{timestamp_ms},\"level\":\"warn\",\"event\":\"{event}\",\"sequence\":{sequence}}}"
    );
}

#[cfg(test)]
mod tests {
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
