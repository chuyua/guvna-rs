//! Fixed-cardinality, lock-free counters: never store prompts, model identifiers, keys or IPs.
//! Reset on process restart. The endpoint requires the same Bearer auth as the gateway.
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Metrics {
    pub requests_dispatched: AtomicU64,
    pub upstream_attempts: AtomicU64,
    pub upstream_success: AtomicU64,
    pub upstream_transport_failures: AtomicU64,
    pub upstream_http_failures: AtomicU64,
    pub upstream_invalid_responses: AtomicU64,
    pub credential_quarantines: AtomicU64,
    pub candidate_fallbacks: AtomicU64,
    pub admission_rejections: AtomicU64,
    pub chat_stream_completed: AtomicU64,
    pub chat_stream_failed: AtomicU64,
    pub chat_stream_cancelled: AtomicU64,
    pub responses_stream_completed: AtomicU64,
    pub responses_stream_incomplete: AtomicU64,
    pub responses_stream_failed: AtomicU64,
    pub responses_stream_cancelled: AtomicU64,
}
impl Metrics {
    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// No labels: zero user data exposure and constant memory independent of models/providers.
    pub fn prometheus(&self, active_requests: usize) -> String {
        let mut result = String::with_capacity(1200);
        macro_rules! emit {
            ($name:literal, $field:expr) => {{
                result.push_str(concat!("# TYPE guvna_", $name, " counter\n"));
                result.push_str(concat!("guvna_", $name, " "));
                result.push_str(&$field.load(Ordering::Relaxed).to_string());
                result.push('\n');
            }};
        }
        emit!("requests_dispatched_total", self.requests_dispatched);
        emit!("upstream_attempts_total", self.upstream_attempts);
        emit!("upstream_success_total", self.upstream_success);
        emit!(
            "upstream_transport_failures_total",
            self.upstream_transport_failures
        );
        emit!("upstream_http_failures_total", self.upstream_http_failures);
        emit!(
            "upstream_invalid_responses_total",
            self.upstream_invalid_responses
        );
        emit!("credential_quarantines_total", self.credential_quarantines);
        emit!("candidate_fallbacks_total", self.candidate_fallbacks);
        emit!("admission_rejections_total", self.admission_rejections);
        emit!("chat_stream_completed_total", self.chat_stream_completed);
        emit!("chat_stream_failed_total", self.chat_stream_failed);
        emit!("chat_stream_cancelled_total", self.chat_stream_cancelled);
        emit!(
            "responses_stream_completed_total",
            self.responses_stream_completed
        );
        emit!(
            "responses_stream_incomplete_total",
            self.responses_stream_incomplete
        );
        emit!(
            "responses_stream_failed_total",
            self.responses_stream_failed
        );
        emit!(
            "responses_stream_cancelled_total",
            self.responses_stream_cancelled
        );
        result.push_str("# TYPE guvna_active_requests gauge\nguvna_active_requests ");
        result.push_str(&active_requests.to_string());
        result.push('\n');
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metrics_have_fixed_names_and_constant_size_without_sensitive_labels() {
        let metrics = Metrics::default();
        for _ in 0..100 {
            Metrics::inc(&metrics.upstream_attempts);
        }
        let rendered = metrics.prometheus(7);
        assert!(rendered.contains("guvna_upstream_attempts_total 100\n"));
        assert!(rendered.contains("guvna_active_requests 7\n"));
        assert!(!rendered.contains('{'));
        assert!(rendered.len() < 2000);
        assert_eq!(rendered.matches("# TYPE guvna_").count(), 17);
    }
}
