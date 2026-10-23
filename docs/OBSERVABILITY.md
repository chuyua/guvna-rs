# Guvna operational telemetry

The Rust gateway exposes lightweight counters in Prometheus text format:

    GET /metrics
    Authorization: Bearer <API_KEYS or ADMIN_KEY member>

The endpoint uses the existing client-key authorization and returns 401 to
unauthorized callers. No public unauthenticated telemetry is exposed.

## Why a fixed-cardinality design?

The deployment targets are low-RAM, single-user Linux instances. Every metric
has a fixed name and no labels. Counters live in relaxed AtomicU64 values;
they do not allocate per request, keep request content, record upstream URLs,
key names, credentials, model identifiers, caller IPs or conversation contents.
They reset on process restart. The endpoint intentionally does not expose
the credential pool's internal key-level state.

All counters ending in _total are cumulative. The active_requests gauge is
the number of acquired request admission permits, not the number of currently
connected clients. The shared 128-permit gate applies **after** authentication
but **before** buffering request bodies, including still-uploading clients.
Long-running SSE streams retain their permits until completion/cancellation.

## Metric families

- guvna_requests_dispatched_total: authorized, parsed requests entering the
  common upstream dispatcher (Responses also uses the common dispatcher).
- guvna_upstream_attempts_total: outbound HTTP attempts, including different
  keys and provider fallbacks.
- guvna_upstream_success_total: fully accepted non-streaming responses and
  Chat SSE streams whose terminal [DONE] was delivered.
- guvna_upstream_transport_failures_total: outbound send/transport errors.
- guvna_upstream_http_failures_total: upstream non-2xx HTTP statuses.
- guvna_upstream_invalid_responses_total: malformed non-streaming 2xx payloads
  or streams that failed initial SSE validation before relay.
- guvna_credential_quarantines_total: credential isolation decisions actually
  applied; stale concurrent failures cannot increase this counter.
- guvna_candidate_fallbacks_total: transitions to the next chain step, not
  each attempted credential or retry.
- guvna_admission_rejections_total: requests rejected with 503 and
  Retry-After: 1 when concurrency admission is exhausted, including requests
  whose body has not yet been read.
- guvna_chat_stream_completed_total, chat_stream_failed_total,
  chat_stream_cancelled_total: validated upstream Chat SSE lifecycle.
- guvna_responses_stream_completed_total, responses_stream_incomplete_total,
  responses_stream_failed_total, responses_stream_cancelled_total: translated
  Responses SSE lifecycle.
- guvna_circuit_openings_total: provider circuits opened/reopened after
  failures, including failed recovery probes.
- guvna_circuit_bypasses_total: candidate calls skipped due to a currently
  open/half-open breaker, without any upstream request.
- guvna_circuit_probe_success_total / circuit_probe_failures_total:
  single half-open recovery probe outcomes.
- guvna_circuit_open_providers: per-process number of open/half-open
  circuits; a gauge without provider labels.
- guvna_active_requests: currently acquired admission permits.

Chat stream counters also include the upstream Chat SSE part of a Responses
stream. For a fully successful Responses SSE request, both the Chat and
Responses completed counters increment. A valid upstream Chat SSE can still
fail in the Responses translation (e.g. unsupported delta), so the two
terminal-count families may differ. This is intentional.

A failed stream and a cancelled stream are distinct:
- Failure means a terminal error frame was emitted after malformed/truncated
  upstream data or a 15-second stall.
- Cancellation means the downstream request body was dropped before any
  terminal frame. The upstream network body and its admission permit are
  dropped together; a client disconnect must not be classified as provider
  failure or credential quarantine.

## Validation and alerting

The Linux release-binary quality gate now asserts the telemetry endpoint
is not public, metrics contain no credentials, and the observed upstream
attempt count matches the mock providers across 2,000 normal requests,
2,000 failure-injected requests and 200 concurrent Responses SSE requests.
It also checks that expected success, fallback and stream counters reconcile,
and that the active request gauge returns to zero.

The metrics endpoint is deliberately small and stateless: there is no disk
database, no background exporter task and no added third-party telemetry
dependency. For external monitoring, use an authenticated scrape over a
trusted loopback/tunnel; do not put tokens in query strings.
