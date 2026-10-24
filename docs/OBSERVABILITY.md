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
- guvna_aggregation_busy_total: SSE-to-JSON aggregation capacity rejections.
- guvna_aggregation_failures_total: failed SSE-to-JSON aggregation attempts
  (idle/lifetime timeout, protocol mismatch, limit or upstream transport).
  Local aggregator limits do not count as provider breaker failures.
- guvna_aggregation_completed_total: validated SSE-only upstream responses
  converted into ordinary JSON.
- guvna_credential_quarantines_total: credential isolation decisions actually
  applied; stale concurrent failures cannot increase this counter.
- guvna_candidate_fallbacks_total: transitions to the next chain step, not
  each attempted credential or retry.
- guvna_rate_limit_exhaustions_total: downstream requests returning HTTP 429
  with bounded Retry-After because every eligible upstream candidate has
  all usable keys in 429 cooldown. Never includes key/provider labels.
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
- guvna_walk_deadline_exhaustions_total: non-streaming requests that
  exhausted the configured TOTAL candidate/key/failover time budget.
  Incremented exactly once on terminal 504, independent of upstream
  invalid-response, transport and credential failures. Includes SSE-only
  upstream aggregation into JSON; normal client-facing SSE is exempt.
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

The metrics endpoint is deliberately small and stateless: optional local SQLite metadata is available only when explicitly enabled. For external monitoring, use an authenticated scrape over a
trusted loopback/tunnel; do not put tokens in query strings.

## Optional local SQLite request metadata (Issue #69)

Set GUVNA_TELEMETRY_SQLITE=1 to enable persistent local request metadata.
Without this setting the database writer is not started. Data is written to
GUVNA_DATA_DIR/telemetry/requests.sqlite3 (or the selected -data directory).

Rows contain timestamp, fixed endpoint category, configured provider/model labels
(bounded and sanitized), downstream status, elapsed milliseconds, SSE-header
state and a fixed failure category. Prompts, request/response bodies, credentials,
URLs, request IDs, sensitive headers and client IPs are never stored. Unconfigured
caller-controlled passthrough model suffixes are labeled `passthrough` instead. Each
downstream request is recorded once; failed intermediate provider candidates are
also retained in failure history and hourly attempt statistics. hourly_stats.request_count
counts provider attempts and failure_count counts failed outcomes. SSE terminal failure
adds a failure row without counting the same attempt twice.
For streaming terminal failures, a separate hourly failure row intentionally
has request_count=0 and failure_count=1. Therefore, failure_count is an event
count, **not** an attempt-failure ratio numerator; it may exceed request_count.
Do not divide hourly failure_count by request_count without reconciling the
separate terminal stream observations.

A 512-item nonblocking sync queue feeds a single SQLite WAL worker (batches
up to 64 rows, 250ms collection wait). The SQLite worker attempts to limit its
own main database growth to 32MiB and targets a 1MiB journal. These are
connection-local settings, not global database or filesystem quotas; external
writers may exceed them. Use a filesystem quota for a hard storage ceiling.
Request details are retained for up to seven days; failure details and hourly
aggregates for up to 30 days. The worker also prunes on idle timeouts and
rejects far-future timestamps. At its connection-local page ceiling it first
prunes expired rows, then may evict a bounded oldest slice to preserve future
write availability rather than permanently disabling telemetry. Legacy request tables are migrated without discarding existing rows.
Write failures trigger bounded reconnect backoff and privacy-safe sampled local
warning events. Optional telemetry startup failures are nonfatal; a full or locked
database cannot block the gateway. On graceful SIGTERM the gateway drains
active requests, then waits up to three seconds for a telemetry queue
barrier; failures emit `telemetry_shutdown_flush_failed`.
New fixed-cardinality metrics:
guvna_telemetry_written_total, guvna_telemetry_dropped_total, and
guvna_telemetry_write_failures_total.

Provider/model labels and fixed failure categories are stored locally. With
health_aware enabled, per-provider/model health summaries are restored from SQLite only
when the last failure is within the 300-second freshness window. Live routing
uses bounded candidate-level scores,
not a learned model. The telemetry directory is local and limited to the service user.
The daemon sets umask 0077 before creating SQLite or binding its control
socket; main DB, WAL and SHM are owner-only (0600) and an existing database
symlink is rejected to avoid opening an unexpected target.
## Local runtime status (Unix only)

On Linux, guvna serve creates GUVNA_DATA_DIR/control.sock. The containing
directory is set to mode 0700 and the socket to 0600. guvna status sends one
bounded JSON-line status command and prints a runtime snapshot containing
uptime, in-flight count, open-circuit count, provider count, Auto/SQLite flags
and fixed request/telemetry counters. No provider names, model names, secrets,
credentials or request bodies are returned. Only the read-only status command
is supported; there is no mutation protocol and no additional TCP listener.
If the socket is absent, status retains the legacy loopback HTTP health probe.
Stale sockets are cleaned on startup only after confirming the path is a Unix
socket and a connection is refused; non-socket files and active sockets are
never replaced.

An unavailable or stale control socket causes `guvna status` to use the
loopback health probe. Existing user-supplied data directories retain their
permissions; newly created ones are secured at 0700. The control socket is
secured at 0600 and peer credentials are verified on Linux.