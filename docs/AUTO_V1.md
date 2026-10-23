# Auto V1: opt-in deterministic routing

Auto is a bounded basic routing mechanism, not a learned model scorer.
It is a safe minimal migration without duplicating the existing upstream
candidate, credential retry or failure handling state machine.

Example in config.yaml:

    auto:
      enabled: true
      candidates:
        - provider: fast
          model: fast-model
          priority: 1
          max_input_bytes: 1024
        - provider: reliable
          model: big-context-model
          priority: 3
          min_input_bytes: 1025
        - provider: reliable
          model: fallback
          priority: 99

The provider names must exist. Auto has 1..32 candidates when enabled.
Candidates are sorted by ascending priority, with stable configuration
order for ties. Eligibility is based on the encoded JSON message size
between optional min_input_bytes (default 0) and max_input_bytes
(default unlimited). It is a rough size heuristic, NOT tokenizer output.
Routing stores no user-specific cache entry, prompt or learned model score.

Interfaces:
- POST /v1/auto accepts a Chat Completion body with model omitted or auto.
- POST /v1/chat/completions with model auto uses the same route.
- GET /v1/models includes the auto model when enabled.
- Both endpoints require the same Bearer authentication and global admission.
- The shared Chat candidate/credential/circuit/fallback and streaming
  lifecycle is reused, without introducing a second retry engine.
- When every eligible Auto candidate is in upstream 429 credential
  cooldown, return HTTP 429 with aggregated, bounded Retry-After and
  no extra provider call. Native Chat uses the same state, counter and
  failure classification. Healthy fallback still works, and mixed
  non-429 faults cannot be misreported as global rate limiting.
- /v1/auto rejects other model names with HTTP 400 invalid_auto_model.
- Disabled Auto returns 404 auto_disabled; no eligible size-match candidate
  returns 422 auto_no_eligible_candidate before any network attempt.
- Auto is disabled by default to preserve old configs. No HTTP management
  endpoint, control socket or outbound model discovery is introduced.

## Optional local privacy-safe logs

Set GUVNA_LOG_EVENTS=1 in the service environment to emit warning JSON-lines
to standard error. Fields: ts_unix_ms, level, event, sequence. All event
names are fixed; no prompt, model, provider, client IP, URL or API key is
written. Only the first eight events per aggregate outcome category and every 128th
later event are emitted (other failures share a global counter). This bounded,
fixed-cardinality scheme keeps rare aggregation failures visible even when
successes vastly outnumber them.
Logs are disabled by default.

Warning kinds: transport_failure, provider_http_failure, upstream_invalid,
stream_preflight_failure, provider_circuit_open, rate_limit_exhausted,
aggregate_capacity and
aggregation_failed. Informational event: aggregation_completed. All
events are local-only and sampled; no per-user or per-provider data is logged.

The pre-existing authenticated /metrics endpoint still works but this
feature adds no endpoints or telemetry databases. systemd/journald can
collect these lines locally using the OS's existing access controls.

## Deferred scope

This is Auto V1, NOT full Go Auto parity: no dynamic learned scoring,
response caching, tokenizer-based context classification, cross-request
model reliability weighting, or semantic prompt profiling. Existing
credential quarantine and provider circuit breaking handle known failures.
Further Auto intelligence should follow deterministic real-world tests.
