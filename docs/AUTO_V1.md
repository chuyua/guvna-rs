# Auto V1: opt-in deterministic routing

Auto is a bounded basic routing mechanism, not a learned model scorer.
It is a safe minimal migration without duplicating the existing upstream
candidate, credential retry or failure handling state machine.

Example in config.yaml:

    auto:
      enabled: true
      max_candidates: 2
      candidates:
        - provider: fast
          model: fast-model
          priority: 1
          max_input_bytes: 1024
          max_context_tokens: 8192
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
(default unlimited). It is a rough size heuristic, NOT tokenizer output. max_context_tokens
uses the full serialized request size plus a 20% reserve and the requested
max_completion_tokens/max_tokens (or 1024 tokens by default). This intentionally
overestimates tokens and includes tool/schema payloads, without a remote tokenizer.
If provider.models is non-empty, a candidate model must match that catalog exactly;
an empty catalog means capability is unknown and does not hard-reject candidates.
The process-local cache stores route metadata only (max 128 entries, 30s TTL),
keys results by request-size estimates and live health scores, and clears on a
route-config generation change. It never stores prompts or request bodies.

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
feature adds no remote endpoints; separately opt in to local SQLite request
metadata using GUVNA_TELEMETRY_SQLITE=1. systemd/journald can
collect these lines locally using the OS's existing access controls.

## Deferred scope

This is not full Go Auto parity: no dynamic learned scoring, response caching,
tokenizer-based context classification, or semantic prompt profiling. The route
cache is not a response cache and never retains prompt content. Existing credential
quarantine and provider circuit breaking continue to handle known failures.

## Auto V2: optional in-process health-aware ordering

Add health_aware: true under auto to enable a bounded failure-weighted health
hint. The default is false and leaves the original V1 priority order intact.
The same config candidates, context budget, max_candidates limit and single
upstream failover loop still apply.

When enabled, each configured provider uses only a few in-memory atomic
counters. Three recent transport/provider failures can demote an unhealthy
provider behind a healthy fallback before max_candidates is applied. HTTP
429 can also contribute; caller-side 4xx and credential auth errors do not.
Each successful validated response reduces the penalty. Recovery hysteresis
requires enough successes to reset demotion, and a 300-second freshness limit
expires old health penalties without a background timer. No remote scorer,
prompts, keys, provider labels, extra HTTP calls or request-path SQL reads.

This remains a conservative V2 foundation, not a learned model capability engine.
When SQLite telemetry is enabled, a bounded provider-level health summary is
restored at startup only if the last failure is within the 300-second freshness
window. Failed intermediate candidates are recorded separately from the single
downstream request row, so fallback failures survive restart without double-counting
the request. If SQLite is disabled or unreadable, routing falls back to in-memory hints.