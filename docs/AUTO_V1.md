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
A candidate may also declare supports_tools, supports_images, or
supports_json_schema as false to hard-exclude incompatible Chat requests.
When omitted, capability is unknown and old configurations remain eligible;
true is an explicit local assertion, not runtime verification of the model.
Capability requirements derive only from Chat tool declarations/history, image
content-part types, and response_format.type=json_schema. The cache key
includes these capability requirements to prevent cross-request misrouting.
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

Warning kinds include transport_failure, provider_http_failure, upstream_invalid,
stream_preflight_failure, provider_circuit_open, rate_limit_exhausted,
aggregate_capacity, aggregation_failed, aggregation_lifetime_timeout,
upstream_body_timeout and walk_deadline_exhausted. Additional bounded
aggregation failure categories (such as aggregation_invalid_sse and
aggregation_idle_timeout) are also emitted. Informational event:
aggregation_completed. All
events are local-only and sampled; no per-user or per-provider data is logged.
The numeric request_id is monotonic only **within one process lifetime**;
it restarts from 1 on each daemon restart. For long-term journald correlation
combine the event timestamp with the process/systemd invocation identity.
The warning list above is illustrative, not an exhaustive enum; additional
fixed local events include telemetry_open_failed, telemetry_write_failed,
telemetry_prune_failed, telemetry_checkpoint_failed and
telemetry_shutdown_flush_failed.

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

When enabled, each configured Auto provider/model candidate uses only a few
in-memory atomic counters. Three recent transport/provider failures can demote
the affected model candidate behind a healthy fallback before max_candidates is applied. HTTP
429 can also contribute; caller-side 4xx and credential auth errors do not.
Each successful validated response reduces the penalty. Recovery hysteresis
requires enough successes to reset demotion, and a 300-second freshness limit
expires old health penalties without a background timer. No remote scorer,
prompts, keys, provider labels, extra HTTP calls or request-path SQL reads.

This remains a conservative V2 foundation, not a learned model capability engine.
When SQLite telemetry is enabled, a bounded per-provider/model health summary is
restored at startup only if the last failure is within the 300-second freshness
window. Failed intermediate candidates are recorded separately from the single
downstream request row, so fallback failures survive restart without double-counting
the request. If SQLite is disabled or unreadable, routing falls back to in-memory hints.
## Issue #129: optional dynamic chain discovery

When auto.enabled: true, auto.discover_from_chains: true can add
provider/model candidates from validated named chains at gateway startup.
Explicit auto.candidates remain first, duplicate provider/model pairs are
removed, the configured default_chain is considered first, and discovery
never produces more than 32 candidates. Steps with custom params or models
missing from an explicitly declared provider model catalog are not silently
coerced into Auto candidates. Missing candidates after discovery
cause startup to fail safely. This discovery is a startup snapshot, not live
reconfiguration; it does not enable remote management or a second scheduler.

## Issue #129: bounded optional LLM Decider (experimental)

Example under the Auto configuration (leave disabled by default):

    auto:
      enabled: true
      decider:
        enabled: true
        provider: p
        model: your-selector-model
        timeout_ms: 750
        cache_ttl_secs: 30

The provider must already be declared and have a credential. The selector
connects directly to that provider's OpenAI-compatible chat endpoint using
the explicitly selected wire model, not the local /v1/auto endpoint.
Only candidate labels, context-size estimates and capability flags are
sent to the selector. Actual user prompts, messages, tool schemas, request
bodies and keys are never part of the ranking prompt or persisted.
Response must be a strict JSON permutation of eligible candidate indexes.
Unavailable, slow or malformed selectors fall back immediately to existing
priority/health ranking, with at most 120 seconds of previously verified stale
ranking as a bounded exception. An in-flight selector does not serialize
concurrent callers: they use deterministic fallback rather than wait. Cache
has 32 entries and is keyed by ordered candidates, size buckets and required
capabilities. This is not an upstream model response cache.
