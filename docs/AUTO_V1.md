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
Capability requirements derive from Chat tool declarations/history (including
non-empty historical tool_calls and explicit required/named tool_choice), image
content-part types, and response_format.type=json_schema. Null/empty historical
tool_calls do not by themselves require tool support. Malformed non-null tool
controls remain conservatively tool-required rather than bypassing a declared
supports_tools: false candidate. The cache key
includes these capability requirements to prevent cross-request misrouting.
The process-local cache stores route metadata only (max 128 entries, 30s TTL),
keys results by request-size estimates, live health scores and the immutable
route-config generation. Old in-flight requests cannot erase the newly
published generation's hot cache; both generations share the same 128-entry
LRU and 30s TTL. It never stores prompts or request bodies.

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
  endpoint or outbound model discovery is introduced. The newer `guvna`
  local CLI uses a private Unix control socket for **local-only** status,
  scoring and chain mutations; it is not an Auto admin HTTP endpoint.

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
SQLite provider-health persistence uses the same failure attribution as the
in-memory Auto health hints: a genuine upstream transport/5xx failure remains
health evidence, while local SSE-to-JSON aggregation policy failures (such as
unsupported delta formats, size caps, or local idle/lifetime limits) remain
visible in failure telemetry without demoting a provider after a restart.
This distinction is verified by an end-to-end Auto fallback/restart test.
Provider-health identity must also be exact: names truncated or sanitized for
diagnostic labels, and the generic passthrough label, never update or restore
SQLite Auto penalties. When migrating an older SQLite database, existing
provider-health rows lack proof of their original identities and are ignored
for Auto ordering; the first new trusted observation resets that row's
penalty/streak rather than inheriting potentially colliding history. Request,
failure, and hourly telemetry remains intact. This intentionally allows
unknown historical health to recover neutrally rather than demoting the wrong
provider/model after an upgrade.
Approved telemetry-based Auto ordering uses a stricter evidence boundary than
the local diagnostic scoring view: each hourly candidate/failure-class bucket
and each recent Chat request must have an exact verified provider/model
identity. Legacy hourly/request rows are migrated as unverified, and even one
sanitized-label collision or non-upstream observation makes the affected
hourly bucket ineligible for Auto ranking. The operator's CLI diagnostics
continue to show the original counts. Ranking safely falls back to the
deterministic Auto order until enough clean, recent evidence accumulates;
historical aggregate counts cannot be retroactively verified.
## Issue #129: optional dynamic chain discovery

When auto.enabled: true, auto.discover_from_chains: true can add
provider/model candidates from validated named chains at gateway startup.
Explicit auto.candidates remain first, duplicate provider/model pairs are
removed, the configured default_chain is considered first, and discovery
never produces more than 32 candidates. Go-style step params are copied as
fill-missing defaults: an explicit caller value, including null, always wins.
Selection-only min_context/max_context in a chain step are not copied, and
models missing from an explicitly declared provider catalog are excluded.
Manual auto.candidates can also define params with the same semantics;
routing-control and selection-only fields are rejected during validation.
Missing candidates after discovery
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
has 32 entries and is keyed by ordered candidates, exact input/context estimates and required
capabilities. This is not an upstream model response cache.

### Non-mutating Shadow rank comparison (Issue #129)

Set auto.decider.shadow_only: true with an enabled decider. The
decider runs asynchronously and its proposed first candidate is compared
against the deterministic live route; it NEVER reorders the live request.
Local-only runtime status exposes auto_shadow_total,
auto_shadow_disagreements and auto_shadow_unavailable without model,
provider or prompt labels. These are ranking agreement measurements, NOT
counterfactual success rates or latency improvement guarantees.

## Optional Go-compatible conservative capability registry (#129)

Set auto.use_model_registry: true to enable an embedded conservative context
window and known text-only model catalogue derived from the Go baseline.
Unknown models receive an 8192-token limit (rather than zero) and otherwise
remain image-eligible. To avoid silently changing legacy routes this feature
is disabled by default.

Explicit per-candidate max_context_tokens / supports_images take precedence.
Local config.auto.capability_overrides can override context_tokens and
supports_images by exact wire model name, without fetching external metadata.
Overrides also work without enabling the embedded registry. The table is a
conservative local hint and is not dynamically proven against upstream.


### Streaming isolation

Streaming Auto requests never wait for an external selector: the gateway
uses a fresh cached ranking, a verified stale ranking (at most 120 seconds
beyond TTL), or the original deterministic/health order. Cold or stale
rankings trigger a bounded background refresh, with concurrent requests
falling back instead of queuing behind the selector. The refresh does not
alter an already-running SSE response or the shared candidate fallback walk.
Nonstreaming requests retain the existing bounded synchronous selector.

## Request-level correlation and fallback evidence (Issue #129)

When SQLite telemetry is explicitly enabled, the writer migrates
request_events and failure_events in place with nullable request_id,
attempt_index (1-based) and stream_requested fields. A random-free,
restart-distinct timestamp/counter trace ID associates one terminal
downstream request row with intermediate candidate failures and terminal
stream failures; no client identity, prompt or credentials are recorded.
Old rows remain readable with NULL trace columns.

The local read-only status --scoring output includes request_metrics with
traced_requests, requests_with_fallback, fallback_rate, and
first_attempt_success_rate_nonstream. These are actual downstream outcomes,
not counterfactual Shadow predictions. A stream that only emitted HTTP
headers does not count as a completed first-attempt success; cancelled SSE
streams are not yet a complete outcome dataset. An old, unmigrated SQLite
database returns null request_metrics until the telemetry writer migrates
it. All existing aggregate scoring fields remain compatible.

Request-level metrics deduplicate by random request ID: a request with two
failed attempts followed by a successful third attempt counts as one traced
request and one request with fallback, not three separate requests. The
nonstream first-attempt success rate includes each nonstream request once
in its denominator (even when its first attempt failed); only a successful
index-one attempt counts in the numerator. In-window requests without a
known terminal outcome may still appear in traced requests, so these metrics
must not be confused with completed-request success rates.
These request-level Auto metrics include only Chat rows, not Embeddings or
other endpoints. Pre-migration SQLite tables missing an endpoint column
return null request_metrics instead of treating mixed old observations as
qualified Chat traffic. The writer upgrades legacy schemas on startup.

## Shadow comparison persistence

When both Shadow and SQLite telemetry are enabled, the bounded telemetry
writer also records one shadow_events row per evaluated request. It stores
the internal request_id, baseline and suggested provider/model labels,
availability and top-choice disagreement, with seven-day retention and
bounded recovery on full disks. The read-only scoring response exposes
shadow_metrics for comparison coverage and disagreements; the shadow route
is **never executed** and is not credited with a hypothetical success.
Operators may correlate shadow_events with request_events by request_id
offline. Old databases without the new table return null shadow_metrics.

## Issue #129: Go Auto context-window request controls

Both POST /v1/auto and Chat Completions with model auto now accept optional
integer min_context and max_context fields (token-window bounds). For example,
min_context: 8192 selects only models with a known window of at least 8192
tokens, and max_context: 16384 excludes larger-window models. A positive
max_context smaller than the effective min_context is rejected locally with
400 invalid_auto_context. Strings, null, booleans and fractional values are
also rejected before any upstream request. Zero/negative bounds are treated
as unspecified, matching the Go selector's nonpositive-bound convention.

These fields are **selection-only**: the gateway strips both before forwarding
to the upstream, including retry and SSE-to-JSON paths. The Auto route cache
and Decider cache include both bounds in their keys to prevent a permissive
ranking from leaking into a stricter request. A candidate without a known
window cannot satisfy an explicit context bound; operators can declare
max_context_tokens per candidate or opt into the local model registry and
capability overrides. The Go default min_context=512 is applied only when
use_model_registry is true. Legacy Rust installations with the registry
disabled retain their prior unrestricted default, avoiding silent outages
when models have no declared context length. The request context estimate
and 20-percent safety margin remain an independent hard upper bound.

## Issue #129: operator-approved telemetry routing (opt-in)

Historical SQLite scoring is advisory by default and never changes routes.
Operators can explicitly opt into it with this Auto config:

    auto:
      enabled: true
      telemetry_ranking:
        enabled: true
        approved: true
        window_hours: 24
        max_snapshot_age_secs: 300

The SQLite writer must also be enabled with GUVNA_TELEMETRY_SQLITE=1.
Active external LLM Decider ranking and telemetry ranking cannot be enabled
together; Decider Shadow-only comparisons may run alongside telemetry.
All configured candidates must have at least 20 genuine observed
success/provider-failure samples in the selected 1h/6h/24h/7d window and
a verified terminal observation no older than max_snapshot_age_secs.
Caller errors, rate limits, unverified SSE starts, stale, missing, corrupt,
locked or incomplete databases never qualify. If any candidate lacks
qualified fresh data, the existing deterministic order is preserved.
A streaming response row marked `success` without a matching SSE terminal
cannot refresh the approved ranking: response headers are not proof of
completion. Only a matching confirmed stream terminal or a real nonstream
success/provider failure can satisfy the freshness gate.
Ranking applies only after all request context/capability filters and is
suppressed when any provider has a live health penalty. It never adds
candidates or bypasses the unified fallback loop.
Both local scoring and Auto's read-only telemetry ranking aggregate confirmed
stream outcomes only for the at-most-32 candidates selected by the bounded
hourly summary; unrelated historical model labels do not enlarge the
in-memory stream outcome map or influence the candidate scores.

A background task refreshes the read-only snapshot every 30 seconds.
Request processing never queries SQLite; snapshots expire after 75 seconds
if refresh stops. The local status command reports only approval and active
state, never sensitive model labels. Turn off telemetry_ranking.enabled
to restore static Auto ordering without modifying the telemetry database.


### Speculative Decider backpressure

At most one background selector task may be in flight across SSE cache
refresh and Shadow comparison. Excess Shadow requests are recorded as
unavailable (not as a disagreement or an upstream failure), while their
real requests continue using the existing deterministic candidate order.
SSE refresh is skipped when the slot is occupied; the next request may
retry. This prevents speculative tasks from growing without bound under
concurrency or a stalled selector. Direct non-streaming Decider requests
continue to use the existing singleflight cache and bounded timeout.

### Go-compatible operator default for min_context

GUVNA_AUTO_MIN_CONTEXT sets a service-level minimum context window at
configuration load (for example 262144), matching the Go operator knob.
Alternatively set auto.default_min_context: 262144 in config.yaml; explicit
YAML wins over the environment. The value must be 1..16777216; malformed,
zero, negative or out-of-range environment values are ignored. A positive
request min_context always overrides the service default, and max_context
smaller than the effective minimum is rejected before dispatch. A request
min_context of zero/negative means use the service default. Without either
override, the existing Rust default remains unrestricted when the registry
is disabled, or 512 with the registry enabled. Changes take effect on the
next configuration load, not in-flight requests. No environment values are
forwarded to the upstream.

### Go-compatible fallback for the operator context floor

When the operator's default min_context excludes every Auto candidate under
the normal selection, the gateway may retry local candidate selection with
that default floor removed *if at least one candidate still passes all hard
filters*. This is not a network retry, and the caller's request is not
modified. Explicit positive min_context or max_context never permit this
relaxation. Each candidate must independently pass capability, model catalog,
input size and estimated context-fit requirements: an ineligible image/tool
model or oversized prompt remains excluded. A different ineligible model does
not prevent a safe, eligible candidate from being used. The effective relaxed
bounds are also used in the Decider/Shadow cache key. Disable Auto or specify
a positive request min_context to enforce a strict window requirement.


### Bounded real-outcome shadow correlation

Local guvna status --scoring includes shadow_metrics.observed_actual_sample:
a maximum of 256 recent shadow rows correlated with up to 512 real terminal
request rows and 512 SSE terminal observations. Multiple attempts for one
request ID are resolved by the highest attempt index, not an ambiguous
one-second timestamp or asynchronous event insertion order. Only a matching
SSE terminal success with matching request ID, attempt, provider and model
counts as a
completed stream; response headers alone do not. A stream request rejected
before headers retains its explicit request-row failure even without a terminal
callback; an unverified success still remains pending.
Only Chat request rows are eligible for actual Shadow correlation. Other
endpoints (including Embeddings) cannot evict Chat traces from the bounded
sample or impersonate a Chat outcome. Legacy endpoint-less request tables
return a null optional outcome sample rather than an unverified match.
Completed, failed, cancelled, pending and unlinked actual requests, actual
fallbacks and first-choice successes are separated. A proposed model
matching the actual final route is NOT a counterfactual success claim:
the shadow proposal is never executed. Legacy SQLite databases lacking
request ID columns report null for this optional sample. Timestamp-indexed
bounded reads preserve low-memory local CLI behavior.


### Local CLI status JSON fallback

The local status socket returns structured JSON. When it is absent but the
loopback health probe succeeds, guvna status --json now also prints a JSON
object with status, source=local_health_probe, loopback host and port rather
than silently returning plain text. Status --window is valid only together
with --scoring; it is never silently ignored. No remote admin endpoint is
introduced.

### Issue #129: deliberate Go configuration compatibility boundary

The Rust configuration remains authoritative. Issue #129 aims for **behavioral
compatibility of core routing controls**, not a byte-for-byte clone of every
Go-only configuration field. Existing Rust Auto V1 defaults and named-chain
behavior are preserved. Supported Go-derived controls include context bounds,
capability filters, configured candidate order, safe per-step parameter
merging, and explicit opt-in for Decider/Telemetry/Shadow.

Unknown fields and unsupported provider/Auto controls must fail validation
clearly rather than silently pretending to work. Compatibility changes must
never relax capability checks, override an explicit caller bound, expose
secrets, create another request executor, or add remote Web/TCP management.
Go-only optional fields that do not affect these contracts may remain
unsupported; document actual differences instead of adding compatibility
state machines. These scope changes do not waive Linux tests or resource
gates.
