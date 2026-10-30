# guvna-rs

Rust successor to [guvna-pro](https://github.com/chuyua/guvna-pro), built for
**single-user, low-memory VPS** operation without inheriting the Go project's
duplicated routing state machines.

**Work in progress — NOT yet a drop-in production replacement.**

## First executable milestone (M1)

- Go-style `config.yaml` parsing and validated import and offline atomic management of `chains.yaml`.
- Explicit CLI `-config` file selection; no hidden home-directory override.
- `/healthz`, `/v1/models`, non-streaming
  `/v1/chat/completions`, `/v1/embeddings`.
- Bearer auth, named fallback chains, direct provider-prefixed models. The
  matched prefix is gateway-local and is stripped before forwarding; nested
  upstream namespaces such as `openai/gpt-4o` remain intact. Providers whose
  real upstream IDs include the gateway namespace (e.g. NVIDIA NIM) can set
  `namespace_on_wire: true` under that provider to forward the **full** client
  model ID. The default is `false`; the flag does not change provider matching,
  named-chain precedence or other providers. Use matching provider prefixes
  (e.g. `nvidia`) and send the upstream's real model ID (e.g.
  `nvidia/nemotron-3-embed-1b`).
- Optional `default_chain: <configured-chain-name>` routes otherwise unknown
  model names to a configured named chain. Exact chain and provider-prefixed
  names take precedence; invalid default chains fail startup validation.
  Without this setting, unknown models return 404. Auto's explicit enable
  and eligibility rules remain unchanged.
- Credential rotation, failure-class isolation and generation fencing.
- One candidate walk shared by Chat and Embeddings.
- Bounded 8 MiB inbound request bodies and 32 MiB upstream response buffering;
  no credential-bearing redirects.

Chat Completions SSE now supports bounded incremental framing, finish-reason
and [DONE] checks, and fallback before the first valid event. After the first
event, an upstream failure sends an explicit SSE error (without [DONE]); the
gateway cannot replay a partially delivered response on another provider.
Disconnecting the client drops the upstream stream. The first event must
arrive within 15 seconds; a stream event is limited to 1 MiB.

Responses now supports bounded non-streaming text and function-call translation,
and incremental text/function-call SSE translation via POST /v1/responses
with stream:true. The streaming bridge emits response lifecycle, text delta,
function-call argument delta/done and terminal events. It preserves stateless
metadata and honors stop versus length finish reasons. Invalid or truncated
upstream streams fail closed with response.failed or an error event; there is
no silent provider replay after streaming begins.

Responses streaming limits: 512 KiB combined generated text and function
arguments, 256 KiB per function's arguments, 32 parallel function calls,
1 MiB maximum framed SSE chunk, and 15 seconds of upstream idle time.
Responses now also maps text.format to the equivalent Chat Completions
response_format for plain text, JSON-object and strict JSON-schema requests,
including streaming. JSON-schema names, field types, and unsupported options
are checked before contacting providers, and nested schemas retain integer
precision. The gateway validates parseable JSON before reporting completion
(non-streaming: failover; streaming: response.failed, never replay after data
has been sent). JSON-schema adherence beyond JSON syntax remains the
responsibility of the upstream model/provider.

Responses user-image URL and base64 data-URI input are translated to Chat
vision parts, preserving image order, text, and detail. User input_audio
(mp3/wav base64) is also forwarded to Chat providers. File-ID images,
image-generation tools, audio output and streaming, advanced reasoning controls, stateful
continuation, and other advanced Responses modes remain unsupported.
The supported reasoning.effort values are translated to Chat reasoning_effort;
reasoning summaries, context modes and other reasoning options remain unsupported.
Responses text.verbosity is mapped to the equivalent Chat verbosity field.
Request-level service_tier, safety_identifier and prompt_cache_key are
validated and forwarded to Chat providers without inventing response metadata.
The gateway is not a full OpenAI Responses implementation or a production
drop-in replacement.

Not yet shipped: learned Auto scoring, complete Go protocol parity and full multi-modal streaming.
The current Auto cache stores only route metadata (128 entries, 30s TTL); it does not learn from or retain prompts.

POST /v1/chat/completions, /v1/embeddings and /v1/responses authenticate
request headers **before** buffering the request body. The 8 MiB
body limit is enforced for authorized requests, including chunked requests;
invalid credentials are rejected without consuming a slow or unbounded body.
Authorized requests acquire a shared admission permit before reading the
request body, preventing slow uploads from evading the global 128-request
limit. A separate eight-slot payload-memory gate bounds concurrent body reads,
JSON parsing and per-candidate serialization without reducing the number of
already-started streams. Waiting for a payload slot is capped at one second,
and authorized body reads at 30 seconds; payload slots release after dispatch,
while admission permits remain held through streamed response delivery.

## Basic Auto routing and local diagnostics

## Exhausted upstream rate-limit recovery

If every eligible candidate has all its usable keys isolated by upstream
HTTP 429, the gateway returns HTTP 429 with a bounded downstream
Retry-After delta rather than a generic 502. It waits on no timer
and initiates no more provider calls while the keys cool down;
configured fallback providers with healthy keys still work immediately.
Mixed upstream 5xx, connection, invalid-result, auth and circuit failures
retain the conservative failover/error behavior. Fixed-cardinality
guvna_rate_limit_exhaustions_total and the opt-in rate_limit_exhausted
local stderr event expose this condition without prompts, credentials
or provider identifiers. No remote management endpoint. Local SQLite telemetry is separately opt-in.

An opt-in Auto route selects model candidates using configured priority,
message byte limits, a full-request context estimate with requested/default output
allowance and a 20% safety reserve,
and an exact model-catalog filter when a provider declares models. POST /v1/auto
or Chat model auto delegates to existing failover and circuit logic; Auto is off by default.
With auto.health_aware: true, in-memory failure hints can temporarily demote
unhealthy providers without replacing configured priorities or fallback.

Set GUVNA_LOG_EVENTS=1 for sparse local stderr JSON-lines, including
sampled SSE aggregation successes and failures, without prompt, provider URL,
client IP or API key. Logs can be collected by the host journal for soak
testing. guvna status reads runtime counters from a read-only Unix domain
socket under the data directory (directory mode 0700, socket mode 0600).
It exposes no mutating command or new TCP port; HTTP health probing remains
a fallback only when the socket is absent.
For optional local SQLite metadata, set GUVNA_TELEMETRY_SQLITE=1 and see
docs/OBSERVABILITY.md. See docs/AUTO_V1.md for routing.

## Non-streaming whole-chain timeout

The gateway now has one timeout budget for a non-streaming request,
including all upstream keys, candidate failover and SSE-to-JSON aggregation.
Configure timeout.walk: 100s (default; supported 100ms to 1h).
Exhaustion returns HTTP 504 upstream_walk_timeout; no retry resets it.
Live client SSE with stream:true is exempt from this total deadline.

Set GUVNA_LOG_REQUESTS=1 for local stderr JSON lines containing an anonymous
request ID, elapsed time, HTTP status and completion phase.
See docs/WALK_DEADLINE.md for privacy and operational details.

## SSE-only providers: non-streaming client compatibility

A provider can be configured with stream_only: true (disabled by default).
For non-streaming Chat requests to that provider, the shared dispatcher sends
stream:true upstream, validates the complete Chat SSE stream, and returns one
standard application/json Chat Completion. A caller's stream:true request
continues using incremental Chat SSE passthrough.

The synthesis is bounded: at most 4 parallel aggregations, 8 MiB of received
SSE data per attempt, 2 MiB of generated text and tool arguments, a 60-second
upstream idle limit and a 100-second aggregation lifetime (capped sooner by
the configured total non-streaming walk deadline). Invalid, truncated
or stalled streams fail closed and use the existing next-chain candidate
before any downstream JSON response is emitted. Results must pass the same
upstream Chat contract checks as ordinary JSON Chat completions.

For text-compatible upstream extensions, the synthesized JSON preserves
refusal, reasoning and reasoning_content fields within the existing shared 2 MiB
output limit. Unsupported audio chunks and invalid field types fail
closed and use the existing candidate fallback path; a mismatched named
tool call is rejected at the first invalid SSE delta. The adapter does
not currently synthesize token logprobs, so a non-streaming request for
A non-streaming client requesting more than 64 choices (or an invalid n)
never consumes SSE-only provider calls: the gateway skips that candidate and
continues to a native-JSON provider in the same chain or Auto selection.
With no eligible JSON provider, the gateway returns HTTP 422
aggregate_n_unsupported instead of treating the unsupported request as a
provider outage. The boundary n=64 still aggregates normally.

logprobs bypasses SSE-only providers in favor of a compatible native-JSON
candidate, whose returned logprobs must also be validated. This is a
documented capability boundary, not silent data loss.

Aggregation warnings now distinguish invalid SSE framing/content,
60-second SSE idle timeout, transport interruption and resource ceilings.
Log keys have constant cardinality, independent sampling and contain no
prompt, API keys, model names or provider addresses. These diagnostics are
local stderr lines only; no new management or logging HTTP endpoint is added.

Set GUVNA_LOG_EVENTS=1 for sampled local stderr JSON-lines containing
aggregation_completed or aggregation_failed with no user content, secret,
provider name or client address. Existing read-only authenticated metrics
also count successes and saturation. No remote management or telemetry DB.

## Manual Linux executable build

GitHub Actions -> Manual Linux x86_64 binary -> Run workflow (main) builds
an actual Linux x86_64 executable after fmt, strict Clippy and Rust tests,
then runs the Release binary through the adversarial resource gate.
The downloadable artifact contains the ELF file guvna-linux-x86_64,
its SHA256 checksum, build metadata and the quality report. It is not a
GitHub auto-generated source code archive.

Verify the binary with sha256sum -c guvna-linux-x86_64.sha256 and check
binary-info.txt for glibc dependencies before using a Linux host.
This workflow does not change deployed services or use remote management.

### Optional GitHub Release (manual-only)

On **Actions → Manual Linux x86_64 binary → Run workflow**, select `main`.
Neither a push nor a tag publishes a GitHub Release automatically.

- Both checkboxes off (default): run all Linux gates and retain the binary,
  SHA256, binary information and resource report as Actions artifacts only.
- **Publish a GitHub Release** on, **Stable release** off: after all gates
  pass, create a uniquely tagged GitHub **pre-release** with those four assets.
- Both checkboxes on, with `PUBLISH_STABLE` in **Stable confirmation**:
  release `v<package.version>` as a stable GitHub Release, only if that tag
  does not already exist. This is an explicit manual choice, never the default.

Every mode uses Ubuntu 24.04 and publishes Linux x86_64 only. A failure in
tests, Release compilation, binary verification or the resource gate prevents
publication. The workflow does not deploy or restart the running service.

## Local runtime chain management (offline)

Manage additional routing chains with the built-in local CLI:

    guvna chains list -config config.yaml -data /var/lib/guvna
    guvna chains set -file my-chain.yaml -config config.yaml -data /var/lib/guvna
    guvna chains delete -name my-chain -config config.yaml -data /var/lib/guvna
    guvna chains recover -config config.yaml -data /var/lib/guvna

Writes are atomic and serialized across processes. The CLI backs up the
previous good version before edits and preserves damaged files during
explicit recovery. Static chains remain read-only. Running servers do not
reload routes: validate and restart manually. Management needs filesystem
access but no remote management port or API key.
See [the chain management contract](docs/CHAIN_MANAGEMENT.md).

**Safety upgrade:** daemon startup now uses exactly the same validated,
size-limited runtime-chain loader as the CLI. Duplicate names and attempted
static-route shadowing fail closed rather than being silently ignored. On
Linux, O_NOFOLLOW plus checks of the opened inode prevent a symlink swap
between inspection and read. The release-binary CI verifies these cases,
as well as concurrent atomic writers and daemon readers.


## Gateway architecture

The public router and shared state live in src/gateway.rs. Protocol
translation, the one shared upstream candidate walk, Chat SSE lifecycle
and result validation have separate private modules. This is a mechanical
file extraction; HTTP behavior, retry/fallback and existing streaming
adapters remain unchanged. See docs/GATEWAY_MODULES.md for the exact
boundaries and invariants.

## SSE-only provider aggregation safety

SSE-only upstream Chat providers can supply ordinary JSON completions via
the existing bounded Chat stream aggregator. It enforces 60-second idle
timeout, a separate 100-second overall aggregation limit and a 2 MiB combined
output/metadata budget. Ordinary non-streaming JSON reads still use their
90-second per-attempt upstream body-read deadline. The configurable
non-streaming walk deadline can end aggregation sooner. Invalid or incomplete SSE fails over before
returning JSON; no partially synthesized result is returned as success.
Local opt-in stderr events report fixed aggregation failure categories.
See docs/QUALITY_GATES.md for real Linux stress checks.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo run -- serve -config config.yaml
```

Credentials are supplied through `API_KEYS`, `ADMIN_KEY` and provider-named
environment variables. `GUVNA_BIND_ADDR` defaults to `127.0.0.1` rather than
exposing an unaudited development gateway publicly.

## Audit inheritance

See [Go audit migration gates](docs/audit/MIGRATION_GATES.md) and the
[source provenance](docs/audit/SOURCE.md). Reported issues are historical
findings, not proof that current Go main still contains those defects.
All P0 gates must have deterministic regression tests before release.

## Outage resilience and bounded upstream suppression

For unreliable model providers, opt into a per-provider circuit breaker with
a configurable failure threshold, exponential cooldown, and a single
half-open recovery probe. Once a provider repeatedly fails, the gateway skips
it and uses a configured fallback without wasting another outbound HTTP call.
Client 4xx errors and downstream cancellation do not open the breaker. This
feature is disabled by default to preserve existing routing semantics.
See [circuit breaker configuration](docs/CIRCUIT_BREAKER.md).
## Telemetry and runtime diagnosis

The authenticated GET /metrics endpoint exposes a fixed set of Prometheus
counters and the active admission-permit gauge. It reports actual upstream
attempts, credential quarantine, candidate fallback, malformed upstream
responses, bounded admission rejection, stream completion, error and client
disconnects without including model/key/IP labels or request content. Counters
use atomics and are reset on restart, with no telemetry database or background
exporter. The Linux release gate reconciles these counters with mock provider
call counts and verifies unauthorized access is denied.

See [observability and metric semantics](docs/OBSERVABILITY.md).
## Performance and fault-injection quality gates

[Linux resource and pressure acceptance criteria](docs/QUALITY_GATES.md) are
checked on each PR against the real Release binary. CI measures idle RSS,
idle CPU, peak RSS across two load waves, post-load memory recovery,
thread/file-descriptor leaks, per-wave P95/P99 and injected upstream
failover. No live provider key or production server is needed.

The Linux x86_64 **idle RSS hard limit is 16 MiB**, with a 12 MiB
optimization goal. These are budgets to verify, not performance claims.
