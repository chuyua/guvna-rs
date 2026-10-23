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
- Bearer auth, named fallback chains, direct provider-prefixed models.
- Credential rotation, failure-class isolation and generation fencing.
- One candidate walk shared by Chat and Embeddings.
- Bounded 32 MiB upstream body buffering, no credential-bearing redirects.

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

Unsupported reasoning, image, audio, stateful continuation, and other
advanced Responses modes remain explicitly unsupported. The gateway must not
be treated as full OpenAI Responses parity or a production drop-in replacement.

Not yet shipped: advanced Auto learning/cache, remote management,
complete Go protocol parity and full multi-modal streaming.

POST /v1/chat/completions, /v1/embeddings and /v1/responses authenticate
request headers **before** buffering the request body. The existing 32 MiB
body limit is enforced for authorized requests, including chunked requests;
invalid credentials are rejected without consuming a slow or unbounded body.
Authorized requests acquire a shared admission permit before reading the
request body, preventing slow uploads from evading the global 128-request
limit. Permits remain held through dispatch and streamed response delivery.

## Basic Auto routing and local diagnostics

An opt-in Auto V1 selects model candidates using configured priority
and a bounded encoded-input-length filter. POST /v1/auto or Chat model
auto delegates to existing failover and circuit logic; Auto is off by default.

Set GUVNA_LOG_EVENTS=1 for sparse local stderr warning lines with no prompt,
provider URL, client IP or API key. No remote management port, telemetry
database or external API is added. See docs/AUTO_V1.md.

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
