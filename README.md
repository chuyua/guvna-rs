# guvna-rs

Rust successor to [guvna-pro](https://github.com/chuyua/guvna-pro), built for
**single-user, low-memory VPS** operation without inheriting the Go project's
duplicated routing state machines.

**Work in progress — NOT yet a drop-in production replacement.**

## First executable milestone (M1)

- Go-style `config.yaml` parsing and read-only import of `chains.yaml`.
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
Unsupported reasoning, image, audio, stateful continuation, and other
advanced Responses modes remain explicitly unsupported. The gateway must not
be treated as full OpenAI Responses parity or a production drop-in replacement.

Not yet shipped: Auto, CLI management, persistent chain mutation, telemetry,
complete Go protocol parity and full multi-modal streaming.

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
