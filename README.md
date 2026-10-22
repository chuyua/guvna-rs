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

Not yet shipped: SSE, Responses translation, Auto, CLI management, persistent
chain mutation, telemetry, all Go protocol validation rules, production parity.
Streaming requests and Responses return 501 instead of false success.

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

## Performance and fault-injection quality gates

[Linux resource and pressure acceptance criteria](docs/QUALITY_GATES.md) are
checked on each PR against the real Release binary. CI measures idle RSS,
idle CPU, peak RSS across two load waves, post-load memory recovery,
thread/file-descriptor leaks, per-wave P95/P99 and injected upstream
failover. No live provider key or production server is needed.

The initial Linux x86_64 **idle RSS hard limit is 32 MiB**, with a 16 MiB
optimization goal. These are budgets to verify, not performance claims.
