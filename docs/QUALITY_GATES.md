# Quality gates for a single-user, low-memory Rust gateway

These are **acceptance limits**, not measured claims. Only Linux x86_64
`--release` measurements are authoritative. The Go gateway remains the
production baseline until protocol parity and resource gates pass.

## Mandatory PR checks

1. Formatting and zero-warning Clippy; all unit and integration tests.
2. Adversarial integration tests with REAL local HTTP sockets, including
   independent failing upstreams, malformed upstream success, authorization,
   integer precision, and concurrency-key fencing. Deterministic random seeds
   must be printed with failures.
3. Real binary `quality_gate.py`: idle startup RSS, idle CPU, two successive
   load waves, post-load idle RSS, HTTP status/error counters, process liveness,
   file descriptor/thread count. Upstreams are isolated local stubs, no real
   provider accounts or network requests.
4. Linux CI uses `cargo build --release --locked`, runs hard gates, and
   saves JSON results as an artifact even on failure.
5. Stress jobs MUST NOT be marked `continue-on-error` and no PR merge on
   red/unknown checks. Flakiness is investigated; no 'just retry until green'.

## Initial limits (Linux x86_64 Release)

| Measure | Block merge when | Long-term goal |
|---|---:|---:|
| Idle RSS after warmup | >16 MiB | <=12 MiB |
| After two load waves, RSS | >16 MiB | <=12 MiB |
| Post-wave2 idle RSS increase from post-wave1 | >6 MiB | <=2 MiB |
| Peak RSS across both 4,000-request waves | >48 MiB | <=24 MiB |
| Idle CPU (single-core percentage) | >1% averaged over 10 s | ~0% |
| Initial startup/health response | >2 s | <=1 s |
| No-leak steady state | FD or thread growth >8 from post-wave1 to post-wave2 | 0 growth |
| Healthy throughput test | Any non-200 among 2 waves | 100% successful |
| Local test P95 / P99 response time | >2000 ms / >5000 ms | <200 ms / <500 ms |
| Chaos fallback with failing primary | Any failed request within controlled load | 100% recovered |
| Open handles/process | After test the child process is terminated | No orphan |

**Important:** A fixed RSS threshold is intentionally aggressive. If a
legitimate measured baseline exceeds it, CI must remain red until there
is a reviewed, documented technical decision to change the limit. Never
quietly relax the threshold in code. Measure PSS separately where
available, but RSS is the hard portable Linux criterion.

The PR test uses small real payloads and 64-client concurrency for
deterministic quality control. It is NOT a substitute for 1,000+
concurrent clients, large-body fuzzing, prolonged soak, or live upstream
compatibility. Those are separate extended/nightly gates and must report
peak memory, P95/P99, error rate and throughput with the machine size.

## Planned broader gates

- Deterministic random fault matrix (429/5xx/invalid JSON/stalled response/
  EOF-mid-body/partial SSE/cancellation) with a bounded retry budget.
- Rust/Go golden-fixture tests for Chat, Embeddings, Responses, SSE and
  tools; no Go vs Rust 'parity success' until the endpoint is implemented.
- 10,000-request burst, 30-minute soak, and 1-hour leak trajectory on
  dedicated Linux runners. Start at 32/64/128/256 concurrency and track
  success, P95/P99, peak RSS, file descriptors, CPU and allocator churn.
- Malformed-input fuzz (cargo-fuzz / libFuzzer), and property-based model
  routing/keypool invariants; these are gating only after stable baselines.

Fail-closed policy: a missing sensor is a failed measurement, never zero.
