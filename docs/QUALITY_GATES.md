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

## Streaming fragmentation and worst-case parser gates

The Rust unit suite checks a 128 KiB event delivered one byte at a time,
two 600 KiB events coalesced in a single input chunk, single-frame oversize
rejection, forbidden trailing bytes after [DONE], zero intermediate frame
allocations on the ordinary single-frame path, and out-of-order completion
across the full 64-choice Chat SSE range. This guards against quadratic
rescanning, unnecessary heap churn, and false terminal success.

The real Linux release-binary gate also fragments every 13th Responses SSE
request across several network writes, including the terminal marker, and
still requires every event sequence and response to complete correctly.
These deterministic checks supplement the existing 200-request/16-client
SSE wave, but do not replace dedicated prolonged soak or fuzzing.
## Opt-in breaker pressure and routing gate

A second real Linux --release instance runs with circuit breaking enabled on
the injected-fault provider. Out of 64 nonconcurrent requests, the first
three upstream failures must open the circuit; 61 later calls must bypass that
provider entirely, and all 64 must still succeed via the healthy fallback.
Every telemetry counter is reconciled to actual mock requests and RSS,
FD/thread resource limits are checked. The normal default-disabled baseline
remains unchanged for compatibility and prior load comparisons.
## Offline chain management on the Release binary

The Linux resource gate runs real CLI commands before serving: static list,
local add/replace/delete, single rolling backup, rejecting corrupted YAML,
explicit recovery with byte-for-byte preservation of the damaged file and
mode-0600 persisted files. No client/provider API keys are needed. The Rust
integration suite also races eight independent CLI writers and verifies that
all named chains survive, with no abandoned temporary files. A running
daemon intentionally requires a restart to load newly committed chains.

## Runtime chain startup and CLI parity (G11 follow-up)

The actual Linux release binary must reject bad chains.yaml at startup,
using the same rejection rules as the CLI: shadowing static routes,
duplicate runtime names, unsupported versions, >1 MiB files, and
symlinked runtime files. Validation cannot rewrite the rejected input.
Bounded O_NOFOLLOW reads prevent symlink following and excessive reads;
atomic replace keeps runtime reader snapshots complete.

## Auto and local logging release gate

An independent Linux --release gateway instance runs with Auto enabled,
64 mixed primary-fault injections and healthy fallback requests through
/v1/auto, while the original 4,000+200 baseline remains unchanged. The
gate requires exactly 64 primary attempts and 64 successful fallback
completions, reconciles metrics, and checks bounded RSS/FD/thread counts.
Local warning JSON-line stderr events are restricted to fixed metadata
fields, contain no API key values and are rate limited to ten lines.

## SSE-only aggregation capability + failure classification

SSE->JSON synthesis is bounded to n=1..64, while native Chat JSON candidates
may support larger n. In the shared candidate loop, unsupported SSE-only n
skips the provider before acquiring any key or issuing HTTP traffic; a
JSON-capable fallback is still attempted. When all candidates are SSE-only,
the gateway returns HTTP 422 aggregate_n_unsupported instead of a misleading
502 upstream failure. Regression covers n=0, 65, 512, Auto, JSON fallback,
and the accepted 64-choice upper boundary. Invalid requests must not consume
upstream attempts.

With GUVNA_LOG_EVENTS=1, local stderr aggregation failures are subdivided
into invalid_sse, idle_timeout, transport_failure and resource_limit; each
fixed event name uses independent sampling so rare classes are not lost in
a high-volume success stream. No user information or provider identifiers are
logged, and no new remote management interface is introduced.

In addition to the Rust integration cases, the real Linux Release quality
gate now sends a direct SSE-only n=65 request and requires HTTP 422 with
zero upstream HTTP calls. The sampled local event gate is deliberately
strict: deterministic failure injection must produce exactly eight samples
in each of aggregation_invalid_sse, aggregation_failed, and
aggregation_completed, with independent sequence numbers starting at
one. The gate checks approved log fields and secret redaction; it does not
merely increase the old generic log count ceiling.

## SSE-only provider: bounded JSON synthesis and local logging

The Linux --release gate runs 64 JSON Chat requests against a fixture that
rejects every upstream request unless stream:true is sent. It returns
valid fragmented SSE for 54 requests and premature DONE for 10 requests.
Guvna must return normal JSON for all 64, aggregate 54 without a fallback,
and fail over for exactly 10 invalid streams. The gate reconciles upstream
calls, success/fallback/invalid/aggregate counters and RSS/FD/thread limits,
and audits sampled local diagnostic logs for credentials and prompt leakage.

SSE-to-JSON completeness regression also verifies that optional refusal
and reasoning/reasoning_content deltas survive aggregation, mixed prompt/body data
never appears in local stderr diagnostics, unsupported audio deltas trigger
bounded candidate failover instead of success, and requests for token
logprobs never return a synthesized JSON response missing the requested
log-probabilities. A named-tool mismatch is rejected as soon as its
first invalid tool delta arrives rather than waiting for an idle timeout.
The original 64-request load count and fallback reconciliation remain
unchanged.

manual-linux-build.yml is explicitly dispatched from main and uploads a
real Linux executable with SHA256, ELF/glibc metadata and the same quality
report. CI is necessary but not sufficient to prove real-provider behavior.

## Provider rate-limit recovery on the actual Release binary

An independent Linux Release gateway faces an upstream forced to return
HTTP 429 and Retry-After 3600. After quarantining the only affected
credential, 32 concurrent native Chat and Auto requests must all return
bounded 429 hints without a second upstream call. A different healthy
fallback still returns valid 200 results. Exact metrics, RSS, FDs, threads
and privacy-safe sampled local logs are checked. The original multiwave
throughput and SSE aggregation gates remain unchanged; this PR does not
initiate the manual GitHub Release publishing workflow.

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
