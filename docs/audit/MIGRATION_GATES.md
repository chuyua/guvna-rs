# Go audit inheritance — Rust migration gates

> Status: **historical audit transferred as engineering requirements**, not a claim
> that each defect is still present on Go main. The uploaded audit studied
> `chuyua/guvna-pro@f90d7d2`; new Rust work compares with the independently
> checked-out Go baseline `a4672c2`. Re-verify before treating historical
> findings as active bugs.

Source artifact: `guvna-audit-report.zip` (provided by the project owner).
SHA-256: `6b87832a141a260591f8a442524d7ea333caaf6d5ce5e2815249d0d586cf3e56`.
Original four auditor tracks: protocol (22), auto (19), serving (16),
platform (26) = **83 findings**; global adjudication reported **11 P0,
23 P1, 21 P2, 28 P3**. Some track-specific priorities differ from the lead's
global P0 list, which is authoritative here.

## Blocking invariants / regression cases

| Audit gate | Contract Rust must enforce | Verification before parity |
|---|---|---|
| G01 · Config precedence | Explicit `--config/-config` wins; log the file really read; never silently load `~/.guvna/config.yaml`. | Temp configs on distinct ports |
| G02 · Telemetry time windows | Store microsecond-precision integer epochs; define half-open `[since,until)`; don't filter nanosecond timestamps with SQLite `julianday()`. | Sub-ms boundary table + repeated tests |
| G03 · Auto context | Unknown model metadata cannot cause a fabricated `context_length_exceeded`; do not ship a registry with zero coverage. | Shipped config + 8192 completion tokens |
| G04 · Reasoning mapping | `reasoning` and `reasoning_content` must both survive Chat→Responses translation. | Stream/non-stream fixtures |
| G05 · Truncated tool calls | Incomplete JSON arguments must never have `completed` status. | Truncated tool-call fixture |
| G06 · Event output ordering | Responses events refer to monotonically ordered `output_index`; terminal events never duplicate. | Interleaved tool fixture + double-Finish |
| G07 · JSON number integrity | Rewriting `model`/`stream` must not round large integer params. | 12345678901234567890 roundtrip |
| G08 · Failure attribution | One outcome classifier powers retry, key quarantine and model breaker; caller errors/transport faults cannot arbitrarily quarantine credentials; unreachable state branches forbidden. | Table-driven status × channel cases |
| G09 · Hot-path storage | Request routing never synchronously queries large SQLite telemetry windows; failures must be observable. | Request-path instrumentation and stress |
| G10 · Credential cool-off | Apply the right provider-configured auth vs 429 window. Concurrent late failures cannot exponentially extend cool-off (lease generation fencing). | Fake clock, concurrent generation test |
| G11 · Persistent chain recovery | Bad chains file does not disappear or permanently brick edits; recovery path must exist and be exercised through public command path. | Corrupt-file recovery via CLI |

Implementation note (G11): the CLI now supports offline version-1 runtime
chain add/update/delete, validation and explicit backup-based recovery. The
unit and Linux release-binary tests exercise malformed-file preservation,
atomic commits and concurrent writers. Live daemon hot reload and online
management are separate, still-unimplemented future milestones.

## Architecture rules derived from the remaining 72 findings

1. **One candidate walk** for Chat, Embeddings and future Responses/Auto.
   Specialized adapters may validate data but cannot clone retry/failover loops.
2. **One streaming parser and one finish-reason vocabulary**; transport validates
   one event once before broadcasting to output renderers.
3. **One outcome classifier, separate policy decisions** for credentials and
   models. No hidden health penalties for 400/404/422 without evidence.
4. **No speculative shadow machinery in the serving hot path.** Auto selection
   is explicitly postponed until correctness and a measurable baseline exist.
5. **Explicitly unsupported endpoints** return 501 with a useful error, never
   fabricated retrieval/cancellation success.
6. **Do not mix admin and public API routes.** Management stays host-local;
   credentials are environment-only and never echoed to logs.
7. **Small config surface**; reject unsupported options rather than silently
   ignoring safety-critical settings. Preserve the Go YAML shape on supported
   features, and document deviations.
8. **Behavior tests, not private-function tests.** The critical acceptance
   suite must exercise handlers and real failure paths with stub upstreams.

## Milestones (implementation status in code and test output, never assumed)

- M0: audit requirements + Cargo project + CI.
- M1: explicit config loading, chain routing, Bearer auth, non-streaming Chat
  and Embeddings via ONE candidate walk; local fake upstream tests.
- M2: KeyPool health/quarantine/retry budgets, atomic chains mutation via Unix
  socket CLI, reliable no-loss telemetry. G02/G08/G10/G11 end-to-end.
- M3: unified SSE, tool calling, Responses conversion, stream-only aggregation.
  G04/G05/G06 fully verified.
- M4: meaningful Auto policies, parity comparisons vs Go, fault injection,
  Linux amd64 build and canary deployment. G03/G09 fully verified.

**Never promote a milestone to production on a successful compilation alone.**
The Go service remains the production baseline until endpoint-level parity,
multiple deterministic test runs and canary results are available.
