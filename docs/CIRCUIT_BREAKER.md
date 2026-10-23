# Provider circuit breaker (opt-in)

On unreliable or frequently offline upstream providers, the Rust gateway can
avoid repeating a failed HTTP call for every user request before falling back
to a healthy provider. The breaker is DISABLED BY DEFAULT for compatibility.

Example provider stanza:

    circuit_breaker:
      enabled: true
      failure_threshold: 3
      cooldown: 2s
      max_cooldown: 30s

- enabled: false by default.
- failure_threshold: 3 by default; allowed 1..32.
- cooldown: initial 2s, at least 100ms.
- max_cooldown: maximum 30s; >= cooldown and <= 1h.
- Each failed recovery probe doubles cooldown until the maximum. Recovery
  resets it to the initial value.
- A fully validated upstream JSON result or Chat SSE terminal [DONE] closes
  the breaker and resets failure count.
- Transport failures, upstream 5xx, truncated/invalid 2xx and premature
  SSE termination are provider failures.
- Caller rejections, credential-scoped auth/rate limits, and downstream
  client cancellation are neutral.
- The breaker is shared across all models and keys of a configured provider.
  An open circuit skips its network request; a healthy fallback still runs.
- Only one half-open request probes recovery; other concurrent calls bypass.
  Abandoned probes release their reservation and back off briefly.
- If all eligible steps are skipped without any network attempt, the gateway
  returns HTTP 503 with Retry-After: 1 and provider_circuit_open.
- All state is bounded by provider count, with small Mutexes. No timers,
  persistent state or new dependencies.

Authenticated /metrics provides circuit_openings_total, circuit_bypasses_total,
circuit_probe_success_total, circuit_probe_failures_total and an
open_providers gauge. No provider, account, model or secret labels are emitted.

Unit and integration gates cover opening, cooldown, exponent doubling,
stale concurrent success, abandoned probes, 128 competing half-open callers,
12-request bypass, 24-request probe race, successful recovery, caller 400,
premature stream end and Retry-After semantics.

The Linux release-binary resource gate runs an additional process with the
breaker enabled and 64 requests against an injected-fault provider; exactly
three requests contact the failed provider, 61 requests bypass it, all 64
fall back successfully, and metrics/RSS/FD/thread limits are verified.

Do not mistake the provider breaker for key-level 429/auth quarantine or
the global request admission limiter.
