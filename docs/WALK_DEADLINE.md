# Issue 65: total upstream walk deadline

Previously every provider/key attempt received a fresh 90-second timeout,
so a non-streaming request could exceed a 120-second front-proxy window.

Configure one deadline for the entire non-streaming request:

    timeout:
      walk: 100s

Default is 100 seconds; Go-style durations from 100ms through 1h remain
parseable for older configurations, but the **effective non-streaming walk
budget is hard-capped at 100 seconds**. This leaves 20 seconds of headroom
before the deployment's 120-second front-proxy timeout, including all
candidate retries, streaming-to-JSON aggregation and permit waits. Setting
`timeout.walk: 600s` no longer permits a ten-minute non-streaming response.
Invalid/unknown configuration keys fail validation. One monotonic budget
includes all credentials/candidates, aggregation slots, upstream headers,
JSON bodies and SSE-to-JSON aggregation. It never resets on fallback.
Each attempt retains its separate 90-second limit; the earlier deadline wins.
On expiry, the gateway aborts I/O and returns HTTP 504 with error code
upstream_walk_timeout, without accepting a late 200 or continuing fallback.

Client-facing stream:true requests are exempt so a healthy SSE generation
may outlive the JSON deadline. Header, preflight, size and idle protections
remain active.

For local-only opt-in logging set GUVNA_LOG_REQUESTS=1. Every dispatcher
response produces one stderr JSON line with a per-process anonymous
request_id, elapsed_ms, status, phase. SSE phase headers_sent is NOT
success of the entire SSE body. No prompt, model, provider URL, token,
client address or request content enters this output. There is no new
remote management/telemetry interface. Separate GUVNA_LOG_EVENTS=1 emits
sampled fixed-category operational warnings.

Issue closeout policy: merge the fixing PR, verify Linux CI and the
adversarial resource gate, confirm issue-specific acceptance tests, then
comment with exact evidence and close the issue. Partial fixes remain open.
