# SSE-only upstream support, manual binary and local logs

This project implements local inference APIs, not remote administration.
No remote service-control endpoint, socket or remotely mutable configuration
is added. Existing /metrics is read-only and requires bearer authentication.

## Manual Linux binary

Run GitHub Actions workflow: Manual Linux x86_64 binary, selecting the
verified main branch. The workflow repeats format, Clippy and all Rust tests,
builds a real Linux x86_64 ELF and publishes artifact guvna-linux-x86_64.
Inside the artifact ZIP are:
- guvna-linux-x86_64 (0755 executable ELF, not a source archive)
- guvna-linux-x86_64.sha256 (SHA256 manifest)
- build-info.txt (source commit, host architecture, glibc and Rust version)

Verify the checksum on the Linux target before using the executable:

    sha256sum --check guvna-linux-x86_64.sha256
    chmod 0755 guvna-linux-x86_64
    ./guvna-linux-x86_64 --version

Ubuntu 24.04 is the build runner; compatibility with older glibc systems
must be verified separately. A green CI alone does not guarantee the
live supplier's production reliability. Deployment and rollback are separate.

## SSE-only -> non-stream Chat JSON

Set stream_only: true for an upstream provider. On a client Chat request
with stream omitted or false, the shared dispatch requests stream:true
from the provider, validates Chat SSE incrementally and only returns an
ordinary, fully completed JSON Chat result after a valid [DONE].
Malformed, truncated, stalled and oversized SSE output triggers existing
candidate failover without a partial JSON success. Streaming clients
continue to receive the existing Chat SSE relay.

Safety limits: at most 4 concurrently aggregating upstreams, 2 seconds
waiting for a permit, 15 seconds maximum SSE idle gap, 90 seconds per
upstream attempt, 8 MiB streamed input, 2 MiB generated output and
16,384 frames. No separate retry or routing loop is created.

## Local logs only

Set GUVNA_LOG_EVENTS=1 to enable JSON-line events on standard error.
A local journal or restricted log file collects and rotates the output.
Do not expose these logs on an unauthenticated route.

- aggregate_completed: sampled info event for successful JSON synthesis
- aggregate_failed: sampled warn event for unsuccessful SSE aggregation
- transport_failure, provider_http_failure and upstream_invalid: failures
- stream_preflight_failure and provider_circuit_open: related failures

Fields are restricted to ts_unix_ms, level, event and per-level sequence.
No prompts, keys, addresses, model names or provider URLs are included.
The first 8 events of each level plus each 128th later event are logged.
Sampling is not a substitute for full per-request accounting. Local
counter metrics continue counting all attempts and outcomes.

For a systemd-managed instance that collects stderr in the journal:

    journalctl -u guvna.service --since today --no-pager
    journalctl -u guvna.service -f

Confirm log retention and rotation without printing environment secrets.
Compare warning rates, fallback and resource usage after multiple days
of soak before claiming real-world stability.

## Mandatory real-binary gate

The Linux Release resource test launches a separate SSE-only provider and
gateway, sends 64 ordinary JSON requests (5 missing [DONE] intentionally),
and asserts 64/64 valid JSON results, deterministic fallback, counters,
bounded RSS/FDs/threads and credential-free local logs.
The existing 4,000 + 200 request gates are unchanged.
