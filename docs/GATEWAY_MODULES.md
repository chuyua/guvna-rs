# Gateway module boundaries

This change is a mechanical file extraction, not a new transport, retry,
routing, conversion or policy implementation. The public
gateway::app(AppState) API and all endpoint paths remain unchanged.

## Module map

| Source | Single responsibility |
| --- | --- |
| src/gateway.rs | Router, public AppState, auth before body buffering, request admission, /models, /healthz and /metrics, common error and circuit helpers |
| src/responses.rs | Strict /v1/responses validation, separate translate_input and translate_chat_result functions, and unchanged Responses SSE delegation |
| src/responses_format.rs | Lossless, strict Responses text.format mapping to Chat response_format, with JSON schema field preservation and fail-closed unknown options |
| src/dispatch.rs | ONE candidate traversal for Chat/Embeddings and the Responses bridge, credential attempts, fallback, circuits, timeout and bounded JSON transport |
| src/chat_stream.rs | Chat SSE preflight, incremental frame delivery, terminal metrics, ownership and cancellation |
| src/response_validation.rs | Pure Chat/Embeddings upstream 2xx contract checks with their unit tests |
| src/sse.rs | Existing incremental Chat SSE parser and finish validation; unchanged |
| src/responses_sse.rs | Existing Chat SSE to Responses conversion; unchanged |

Modules except gateway.rs, sse.rs and responses_sse.rs are private gateway
children, bound with explicit path attributes to preserve the requested
src/*.rs filenames. Only the narrow pub(super) functions and enums needed
to connect adapters, dispatch, stream lifecycle and validators are shared
among them. No new publicly reachable module functions are introduced.

## Invariants

- Exactly one upstream candidate loop (dispatch.rs). Never duplicate retry,
  breaker, credential quarantine or failure attribution per API endpoint.
- Bearer auth and the 128-request admission permit precede upload buffering.
  The permit lives through streaming and is freed on client disconnect.
- Chat SSE must not replay through another provider after the first valid
  event. Frame bounds, DONE and terminal/error metrics stay in chat_stream.rs.
- Response validation never mutates provider state or calls the network.
- Responses remains stateless, unsupported modes fail closed and its streaming
  converter stays in responses_sse.rs. Structured text output mapping lives
  in responses_format.rs; invalid non-streaming JSON triggers provider failover,
  while invalid JSON after streamed deltas yields response.failed, never a
  fabricated completed response or a replay. Upstream must enforce full
  JSON-schema semantics beyond parseability.
- Preserve HTTP statuses, error codes, model resolution, function arguments,
  usage, event ordering and cancellation/retry behavior during extraction.

## Acceptance tests

Existing public-handler integration tests cover auth, admission, failure and
fallback, Chat/Embeddings/Responses, function calls, malformed responses,
fragmented and truncated SSE, upstream stalls, downstream disconnect and
terminal counters.

The response contract unit tests now live in response_validation.rs.
Authorization, admission and Retry-After tests remain in gateway.rs.
Linux CI must pass rustfmt, strict Clippy, full cargo test, Release binary
build and the unchanged multiwave SSE/resource gate before merging.
