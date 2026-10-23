# Responses SSE bridge: implemented boundary

The /v1/responses route accepts stream:true on supported stateless text
and function-tool inputs. The same auth, provider/key failover, bounded
admission, 15-second SSE inactivity timeout, and upstream finish validation
as Chat Completions apply.

## Observable event contract

- response.created and response.in_progress begin a Response.
- Text: response.output_item.added, response.content_part.added,
  response.output_text.delta, and matching .done events.
- Function tools: response.output_item.added,
  response.function_call_arguments.delta, .done, and
  response.output_item.done. Mixed text then parallel function calls
  preserve output order and call IDs.
- A verified stop and terminal upstream [DONE] emits response.completed.
  Finish reason length emits response.incomplete with max_output_tokens.
  A stream error emits one response.failed event (or an error event if
  the upstream never supplied a valid ID).

The bridge uses the existing incremental upstream Chat SSE parser; it never
replays a partially delivered response, and it does not manufacture
response.completed for missing [DONE], corrupt JSON or invalid tool
arguments. SSE event data includes a monotonically increasing sequence number.

## Explicit resource and compatibility boundaries

- Incoming normalized event: at most 1 MiB.
- Total generated text and buffered function arguments: at most 512 KiB.
- One function's arguments: at most 256 KiB.
- Number of functions in a single response: at most 32.
- Timeout waiting for the next upstream SSE chunk: 15 seconds.
- Each chunk is converted and emitted incrementally with backpressure.
  Disconnecting a client drops the upstream stream.
- Text, tool calls and usage are handled; tools require a fully formed
  name and ID on their first delta and final JSON-object arguments.
- Unsupported provider-specific deltas, reasoning/multimodal data and
  stateful sessions are rejected or result in explicit failure events.

These limits protect a low-memory, single-user VPS. They do not imply full
OpenAI Responses protocol or Go-legacy feature parity. Linux CI and
adversarial resource testing are the release gate for every change.
