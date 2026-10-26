//! Bounded Chat SSE -> non-stream Chat completion synthesis.
//! Reuses the already hardened ChatSse validator: no alternate framing
//! logic, no replay after partial content, and no relaxed tool-call rules.
use crate::{circuit::Ticket, sse::ChatSse};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

static SYNTHETIC_ID_SEQUENCE: AtomicU64 = AtomicU64::new(0);

const MAX_CHUNK_BYTES: usize = 2 * 1024 * 1024;
const MAX_TOTAL_STREAM_BYTES: usize = 8 * 1024 * 1024;
const MAX_AGGREGATED_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
// Reasoning models may emit one SSE event per reasoning token. Keep this
// count high enough for long generations while the 8 MiB input-byte ceiling
// makes this event limit a secondary defensive bound, not a promised number
// of accepted frames.
const MAX_EVENTS: usize = 262_144;
// A long prompt can leave a reasoning model silent for more than 15 seconds.
// The separate 100-second absolute lifetime and shared walk budget still bound
// resource use and total request latency.
pub(super) const STREAM_IDLE: Duration = Duration::from_secs(60);
// Long generations from SSE-only providers must not inherit the ordinary
// 90-second JSON-body deadline. The idle timer still rejects stalled streams,
// and this separate hard cap prevents indefinite drip-fed connections.
pub(super) const MAX_AGGREGATION_LIFETIME: Duration = Duration::from_secs(100);

// Error classes are fixed, local-only log keys. Never attach prompt/model,
// provider URL, API key, response data or client identity to these events.
#[derive(Debug, Clone, Copy)]
pub(super) enum AggregateError {
    InvalidStream,
    IdleTimeout,
    UpstreamTransport,
    EventLimit,
    InputLimit,
    OutputLimit,
}
impl From<()> for AggregateError {
    fn from(_: ()) -> Self {
        Self::InvalidStream
    }
}
impl AggregateError {
    pub(super) const fn log_event(self) -> &'static str {
        match self {
            Self::InvalidStream => "aggregation_invalid_sse",
            Self::IdleTimeout => "aggregation_idle_timeout",
            Self::UpstreamTransport => "aggregation_transport_failure",
            Self::EventLimit => "aggregation_event_limit",
            Self::InputLimit => "aggregation_input_limit",
            Self::OutputLimit => "aggregation_output_limit",
        }
    }
}

#[derive(Default)]
struct Tool {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

#[derive(Default)]
struct Choice {
    content: String,
    // Some providers emit these Chat delta extensions. Preserve them so
    // non-stream clients never receive a synthetic answer missing refusal
    // or reasoning fields from the upstream completion.
    refusal: Option<String>,
    reasoning: Option<String>,
    reasoning_content: Option<String>,
    reasoning_ordered: String,
    tools: BTreeMap<u64, Tool>,
    finish_reason: Option<String>,
}

fn account_bounded(used: &mut usize, size: usize) -> Result<(), AggregateError> {
    *used = used.checked_add(size).ok_or(AggregateError::OutputLimit)?;
    if *used > MAX_AGGREGATED_OUTPUT_BYTES {
        return Err(AggregateError::OutputLimit);
    }
    Ok(())
}

fn account_event(event_count: &mut usize) -> Result<(), AggregateError> {
    *event_count = (*event_count)
        .checked_add(1)
        .ok_or(AggregateError::EventLimit)?;
    if *event_count > MAX_EVENTS {
        return Err(AggregateError::EventLimit);
    }
    Ok(())
}

fn append_bounded(
    into: &mut String,
    fragment: &str,
    used: &mut usize,
) -> Result<(), AggregateError> {
    account_bounded(used, fragment.len())?;
    into.push_str(fragment);
    Ok(())
}

fn apply_event(
    value: &Value,
    choices: &mut [Choice],
    used: &mut usize,
    usage: &mut Value,
    id: &mut Option<String>,
    created: &mut Option<u64>,
    model: &mut Option<String>,
) -> Result<(), AggregateError> {
    if let Some(raw) = value.get("id") {
        let current = raw.as_str().filter(|id| !id.is_empty()).ok_or(())?;
        if id.as_deref().is_some_and(|old| old != current) {
            return Err(AggregateError::InvalidStream);
        }
        if id.is_none() {
            account_bounded(used, current.len())?;
            *id = Some(current.to_owned());
        }
    }
    if let Some(raw) = value.get("created") {
        let current = raw.as_u64().ok_or(())?;
        if created.is_some_and(|old| old != current) {
            return Err(AggregateError::InvalidStream);
        }
        *created = Some(current);
    }
    if let Some(raw) = value.get("model") {
        let current = raw.as_str().filter(|value| !value.is_empty()).ok_or(())?;
        if model.as_deref().is_some_and(|old| old != current) {
            return Err(AggregateError::InvalidStream);
        }
        if model.is_none() {
            account_bounded(used, current.len())?;
            *model = Some(current.to_owned());
        }
    }
    if let Some(value_usage) = value.get("usage").filter(|usage| !usage.is_null()) {
        if !value_usage.is_object() || !usage.is_null() {
            return Err(AggregateError::InvalidStream);
        }
        let prompt = value_usage
            .get("prompt_tokens")
            .and_then(Value::as_u64)
            .ok_or(())?;
        let completion = value_usage
            .get("completion_tokens")
            .and_then(Value::as_u64)
            .ok_or(())?;
        let total = value_usage
            .get("total_tokens")
            .and_then(Value::as_u64)
            .ok_or(())?;
        if prompt.checked_add(completion) != Some(total) {
            return Err(AggregateError::InvalidStream);
        }
        // Usage extras are client-visible JSON as well. The original limit
        // only counted text and tool arguments, allowing a large usage object
        // to bypass the advertised bounded output budget.
        account_bounded(used, value_usage.to_string().len())?;
        *usage = value_usage.clone();
    }
    for event_choice in value.get("choices").and_then(Value::as_array).ok_or(())? {
        let index = event_choice
            .get("index")
            .and_then(Value::as_u64)
            .ok_or(())?;
        let choice = choices
            .get_mut(usize::try_from(index).map_err(|_| ())?)
            .ok_or(())?;
        let empty_delta = serde_json::Map::new();
        let delta = match event_choice.get("delta") {
            Some(Value::Object(delta)) => delta,
            Some(Value::Null)
                if event_choice
                    .get("finish_reason")
                    .is_some_and(Value::is_string) =>
            {
                &empty_delta
            }
            _ => return Err(AggregateError::InvalidStream),
        };
        if let Some(content) = delta.get("content").filter(|content| !content.is_null()) {
            append_bounded(&mut choice.content, content.as_str().ok_or(())?, used)?;
        }
        for (field, slot) in [
            ("refusal", &mut choice.refusal),
            ("reasoning", &mut choice.reasoning),
            ("reasoning_content", &mut choice.reasoning_content),
        ] {
            if let Some(fragment) = delta.get(field).filter(|value| !value.is_null()) {
                let fragment = fragment.as_str().ok_or(())?;
                // Mirrored aliases in one frame contain the same content,
                // not two different thoughts. Keep first occurrence.
                if field == "reasoning_content" && delta.get("reasoning") == delta.get(field) {
                    continue;
                }
                append_bounded(slot.get_or_insert_with(String::new), fragment, used)?;
                if matches!(field, "reasoning" | "reasoning_content") {
                    choice.reasoning_ordered.push_str(fragment);
                }
            }
        }
        // Unknown non-null deltas (legacy function_call, images, audio, etc.)
        // cannot be synthesized losslessly as a JSON Chat Completion. Fail
        // over instead of silently discarding output and returning HTTP 200.
        if delta.iter().any(|(field, value)| {
            !value.is_null()
                && !matches!(
                    field.as_str(),
                    "role"
                        | "content"
                        | "refusal"
                        | "reasoning"
                        | "reasoning_content"
                        | "tool_calls"
                )
        }) {
            return Err(AggregateError::InvalidStream);
        }
        if let Some(tool_calls) = delta.get("tool_calls") {
            for tool_call in tool_calls.as_array().ok_or(())? {
                let index = tool_call.get("index").and_then(Value::as_u64).ok_or(())?;
                if index >= 32 {
                    return Err(AggregateError::InvalidStream);
                }
                let slot = choice.tools.entry(index).or_default();
                if let Some(call_id) = tool_call.get("id") {
                    let call_id = call_id.as_str().filter(|id| !id.is_empty()).ok_or(())?;
                    if slot.id.as_deref().is_some_and(|old| old != call_id) {
                        return Err(AggregateError::InvalidStream);
                    }
                    if slot.id.is_none() {
                        account_bounded(used, call_id.len())?;
                    }
                    slot.id = Some(call_id.to_owned());
                }
                if let Some(function) = tool_call.get("function") {
                    if let Some(name) = function.get("name") {
                        let name = name.as_str().filter(|name| !name.is_empty()).ok_or(())?;
                        if slot.name.as_deref().is_some_and(|old| old != name) {
                            return Err(AggregateError::InvalidStream);
                        }
                        if slot.name.is_none() {
                            account_bounded(used, name.len())?;
                        }
                        slot.name = Some(name.to_owned());
                    }
                    if let Some(arguments) = function.get("arguments") {
                        append_bounded(&mut slot.arguments, arguments.as_str().ok_or(())?, used)?;
                    }
                }
            }
        }
        if let Some(reason) = event_choice
            .get("finish_reason")
            .filter(|reason| !reason.is_null())
        {
            let reason = reason.as_str().ok_or(())?;
            if choice.finish_reason.is_some() {
                return Err(AggregateError::InvalidStream);
            }
            account_bounded(used, reason.len())?;
            choice.finish_reason = Some(reason.to_owned());
        }
    }
    Ok(())
}

fn event_payload(frame: &[u8]) -> Result<Option<Value>, AggregateError> {
    if frame == b"data: [DONE]\n\n" {
        return Ok(None);
    }
    let data = frame
        .strip_prefix(b"data: ")
        .and_then(|data| data.strip_suffix(b"\n\n"))
        .ok_or(())?;
    let value = serde_json::from_slice(data).map_err(|_| ())?;
    Ok(Some(value))
}

/// Called only for a Chat upstream that returns SSE to a client that wants
/// JSON. Returns Err for every incomplete, malformed, stalled or oversized
/// stream so the existing dispatcher can try another candidate.
pub(super) async fn aggregate(
    mut upstream: reqwest::Response,
    expected: u64,
    requested_model: &str,
    tool_choice: Option<&Value>,
    mut circuit_ticket: Option<&mut Ticket>,
) -> Result<Value, AggregateError> {
    let mime = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|header| header.to_str().ok())
        .unwrap_or("");
    if !mime
        .split(';')
        .next()
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"))
    {
        return Err(AggregateError::InvalidStream);
    }
    // Apply the same named/required/none tool contract as direct Chat SSE.
    // Reject an incompatible first tool delta immediately rather than
    // holding the aggregation semaphore until the 60-second idle timeout.
    let mut parser = ChatSse::new(expected)
        .map_err(|_| ())?
        .with_tool_choice(tool_choice);
    let mut choices = (0..expected).map(|_| Choice::default()).collect::<Vec<_>>();
    let mut used = 0_usize;
    let mut total_received = 0_usize;
    let mut event_count = 0_usize;
    let mut usage = Value::Null;
    let mut id = None;
    let mut created = None;
    let mut model = None;
    loop {
        let bytes = match tokio::time::timeout(STREAM_IDLE, upstream.chunk()).await {
            Err(_) => return Err(AggregateError::IdleTimeout),
            Ok(Err(_)) => return Err(AggregateError::UpstreamTransport),
            Ok(Ok(None)) => {
                // Some OpenAI-compatible SSE servers end cleanly at EOF
                // after every choice's finish_reason without the optional
                // [DONE] sentinel. Match direct Chat streaming behavior,
                // but never accept a partial line, event or unfinished choice.
                if parser.finalize_on_eof().is_none() {
                    return Err(AggregateError::InvalidStream);
                }
                parser.finish().map_err(|_| ())?;
                break;
            }
            Ok(Ok(Some(bytes))) => bytes,
        };
        if bytes.len() > MAX_CHUNK_BYTES {
            return Err(AggregateError::InputLimit);
        }
        total_received = total_received
            .checked_add(bytes.len())
            .ok_or(AggregateError::InputLimit)?;
        if total_received > MAX_TOTAL_STREAM_BYTES {
            return Err(AggregateError::InputLimit);
        }
        for frame in parser.push(&bytes).map_err(|_| ())? {
            account_event(&mut event_count)?;
            if let Some(event) = event_payload(&frame)? {
                apply_event(
                    &event,
                    &mut choices,
                    &mut used,
                    &mut usage,
                    &mut id,
                    &mut created,
                    &mut model,
                )?;
            }
        }
        // SSE-only JSON aggregation is also a long-lived probe. Only valid
        // content/tool deltas extend its half-open idle lease; completion
        // still requires the full, valid stream.
        if parser.take_progress()
            && let Some(ticket) = circuit_ticket.as_deref_mut()
        {
            ticket.progress();
        }
        if parser.is_done() {
            parser.finish().map_err(|_| ())?;
            break;
        }
    }
    let created = created.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    });
    let id = id.unwrap_or_else(|| {
        let sequence = SYNTHETIC_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        format!("chatcmpl-agg-{created}-{}-{sequence}", std::process::id())
    });
    let choices = choices.into_iter().enumerate().map(|(index, choice)| {
        let calls = if choice.tools.is_empty() {
            None
        } else {
            Some(choice.tools.into_values().map(|tool| {
                json!({"id":tool.id,"type":"function","function":{"name":tool.name,"arguments":tool.arguments}})
            }).collect::<Vec<_>>())
        };
        let content = if choice.content.is_empty() { Value::Null } else { Value::String(choice.content) };
        let mut message = json!({"role":"assistant","content":content});
        if let Some(refusal) = choice.refusal {
            message["refusal"] = Value::String(refusal);
        }
        if let Some(reasoning) = choice.reasoning {
            message["reasoning"] = Value::String(reasoning);
        }
        if let Some(reasoning_content) = choice.reasoning_content {
            message["reasoning_content"] = Value::String(reasoning_content);
        }
        if !choice.reasoning_ordered.is_empty() {
            message["reasoning_ordered"] = Value::String(choice.reasoning_ordered);
        }
        if let Some(calls) = calls {
            message["tool_calls"] = json!(calls);
        }
        json!({"index":index,"message":message,"finish_reason":choice.finish_reason})
    }).collect::<Vec<_>>();
    Ok(json!({
        "id":id,
        "object":"chat.completion",
        "created":created,
        "model":model.unwrap_or_else(|| requested_model.to_owned()),
        "choices":choices,
        "usage":usage,
    }))
}

#[cfg(test)]
mod output_budget_tests {
    use super::*;
    #[test]
    fn terminal_null_delta_is_valid_but_nonterminal_null_is_not() {
        let mut choices = vec![Choice::default()];
        let mut used = 0;
        let mut usage = Value::Null;
        let mut id = None;
        let mut created = None;
        let mut model = None;
        let mut feed = |value: Value| {
            apply_event(
                &value,
                &mut choices,
                &mut used,
                &mut usage,
                &mut id,
                &mut created,
                &mut model,
            )
        };
        feed(json!({"choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":null}]}))
            .unwrap();
        feed(json!({"choices":[{"index":0,"delta":null,"finish_reason":"stop"}]})).unwrap();
        assert_eq!(choices[0].content, "ok");
        assert_eq!(choices[0].finish_reason.as_deref(), Some("stop"));
        let mut used = 0;
        assert!(
            with_event(
                json!({"choices":[{"index":0,"delta":null,"finish_reason":null}]}),
                &mut used
            )
            .is_err()
        );
    }
    fn with_event(value: Value, used: &mut usize) -> Result<(), AggregateError> {
        let mut choices = vec![Choice::default()];
        let mut usage = Value::Null;
        let mut id = None;
        let mut created = None;
        let mut model = None;
        apply_event(
            &value,
            &mut choices,
            used,
            &mut usage,
            &mut id,
            &mut created,
            &mut model,
        )
    }

    #[test]
    fn reasoning_fragments_follow_arrival_order_and_mirrors_count_once() {
        let mut choices = vec![Choice::default()];
        let mut used = 0;
        let mut usage = Value::Null;
        let mut id = None;
        let mut created = None;
        let mut model = None;
        for event in [
            json!({"choices":[{"index":0,"delta":{"reasoning_content":"thinking "},"finish_reason":null}]}),
            json!({"choices":[{"index":0,"delta":{"reasoning":"continues"},"finish_reason":null}]}),
            json!({"choices":[{"index":0,"delta":{"reasoning":" mirror","reasoning_content":" mirror"},"finish_reason":null}]}),
        ] {
            apply_event(
                &event,
                &mut choices,
                &mut used,
                &mut usage,
                &mut id,
                &mut created,
                &mut model,
            )
            .unwrap();
        }
        assert_eq!(choices[0].reasoning_ordered, "thinking continues mirror");
    }

    #[test]
    fn usage_metadata_and_identifiers_share_the_content_memory_budget() {
        let mut used = MAX_AGGREGATED_OUTPUT_BYTES - 100;
        let usage = json!({
            "prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5,
            "provider_specific": "z".repeat(128)
        });
        assert!(matches!(
            with_event(json!({"choices":[],"usage":usage}), &mut used),
            Err(AggregateError::OutputLimit)
        ));

        for event in [
            json!({"id":"x".repeat(128),"choices":[]}),
            json!({"model":"x".repeat(128),"choices":[]}),
            json!({"choices":[{"index":0, "delta":{}, "finish_reason":"x".repeat(128)}]}),
            json!({"choices":[{"index":0, "delta":{"tool_calls":[{
                "index":0, "id":"x".repeat(128)}]}, "finish_reason":null}]}),
            json!({"choices":[{"index":0, "delta":{"tool_calls":[{
                "index":0, "function":{"name":"x".repeat(128)}}]},
                "finish_reason":null}]}),
        ] {
            let mut used = MAX_AGGREGATED_OUTPUT_BYTES - 100;
            assert!(matches!(
                with_event(event, &mut used),
                Err(AggregateError::OutputLimit)
            ));
        }
    }

    #[test]
    fn stable_metadata_in_repeated_events_is_only_accounted_once() {
        let mut choices = vec![Choice::default()];
        let mut used = 0;
        let mut usage = Value::Null;
        let mut id = None;
        let mut created = None;
        let mut model = None;
        let first = json!({"id":"id1","model":"model1",
            "choices":[{"index":0,"delta":{"content":"a"},"finish_reason":null}]});
        for _ in 0..1000 {
            apply_event(
                &first,
                &mut choices,
                &mut used,
                &mut usage,
                &mut id,
                &mut created,
                &mut model,
            )
            .unwrap();
        }
        // No count inflation for stable id/model repeated by the provider.
        assert_eq!(used, 3 + 6 + 1000);
    }

    #[test]
    fn lifetime_is_longer_than_header_timeout_but_not_unbounded() {
        assert!(MAX_AGGREGATION_LIFETIME > Duration::from_secs(90));
        assert!(MAX_AGGREGATION_LIFETIME <= Duration::from_secs(100));
        assert!(STREAM_IDLE >= Duration::from_secs(60));
        assert!(STREAM_IDLE < MAX_AGGREGATION_LIFETIME);
    }

    #[test]
    fn long_reasoning_generations_fit_event_budget_but_remain_bounded() {
        let mut count = 0;
        for _ in 0..22_000 {
            account_event(&mut count).unwrap();
        }
        assert_eq!(count, 22_000);
        let remaining = MAX_EVENTS - count;
        for _ in 0..remaining {
            account_event(&mut count).unwrap();
        }
        assert_eq!(count, MAX_EVENTS);
        assert!(matches!(
            account_event(&mut count),
            Err(AggregateError::EventLimit)
        ));
    }
}

#[cfg(test)]
mod error_contract_tests {
    use super::AggregateError;

    #[test]
    fn categories_are_fixed_redaction_safe_local_events() {
        for (error, expected) in [
            (AggregateError::InvalidStream, "aggregation_invalid_sse"),
            (AggregateError::IdleTimeout, "aggregation_idle_timeout"),
            (
                AggregateError::UpstreamTransport,
                "aggregation_transport_failure",
            ),
            (AggregateError::EventLimit, "aggregation_event_limit"),
            (AggregateError::InputLimit, "aggregation_input_limit"),
            (AggregateError::OutputLimit, "aggregation_output_limit"),
        ] {
            assert_eq!(error.log_event(), expected);
            assert!(
                expected
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c == b'_')
            );
        }
    }
}
