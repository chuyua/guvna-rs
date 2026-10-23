//! Bounded Chat SSE -> non-stream Chat completion synthesis.
//! Reuses the already hardened ChatSse validator: no alternate framing
//! logic, no replay after partial content, and no relaxed tool-call rules.
use crate::sse::ChatSse;
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
const MAX_EVENTS: usize = 16_384;
const STREAM_IDLE: Duration = Duration::from_secs(15);

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
    tools: BTreeMap<u64, Tool>,
    finish_reason: Option<String>,
}

fn append_bounded(into: &mut String, fragment: &str, used: &mut usize) -> Result<(), ()> {
    *used = used.checked_add(fragment.len()).ok_or(())?;
    if *used > MAX_AGGREGATED_OUTPUT_BYTES {
        return Err(());
    }
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
) -> Result<(), ()> {
    if let Some(raw) = value.get("id") {
        let current = raw.as_str().filter(|id| !id.is_empty()).ok_or(())?;
        if id.as_deref().is_some_and(|old| old != current) {
            return Err(());
        }
        if id.is_none() {
            *id = Some(current.to_owned());
        }
    }
    if let Some(raw) = value.get("created") {
        let current = raw.as_u64().ok_or(())?;
        if created.is_some_and(|old| old != current) {
            return Err(());
        }
        *created = Some(current);
    }
    if let Some(raw) = value.get("model") {
        let current = raw.as_str().filter(|value| !value.is_empty()).ok_or(())?;
        if model.as_deref().is_some_and(|old| old != current) {
            return Err(());
        }
        if model.is_none() {
            *model = Some(current.to_owned());
        }
    }
    if let Some(value_usage) = value.get("usage").filter(|usage| !usage.is_null()) {
        if !value_usage.is_object() || !usage.is_null() {
            return Err(());
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
            return Err(());
        }
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
        let delta = event_choice
            .get("delta")
            .and_then(Value::as_object)
            .ok_or(())?;
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
                append_bounded(slot.get_or_insert_with(String::new), fragment, used)?;
            }
        }
        // Audio chunks cannot be represented by an ordinary text Chat
        // Completion. Fail over instead of returning an incomplete 200.
        if delta.get("audio").is_some_and(|value| !value.is_null()) {
            return Err(());
        }
        if let Some(tool_calls) = delta.get("tool_calls") {
            for tool_call in tool_calls.as_array().ok_or(())? {
                let index = tool_call.get("index").and_then(Value::as_u64).ok_or(())?;
                if index >= 32 {
                    return Err(());
                }
                let slot = choice.tools.entry(index).or_default();
                if let Some(call_id) = tool_call.get("id") {
                    let call_id = call_id.as_str().filter(|id| !id.is_empty()).ok_or(())?;
                    if slot.id.as_deref().is_some_and(|old| old != call_id) {
                        return Err(());
                    }
                    slot.id = Some(call_id.to_owned());
                }
                if let Some(function) = tool_call.get("function") {
                    if let Some(name) = function.get("name") {
                        let name = name.as_str().filter(|name| !name.is_empty()).ok_or(())?;
                        if slot.name.as_deref().is_some_and(|old| old != name) {
                            return Err(());
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
                return Err(());
            }
            choice.finish_reason = Some(reason.to_owned());
        }
    }
    Ok(())
}

fn event_payload(frame: &[u8]) -> Result<Option<Value>, ()> {
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
) -> Result<Value, ()> {
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
        return Err(());
    }
    // Apply the same named/required/none tool contract as direct Chat SSE.
    // Reject an incompatible first tool delta immediately rather than
    // holding the aggregation semaphore until the 15-second idle timeout.
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
        let bytes = tokio::time::timeout(STREAM_IDLE, upstream.chunk())
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?
            .ok_or(())?;
        if bytes.len() > MAX_CHUNK_BYTES {
            return Err(());
        }
        total_received = total_received.checked_add(bytes.len()).ok_or(())?;
        if total_received > MAX_TOTAL_STREAM_BYTES {
            return Err(());
        }
        for frame in parser.push(&bytes).map_err(|_| ())? {
            event_count += 1;
            if event_count > MAX_EVENTS {
                return Err(());
            }
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
