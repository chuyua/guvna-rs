//! OpenAI Responses compatibility adapter.
//! Request validation, request translation, chat-result translation and
//! Responses SSE wrapping live here. Dispatch still uses one shared
//! candidate/failover loop in dispatch.rs; this module never retries upstream.
use super::dispatch::{BufferedResponseLease, Endpoint, dispatch_parsed, hold_buffered_response};
use super::responses_format;
use super::{AppState, MAX_BODY_BYTES, authenticated_body, error};
use axum::{
    Json,
    body::{Body, to_bytes},
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::sync::Arc;

// Strict stateless Responses compatibility adapter; unsupported modes fail closed.
pub(super) async fn handle(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (body, permit, payload_permit) = match authenticated_body(&state, request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };
    let mut value: Value = match serde_json::from_slice::<Value>(&body) {
        Ok(value) if value.is_object() => value,
        _ => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                "Expected one JSON object",
            );
        }
    };
    // The translated Value owns its data; do not retain the original body
    // buffer while validating and dispatching the translated request.
    let large_payload = body.len() >= super::LARGE_REQUEST_BODY_BYTES;
    drop(body);
    // Responses rewrites the input tree and then awaits the shared candidate
    // walker. Keep the original admission reservation for large requests
    // through conversion and dispatch; do not retain it for tiny requests.
    let _large_payload_reservation = if large_payload {
        Some(payload_permit)
    } else {
        drop(payload_permit);
        None
    };
    let streaming = value.get("stream") == Some(&Value::Bool(true));

    if value.get("stream").is_some_and(|v| !v.is_boolean()) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_stream",
            "stream must be a boolean",
        );
    }
    let Some(logical_model) = value
        .get("model")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty())
        .map(str::to_owned)
    else {
        return error(
            StatusCode::BAD_REQUEST,
            "missing_model",
            "model must be a nonempty string",
        );
    };
    let Some(input) = value.get("input").cloned() else {
        return error(
            StatusCode::BAD_REQUEST,
            "missing_input",
            "input is required",
        );
    };
    let messages = match translate_input(input) {
        Ok(messages) => messages,
        Err(response) => return *response,
    };
    for (name, min, max) in [("temperature", 0.0, 2.0), ("top_p", 0.0, 1.0)] {
        if value.get(name).is_some_and(|parameter| {
            parameter
                .as_f64()
                .is_none_or(|number| !number.is_finite() || number < min || number > max)
        }) {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_sampling_parameter",
                "Sampling parameter is outside its valid range",
            );
        }
    }
    if value
        .get("max_output_tokens")
        .is_some_and(|v| v.as_u64().is_none_or(|n| n == 0))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_max_output_tokens",
            "max_output_tokens must be a positive integer",
        );
    }
    if value.get("user").is_some_and(|v| !v.is_string()) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_user",
            "user must be a string",
        );
    }
    if value.get("instructions").is_some_and(|v| !v.is_string()) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_instructions",
            "instructions must be a string",
        );
    }
    if value.get("n").is_some_and(|n| n.as_u64() != Some(1)) {
        return error(
            StatusCode::NOT_IMPLEMENTED,
            "responses_multi_choice_unsupported",
            "Responses requires one output choice",
        );
    }
    // Stateless proxy: only explicit non-persistent modes are safe to accept.
    for (key, expected) in [
        ("store", Value::Bool(false)),
        ("background", Value::Bool(false)),
        ("truncation", Value::String("disabled".into())),
    ] {
        if value.get(key).is_some_and(|actual| actual != &expected) {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "responses_feature_unsupported",
                "This stateful Responses mode is not supported",
            );
        }
    }
    let metadata = match value.get("metadata") {
        None | Some(Value::Null) => Value::Null,
        Some(Value::Object(map))
            if map.len() <= 16
                && map.iter().all(|(key, value)| {
                    !key.is_empty()
                        && key.len() <= 64
                        && value.as_str().is_some_and(|text| text.len() <= 512)
                }) =>
        {
            Value::Object(map.clone())
        }
        Some(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_metadata",
                "metadata must contain at most 16 short string entries",
            );
        }
    };
    let object = value.as_object_mut().expect("checked above");
    for key in [
        "previous_response_id",
        "conversation",
        "reasoning",
        "modalities",
        "include",
        "service_tier",
        "safety_identifier",
        "prompt",
        "prompt_cache_key",
        "messages",
        "max_tokens",
    ] {
        if object.contains_key(key) {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "responses_feature_unsupported",
                "This Responses feature is not yet supported",
            );
        }
    }
    // Allow only parameters whose Chat Completions semantics are equivalent.
    for key in object.keys() {
        if ![
            "model",
            "n",
            "input",
            "stream",
            "text",
            "max_output_tokens",
            "temperature",
            "top_p",
            "user",
            "instructions",
            "tools",
            "tool_choice",
            "parallel_tool_calls",
            "metadata",
            "store",
            "background",
            "truncation",
        ]
        .contains(&key.as_str())
        {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "responses_parameter_unsupported",
                "Unsupported Responses parameter",
            );
        }
    }
    if object
        .get("parallel_tool_calls")
        .is_some_and(|v| !v.is_boolean())
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_parallel_tool_calls",
            "parallel_tool_calls must be boolean",
        );
    }
    // Structured text output uses a different envelope in Responses and Chat.
    // Translate only modes whose parameters can be forwarded without loss.
    let mapped_text_format = match responses_format::translate(object.get("text")) {
        Ok(format) => format,
        Err(problem) => return error(problem.0, problem.1, problem.2),
    };
    let json_mode = mapped_text_format
        .as_ref()
        .and_then(|format| format.get("type").and_then(Value::as_str))
        .and_then(|kind| match kind {
            "json_object" => Some(true),
            "json_schema" => Some(false),
            _ => None,
        });
    // Map the supported Responses function-tool declaration to Chat Completions.
    let mut declared_functions = std::collections::HashSet::<String>::new();
    if let Some(tools) = object.get_mut("tools") {
        let Some(items) = tools.as_array_mut() else {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_tools",
                "tools must be an array",
            );
        };
        if items.is_empty() {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_tools",
                "tools must not be empty",
            );
        }
        for tool in items {
            let Some(map) = tool.as_object() else {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_tools",
                    "tool must be an object",
                );
            };
            if map.get("type").and_then(Value::as_str) != Some("function")
                || map
                    .get("name")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                || map.keys().any(|key| {
                    !["type", "name", "description", "parameters", "strict"].contains(&key.as_str())
                })
            {
                return error(
                    StatusCode::NOT_IMPLEMENTED,
                    "responses_tools_unsupported",
                    "Only named function tools are supported",
                );
            }
            let name = map["name"].as_str().expect("validated above");
            // Chat providers enforce the OpenAI function identifier grammar.
            // Reject locally rather than wasting a provider attempt or leaking
            // invalid declarations into the shared fallback candidate walk.
            if !valid_function_name(name) {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_function_name",
                    "Function names must be 1-64 ASCII letters, digits, '_' or '-'",
                );
            }
            if !declared_functions.insert(name.to_owned()) {
                return error(
                    StatusCode::BAD_REQUEST,
                    "duplicate_function",
                    "Function names must be unique",
                );
            }
            if map.get("description").is_some_and(|v| !v.is_string())
                || map.get("parameters").is_some_and(|v| !v.is_object())
                || map.get("strict").is_some_and(|v| !v.is_boolean())
            {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_function",
                    "Invalid function declaration fields",
                );
            }
            let mut function = map.clone();
            function.remove("type");
            *tool = json!({"type":"function","function":function});
        }
    }
    if let Some(choice) = object.get_mut("tool_choice") {
        if let Some(name) = choice.as_str() {
            if !matches!(name, "auto" | "none" | "required") {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_tool_choice",
                    "Unsupported tool choice",
                );
            }
        } else if let Some(map) = choice.as_object() {
            if map.get("type").and_then(Value::as_str) != Some("function")
                || map
                    .get("name")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                || map.len() != 2
            {
                return error(
                    StatusCode::NOT_IMPLEMENTED,
                    "responses_tool_choice_unsupported",
                    "Unsupported tool choice",
                );
            }
            if !declared_functions.contains(map["name"].as_str().expect("validated above")) {
                return error(
                    StatusCode::BAD_REQUEST,
                    "unknown_function",
                    "Selected function was not declared",
                );
            }
            *choice = json!({"type":"function","function":{"name":map["name"]}});
        } else {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_tool_choice",
                "Invalid tool choice",
            );
        }
    }
    if object
        .get("tool_choice")
        .is_some_and(|choice| choice == "required" || choice.is_object())
        && declared_functions.is_empty()
    {
        return error(
            StatusCode::BAD_REQUEST,
            "missing_tools",
            "A required tool choice needs declared tools",
        );
    }
    // Instructions become a leading system message, not a provider-specific field.
    // Validate before moving any data into the shared chat dispatcher.
    for key in ["metadata", "store", "background", "truncation"] {
        object.remove(key);
    }
    object.remove("text");
    if let Some(format) = mapped_text_format {
        object.insert("response_format".into(), format);
    }
    let instructions = match object.remove("instructions") {
        Some(Value::String(text)) => Some(text),
        Some(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_instructions",
                "instructions must be a string",
            );
        }
        None => None,
    };
    let mut messages = messages;
    if let (Some(text), Some(items)) = (instructions, messages.as_array_mut()) {
        items.insert(0, json!({"role":"system","content":text}));
    }
    object.remove("input");
    if !streaming {
        object.remove("stream");
    }
    object.insert("messages".into(), messages);
    // Responses has exactly one output choice. A configured Chat chain may
    // specify params.n=2 or more as a default; caller-wins dispatch merges
    // would otherwise forward that default and then reject the upstream's
    // multi-choice success as an invalid Responses result. Send n=1
    // explicitly so every fallback candidate honors this endpoint contract.
    if let Some(tokens) = object.remove("max_output_tokens") {
        object.insert("max_tokens".into(), tokens);
    }
    // A Responses result is always a single assistant choice, even when a
    // routing-chain default injects n > 1 for ordinary Chat requests.
    object.insert("n".into(), Value::from(1));
    let response_metrics = state.metrics.clone();
    let response = dispatch_parsed(
        state,
        value,
        large_payload,
        Endpoint::Responses,
        permit,
        None,
    )
    .await;
    if response.status() != StatusCode::OK {
        return response;
    }
    if streaming {
        return crate::responses_sse::wrap(
            response,
            logical_model,
            metadata,
            response_metrics,
            json_mode,
        );
    }
    let memory_lease = response
        .extensions()
        .get::<BufferedResponseLease>()
        .cloned();
    let bytes = match to_bytes(response.into_body(), MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return error(
                StatusCode::BAD_GATEWAY,
                "invalid_upstream",
                "Chat response exceeds size limit",
            );
        }
    };
    let chat: Value = match serde_json::from_slice(&bytes) {
        Ok(chat) => chat,
        Err(_) => {
            return error(
                StatusCode::BAD_GATEWAY,
                "invalid_upstream",
                "Invalid upstream chat response",
            );
        }
    };
    // The original Chat body may have been drained, but its converted
    // Responses body can still be large and held by a slow TCP consumer.
    // Reuse that same lease rather than releasing it between translations.
    hold_buffered_response(
        translate_chat_result(chat, logical_model, metadata),
        memory_lease,
    )
}

// Match Chat Completions' function name contract. Byte-based validation
// also rejects visually deceptive Unicode identifiers and oversized names.
fn valid_function_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

// Map input text or complete message/function-call history into
// Chat messages. Preserve all original rejection codes and requirements.
fn translate_input(input: Value) -> Result<Value, Box<Response>> {
    // Translate the common Responses text message shape into Chat Completions.
    // Reject non-text content rather than silently discarding image/tool data.
    let messages = match input {
        Value::String(text) => json!([{"role":"user","content":text}]),
        Value::Array(items) if !items.is_empty() => {
            let mut messages = Vec::with_capacity(items.len());
            let mut pending_calls = std::collections::HashSet::<String>::new();
            let mut seen_calls = std::collections::HashSet::<String>::new();
            let mut pending_reasoning: Option<String> = None;
            for item in items {
                if item.get("type").and_then(Value::as_str) == Some("reasoning") {
                    // Replay only our own completed reasoning wire shape;
                    // rejecting extra fields avoids silently losing content.
                    let parts = item.get("content").and_then(Value::as_array);
                    let valid = item.as_object().is_some_and(|obj| {
                        obj.keys().all(|key| {
                            ["type", "id", "status", "summary", "content"].contains(&key.as_str())
                        })
                    }) && item["id"].as_str().is_some_and(|id| !id.is_empty())
                        && item["status"] == "completed"
                        && item["summary"].as_array().is_some_and(Vec::is_empty)
                        && parts.is_some_and(|items| {
                            items.len() == 1
                                && items[0]["type"] == "reasoning_text"
                                && items[0]["text"].is_string()
                                && items[0].as_object().is_some_and(|obj| {
                                    obj.keys()
                                        .all(|key| ["type", "text"].contains(&key.as_str()))
                                })
                        });
                    if !valid || pending_reasoning.is_some() || !pending_calls.is_empty() {
                        return Err(Box::new(error(
                            StatusCode::NOT_IMPLEMENTED,
                            "responses_input_unsupported",
                            "Unsupported reasoning history",
                        )));
                    }
                    pending_reasoning = item["content"][0]["text"].as_str().map(str::to_owned);
                    continue;
                }
                if item.get("type").and_then(Value::as_str) == Some("function_call_output") {
                    if pending_reasoning.is_some() {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "unsupported_input",
                            "Reasoning cannot precede a tool result",
                        )));
                    }
                    let Some(call_id) = item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                    else {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_output",
                            "function_call_output requires call_id",
                        )));
                    };
                    let Some(output) = item.get("output").and_then(Value::as_str) else {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_output",
                            "function_call_output requires string output",
                        )));
                    };
                    if item.as_object().is_none_or(|map| {
                        map.keys()
                            .any(|key| !["type", "call_id", "output"].contains(&key.as_str()))
                    }) {
                        return Err(Box::new(error(
                            StatusCode::NOT_IMPLEMENTED,
                            "function_output_unsupported",
                            "Unsupported function output fields",
                        )));
                    }
                    if !pending_calls.remove(call_id) {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "orphan_function_output",
                            "Function output has no matching preceding call",
                        )));
                    }
                    messages.push(json!({"role":"tool","tool_call_id":call_id,"content":output}));
                    continue;
                }
                if item.get("type").and_then(Value::as_str) == Some("function_call") {
                    let Some(call_id) = item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                    else {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_call",
                            "function_call requires call_id",
                        )));
                    };
                    let Some(name) = item
                        .get("name")
                        .and_then(Value::as_str)
                        .filter(|name| !name.is_empty())
                    else {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_call",
                            "function_call requires name",
                        )));
                    };
                    let Some(arguments) = item.get("arguments").and_then(Value::as_str) else {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_call",
                            "function_call requires arguments",
                        )));
                    };
                    if !valid_function_name(name) {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_name",
                            "Function names must be 1-64 ASCII letters, digits, '_' or '-'",
                        )));
                    }
                    if !serde_json::from_str::<Value>(arguments)
                        .is_ok_and(|value| value.is_object())
                        || item.as_object().is_none_or(|map| {
                            map.keys().any(|key| {
                                !["type", "call_id", "name", "arguments", "id", "status"]
                                    .contains(&key.as_str())
                            })
                        })
                    {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_call",
                            "Invalid function call arguments or fields",
                        )));
                    }
                    if item
                        .get("id")
                        .is_some_and(|v| v.as_str().is_none_or(str::is_empty))
                        || item.get("status").is_some_and(|v| v != "completed")
                    {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_call",
                            "Function call id and status must be valid",
                        )));
                    }
                    if !seen_calls.insert(call_id.to_owned())
                        || !pending_calls.insert(call_id.to_owned())
                    {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "duplicate_function_call",
                            "Function call id is already pending",
                        )));
                    }
                    let tool_call = json!({"id":call_id,"type":"function","function":{"name":name,"arguments":arguments}});
                    if let Some(last) = messages.last_mut().filter(|message: &&mut Value| {
                        message["role"] == "assistant" && message["tool_calls"].is_array()
                    }) {
                        if let Some(reasoning) = pending_reasoning.take() {
                            last["reasoning_content"] = Value::String(reasoning);
                        }
                        last["tool_calls"]
                            .as_array_mut()
                            .expect("checked above")
                            .push(tool_call);
                    } else {
                        let mut assistant = json!({"role":"assistant","content":Value::Null,"tool_calls":[tool_call]});
                        if let Some(reasoning) = pending_reasoning.take() {
                            assistant["reasoning_content"] = Value::String(reasoning);
                        }
                        messages.push(assistant);
                    }
                    continue;
                }
                if !pending_calls.is_empty() {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "pending_function_calls",
                        "Function calls must be resolved before another message",
                    )));
                }
                let Some(role) = item.get("role").and_then(Value::as_str) else {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "unsupported_input",
                        "Input messages require a role",
                    )));
                };
                if !matches!(role, "user" | "assistant" | "system" | "developer")
                    || item.get("type").is_some_and(|kind| kind != "message")
                    || item.as_object().is_none_or(|obj| {
                        obj.keys().any(|k| {
                            !["role", "content", "type", "id", "status"].contains(&k.as_str())
                        }) || obj.get("id").is_some_and(|v| !v.is_string())
                            || obj.get("status").is_some_and(|v| v != "completed")
                    })
                {
                    return Err(Box::new(error(
                        StatusCode::NOT_IMPLEMENTED,
                        "responses_input_unsupported",
                        "Only plain text messages are supported",
                    )));
                }
                let Some(content) = item.get("content") else {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "unsupported_input",
                        "Input messages require content",
                    )));
                };
                let text = if let Some(text) = content.as_str() {
                    text.to_owned()
                } else if let Some(parts) = content.as_array() {
                    if parts.is_empty() {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "unsupported_input",
                            "Empty content parts",
                        )));
                    }
                    let mut text = String::new();
                    for part in parts {
                        if part.get("type").and_then(Value::as_str) == Some("refusal") {
                            if role != "assistant" {
                                return Err(Box::new(error(
                                    StatusCode::BAD_REQUEST,
                                    "invalid_refusal_role",
                                    "refusal parts require assistant role",
                                )));
                            }
                            if part.as_object().is_none_or(|obj| {
                                obj.keys()
                                    .any(|key| !["type", "refusal"].contains(&key.as_str()))
                            }) {
                                return Err(Box::new(error(
                                    StatusCode::NOT_IMPLEMENTED,
                                    "responses_input_unsupported",
                                    "Unsupported refusal part fields",
                                )));
                            }
                            let Some(fragment) = part.get("refusal").and_then(Value::as_str) else {
                                return Err(Box::new(error(
                                    StatusCode::BAD_REQUEST,
                                    "unsupported_input",
                                    "refusal parts require refusal text",
                                )));
                            };
                            text.push_str(fragment);
                            continue;
                        }
                        if !matches!(
                            part.get("type").and_then(Value::as_str),
                            Some("input_text") | Some("output_text")
                        ) || part.as_object().is_none_or(|obj| {
                            obj.keys()
                                .any(|k| !["type", "text", "annotations"].contains(&k.as_str()))
                                || obj
                                    .get("annotations")
                                    .is_some_and(|v| v.as_array().is_none_or(|a| !a.is_empty()))
                        }) {
                            return Err(Box::new(error(
                                StatusCode::NOT_IMPLEMENTED,
                                "responses_input_unsupported",
                                "Only input_text or output_text content parts are supported",
                            )));
                        }
                        if part["type"] == "output_text" && role != "assistant" {
                            return Err(Box::new(error(
                                StatusCode::BAD_REQUEST,
                                "invalid_output_text_role",
                                "output_text parts require assistant role",
                            )));
                        }
                        let Some(fragment) = part.get("text").and_then(Value::as_str) else {
                            return Err(Box::new(error(
                                StatusCode::BAD_REQUEST,
                                "unsupported_input",
                                "input_text requires text",
                            )));
                        };
                        text.push_str(fragment);
                    }
                    text
                } else {
                    return Err(Box::new(error(
                        StatusCode::NOT_IMPLEMENTED,
                        "responses_input_unsupported",
                        "Unsupported message content",
                    )));
                };
                if role != "assistant"
                    && let Some(reasoning) = pending_reasoning.take()
                {
                    messages.push(json!({"role":"assistant","content":Value::Null,
                        "reasoning_content":reasoning}));
                }
                let mut message = json!({"role":role,"content":text});
                if role == "assistant"
                    && let Some(reasoning) = pending_reasoning.take()
                {
                    message["reasoning_content"] = Value::String(reasoning);
                }
                messages.push(message);
            }
            if !pending_calls.is_empty() {
                return Err(Box::new(error(
                    StatusCode::BAD_REQUEST,
                    "pending_function_calls",
                    "Function calls require matching output before continuing",
                )));
            }
            if let Some(reasoning) = pending_reasoning {
                messages.push(json!({"role":"assistant","content":Value::Null,
                    "reasoning_content":reasoning}));
            }
            Value::Array(messages)
        }
        _ => {
            return Err(Box::new(error(
                StatusCode::BAD_REQUEST,
                "unsupported_input",
                "Only text or text message arrays are supported",
            )));
        }
    };
    Ok(messages)
}

// Convert one fully buffered Chat Completion into a non-streaming Responses
// document. This function performs no I/O, routing or retry decisions.
fn reasoning_text(message: &Value) -> Result<Option<String>, ()> {
    if let Some(ordered) = message.get("reasoning_ordered") {
        return Ok(ordered
            .as_str()
            .filter(|text| !text.is_empty())
            .map(str::to_owned));
    }
    let reasoning = message.get("reasoning").filter(|value| !value.is_null());
    let reasoning_content = message
        .get("reasoning_content")
        .filter(|value| !value.is_null());
    let text = match (reasoning, reasoning_content) {
        (Some(left), Some(right)) if left == right => Some(left.as_str().ok_or(())?.to_owned()),
        (Some(left), Some(right)) => Some(format!(
            "{}{}",
            left.as_str().ok_or(())?,
            right.as_str().ok_or(())?
        )),
        (Some(value), None) | (None, Some(value)) => Some(value.as_str().ok_or(())?.to_owned()),
        (None, None) => None,
    };
    Ok(text.filter(|text| !text.is_empty()))
}

fn reasoning_item(response_id: &str, text: &str, status: &str) -> Value {
    json!({
        "id":format!("rs_{response_id}"),"type":"reasoning","status":status,
        "summary":[],"content":[{"type":"reasoning_text","text":text}]
    })
}

/// Validate the endpoint-specific conversion contract *before* the shared
/// candidate walker commits its success, health and key-lease accounting.
/// This does not route/retry; it only rejects unusable Chat responses.
pub(super) fn translatable_chat_result(chat: &Value) -> bool {
    let usage = &chat["usage"];
    match (
        usage["prompt_tokens"].as_u64(),
        usage["completion_tokens"].as_u64(),
        usage["total_tokens"].as_u64(),
    ) {
        (Some(input), Some(output), Some(total)) if input.checked_add(output) == Some(total) => {}
        (None, None, None) => {}
        _ => return false,
    }
    let Some(choices) = chat.get("choices").and_then(Value::as_array) else {
        return false;
    };
    if choices.len() != 1
        || choices[0]["index"].as_u64() != Some(0)
        || choices[0]["message"]["role"] != "assistant"
    {
        return false;
    }
    if chat
        .get("id")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
        || chat
            .get("created")
            .is_some_and(|created| created.as_i64().is_none_or(|timestamp| timestamp < 0))
        || chat
            .get("model")
            .is_some_and(|model| model.as_str().is_none_or(str::is_empty))
    {
        return false;
    }
    let choice = &choices[0];
    let message = &choice["message"];
    if reasoning_text(message).is_err() {
        return false;
    }
    let calls = message["tool_calls"].as_array();
    if message
        .get("tool_calls")
        .is_some_and(|value| !value.is_array())
    {
        return false;
    }
    if let Some(calls) = calls.filter(|calls| !calls.is_empty()) {
        let finish = choice["finish_reason"].as_str();
        if !matches!(finish, Some("tool_calls" | "length" | "content_filter"))
            || !(message["content"].is_null() || message["content"].as_str().is_some())
        {
            return false;
        }
        let mut seen_ids = std::collections::HashSet::new();
        return calls.iter().all(|call| {
            let Some(id) = call["id"].as_str().filter(|id| !id.is_empty()) else {
                return false;
            };
            call["type"] == "function"
                && seen_ids.insert(id)
                && call["function"]["name"]
                    .as_str()
                    .is_some_and(|name| !name.is_empty())
                && call["function"]["arguments"].as_str().is_some_and(|args| {
                    finish != Some("tool_calls")
                        || args.is_empty()
                        || serde_json::from_str::<Value>(args).is_ok_and(|value| value.is_object())
                })
        });
    }
    if choice["finish_reason"] == "tool_calls" {
        return false;
    }
    let supports_content = message["content"].as_str().is_some()
        || (message["content"].is_null()
            && (reasoning_text(message).ok().flatten().is_some()
                || message["refusal"].as_str().is_some()));
    supports_content
        && matches!(
            choice["finish_reason"].as_str(),
            Some("stop" | "length" | "content_filter")
        )
}

fn translate_chat_result(chat: Value, logical_model: String, metadata: Value) -> Response {
    // Validate and translate usage once for both text and function-call responses.
    let usage = &chat["usage"];
    let translated_usage = match (
        usage["prompt_tokens"].as_u64(),
        usage["completion_tokens"].as_u64(),
        usage["total_tokens"].as_u64(),
    ) {
        (Some(input), Some(output), Some(total)) if input.checked_add(output) == Some(total) => {
            json!({"input_tokens":input,"output_tokens":output,"total_tokens":total})
        }
        (None, None, None) => Value::Null,
        _ => {
            return error(
                StatusCode::BAD_GATEWAY,
                "invalid_upstream_usage",
                "Inconsistent upstream token usage",
            );
        }
    };
    // Do not translate a multi-choice Chat Completion as one Responses output.
    let Some(choices) = chat.get("choices").and_then(Value::as_array) else {
        return error(
            StatusCode::BAD_GATEWAY,
            "invalid_chat_choices",
            "Upstream chat choices must be an array",
        );
    };
    if choices.len() != 1
        || choices[0]["index"].as_u64() != Some(0)
        || choices[0]["message"]["role"] != "assistant"
    {
        return error(
            StatusCode::BAD_GATEWAY,
            "invalid_chat_choices",
            "Responses requires one assistant choice with index zero",
        );
    }
    let Some(response_id) = chat
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
    else {
        return error(
            StatusCode::BAD_GATEWAY,
            "invalid_chat_id",
            "Upstream chat response id is missing",
        );
    };
    if chat
        .get("created")
        .is_some_and(|created| created.as_i64().is_none_or(|timestamp| timestamp < 0))
    {
        return error(
            StatusCode::BAD_GATEWAY,
            "invalid_chat_created",
            "Upstream created timestamp must be a nonnegative integer",
        );
    }
    if chat
        .get("model")
        .is_some_and(|model| model.as_str().is_none_or(str::is_empty))
    {
        return error(
            StatusCode::BAD_GATEWAY,
            "invalid_chat_model",
            "Upstream chat model must be a nonempty string",
        );
    }
    // OpenAI-compatible Chat providers may omit their optional timestamp.
    // The Responses wire contract still requires a numeric created_at, just
    // like our streaming adapter. Preserve a valid upstream value or stamp
    // the conversion locally rather than emitting created_at:null.
    let created_at = chat["created"].as_i64().unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .min(i64::MAX as u64) as i64
    });
    let message = &chat["choices"][0]["message"];
    let reasoning = match reasoning_text(message) {
        Ok(reasoning) => reasoning,
        Err(()) => {
            return error(
                StatusCode::BAD_GATEWAY,
                "invalid_chat_reasoning",
                "Upstream reasoning fields must contain text",
            );
        }
    };
    let calls = message["tool_calls"].as_array();
    if chat["choices"][0]["finish_reason"] == "tool_calls" && calls.is_none_or(Vec::is_empty) {
        return error(
            StatusCode::BAD_GATEWAY,
            "missing_tool_calls",
            "Tool finish reason requires nonempty tool_calls",
        );
    }
    if chat["choices"][0]["message"]
        .get("tool_calls")
        .is_some_and(|value| !value.is_array())
    {
        return error(
            StatusCode::BAD_GATEWAY,
            "invalid_tool_calls",
            "tool_calls must be an array",
        );
    }
    if calls.is_some_and(|items| !items.is_empty()) {
        let finish_reason = chat["choices"][0]["finish_reason"].as_str();
        let (output_status, incomplete_details) = match finish_reason {
            Some("tool_calls") => ("completed", Value::Null),
            Some("length") => ("incomplete", json!({"reason":"max_output_tokens"})),
            Some("content_filter") => ("incomplete", json!({"reason":"content_filter"})),
            _ => {
                return error(
                    StatusCode::BAD_GATEWAY,
                    "invalid_tool_finish",
                    "Tool calls require tool_calls, length, or content_filter finish reason",
                );
            }
        };
        let mut output = Vec::with_capacity(calls.map_or(0, Vec::len) + 2);
        if let Some(reasoning) = reasoning.as_deref() {
            output.push(reasoning_item(response_id, reasoning, output_status));
        }
        let content = &message["content"];
        if let Some(text) = content.as_str().filter(|text| !text.is_empty()) {
            output.push(json!({"id":format!("msg_{}",response_id),"type":"message","status":output_status,"role":"assistant","content":[{"type":"output_text","text":text,"annotations":[]}]}));
        } else if !content.is_null() && content.as_str() != Some("") {
            return error(
                StatusCode::BAD_GATEWAY,
                "invalid_tool_content",
                "Tool call content must be text or null",
            );
        }
        let mut seen_ids = std::collections::HashSet::with_capacity(calls.map_or(0, Vec::len));
        for call in calls.expect("checked above") {
            let Some(id) = call["id"].as_str().filter(|id| !id.is_empty()) else {
                return error(
                    StatusCode::BAD_GATEWAY,
                    "invalid_tool_call",
                    "Missing tool call id",
                );
            };
            let Some(name) = call["function"]["name"]
                .as_str()
                .filter(|name| !name.is_empty())
            else {
                return error(
                    StatusCode::BAD_GATEWAY,
                    "invalid_tool_call",
                    "Missing function name",
                );
            };
            let Some(arguments) = call["function"]["arguments"].as_str() else {
                return error(
                    StatusCode::BAD_GATEWAY,
                    "invalid_tool_call",
                    "Missing function arguments",
                );
            };
            if call["type"] != "function"
                || (output_status == "completed"
                    && !arguments.is_empty()
                    && !serde_json::from_str::<Value>(arguments)
                        .is_ok_and(|value| value.is_object()))
                || !seen_ids.insert(id)
            {
                return error(
                    StatusCode::BAD_GATEWAY,
                    "invalid_tool_call",
                    "Invalid or duplicate function call",
                );
            }
            output.push(json!({"type":"function_call","id":id,"call_id":id,"name":name,"arguments":if arguments.is_empty() && output_status == "completed" {"{}"} else {arguments},"status":output_status}));
        }
        return (StatusCode::OK, Json(json!({
            "id":chat["id"], "object":"response", "status":output_status, "model":logical_model,
            "output":output, "output_text":content.as_str().unwrap_or(""), "usage":translated_usage, "created_at":created_at, "incomplete_details":incomplete_details, "metadata":metadata,
        }))).into_response();
    }
    let content_value = &message["content"];
    let refusal = message
        .get("refusal")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    let content = match content_value.as_str() {
        Some(content) => content,
        None if content_value.is_null() && (reasoning.is_some() || refusal.is_some()) => "",
        _ => {
            return error(
                StatusCode::BAD_GATEWAY,
                "unsupported_chat_output",
                "Only text chat responses can be translated",
            );
        }
    };
    // A truncated chat completion must never be reported as a completed Response.
    let finish_reason = chat["choices"][0]["finish_reason"].as_str();
    let (response_status, incomplete_details) = match finish_reason {
        Some("stop") => ("completed", Value::Null),
        Some("length") => ("incomplete", json!({"reason":"max_output_tokens"})),
        Some("content_filter") => ("incomplete", json!({"reason":"content_filter"})),
        _ => {
            return error(
                StatusCode::BAD_GATEWAY,
                "unsupported_chat_finish",
                "Upstream completion did not finish normally",
            );
        }
    };
    let output_status = response_status;
    let mut output = Vec::with_capacity(2);
    if let Some(reasoning) = reasoning.as_deref() {
        output.push(reasoning_item(response_id, reasoning, output_status));
    }
    let mirrored_refusal = refusal.is_some_and(|value| value == content);
    let output_text = if mirrored_refusal { "" } else { content };
    let mut parts = Vec::with_capacity(2);
    if !output_text.is_empty() || (refusal.is_none() && reasoning.is_none()) {
        parts.push(json!({"type":"output_text","text":output_text,"annotations":[]}));
    }
    if let Some(refusal) = refusal {
        parts.push(json!({"type":"refusal","refusal":refusal}));
    }
    if !parts.is_empty() {
        output.push(
            json!({"id":format!("msg_{response_id}"), "type":"message", "status":output_status,
            "role":"assistant", "content":parts}),
        );
    }
    (StatusCode::OK, Json(json!({
        "id":chat["id"], "object":"response", "status":response_status, "incomplete_details":incomplete_details, "created_at":created_at, "model":logical_model,
        "output":output,
        "output_text":output_text,
        "usage": translated_usage, "metadata":metadata
    }))).into_response()
}

#[cfg(test)]
mod reasoning_tests {
    use super::*;

    #[tokio::test]
    async fn responses_created_at_is_numeric_and_preserves_upstream_timestamps() {
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let template = json!({
            "id":"chatcmpl-timestamp","model":"physical",
            "choices":[{"index":0,"message":{"role":"assistant","content":"ok"},
                "finish_reason":"stop"}]
        });
        for (created, expected) in [(None, None), (Some(json!(123)), Some(123_i64))] {
            let mut chat = template.clone();
            if let Some(created) = created {
                chat["created"] = created;
            }
            let response = translate_chat_result(chat, "logical".into(), Value::Null);
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
            let output: Value = serde_json::from_slice(&body).unwrap();
            let created = output["created_at"].as_i64().expect("created_at integer");
            if let Some(expected) = expected {
                assert_eq!(created, expected);
            } else {
                let after = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                assert!((before..=after).contains(&created));
            }
        }
        for bad in [json!(-1), json!("yesterday"), json!(null), json!(1.5)] {
            let mut invalid = template.clone();
            invalid["created"] = bad;
            let response = translate_chat_result(invalid, "logical".into(), Value::Null);
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        }
    }

    #[tokio::test]
    async fn standalone_mirrored_and_mixed_chat_refusals_are_typed_responses_parts() {
        for (content, refusal, expected_text, expected_parts) in [
            (
                Value::Null,
                "Cannot help",
                "",
                json!([{"type":"refusal","refusal":"Cannot help"}]),
            ),
            (
                json!("Cannot help"),
                "Cannot help",
                "",
                json!([{"type":"refusal","refusal":"Cannot help"}]),
            ),
            (
                json!("Here is safe information"),
                "Cannot do the unsafe request",
                "Here is safe information",
                json!([
                    {"type":"output_text","text":"Here is safe information","annotations":[]},
                    {"type":"refusal","refusal":"Cannot do the unsafe request"}
                ]),
            ),
        ] {
            let response = translate_chat_result(
                json!({
                    "id":"chatcmpl-refusal",
                    "choices":[{"index":0,
                        "message":{"role":"assistant","content":content,"refusal":refusal},
                        "finish_reason":"stop"}]
                }),
                "logical".into(),
                Value::Null,
            );
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
            let result: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(result["output_text"], expected_text);
            assert_eq!(result["output"][0]["content"], expected_parts);
            let replayed = translate_input(json!([result["output"][0].clone()])).unwrap();
            let expected_replay = format!("{expected_text}{refusal}");
            assert_eq!(replayed[0]["content"], expected_replay);
        }
        for role in ["user", "system"] {
            assert!(
                translate_input(json!([{
                    "type":"message","role":role,
                    "content":[{"type":"refusal","refusal":"no"}]
                }]))
                .is_err()
            );
        }
        assert!(
            translate_input(json!([{
                "type":"message","role":"assistant",
                "content":[{"type":"refusal","refusal":42}]
            }]))
            .is_err()
        );
    }

    #[test]
    fn nonstream_reasoning_prefers_arrival_order_and_deduplicates_mirrors() {
        let ordered = json!({"reasoning":"continues",
            "reasoning_content":"thinking ",
            "reasoning_ordered":"thinking continues"});
        assert_eq!(
            reasoning_text(&ordered).unwrap(),
            Some("thinking continues".into())
        );
        let mirrored = json!({"reasoning":"I think","reasoning_content":"I think"});
        assert_eq!(reasoning_text(&mirrored).unwrap(), Some("I think".into()));
    }

    #[tokio::test]
    async fn nonstream_reasoning_fields_are_preserved_in_responses_output() {
        for field in ["reasoning_content", "reasoning"] {
            let mut message = json!({"role":"assistant","content":"answer"});
            message[field] = json!("upstream reasoning");
            let chat = json!({
                "id":"chatcmpl-reasoning","created":123,"model":"provider-model",
                "usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3},
                "choices":[{"index":0,"message":message,"finish_reason":"stop"}]
            });
            let response = translate_chat_result(chat, "logical-model".into(), json!({}));
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
            let value: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["output"][0]["type"], "reasoning");
            assert_eq!(value["output"][0]["content"][0]["type"], "reasoning_text");
            assert_eq!(
                value["output"][0]["content"][0]["text"],
                "upstream reasoning"
            );
            assert_eq!(value["output"][1]["type"], "message");
            assert_eq!(value["output_text"], "answer");
        }
    }

    #[tokio::test]
    async fn content_filter_is_incomplete_in_nonstream_mode_like_streaming() {
        let chat = json!({
            "id":"chatcmpl-filtered","created":123,"model":"provider-model",
            "choices":[{"index":0,"message":{"role":"assistant","content":"partial"},
                "finish_reason":"content_filter"}]
        });
        let response = translate_chat_result(chat, "logical-model".into(), Value::Null);
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["status"], "incomplete");
        assert_eq!(value["incomplete_details"]["reason"], "content_filter");
        assert_eq!(value["output"][0]["status"], "incomplete");
        assert_eq!(value["output_text"], "partial");
    }

    #[tokio::test]
    async fn incomplete_tool_calls_preserve_partial_arguments_without_claiming_completion() {
        for (finish_reason, incomplete_reason) in [
            ("length", "max_output_tokens"),
            ("content_filter", "content_filter"),
        ] {
            let partial = r#"{"query":"unfinished"#;
            let chat = json!({
                "id":"chatcmpl-partial","created":123,"model":"provider-model",
                "choices":[{"index":0,"message":{"role":"assistant","content":null,
                    "tool_calls":[{"id":"call_partial","type":"function",
                        "function":{"name":"lookup","arguments":partial}}]},
                    "finish_reason":finish_reason}]
            });
            let response = translate_chat_result(chat, "logical-model".into(), Value::Null);
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
            let value: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["status"], "incomplete");
            assert_eq!(value["incomplete_details"]["reason"], incomplete_reason);
            assert_eq!(value["output"][0]["type"], "function_call");
            assert_eq!(value["output"][0]["arguments"], partial);
            assert_eq!(value["output"][0]["status"], "incomplete");
        }
        let invalid_completed = json!({
            "id":"chatcmpl-broken","choices":[{"index":0,"message":{"role":"assistant","content":null,
                "tool_calls":[{"id":"call_bad","type":"function",
                    "function":{"name":"lookup","arguments":"{"}}]},
                "finish_reason":"tool_calls"}]
        });
        let response = translate_chat_result(invalid_completed, "model".into(), Value::Null);
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn completed_responses_messages_with_metadata_replay_as_chat_input() {
        let input = json!([
            {"type":"message","id":"msg_123","status":"completed","role":"assistant",
                "content":[{"type":"output_text","text":"hello","annotations":[]}]},
            {"type":"message","role":"user","content":[{"type":"input_text","text":"next"}]}
        ]);
        assert_eq!(
            translate_input(input).unwrap(),
            json!([{"role":"assistant","content":"hello"},{"role":"user","content":"next"}])
        );
        assert!(
            translate_input(json!([{"type":"message","role":"assistant",
            "status":"in_progress","content":"untrusted"}]))
            .is_err()
        );
        assert!(
            translate_input(json!([{"type":"message","role":"assistant",
            "content":[{"type":"output_text","text":"hi","annotations":[{"type":"citation"}]}]}]))
            .is_err()
        );
    }

    #[tokio::test]
    async fn reasoning_only_response_can_have_null_visible_content() {
        let chat = json!({
            "id":"chatcmpl-reasoning-only","created":123,"model":"provider-model",
            "choices":[{"index":0,"message":{"role":"assistant","content":null,
                "reasoning_content":"upstream reasoning"},"finish_reason":"stop"}]
        });
        let response = translate_chat_result(chat, "logical-model".into(), json!({}));
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["output"].as_array().unwrap().len(), 1);
        assert_eq!(value["output"][0]["type"], "reasoning");
        assert_eq!(value["output_text"], "");
    }

    #[tokio::test]
    async fn completed_reasoning_and_text_replay_preserves_both() {
        let chat = json!({"id":"r1","model":"upstream","choices":[{"index":0,
            "message":{"role":"assistant","reasoning_content":"thinking","content":"answer"},
            "finish_reason":"stop"}]});
        let response = translate_chat_result(chat, "logical".into(), Value::Null);
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        let output: Value = serde_json::from_slice(&bytes).unwrap();
        let mut history = output["output"].as_array().unwrap().clone();
        history.push(json!({"role":"user","content":"continue"}));
        assert_eq!(
            translate_input(Value::Array(history)).unwrap(),
            json!([
                {"role":"assistant","content":"answer","reasoning_content":"thinking"},
                {"role":"user","content":"continue"}
            ])
        );
    }

    #[test]
    fn reasoning_only_and_tool_call_replay_rejects_unknown_or_incomplete_items() {
        assert_eq!(
            translate_input(json!([reasoning_item("r", "thought", "completed")])).unwrap(),
            json!([{"role":"assistant","content":null,"reasoning_content":"thought"}])
        );
        let history = json!([
            reasoning_item("t", "why tool", "completed"),
            {"type":"function_call","id":"call_a","call_id":"call_a",
                "name":"lookup","arguments":"{}","status":"completed"},
            {"type":"function_call_output","call_id":"call_a","output":"found"}
        ]);
        assert_eq!(
            translate_input(history).unwrap(),
            json!([
                {"role":"assistant","content":null,"reasoning_content":"why tool",
                    "tool_calls":[{"id":"call_a","type":"function",
                        "function":{"name":"lookup","arguments":"{}"}}]},
                {"role":"tool","tool_call_id":"call_a","content":"found"}
            ])
        );
        for invalid in [
            reasoning_item("bad", "truncated", "incomplete"),
            json!({"type":"reasoning","id":"r","status":"completed",
                "summary":[{"text":"cannot discard"}],
                "content":[{"type":"reasoning_text","text":"thought"}]}),
            json!({"type":"reasoning","id":"r","status":"completed","summary":[],
                "content":[{"type":"reasoning_text","text":"thought","unknown":"field"}]}),
        ] {
            assert!(translate_input(json!([invalid])).is_err());
        }
    }

    #[test]
    fn both_reasoning_aliases_are_preserved_in_order() {
        let message = json!({"reasoning":"first ","reasoning_content":"second"});
        assert_eq!(
            reasoning_text(&message).unwrap().as_deref(),
            Some("first second")
        );
        let invalid = json!({"reasoning":["invalid"]});
        assert!(reasoning_text(&invalid).is_err());
    }
}
