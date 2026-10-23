//! OpenAI Responses compatibility adapter.
//! Request validation, request translation, chat-result translation and
//! Responses SSE wrapping live here. Dispatch still uses one shared
//! candidate/failover loop in dispatch.rs; this module never retries upstream.
use super::dispatch::{Endpoint, dispatch};
use super::{AppState, MAX_BODY_BYTES, authenticated_body, error};
use axum::{
    Json,
    body::{Body, Bytes, to_bytes},
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::sync::Arc;

// Strict stateless Responses compatibility adapter; unsupported modes fail closed.
pub(super) async fn handle(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (body, permit) = match authenticated_body(&state, request).await {
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
    // Translate the common Responses text message shape into Chat Completions.
    // Reject non-text content rather than silently discarding image/tool data.
    let messages = match input {
        Value::String(text) => json!([{"role":"user","content":text}]),
        Value::Array(items) if !items.is_empty() => {
            let mut messages = Vec::with_capacity(items.len());
            let mut pending_calls = std::collections::HashSet::<String>::new();
            let mut seen_calls = std::collections::HashSet::<String>::new();
            for item in items {
                if item.get("type").and_then(Value::as_str) == Some("function_call_output") {
                    let Some(call_id) = item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                    else {
                        return error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_output",
                            "function_call_output requires call_id",
                        );
                    };
                    let Some(output) = item.get("output").and_then(Value::as_str) else {
                        return error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_output",
                            "function_call_output requires string output",
                        );
                    };
                    if item.as_object().is_none_or(|map| {
                        map.keys()
                            .any(|key| !["type", "call_id", "output"].contains(&key.as_str()))
                    }) {
                        return error(
                            StatusCode::NOT_IMPLEMENTED,
                            "function_output_unsupported",
                            "Unsupported function output fields",
                        );
                    }
                    if !pending_calls.remove(call_id) {
                        return error(
                            StatusCode::BAD_REQUEST,
                            "orphan_function_output",
                            "Function output has no matching preceding call",
                        );
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
                        return error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_call",
                            "function_call requires call_id",
                        );
                    };
                    let Some(name) = item
                        .get("name")
                        .and_then(Value::as_str)
                        .filter(|name| !name.is_empty())
                    else {
                        return error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_call",
                            "function_call requires name",
                        );
                    };
                    let Some(arguments) = item.get("arguments").and_then(Value::as_str) else {
                        return error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_call",
                            "function_call requires arguments",
                        );
                    };
                    if !serde_json::from_str::<Value>(arguments)
                        .is_ok_and(|value| value.is_object())
                        || item.as_object().is_none_or(|map| {
                            map.keys().any(|key| {
                                !["type", "call_id", "name", "arguments", "id", "status"]
                                    .contains(&key.as_str())
                            })
                        })
                    {
                        return error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_call",
                            "Invalid function call arguments or fields",
                        );
                    }
                    if item
                        .get("id")
                        .is_some_and(|v| v.as_str().is_none_or(str::is_empty))
                        || item.get("status").is_some_and(|v| v != "completed")
                    {
                        return error(
                            StatusCode::BAD_REQUEST,
                            "invalid_function_call",
                            "Function call id and status must be valid",
                        );
                    }
                    if !seen_calls.insert(call_id.to_owned())
                        || !pending_calls.insert(call_id.to_owned())
                    {
                        return error(
                            StatusCode::BAD_REQUEST,
                            "duplicate_function_call",
                            "Function call id is already pending",
                        );
                    }
                    let tool_call = json!({"id":call_id,"type":"function","function":{"name":name,"arguments":arguments}});
                    if let Some(last) = messages.last_mut().filter(|message: &&mut Value| {
                        message["role"] == "assistant" && message["tool_calls"].is_array()
                    }) {
                        last["tool_calls"]
                            .as_array_mut()
                            .expect("checked above")
                            .push(tool_call);
                    } else {
                        messages.push(json!({"role":"assistant","content":Value::Null,"tool_calls":[tool_call]}));
                    }
                    continue;
                }
                if !pending_calls.is_empty() {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "pending_function_calls",
                        "Function calls must be resolved before another message",
                    );
                }
                let Some(role) = item.get("role").and_then(Value::as_str) else {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "unsupported_input",
                        "Input messages require a role",
                    );
                };
                if !matches!(role, "user" | "assistant" | "system" | "developer")
                    || item.get("type").is_some_and(|kind| kind != "message")
                    || item.as_object().is_none_or(|obj| {
                        obj.keys()
                            .any(|k| !["role", "content", "type"].contains(&k.as_str()))
                    })
                {
                    return error(
                        StatusCode::NOT_IMPLEMENTED,
                        "responses_input_unsupported",
                        "Only plain text messages are supported",
                    );
                }
                let Some(content) = item.get("content") else {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "unsupported_input",
                        "Input messages require content",
                    );
                };
                let text = if let Some(text) = content.as_str() {
                    text.to_owned()
                } else if let Some(parts) = content.as_array() {
                    if parts.is_empty() {
                        return error(
                            StatusCode::BAD_REQUEST,
                            "unsupported_input",
                            "Empty content parts",
                        );
                    }
                    let mut text = String::new();
                    for part in parts {
                        if !matches!(
                            part.get("type").and_then(Value::as_str),
                            Some("input_text") | Some("output_text")
                        ) || part.as_object().is_none_or(|obj| obj.len() != 2)
                        {
                            return error(
                                StatusCode::NOT_IMPLEMENTED,
                                "responses_input_unsupported",
                                "Only input_text or output_text content parts are supported",
                            );
                        }
                        if part["type"] == "output_text" && role != "assistant" {
                            return error(
                                StatusCode::BAD_REQUEST,
                                "invalid_output_text_role",
                                "output_text parts require assistant role",
                            );
                        }
                        let Some(fragment) = part.get("text").and_then(Value::as_str) else {
                            return error(
                                StatusCode::BAD_REQUEST,
                                "unsupported_input",
                                "input_text requires text",
                            );
                        };
                        text.push_str(fragment);
                    }
                    text
                } else {
                    return error(
                        StatusCode::NOT_IMPLEMENTED,
                        "responses_input_unsupported",
                        "Unsupported message content",
                    );
                };
                messages.push(json!({"role":role,"content":text}));
            }
            if !pending_calls.is_empty() {
                return error(
                    StatusCode::BAD_REQUEST,
                    "pending_function_calls",
                    "Function calls require matching output before continuing",
                );
            }
            Value::Array(messages)
        }
        _ => {
            return error(
                StatusCode::BAD_REQUEST,
                "unsupported_input",
                "Only text or text message arrays are supported",
            );
        }
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
        "text",
        "modalities",
        "include",
        "service_tier",
        "safety_identifier",
        "prompt",
        "prompt_cache_key",
        "messages",
        "max_tokens",
        "n",
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
            "input",
            "stream",
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
    if let Some(tokens) = object.remove("max_output_tokens") {
        object.insert("max_tokens".into(), tokens);
    }
    let response_metrics = state.metrics.clone();
    let response = dispatch(
        state,
        Bytes::from(value.to_string()),
        Endpoint::Chat,
        permit,
    )
    .await;
    if response.status() != StatusCode::OK {
        return response;
    }
    if streaming {
        return crate::responses_sse::wrap(response, logical_model, metadata, response_metrics);
    }
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
    let created_at = chat["created"].as_i64();
    let calls = chat["choices"][0]["message"]["tool_calls"].as_array();
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
        if chat["choices"][0]["finish_reason"] != "tool_calls" {
            return error(
                StatusCode::BAD_GATEWAY,
                "invalid_tool_finish",
                "Tool calls require tool_calls finish reason",
            );
        }
        let mut output = Vec::with_capacity(calls.map_or(0, Vec::len) + 1);
        let content = &chat["choices"][0]["message"]["content"];
        if let Some(text) = content.as_str().filter(|text| !text.is_empty()) {
            output.push(json!({"id":format!("msg_{}",response_id),"type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":text,"annotations":[]}]}));
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
                || !serde_json::from_str::<Value>(arguments).is_ok_and(|value| value.is_object())
                || !seen_ids.insert(id)
            {
                return error(
                    StatusCode::BAD_GATEWAY,
                    "invalid_tool_call",
                    "Invalid or duplicate function call",
                );
            }
            output.push(json!({"type":"function_call","id":id,"call_id":id,"name":name,"arguments":arguments,"status":"completed"}));
        }
        return (StatusCode::OK, Json(json!({
            "id":chat["id"], "object":"response", "status":"completed", "model":logical_model,
            "output":output, "output_text":content.as_str().unwrap_or(""), "usage":translated_usage, "created_at":created_at, "incomplete_details":Value::Null, "metadata":metadata,
        }))).into_response();
    }
    let content = chat["choices"][0]["message"]["content"].as_str();
    let Some(content) = content else {
        return error(
            StatusCode::BAD_GATEWAY,
            "unsupported_chat_output",
            "Only text chat responses can be translated",
        );
    };
    // A truncated chat completion must never be reported as a completed Response.
    let finish_reason = chat["choices"][0]["finish_reason"].as_str();
    let (response_status, incomplete_details) = match finish_reason {
        Some("stop") => ("completed", Value::Null),
        Some("length") => ("incomplete", json!({"reason":"max_output_tokens"})),
        _ => {
            return error(
                StatusCode::BAD_GATEWAY,
                "unsupported_chat_finish",
                "Upstream completion did not finish normally",
            );
        }
    };
    let output_status = response_status;
    (StatusCode::OK, Json(json!({
        "id":chat["id"], "object":"response", "status":response_status, "incomplete_details":incomplete_details, "created_at":created_at, "model":logical_model,
        "output":[{"id":format!("msg_{}",response_id), "type":"message", "status":output_status, "role":"assistant", "content":[{"type":"output_text","text":content,"annotations":[]}]}],
        "output_text":content,
        "usage": translated_usage, "metadata":metadata
    }))).into_response()
}
