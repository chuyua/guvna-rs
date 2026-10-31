//! OpenAI Responses compatibility adapter.
//! Request validation, request translation, chat-result translation and
//! Responses SSE wrapping live here. Dispatch still uses one shared
//! candidate/failover loop in dispatch.rs; this module never retries upstream.
use super::chat_stream::NativeResponseStream;
use super::dispatch::{
    BufferedResponseLease, ParsedChatResponse, ParsedNativeResponse, dispatch_responses_parsed,
    hold_buffered_response,
};
use super::responses_format;
use super::{AppState, authenticated_body, error};
#[cfg(test)]
use axum::body::to_bytes;
use axum::{
    Json,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::sync::Arc;

// Strict stateless Responses compatibility adapter; unsupported modes fail closed.
pub(super) async fn handle(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let Some(owner) = state.client_identity(request.headers()) else {
        return super::unauthorized();
    };
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
    // Common schema checks run before choosing native passthrough as well as
    // the Chat bridge. Capability selection must not bypass caller validation.
    for key in ["stream", "store", "background", "parallel_tool_calls"] {
        if value
            .get(key)
            .is_some_and(|v| !v.is_null() && !v.is_boolean())
        {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_boolean_parameter",
                "Expected a boolean or null",
            );
        }
    }
    for (key, min, max) in [("temperature", 0.0, 2.0), ("top_p", 0.0, 1.0)] {
        if value.get(key).is_some_and(|v| {
            !v.is_null()
                && v.as_f64()
                    .is_none_or(|v| v < min || v > max || !v.is_finite())
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
        .is_some_and(|v| !v.is_null() && v.as_u64().is_none_or(|n| n < 16))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_max_output_tokens",
            "max_output_tokens must be an integer of at least 16",
        );
    }
    if value
        .get("top_logprobs")
        .is_some_and(|v| !v.is_null() && v.as_u64().is_none_or(|n| n > 20))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_top_logprobs",
            "top_logprobs must be between 0 and 20",
        );
    }
    if value
        .get("tools")
        .is_some_and(|v| !v.is_null() && !v.is_array())
        || value
            .get("input")
            .is_some_and(|v| !v.is_null() && !v.is_string() && !v.is_array())
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_responses_input",
            "Invalid Responses input or tools",
        );
    }
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

    if value
        .get("stream")
        .is_some_and(|v| !v.is_boolean() && !v.is_null())
    {
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
    // A native supplier may support tools, conversation state, and input item
    // kinds that cannot be represented safely by Chat Completions.
    let has_native = state
        .config
        .providers
        .iter()
        .any(|p| p.has_native_responses());
    let context = match super::responses_lifecycle::context(&state, owner, &value) {
        Ok(context) => context,
        Err(response) => return *response,
    };
    let bridge_fields = [
        "model",
        "n",
        "input",
        "stream",
        "text",
        "max_output_tokens",
        "temperature",
        "top_p",
        "user",
        "service_tier",
        "safety_identifier",
        "prompt_cache_key",
        "instructions",
        "reasoning",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
        "metadata",
        "store",
        "background",
        "truncation",
        "min_context",
        "max_context",
    ];
    let native_only =
        value
            .as_object()
            .expect("object")
            .iter()
            .any(|(key, value)| !value.is_null() && !bridge_fields.contains(&key.as_str()))
            || value.get("background") == Some(&Value::Bool(true))
            || value.get("store") == Some(&Value::Bool(true))
            || value
                .get("truncation")
                .is_some_and(|v| !v.is_null() && v != "disabled")
            || value
                .get("tools")
                .and_then(Value::as_array)
                .is_some_and(|tools| tools.iter().any(|tool| tool["type"] != "function"))
            || value
                .get("tool_choice")
                .and_then(Value::as_object)
                .is_some_and(|choice| choice.get("type").is_some_and(|v| v != "function"))
            || value
                .get("reasoning")
                .and_then(Value::as_object)
                .is_some_and(|options| options.keys().any(|key| key != "effort"))
            || value
                .get("text")
                .and_then(Value::as_object)
                .is_some_and(|text| {
                    text.keys()
                        .any(|key| !matches!(key.as_str(), "format" | "verbosity"))
                        || text
                            .get("format")
                            .and_then(Value::as_object)
                            .is_some_and(|format| {
                                format.keys().any(|key| {
                                    !["type", "name", "schema", "strict", "description"]
                                        .contains(&key.as_str())
                                }) || format.get("type").and_then(Value::as_str).is_some_and(
                                    |kind| !matches!(kind, "text" | "json_object" | "json_schema"),
                                )
                            })
                })
                && has_native
            || has_native
                && value
                    .get("tools")
                    .and_then(Value::as_array)
                    .is_some_and(|tools| {
                        tools.iter().any(|tool| {
                            tool.as_object().is_some_and(|fields| {
                                fields.keys().any(|key| {
                                    !["type", "name", "description", "parameters", "strict"]
                                        .contains(&key.as_str())
                                })
                            })
                        })
                    });
    if native_only {
        return handle_native_only(state, value, logical_model, large_payload, permit, context)
            .await;
    }
    // Only clone the original wire tree when a native supplier exists.
    let native_request = has_native.then(|| value.clone());
    // The Responses schema permits null for optional controls. On a Chat
    // bridge null means use the provider default, not send a null-valued Chat
    // option (which some Chat-only suppliers reject). Preserve the untouched
    // native request above for lossless native-provider fallback.
    for key in [
        "stream",
        "temperature",
        "top_p",
        "max_output_tokens",
        "instructions",
        "user",
        "safety_identifier",
        "prompt_cache_key",
        "service_tier",
        "parallel_tool_calls",
        "tools",
        "tool_choice",
        "background",
        "truncation",
        "previous_response_id",
        "conversation",
        "include",
        "top_logprobs",
    ] {
        if value.get(key).is_some_and(Value::is_null) {
            value.as_object_mut().expect("checked above").remove(key);
        }
    }
    // Input can be a multi-megabyte conversation. Move its JSON tree into the
    // translator instead of cloning the entire tree while the parsed request
    // still owns the original. The field is removed before upstream dispatch
    // either way, so this does not alter the wire contract.
    let Some(input) = value
        .as_object_mut()
        .expect("checked above")
        .remove("input")
    else {
        return error(
            StatusCode::BAD_REQUEST,
            "missing_input",
            "input is required",
        );
    };
    let messages = match translate_input(input) {
        Ok(messages) => messages,
        Err(response) => {
            if let Some(native) = native_request {
                return handle_native_only(
                    state,
                    native,
                    logical_model,
                    large_payload,
                    permit,
                    context,
                )
                .await;
            }
            return *response;
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
        .is_some_and(|v| v.as_u64().is_none_or(|n| n < 16))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_max_output_tokens",
            "max_output_tokens must be an integer of at least 16",
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
    // The official Responses reasoning.effort has an exact Chat equivalent.
    // Other reasoning settings (summaries, context, mode) do not: never drop
    // them silently or imply that this stateless adapter implements them.
    let reasoning_effort = match value.get("reasoning") {
        None | Some(Value::Null) => None,
        Some(Value::Object(fields)) if fields.keys().all(|key| key == "effort") => {
            match fields.get("effort") {
                None | Some(Value::Null) => None,
                Some(Value::String(effort))
                    if matches!(
                        effort.as_str(),
                        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
                    ) =>
                {
                    Some(effort.clone())
                }
                _ => {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "invalid_reasoning_effort",
                        "reasoning.effort must be a supported effort level",
                    );
                }
            }
        }
        Some(Value::Object(_)) => {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "responses_feature_unsupported",
                "Other reasoning options cannot be mapped to Chat Completions",
            );
        }
        Some(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_reasoning",
                "reasoning must be an object or null",
            );
        }
    };
    // These Responses request controls are also native Chat parameters.
    // Preserve them across the adapter (never impersonate an effective
    // service tier in the translated result).
    if value.get("service_tier").is_some_and(|tier| {
        !tier.is_null()
            && !matches!(
                tier.as_str(),
                Some("auto" | "default" | "flex" | "scale" | "priority" | "fast" | "ultrafast")
            )
    }) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_service_tier",
            "Unknown service_tier",
        );
    }
    if value
        .get("safety_identifier")
        .is_some_and(|id| !id.is_null() && id.as_str().is_none_or(|value| value.len() > 128))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_safety_identifier",
            "safety_identifier must be a string of at most 128 characters",
        );
    }
    if value
        .get("prompt_cache_key")
        .is_some_and(|key| !key.is_null() && !key.is_string())
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_prompt_cache_key",
            "prompt_cache_key must be a string",
        );
    }
    let object = value.as_object_mut().expect("checked above");
    for key in [
        "previous_response_id",
        "conversation",
        "modalities",
        "include",
        "prompt",
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
            "service_tier",
            "safety_identifier",
            "prompt_cache_key",
            "instructions",
            "reasoning",
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
    let mut text_config = object.remove("text");
    let verbosity = match responses_format::take_verbosity(&mut text_config) {
        Ok(verbosity) => verbosity,
        Err(problem) => return error(problem.0, problem.1, problem.2),
    };
    let mapped_text_format = match responses_format::translate_owned(text_config) {
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
    // An empty tools list is a valid official Responses request. Do not
    // forward it to providers that reject an otherwise unused Chat tools: [].
    if object
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        object.remove("tools");
    }
    if let Some(tools) = object.get_mut("tools") {
        let Some(items) = tools.as_array_mut() else {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_tools",
                "tools must be an array",
            );
        };
        for tool in items {
            let Some(map) = tool.as_object_mut() else {
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
            wrap_validated_function_tool(tool);
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
    for key in ["metadata", "store", "background", "truncation", "reasoning"] {
        object.remove(key);
    }
    if let Some(effort) = reasoning_effort {
        object.insert("reasoning_effort".into(), Value::String(effort));
    }
    object.remove("text");
    if let Some(format) = mapped_text_format {
        object.insert("response_format".into(), format);
    }
    if let Some(verbosity) = verbosity {
        object.insert("verbosity".into(), verbosity);
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
    let response = dispatch_responses_parsed(
        state,
        value,
        native_request,
        false,
        large_payload,
        permit,
        context,
    )
    .await;
    if response.status() != StatusCode::OK {
        return response;
    }
    if streaming {
        if response
            .extensions()
            .get::<NativeResponseStream>()
            .is_some()
        {
            // The native event stream is already in Responses wire format.
            return response;
        }
        return crate::responses_sse::wrap(
            response,
            logical_model,
            metadata,
            response_metrics,
            json_mode,
        );
    }
    let mut response = response;
    let memory_lease = response
        .extensions()
        .get::<BufferedResponseLease>()
        .cloned();
    if let Some(ParsedNativeResponse(mut native)) =
        response.extensions_mut().remove::<ParsedNativeResponse>()
    {
        native["model"] = Value::String(logical_model);
        return hold_buffered_response(
            (StatusCode::OK, Json(native)).into_response(),
            memory_lease,
        );
    }
    let Some(ParsedChatResponse(chat)) = response.extensions_mut().remove::<ParsedChatResponse>()
    else {
        return error(
            StatusCode::BAD_GATEWAY,
            "invalid_upstream",
            "Missing validated upstream Chat response",
        );
    };
    // The original Chat body may have been drained, but its converted
    // Responses body can still be large and held by a slow TCP consumer.
    // Reuse that same lease rather than releasing it between translations.
    hold_buffered_response(
        translate_chat_result(chat, logical_model, metadata),
        memory_lease,
    )
}

/// Native-only capabilities keep their original request body. The minimal
/// Chat envelope is routing metadata only and is never sent to any supplier.
async fn handle_native_only(
    state: Arc<AppState>,
    raw: Value,
    logical_model: String,
    large_payload: bool,
    permit: tokio::sync::OwnedSemaphorePermit,
    context: super::response_routes::Context,
) -> Response {
    let streaming = raw.get("stream") == Some(&Value::Bool(true));
    let mut route = json!({
        "model":logical_model,
        "messages":[{"role":"user","content":""}],
        "n":1,
        "stream":streaming,
    });
    for key in ["min_context", "max_context"] {
        if let Some(value) = raw.get(key) {
            route[key] = value.clone();
        }
    }
    let response = dispatch_responses_parsed(
        state,
        route,
        Some(raw),
        true,
        large_payload,
        permit,
        context,
    )
    .await;
    if response.status() != StatusCode::OK {
        return response;
    }
    if streaming {
        if response
            .extensions()
            .get::<NativeResponseStream>()
            .is_some()
        {
            return response;
        }
        return error(
            StatusCode::BAD_GATEWAY,
            "invalid_native_stream",
            "Native-only request did not return a native Responses stream",
        );
    }
    let mut response = response;
    let lease = response
        .extensions()
        .get::<BufferedResponseLease>()
        .cloned();
    if let Some(ParsedNativeResponse(mut result)) =
        response.extensions_mut().remove::<ParsedNativeResponse>()
    {
        result["model"] = Value::String(logical_model);
        return hold_buffered_response((StatusCode::OK, Json(result)).into_response(), lease);
    }
    error(
        StatusCode::BAD_GATEWAY,
        "invalid_native_response",
        "Native-only request did not return a native Responses result",
    )
}

/// Called only after validating a Responses function declaration. Keep its
/// nested parameters tree in the same allocation when wrapping for Chat.
fn wrap_validated_function_tool(tool: &mut Value) {
    let map = tool.as_object_mut().expect("validated above");
    let mut function = std::mem::take(map);
    function.remove("type");
    let mut wrapped = serde_json::Map::new();
    wrapped.insert("type".into(), Value::String("function".into()));
    wrapped.insert("function".into(), Value::Object(function));
    *tool = Value::Object(wrapped);
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
fn chat_text_message(role: &str, content: String) -> Value {
    let mut message = json!({"role":role,"content":Value::Null});
    // json! borrows owned Strings and allocates a second copy. Transfer the
    // already-parsed text into the Chat envelope directly.
    message["content"] = Value::String(content);
    message
}

// Map user multimedia input to Chat parts by moving large base64 payloads.
// Reject any item without an equivalent Chat representation.
fn translate_multimodal_parts(parts: Vec<Value>) -> Result<Value, Box<Response>> {
    let mut output = Vec::with_capacity(parts.len());
    let mut text = String::new();
    for mut part in parts {
        match part.get("type").and_then(Value::as_str) {
            Some("input_text") => {
                if part.as_object().is_none_or(|obj| {
                    obj.keys()
                        .any(|key| !["type", "text", "annotations"].contains(&key.as_str()))
                        || obj
                            .get("annotations")
                            .is_some_and(|v| v.as_array().is_none_or(|a| !a.is_empty()))
                }) {
                    return Err(Box::new(error(
                        StatusCode::NOT_IMPLEMENTED,
                        "responses_input_unsupported",
                        "Unsupported text part fields",
                    )));
                }
                let Some(Value::String(fragment)) =
                    part.as_object_mut().expect("validated part").remove("text")
                else {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "unsupported_input",
                        "input_text requires text",
                    )));
                };
                if text.is_empty() {
                    text = fragment;
                } else {
                    text.push_str(&fragment);
                }
            }
            Some("input_image") => {
                if part.as_object().is_none_or(|obj| {
                    obj.keys()
                        .any(|key| !["type", "image_url", "detail"].contains(&key.as_str()))
                }) {
                    return Err(Box::new(error(
                        StatusCode::NOT_IMPLEMENTED,
                        "responses_input_unsupported",
                        "Unsupported image fields",
                    )));
                }
                let obj = part.as_object_mut().expect("validated part");
                let mut url = match obj.remove("image_url") {
                    Some(Value::String(s)) if !s.is_empty() => {
                        let mut map = serde_json::Map::new();
                        map.insert("url".into(), Value::String(s));
                        map
                    }
                    Some(Value::Object(map))
                        if map
                            .get("url")
                            .and_then(Value::as_str)
                            .is_some_and(|s| !s.is_empty())
                            && map
                                .keys()
                                .all(|key| ["url", "detail"].contains(&key.as_str())) =>
                    {
                        map
                    }
                    _ => {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "invalid_image_url",
                            "input_image requires image_url",
                        )));
                    }
                };
                if let Some(detail) = obj.remove("detail") {
                    url.insert("detail".into(), detail);
                }
                if url.get("detail").is_some_and(|v| {
                    !matches!(v.as_str(), Some("auto" | "low" | "high" | "original"))
                }) {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "invalid_image_detail",
                        "Unknown image detail level",
                    )));
                }
                if !text.is_empty() {
                    let mut item = serde_json::Map::new();
                    item.insert("type".into(), Value::String("text".into()));
                    item.insert("text".into(), Value::String(std::mem::take(&mut text)));
                    output.push(Value::Object(item));
                }
                let mut item = serde_json::Map::new();
                item.insert("type".into(), Value::String("image_url".into()));
                item.insert("image_url".into(), Value::Object(url));
                output.push(Value::Object(item));
            }
            Some("input_audio") => {
                if part.as_object().is_none_or(|obj| {
                    obj.keys()
                        .any(|key| !["type", "input_audio"].contains(&key.as_str()))
                }) {
                    return Err(Box::new(error(
                        StatusCode::NOT_IMPLEMENTED,
                        "responses_input_unsupported",
                        "Unsupported input_audio fields",
                    )));
                }
                let Some(Value::Object(audio)) = part
                    .as_object_mut()
                    .expect("validated part")
                    .remove("input_audio")
                else {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "invalid_input_audio",
                        "input_audio requires data and format",
                    )));
                };
                if !audio
                    .keys()
                    .all(|key| ["data", "format"].contains(&key.as_str()))
                    || !audio
                        .get("data")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.is_empty())
                    || !matches!(
                        audio.get("format").and_then(Value::as_str),
                        Some("mp3" | "wav")
                    )
                {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "invalid_input_audio",
                        "input_audio needs nonempty data and mp3/wav format",
                    )));
                }
                if !text.is_empty() {
                    let mut item = serde_json::Map::new();
                    item.insert("type".into(), Value::String("text".into()));
                    item.insert("text".into(), Value::String(std::mem::take(&mut text)));
                    output.push(Value::Object(item));
                }
                let mut item = serde_json::Map::new();
                item.insert("type".into(), Value::String("input_audio".into()));
                item.insert("input_audio".into(), Value::Object(audio));
                output.push(Value::Object(item));
            }
            Some("input_file") => {
                // Only inline PDF data is shared between Responses and Chat.
                // File IDs are provider-specific; file URLs and detail levels
                // have no lossless Chat counterpart.
                if part.as_object().is_none_or(|obj| {
                    obj.keys()
                        .any(|key| !["type", "filename", "file_data"].contains(&key.as_str()))
                }) {
                    return Err(Box::new(error(
                        StatusCode::NOT_IMPLEMENTED,
                        "responses_input_unsupported",
                        "Only inline PDF without detail can be mapped to Chat",
                    )));
                }
                let obj = part.as_object_mut().expect("validated part");
                let Some(Value::String(name)) = obj.remove("filename") else {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "invalid_input_file",
                        "PDF file requires filename",
                    )));
                };
                if !name.to_ascii_lowercase().ends_with(".pdf") {
                    return Err(Box::new(error(
                        StatusCode::NOT_IMPLEMENTED,
                        "responses_input_unsupported",
                        "Only PDF files are supported",
                    )));
                }
                let Some(Value::String(data)) = obj.remove("file_data") else {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "invalid_input_file",
                        "PDF file requires file_data",
                    )));
                };
                if !data.starts_with("data:application/pdf;base64,")
                    || data.len() <= "data:application/pdf;base64,".len()
                {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "invalid_input_file",
                        "PDF file_data must be a PDF base64 data URI",
                    )));
                }
                if !text.is_empty() {
                    let mut item = serde_json::Map::new();
                    item.insert("type".into(), Value::String("text".into()));
                    item.insert("text".into(), Value::String(std::mem::take(&mut text)));
                    output.push(Value::Object(item));
                }
                let mut file = serde_json::Map::new();
                file.insert("filename".into(), Value::String(name));
                file.insert("file_data".into(), Value::String(data));
                let mut item = serde_json::Map::new();
                item.insert("type".into(), Value::String("file".into()));
                item.insert("file".into(), Value::Object(file));
                output.push(Value::Object(item));
            }
            _ => {
                return Err(Box::new(error(
                    StatusCode::NOT_IMPLEMENTED,
                    "responses_input_unsupported",
                    "Unsupported multimodal content part",
                )));
            }
        }
    }
    if !text.is_empty() {
        let mut item = serde_json::Map::new();
        item.insert("type".into(), Value::String("text".into()));
        item.insert("text".into(), Value::String(text));
        output.push(Value::Object(item));
    }
    Ok(Value::Array(output))
}

fn translate_input(input: Value) -> Result<Value, Box<Response>> {
    // Translate the common Responses text message shape into Chat Completions.
    // Preserve supported image and audio input where Chat has an equivalent.
    let messages = match input {
        Value::String(text) => Value::Array(vec![chat_text_message("user", text)]),
        Value::Array(items) if !items.is_empty() => {
            let mut messages = Vec::with_capacity(items.len());
            let mut pending_calls = std::collections::HashSet::<String>::new();
            let mut seen_calls = std::collections::HashSet::<String>::new();
            let mut pending_reasoning: Option<String> = None;
            for mut item in items {
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
                    // Validation above guarantees a single reasoning_text
                    // string. Move it from the parsed history instead of
                    // retaining and copying a large reasoning item.
                    let mut content = item
                        .as_object_mut()
                        .expect("reasoning shape validated")
                        .remove("content")
                        .expect("reasoning content validated");
                    pending_reasoning = content[0]
                        .as_object_mut()
                        .expect("reasoning part validated")
                        .remove("text")
                        .and_then(|value| match value {
                            Value::String(text) => Some(text),
                            _ => None,
                        });
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
                    let Some(_output) = item.get("output").and_then(Value::as_str) else {
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
                    let mut tool_message =
                        json!({"role":"tool","tool_call_id":call_id,"content":Value::Null});
                    tool_message["content"] = item
                        .as_object_mut()
                        .expect("function output validated")
                        .remove("output")
                        .expect("function output validated");
                    messages.push(tool_message);
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
                    if !crate::sse::valid_json_output(arguments, true)
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
                    let mut tool_call = json!({"id":call_id,"type":"function",
                        "function":{"name":name,"arguments":Value::Null}});
                    tool_call["function"]["arguments"] = item
                        .as_object_mut()
                        .expect("function call validated")
                        .remove("arguments")
                        .expect("function arguments validated");
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
                        let mut assistant =
                            json!({"role":"assistant","content":Value::Null,"tool_calls":[]});
                        assistant["tool_calls"] = Value::Array(vec![tool_call]);
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
                let Some(role) = item.get("role").and_then(Value::as_str).map(str::to_owned) else {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "unsupported_input",
                        "Input messages require a role",
                    )));
                };
                if !matches!(role.as_str(), "user" | "assistant" | "system" | "developer")
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
                let Some(content) = item
                    .as_object_mut()
                    .expect("validated message object")
                    .remove("content")
                else {
                    return Err(Box::new(error(
                        StatusCode::BAD_REQUEST,
                        "unsupported_input",
                        "Input messages require content",
                    )));
                };
                if content.as_array().is_some_and(|parts| {
                    parts.iter().any(|part| {
                        matches!(
                            part.get("type").and_then(Value::as_str),
                            Some("input_image" | "input_audio" | "input_file")
                        )
                    })
                }) {
                    if role != "user" {
                        return Err(Box::new(error(
                            StatusCode::NOT_IMPLEMENTED,
                            "responses_input_unsupported",
                            "Only users may submit multimedia input",
                        )));
                    }
                    let Value::Array(parts) = content else {
                        unreachable!()
                    };
                    let chat_content = translate_multimodal_parts(parts)?;
                    if let Some(reasoning) = pending_reasoning.take() {
                        let mut assistant = json!({"role":"assistant","content":Value::Null});
                        assistant["reasoning_content"] = Value::String(reasoning);
                        messages.push(assistant);
                    }
                    let mut message = json!({"role":role,"content":Value::Null});
                    message["content"] = chat_content;
                    messages.push(message);
                    continue;
                }
                let text = if let Value::String(text) = content {
                    text
                } else if let Value::Array(parts) = content {
                    if parts.is_empty() {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "unsupported_input",
                            "Empty content parts",
                        )));
                    }
                    let single_part = parts.len() == 1;
                    let mut text = String::new();
                    for mut part in parts {
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
                            if single_part {
                                text = match part
                                    .as_object_mut()
                                    .expect("refusal part validated")
                                    .remove("refusal")
                                {
                                    Some(Value::String(content)) => content,
                                    _ => unreachable!("refusal text validated"),
                                };
                            } else {
                                text.push_str(fragment);
                            }
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
                        if single_part {
                            text = match part
                                .as_object_mut()
                                .expect("text part validated")
                                .remove("text")
                            {
                                Some(Value::String(content)) => content,
                                _ => unreachable!("text content validated"),
                            };
                        } else {
                            text.push_str(fragment);
                        }
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
                    let mut assistant = json!({"role":"assistant","content":Value::Null});
                    assistant["reasoning_content"] = Value::String(reasoning);
                    messages.push(assistant);
                }
                let mut message = chat_text_message(&role, text);
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
                let mut assistant = json!({"role":"assistant","content":Value::Null});
                assistant["reasoning_content"] = Value::String(reasoning);
                messages.push(assistant);
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

// Validate reasoning without allocating a second copy of the possibly huge
// upstream string. Only the final Responses conversion needs an owned value.
fn reasoning_fragments(message: &Value) -> Result<(Option<&str>, Option<&str>), ()> {
    if let Some(ordered) = message
        .get("reasoning_ordered")
        .filter(|value| !value.is_null())
    {
        let value = ordered.as_str().ok_or(())?;
        if !value.is_empty() {
            return Ok((Some(value), None));
        }
        // An empty hint is not a completed reasoning segment. Preserve
        // actual reasoning from the ordinary provider aliases below.
    }
    let read = |name: &str| match message.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_str().map(Some).ok_or(()),
    };
    let (left, right) = (read("reasoning")?, read("reasoning_content")?);
    // Providers sometimes mirror the same reasoning in both aliases. Never
    // concatenate it twice, in either validation or conversion.
    let right = if left == right { None } else { right };
    Ok((
        left.filter(|text| !text.is_empty()),
        right.filter(|text| !text.is_empty()),
    ))
}

// Convert one fully buffered Chat Completion into a non-streaming Responses
// document. This function performs no I/O, routing or retry decisions.
fn reasoning_text(message: &Value) -> Result<Option<String>, ()> {
    let (left, right) = reasoning_fragments(message)?;
    if left.is_none() && right.is_none() {
        return Ok(None);
    }
    let mut result = String::with_capacity(
        left.map_or(0, str::len)
            .saturating_add(right.map_or(0, str::len)),
    );
    if let Some(left) = left {
        result.push_str(left);
    }
    if let Some(right) = right {
        result.push_str(right);
    }
    Ok(Some(result))
}

fn reasoning_item(response_id: &str, text: impl Into<String>, status: &str) -> Value {
    // Build only the small envelope first. json! can clone owned Strings;
    // insert the large reasoning text directly to preserve its allocation.
    let mut item = json!({
        "id":format!("rs_{response_id}"),"type":"reasoning","status":status,
        "summary":[],"content":[{"type":"reasoning_text","text":Value::Null}]
    });
    item["content"][0]["text"] = Value::String(text.into());
    item
}

/// Missing/null usage is compatible with older Chat providers. A present
/// usage payload must never be silently discarded, including empty objects.
pub(super) fn parse_chat_usage(chat: &Value) -> Result<Option<(u64, u64, u64)>, ()> {
    let Some(usage) = chat.get("usage").filter(|usage| !usage.is_null()) else {
        return Ok(None);
    };
    let (Some(input), Some(output), Some(total)) = (
        usage.get("prompt_tokens").and_then(Value::as_u64),
        usage.get("completion_tokens").and_then(Value::as_u64),
        usage.get("total_tokens").and_then(Value::as_u64),
    ) else {
        return Err(());
    };
    if input.checked_add(output) != Some(total) {
        return Err(());
    }
    Ok(Some((input, output, total)))
}

/// Preserve upstream Chat token breakdowns in the Responses usage envelope.
/// Missing details stay missing; malformed details invalidate the candidate.
pub(super) fn translated_chat_usage(chat: &Value) -> Result<Value, ()> {
    let Some((input, output, total)) = parse_chat_usage(chat)? else {
        return Ok(Value::Null);
    };
    let usage = &chat["usage"];
    let mut translated = json!({
        "input_tokens":input,"output_tokens":output,"total_tokens":total
    });
    if let Some(details) = usage.get("prompt_tokens_details").filter(|v| !v.is_null()) {
        let map = details.as_object().ok_or(())?;
        if let Some(cached) = map.get("cached_tokens") {
            let tokens = cached
                .as_u64()
                .filter(|&tokens| tokens <= input)
                .ok_or(())?;
            translated["input_tokens_details"] = json!({"cached_tokens":tokens});
        }
    }
    if let Some(details) = usage
        .get("completion_tokens_details")
        .filter(|v| !v.is_null())
    {
        let map = details.as_object().ok_or(())?;
        if let Some(reasoning) = map.get("reasoning_tokens") {
            let tokens = reasoning
                .as_u64()
                .filter(|&tokens| tokens <= output)
                .ok_or(())?;
            translated["output_tokens_details"] = json!({"reasoning_tokens":tokens});
        }
    }
    Ok(translated)
}

/// Validate the endpoint-specific conversion contract *before* the shared
/// candidate walker commits its success, health and key-lease accounting.
/// This does not route/retry; it only rejects unusable Chat responses.
pub(super) fn translatable_chat_result(chat: &Value) -> bool {
    if translated_chat_usage(chat).is_err() {
        return false;
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
    let (reasoning_left, reasoning_right) = match reasoning_fragments(message) {
        Ok(fragments) => fragments,
        Err(()) => return false,
    };
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
                        || crate::sse::valid_json_output(args, true)
                })
        });
    }
    if choice["finish_reason"] == "tool_calls" {
        return false;
    }
    let supports_content = message["content"].as_str().is_some()
        || (message["content"].is_null()
            && ((reasoning_left.is_some() || reasoning_right.is_some())
                || message["refusal"].as_str().is_some()));
    supports_content
        && matches!(
            choice["finish_reason"].as_str(),
            Some("stop" | "length" | "content_filter")
        )
}

fn translate_chat_result(chat: Value, logical_model: String, metadata: Value) -> Response {
    // Validate and translate usage once for both text and function-call responses.
    let translated_usage = match translated_chat_usage(&chat) {
        Ok(usage) => usage,
        Err(()) => {
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
    let mut reasoning = match reasoning_text(message) {
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
        if let Some(reasoning) = reasoning.take() {
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
                    && !crate::sse::valid_json_output(arguments, true))
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
            "error":null, "store":false, "background":false,
            "previous_response_id":null, "truncation":"disabled",
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
    let had_reasoning = reasoning.is_some();
    if let Some(reasoning) = reasoning.take() {
        output.push(reasoning_item(response_id, reasoning, output_status));
    }
    let mirrored_refusal = refusal.is_some_and(|value| value == content);
    let output_text = if mirrored_refusal { "" } else { content };
    let mut parts = Vec::with_capacity(2);
    if !output_text.is_empty() || (refusal.is_none() && !had_reasoning) {
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
        "usage": translated_usage, "metadata":metadata,
        "error":null, "store":false, "background":false,
        "previous_response_id":null, "truncation":"disabled",
    }))).into_response()
}

#[cfg(test)]
mod reasoning_tests {
    use super::*;

    #[test]
    fn inline_pdf_moves_data_without_copying_and_rejects_unportable_files() {
        let pdf = format!(
            "data:application/pdf;base64,{}",
            "JVBERi0xLjQK".repeat(80_000)
        );
        let message = json!([{"role":"user","content":[
            {"type":"input_text","text":"Summarize "},
            {"type":"input_file","filename":"proposal.pdf","file_data":pdf},
            {"type":"input_text","text":"and compare"}
        ]}]);
        let pointer = message[0]["content"][1]["file_data"]
            .as_str()
            .unwrap()
            .as_ptr();
        let converted = translate_input(message).unwrap();
        assert_eq!(converted[0]["content"].as_array().unwrap().len(), 3);
        assert_eq!(
            converted[0]["content"][0],
            json!({"type":"text","text":"Summarize "})
        );
        assert_eq!(converted[0]["content"][1]["type"], "file");
        assert_eq!(
            converted[0]["content"][1]["file"]["filename"],
            "proposal.pdf"
        );
        assert_eq!(
            converted[0]["content"][1]["file"]["file_data"]
                .as_str()
                .unwrap()
                .as_ptr(),
            pointer
        );
        assert_eq!(
            converted[0]["content"][2],
            json!({"type":"text","text":"and compare"})
        );
        for invalid in [
            json!({"type":"input_file","file_id":"file_abc"}),
            json!({"type":"input_file","file_url":"https://example.test/a.pdf"}),
            json!({"type":"input_file","filename":"data.csv","file_data":"data:application/pdf;base64,YQ=="}),
            json!({"type":"input_file","filename":"a.pdf","file_data":""}),
            json!({"type":"input_file","filename":"a.pdf","file_data":"base64-raw"}),
            json!({"type":"input_file","filename":"a.pdf","file_data":"data:application/pdf;base64,YQ==","detail":"high"}),
        ] {
            assert!(translate_input(json!([{"role":"user","content":[invalid]}])).is_err());
        }
    }

    #[test]
    fn audio_input_moves_large_data_and_preserves_interleaving() {
        let data = "YQ==".repeat(180_000);
        let input = json!([{"role":"user","content":[
            {"type":"input_text","text":"listen "},
            {"type":"input_audio","input_audio":{"data":data,"format":"wav"}},
            {"type":"input_text","text":"then look "},
            {"type":"input_image","image_url":"https://example.test/a.png"}
        ]}]);
        let pointer = input[0]["content"][1]["input_audio"]["data"]
            .as_str()
            .unwrap()
            .as_ptr();
        let replay = translate_input(input).unwrap();
        let parts = &replay[0]["content"];
        assert_eq!(parts.as_array().unwrap().len(), 4);
        assert_eq!(parts[0], json!({"type":"text","text":"listen "}));
        assert_eq!(parts[1]["type"], "input_audio");
        assert_eq!(parts[1]["input_audio"]["format"], "wav");
        assert_eq!(
            parts[1]["input_audio"]["data"].as_str().unwrap().as_ptr(),
            pointer
        );
        assert_eq!(parts[2], json!({"type":"text","text":"then look "}));
        assert_eq!(parts[3]["type"], "image_url");
        for bad in [
            json!({"type":"input_audio","input_audio":{"data":"","format":"mp3"}}),
            json!({"type":"input_audio","input_audio":{"data":"YQ==","format":"flac"}}),
            json!({"type":"input_audio","input_audio":{"data":"YQ==","format":"wav","extra":1}}),
            json!({"type":"input_audio","input_audio":"wrong"}),
        ] {
            assert!(translate_input(json!([{"role":"user","content":[bad]}])).is_err());
        }
    }

    #[test]
    fn user_vision_parts_preserve_order_and_move_large_data_uri() {
        let data_uri = format!("data:image/png;base64,{}", "YQ==".repeat(180_000));
        let input = json!([{"role":"user","content":[
            {"type":"input_text","text":"What "},
            {"type":"input_text","text":"is in "},
            {"type":"input_image","image_url":"https://example.test/cat.png","detail":"original"},
            {"type":"input_text","text":"this and "},
            {"type":"input_image","image_url":{"url":data_uri,"detail":"low"}},
            {"type":"input_text","text":"that?"}
        ]}]);
        let old_ptr = input[0]["content"][4]["image_url"]["url"]
            .as_str()
            .unwrap()
            .as_ptr();
        let translated = translate_input(input).unwrap();
        let parts = &translated[0]["content"];
        assert_eq!(parts.as_array().unwrap().len(), 5);
        assert_eq!(parts[0], json!({"type":"text","text":"What is in "}));
        assert_eq!(
            parts[1],
            json!({"type":"image_url","image_url":{
                "url":"https://example.test/cat.png","detail":"original"
            }})
        );
        assert_eq!(parts[2], json!({"type":"text","text":"this and "}));
        assert_eq!(parts[3]["image_url"]["detail"], "low");
        assert_eq!(
            parts[3]["image_url"]["url"].as_str().unwrap().as_ptr(),
            old_ptr
        );
        assert_eq!(parts[4], json!({"type":"text","text":"that?"}));
    }

    #[test]
    fn unsupported_image_items_fail_closed_without_dropping_prompt_parts() {
        for content in [
            json!([{"type":"input_image","file_id":"file_123"}]),
            json!([{"type":"input_image","image_url":""}]),
            json!([{"type":"input_image","image_url":123}]),
            json!([{"type":"input_image","image_url":"https://example.test/a","detail":"giant"}]),
            json!([{"type":"input_image","image_url":{"url":"https://example.test/a","extra":1}}]),
            json!([{"type":"input_image","image_url":"https://example.test/a"},
                {"type":"input_file","file_id":"file_123"}]),
            json!([{"type":"input_image","image_url":"https://example.test/a"},
                {"type":"input_text","text":123}]),
            json!([{"type":"input_image","image_url":"https://example.test/a"},
                {"type":"output_text","text":"not user text"}]),
        ] {
            assert!(translate_input(json!([{"role":"user","content":content}])).is_err());
        }
        assert!(
            translate_input(json!([{"role":"assistant","content":[
                {"type":"input_image","image_url":"https://example.test/a"}
            ]}]))
            .is_err()
        );
    }

    #[test]
    fn simple_responses_text_inputs_retain_original_large_string_buffers() {
        let input = Value::String("prompt".repeat(256 * 1024));
        let initial_ptr = input.as_str().unwrap().as_ptr();
        let translated = translate_input(input).unwrap();
        assert_eq!(translated[0]["role"], "user");
        assert_eq!(
            translated[0]["content"].as_str().unwrap().as_ptr(),
            initial_ptr
        );
        assert_eq!(
            translated[0]["content"].as_str().unwrap().len(),
            6 * 256 * 1024
        );

        let input = json!([{"role":"user","content":"history".repeat(128 * 1024)},
            {"role":"assistant","content":"answer".repeat(128 * 1024)}]);
        let first = input[0]["content"].as_str().unwrap().as_ptr();
        let second = input[1]["content"].as_str().unwrap().as_ptr();
        let translated = translate_input(input).unwrap();
        assert_eq!(translated[0]["content"].as_str().unwrap().as_ptr(), first);
        assert_eq!(translated[1]["content"].as_str().unwrap().as_ptr(), second);
        assert_eq!(translated[0]["role"], "user");
        assert_eq!(translated[1]["role"], "assistant");
    }

    #[test]
    fn single_content_part_moves_validated_text_and_refusal_buffers() {
        for (role, part_type, field) in [
            ("user", "input_text", "text"),
            ("assistant", "output_text", "text"),
            ("assistant", "refusal", "refusal"),
        ] {
            let mut part = json!({"type":part_type});
            part[field] = Value::String("token".repeat(256 * 1024));
            let history = json!([{"role":role,"content":[part]}]);
            let source_ptr = history[0]["content"][0][field].as_str().unwrap().as_ptr();
            let replay = translate_input(history).unwrap();
            assert_eq!(replay[0]["content"].as_str().unwrap().as_ptr(), source_ptr);
            assert_eq!(replay[0]["role"], role);
        }

        let multipart = json!([{"role":"user","content":[
            {"type":"input_text","text":"first"},
            {"type":"input_text","text":" second"}
        ]}]);
        assert_eq!(
            translate_input(multipart).unwrap()[0]["content"],
            "first second"
        );
    }

    #[test]
    fn function_history_replay_moves_validated_large_arguments_and_outputs() {
        let arguments = format!(r#"{{"payload":"{}"}}"#, "a".repeat(512 * 1024));
        let output = "result".repeat(96 * 1024);
        let history = json!([
            {"type":"function_call","call_id":"c1","name":"search","arguments":arguments},
            {"type":"function_call_output","call_id":"c1","output":output},
        ]);
        let args_ptr = history[0]["arguments"].as_str().unwrap().as_ptr();
        let output_ptr = history[1]["output"].as_str().unwrap().as_ptr();
        let replay = translate_input(history).unwrap();
        assert_eq!(replay[0]["role"], "assistant");
        assert_eq!(replay[0]["tool_calls"][0]["function"]["name"], "search");
        assert_eq!(
            replay[0]["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .unwrap()
                .as_ptr(),
            args_ptr
        );
        assert_eq!(replay[1]["role"], "tool");
        assert_eq!(replay[1]["tool_call_id"], "c1");
        assert_eq!(replay[1]["content"].as_str().unwrap().as_ptr(), output_ptr);
    }

    #[test]
    fn completed_reasoning_replay_moves_large_text_without_an_extra_copy() {
        let reasoning = reasoning_item("large", "thinking".repeat(128 * 1024), "completed");
        let source_ptr = reasoning["content"][0]["text"].as_str().unwrap().as_ptr();
        let replay = translate_input(Value::Array(vec![reasoning])).unwrap();
        assert_eq!(replay[0]["role"], "assistant");
        assert_eq!(replay[0]["content"], Value::Null);
        assert_eq!(
            replay[0]["reasoning_content"].as_str().unwrap().as_ptr(),
            source_ptr
        );
    }

    #[test]
    fn function_tool_wrapper_preserves_large_schema_allocation() {
        let mut tool = json!({"type":"function","name":"search",
            "parameters":{"type":"object","description":"P".repeat(512 * 1024)}});
        let source_ptr = tool["parameters"]["description"].as_str().unwrap().as_ptr();
        wrap_validated_function_tool(&mut tool);
        let forwarded = tool["function"]["parameters"]["description"]
            .as_str()
            .unwrap();
        assert_eq!(forwarded.as_ptr(), source_ptr);
        assert_eq!(forwarded.len(), 512 * 1024);
        assert_eq!(tool["type"], "function");
        assert_eq!(tool["function"]["name"], "search");
    }

    #[test]
    fn large_reasoning_validation_borrows_upstream_text_without_copies() {
        let long = "r".repeat(1024 * 1024);
        let message = json!({
            "role":"assistant", "content":Value::Null,
            "reasoning":long, "reasoning_content":long
        });
        let (left, right) = reasoning_fragments(&message).unwrap();
        let upstream = message["reasoning"].as_str().unwrap();
        assert_eq!(left.unwrap().as_ptr(), upstream.as_ptr());
        assert_eq!(left.unwrap().len(), 1024 * 1024);
        assert!(right.is_none(), "mirrored reasoning must not be doubled");
        assert_eq!(
            reasoning_text(&message).unwrap().unwrap().len(),
            1024 * 1024
        );

        let malformed = json!({"reasoning_ordered":{"unexpected":"object"}});
        assert!(reasoning_fragments(&malformed).is_err());
        assert!(reasoning_text(&malformed).is_err());
        let separate = json!({"reasoning":"left","reasoning_content":"right"});
        assert_eq!(
            reasoning_text(&separate).unwrap().as_deref(),
            Some("leftright")
        );
        for empty in [json!(""), Value::Null] {
            let message = json!({
                "reasoning_ordered":empty, "reasoning":"actual",
                "reasoning_content":"actual"
            });
            assert_eq!(reasoning_text(&message).unwrap().as_deref(), Some("actual"));
        }
        assert_eq!(
            reasoning_text(&json!({"reasoning_ordered":""})).unwrap(),
            None
        );
    }

    #[test]
    fn reasoning_output_moves_large_owned_string_without_copying() {
        let reasoning = "deep".repeat(256 * 1024);
        let source_ptr = reasoning.as_ptr();
        let output = reasoning_item("allocated", reasoning, "completed");
        let forwarded = output["content"][0]["text"].as_str().unwrap();
        assert_eq!(forwarded.as_ptr(), source_ptr);
        assert_eq!(forwarded.len(), 1024 * 1024);
        assert_eq!(output["status"], "completed");
        assert_eq!(output["type"], "reasoning");
    }

    #[test]
    fn responses_usage_keeps_cache_and_reasoning_breakdown() {
        let chat = json!({"usage":{
            "prompt_tokens":12,"completion_tokens":8,"total_tokens":20,
            "prompt_tokens_details":{"cached_tokens":9,"audio_tokens":2},
            "completion_tokens_details":{"reasoning_tokens":5}
        }});
        assert_eq!(
            translated_chat_usage(&chat).unwrap(),
            json!({
                "input_tokens":12,"output_tokens":8,"total_tokens":20,
                "input_tokens_details":{"cached_tokens":9},
                "output_tokens_details":{"reasoning_tokens":5}
            })
        );
        assert_eq!(translated_chat_usage(&json!({})).unwrap(), Value::Null);
        let no_details = json!({"usage":{
            "prompt_tokens":12,"completion_tokens":8,"total_tokens":20
        }});
        assert_eq!(
            translated_chat_usage(&no_details).unwrap(),
            json!({"input_tokens":12,"output_tokens":8,"total_tokens":20})
        );
        for invalid in [
            json!({"prompt_tokens_details":{"cached_tokens":13}}),
            json!({"completion_tokens_details":{"reasoning_tokens":9}}),
            json!({"prompt_tokens_details":{"cached_tokens":-1}}),
            json!({"completion_tokens_details":{"reasoning_tokens":"5"}}),
            json!({"prompt_tokens_details":[]}),
        ] {
            let mut malformed = no_details.clone();
            malformed["usage"]
                .as_object_mut()
                .unwrap()
                .extend(invalid.as_object().unwrap().clone());
            assert!(translated_chat_usage(&malformed).is_err());
        }
    }

    #[tokio::test]
    async fn malformed_present_chat_usage_cannot_be_silently_discarded() {
        let mut chat = json!({
            "id":"chatcmpl-usage","model":"physical","created":123,
            "choices":[{"index":0,"message":{"role":"assistant","content":"ok"},
                "finish_reason":"stop"}]
        });
        for invalid in [
            json!({}),
            json!({"prompt_tokens":4}),
            json!({"prompt_tokens":4,"completion_tokens":2}),
            json!({"prompt_tokens":4,"completion_tokens":2,"total_tokens":7}),
            json!({"prompt_tokens":-1,"completion_tokens":2,"total_tokens":1}),
            json!("not a usage object"),
        ] {
            chat["usage"] = invalid;
            assert!(!translatable_chat_result(&chat));
            assert_eq!(
                translate_chat_result(chat.clone(), "demo".into(), Value::Null).status(),
                StatusCode::BAD_GATEWAY
            );
        }
        chat["usage"] = json!({"prompt_tokens":4,"completion_tokens":2,"total_tokens":6});
        assert!(translatable_chat_result(&chat));
        let good = translate_chat_result(chat.clone(), "demo".into(), Value::Null);
        let body = to_bytes(good.into_body(), 1 << 20).await.unwrap();
        let response: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(response["usage"]["total_tokens"], 6);
        chat["usage"] = Value::Null;
        assert!(translatable_chat_result(&chat));
        chat.as_object_mut().unwrap().remove("usage");
        assert!(translatable_chat_result(&chat));
    }

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
