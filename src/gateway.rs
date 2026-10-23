//! Public API; Chat and Embeddings share the same candidate walk.
use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    config::Config,
    keypool::{KeyPool, Lease},
    metrics::Metrics,
    outcome::{Outcome, classify},
    routing,
    sse::ChatSse,
};

const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const MAX_INFLIGHT_REQUESTS: usize = 128;

pub struct AppState {
    pub config: Config,
    client: reqwest::Client,
    pools: Vec<KeyPool>,
    client_keys: Vec<String>,
    inflight: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    model_catalog: Value,
    model_details: HashMap<String, Value>,
}

impl AppState {
    pub fn new(
        config: Config,
        credentials: HashMap<String, String>,
        client_keys: Vec<String>,
    ) -> Result<Self, String> {
        config.validate()?;
        if client_keys.is_empty() || client_keys.iter().any(String::is_empty) {
            return Err("API_KEYS or ADMIN_KEY is required".into());
        }
        let pools = config
            .providers
            .iter()
            .map(|p| KeyPool::new(p, &credentials))
            .collect::<Result<Vec<_>, _>>()?;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none()) // Do not redirect provider credentials.
            .build()
            .map_err(|e| format!("building HTTP client: {e}"))?;
        // Expose configured named chains and explicitly catalogued provider models.
        let mut model_details = HashMap::new();
        for chain in &config.chains {
            model_details.insert(
                chain.name.clone(),
                json!({"id":chain.name,"object":"model","owned_by":"guvna"}),
            );
        }
        for provider in &config.providers {
            for prefix in provider.prefixes() {
                for model in &provider.models {
                    let id = format!("{prefix}/{model}");
                    model_details.entry(id.clone()).or_insert_with(
                        || json!({"id":id,"object":"model","owned_by":provider.name}),
                    );
                }
            }
        }
        let mut data: Vec<_> = model_details.values().cloned().collect();
        data.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        let model_catalog = json!({"object":"list","data":data});
        Ok(Self {
            model_catalog,
            model_details,
            config,
            client,
            pools,
            client_keys,
            inflight: Arc::new(Semaphore::new(MAX_INFLIGHT_REQUESTS)),
            metrics: Arc::new(Metrics::default()),
        })
    }

    fn allowed(&self, headers: &HeaderMap) -> bool {
        let Some(header) = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
        else {
            return false;
        };
        self.client_keys
            .iter()
            .any(|key| constant_time_equal(key.as_bytes(), header.as_bytes()))
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= usize::from(a ^ b);
    }
    diff == 0
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({"error": {"type":"gateway_error","code":code,"message":message}})),
    )
        .into_response()
}

fn unauthorized() -> Response {
    error(
        StatusCode::UNAUTHORIZED,
        "invalid_api_key",
        "Bearer API key is required",
    )
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_endpoint))
        .route("/v1/models", get(models))
        .route("/v1/models/{model}", get(model_detail))
        .route("/v1/chat/completions", post(chat))
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/responses", post(responses))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(Arc::new(state))
}

async fn healthz() -> &'static str {
    "ok"
}

async fn metrics_endpoint(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !state.allowed(&headers) {
        return unauthorized();
    }
    // Metrics contain no user/provider labels or secrets. The endpoint is authenticated
    // because even aggregate traffic counts are operationally sensitive.
    let active = MAX_INFLIGHT_REQUESTS - state.inflight.available_permits();
    (
        [
            ("content-type", "text/plain; version=0.0.4; charset=utf-8"),
            ("cache-control", "no-store"),
        ],
        state.metrics.prometheus(active),
    )
        .into_response()
}
async fn models(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !state.allowed(&headers) {
        return unauthorized();
    }
    (StatusCode::OK, Json(state.model_catalog.clone())).into_response()
}

async fn model_detail(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath(model): AxumPath<String>,
) -> Response {
    if !state.allowed(&headers) {
        return unauthorized();
    }
    match state.model_details.get(&model) {
        Some(detail) => (StatusCode::OK, Json(detail.clone())).into_response(),
        None => error(StatusCode::NOT_FOUND, "model_not_found", "Unknown model"),
    }
}

// Minimal non-streaming Responses compatibility adapter. Unsupported modes fail closed.
async fn responses(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !state.allowed(&headers) {
        return unauthorized();
    }
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
        headers,
        Bytes::from(value.to_string()),
        Endpoint::Chat,
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
#[derive(Clone, Copy)]
enum Endpoint {
    Chat,
    Embeddings,
}
impl Endpoint {
    fn path(self) -> &'static str {
        match self {
            Self::Chat => "/chat/completions",
            Self::Embeddings => "/embeddings",
        }
    }
}

async fn chat(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    dispatch(state, headers, body, Endpoint::Chat).await
}
async fn embeddings(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    dispatch(state, headers, body, Endpoint::Embeddings).await
}

async fn dispatch(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
    endpoint: Endpoint,
) -> Response {
    if !state.allowed(&headers) {
        return unauthorized();
    }
    let request: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                "Expected one JSON object",
            );
        }
    };
    if !request.is_object() {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Expected JSON object",
        );
    }
    if request
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(str::is_empty)
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_model",
            "model must not be empty",
        );
    }
    if request
        .get("stream")
        .is_some_and(|value| !value.is_boolean())
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_stream",
            "stream must be a boolean",
        );
    }
    Metrics::inc(&state.metrics.requests_dispatched);
    let streaming = request.get("stream").and_then(Value::as_bool) == Some(true);
    if streaming && !matches!(endpoint, Endpoint::Chat) {
        return error(
            StatusCode::NOT_IMPLEMENTED,
            "migration_pending",
            "Only Chat Completions streaming is supported",
        );
    }
    let Some(model) = request.get("model").and_then(Value::as_str) else {
        return error(
            StatusCode::BAD_REQUEST,
            "missing_model",
            "model must be a string",
        );
    };
    let expected = request.get("n").map_or(1, |v| v.as_u64().unwrap_or(0));
    if streaming && ChatSse::new(expected).is_err() {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_n",
            "stream n must be 1..64",
        );
    }
    let steps = match routing::resolve(&state.config, model) {
        Ok(steps) => steps,
        Err(_) => {
            return error(
                StatusCode::NOT_FOUND,
                "unknown_model",
                "Unknown chain/model; configure a chain first",
            );
        }
    };

    // Reject overload immediately instead of building unbounded waiting tasks.
    let mut permit = Some(match state.inflight.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            Metrics::inc(&state.metrics.admission_rejections);
            let mut response = error(
                StatusCode::SERVICE_UNAVAILABLE,
                "gateway_overloaded",
                "Too many concurrent upstream requests",
            );
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
            return response;
        }
    });

    // Count actual candidate transitions, not provider-key retries.
    let mut candidates_visited = 0usize;
    // Exactly ONE candidate loop for chat + embedding. Key failover is confined to this loop.
    for step in steps {
        let provider = &state.config.providers[step.provider_index];
        if provider.stream_only && !streaming {
            continue; // A stream-only upstream cannot serve non-streaming requests.
        }
        if candidates_visited != 0 {
            Metrics::inc(&state.metrics.candidate_fallbacks);
        }
        candidates_visited += 1;
        let mut payload = request.clone();
        let object = payload.as_object_mut().expect("checked above");
        object.insert("model".into(), Value::String(step.model.clone()));
        for (name, value) in &step.params {
            object.entry(name).or_insert_with(|| value.clone()); // caller always wins.
        }

        // Serialize once per candidate, not once for every credential retry.
        let payload_json = Bytes::from(payload.to_string());
        let mut tried = HashSet::new();
        while let Some(lease) = state.pools[step.provider_index].acquire(&tried) {
            tried.insert(lease.env.clone());
            let url = provider.endpoint(endpoint.path());
            Metrics::inc(&state.metrics.upstream_attempts);
            // A request-wide reqwest timeout would terminate a healthy SSE
            // generation after 90s despite regular progress. Time only the
            // HTTP headers and non-streaming bounded JSON body instead.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
            let send = state
                .client
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .bearer_auth(&lease.secret)
                .body(payload_json.clone())
                .send();
            let response = match tokio::time::timeout_at(deadline, send).await {
                Ok(result) => result,
                Err(_) => {
                    Metrics::inc(&state.metrics.upstream_transport_failures);
                    break;
                }
            };
            let upstream = match response {
                Ok(response) => response,
                Err(_) => {
                    Metrics::inc(&state.metrics.upstream_transport_failures);
                    break;
                } // key-neutral
            };
            let status = upstream.status().as_u16();
            let outcome = classify(Some(status));
            if outcome.quarantine_credential() {
                Metrics::inc(&state.metrics.upstream_http_failures);
                if state.pools[step.provider_index].record(&lease, outcome) {
                    Metrics::inc(&state.metrics.credential_quarantines);
                }
                continue; // try a different key; each key is attempted once
            }
            if outcome == Outcome::Success {
                if streaming {
                    match start_chat_stream(
                        upstream,
                        expected,
                        state.clone(),
                        step.provider_index,
                        lease.clone(),
                        &mut permit,
                    )
                    .await
                    {
                        Ok(stream_response) => return stream_response,
                        Err(_) => {
                            Metrics::inc(&state.metrics.upstream_invalid_responses);
                            break;
                        } // preflight rejected
                    }
                }
                match tokio::time::timeout_at(deadline, bounded_json(upstream)).await {
                    Ok(Ok(value)) if valid_result(endpoint, &payload, &value) => {
                        state.pools[step.provider_index].record_success(&lease);
                        Metrics::inc(&state.metrics.upstream_success);
                        return (StatusCode::OK, Json(value)).into_response();
                    }
                    _ => {
                        Metrics::inc(&state.metrics.upstream_invalid_responses);
                        break;
                    } // next step
                }
            }
            Metrics::inc(&state.metrics.upstream_http_failures);
            break; // 4xx or 5xx: next chain step; no uncontrolled retries
        }
    }
    error(
        StatusCode::BAD_GATEWAY,
        "upstream_unavailable",
        "No upstream returned a valid response",
    )
}

// Bound a single upstream read: even valid tiny events must not create an unbounded queue.
const MAX_STREAM_CHUNK_BYTES: usize = 2 * 1024 * 1024;

struct ChatStreamState {
    owner: Arc<AppState>,
    provider_index: usize,
    lease: Lease,
    upstream: reqwest::Response,
    parser: ChatSse,
    pending: VecDeque<Bytes>,
    terminated: bool,
    terminal_reported: bool,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Drop for ChatStreamState {
    fn drop(&mut self) {
        // Dropping a partially consumed body must free the upstream request permit.
        if self._permit.is_some() && !self.terminal_reported {
            Metrics::inc(&self.owner.metrics.chat_stream_cancelled);
        }
    }
}
fn stream_error() -> Bytes {
    Bytes::from_static(b"data: {\"error\":{\"type\":\"gateway_error\",\"code\":\"upstream_stream_incomplete\"}}\n\n")
}

async fn start_chat_stream(
    upstream: reqwest::Response,
    expected: u64,
    owner: Arc<AppState>,
    provider_index: usize,
    lease: Lease,
    permit: &mut Option<OwnedSemaphorePermit>,
) -> Result<Response, ()> {
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if !content_type
        .split(';')
        .next()
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"))
    {
        return Err(());
    }
    let mut state = ChatStreamState {
        owner,
        provider_index,
        lease,
        upstream,
        parser: ChatSse::new(expected).map_err(|_| ())?,
        pending: VecDeque::new(),
        terminated: false,
        terminal_reported: false,
        _permit: None,
    };
    preflight_stream(&mut state).await?;
    state._permit = permit.take();
    Ok(stream_response(state))
}

async fn preflight_stream(state: &mut ChatStreamState) -> Result<(), ()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let chunk = state.upstream.chunk().await.map_err(|_| ())?.ok_or(())?;
            if chunk.len() > MAX_STREAM_CHUNK_BYTES {
                return Err(());
            }
            let events = state.parser.push(&chunk).map_err(|_| ())?;
            state.pending.extend(events);
            if !state.pending.is_empty() {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|_| ())?
}

fn stream_response(state: ChatStreamState) -> Response {
    let output = futures_util::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                if event == Bytes::from_static(b"data: [DONE]\n\n") {
                    state.owner.pools[state.provider_index].record_success(&state.lease);
                    Metrics::inc(&state.owner.metrics.upstream_success);
                    Metrics::inc(&state.owner.metrics.chat_stream_completed);
                    state.terminal_reported = true;
                }
                return Some((Ok::<Bytes, std::io::Error>(event), state));
            }
            if state.terminated {
                return None;
            }
            if state.parser.is_done() {
                return None;
            }
            // Bound upstream stalls after the first event without buffering the response.
            match tokio::time::timeout(Duration::from_secs(15), state.upstream.chunk()).await {
                Ok(Ok(Some(bytes))) => {
                    if bytes.len() > MAX_STREAM_CHUNK_BYTES {
                        state.terminated = true;
                    } else {
                        match state.parser.push(&bytes) {
                            Ok(events) => state.pending.extend(events),
                            Err(_) => state.terminated = true,
                        }
                    }
                }
                Ok(Ok(None)) | Ok(Err(_)) | Err(_) => state.terminated = true,
            }
            if state.terminated {
                Metrics::inc(&state.owner.metrics.chat_stream_failed);
                state.terminal_reported = true;
                return Some((Ok(stream_error()), state));
            }
        }
    });
    Response::builder()
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(Body::from_stream(output))
        .expect("static response headers")
}

async fn bounded_json(response: reqwest::Response) -> Result<Value, ()> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| ())?;
        if chunk.len() > MAX_BODY_BYTES.saturating_sub(bytes.len()) {
            return Err(());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| ())
}

fn valid_result(endpoint: Endpoint, request: &Value, response: &Value) -> bool {
    match endpoint {
        Endpoint::Chat => validate_chat(request, response),
        Endpoint::Embeddings => validate_embeddings(request, response),
    }
}

// A result is only usable when every choice completed; a partial 2xx is NOT success.
fn validate_chat(request: &Value, response: &Value) -> bool {
    let Some(choices) = response.get("choices").and_then(Value::as_array) else {
        return false;
    };
    if choices.is_empty() {
        return false;
    }
    let expected = request.get("n").and_then(Value::as_u64).unwrap_or(1);
    if expected > 1 && choices.len() as u64 != expected {
        return false;
    }
    let mut indices = HashSet::new();
    for choice in choices {
        let index = match choice.get("index") {
            Some(index) => match index.as_u64() {
                Some(x) => x,
                None => return false,
            },
            None if choices.len() == 1 => 0,
            None => return false,
        };
        if index >= choices.len() as u64 || !indices.insert(index) {
            return false;
        }
        let Some(message) = choice.get("message").and_then(Value::as_object) else {
            return false;
        };
        if !choice
            .get("finish_reason")
            .and_then(Value::as_str)
            .is_some_and(|x| {
                [
                    "stop",
                    "length",
                    "tool_calls",
                    "function_call",
                    "content_filter",
                ]
                .contains(&x)
            })
        {
            return false;
        }
        if let Some(tool_calls) = message.get("tool_calls") {
            let Some(calls) = tool_calls.as_array() else {
                return false;
            };
            for call in calls {
                let valid = call
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty())
                    && call.get("type").and_then(Value::as_str) == Some("function")
                    && call
                        .pointer("/function/name")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.is_empty())
                    && call
                        .pointer("/function/arguments")
                        .and_then(Value::as_str)
                        .is_some_and(|s| serde_json::from_str::<Value>(s).is_ok());
                if !valid {
                    return false;
                }
            }
        }
    }
    true
}

// Do not accept duplicate / missing embeddings, mixed dimensions or corrupt vectors.
fn validate_embeddings(request: &Value, response: &Value) -> bool {
    let expected = match request.get("input") {
        Some(Value::String(_)) => 1,
        Some(Value::Array(items)) if !items.is_empty() => items.len(),
        _ => return false,
    };
    let Some(data) = response.get("data").and_then(Value::as_array) else {
        return false;
    };
    if data.len() != expected {
        return false;
    }
    let desired_dim = request
        .get("dimensions")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0);
    let encoding = request.get("encoding_format").and_then(Value::as_str);
    let mut observed_dimension = None;
    let mut observed_format = None;
    let mut indices = HashSet::new();
    for entry in data {
        let index = match entry.get("index") {
            Some(index) => match index.as_u64() {
                Some(x) => x,
                None => return false,
            },
            None if expected == 1 => 0,
            None => return false,
        };
        if index >= expected as u64 || !indices.insert(index) {
            return false;
        }
        let Some(vector) = entry.get("embedding") else {
            return false;
        };
        let (format, dimension) = match vector {
            Value::Array(numbers) if !numbers.is_empty() => {
                if numbers
                    .iter()
                    .all(|n| n.as_f64().is_some_and(f64::is_finite))
                {
                    ("float", numbers.len())
                } else {
                    return false;
                }
            }
            Value::String(data) if !data.is_empty() => {
                use base64::{Engine, engine::general_purpose::STANDARD};
                let Ok(raw) = STANDARD.decode(data) else {
                    return false;
                };
                if raw.is_empty()
                    || raw.len() % 4 != 0
                    || !raw
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .all(|part| f32::from_le_bytes(*part).is_finite())
                {
                    return false;
                }
                ("base64", raw.len() / 4)
            }
            _ => return false,
        };
        if encoding.is_some_and(|e| e == "float" || e == "base64") && encoding != Some(format) {
            return false;
        }
        if desired_dim.is_some_and(|wanted| wanted != dimension) {
            return false;
        }
        if observed_dimension.is_some_and(|previous| previous != dimension)
            || observed_format.is_some_and(|previous| previous != format)
        {
            return false;
        }
        observed_dimension = Some(dimension);
        observed_format = Some(format);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn admission_limit_rejects_immediately_without_contacting_provider() {
        let config: Config = serde_yaml::from_str(
            "providers:\n  - name: p\n    type: openai\n    base_url: http://127.0.0.1:1\n    key_env: KEY\nchains:\n  - name: demo\n    steps:\n      - provider: p\n        model: test\n"
        ).unwrap();
        let state = Arc::new(
            AppState::new(
                config,
                HashMap::from([("KEY".to_owned(), "test-only".to_owned())]),
                vec!["client".to_owned()],
            )
            .unwrap(),
        );
        let all_permits = state
            .inflight
            .clone()
            .try_acquire_many_owned(MAX_INFLIGHT_REQUESTS as u32)
            .unwrap();
        assert_eq!(state.inflight.available_permits(), 0);
        let headers = HeaderMap::from_iter([(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer client"),
        )]);
        let response = tokio::time::timeout(
            Duration::from_millis(200),
            dispatch(
                state.clone(),
                headers,
                Bytes::from_static(
                    br#"{"model":"demo","messages":[{"role":"user","content":"hi"}]}"#,
                ),
                Endpoint::Chat,
            ),
        )
        .await
        .expect("overload should fail fast");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
        assert_eq!(
            state
                .metrics
                .admission_rejections
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            state
                .metrics
                .upstream_attempts
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert!(
            state
                .metrics
                .prometheus(MAX_INFLIGHT_REQUESTS)
                .contains("guvna_active_requests 128\n")
        );
        drop(all_permits);
        assert_eq!(state.inflight.available_permits(), MAX_INFLIGHT_REQUESTS);
    }
    #[test]
    fn protects_credentials_and_big_numbers() {
        assert!(constant_time_equal(b"a", b"a"));
        assert!(!constant_time_equal(b"a", b"b"));
        assert!(!constant_time_equal(b"a", b"aa"));
        let request: Value = serde_json::from_str(r#"{"input":"hello"}"#).unwrap();
        assert!(valid_result(
            Endpoint::Embeddings,
            &request,
            &json!({
                "data":[{"embedding":[0.1,0.2],"index":0}]
            })
        ));
        assert!(!valid_result(
            Endpoint::Embeddings,
            &request,
            &json!({
                "data":[{"embedding":[]}]
            })
        ));
    }
}

#[cfg(test)]
mod response_contract_tests {
    use super::*;

    fn assistant(index: u64) -> Value {
        json!({"index":index,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"})
    }

    #[test]
    fn chat_requires_all_choices_and_unique_indices() {
        let request = json!({"n":2});
        let good = json!({"choices":[assistant(1),assistant(0)]});
        assert!(validate_chat(&request, &good));
        assert!(!validate_chat(&request, &json!({"choices":[assistant(0)]})));
        assert!(!validate_chat(
            &request,
            &json!({"choices":[assistant(0),assistant(0)]})
        ));
        assert!(!validate_chat(
            &request,
            &json!({"choices":[assistant(0),assistant(2)]})
        ));
        assert!(!validate_chat(
            &request,
            &json!({"choices":[{"index":0,"message":{},"finish_reason":null},assistant(1)]})
        ));
    }

    #[test]
    fn completed_tool_calls_require_valid_json_arguments() {
        let request = json!({"n":1});
        let item = |args: &str| {
            json!({"choices":[{
                "index":0,"message":{"tool_calls":[{
                    "id":"call_1","type":"function","function":{"name":"read","arguments":args}
                }]},"finish_reason":"tool_calls"
            }]})
        };
        assert!(validate_chat(&request, &item(r#"{"path":"/tmp"}"#)));
        assert!(!validate_chat(&request, &item(r#"{"path":"#)));
    }

    #[test]
    fn embeddings_require_complete_unique_finite_vectors_and_equal_dimensions() {
        let request = json!({"input":["a","b"],"dimensions":2,"encoding_format":"float"});
        let good = json!({"data":[
            {"index":1,"embedding":[0.1,0.2]},
            {"index":0,"embedding":[0.3,0.4]}
        ]});
        assert!(validate_embeddings(&request, &good));
        assert!(!validate_embeddings(
            &request,
            &json!({"data":[
                {"index":0,"embedding":[0.1,0.2]},
                {"index":0,"embedding":[0.3,0.4]}
            ]})
        ));
        assert!(!validate_embeddings(
            &request,
            &json!({"data":[
                {"index":0,"embedding":[0.1,0.2]},
                {"index":1,"embedding":[0.3]}
            ]})
        ));
        assert!(!validate_embeddings(
            &request,
            &json!({"data":[
                {"index":0,"embedding":[0.1,0.2]}
            ]})
        ));
    }

    #[test]
    fn base64_embeddings_reject_invalid_payload_and_nonfinite_float32() {
        use base64::{Engine, engine::general_purpose::STANDARD};
        let request = json!({"input":"a","dimensions":1,"encoding_format":"base64"});
        let good = STANDARD.encode(1.25_f32.to_le_bytes());
        assert!(validate_embeddings(
            &request,
            &json!({"data":[{"embedding":good}]})
        ));
        assert!(!validate_embeddings(
            &request,
            &json!({"data":[{"embedding":"not-base64!"}]})
        ));
        let nan = STANDARD.encode(f32::NAN.to_le_bytes());
        assert!(!validate_embeddings(
            &request,
            &json!({"data":[{"embedding":nan}]})
        ));
    }
}
