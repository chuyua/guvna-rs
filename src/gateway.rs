//! Public API; Chat and Embeddings share the same candidate walk.
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
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

use crate::{
    config::Config,
    keypool::{KeyPool, Lease},
    outcome::{Outcome, classify},
    routing,
    sse::ChatSse,
};

const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

pub struct AppState {
    pub config: Config,
    client: reqwest::Client,
    pools: Vec<KeyPool>,
    client_keys: Vec<String>,
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
            .timeout(Duration::from_secs(90))
            .redirect(reqwest::redirect::Policy::none()) // Do not redirect provider credentials.
            .build()
            .map_err(|e| format!("building HTTP client: {e}"))?;
        Ok(Self {
            config,
            client,
            pools,
            client_keys,
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
        .route("/v1/models", get(models))
        .route("/v1/chat/completions", post(chat))
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/responses", post(not_implemented))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(Arc::new(state))
}

async fn healthz() -> &'static str {
    "ok"
}

async fn models(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !state.allowed(&headers) {
        return unauthorized();
    }
    let models: Vec<_> = state
        .config
        .chains
        .iter()
        .map(|ch| {
            json!({
                "id": ch.name, "object": "model", "owned_by": "guvna"
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(json!({"object":"list", "data":models})),
    )
        .into_response()
}

async fn not_implemented() -> Response {
    error(
        StatusCode::NOT_IMPLEMENTED,
        "migration_pending",
        "Responses translation is not implemented in this milestone",
    )
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

    // Exactly ONE candidate loop for chat + embedding. Key failover is confined to this loop.
    for step in steps {
        let provider = &state.config.providers[step.provider_index];
        if provider.stream_only && !streaming {
            continue; // A stream-only upstream cannot serve non-streaming requests.
        }
        let mut payload = request.clone();
        let object = payload.as_object_mut().expect("checked above");
        object.insert("model".into(), Value::String(step.model.clone()));
        for (name, value) in &step.params {
            object.entry(name).or_insert_with(|| value.clone()); // caller always wins.
        }

        let mut tried = HashSet::new();
        while let Some(lease) = state.pools[step.provider_index].acquire(&tried) {
            tried.insert(lease.env.clone());
            let url = provider.endpoint(endpoint.path());
            let response = state
                .client
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .bearer_auth(&lease.secret)
                .body(payload.to_string())
                .send()
                .await;
            let upstream = match response {
                Ok(response) => response,
                Err(_) => break, // transport faults are key-neutral
            };
            let status = upstream.status().as_u16();
            let outcome = classify(Some(status));
            if outcome.quarantine_credential() {
                state.pools[step.provider_index].record(&lease, outcome);
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
                    )
                    .await
                    {
                        Ok(stream_response) => return stream_response,
                        Err(_) => break, // Fail over only BEFORE any bytes reach the client.
                    }
                }
                match bounded_json(upstream).await {
                    Ok(value) if valid_result(endpoint, &payload, &value) => {
                        state.pools[step.provider_index].record_success(&lease);
                        return (StatusCode::OK, Json(value)).into_response();
                    }
                    _ => break, // malformed 2xx is not a usable success; next chain step
                }
            }
            break; // 4xx or 5xx: next chain step; no uncontrolled retries
        }
    }
    error(
        StatusCode::BAD_GATEWAY,
        "upstream_unavailable",
        "No upstream returned a valid response",
    )
}

struct ChatStreamState {
    owner: Arc<AppState>,
    provider_index: usize,
    lease: Lease,
    upstream: reqwest::Response,
    parser: ChatSse,
    pending: VecDeque<Bytes>,
    terminated: bool,
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
) -> Result<Response, ()> {
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if !content_type.starts_with("text/event-stream") {
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
    };
    preflight_stream(&mut state).await?;
    Ok(stream_response(state))
}

async fn preflight_stream(state: &mut ChatStreamState) -> Result<(), ()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let chunk = state.upstream.chunk().await.map_err(|_| ())?.ok_or(())?;
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
                return Some((Ok::<Bytes, std::io::Error>(event), state));
            }
            if state.terminated {
                return None;
            }
            if state.parser.is_done() {
                state.owner.pools[state.provider_index].record_success(&state.lease);
                return None;
            }
            match state.upstream.chunk().await {
                Ok(Some(bytes)) => match state.parser.push(&bytes) {
                    Ok(events) => state.pending.extend(events),
                    Err(_) => state.terminated = true,
                },
                Ok(None) | Err(_) => state.terminated = true,
            }
            if state.terminated {
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
