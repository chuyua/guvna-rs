//! Public API; Chat and Embeddings share the same candidate walk.
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use crate::{
    config::Config,
    keypool::KeyPool,
    outcome::{Outcome, classify},
    routing,
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
    if request.get("stream").and_then(Value::as_bool) == Some(true) {
        return error(
            StatusCode::NOT_IMPLEMENTED,
            "migration_pending",
            "Streaming is not implemented in this milestone",
        );
    }
    let Some(model) = request.get("model").and_then(Value::as_str) else {
        return error(
            StatusCode::BAD_REQUEST,
            "missing_model",
            "model must be a string",
        );
    };
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
        if provider.stream_only {
            continue; // Explicitly unsupported stream-only aggregation in M1.
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
                match bounded_json(upstream).await {
                    Ok(value) if valid_result(endpoint, &request, &value) => {
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
        Endpoint::Chat => {
            let Some(choices) = response.get("choices").and_then(Value::as_array) else {
                return false;
            };
            !choices.is_empty()
                && choices.iter().all(|choice| {
                    choice.get("message").is_some_and(Value::is_object)
                        && choice
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
                })
        }
        Endpoint::Embeddings => {
            let Some(data) = response.get("data").and_then(Value::as_array) else {
                return false;
            };
            let expected = match request.get("input") {
                Some(Value::String(_)) => 1,
                Some(Value::Array(items)) if !items.is_empty() => items.len(),
                _ => return false,
            };
            data.len() == expected
                && data.iter().all(|entry| match entry.get("embedding") {
                    Some(Value::Array(xs)) => !xs.is_empty() && xs.iter().all(Value::is_number),
                    Some(Value::String(s)) => !s.is_empty(),
                    _ => false,
                })
        }
    }
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
