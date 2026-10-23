//! Public API; Chat and Embeddings share the same candidate walk.
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, FromRequest, Path as AxumPath, State},
    http::{HeaderMap, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    circuit::{Circuit, Ticket},
    config::Config,
    keypool::KeyPool,
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
    circuits: Vec<Option<Arc<Circuit>>>,
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
        let circuits = config
            .providers
            .iter()
            .map(|p| Circuit::new(&p.circuit_breaker))
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
            circuits,
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

// RFC Retry-After permits delta seconds or an HTTP date. Apply only to a
// rate-limited credential, cap upstream hints at one day, and retain the
// configured cooldown if the hint is malformed or already expired.
fn retry_after_delay(headers: &reqwest::header::HeaderMap, now: SystemTime) -> Option<Duration> {
    let raw = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    let delay = if !raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit()) {
        Duration::from_secs(raw.parse::<u64>().unwrap_or(u64::MAX))
    } else {
        httpdate::parse_http_date(raw)
            .ok()?
            .duration_since(now)
            .ok()?
    };
    Some(delay.min(Duration::from_secs(24 * 3600)))
}

// Authenticate before invoking Bytes::from_request: the normal Bytes handler
// extractor buffers the body before handler-level authorization can run.
enum RequestBodyError {
    Unauthorized,
    Overloaded,
    Invalid(StatusCode),
}

impl RequestBodyError {
    fn into_response(self) -> Response {
        match self {
            Self::Unauthorized => unauthorized(),
            Self::Overloaded => {
                let mut response = error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "gateway_overloaded",
                    "Too many concurrent upstream requests",
                );
                response.headers_mut().insert(
                    axum::http::header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
                response
            }
            Self::Invalid(status) => error(
                status,
                "invalid_body",
                "Request body is invalid or exceeds the configured limit",
            ),
        }
    }
}

async fn authenticated_body(
    state: &Arc<AppState>,
    request: Request<Body>,
) -> Result<(Bytes, OwnedSemaphorePermit), RequestBodyError> {
    if !state.allowed(request.headers()) {
        return Err(RequestBodyError::Unauthorized);
    }
    // Reserve a slot before reading a potentially slow 32 MiB upload. The
    // permit follows the request through parsing, routing and SSE delivery.
    let permit = state.inflight.clone().try_acquire_owned().map_err(|_| {
        Metrics::inc(&state.metrics.admission_rejections);
        RequestBodyError::Overloaded
    })?;
    let body = Bytes::from_request(request, state)
        .await
        .map_err(|rejection| RequestBodyError::Invalid(rejection.status()))?;
    Ok((body, permit))
}

// Keep one classification path for all candidate and SSE circuit outcomes.
fn record_circuit_failure(ticket: &mut Option<Ticket>, metrics: &Metrics) {
    if let Some(ticket) = ticket {
        let probe = ticket.is_probe();
        if ticket.failure() {
            Metrics::inc(&metrics.circuit_openings);
        }
        if probe {
            Metrics::inc(&metrics.circuit_probe_failures);
        }
    }
}

fn record_circuit_success(ticket: &mut Option<Ticket>, metrics: &Metrics) {
    if let Some(ticket) = ticket {
        let probe = ticket.is_probe();
        ticket.success();
        if probe {
            Metrics::inc(&metrics.circuit_probe_success);
        }
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_endpoint))
        .route("/v1/models", get(models))
        .route("/v1/models/{model}", get(model_detail))
        .route("/v1/chat/completions", post(chat))
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/responses", post(responses::handle))
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
        state.metrics.prometheus(
            active,
            state
                .circuits
                .iter()
                .filter_map(Option::as_ref)
                .filter(|c| c.is_open())
                .count(),
        ),
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

// The Responses contract and all Chat<->Responses translation live in their
// own module. Keep gateway responsible for auth, routing, and shared dispatch.
#[path = "responses.rs"]
mod responses;

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

async fn chat(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (body, permit) = match authenticated_body(&state, request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };
    dispatch(state, body, Endpoint::Chat, permit).await
}
async fn embeddings(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (body, permit) = match authenticated_body(&state, request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };
    dispatch(state, body, Endpoint::Embeddings, permit).await
}

async fn dispatch(
    state: Arc<AppState>,
    body: Bytes,
    endpoint: Endpoint,
    admission: OwnedSemaphorePermit,
) -> Response {
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

    let mut permit = Some(admission);

    // Count actual candidate transitions, not provider-key retries.
    let mut candidates_visited = 0usize;
    let mut attempted_network = false;
    let mut bypassed_circuit = false;
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
        let mut circuit_ticket = match &state.circuits[step.provider_index] {
            Some(circuit) => match circuit.admit() {
                Some(ticket) => Some(ticket),
                None => {
                    Metrics::inc(&state.metrics.circuit_bypasses);
                    bypassed_circuit = true;
                    continue; // No HTTP calls while open or a recovery probe runs.
                }
            },
            None => None, // Preserve existing routing behavior by default.
        };
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
            attempted_network = true;
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
                    record_circuit_failure(&mut circuit_ticket, &state.metrics);
                    break;
                }
            };
            let upstream = match response {
                Ok(response) => response,
                Err(_) => {
                    Metrics::inc(&state.metrics.upstream_transport_failures);
                    record_circuit_failure(&mut circuit_ticket, &state.metrics);
                    break;
                } // key-neutral
            };
            let status = upstream.status().as_u16();
            let outcome = classify(Some(status));
            if outcome.quarantine_credential() {
                Metrics::inc(&state.metrics.upstream_http_failures);
                let retry_after = if outcome == Outcome::RateLimited {
                    retry_after_delay(upstream.headers(), SystemTime::now())
                } else {
                    None
                };
                if state.pools[step.provider_index].record_with_retry_after(
                    &lease,
                    outcome,
                    retry_after,
                ) {
                    Metrics::inc(&state.metrics.credential_quarantines);
                }
                continue; // try a different key; each key is attempted once
            }
            if outcome == Outcome::Success {
                if streaming {
                    match chat_stream::start(
                        upstream,
                        expected,
                        state.clone(),
                        step.provider_index,
                        lease.clone(),
                        &mut permit,
                        &mut circuit_ticket,
                    )
                    .await
                    {
                        Ok(stream_response) => return stream_response,
                        Err(_) => {
                            Metrics::inc(&state.metrics.upstream_invalid_responses);
                            record_circuit_failure(&mut circuit_ticket, &state.metrics);
                            break;
                        } // preflight rejected
                    }
                }
                match tokio::time::timeout_at(deadline, bounded_json(upstream)).await {
                    Ok(Ok(value))
                        if response_validation::valid_result(endpoint, &payload, &value) =>
                    {
                        state.pools[step.provider_index].record_success(&lease);
                        record_circuit_success(&mut circuit_ticket, &state.metrics);
                        Metrics::inc(&state.metrics.upstream_success);
                        return (StatusCode::OK, Json(value)).into_response();
                    }
                    _ => {
                        Metrics::inc(&state.metrics.upstream_invalid_responses);
                        record_circuit_failure(&mut circuit_ticket, &state.metrics);
                        break;
                    } // next step
                }
            }
            Metrics::inc(&state.metrics.upstream_http_failures);
            if outcome == Outcome::ProviderTransient
                && let Some(ticket) = &mut circuit_ticket
            {
                let probe = ticket.is_probe();
                if ticket.failure() {
                    Metrics::inc(&state.metrics.circuit_openings);
                }
                if probe {
                    Metrics::inc(&state.metrics.circuit_probe_failures);
                }
            }
            break; // 4xx or 5xx: next chain step; no uncontrolled retries
        }
    }
    if bypassed_circuit && !attempted_network {
        let mut response = error(
            StatusCode::SERVICE_UNAVAILABLE,
            "provider_circuit_open",
            "All eligible providers are temporarily unavailable",
        );
        response.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
        return response;
    }
    error(
        StatusCode::BAD_GATEWAY,
        "upstream_unavailable",
        "No upstream returned a valid response",
    )
}

#[path = "chat_stream.rs"]
mod chat_stream;

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

#[path = "response_validation.rs"]
mod response_validation;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_parser_accepts_delta_and_date_with_saturating_bounds() {
        let now = std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let make_headers = |value: &str| {
            reqwest::header::HeaderMap::from_iter([(
                reqwest::header::RETRY_AFTER,
                reqwest::header::HeaderValue::from_str(value).unwrap(),
            )])
        };
        assert_eq!(
            retry_after_delay(&make_headers("37"), now),
            Some(Duration::from_secs(37))
        );
        assert_eq!(
            retry_after_delay(&make_headers("999999999999999999999999999999"), now),
            Some(Duration::from_secs(86400))
        );
        assert_eq!(
            retry_after_delay(&make_headers("0"), now),
            Some(Duration::ZERO)
        );
        let future = httpdate::fmt_http_date(now + Duration::from_secs(75));
        assert_eq!(
            retry_after_delay(&make_headers(&future), now),
            Some(Duration::from_secs(75))
        );
        let expired = httpdate::fmt_http_date(now - Duration::from_secs(30));
        assert_eq!(retry_after_delay(&make_headers(&expired), now), None);
        assert_eq!(retry_after_delay(&make_headers("not-a-date"), now), None);
        assert_eq!(retry_after_delay(&make_headers("-5"), now), None);
    }

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
        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("authorization", "Bearer client")
            .body(Body::from(
                r#"{"model":"demo","messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .unwrap();
        let response = tokio::time::timeout(
            Duration::from_millis(200),
            authenticated_body(&state, request),
        )
        .await
        .expect("overload should fail fast");
        let response = match response {
            Err(error) => error.into_response(),
            Ok(_) => panic!("overloaded request was admitted"),
        };
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
                .prometheus(MAX_INFLIGHT_REQUESTS, 0)
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
        assert!(response_validation::valid_result(
            Endpoint::Embeddings,
            &request,
            &json!({
                "data":[{"embedding":[0.1,0.2],"index":0}]
            })
        ));
        assert!(!response_validation::valid_result(
            Endpoint::Embeddings,
            &request,
            &json!({
                "data":[{"embedding":[]}]
            })
        ));
    }
}
