//! Public API entry: routes, shared AppState, authentication and admission.
//! Endpoint adapters, stream lifecycle, response checks and dispatch stay
//! in private child modules; there is exactly one upstream candidate walk.
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, FromRequest, Path as AxumPath, State},
    http::{HeaderMap, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    circuit::{Circuit, Ticket},
    config::Config,
    keypool::KeyPool,
    metrics::Metrics,
};

// Keep these children private to the gateway: only the route handlers and
// narrow pub(super) bridge methods are shared, not a second public API.
#[path = "chat_aggregate.rs"]
mod chat_aggregate;
#[path = "chat_stream.rs"]
mod chat_stream;
#[path = "dispatch.rs"]
mod dispatch;
#[path = "response_validation.rs"]
mod response_validation;
#[path = "responses.rs"]
mod responses;
#[path = "responses_format.rs"]
mod responses_format;

const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const MAX_INFLIGHT_REQUESTS: usize = 128;
const MAX_CONCURRENT_AGGREGATIONS: usize = 4;

pub struct AppState {
    pub config: Config,
    client: reqwest::Client,
    pools: Vec<KeyPool>,
    circuits: Vec<Option<Arc<Circuit>>>,
    client_keys: Vec<String>,
    inflight: Arc<Semaphore>,
    aggregation_slots: Arc<Semaphore>,
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
        if config.auto.enabled {
            model_details.insert(
                "auto".to_owned(),
                json!({"id":"auto","object":"model","owned_by":"guvna-auto"}),
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
            aggregation_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_AGGREGATIONS)),
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
        .route("/v1/auto", post(auto))
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

async fn chat(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (body, permit) = match authenticated_body(&state, request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };
    dispatch::dispatch(state, body, dispatch::Endpoint::Chat, permit).await
}
/// Optional Auto Chat alias. It rewrites only the logical model name and
/// then invokes the exact same dispatch loop as ordinary Chat completions.
async fn auto(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (body, permit) = match authenticated_body(&state, request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };
    if !state.config.auto.enabled {
        return error(
            StatusCode::NOT_FOUND,
            "auto_disabled",
            "Auto routing is not configured",
        );
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
    if value.get("model").is_some_and(|model| model != "auto") {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_auto_model",
            "/v1/auto accepts only model auto or an omitted model",
        );
    }
    value
        .as_object_mut()
        .expect("checked")
        .insert("model".into(), Value::String("auto".into()));
    dispatch::dispatch(
        state,
        Bytes::from(value.to_string()),
        dispatch::Endpoint::Chat,
        permit,
    )
    .await
}

async fn embeddings(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (body, permit) = match authenticated_body(&state, request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };
    dispatch::dispatch(state, body, dispatch::Endpoint::Embeddings, permit).await
}

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
    fn protects_credentials() {
        assert!(constant_time_equal(b"a", b"a"));
        assert!(!constant_time_equal(b"a", b"b"));
        assert!(!constant_time_equal(b"a", b"aa"));
    }
}
