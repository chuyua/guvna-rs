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
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    auto_health::AutoHealth,
    circuit::{Circuit, Ticket},
    config::Config,
    keypool::KeyPool,
    metrics::Metrics,
    routing::{self, AutoRouteCache},
    telemetry::Telemetry,
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

const MAX_REQUEST_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const MAX_INFLIGHT_REQUESTS: usize = 128;
// Bound concurrent request-body buffers and JSON trees independently from
// long-lived SSE responses, which may remain admitted after parsing is done.
const MAX_INFLIGHT_PAYLOADS: usize = 8;
const PAYLOAD_SLOT_WAIT: Duration = Duration::from_secs(1);
const REQUEST_BODY_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONCURRENT_AGGREGATIONS: usize = 4;

pub struct AppState {
    pub config: Config,
    client: reqwest::Client,
    pools: Vec<KeyPool>,
    circuits: Vec<Option<Arc<Circuit>>>,
    client_keys: Vec<String>,
    inflight: Arc<Semaphore>,
    payload_slots: Arc<Semaphore>,
    aggregation_slots: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    telemetry: Option<Telemetry>,
    auto_health: Option<AutoHealth>,
    auto_route_cache: AutoRouteCache,
    auto_generation: u64,
    started_at: Instant,
    model_catalog: Value,
    model_details: HashMap<String, Value>,
}

impl AppState {
    pub(crate) fn telemetry_model_name<'a>(
        &self,
        provider_index: usize,
        model: &'a str,
    ) -> &'a str {
        let provider = &self.config.providers[provider_index];
        if provider.models.iter().any(|name| name == model)
            || self
                .config
                .chains
                .iter()
                .flat_map(|chain| &chain.steps)
                .any(|step| step.provider == provider.name && step.model == model)
            || self
                .config
                .auto
                .candidates
                .iter()
                .any(|step| step.provider == provider.name && step.model == model)
        {
            model
        } else {
            "passthrough"
        }
    }
    pub fn record_telemetry_start_failure(&self) {
        Metrics::inc(&self.metrics.telemetry_write_failures);
    }
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
        let auto_generation = routing::config_generation(&config);
        let auto_route_cache = AutoRouteCache::new();
        let auto_health = config
            .auto
            .health_aware
            .then(|| AutoHealth::new(config.auto.candidates.len()));
        Ok(Self {
            model_catalog,
            model_details,
            config,
            client,
            pools,
            circuits,
            client_keys,
            inflight: Arc::new(Semaphore::new(MAX_INFLIGHT_REQUESTS)),
            payload_slots: Arc::new(Semaphore::new(MAX_INFLIGHT_PAYLOADS)),
            aggregation_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_AGGREGATIONS)),
            metrics: Arc::new(Metrics::default()),
            telemetry: None,
            auto_health,
            auto_route_cache,
            auto_generation,
            started_at: Instant::now(),
        })
    }

    fn auto_candidate_index(&self, provider_index: usize, model: &str) -> Option<usize> {
        let provider = self.config.providers.get(provider_index)?;
        self.config
            .auto
            .candidates
            .iter()
            .position(|candidate| candidate.provider == provider.name && candidate.model == model)
    }

    fn mark_auto_failure(&self, provider_index: usize, model: &str) {
        if let Some(health) = &self.auto_health
            && let Some(index) = self.auto_candidate_index(provider_index, model)
        {
            health.mark_failure(index);
        }
    }

    fn mark_auto_success(&self, provider_index: usize, model: &str) {
        if let Some(health) = &self.auto_health
            && let Some(index) = self.auto_candidate_index(provider_index, model)
        {
            health.mark_success(index);
        }
    }
    /// Optional local-only telemetry. Main enables this explicitly; test AppStates stay off.
    pub fn enable_telemetry(&mut self, data_dir: &std::path::Path) -> Result<(), String> {
        let restored = if self.auto_health.is_some() {
            Telemetry::load_provider_health(data_dir)
        } else {
            Vec::new()
        };
        let telemetry = Telemetry::start(data_dir, Arc::clone(&self.metrics))?;
        if let Some(health) = &self.auto_health {
            for (index, candidate) in self.config.auto.candidates.iter().enumerate() {
                let provider_label = Telemetry::normalize_label(&candidate.provider);
                let model_label = Telemetry::normalize_label(&candidate.model);
                if let Some(snapshot) = restored
                    .iter()
                    .find(|item| item.provider == provider_label && item.model == model_label)
                {
                    health.restore(
                        index,
                        snapshot.penalty,
                        snapshot.consecutive_failures,
                        snapshot.demoted,
                        snapshot.last_failure_secs,
                    );
                }
            }
        }
        self.telemetry = Some(telemetry);
        Ok(())
    }
    pub fn runtime_status(&self) -> Value {
        let active = MAX_INFLIGHT_REQUESTS - self.inflight.available_permits();
        let open_circuits = self
            .circuits
            .iter()
            .filter_map(Option::as_ref)
            .filter(|c| c.is_open())
            .count();
        json!({
            "status": "running",
            "version": env!("CARGO_PKG_VERSION"),
            "uptime_seconds": self.started_at.elapsed().as_secs(),
            "providers_configured": self.config.providers.len(),
            "active_requests": active,
            "circuit_open_providers": open_circuits,
            "auto_enabled": self.config.auto.enabled,
            "auto_health_aware": self.config.auto.health_aware,
            "sqlite_telemetry_enabled": self.telemetry.is_some(),
            "requests_dispatched": self.metrics.requests_dispatched.load(Ordering::Relaxed),
            "upstream_attempts": self.metrics.upstream_attempts.load(Ordering::Relaxed),
            "upstream_success": self.metrics.upstream_success.load(Ordering::Relaxed),
            "upstream_http_failures": self.metrics.upstream_http_failures.load(Ordering::Relaxed),
            "upstream_transport_failures": self.metrics.upstream_transport_failures.load(Ordering::Relaxed),
            "telemetry_written": self.metrics.telemetry_written.load(Ordering::Relaxed),
            "telemetry_dropped": self.metrics.telemetry_dropped.load(Ordering::Relaxed),
            "telemetry_write_failures": self.metrics.telemetry_write_failures.load(Ordering::Relaxed)
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
        Duration::from_secs(raw.parse::<u64>().ok()?)
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
#[derive(Debug)]
enum RequestBodyError {
    Unauthorized,
    Overloaded,
    Invalid(StatusCode),
    Timeout,
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
            Self::Timeout => error(
                StatusCode::REQUEST_TIMEOUT,
                "request_body_timeout",
                "Request body was not fully received before the deadline",
            ),
        }
    }
}

async fn authenticated_body(
    state: &Arc<AppState>,
    request: Request<Body>,
) -> Result<(Bytes, OwnedSemaphorePermit, OwnedSemaphorePermit), RequestBodyError> {
    if !state.allowed(request.headers()) {
        return Err(RequestBodyError::Unauthorized);
    }
    // Bound the number of waiting requests before waiting for a payload slot.
    let permit = state.inflight.clone().try_acquire_owned().map_err(|_| {
        Metrics::inc(&state.metrics.admission_rejections);
        RequestBodyError::Overloaded
    })?;
    // Reserve bounded payload memory before reading any body bytes. Waiting is
    // short and bounded; body reads also have a hard deadline so slow uploads
    // cannot monopolize the memory slots indefinitely.
    let payload_permit = match tokio::time::timeout(
        PAYLOAD_SLOT_WAIT,
        state.payload_slots.clone().acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        _ => {
            Metrics::inc(&state.metrics.admission_rejections);
            return Err(RequestBodyError::Overloaded);
        }
    };
    let body = match tokio::time::timeout(REQUEST_BODY_TIMEOUT, Bytes::from_request(request, state))
        .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(rejection)) => return Err(RequestBodyError::Invalid(rejection.status())),
        Err(_) => return Err(RequestBodyError::Timeout),
    };
    Ok((body, permit, payload_permit))
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
    app_with_state(state).0
}

pub fn app_with_state(state: AppState) -> (Router, Arc<AppState>) {
    let state = Arc::new(state);
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_endpoint))
        .route("/v1/models", get(models))
        .route("/v1/models/{model}", get(model_detail))
        .route("/v1/chat/completions", post(chat))
        .route("/v1/auto", post(auto))
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/responses", post(responses::handle))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(Arc::clone(&state));
    (app, state)
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
    let (body, permit, payload_permit) = match authenticated_body(&state, request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };
    dispatch::dispatch(
        state,
        body,
        dispatch::Endpoint::Chat,
        permit,
        Some(payload_permit),
    )
    .await
}
/// Optional Auto Chat alias. It rewrites only the logical model name and
/// then invokes the exact same dispatch loop as ordinary Chat completions.
async fn auto(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (body, permit, payload_permit) = match authenticated_body(&state, request).await {
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
        Some(payload_permit),
    )
    .await
}

async fn embeddings(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (body, permit, payload_permit) = match authenticated_body(&state, request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };
    dispatch::dispatch(
        state,
        body,
        dispatch::Endpoint::Embeddings,
        permit,
        Some(payload_permit),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn auto_model_health_isolated_within_shared_provider() {
        let config: Config = serde_yaml::from_str(
            r#"
providers:
  - name: shared
    type: openai
    base_url: https://shared.example
    key_env: SHARED
    models: [unstable, stable]
auto:
  enabled: true
  health_aware: true
  candidates:
    - provider: shared
      model: unstable
      priority: 1
    - provider: shared
      model: stable
      priority: 2
"#,
        )
        .unwrap();
        let state = AppState::new(
            config,
            HashMap::from([("SHARED".to_owned(), "placeholder".to_owned())]),
            vec!["local-client".to_owned()],
        )
        .unwrap();
        assert_eq!(state.auto_candidate_index(0, "unstable"), Some(0));
        assert_eq!(state.auto_candidate_index(0, "stable"), Some(1));
        assert_eq!(state.auto_candidate_index(0, "other"), None);
        for _ in 0..3 {
            state.mark_auto_failure(0, "unstable");
        }
        let health = state.auto_health.as_ref().unwrap();
        assert_eq!(health.penalties(), vec![100, 0]);
        state.mark_auto_success(0, "stable");
        assert_eq!(health.penalties(), vec![100, 0]);
        state.mark_auto_success(0, "unstable");
        assert_eq!(health.penalties(), vec![80, 0]);
    }
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

    #[tokio::test]
    async fn payload_memory_gate_rejects_before_reading_body_and_releases_slots() {
        let config: Config = serde_yaml::from_str(
            "providers:\n  - name: p\n    type: openai\n    base_url: http://127.0.0.1:1\n    key_env: KEY\nchains:\n  - name: demo\n    steps:\n      - provider: p\n        model: test\n",
        )
        .unwrap();
        let state = Arc::new(
            AppState::new(
                config,
                HashMap::from([("KEY".to_owned(), "test-only".to_owned())]),
                vec!["client".to_owned()],
            )
            .unwrap(),
        );
        let all_payload_slots = state
            .payload_slots
            .clone()
            .try_acquire_many_owned(MAX_INFLIGHT_PAYLOADS as u32)
            .unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let body = Body::from_stream(futures_util::stream::poll_fn(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::<Option<Result<Bytes, std::io::Error>>>::Pending
        }));
        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("authorization", "Bearer client")
            .body(body)
            .unwrap();
        let result =
            tokio::time::timeout(Duration::from_secs(2), authenticated_body(&state, request))
                .await
                .expect("payload overload should fail fast");
        let response = match result {
            Err(error) => error.into_response(),
            Ok(_) => panic!("request was admitted without a payload-memory slot"),
        };
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "body was polled before memory admission"
        );
        assert_eq!(
            state.metrics.admission_rejections.load(Ordering::Relaxed),
            1
        );

        drop(all_payload_slots);
        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("authorization", "Bearer client")
            .body(Body::from("{}"))
            .unwrap();
        let admitted = authenticated_body(&state, request)
            .await
            .expect("payload slot should recover after release");
        drop(admitted);
        assert_eq!(
            state.payload_slots.available_permits(),
            MAX_INFLIGHT_PAYLOADS
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_authorized_body_times_out_and_releases_both_permits() {
        let config: Config = serde_yaml::from_str(
            "providers:\n  - name: p\n    type: openai\n    base_url: http://127.0.0.1:1\n    key_env: KEY\nchains:\n  - name: demo\n    steps:\n      - provider: p\n        model: test\n",
        )
        .unwrap();
        let state = Arc::new(
            AppState::new(
                config,
                HashMap::from([("KEY".to_owned(), "test-only".to_owned())]),
                vec!["client".to_owned()],
            )
            .unwrap(),
        );
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let body = Body::from_stream(futures_util::stream::poll_fn(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::<Option<Result<Bytes, std::io::Error>>>::Pending
        }));
        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("authorization", "Bearer client")
            .body(body)
            .unwrap();
        let state_for_task = Arc::clone(&state);
        let task = tokio::spawn(async move { authenticated_body(&state_for_task, request).await });
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if polls.load(Ordering::SeqCst) > 0 {
                break;
            }
        }
        assert!(
            polls.load(Ordering::SeqCst) > 0,
            "body should be polled after admission"
        );
        tokio::time::advance(REQUEST_BODY_TIMEOUT).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if task.is_finished() {
                break;
            }
        }
        let result = task.await.unwrap();
        assert!(matches!(result, Err(RequestBodyError::Timeout)));
        assert_eq!(state.inflight.available_permits(), MAX_INFLIGHT_REQUESTS);
        assert_eq!(
            state.payload_slots.available_permits(),
            MAX_INFLIGHT_PAYLOADS
        );
    }

    #[test]
    fn protects_credentials() {
        assert!(constant_time_equal(b"a", b"a"));
        assert!(!constant_time_equal(b"a", b"b"));
        assert!(!constant_time_equal(b"a", b"aa"));
    }
}
