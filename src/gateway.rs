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
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    auto_decider::AutoDecider,
    auto_health::AutoHealth,
    circuit::{Circuit, Ticket},
    config::Config,
    keypool::{KeyHealthSnapshot, KeyPool},
    metrics::Metrics,
    routing::{self, AutoRouteCache},
    telemetry::Telemetry,
    telemetry_rank::{self, TelemetryRank},
};

// Keep these children private to the gateway: only the route handlers and
// narrow pub(super) bridge methods are shared, not a second public API.
#[path = "chat_aggregate.rs"]
mod chat_aggregate;
#[path = "chat_responses.rs"]
mod chat_responses;
#[path = "chat_responses_stream.rs"]
mod chat_responses_stream;
#[path = "chat_stream.rs"]
mod chat_stream;
#[path = "dispatch.rs"]
mod dispatch;
#[path = "native_files.rs"]
mod native_files;
#[path = "response_routes.rs"]
mod response_routes;
#[path = "response_validation.rs"]
mod response_validation;
#[path = "responses.rs"]
mod responses;
#[path = "responses_format.rs"]
mod responses_format;
#[path = "responses_lifecycle.rs"]
mod responses_lifecycle;

// Share the exact usage conversion with the crate-level SSE wrapper without
// making the private Responses adapter module visible outside this gateway.
pub(crate) fn translated_responses_usage(chat: &Value) -> Result<Value, ()> {
    responses::translated_chat_usage(chat)
}

pub(crate) fn valid_native_responses(value: &Value) -> bool {
    response_validation::valid_native_responses(value)
}

pub(crate) fn selector_native_request(value: &Value) -> Result<Value, ()> {
    chat_responses::request(value)
}
pub(crate) fn selector_native_result(value: &Value, model: &str) -> Result<Value, ()> {
    chat_responses::result(value, model)
}

const MAX_REQUEST_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const MAX_INFLIGHT_REQUESTS: usize = 128;
// Bound concurrent request-body buffers and JSON trees independently from
// long-lived SSE responses, which may remain admitted after parsing is done.
const MAX_INFLIGHT_PAYLOADS: usize = 8;
// #137/F03: a buffered 32 MiB upstream JSON reply can outlive dispatch while
// a downstream TCP client reads slowly. Bound those large retained bodies
// separately from the 128 lightweight in-flight requests.
const MAX_LARGE_RESPONSE_BODIES: usize = 2;
const LARGE_RESPONSE_BODY_BYTES: usize = 128 * 1024;
// Each of the 128 admitted requests may retain a small JSON tree without
// exhausting a single-user gateway. Large trees need one of the eight
// payload reservations for their full parsed lifetime, not just the initial
// Bytes-to-Value conversion; otherwise all 128 requests can retain 8 MiB.
const LARGE_REQUEST_BODY_BYTES: usize = 128 * 1024;
const PAYLOAD_SLOT_WAIT: Duration = Duration::from_secs(1);
const REQUEST_BODY_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONCURRENT_AGGREGATIONS: usize = 4;
// Only one speculative selector call may run; the real request path never waits.
const MAX_BACKGROUND_DECIDERS: usize = 1;

pub struct AppState {
    pub config: Config,
    client: reqwest::Client,
    pools: Vec<KeyPool>,
    circuits: Vec<Option<Arc<Circuit>>>,
    client_keys: Vec<String>,
    response_routes: response_routes::ResponseRoutes,
    inflight: Arc<Semaphore>,
    payload_slots: Arc<Semaphore>,
    large_response_slots: Arc<Semaphore>,
    aggregation_slots: Arc<Semaphore>,
    schema_slots: Arc<Semaphore>,
    auto_background_slots: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    telemetry: Option<Telemetry>,
    auto_route_cache: AutoRouteCache,
    auto_decider: Option<Arc<AutoDecider>>,
    auto_telemetry_rank: Option<Arc<TelemetryRank>>,
    shadow_total: AtomicU64,
    shadow_disagreements: AtomicU64,
    shadow_unavailable: AtomicU64,
    started_at: Instant,
    // A single publication makes the route resolver, model list and detail
    // view consistent. Readers keep an immutable Arc across a live update.
    live_routes: Arc<RwLock<Arc<LiveRouteSnapshot>>>,
}

struct LiveRouteSnapshot {
    config: Arc<Config>,
    model_catalog: Value,
    model_details: HashMap<String, Value>,
    auto_health: Option<AutoHealth>,
    auto_generation: u64,
}

impl LiveRouteSnapshot {
    fn new(config: Config, previous: Option<&Self>) -> Self {
        let (model_catalog, model_details) = collect_models(&config);
        let auto_health = config.auto.health_aware.then(|| {
            previous
                .and_then(|old| old.auto_health.as_ref().map(|health| (old, health)))
                .map_or_else(
                    || AutoHealth::new(config.auto.candidates.len()),
                    |(old, health)| {
                        AutoHealth::remap(
                            health,
                            &old.config.auto.candidates,
                            &config.auto.candidates,
                        )
                    },
                )
        });
        Self {
            auto_generation: routing::config_generation(&config),
            config: Arc::new(config),
            model_catalog,
            model_details,
            auto_health,
        }
    }
}

fn collect_models(config: &Config) -> (Value, HashMap<String, Value>) {
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
                model_details
                    .entry(id.clone())
                    .or_insert_with(|| json!({"id":id,"object":"model","owned_by":provider.name}));
            }
        }
    }
    let mut data: Vec<_> = model_details.values().cloned().collect();
    data.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let model_catalog = json!({"object":"list","data":data});
    (model_catalog, model_details)
}

impl AppState {
    #[cfg(test)]
    pub(crate) fn telemetry_model_name<'a>(
        &self,
        provider_index: usize,
        model: &'a str,
    ) -> &'a str {
        let routes = self.route_snapshot();
        self.telemetry_model_name_in(&routes, provider_index, model)
    }

    /// A request or SSE stream must attribute events using the route
    /// generation with which it was admitted. A later online chains delete
    /// must not turn an already running named model into 'passthrough'.
    pub(crate) fn telemetry_model_name_in<'a>(
        &self,
        routes: &Config,
        provider_index: usize,
        model: &'a str,
    ) -> &'a str {
        let provider = &self.config.providers[provider_index];
        if provider.models.iter().any(|name| name == model)
            || routes
                .chains
                .iter()
                .flat_map(|chain| &chain.steps)
                .any(|step| step.provider == provider.name && step.model == model)
            || routes
                .auto
                .candidates
                .iter()
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
    pub fn flush_telemetry(&self, deadline: Duration) -> bool {
        self.telemetry
            .as_ref()
            .is_none_or(|telemetry| telemetry.flush(deadline))
    }
    pub fn new(
        mut config: Config,
        credentials: HashMap<String, String>,
        client_keys: Vec<String>,
    ) -> Result<Self, String> {
        config.validate()?;
        config.discover_auto_candidates();
        if config.auto.enabled && config.auto.candidates.is_empty() {
            return Err("auto discovery produced no eligible provider/model candidates".into());
        }
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
        let auto_decider =
            AutoDecider::from_config(&config, &credentials, client.clone())?.map(Arc::new);
        if config.auto.decider.enabled && auto_decider.is_none() {
            crate::ops_log::warn("auto_decider_disabled_no_key");
        }
        let auto_route_cache = AutoRouteCache::new();
        let live = Arc::new(LiveRouteSnapshot::new(config.clone(), None));
        Ok(Self {
            live_routes: Arc::new(RwLock::new(live)),
            config,
            client,
            pools,
            circuits,
            client_keys,
            response_routes: response_routes::ResponseRoutes::default(),
            inflight: Arc::new(Semaphore::new(MAX_INFLIGHT_REQUESTS)),
            payload_slots: Arc::new(Semaphore::new(MAX_INFLIGHT_PAYLOADS)),
            large_response_slots: Arc::new(Semaphore::new(MAX_LARGE_RESPONSE_BODIES)),
            aggregation_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_AGGREGATIONS)),
            schema_slots: Arc::new(Semaphore::new(4)),
            auto_background_slots: Arc::new(Semaphore::new(MAX_BACKGROUND_DECIDERS)),
            metrics: Arc::new(Metrics::default()),
            telemetry: None,
            auto_route_cache,
            auto_decider,
            auto_telemetry_rank: None,
            shadow_total: AtomicU64::new(0),
            shadow_disagreements: AtomicU64::new(0),
            shadow_unavailable: AtomicU64::new(0),
            started_at: Instant::now(),
        })
    }

    fn live_snapshot(&self) -> Arc<LiveRouteSnapshot> {
        Arc::clone(&self.live_routes.read().unwrap_or_else(|e| e.into_inner()))
    }

    pub fn route_snapshot(&self) -> Arc<Config> {
        Arc::clone(&self.live_snapshot().config)
    }

    /// Atomically publish the already-persisted and revalidated local route
    /// snapshot. Requests already underway retain the old Arc snapshot.
    pub fn replace_route_snapshot(&self, mut next: Config) -> Result<(), String> {
        next.validate()?;
        // Pools, circuit breakers, selector credentials, and request permits
        // are built once at startup and indexed by the ORIGINAL provider
        // positions. A live config can reorder provider definitions, but
        // publishing those reordered indices directly would send provider
        // A's traffic using provider B's credentials and health state.
        // Normalize harmless permutations; reject additions, removals, and
        // physical setting changes that require a full gateway restart.
        if next.providers.len() != self.config.providers.len() {
            return Err("live provider additions/removals require a gateway restart".into());
        }
        let mut physical_order = Vec::with_capacity(self.config.providers.len());
        for original in &self.config.providers {
            let Some(incoming) = next.providers.iter().find(|p| p.name == original.name) else {
                return Err("live provider additions/removals require a gateway restart".into());
            };
            if format!("{incoming:?}") != format!("{original:?}") {
                return Err("live provider configuration changes require a gateway restart".into());
            }
            physical_order.push(original.clone());
        }
        next.providers = physical_order;
        // Runtime control mutates route metadata, not the listeners, HTTP
        // timeout, optional selector, or telemetry approval. Those runtime
        // objects are constructed once and are still read from self.config.
        // Refuse a half-applied live config rather than advertise settings
        // the running gateway cannot actually honor. The candidate set can
        // change explicitly or via chain discovery below.
        let mut immutable_view = next.clone();
        immutable_view.chains = self.config.chains.clone();
        immutable_view.auto.candidates = self.config.auto.candidates.clone();
        if format!("{immutable_view:?}") != format!("{:?}", self.config) {
            return Err("live service settings changes require a gateway restart".into());
        }
        // The local control socket supplies the unmaterialized base config
        // plus current runtime chains. Re-discover after every mutation,
        // including deletions; never inherit stale discovered candidates.
        next.discover_auto_candidates();
        // Config::validate allows an initially empty Auto candidate list
        // when chain discovery is enabled, because discovery happens later.
        // A live deletion must meet the same post-discovery invariant as
        // AppState::new: never publish Auto enabled with zero actual routes
        // when the next process restart would refuse to boot.
        if next.auto.enabled && next.auto.candidates.is_empty() {
            return Err(
                "live Auto discovery produced no candidates; keep at least one candidate".into(),
            );
        }
        next.validate()?;
        // Building catalogs and hashing potentially large candidate schemas
        // can be expensive. Do it ONCE without the global route write lock;
        // request admissions must not wait while we serialize route metadata.
        let mut prepared = LiveRouteSnapshot::new(next, None);
        loop {
            let previous = self.live_snapshot();
            // Preserve health state against the last *published* generation,
            // not a snapshot that may have been superseded while preparing.
            if prepared.config.auto.health_aware {
                prepared.auto_health = Some(previous.auto_health.as_ref().map_or_else(
                    || AutoHealth::new(prepared.config.auto.candidates.len()),
                    |health| {
                        AutoHealth::remap(
                            health,
                            &previous.config.auto.candidates,
                            &prepared.config.auto.candidates,
                        )
                    },
                ));
            }
            let mut guard = self.live_routes.write().unwrap_or_else(|e| e.into_inner());
            if Arc::ptr_eq(&previous, &guard) {
                *guard = Arc::new(prepared);
                return Ok(());
            }
            // Another writer published first. Retry the inexpensive health
            // remap; catalogs, generation hash and config stay immutable.
        }
    }

    #[cfg(test)]
    fn auto_candidate_index(&self, provider_index: usize, model: &str) -> Option<usize> {
        let provider = self.config.providers.get(provider_index)?;
        self.live_snapshot()
            .config
            .auto
            .candidates
            .iter()
            .position(|candidate| candidate.provider == provider.name && candidate.model == model)
    }

    #[cfg(test)]
    fn mark_auto_failure(&self, provider_index: usize, model: &str) {
        self.mark_auto_failure_in(&self.live_snapshot(), provider_index, model);
    }

    fn mark_auto_failure_in(
        &self,
        snapshot: &LiveRouteSnapshot,
        provider_index: usize,
        model: &str,
    ) {
        if let Some(health) = &snapshot.auto_health
            && let Some(provider) = self.config.providers.get(provider_index)
            && let Some(index) = snapshot
                .config
                .auto
                .candidates
                .iter()
                .position(|candidate| {
                    candidate.provider == provider.name && candidate.model == model
                })
        {
            health.mark_failure(index);
        }
    }

    #[cfg(test)]
    fn mark_auto_success(&self, provider_index: usize, model: &str) {
        self.mark_auto_success_in(&self.live_snapshot(), provider_index, model);
    }

    fn mark_auto_success_in(
        &self,
        snapshot: &LiveRouteSnapshot,
        provider_index: usize,
        model: &str,
    ) {
        if let Some(health) = &snapshot.auto_health
            && let Some(provider) = self.config.providers.get(provider_index)
            && let Some(index) = snapshot
                .config
                .auto
                .candidates
                .iter()
                .position(|candidate| {
                    candidate.provider == provider.name && candidate.model == model
                })
        {
            health.mark_success(index);
        }
    }
    /// Optional local-only telemetry. Main enables this explicitly; test AppStates stay off.
    pub fn enable_telemetry(&mut self, data_dir: &std::path::Path) -> Result<(), String> {
        let live = self.live_snapshot();
        let restored = if live.auto_health.is_some() {
            Telemetry::load_provider_health(data_dir)
        } else {
            Vec::new()
        };
        let telemetry = Telemetry::start(data_dir, Arc::clone(&self.metrics))?;
        if let Some(health) = &live.auto_health {
            for (index, candidate) in live.config.auto.candidates.iter().enumerate() {
                let provider_label = Telemetry::normalize_label(&candidate.provider);
                let model_label = Telemetry::normalize_label(&candidate.model);
                // Sanitized/truncated labels are not injective. An unsafe
                // candidate must never inherit another model's persisted
                // health, even if its normalized key appears in SQLite.
                if provider_label != candidate.provider
                    || model_label != candidate.model
                    || candidate.model == "passthrough"
                {
                    continue;
                }
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
        if self.config.auto.telemetry_ranking.enabled {
            let initial = telemetry_rank::load(data_dir, &live.config);
            let rank = Arc::new(TelemetryRank::new(initial, live.auto_generation));
            // No request-path database reads, and no extra permanent OS thread.
            // If a test constructs state outside Tokio, the initial snapshot
            // safely expires rather than staying approved indefinitely.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let weak = Arc::downgrade(&rank);
                let routes = Arc::clone(&self.live_routes);
                let directory = data_dir.to_path_buf();
                handle.spawn(async move {
                    let mut ticker = tokio::time::interval(telemetry_rank::refresh_interval());
                    ticker.tick().await;
                    loop {
                        ticker.tick().await;
                        let Some(rank) = weak.upgrade() else {
                            break;
                        };
                        let live = Arc::clone(&routes.read().unwrap_or_else(|e| e.into_inner()));
                        let config = Arc::clone(&live.config);
                        let generation = live.auto_generation;
                        let directory = directory.clone();
                        // SQLite is exclusively accessed by a bounded blocking
                        // task, never on the gateway's async request workers.
                        let snapshot = tokio::task::spawn_blocking(move || {
                            telemetry_rank::load(&directory, &config)
                        })
                        .await
                        .ok()
                        .flatten();
                        rank.set(snapshot, generation);
                    }
                });
            }
            self.auto_telemetry_rank = Some(rank);
        }
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
        let mut key_health = KeyHealthSnapshot::default();
        for pool in &self.pools {
            key_health += pool.health_snapshot();
        }
        json!({
            "status": "running",
            "version": env!("CARGO_PKG_VERSION"),
            "uptime_seconds": self.started_at.elapsed().as_secs(),
            "providers_configured": self.config.providers.len(),
            "key_health": key_health,
            "active_requests": active,
            "circuit_open_providers": open_circuits,
            "auto_enabled": self.config.auto.enabled,
            "auto_health_aware": self.config.auto.health_aware,
            "auto_telemetry_rank_approved": self.config.auto.telemetry_ranking.enabled && self.config.auto.telemetry_ranking.approved,
            "auto_telemetry_rank_active": self.auto_telemetry_rank.as_ref().is_some_and(|rank| rank.active_for_generation(self.live_snapshot().auto_generation)),
            "auto_candidate_count": self.live_snapshot().config.auto.candidates.len(),
            "auto_shadow_total": self.shadow_total.load(Ordering::Relaxed),
            "auto_shadow_disagreements": self.shadow_disagreements.load(Ordering::Relaxed),
            "auto_shadow_unavailable": self.shadow_unavailable.load(Ordering::Relaxed),
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
        self.client_identity(headers).is_some()
    }
    fn client_identity(&self, headers: &HeaderMap) -> Option<usize> {
        let header = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))?;
        self.client_keys
            .iter()
            .position(|key| constant_time_equal(key.as_bytes(), header.as_bytes()))
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
        // Reject unrepresentable decimal hints as malformed. Never turn an
        // invalid attacker-controlled value into a full 24h quarantine.
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
        let opened = ticket.failure();
        if opened {
            Metrics::inc(&metrics.circuit_openings);
        }
        if probe && opened {
            Metrics::inc(&metrics.circuit_probe_failures);
        }
    }
}

fn record_circuit_success(ticket: &mut Option<Ticket>, metrics: &Metrics) {
    if let Some(ticket) = ticket {
        let probe = ticket.is_probe();
        let accepted = ticket.success();
        if probe && accepted {
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
        .route(
            "/v1/responses",
            post(responses::handle).get(responses_lifecycle::unsupported_transport),
        )
        .route("/v1/responses/compact", post(responses_lifecycle::compact))
        .route(
            "/v1/responses/input_tokens",
            post(responses_lifecycle::input_tokens),
        )
        .route(
            "/v1/responses/{id}",
            get(responses_lifecycle::retrieve).delete(responses_lifecycle::delete),
        )
        .route(
            "/v1/responses/{id}/cancel",
            post(responses_lifecycle::cancel),
        )
        .route(
            "/v1/responses/{id}/input_items",
            get(responses_lifecycle::input_items),
        )
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
    let snapshot = state.live_snapshot().model_catalog.clone();
    (StatusCode::OK, Json(snapshot)).into_response()
}

async fn model_detail(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath(model): AxumPath<String>,
) -> Response {
    if !state.allowed(&headers) {
        return unauthorized();
    }
    let live = state.live_snapshot();
    match live.model_details.get(&model) {
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
    let original_large_payload = body.len() >= LARGE_REQUEST_BODY_BYTES;
    drop(body);
    dispatch::dispatch_parsed(
        state,
        value,
        original_large_payload,
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
    fn live_auto_candidate_deletion_requires_restart_viable_discovery_result() {
        let base: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://example.invalid
    key_env: K
auto:
  enabled: true
  discover_from_chains: true
  candidates:
    - provider: p
      model: fixed
chains:
  - name: existing
    steps:
      - provider: p
        model: discovered
"#,
        )
        .unwrap();
        let state = AppState::new(
            base.clone(),
            HashMap::from([("K".into(), "test-key".into())]),
            vec!["client".into()],
        )
        .unwrap();
        let before = state.route_snapshot();
        assert_eq!(before.auto.candidates.len(), 2);
        let mut delete_all = base.clone();
        delete_all.auto.candidates.clear();
        delete_all.chains.clear();
        assert!(
            delete_all.validate().is_ok(),
            "pre-discovery config allows this"
        );
        assert!(
            state
                .replace_route_snapshot(delete_all)
                .unwrap_err()
                .contains("no candidates")
        );
        assert_eq!(state.route_snapshot().auto.candidates.len(), 2);
        assert_eq!(state.route_snapshot().chains.len(), 1);
        // Clearing explicit candidates remains supported as long as chain
        // discovery can materialize at least one valid route.
        let mut discovered_only = base;
        discovered_only.auto.candidates.clear();
        state.replace_route_snapshot(discovered_only).unwrap();
        assert_eq!(state.route_snapshot().auto.candidates.len(), 1);
        assert_eq!(
            state.route_snapshot().auto.candidates[0].model,
            "discovered"
        );
    }

    #[test]
    fn dynamic_passthrough_model_names_never_enter_telemetry_labels() {
        let config: Config = serde_yaml::from_str(
            r#"
providers:
  - name: bridge
    type: openai
    base_url: https://bridge.example
    key_env: BRIDGE
    models: [listed]
chains:
  - name: named
    steps:
      - provider: bridge
        model: chain-model
auto:
  enabled: true
  candidates:
    - provider: bridge
      model: auto-model
"#,
        )
        .unwrap();
        let state = AppState::new(
            config,
            HashMap::from([("BRIDGE".to_owned(), "test-only".to_owned())]),
            vec!["client".to_owned()],
        )
        .unwrap();
        for candidate in ["listed", "chain-model", "auto-model"] {
            assert_eq!(state.telemetry_model_name(0, candidate), candidate);
        }
        for index in 0..512 {
            let untrusted = format!("private_prompt_{index}_user_controlled");
            assert_eq!(state.telemetry_model_name(0, &untrusted), "passthrough");
        }
        assert_eq!(
            state.telemetry_model_name(0, "api_key_secret_fragment"),
            "passthrough"
        );
    }

    #[test]
    fn live_chain_identity_uses_admission_generation_even_after_delete() {
        let initial: Config = serde_yaml::from_str(
            "providers:\n  - name: p\n    type: openai\n    base_url: https://example.test\n    key_env: KEY\nchains:\n  - name: fixed\n    steps:\n      - provider: p\n        model: static\n",
        )
        .unwrap();
        let state = AppState::new(
            initial.clone(),
            HashMap::from([("KEY".to_owned(), "key".to_owned())]),
            vec!["client".to_owned()],
        )
        .unwrap();
        assert_eq!(state.telemetry_model_name(0, "fresh"), "passthrough");
        let updated: Config = serde_yaml::from_str(
            "providers:\n  - name: p\n    type: openai\n    base_url: https://example.test\n    key_env: KEY\nchains:\n  - name: fixed\n    steps:\n      - provider: p\n        model: static\n  - name: dynamic\n    steps:\n      - provider: p\n        model: fresh\n",
        )
        .unwrap();
        state.replace_route_snapshot(updated).unwrap();
        let admitted = state.route_snapshot();
        assert_eq!(state.telemetry_model_name(0, "fresh"), "fresh");
        assert_eq!(
            state.telemetry_model_name_in(&admitted, 0, "fresh"),
            "fresh"
        );
        assert_eq!(
            state.telemetry_model_name(0, "unknown-password-like-model"),
            "passthrough"
        );
        state.replace_route_snapshot(initial).unwrap();
        assert_eq!(state.telemetry_model_name(0, "fresh"), "passthrough");
        assert_eq!(
            state.telemetry_model_name_in(&admitted, 0, "fresh"),
            "fresh",
            "an in-flight stream cannot be retroactively relabeled by a later chain delete"
        );
        assert_eq!(state.telemetry_model_name(0, "static"), "static");
    }

    #[test]
    fn live_auto_discovery_preserves_health_by_identity_and_drops_deleted_candidates() {
        let base: Config = serde_yaml::from_str(
            "providers:\n  - name: p\n    type: openai\n    base_url: https://example.test\n    key_env: KEY\nauto:\n  enabled: true\n  health_aware: true\n  discover_from_chains: true\n  candidates:\n    - provider: p\n      model: fixed\nchains:\n  - name: original\n    steps:\n      - provider: p\n        model: stable\n",
        )
        .unwrap();
        let state = AppState::new(
            base.clone(),
            HashMap::from([("KEY".to_owned(), "key".to_owned())]),
            vec!["client".to_owned()],
        )
        .unwrap();
        let old = state.live_snapshot();
        assert_eq!(old.config.auto.candidates.len(), 2);
        for _ in 0..3 {
            state.mark_auto_failure_in(&old, 0, "stable");
        }
        assert_eq!(old.auto_health.as_ref().unwrap().penalties(), vec![0, 100]);
        let mut added = base.clone();
        added.chains.insert(
            0,
            serde_yaml::from_str("name: dynamic\nsteps:\n  - provider: p\n    model: fresh\n")
                .unwrap(),
        );
        state.replace_route_snapshot(added).unwrap();
        let next = state.live_snapshot();
        assert_ne!(old.auto_generation, next.auto_generation);
        assert_eq!(
            next.config
                .auto
                .candidates
                .iter()
                .map(|c| c.model.as_str())
                .collect::<Vec<_>>(),
            vec!["fixed", "fresh", "stable"]
        );
        assert_eq!(
            next.auto_health.as_ref().unwrap().penalties(),
            vec![0, 0, 100]
        );
        // An admitted request using the previous generation must still
        // update the same physical candidate, not the newly inserted index.
        state.mark_auto_success_in(&old, 0, "stable");
        assert_eq!(
            next.auto_health.as_ref().unwrap().penalties(),
            vec![0, 0, 80]
        );
        state.replace_route_snapshot(base).unwrap();
        let deleted = state.live_snapshot();
        assert_eq!(deleted.config.auto.candidates.len(), 2);
        assert!(
            deleted
                .config
                .auto
                .candidates
                .iter()
                .all(|c| c.model != "fresh")
        );
        assert_eq!(
            deleted.auto_health.as_ref().unwrap().penalties(),
            vec![0, 80]
        );
    }

    #[test]
    fn concurrent_hot_route_publishes_keep_candidate_health_and_catalog_consistent() {
        use std::sync::Barrier;
        let base: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://example.invalid
    key_env: KEY
auto:
  enabled: true
  health_aware: true
  discover_from_chains: true
  candidates:
    - provider: p
      model: durable
"#,
        )
        .unwrap();
        let state = Arc::new(
            AppState::new(
                base.clone(),
                HashMap::from([("KEY".into(), "secret".into())]),
                vec!["client".into()],
            )
            .unwrap(),
        );
        for _ in 0..3 {
            state.mark_auto_failure(0, "durable");
        }
        assert_eq!(
            state
                .live_snapshot()
                .auto_health
                .as_ref()
                .unwrap()
                .penalties(),
            vec![100]
        );
        let barrier = Arc::new(Barrier::new(5));
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let state = Arc::clone(&state);
                let barrier = Arc::clone(&barrier);
                let base = base.clone();
                scope.spawn(move || {
                    barrier.wait();
                    for revision in 0..60 {
                        let mut config = base.clone();
                        config.chains.push(
                            serde_yaml::from_str::<crate::config::Chain>(&format!(
                                "name: chain-{worker}-{revision}\nsteps:\n  - provider: p\n    model: transient-{worker}-{revision}\n"
                            ))
                            .unwrap(),
                        );
                        state.replace_route_snapshot(config).unwrap();
                        let snapshot = state.live_snapshot();
                        assert_eq!(snapshot.config.chains.len(), 1);
                        let chain_name = &snapshot.config.chains[0].name;
                        assert_eq!(snapshot.model_details[chain_name]["id"].as_str(), Some(chain_name.as_str()));
                        assert_eq!(snapshot.auto_health.as_ref().unwrap().penalties()[0], 100);
                    }
                });
            }
            barrier.wait();
        });
        let latest = state.live_snapshot();
        assert_eq!(latest.config.chains.len(), 1);
        assert_eq!(latest.auto_health.as_ref().unwrap().penalties()[0], 100);
    }

    #[test]
    fn online_route_and_model_views_publish_one_consistent_generation() {
        use std::sync::Barrier;

        let config_for = |model: &str| -> Config {
            serde_yaml::from_str(&format!(
                "providers:\n  - name: p\n    type: openai\n    base_url: https://provider.example\n    key_env: KEY\nchains:\n  - name: chain-{model}\n    steps:\n      - provider: p\n        model: {model}\n"
            ))
            .unwrap()
        };
        let state = Arc::new(
            AppState::new(
                config_for("before"),
                HashMap::from([("KEY".to_owned(), "secret".to_owned())]),
                vec!["client".to_owned()],
            )
            .unwrap(),
        );
        let held_before = state.live_snapshot();
        let ready = Arc::new(Barrier::new(5));
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let state = Arc::clone(&state);
                let ready = Arc::clone(&ready);
                scope.spawn(move || {
                    ready.wait();
                    for _ in 0..600 {
                        let snapshot = state.live_snapshot();
                        let configured = &snapshot.config.chains[0].steps[0].model;
                        let route_name = &snapshot.config.chains[0].name;
                        assert_eq!(route_name, &format!("chain-{configured}"));
                        let detail = snapshot.model_details.get(route_name).unwrap();
                        let listed = snapshot.model_catalog["data"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|value| value["id"].as_str() == Some(route_name.as_str()))
                            .unwrap();
                        assert_eq!(detail, listed);
                        assert_eq!(detail["id"].as_str(), Some(route_name.as_str()));
                        assert!(matches!(configured.as_str(), "before" | "after"));
                        // Every reader owns one internally coherent
                        // generation; no separate catalog/details locks.
                        assert_eq!(
                            snapshot.model_catalog["data"][0]["id"].as_str(),
                            Some(route_name.as_str())
                        );
                        assert_eq!(snapshot.model_details.len(), 1);
                    }
                });
            }
            ready.wait();
            for index in 0..100 {
                state
                    .replace_route_snapshot(config_for(if index % 2 == 0 {
                        "after"
                    } else {
                        "before"
                    }))
                    .unwrap();
            }
        });
        assert_eq!(
            held_before.config.chains[0].steps[0].model, "before",
            "in-flight requests keep their original route snapshot"
        );
        assert_eq!(
            held_before.model_details["chain-before"]["id"],
            "chain-before"
        );
        let latest = state.live_snapshot();
        assert_eq!(latest.config.chains[0].steps[0].model, "before");
    }

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
        let live = state.live_snapshot();
        let health = live.auto_health.as_ref().unwrap();
        assert_eq!(health.penalties(), vec![100, 0]);
        state.mark_auto_success(0, "stable");
        assert_eq!(health.penalties(), vec![100, 0]);
        state.mark_auto_success(0, "unstable");
        assert_eq!(health.penalties(), vec![80, 0]);
    }

    #[test]
    fn normalized_model_collision_cannot_restore_wrong_auto_candidate_health() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("telemetry")).unwrap();
        let db = rusqlite::Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
        db.execute_batch(
            "CREATE TABLE provider_health(
                provider TEXT NOT NULL, model TEXT NOT NULL,
                penalty INTEGER NOT NULL, consecutive_failures INTEGER NOT NULL,
                demoted INTEGER NOT NULL, last_failure_ts INTEGER NOT NULL,
                last_success_ts INTEGER NOT NULL, updated_ts INTEGER NOT NULL,
                identity_verified INTEGER NOT NULL,
                PRIMARY KEY(provider, model)
             );",
        )
        .unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        db.execute(
            "INSERT INTO provider_health VALUES('p','model_a',100,3,1,?1,0,?1,1)",
            [now],
        )
        .unwrap();
        drop(db);
        let config: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://example.test
    key_env: K
    models: ["model a", model_a]
auto:
  enabled: true
  health_aware: true
  candidates:
    - provider: p
      model: "model a"
    - provider: p
      model: model_a
"#,
        )
        .unwrap();
        let mut state = AppState::new(
            config,
            HashMap::from([("K".to_owned(), "test-key".to_owned())]),
            vec!["client".to_owned()],
        )
        .unwrap();
        state.enable_telemetry(dir.path()).unwrap();
        assert_eq!(
            state
                .live_snapshot()
                .auto_health
                .as_ref()
                .unwrap()
                .penalties(),
            vec![0, 100],
            "only the exact unmodified model_a may inherit verified health"
        );
    }

    #[test]
    fn retry_after_parser_caps_valid_hints_but_rejects_overflow() {
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
            None
        );
        assert_eq!(
            retry_after_delay(&make_headers("86401"), now),
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
