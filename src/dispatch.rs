//! Single shared Chat/Embeddings (and Responses bridge) upstream candidate
//! walk. Retry, credential quarantine, failover, breaker and timeout policies
//! must remain here, never duplicated per API endpoint.
use super::{
    AppState, MAX_BODY_BYTES, chat_aggregate, chat_stream, error, record_circuit_failure,
    record_circuit_success, response_validation, retry_after_delay,
};
use crate::{
    auto_decider,
    metrics::Metrics,
    ops_log,
    outcome::{Outcome, classify},
    routing,
    sse::ChatSse,
    telemetry::TelemetryContext,
};
use axum::{
    Json,
    body::{Body, Bytes},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime},
};
use tokio::sync::OwnedSemaphorePermit;

#[derive(Clone, Copy)]
pub(super) enum Endpoint {
    Chat,
    Responses,
    Embeddings,
}
impl Endpoint {
    fn path(self) -> &'static str {
        match self {
            Self::Chat | Self::Responses => "/chat/completions",
            Self::Embeddings => "/embeddings",
        }
    }
    fn is_chat(self) -> bool {
        matches!(self, Self::Chat | Self::Responses)
    }
}

enum SavedStreamOptions {
    Missing,
    Object(Option<Value>),
    Replaced(Value),
}

/// Mutate the caller's optional SSE metadata in place rather than cloning it.
fn force_sse_usage(object: &mut serde_json::Map<String, Value>) -> SavedStreamOptions {
    match object.get_mut("stream_options") {
        Some(Value::Object(options)) => {
            SavedStreamOptions::Object(options.insert("include_usage".into(), Value::Bool(true)))
        }
        Some(value) => {
            SavedStreamOptions::Replaced(std::mem::replace(value, json!({"include_usage":true})))
        }
        None => {
            object.insert("stream_options".into(), json!({"include_usage":true}));
            SavedStreamOptions::Missing
        }
    }
}

fn restore_sse_usage(object: &mut serde_json::Map<String, Value>, old: SavedStreamOptions) {
    match old {
        SavedStreamOptions::Missing => {
            object.remove("stream_options");
        }
        SavedStreamOptions::Object(previous) => {
            let options = object
                .get_mut("stream_options")
                .and_then(Value::as_object_mut)
                .expect("usage options object still present");
            if let Some(previous) = previous {
                options.insert("include_usage".into(), previous);
            } else {
                options.remove("include_usage");
            }
        }
        SavedStreamOptions::Replaced(previous) => {
            object.insert("stream_options".into(), previous);
        }
    }
}

pub(super) async fn dispatch(
    state: Arc<AppState>,
    body: Bytes,
    endpoint: Endpoint,
    admission: OwnedSemaphorePermit,
    payload_permit: Option<OwnedSemaphorePermit>,
) -> Response {
    dispatch_request(
        state,
        DispatchInput::Raw(body),
        endpoint,
        admission,
        payload_permit,
    )
    .await
}

/// Auto and Responses already validate/translate a parsed object. Retain
/// a single candidate walk without serializing and parsing the JSON again.
pub(super) async fn dispatch_parsed(
    state: Arc<AppState>,
    request: Value,
    original_large_payload: bool,
    endpoint: Endpoint,
    admission: OwnedSemaphorePermit,
    payload_permit: Option<OwnedSemaphorePermit>,
) -> Response {
    dispatch_request(
        state,
        DispatchInput::Parsed {
            request,
            original_large_payload,
        },
        endpoint,
        admission,
        payload_permit,
    )
    .await
}

enum DispatchInput {
    Raw(Bytes),
    Parsed {
        request: Value,
        original_large_payload: bool,
    },
}

async fn dispatch_request(
    state: Arc<AppState>,
    input: DispatchInput,
    endpoint: Endpoint,
    admission: OwnedSemaphorePermit,
    payload_permit: Option<OwnedSemaphorePermit>,
) -> Response {
    let started = tokio::time::Instant::now();
    let id = ops_log::next_request_id();
    // Correlation survives process restarts without deriving an id from user data.
    let telemetry = state.telemetry.clone();
    let request_id = telemetry.as_ref().map(|_| {
        format!(
            "{:x}-{:x}",
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            id
        )
    });
    let metrics = Arc::clone(&state.metrics);
    let response = dispatch_inner(
        state,
        input,
        endpoint,
        admission,
        payload_permit,
        started,
        request_id.as_deref().unwrap_or(""),
    )
    .await;
    let telemetry_context = response.extensions().get::<TelemetryContext>().cloned();
    let sse_headers = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .is_some_and(|value| value.as_bytes().starts_with(b"text/event-stream"));
    ops_log::request_finished(
        id,
        started.elapsed(),
        response.status().as_u16(),
        sse_headers,
    );
    if let Some(telemetry) = telemetry {
        let label = match endpoint {
            Endpoint::Chat => "chat",
            Endpoint::Responses => "responses",
            Endpoint::Embeddings => "embeddings",
        };
        let mut context = telemetry_context
            .unwrap_or_else(|| TelemetryContext::local(response.status().as_u16(), sse_headers));
        if let Some(request_id) = &request_id {
            context.ensure_request_id(request_id);
        }
        telemetry.submit(
            label,
            response.status().as_u16(),
            started.elapsed(),
            sse_headers,
            Some(context),
            &metrics,
        );
    }
    response
}

/// Decider advice can reorder only within the current health penalty tier.
/// Recheck after any asynchronous selector call, before sending an upstream request.
fn preserve_auto_health_priority(
    config: &crate::config::Config,
    steps: &mut [routing::ResolvedStep],
    penalties: &[u8],
) {
    if !config.auto.health_aware || !penalties.iter().any(|value| *value > 0) {
        return;
    }
    steps.sort_by_key(|step| {
        let provider = config
            .providers
            .get(step.provider_index)
            .map(|p| p.name.as_str());
        config
            .auto
            .candidates
            .iter()
            .position(|candidate| {
                Some(candidate.provider.as_str()) == provider && candidate.model == step.model
            })
            .and_then(|index| penalties.get(index).copied())
            .unwrap_or(u8::MAX)
    });
}

/// Shadow is advisory only: every available proposal or unavailable attempt
/// must be counted once, without independently replaying the business
/// candidate loop. Share this one outcome path across successful selector
/// calls, semaphore saturation and missing selector credentials.
fn record_shadow_observation<T: auto_decider::SelectorIdentity>(
    state: &AppState,
    config: &crate::config::Config,
    request_id: &str,
    baseline: &[T],
    proposal: Option<&[usize]>,
) {
    if let (Some(telemetry), Some(expected)) = (&state.telemetry, baseline.first())
        && let Some(provider) = config.providers.get(expected.provider_index())
    {
        let suggested = proposal
            .and_then(|order| order.first())
            .and_then(|&index| baseline.get(index))
            .and_then(|step| {
                config
                    .providers
                    .get(step.provider_index())
                    .map(|provider| (provider.name.as_str(), step.model()))
            });
        telemetry.observe_shadow(
            request_id,
            (provider.name.as_str(), expected.model()),
            suggested,
            &state.metrics,
        );
    }
    match proposal {
        Some(order) => {
            state.shadow_total.fetch_add(1, Ordering::Relaxed);
            if let (Some(expected), Some(actual)) = (
                baseline.first(),
                order.first().and_then(|&index| baseline.get(index)),
            ) && (expected.provider_index() != actual.provider_index()
                || expected.model() != actual.model())
            {
                state.shadow_disagreements.fetch_add(1, Ordering::Relaxed);
            }
        }
        None => {
            state.shadow_unavailable.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn attach_telemetry_context(mut response: Response, context: Option<TelemetryContext>) -> Response {
    if let Some(context) = context {
        response.extensions_mut().insert(context);
    }
    response
}

/// Bound large JSON response memory until the body is drained or dropped.
/// A cloned Arc also spans the non-streaming Responses bridge conversion.
#[derive(Clone)]
pub(super) struct BufferedResponseLease {
    _permit: Arc<OwnedSemaphorePermit>,
}

/// An internal, already-validated Chat JSON tree for the non-streaming
/// Responses bridge. Returning it as an extension avoids serializing a
/// potentially 32 MiB Chat body only to parse it back into a Value.
#[derive(Clone)]
pub(super) struct ParsedChatResponse(pub(super) Value);

pub(super) fn hold_buffered_response(
    response: Response,
    reservation: Option<BufferedResponseLease>,
) -> Response {
    let Some(reservation) = reservation else {
        return response;
    };
    let (mut parts, body) = response.into_parts();
    parts.extensions.insert(reservation.clone());
    let source = Box::pin(body.into_data_stream());
    // Json(value) normally produces ONE enormous Bytes frame. A downstream
    // TCP sender could buffer that whole frame, poll Body to EOF and release
    // its reservation while a slow client still has megabytes outstanding.
    // Frame at most 16 KiB so Hyper's transport backpressure keeps the Body
    // and its memory lease live until the large buffer is truly drained.
    const FRAME_BYTES: usize = 16 * 1024;
    let guarded = futures_util::stream::unfold(
        (source, Bytes::new(), reservation),
        |(mut source, mut pending, reservation)| async move {
            loop {
                if !pending.is_empty() {
                    let frame = pending.split_to(pending.len().min(FRAME_BYTES));
                    return Some((Ok(frame), (source, pending, reservation)));
                }
                match source.next().await {
                    Some(Ok(data)) => pending = data,
                    Some(Err(error)) => {
                        return Some((Err(error), (source, pending, reservation)));
                    }
                    None => return None,
                }
            }
        },
    );
    Response::from_parts(parts, Body::from_stream(guarded))
}

#[derive(Debug)]
enum JsonReadFailure {
    Invalid,
    MemoryBusy,
    Transport,
}

fn outcome_failure_type(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Success => "success",
        Outcome::CredentialAuth => "credential_error",
        Outcome::RateLimited => "rate_limited",
        Outcome::ProviderTransient => "provider_5xx",
        Outcome::TransportFailure => "transport_error",
        Outcome::CallerRejected => "provider_4xx",
    }
}
fn telemetry_endpoint(endpoint: Endpoint) -> &'static str {
    match endpoint {
        Endpoint::Chat => "chat",
        Endpoint::Responses => "responses",
        Endpoint::Embeddings => "embeddings",
    }
}

fn fallback_failure_status(failure_type: &'static str) -> u16 {
    match failure_type {
        "rate_limited" => 429,
        "credential_error" => 401,
        "caller_error" | "provider_4xx" => 400,
        "provider_5xx" | "upstream_unavailable" | "circuit_open" | "aggregation_busy" => 503,
        "timeout" => 504,
        "capability_mismatch" => 422,
        "local_error" => 500,
        _ => 502,
    }
}

fn record_intermediate_candidate_failure(
    state: &AppState,
    endpoint: Endpoint,
    context: TelemetryContext,
    elapsed: Duration,
    attempted_network: bool,
) {
    let failure_type = context.failure_type();
    if matches!(failure_type, "success" | "stream_started") {
        return;
    }
    if !attempted_network
        && !matches!(
            failure_type,
            "circuit_open" | "capability_mismatch" | "credential_error" | "rate_limited"
        )
    {
        return;
    }
    if let Some(telemetry) = &state.telemetry {
        telemetry.observe_attempt(
            telemetry_endpoint(endpoint),
            fallback_failure_status(failure_type),
            elapsed,
            context,
            &state.metrics,
        );
    }
}
fn walk_expired(deadline: Option<tokio::time::Instant>) -> bool {
    deadline.is_some_and(|until| tokio::time::Instant::now() >= until)
}
fn nonstream_walk_budget(configured: Duration) -> Duration {
    configured.min(chat_aggregate::MAX_AGGREGATION_LIFETIME)
}

fn walk_timeout(metrics: &Metrics) -> Response {
    // Deadline errors are local policy, not malformed upstream 2xx, a
    // credential problem, or an upstream 5xx. Count them independently
    // exactly once at the terminal response construction site.
    Metrics::inc(&metrics.walk_deadline_exhaustions);
    ops_log::warn("walk_deadline_exhausted");
    error(
        StatusCode::GATEWAY_TIMEOUT,
        "upstream_walk_timeout",
        "Non-streaming upstream failover exceeded its total time budget",
    )
}

async fn dispatch_inner(
    state: Arc<AppState>,
    input: DispatchInput,
    endpoint: Endpoint,
    admission: OwnedSemaphorePermit,
    payload_permit: Option<OwnedSemaphorePermit>,
    started: tokio::time::Instant,
    request_id: &str,
) -> Response {
    let (mut request, large_payload): (Value, bool) = match input {
        DispatchInput::Raw(body) => {
            let large_payload = body.len() >= super::LARGE_REQUEST_BODY_BYTES;
            let request = match serde_json::from_slice(&body) {
                Ok(value) => value,
                Err(_) => {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "invalid_json",
                        "Expected one JSON object",
                    );
                }
            };
            // Drop the raw body as soon as the parsed tree owns its strings.
            (request, large_payload)
        }
        DispatchInput::Parsed {
            request,
            original_large_payload,
        } => (request, original_large_payload),
    };
    // F02: hold a reservation until the parsed, possibly multi-megabyte JSON
    // Value is actually dropped. Small requests release their slot after
    // parse so 128 normal Chat/SSE requests remain independently admitted.
    let _large_payload_reservation = if large_payload {
        payload_permit
    } else {
        drop(payload_permit);
        None
    };
    if !request.is_object() {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Expected JSON object",
        );
    }
    if matches!(endpoint, Endpoint::Embeddings) {
        let input_valid = match request.get("input") {
            Some(Value::String(s)) => !s.is_empty(),
            Some(Value::Array(items)) if !items.is_empty() => {
                items
                    .iter()
                    .all(|v| v.as_str().is_some_and(|s| !s.is_empty()))
                    || items.iter().all(|v| v.as_u64().is_some())
                    || items.iter().all(|v| {
                        v.as_array().is_some_and(|a| {
                            !a.is_empty() && a.iter().all(|n| n.as_u64().is_some())
                        })
                    })
            }
            _ => false,
        };
        if !input_valid {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_input",
                "Embeddings input must contain text or nonempty token arrays",
            );
        }
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
    // One monotonic deadline across ALL candidates, key attempts, 429
    // retries, aggregation permit waits and response bodies. Client-facing
    // SSE is intentionally exempt: only its header preflight is time-bounded.
    // The gateway sits behind a 120-second upstream reverse proxy in its
    // primary deployment. End all non-streaming walks with a response before
    // that external hard limit, even when an older config requests 10 minutes.
    let walk_deadline = (!streaming).then(|| {
        started
            + nonstream_walk_budget(
                state
                    .config
                    .timeout
                    .walk_duration()
                    .expect("config validated"),
            )
    });
    if streaming && !endpoint.is_chat() {
        return error(
            StatusCode::NOT_IMPLEMENTED,
            "migration_pending",
            "Only Chat Completions streaming is supported",
        );
    }
    if matches!(endpoint, Endpoint::Embeddings)
        && request.get("stream") == Some(&Value::Bool(false))
    {
        // Embeddings has no streaming wire field. A harmless explicit false
        // should not be forwarded to a strict embeddings supplier.
        request
            .as_object_mut()
            .expect("validated request object")
            .remove("stream");
    }
    let Some(model) = request
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return error(
            StatusCode::BAD_REQUEST,
            "missing_model",
            "model must be a string",
        );
    };
    let expected = request.get("n").map_or(1, |v| v.as_u64().unwrap_or(0));
    // Reject malformed n for both JSON and SSE requests locally. The 64
    // choice bound is specific to the streaming parser; a JSON-only provider
    // may legitimately support larger positive n values.
    if endpoint.is_chat() && (expected == 0 || (streaming && ChatSse::new(expected).is_err())) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_n",
            "n must be a positive integer (stream n must be 1..64)",
        );
    }
    let auto_enabled = model == "auto" && state.config.auto.enabled;
    // Go distinguishes an operator's default context floor from a caller's
    // explicit hard bounds. Capture that distinction before stripping the
    // Auto-only fields from the upstream request.
    let caller_context_bound = auto_enabled
        && ["min_context", "max_context"].iter().any(|name| {
            request
                .get(*name)
                .and_then(Value::as_u64)
                .is_some_and(|value| value > 0)
        });
    let mut auto_bounds = if auto_enabled {
        match routing::auto_context_bounds_with_default(
            &request,
            state.config.auto.use_model_registry,
            state.config.auto.default_min_context,
        ) {
            Ok(bounds) => Some(bounds),
            Err(message) => {
                return error(StatusCode::BAD_REQUEST, "invalid_auto_context", message);
            }
        }
    } else {
        None
    };
    // Go Auto context controls are selection-only. Never send them to a
    // provider (including SSE-only aggregation and later fallback steps).
    if auto_enabled {
        let object = request.as_object_mut().expect("checked above");
        object.remove("min_context");
        object.remove("max_context");
    }
    // A request owns the named-chain generation used for routing and
    // telemetry. Later local CLI mutations cannot relabel its outcome.
    let live = state.live_snapshot();
    let route_snapshot = Arc::clone(&live.config);
    // #129 audit: the initial candidate walk and the optional Decider used
    // to serialize the same (potentially multi-megabyte) messages/request
    // independently. Analyze request features once; every ordering policy
    // consumes the exact same already-validated signals and candidate list.
    let auto_signals = auto_enabled.then(|| {
        let (request_bytes, context_estimate) =
            routing::auto_request_bytes_and_context_estimate(&request);
        (
            routing::auto_input_bytes(request.get("messages")),
            request_bytes,
            context_estimate,
            routing::auto_requirements(&request),
        )
    });
    let initial_auto_penalties = if auto_enabled {
        live.auto_health
            .as_ref()
            .map(|health| health.penalties())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    // Keep byte-range eligibility and context-window budgeting separate.
    // The context estimate includes the full request, output allowance and safety reserve.
    let mut steps = match if auto_enabled {
        let (input_bytes, request_bytes, context_estimate, signals) =
            auto_signals.expect("Auto signals present");
        let (min_context, max_context) = auto_bounds.expect("Auto bounds validated");
        let requirements = signals.with_context_bounds(min_context, max_context);
        // Small scalar candidate defaults are safe to cache: the key tracks
        // which defaults the caller overrides. Large/structured defaults
        // (notably tool JSON schemas) bypass the LRU to avoid retaining up
        // to 128 additional copies in single-user low-memory deployments.
        let cacheable_defaults = routing::cacheable_auto_defaults(&route_snapshot);
        let choose = |features| {
            if cacheable_defaults {
                state.auto_route_cache.resolve_for_request(
                    &route_snapshot,
                    routing::AutoRequestBudget {
                        input_bytes,
                        request_bytes,
                        context_estimate,
                    },
                    &request,
                    features,
                    &initial_auto_penalties,
                    live.auto_generation,
                )
            } else {
                routing::resolve_auto_for_request_sized(
                    &route_snapshot,
                    input_bytes,
                    &request,
                    request_bytes,
                    features,
                    &initial_auto_penalties,
                )
            }
        };
        let mut resolved = choose(requirements);
        if matches!(&resolved, Ok(steps) if steps.is_empty())
            && !caller_context_bound
            && routing::auto_operator_default_can_degrade_for_request_sized(
                &route_snapshot,
                input_bytes,
                &request,
                request_bytes,
                requirements,
            )
        {
            // Keep context-length budgeting and capability hard filters intact.
            // A second bounded cache lookup cannot issue network requests.
            resolved = choose(requirements.with_context_bounds(0, None));
            auto_bounds = Some((0, None));
        }
        resolved
    } else {
        routing::resolve(&route_snapshot, &model)
    } {
        Ok(steps) => steps,
        Err(_) => {
            return error(
                StatusCode::NOT_FOUND,
                "unknown_model",
                "Unknown chain/model; configure a chain first",
            );
        }
    };

    if auto_enabled && steps.is_empty() {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "auto_no_eligible_candidate",
            "No configured Auto candidate satisfies input-size, context-window and model-capability constraints",
        );
    }

    if auto_enabled && let Some(rank) = &state.auto_telemetry_rank {
        // A live health demotion takes precedence over historical scoring.
        // Ranking only reorders the already-eligible bounded candidate walk.
        // Resolve and telemetry ordering see the same pre-Decider health
        // snapshot. A fresh snapshot is still taken after any async ranking
        // below to prevent promotions of newly demoted candidates.
        let penalized = initial_auto_penalties.iter().any(|value| *value > 0);
        rank.rank(&route_snapshot, &mut steps, penalized, live.auto_generation);
    }

    if auto_enabled && let Some(decider) = &state.auto_decider {
        let (input_bytes, _request_bytes, context_estimate, signals) =
            auto_signals.expect("Auto signals present");
        let (min_context, max_context) = auto_bounds.expect("Auto bounds validated");
        let requirements = signals.with_context_bounds(min_context, max_context);
        if state.config.auto.decider.shadow_only {
            // Shadow adds zero selector latency to actual response selection.
            let owner = Arc::clone(&state);
            let selector = Arc::clone(decider);
            if let Ok(permit) = state.auto_background_slots.clone().try_acquire_owned() {
                // Under pressure, skip any deep copy of default tool schemas
                // when a background selector cannot be scheduled.
                let baseline = auto_decider::selector_candidates(&steps);
                let trace_id = request_id.to_owned();
                let selector_config = Arc::clone(&route_snapshot);
                let selector_generation = live.auto_generation;
                tokio::spawn(async move {
                    let _permit = permit;
                    let choice = selector
                        .rank(
                            &selector_config,
                            &baseline,
                            input_bytes,
                            context_estimate,
                            requirements,
                            selector_generation,
                        )
                        .await;
                    record_shadow_observation(
                        &owner,
                        &selector_config,
                        &trace_id,
                        &baseline,
                        choice.as_deref(),
                    );
                });
            } else {
                // Under load, skip speculative ranking instead of queuing
                // unbounded background tasks or delaying live traffic.
                record_shadow_observation(&state, &route_snapshot, request_id, &steps, None);
            }
        } else if streaming {
            // A selector can take up to 2s; never add that delay to SSE
            // response headers or its first token. Cold/stale rankings are
            // refreshed in the background, with one in-flight fetch per cache.
            let (cached, refresh) = decider.cached_rank(
                &steps,
                input_bytes,
                context_estimate,
                requirements,
                live.auto_generation,
            );
            if refresh && let Ok(permit) = state.auto_background_slots.clone().try_acquire_owned() {
                // A non-refreshing cache hit never clones another candidate
                // list with potentially large JSON tool defaults.
                let baseline = auto_decider::selector_candidates(&steps);
                let selector = Arc::clone(decider);
                let selector_config = Arc::clone(&route_snapshot);
                let selector_generation = live.auto_generation;
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = selector
                        .rank(
                            &selector_config,
                            &baseline,
                            input_bytes,
                            context_estimate,
                            requirements,
                            selector_generation,
                        )
                        .await;
                });
            }
            if let Some(order) = cached {
                // Move the existing payload-owning candidates: do not clone
                // large tool schemas on every warm SSE selector cache hit.
                auto_decider::apply_cached_order(&mut steps, &order);
            }
        } else {
            // #129: a non-streaming Decider is advisory. Its own configured
            // timeout may exceed a small request walk budget; never let a
            // slow selector use up all the time reserved for business
            // candidates. Leave up to 250ms (or half a tiny budget) to make
            // the first real upstream attempt after falling back.
            let proposed = {
                // Drop the future (and its immutable borrow of steps) before
                // moving the owned candidate list according to its result.
                let selection = decider.rank(
                    &route_snapshot,
                    &steps,
                    input_bytes,
                    context_estimate,
                    requirements,
                    live.auto_generation,
                );
                if let Some(deadline) = walk_deadline {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    let reserve = (remaining / 2).min(Duration::from_millis(250));
                    match tokio::time::timeout_at(deadline - reserve, selection).await {
                        Ok(order) => order,
                        Err(_) => {
                            decider.cooldown_cancelled(
                                &steps,
                                input_bytes,
                                context_estimate,
                                requirements,
                                live.auto_generation,
                            );
                            None
                        }
                    }
                } else {
                    None
                }
            };
            if let Some(order) = proposed {
                auto_decider::apply_cached_order(&mut steps, &order);
            }
        }
    }

    // An opt-in shadow selector without credentials is unavailable, not
    // silently absent. Preserve the baseline and account for every real
    // request without sending any selector traffic or changing routing.
    if auto_enabled && state.config.auto.decider.shadow_only && state.auto_decider.is_none() {
        record_shadow_observation(&state, &route_snapshot, request_id, &steps, None);
    }

    if auto_enabled && let Some(health) = &live.auto_health {
        preserve_auto_health_priority(&route_snapshot, &mut steps, &health.penalties());
    }

    let mut permit = Some(admission);

    // Count actual candidate transitions, not provider-key retries.
    let mut candidates_visited = 0usize;
    let mut attempted_network = false;
    let mut last_upstream_client_error: Option<(StatusCode, Value)> = None;
    // A 4xx from one provider does not prove the caller is wrong if another
    // provider encountered a 5xx, network fault or invalid 2xx payload.
    // Return a sanitized caller rejection only when every observed network
    // result in this candidate walk was a caller rejection.
    let mut saw_non_client_network_result = false;
    let mut bypassed_circuit = false;
    let mut aggregation_busy = false;
    let mut buffered_response_busy = false;
    let mut unsupported_aggregate_logprobs = false;
    let mut unsupported_aggregate_n = false;
    // Propagate a useful downstream Retry-After only when *all* eligible
    // provider candidates are unavailable solely due to 429 credential
    // cooldown. A 5xx, invalid body, transport error, open breaker, missing
    // key, or capability-mismatched candidate must not masquerade as 429.
    let mut all_candidates_rate_limited = true;
    let mut rate_limit_retry: Option<Duration> = None;
    let mut telemetry_context: Option<TelemetryContext> = None;
    let mut candidate_started: Option<tokio::time::Instant> = None;
    let mut candidate_attempted_network = false;
    // Retry distinct models on the same provider, but never reattempt the
    // exact failed SSE-only provider/model during a single candidate walk.
    let mut failed_aggregation_routes = HashSet::new();
    // Exactly ONE candidate loop for chat + embedding. Key failover is confined to this loop.
    for step in steps {
        if let Some(previous) = telemetry_context.take() {
            let elapsed = candidate_started
                .take()
                .map_or(Duration::ZERO, |started| started.elapsed());
            record_intermediate_candidate_failure(
                &state,
                endpoint,
                previous,
                elapsed,
                candidate_attempted_network,
            );
        }
        if walk_expired(walk_deadline) {
            let mut context = telemetry_context.take();
            if let Some(context) = context.as_mut() {
                context.set_failure_type("timeout");
                context.set_health_observation(false);
            }
            return attach_telemetry_context(walk_timeout(&state.metrics), context);
        }
        let provider = &state.config.providers[step.provider_index];
        if !streaming
            && endpoint.is_chat()
            && provider.stream_only
            && failed_aggregation_routes.contains(&(step.provider_index, step.model.clone()))
        {
            all_candidates_rate_limited = false;
            continue;
        }
        if provider.stream_only && !streaming && !endpoint.is_chat() {
            all_candidates_rate_limited = false;
            continue; // Only Chat has a validated SSE-to-JSON adapter.
        }
        // The streaming validator and bounded aggregator intentionally cap
        // choice count at 64. Skip incapable SSE-only providers BEFORE any
        // credential lease or HTTP call; JSON-capable backups may still handle
        // a wider or invalid caller-supplied n under their own contract.
        if provider.stream_only && !streaming && ChatSse::new(expected).is_err() {
            unsupported_aggregate_n = true;
            all_candidates_rate_limited = false;
            continue;
        }
        // Ordinary Chat JSON can carry token log-probabilities, but this
        // bounded SSE synthesizer does not aggregate them. Prefer another
        // candidate to returning an apparently complete but lossy response.
        if provider.stream_only
            && !streaming
            && (request.get("logprobs") == Some(&Value::Bool(true))
                || request.get("top_logprobs").is_some())
        {
            unsupported_aggregate_logprobs = true;
            all_candidates_rate_limited = false;
            continue;
        }
        // Four permits bound at most 8 MiB of buffered JSON output globally.
        // After two seconds, try the next candidate without consuming any
        // provider key or altering circuit state.
        let _aggregate_budget = if provider.stream_only && !streaming {
            let slot_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let slot_deadline =
                walk_deadline.map_or(slot_deadline, |until| until.min(slot_deadline));
            match tokio::time::timeout_at(
                slot_deadline,
                state.aggregation_slots.clone().acquire_owned(),
            )
            .await
            {
                Ok(Ok(permit)) => Some(permit),
                _ => {
                    if walk_expired(walk_deadline) {
                        let mut context = telemetry_context.take();
                        if let Some(context) = context.as_mut() {
                            context.set_failure_type("timeout");
                            context.set_health_observation(false);
                        }
                        return attach_telemetry_context(walk_timeout(&state.metrics), context);
                    }
                    Metrics::inc(&state.metrics.aggregation_busy);
                    crate::ops_log::warn("aggregate_capacity");
                    aggregation_busy = true;
                    all_candidates_rate_limited = false;
                    continue;
                }
            }
        } else {
            None
        };
        if candidates_visited != 0 {
            Metrics::inc(&state.metrics.candidate_fallbacks);
        }
        candidates_visited += 1;
        candidate_started = Some(tokio::time::Instant::now());
        candidate_attempted_network = false;
        let mut context = TelemetryContext::new(
            &provider.name,
            state.telemetry_model_name_in(&route_snapshot, step.provider_index, &step.model),
            "upstream_unavailable",
        );
        if !request_id.is_empty() {
            context.set_trace(
                request_id,
                u16::try_from(candidates_visited).unwrap_or(u16::MAX),
                streaming,
            );
        }
        telemetry_context = Some(context);
        let mut circuit_ticket = match &state.circuits[step.provider_index] {
            Some(circuit) => match circuit.admit() {
                Some(ticket) => Some(ticket),
                None => {
                    Metrics::inc(&state.metrics.circuit_bypasses);
                    bypassed_circuit = true;
                    if let Some(context) = telemetry_context.as_mut() {
                        context.set_failure_type("circuit_open");
                    }
                    all_candidates_rate_limited = false;
                    continue; // No HTTP calls while open or a recovery probe runs.
                }
            },
            None => None, // Preserve existing routing behavior by default.
        };
        let upstream_stream = streaming || (provider.stream_only && endpoint.is_chat());
        if upstream_stream
            && !streaming
            && let Some(ticket) = circuit_ticket.as_mut()
        {
            ticket.configure_stream_idle_timeout(chat_aggregate::STREAM_IDLE);
        } else if !upstream_stream
            && !streaming
            && let Some(ticket) = circuit_ticket.as_mut()
        {
            // Native JSON can legitimately spend nearly the entire 90s
            // upstream attempt reading headers and body. The default 20s
            // probe lease must not fence a healthy recovery before that
            // same bounded request deadline expires.
            ticket.configure_stream_idle_timeout(Duration::from_secs(90));
        }
        // Chain defaults are applied per candidate. An injected n must drive
        // the SSE parser and SSE-to-JSON aggregator just like a caller n;
        // otherwise valid multi-choice streams are rejected as malformed.
        let effective_n = request
            .get("n")
            .or_else(|| step.params.get("n"))
            .map_or(1, |v| v.as_u64().unwrap_or(0));
        if upstream_stream && ChatSse::new(effective_n).is_err() {
            if let Some(context) = telemetry_context.as_mut() {
                context.set_failure_type("capability_mismatch");
            }
            all_candidates_rate_limited = false;
            continue;
        }

        // Apply candidate-only overrides in place, serialize once for this
        // candidate, then restore the original request before any await. This
        // avoids cloning a potentially multi-megabyte JSON tree for each step.
        let object = request.as_object_mut().expect("checked above");
        let original_model = object.insert("model".into(), Value::String(step.model.clone()));
        let override_stream = upstream_stream && !streaming;
        let original_stream = if override_stream {
            object.insert("stream".into(), Value::Bool(true))
        } else {
            None
        };
        let original_stream_options = override_stream.then(|| force_sse_usage(object));
        let mut injected_params = Vec::new();
        for (name, value) in &step.params {
            if matches!(endpoint, Endpoint::Embeddings)
                && matches!(name.as_str(), "stream_options" | "n")
            {
                // These are Chat-only defaults. A chain reused by embeddings
                // cannot inject an unsupported stream mode or choice count.
                continue;
            }
            if !object.contains_key(name) {
                object.insert(name.clone(), value.clone()); // caller always wins.
                injected_params.push(name.clone());
            }
        }
        let payload_json = Bytes::from(request.to_string());
        let object = request.as_object_mut().expect("checked above");
        for name in injected_params {
            object.remove(&name);
        }
        if override_stream {
            restore_sse_usage(
                object,
                original_stream_options.expect("SSE usage override saved"),
            );
            if let Some(original) = original_stream {
                object.insert("stream".into(), original);
            } else {
                object.remove("stream");
            }
        }
        if let Some(original) = original_model {
            object.insert("model".into(), original);
        }
        let mut tried = HashSet::new();
        // Several credentials can independently return 429 for one logical
        // candidate. Rate-limit the keys individually, but count one health
        // observation rather than demoting Auto from one client request.
        let mut saw_new_rate_limit = false;
        while let Some(lease) = state.pools[step.provider_index].acquire(&tried) {
            if walk_expired(walk_deadline) {
                let mut context = telemetry_context.take();
                if let Some(context) = context.as_mut() {
                    context.set_failure_type("timeout");
                    context.set_health_observation(false);
                }
                return attach_telemetry_context(walk_timeout(&state.metrics), context);
            }
            tried.insert(lease.env.clone());
            let url = provider.endpoint(endpoint.path());
            if let Some(context) = telemetry_context.as_mut() {
                context.set_upstream_status(None);
            }
            attempted_network = true;
            candidate_attempted_network = true;
            Metrics::inc(&state.metrics.upstream_attempts);
            // A request-wide reqwest timeout would terminate a healthy SSE
            // generation after 90s despite regular progress. Time only the
            // HTTP headers and non-streaming bounded JSON body instead.
            let attempt_deadline = tokio::time::Instant::now() + Duration::from_secs(90);
            let deadline =
                walk_deadline.map_or(attempt_deadline, |until| until.min(attempt_deadline));
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
                    saw_non_client_network_result = true;
                    if walk_expired(walk_deadline) {
                        let mut context = telemetry_context.take();
                        if let Some(context) = context.as_mut() {
                            context.set_failure_type("timeout");
                            context.set_health_observation(false);
                        }
                        return attach_telemetry_context(walk_timeout(&state.metrics), context);
                    }
                    Metrics::inc(&state.metrics.upstream_transport_failures);
                    ops_log::warn("transport_failure");
                    if state.pools[step.provider_index].record(&lease, Outcome::TransportFailure) {
                        Metrics::inc(&state.metrics.credential_quarantines);
                    }
                    record_circuit_failure(&mut circuit_ticket, &state.metrics);
                    state.mark_auto_failure_in(&live, step.provider_index, &step.model);
                    if let Some(context) = telemetry_context.as_mut() {
                        context.set_failure_type("timeout");
                    }
                    break;
                }
            };
            let upstream = match response {
                Ok(response) => response,
                Err(_) => {
                    saw_non_client_network_result = true;
                    Metrics::inc(&state.metrics.upstream_transport_failures);
                    ops_log::warn("transport_failure");
                    if state.pools[step.provider_index].record(&lease, Outcome::TransportFailure) {
                        Metrics::inc(&state.metrics.credential_quarantines);
                    }
                    record_circuit_failure(&mut circuit_ticket, &state.metrics);
                    state.mark_auto_failure_in(&live, step.provider_index, &step.model);
                    if let Some(context) = telemetry_context.as_mut() {
                        context.set_failure_type("transport_error");
                    }
                    break;
                } // key-neutral
            };
            let status = upstream.status().as_u16();
            let outcome = classify(Some(status));
            // A key-local 429 is not a model validation result. Other keys
            // may still establish a true caller 4xx; preserve that historical
            // response even if a different credential exhausted its quota.
            if outcome != Outcome::RateLimited
                && !(outcome == Outcome::CallerRejected && (400..500).contains(&status))
            {
                saw_non_client_network_result = true;
            }
            if let Some(context) = telemetry_context.as_mut() {
                context.set_upstream_status(Some(status));
                context.set_failure_type(if matches!(status, 408 | 504) {
                    "timeout"
                } else {
                    outcome_failure_type(outcome)
                });
            }
            if outcome.quarantine_credential() {
                Metrics::inc(&state.metrics.upstream_http_failures);
                ops_log::warn("provider_http_failure");
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
                if outcome == Outcome::RateLimited {
                    // A single key's quota exhaustion is not evidence that
                    // the model is unavailable. Another key may succeed
                    // immediately, and counting both failure then success
                    // slowly demotes a perfectly usable Auto candidate.
                    saw_new_rate_limit = true;
                }
                continue; // try a different key; each key is attempted once
            }
            if outcome == Outcome::Success {
                if streaming {
                    match chat_stream::start(
                        upstream,
                        ChatSse::new(effective_n)
                            .expect("validated candidate stream n")
                            .with_tool_choice(
                                request
                                    .get("tool_choice")
                                    .or_else(|| step.params.get("tool_choice")),
                            )
                            .with_json_format(
                                request
                                    .get("response_format")
                                    .or_else(|| step.params.get("response_format")),
                            ),
                        state.clone(),
                        chat_stream::StreamRoute {
                            provider_index: step.provider_index,
                            model: step.model.clone(),
                            live: Arc::clone(&live),
                            telemetry_model: state
                                .telemetry_model_name_in(
                                    &route_snapshot,
                                    step.provider_index,
                                    &step.model,
                                )
                                .to_owned(),
                            request_id: request_id.to_owned(),
                            attempt_index: u16::try_from(candidates_visited).unwrap_or(u16::MAX),
                        },
                        lease.clone(),
                        &mut permit,
                        &mut circuit_ticket,
                    )
                    .await
                    {
                        Ok(stream_response) => {
                            if let Some(context) = telemetry_context.as_mut() {
                                context.set_failure_type("stream_started");
                            }
                            return attach_telemetry_context(
                                stream_response,
                                telemetry_context.take(),
                            );
                        }
                        Err(error) => {
                            // HTTP headers can succeed while the *first*
                            // upstream body read fails. Do not misclassify
                            // that transport fault as malformed SSE or leave
                            // its bad credential eligible on the next call.
                            match error {
                                chat_stream::PreflightError::TransportFailure => {
                                    Metrics::inc(&state.metrics.upstream_transport_failures);
                                    if state.pools[step.provider_index]
                                        .record(&lease, Outcome::TransportFailure)
                                    {
                                        Metrics::inc(&state.metrics.credential_quarantines);
                                    }
                                }
                                chat_stream::PreflightError::InvalidStream => {
                                    Metrics::inc(&state.metrics.upstream_invalid_responses);
                                }
                                chat_stream::PreflightError::Timeout
                                | chat_stream::PreflightError::LocalLimit => {}
                            }
                            if error.blame_provider() {
                                record_circuit_failure(&mut circuit_ticket, &state.metrics);
                                state.mark_auto_failure_in(&live, step.provider_index, &step.model);
                            }
                            ops_log::warn("stream_preflight_failure");
                            if let Some(context) = telemetry_context.as_mut() {
                                context.set_failure_type(error.failure_type());
                                if !error.blame_provider() {
                                    context.set_health_observation(false);
                                }
                            }
                            break;
                        } // preflight rejected
                    }
                }
                let mut aggregation_transient_failure = false;
                let mut aggregation_upstream_transport = false;
                let result = if upstream_stream && !streaming {
                    // JSON-capable providers retain a bounded body read.
                    // SSE-only generations have both a 60s idle limit and a
                    // 100s absolute cap. The shared non-streaming walk
                    // budget still wins if shorter, preventing indefinite
                    // retries or heartbeats across multiple candidates.
                    let lifetime =
                        tokio::time::Instant::now() + chat_aggregate::MAX_AGGREGATION_LIFETIME;
                    let lifetime = walk_deadline.map_or(lifetime, |until| until.min(lifetime));
                    let aggregated = tokio::time::timeout_at(
                        lifetime,
                        chat_aggregate::aggregate(
                            upstream,
                            effective_n,
                            &step.model,
                            request
                                .get("tool_choice")
                                .or_else(|| step.params.get("tool_choice")),
                            circuit_ticket.as_mut(),
                        ),
                    )
                    .await;
                    if aggregated.is_err() {
                        aggregation_transient_failure = true;
                        ops_log::warn("aggregation_lifetime_timeout");
                    }
                    aggregated.map(|value| {
                        value.map(|json| (json, None)).map_err(|error| {
                            if matches!(
                                error,
                                chat_aggregate::AggregateError::IdleTimeout
                                    | chat_aggregate::AggregateError::UpstreamTransport
                            ) {
                                aggregation_transient_failure = true;
                            }
                            aggregation_upstream_transport =
                                matches!(error, chat_aggregate::AggregateError::UpstreamTransport);
                            ops_log::warn(error.log_event());
                            JsonReadFailure::Invalid
                        })
                    })
                } else {
                    tokio::time::timeout_at(
                        deadline,
                        bounded_json(
                            upstream,
                            Arc::clone(&state.large_response_slots),
                            circuit_ticket.as_mut(),
                        ),
                    )
                    .await
                };
                if walk_expired(walk_deadline) {
                    if upstream_stream && !streaming {
                        Metrics::inc(&state.metrics.aggregation_failures);
                    }
                    let mut context = telemetry_context.take();
                    if let Some(context) = context.as_mut() {
                        context.set_failure_type("timeout");
                        context.set_health_observation(false);
                    }
                    return attach_telemetry_context(walk_timeout(&state.metrics), context);
                }
                match result {
                    Ok(Ok((value, response_reservation)))
                        if response_validation::valid_result_with_params(
                            endpoint,
                            &request,
                            &step.params,
                            &value,
                        ) =>
                    {
                        state.pools[step.provider_index].record_success(&lease);
                        record_circuit_success(&mut circuit_ticket, &state.metrics);
                        state.mark_auto_success_in(&live, step.provider_index, &step.model);
                        Metrics::inc(&state.metrics.upstream_success);
                        if upstream_stream && !streaming {
                            Metrics::inc(&state.metrics.aggregation_completed);
                            ops_log::info("aggregation_completed");
                        }
                        if let Some(context) = telemetry_context.as_mut() {
                            context.set_failure_type("success");
                        }
                        let response = if matches!(endpoint, Endpoint::Responses) {
                            let mut response = Response::new(Body::empty());
                            *response.status_mut() = StatusCode::OK;
                            response.extensions_mut().insert(ParsedChatResponse(value));
                            response
                        } else {
                            (StatusCode::OK, Json(value)).into_response()
                        };
                        return attach_telemetry_context(
                            hold_buffered_response(response, response_reservation),
                            telemetry_context.take(),
                        );
                    }
                    Ok(Err(JsonReadFailure::MemoryBusy)) => {
                        // Client-side backpressure cannot be charged to a
                        // healthy upstream's key, circuit or Auto score.
                        buffered_response_busy = true;
                        Metrics::inc(&state.metrics.admission_rejections);
                        if let Some(context) = telemetry_context.as_mut() {
                            context.set_failure_type("local_error");
                            context.set_health_observation(false);
                        }
                        break;
                    }
                    Ok(Err(JsonReadFailure::Transport)) => {
                        // A reset while reading a valid HTTP 200 body is a
                        // network failure, not malformed model JSON. Apply
                        // the same bounded key/circuit recovery as send().
                        Metrics::inc(&state.metrics.upstream_transport_failures);
                        ops_log::warn("transport_failure");
                        if state.pools[step.provider_index]
                            .record(&lease, Outcome::TransportFailure)
                        {
                            Metrics::inc(&state.metrics.credential_quarantines);
                        }
                        record_circuit_failure(&mut circuit_ticket, &state.metrics);
                        state.mark_auto_failure_in(&live, step.provider_index, &step.model);
                        if let Some(context) = telemetry_context.as_mut() {
                            context.set_failure_type("transport_error");
                        }
                        break;
                    }
                    Err(_) => {
                        if upstream_stream && !streaming {
                            Metrics::inc(&state.metrics.aggregation_failures);
                            failed_aggregation_routes
                                .insert((step.provider_index, step.model.clone()));
                            ops_log::warn("aggregation_failed");
                        } else {
                            ops_log::warn("upstream_body_timeout");
                        }
                        if !(upstream_stream && !streaming) {
                            Metrics::inc(&state.metrics.upstream_transport_failures);
                            if state.pools[step.provider_index]
                                .record(&lease, Outcome::TransportFailure)
                            {
                                Metrics::inc(&state.metrics.credential_quarantines);
                            }
                            record_circuit_failure(&mut circuit_ticket, &state.metrics);
                            state.mark_auto_failure_in(&live, step.provider_index, &step.model);
                        }
                        if let Some(context) = telemetry_context.as_mut() {
                            context.set_failure_type("timeout");
                            if upstream_stream && !streaming {
                                // The gateway's aggregation deadline is local
                                // policy, not evidence of provider health.
                                // Match the in-memory AutoHealth decision.
                                context.set_health_observation(false);
                            }
                        }
                        break;
                    }
                    _ => {
                        if upstream_stream && !streaming {
                            Metrics::inc(&state.metrics.aggregation_failures);
                            failed_aggregation_routes
                                .insert((step.provider_index, step.model.clone()));
                            if aggregation_upstream_transport {
                                Metrics::inc(&state.metrics.upstream_transport_failures);
                                if state.pools[step.provider_index]
                                    .record(&lease, Outcome::TransportFailure)
                                {
                                    Metrics::inc(&state.metrics.credential_quarantines);
                                }
                            } else if !aggregation_transient_failure {
                                Metrics::inc(&state.metrics.upstream_invalid_responses);
                            }
                            ops_log::warn("aggregation_failed");
                        } else {
                            Metrics::inc(&state.metrics.upstream_invalid_responses);
                            ops_log::warn("upstream_invalid");
                        }
                        // Local aggregation policy (idle/lifetime, size or
                        // incompatible SSE) is not evidence that the model
                        // service has failed. Avoid cascading breaker opens.
                        if !(upstream_stream && !streaming) || aggregation_upstream_transport {
                            record_circuit_failure(&mut circuit_ticket, &state.metrics);
                            state.mark_auto_failure_in(&live, step.provider_index, &step.model);
                        }
                        if let Some(context) = telemetry_context.as_mut() {
                            context.set_failure_type(if aggregation_transient_failure {
                                "timeout"
                            } else {
                                "invalid_response"
                            });
                            if upstream_stream && !streaming && !aggregation_upstream_transport {
                                // Unsupported SSE-to-JSON forms, idle/absolute
                                // caps and local size limits are not supplier
                                // failures. Only genuine upstream transport
                                // loss should persist a health penalty.
                                context.set_health_observation(false);
                            }
                        }
                        break;
                    } // next step
                }
            }
            if (400..500).contains(&status) && !matches!(status, 401 | 403 | 408 | 429) {
                let status_code = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST);
                // Never reflect arbitrary upstream messages: some providers
                // include credential fragments or request text in them. Map
                // only recognized machine-readable error codes to fixed text.
                let error_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
                let error_deadline =
                    walk_deadline.map_or(error_deadline, |until| until.min(error_deadline));
                let detail = tokio::time::timeout_at(error_deadline, bounded_error_json(upstream))
                    .await
                    .ok()
                    .flatten();
                if walk_expired(walk_deadline) {
                    if let Some(context) = telemetry_context.as_mut() {
                        context.set_failure_type("timeout");
                        context.set_health_observation(false);
                    }
                    return attach_telemetry_context(
                        walk_timeout(&state.metrics),
                        telemetry_context.take(),
                    );
                }
                let message = match detail
                    .as_ref()
                    .and_then(|body| body.pointer("/error/code").and_then(Value::as_str))
                {
                    Some("context_length_exceeded") => "Upstream context window exceeded",
                    Some("model_not_found") => "The upstream model is unavailable",
                    Some("invalid_request_error") => "Upstream rejected request parameters",
                    _ => "Upstream rejected the request",
                };
                last_upstream_client_error = Some((
                    status_code,
                    json!({
                        "error":{"type":"upstream_client_error",
                            "code":"upstream_rejected","message":message}
                    }),
                ));
            }
            Metrics::inc(&state.metrics.upstream_http_failures);
            ops_log::warn("provider_http_failure");
            if outcome.blame_provider() {
                if state.pools[step.provider_index].record(&lease, outcome) {
                    Metrics::inc(&state.metrics.credential_quarantines);
                }
                state.mark_auto_failure_in(&live, step.provider_index, &step.model);
            }
            if outcome.blame_provider() {
                record_circuit_failure(&mut circuit_ticket, &state.metrics);
            }
            break; // 4xx or 5xx: next chain step; no uncontrolled retries
        }
        let rate_limit_delay = state.pools[step.provider_index].all_keys_rate_limited_for();
        if saw_new_rate_limit && rate_limit_delay.is_some() {
            // Only a freshly observed *candidate-wide* 429 exhaustion
            // teaches AutoHealth that this physical model is unavailable.
            // Already-isolated keys cannot be counted again by each caller.
            state.mark_auto_failure_in(&live, step.provider_index, &step.model);
        }
        if !candidate_attempted_network && let Some(context) = telemetry_context.as_mut() {
            if rate_limit_delay.is_some() {
                context.set_failure_type("rate_limited");
                context.set_upstream_status(Some(429));
            } else {
                context.set_failure_type("credential_error");
                context.set_upstream_status(Some(401));
            }
            context.set_health_observation(false);
        }
        if let Some(delay) = rate_limit_delay {
            rate_limit_retry = Some(rate_limit_retry.map_or(delay, |previous| previous.min(delay)));
        } else {
            all_candidates_rate_limited = false;
        }
    }
    if walk_expired(walk_deadline) {
        let mut context = telemetry_context.take();
        if let Some(context) = context.as_mut() {
            context.set_failure_type("timeout");
            context.set_health_observation(false);
        }
        return attach_telemetry_context(walk_timeout(&state.metrics), context);
    }
    if all_candidates_rate_limited && let Some(delay) = rate_limit_retry {
        // A *bounded* delta-seconds retry hint, rounded up so fast retries do
        // not re-hit isolated keys. Never reflect an upstream header verbatim.
        let seconds = delay
            .as_secs()
            .saturating_add(u64::from(delay.subsec_nanos() != 0));
        let seconds = seconds.clamp(1, 24 * 3600);
        let mut response = error(
            StatusCode::TOO_MANY_REQUESTS,
            "providers_rate_limited",
            "All eligible upstream credentials are temporarily rate limited",
        );
        response.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_str(&seconds.to_string()).expect("bounded retry seconds"),
        );
        Metrics::inc(&state.metrics.rate_limit_exhaustions);
        ops_log::warn("rate_limit_exhausted");
        return attach_telemetry_context(response, telemetry_context.take());
    }
    if buffered_response_busy {
        let mut response = error(
            StatusCode::SERVICE_UNAVAILABLE,
            "large_response_busy",
            "Large buffered response capacity is temporarily exhausted",
        );
        response.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
        return attach_telemetry_context(response, telemetry_context.take());
    }
    if unsupported_aggregate_n && !attempted_network {
        let response = error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "aggregate_n_unsupported",
            "SSE-only Chat aggregation supports n in the range 1..64; configure a JSON-capable fallback",
        );
        let mut context = telemetry_context.take();
        if let Some(context) = context.as_mut() {
            context.set_failure_type("capability_mismatch");
        }
        return attach_telemetry_context(response, context);
    }
    if unsupported_aggregate_logprobs && !attempted_network {
        let response = error(
            StatusCode::NOT_IMPLEMENTED,
            "aggregate_logprobs_unsupported",
            "SSE-only Chat aggregation cannot preserve token logprobs; use a JSON-capable provider",
        );
        let mut context = telemetry_context.take();
        if let Some(context) = context.as_mut() {
            context.set_failure_type("capability_mismatch");
        }
        return attach_telemetry_context(response, context);
    }
    if aggregation_busy && !attempted_network {
        crate::ops_log::warn("aggregate_capacity");
        let mut response = error(
            StatusCode::SERVICE_UNAVAILABLE,
            "aggregate_busy",
            "SSE aggregation capacity is temporarily exhausted",
        );
        response.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
        let mut context = telemetry_context.take();
        if let Some(context) = context.as_mut() {
            context.set_failure_type("aggregation_busy");
        }
        return attach_telemetry_context(response, context);
    }
    if bypassed_circuit && !attempted_network {
        ops_log::warn("provider_circuit_open");
        let mut response = error(
            StatusCode::SERVICE_UNAVAILABLE,
            "provider_circuit_open",
            "All eligible providers are temporarily unavailable",
        );
        response.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
        let mut context = telemetry_context.take();
        if let Some(context) = context.as_mut() {
            context.set_failure_type("circuit_open");
        }
        return attach_telemetry_context(response, context);
    }
    if !saw_non_client_network_result && let Some((status, value)) = last_upstream_client_error {
        return attach_telemetry_context(
            (status, Json(value)).into_response(),
            telemetry_context.take(),
        );
    }
    let context = telemetry_context.take();
    attach_telemetry_context(
        error(
            StatusCode::BAD_GATEWAY,
            "upstream_unavailable",
            "No upstream returned a valid response",
        ),
        context,
    )
}

async fn bounded_error_json(mut upstream: reqwest::Response) -> Option<Value> {
    let mut bytes = Vec::new();
    while let Some(chunk) = upstream.chunk().await.ok()? {
        if chunk.len() > 8192usize.saturating_sub(bytes.len()) {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).ok()
}

/// An upstream Content-Length is untrusted. Never let a claimed large body
/// allocate outside the two-slot response budget before actual bytes cross
/// the threshold. Once the permit is held, reserve the remaining declared
/// capacity in one step instead of repeatedly growing a large Vec.
fn reserve_declared_json_capacity(
    bytes: &mut Vec<u8>,
    declared_len: Option<usize>,
    large_permit_held: bool,
) -> Result<(), JsonReadFailure> {
    let Some(length) = declared_len.filter(|&length| length <= MAX_BODY_BYTES) else {
        return Ok(());
    };
    if length > super::LARGE_RESPONSE_BODY_BYTES && !large_permit_held {
        return Ok(());
    }
    if length > bytes.capacity() {
        bytes
            .try_reserve_exact(length.saturating_sub(bytes.len()))
            .map_err(|_| JsonReadFailure::MemoryBusy)?;
    }
    Ok(())
}

async fn bounded_json(
    response: reqwest::Response,
    large_slots: Arc<tokio::sync::Semaphore>,
    mut circuit_ticket: Option<&mut crate::circuit::Ticket>,
) -> Result<(Value, Option<BufferedResponseLease>), JsonReadFailure> {
    // Use one bounded allocation when the upstream declares its size.
    // Geometric Vec growth for 12 MiB JSON otherwise reallocates several
    // large buffers per request and may retain idle RSS after TCP drain.
    // Do not trust a declared length beyond the actual response size cap.
    let declared_len = response
        .content_length()
        .and_then(|size| usize::try_from(size).ok())
        .filter(|size| *size <= MAX_BODY_BYTES);
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    let mut reservation = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| JsonReadFailure::Transport)?;
        if chunk.len() > MAX_BODY_BYTES.saturating_sub(bytes.len()) {
            return Err(JsonReadFailure::Invalid);
        }
        if !chunk.is_empty() {
            // Real upstream progress renews the idle lease; merely waiting
            // for the body or acquiring local capacity must not do so.
            if let Some(ticket) = circuit_ticket.as_mut() {
                ticket.progress();
            }
        }
        if reservation.is_none()
            && bytes.len().saturating_add(chunk.len()) > super::LARGE_RESPONSE_BODY_BYTES
        {
            // Each of the 128 admitted requests may read a small JSON answer.
            // Once a reply would become large, acquire one of only two
            // 32 MiB body slots BEFORE extending the buffer. The slot is
            // carried into the downstream Body, including a paused reader.
            let permit =
                tokio::time::timeout(Duration::from_secs(1), large_slots.clone().acquire_owned())
                    .await
                    .map_err(|_| JsonReadFailure::MemoryBusy)?
                    .map_err(|_| JsonReadFailure::MemoryBusy)?;
            reservation = Some(BufferedResponseLease {
                _permit: Arc::new(permit),
            });
        }
        if !chunk.is_empty() {
            reserve_declared_json_capacity(&mut bytes, declared_len, reservation.is_some())?;
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes)
        .map(|value| (value, reservation))
        .map_err(|_| JsonReadFailure::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_sse_usage_preserves_large_original_metadata_and_restores_every_variant() {
        for original_options in [
            None,
            Some(Value::Null),
            Some(json!("invalid")),
            Some(json!({})),
            Some(json!({"include_usage":false})),
            Some(json!({"include_usage":true,"trace":"z".repeat(1024*1024)})),
            Some(json!({"trace":"t".repeat(1024*1024),"source":7})),
        ] {
            let mut request = json!({"model":"auto","messages":[]});
            if let Some(options) = original_options {
                request["stream_options"] = options;
            }
            let prior = request.clone();
            let original_ptr = request
                .pointer("/stream_options/trace")
                .and_then(Value::as_str)
                .map(str::as_ptr);
            let backup = force_sse_usage(request.as_object_mut().unwrap());
            assert_eq!(request["stream_options"]["include_usage"], true);
            assert_eq!(
                request
                    .pointer("/stream_options/trace")
                    .and_then(Value::as_str)
                    .map(str::as_ptr),
                original_ptr,
                "large metadata was copied when forcing the SSE usage flag"
            );
            restore_sse_usage(request.as_object_mut().unwrap(), backup);
            assert_eq!(
                request, prior,
                "next candidate must see the original request"
            );
            assert_eq!(
                request
                    .pointer("/stream_options/trace")
                    .and_then(Value::as_str)
                    .map(str::as_ptr),
                original_ptr
            );
        }
    }

    #[test]
    fn untrusted_content_length_never_reserves_large_memory_without_permit() {
        let mut bytes = Vec::new();
        // A malicious upstream may advertise 32 MiB and only send one byte.
        // With 128 in-flight requests the old first-chunk reservation could
        // allocate gigabytes before acquiring either large response slot.
        reserve_declared_json_capacity(&mut bytes, Some(MAX_BODY_BYTES), false).unwrap();
        assert_eq!(bytes.capacity(), 0);
        bytes.push(b'{');
        reserve_declared_json_capacity(&mut bytes, Some(MAX_BODY_BYTES), false).unwrap();
        assert!(bytes.capacity() < super::super::LARGE_RESPONSE_BODY_BYTES);

        // Small responses can still reserve without consuming a large slot.
        let mut small = Vec::new();
        reserve_declared_json_capacity(
            &mut small,
            Some(super::super::LARGE_RESPONSE_BODY_BYTES),
            false,
        )
        .unwrap();
        assert!(small.capacity() >= super::super::LARGE_RESPONSE_BODY_BYTES);

        // Once the large-body permit has been acquired, preallocation is
        // allowed even if the first upstream chunks were tiny.
        let mut large = Vec::new();
        large.extend_from_slice(b"{");
        reserve_declared_json_capacity(&mut large, Some(2 * 1024 * 1024), true).unwrap();
        assert!(large.capacity() >= 2 * 1024 * 1024);
        let mut oversized = Vec::new();
        reserve_declared_json_capacity(&mut oversized, Some(MAX_BODY_BYTES + 1), true).unwrap();
        assert_eq!(oversized.capacity(), 0);
    }

    #[test]
    fn shadow_accounting_is_identical_for_cached_proposal_and_unavailable_fallback() {
        let config: crate::config::Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://example.invalid
    key_env: K
auto:
  enabled: true
  candidates:
    - provider: p
      model: first
    - provider: p
      model: second
"#,
        )
        .unwrap();
        let state = AppState::new(
            config.clone(),
            std::collections::HashMap::from([("K".to_owned(), "test-secret".to_owned())]),
            vec!["client".into()],
        )
        .unwrap();
        let step = |model: &str| routing::ResolvedStep {
            provider_index: 0,
            model: model.into(),
            params: Default::default(),
        };
        let baseline = [step("first"), step("second")];
        record_shadow_observation(&state, &config, "trace1", &baseline, Some(&[1, 0]));
        record_shadow_observation(&state, &config, "trace2", &baseline, Some(&[0, 1]));
        record_shadow_observation(&state, &config, "trace3", &baseline, None);
        let status = state.runtime_status();
        assert_eq!(status["auto_shadow_total"], 2);
        assert_eq!(status["auto_shadow_disagreements"], 1);
        assert_eq!(status["auto_shadow_unavailable"], 1);
        // Invalid candidates do not cause a panic or a mismatched telemetry
        // label to enter the gateway; the counters remain independent.
        record_shadow_observation::<routing::ResolvedStep>(&state, &config, "trace4", &[], None);
        assert_eq!(state.runtime_status()["auto_shadow_unavailable"], 2);
    }

    #[test]
    fn nonstream_walk_cannot_cross_the_120_second_front_proxy_window() {
        assert_eq!(
            nonstream_walk_budget(Duration::from_secs(600)),
            Duration::from_secs(100),
        );
        assert_eq!(
            nonstream_walk_budget(Duration::from_secs(15)),
            Duration::from_secs(15),
        );
        assert!(nonstream_walk_budget(Duration::from_secs(3600)) < Duration::from_secs(120));
    }
    #[test]
    fn remote_decider_cannot_promote_demoted_auto_candidate() {
        let yaml = r#"
providers:
  - name: p
    type: openai
    base_url: https://upstream.example
    key_env: K
auto:
  enabled: true
  health_aware: true
  candidates:
    - provider: p
      model: degraded
    - provider: p
      model: healthy
    - provider: p
      model: equally_healthy
"#;
        let mut config: crate::config::Config = serde_yaml::from_str(yaml).unwrap();
        config.validate().unwrap();
        let step = |model: &str| routing::ResolvedStep {
            provider_index: 0,
            model: model.to_owned(),
            params: Default::default(),
        };
        // Malicious Decider ranks the demoted model first. Healthy models
        // retain the Decider's relative preference within their own tier.
        let mut steps = vec![step("degraded"), step("equally_healthy"), step("healthy")];
        preserve_auto_health_priority(&config, &mut steps, &[100, 0, 0]);
        assert_eq!(
            steps.iter().map(|s| s.model.as_str()).collect::<Vec<_>>(),
            vec!["equally_healthy", "healthy", "degraded"]
        );
        // When health awareness is off, opt-in Decider order is unchanged.
        config.auto.health_aware = false;
        let mut steps = vec![step("degraded"), step("healthy")];
        preserve_auto_health_priority(&config, &mut steps, &[100, 0, 0]);
        assert_eq!(steps[0].model, "degraded");
    }
}
