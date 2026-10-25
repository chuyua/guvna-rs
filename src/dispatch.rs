//! Single shared Chat/Embeddings (and Responses bridge) upstream candidate
//! walk. Retry, credential quarantine, failover, breaker and timeout policies
//! must remain here, never duplicated per API endpoint.
use super::{
    AppState, MAX_BODY_BYTES, chat_aggregate, chat_stream, error, record_circuit_failure,
    record_circuit_success, response_validation, retry_after_delay,
};
use crate::{
    metrics::Metrics,
    ops_log,
    outcome::{Outcome, classify},
    routing,
    sse::ChatSse,
    telemetry::TelemetryContext,
};
use axum::{
    Json,
    body::Bytes,
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

pub(super) async fn dispatch(
    state: Arc<AppState>,
    body: Bytes,
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
        body,
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

fn attach_telemetry_context(mut response: Response, context: Option<TelemetryContext>) -> Response {
    if let Some(context) = context {
        response.extensions_mut().insert(context);
    }
    response
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
    body: Bytes,
    endpoint: Endpoint,
    admission: OwnedSemaphorePermit,
    payload_permit: Option<OwnedSemaphorePermit>,
    started: tokio::time::Instant,
    request_id: &str,
) -> Response {
    let mut request: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                "Expected one JSON object",
            );
        }
    };
    // The parsed Value owns its strings; release the raw body buffer before
    // routing and candidate serialization to reduce the per-request peak.
    drop(body);
    // Payload slots cover only receiving and parsing JSON, not upstream waits.
    drop(payload_permit);
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
    // Keep byte-range eligibility and context-window budgeting separate.
    // The context estimate includes the full request, output allowance and safety reserve.
    let mut steps = match if auto_enabled {
        let input_bytes = routing::auto_input_bytes(request.get("messages"));
        let context_estimate = routing::auto_context_estimate(&request);
        let (min_context, max_context) = auto_bounds.expect("Auto bounds validated");
        let requirements =
            routing::auto_requirements(&request).with_context_bounds(min_context, max_context);
        let penalties = state
            .auto_health
            .as_ref()
            .map(|health| health.penalties())
            .unwrap_or_default();
        let mut resolved = state.auto_route_cache.resolve(
            &state.config,
            input_bytes,
            context_estimate,
            requirements,
            &penalties,
            state.auto_generation,
        );
        if matches!(&resolved, Ok(steps) if steps.is_empty())
            && !caller_context_bound
            && routing::auto_operator_default_can_degrade(
                &state.config,
                input_bytes,
                context_estimate,
                requirements,
            )
        {
            // Keep context-length budgeting and capability hard filters intact.
            // A second bounded cache lookup cannot issue network requests.
            resolved = state.auto_route_cache.resolve(
                &state.config,
                input_bytes,
                context_estimate,
                requirements.with_context_bounds(0, None),
                &penalties,
                state.auto_generation,
            );
            auto_bounds = Some((0, None));
        }
        resolved
    } else {
        routing::resolve(&state.route_snapshot(), &model)
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
        let penalized = state
            .auto_health
            .as_ref()
            .is_some_and(|health| health.penalties().iter().any(|value| *value > 0));
        rank.rank(&state.config, &mut steps, penalized);
    }

    if auto_enabled && let Some(decider) = &state.auto_decider {
        let input_bytes = routing::auto_input_bytes(request.get("messages"));
        let context_estimate = routing::auto_context_estimate(&request);
        let (min_context, max_context) = auto_bounds.expect("Auto bounds validated");
        let requirements =
            routing::auto_requirements(&request).with_context_bounds(min_context, max_context);
        if state.config.auto.decider.shadow_only {
            // Shadow adds zero selector latency to actual response selection.
            let owner = Arc::clone(&state);
            let selector = Arc::clone(decider);
            let baseline = steps.clone();
            let trace_id = request_id.to_owned();
            if let Ok(permit) = state.auto_background_slots.clone().try_acquire_owned() {
                tokio::spawn(async move {
                    let _permit = permit;
                    let choice = selector
                        .rank(
                            &owner.config,
                            &baseline,
                            input_bytes,
                            context_estimate,
                            requirements,
                        )
                        .await;
                    if let (Some(telemetry), Some(expected)) = (&owner.telemetry, baseline.first())
                    {
                        let expected_provider =
                            &owner.config.providers[expected.provider_index].name;
                        let suggested =
                            choice
                                .as_ref()
                                .and_then(|ranked| ranked.first())
                                .and_then(|step| {
                                    owner.config.providers.get(step.provider_index).map(
                                        |provider| (provider.name.as_str(), step.model.as_str()),
                                    )
                                });
                        telemetry.observe_shadow(
                            &trace_id,
                            (expected_provider, expected.model.as_str()),
                            suggested,
                            &owner.metrics,
                        );
                    }
                    match choice {
                        Some(proposal) => {
                            owner.shadow_total.fetch_add(1, Ordering::Relaxed);
                            if let (Some(expected), Some(actual)) =
                                (baseline.first(), proposal.first())
                                && (expected.provider_index != actual.provider_index
                                    || expected.model != actual.model)
                            {
                                owner.shadow_disagreements.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        None => {
                            owner.shadow_unavailable.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            } else {
                // Under load, skip speculative ranking instead of queuing
                // unbounded background tasks or delaying live traffic.
                state.shadow_unavailable.fetch_add(1, Ordering::Relaxed);
                if let (Some(telemetry), Some(expected)) = (&state.telemetry, steps.first()) {
                    let provider = &state.config.providers[expected.provider_index].name;
                    telemetry.observe_shadow(
                        request_id,
                        (provider, expected.model.as_str()),
                        None,
                        &state.metrics,
                    );
                }
            }
        } else if streaming {
            // A selector can take up to 2s; never add that delay to SSE
            // response headers or its first token. Cold/stale rankings are
            // refreshed in the background, with one in-flight fetch per cache.
            let baseline = steps.clone();
            let (cached, refresh) =
                decider.cached_rank(&baseline, input_bytes, context_estimate, requirements);
            if let Some(ranked) = cached {
                steps = ranked;
            }
            if refresh && let Ok(permit) = state.auto_background_slots.clone().try_acquire_owned() {
                let owner = Arc::clone(&state);
                let selector = Arc::clone(decider);
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = selector
                        .rank(
                            &owner.config,
                            &baseline,
                            input_bytes,
                            context_estimate,
                            requirements,
                        )
                        .await;
                });
            }
        } else if let Some(ranked) = decider
            .rank(
                &state.config,
                &steps,
                input_bytes,
                context_estimate,
                requirements,
            )
            .await
        {
            steps = ranked;
        }
    }

    if auto_enabled && let Some(health) = &state.auto_health {
        preserve_auto_health_priority(&state.config, &mut steps, &health.penalties());
    }

    let mut permit = Some(admission);

    // Count actual candidate transitions, not provider-key retries.
    let mut candidates_visited = 0usize;
    let mut attempted_network = false;
    let mut last_upstream_client_error: Option<(StatusCode, Value)> = None;
    let mut bypassed_circuit = false;
    let mut aggregation_busy = false;
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
            state.telemetry_model_name(step.provider_index, &step.model),
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
        let original_stream_options = if override_stream {
            let mut options = object
                .get("stream_options")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            options.insert("include_usage".into(), Value::Bool(true));
            object.insert("stream_options".into(), Value::Object(options))
        } else {
            None
        };
        let mut injected_params = Vec::new();
        for (name, value) in &step.params {
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
            if let Some(original) = original_stream_options {
                object.insert("stream_options".into(), original);
            } else {
                object.remove("stream_options");
            }
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
        let mut rate_limit_health_recorded = false;
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
                    state.mark_auto_failure(step.provider_index, &step.model);
                    if let Some(context) = telemetry_context.as_mut() {
                        context.set_failure_type("timeout");
                    }
                    break;
                }
            };
            let upstream = match response {
                Ok(response) => response,
                Err(_) => {
                    Metrics::inc(&state.metrics.upstream_transport_failures);
                    ops_log::warn("transport_failure");
                    if state.pools[step.provider_index].record(&lease, Outcome::TransportFailure) {
                        Metrics::inc(&state.metrics.credential_quarantines);
                    }
                    record_circuit_failure(&mut circuit_ticket, &state.metrics);
                    state.mark_auto_failure(step.provider_index, &step.model);
                    if let Some(context) = telemetry_context.as_mut() {
                        context.set_failure_type("transport_error");
                    }
                    break;
                } // key-neutral
            };
            let status = upstream.status().as_u16();
            let outcome = classify(Some(status));
            if let Some(context) = telemetry_context.as_mut() {
                context.set_upstream_status(Some(status));
                context.set_failure_type(outcome_failure_type(outcome));
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
                if outcome == Outcome::RateLimited && !rate_limit_health_recorded {
                    rate_limit_health_recorded = true;
                    state.mark_auto_failure(step.provider_index, &step.model);
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
                            ),
                        state.clone(),
                        chat_stream::StreamRoute {
                            provider_index: step.provider_index,
                            model: step.model.clone(),
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
                        Err(_) => {
                            Metrics::inc(&state.metrics.upstream_invalid_responses);
                            ops_log::warn("stream_preflight_failure");
                            record_circuit_failure(&mut circuit_ticket, &state.metrics);
                            state.mark_auto_failure(step.provider_index, &step.model);
                            if let Some(context) = telemetry_context.as_mut() {
                                context.set_failure_type("invalid_response");
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
                        ),
                    )
                    .await;
                    if aggregated.is_err() {
                        aggregation_transient_failure = true;
                        ops_log::warn("aggregation_lifetime_timeout");
                    }
                    aggregated.map(|value| {
                        value.map_err(|error| {
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
                        })
                    })
                } else {
                    tokio::time::timeout_at(deadline, bounded_json(upstream)).await
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
                    Ok(Ok(value))
                        if response_validation::valid_result_with_params(
                            endpoint,
                            &request,
                            &step.params,
                            &value,
                        ) =>
                    {
                        state.pools[step.provider_index].record_success(&lease);
                        record_circuit_success(&mut circuit_ticket, &state.metrics);
                        state.mark_auto_success(step.provider_index, &step.model);
                        Metrics::inc(&state.metrics.upstream_success);
                        if upstream_stream && !streaming {
                            Metrics::inc(&state.metrics.aggregation_completed);
                            ops_log::info("aggregation_completed");
                        }
                        if let Some(context) = telemetry_context.as_mut() {
                            context.set_failure_type("success");
                        }
                        return attach_telemetry_context(
                            (StatusCode::OK, Json(value)).into_response(),
                            telemetry_context.take(),
                        );
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
                            state.mark_auto_failure(step.provider_index, &step.model);
                        }
                        if let Some(context) = telemetry_context.as_mut() {
                            context.set_failure_type("timeout");
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
                            state.mark_auto_failure(step.provider_index, &step.model);
                        }
                        if let Some(context) = telemetry_context.as_mut() {
                            context.set_failure_type(if aggregation_transient_failure {
                                "timeout"
                            } else {
                                "invalid_response"
                            });
                        }
                        break;
                    } // next step
                }
            }
            if (400..500).contains(&status) && !matches!(status, 401 | 403 | 429) {
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
                state.mark_auto_failure(step.provider_index, &step.model);
            }
            if outcome.blame_provider() {
                record_circuit_failure(&mut circuit_ticket, &state.metrics);
            }
            break; // 4xx or 5xx: next chain step; no uncontrolled retries
        }
        let rate_limit_delay = state.pools[step.provider_index].all_keys_rate_limited_for();
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
    if let Some((status, value)) = last_upstream_client_error {
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

#[cfg(test)]
mod tests {
    use super::*;

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
