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
use serde_json::Value;
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::OwnedSemaphorePermit;

#[derive(Clone, Copy)]
pub(super) enum Endpoint {
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

pub(super) async fn dispatch(
    state: Arc<AppState>,
    body: Bytes,
    endpoint: Endpoint,
    admission: OwnedSemaphorePermit,
) -> Response {
    let started = tokio::time::Instant::now();
    let id = ops_log::next_request_id();
    let telemetry = state.telemetry.clone();
    let metrics = Arc::clone(&state.metrics);
    let response = dispatch_inner(state, body, endpoint, admission, started).await;
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
            Endpoint::Embeddings => "embeddings",
        };
        telemetry.submit(
            label,
            response.status().as_u16(),
            started.elapsed(),
            sse_headers,
            telemetry_context,
            &metrics,
        );
    }
    response
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
    started: tokio::time::Instant,
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
    // One monotonic deadline across ALL candidates, key attempts, 429
    // retries, aggregation permit waits and response bodies. Client-facing
    // SSE is intentionally exempt: only its header preflight is time-bounded.
    let walk_deadline = (!streaming).then(|| {
        started
            + state
                .config
                .timeout
                .walk_duration()
                .expect("config validated")
    });
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
    let auto_enabled = model == "auto" && state.config.auto.enabled;
    // Keep byte-range eligibility and context-window budgeting separate.
    // The context estimate includes the full request, output allowance and safety reserve.
    let steps = match if auto_enabled {
        let input_bytes = routing::auto_input_bytes(request.get("messages"));
        let context_estimate = routing::auto_context_estimate(&request);
        let penalties = state
            .auto_health
            .as_ref()
            .map(|health| health.penalties())
            .unwrap_or_default();
        state.auto_route_cache.resolve(
            &state.config,
            input_bytes,
            context_estimate,
            &penalties,
            state.auto_generation,
        )
    } else {
        routing::resolve(&state.config, model)
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

    let mut permit = Some(admission);

    // Count actual candidate transitions, not provider-key retries.
    let mut candidates_visited = 0usize;
    let mut attempted_network = false;
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
        if provider.stream_only && !streaming && !matches!(endpoint, Endpoint::Chat) {
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
        telemetry_context = Some(TelemetryContext::new(
            &provider.name,
            &step.model,
            "upstream_unavailable",
        ));
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
        let mut payload = request.clone();
        let object = payload.as_object_mut().expect("checked above");
        object.insert("model".into(), Value::String(step.model.clone()));
        let upstream_stream =
            streaming || (provider.stream_only && matches!(endpoint, Endpoint::Chat));
        if upstream_stream && !streaming {
            object.insert("stream".into(), Value::Bool(true));
        }
        for (name, value) in &step.params {
            object.entry(name).or_insert_with(|| value.clone()); // caller always wins.
        }

        // Chain defaults are applied per candidate. An injected n must drive
        // the SSE parser and SSE-to-JSON aggregator just like a caller n;
        // otherwise valid multi-choice streams are rejected as malformed.
        let effective_n = payload.get("n").map_or(1, |v| v.as_u64().unwrap_or(0));
        if upstream_stream && ChatSse::new(effective_n).is_err() {
            if let Some(context) = telemetry_context.as_mut() {
                context.set_failure_type("capability_mismatch");
            }
            all_candidates_rate_limited = false;
            continue;
        }

        // Serialize once per candidate, not once for every credential retry.
        let payload_json = Bytes::from(payload.to_string());
        let mut tried = HashSet::new();
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
                if outcome == Outcome::RateLimited {
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
                            .with_tool_choice(payload.get("tool_choice")),
                        state.clone(),
                        chat_stream::StreamRoute {
                            provider_index: step.provider_index,
                            model: step.model.clone(),
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
                let result = if upstream_stream && !streaming {
                    // JSON-capable providers retain a bounded body read.
                    // SSE-only generations have both a 15s idle limit and a
                    // 10min absolute cap. The shared non-streaming walk
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
                            payload.get("tool_choice"),
                        ),
                    )
                    .await;
                    if aggregated.is_err() {
                        ops_log::warn("aggregation_lifetime_timeout");
                    }
                    aggregated.map(|value| {
                        value.map_err(|error| {
                            ops_log::warn(error.log_event());
                        })
                    })
                } else {
                    tokio::time::timeout_at(deadline, bounded_json(upstream)).await
                };
                if walk_expired(walk_deadline) {
                    let mut context = telemetry_context.take();
                    if let Some(context) = context.as_mut() {
                        context.set_failure_type("timeout");
                        context.set_health_observation(false);
                    }
                    return attach_telemetry_context(walk_timeout(&state.metrics), context);
                }
                match result {
                    Ok(Ok(value))
                        if response_validation::valid_result(endpoint, &payload, &value) =>
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
                    _ => {
                        Metrics::inc(&state.metrics.upstream_invalid_responses);
                        if upstream_stream && !streaming {
                            ops_log::warn("aggregation_failed");
                        } else {
                            ops_log::warn("upstream_invalid");
                        }
                        record_circuit_failure(&mut circuit_ticket, &state.metrics);
                        state.mark_auto_failure(step.provider_index, &step.model);
                        if let Some(context) = telemetry_context.as_mut() {
                            context.set_failure_type("invalid_response");
                        }
                        break;
                    } // next step
                }
            }
            Metrics::inc(&state.metrics.upstream_http_failures);
            ops_log::warn("provider_http_failure");
            if outcome == Outcome::ProviderTransient {
                state.mark_auto_failure(step.provider_index, &step.model);
            }
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
