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
    let auto_enabled = model == "auto" && state.config.auto.enabled;
    // Bytes of encoded message content are a deterministic bounded proxy
    // for context size; no remote tokenizer or prompt logging is involved.
    let steps = match if auto_enabled {
        let input_bytes = request.get("messages").map_or(0, |m| m.to_string().len());
        routing::resolve_auto(&state.config, input_bytes)
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
            "No configured Auto candidate accepts this input size",
        );
    }

    let mut permit = Some(admission);

    // Count actual candidate transitions, not provider-key retries.
    let mut candidates_visited = 0usize;
    let mut attempted_network = false;
    let mut bypassed_circuit = false;
    let mut aggregation_busy = false;
    let mut unsupported_aggregate_logprobs = false;
    // Exactly ONE candidate loop for chat + embedding. Key failover is confined to this loop.
    for step in steps {
        let provider = &state.config.providers[step.provider_index];
        if provider.stream_only && !streaming && !matches!(endpoint, Endpoint::Chat) {
            continue; // Only Chat has a validated SSE-to-JSON adapter.
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
            continue;
        }
        // Four permits bound at most 8 MiB of buffered JSON output globally.
        // After two seconds, try the next candidate without consuming any
        // provider key or altering circuit state.
        let _aggregate_budget = if provider.stream_only && !streaming {
            match tokio::time::timeout(
                Duration::from_secs(2),
                state.aggregation_slots.clone().acquire_owned(),
            )
            .await
            {
                Ok(Ok(permit)) => Some(permit),
                _ => {
                    Metrics::inc(&state.metrics.aggregation_busy);
                    crate::ops_log::warn("aggregate_capacity");
                    aggregation_busy = true;
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
        let upstream_stream =
            streaming || (provider.stream_only && matches!(endpoint, Endpoint::Chat));
        if upstream_stream && !streaming {
            object.insert("stream".into(), Value::Bool(true));
        }
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
                    ops_log::warn("transport_failure");
                    record_circuit_failure(&mut circuit_ticket, &state.metrics);
                    break;
                }
            };
            let upstream = match response {
                Ok(response) => response,
                Err(_) => {
                    Metrics::inc(&state.metrics.upstream_transport_failures);
                    ops_log::warn("transport_failure");
                    record_circuit_failure(&mut circuit_ticket, &state.metrics);
                    break;
                } // key-neutral
            };
            let status = upstream.status().as_u16();
            let outcome = classify(Some(status));
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
                continue; // try a different key; each key is attempted once
            }
            if outcome == Outcome::Success {
                if streaming {
                    match chat_stream::start(
                        upstream,
                        ChatSse::new(expected)
                            .expect("validated stream n")
                            .with_tool_choice(payload.get("tool_choice")),
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
                            ops_log::warn("stream_preflight_failure");
                            record_circuit_failure(&mut circuit_ticket, &state.metrics);
                            break;
                        } // preflight rejected
                    }
                }
                let result = if upstream_stream && !streaming {
                    tokio::time::timeout_at(
                        deadline,
                        chat_aggregate::aggregate(
                            upstream,
                            expected,
                            &step.model,
                            payload.get("tool_choice"),
                        ),
                    )
                    .await
                } else {
                    tokio::time::timeout_at(deadline, bounded_json(upstream)).await
                };
                match result {
                    Ok(Ok(value))
                        if response_validation::valid_result(endpoint, &payload, &value) =>
                    {
                        state.pools[step.provider_index].record_success(&lease);
                        record_circuit_success(&mut circuit_ticket, &state.metrics);
                        Metrics::inc(&state.metrics.upstream_success);
                        if upstream_stream && !streaming {
                            Metrics::inc(&state.metrics.aggregation_completed);
                            ops_log::info("aggregation_completed");
                        }
                        return (StatusCode::OK, Json(value)).into_response();
                    }
                    _ => {
                        Metrics::inc(&state.metrics.upstream_invalid_responses);
                        if upstream_stream && !streaming {
                            ops_log::warn("aggregation_failed");
                        } else {
                            ops_log::warn("upstream_invalid");
                        }
                        record_circuit_failure(&mut circuit_ticket, &state.metrics);
                        break;
                    } // next step
                }
            }
            Metrics::inc(&state.metrics.upstream_http_failures);
            ops_log::warn("provider_http_failure");
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
    if unsupported_aggregate_logprobs && !attempted_network {
        return error(
            StatusCode::NOT_IMPLEMENTED,
            "aggregate_logprobs_unsupported",
            "SSE-only Chat aggregation cannot preserve token logprobs; use a JSON-capable provider",
        );
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
        return response;
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
        return response;
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
