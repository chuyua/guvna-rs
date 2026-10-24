//! Owns the Chat SSE streaming lifecycle, preflight and cancellation metrics.
//! No request routing or key retry policy: the single dispatch candidate walk
//! transfers ownership only after the first valid event is observed.
use super::{AppState, record_circuit_failure, record_circuit_success};
use crate::{circuit::Ticket, keypool::Lease, metrics::Metrics, sse::ChatSse};
use axum::{
    body::{Body, Bytes},
    response::Response,
};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::OwnedSemaphorePermit;

// Bound a single upstream read: even valid tiny events must not create an unbounded queue.
const MAX_STREAM_CHUNK_BYTES: usize = 2 * 1024 * 1024;

pub(super) struct StreamRoute {
    pub(super) provider_index: usize,
    pub(super) model: String,
}
struct ChatStreamState {
    owner: Arc<AppState>,
    provider_index: usize,
    model: String,
    started_at: tokio::time::Instant,
    lease: Lease,
    upstream: reqwest::Response,
    parser: ChatSse,
    pending: VecDeque<Bytes>,
    terminated: bool,
    terminal_reported: bool,
    _permit: Option<OwnedSemaphorePermit>,
    circuit_ticket: Option<Ticket>,
}

impl Drop for ChatStreamState {
    fn drop(&mut self) {
        // Dropping a partially consumed body must free the upstream request permit.
        if self._permit.is_some() && !self.terminal_reported {
            Metrics::inc(&self.owner.metrics.chat_stream_cancelled);
        }
    }
}
fn stream_error() -> Bytes {
    Bytes::from_static(b"data: {\"error\":{\"type\":\"gateway_error\",\"code\":\"upstream_stream_incomplete\"}}\n\n")
}

pub(super) async fn start(
    upstream: reqwest::Response,
    parser: ChatSse,
    owner: Arc<AppState>,
    route: StreamRoute,
    lease: Lease,
    permit: &mut Option<OwnedSemaphorePermit>,
    circuit_ticket: &mut Option<Ticket>,
) -> Result<Response, ()> {
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if !content_type
        .split(';')
        .next()
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"))
    {
        return Err(());
    }
    let mut state = ChatStreamState {
        owner,
        provider_index: route.provider_index,
        model: route.model,
        started_at: tokio::time::Instant::now(),
        lease,
        upstream,
        parser,
        pending: VecDeque::new(),
        terminated: false,
        terminal_reported: false,
        _permit: None,
        circuit_ticket: None,
    };
    preflight_stream(&mut state).await?;
    state._permit = permit.take();
    state.circuit_ticket = circuit_ticket.take();
    Ok(stream_response(state))
}

async fn preflight_stream(state: &mut ChatStreamState) -> Result<(), ()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let chunk = state.upstream.chunk().await.map_err(|_| ())?.ok_or(())?;
            if chunk.len() > MAX_STREAM_CHUNK_BYTES {
                return Err(());
            }
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
                if event == Bytes::from_static(b"data: [DONE]\n\n") {
                    state.owner.pools[state.provider_index].record_success(&state.lease);
                    record_circuit_success(&mut state.circuit_ticket, &state.owner.metrics);
                    Metrics::inc(&state.owner.metrics.upstream_success);
                    state
                        .owner
                        .mark_auto_success(state.provider_index, &state.model);

                    if let Some(telemetry) = &state.owner.telemetry {
                        let provider = &state.owner.config.providers[state.provider_index].name;
                        telemetry.observe_health(
                            "chat",
                            provider,
                            &state.model,
                            "success",
                            state.started_at.elapsed(),
                            &state.owner.metrics,
                        );
                    }
                    Metrics::inc(&state.owner.metrics.chat_stream_completed);
                    state.terminal_reported = true;
                }
                return Some((Ok::<Bytes, std::io::Error>(event), state));
            }
            if state.terminated {
                return None;
            }
            if state.parser.is_done() {
                return None;
            }
            // Bound upstream stalls after the first event without buffering the response.
            match tokio::time::timeout(Duration::from_secs(15), state.upstream.chunk()).await {
                Ok(Ok(Some(bytes))) => {
                    if bytes.len() > MAX_STREAM_CHUNK_BYTES {
                        state.terminated = true;
                    } else {
                        match state.parser.push(&bytes) {
                            Ok(events) => state.pending.extend(events),
                            Err(_) => state.terminated = true,
                        }
                    }
                }
                Ok(Ok(None)) | Ok(Err(_)) | Err(_) => state.terminated = true,
            }
            if state.terminated {
                Metrics::inc(&state.owner.metrics.chat_stream_failed);
                state
                    .owner
                    .mark_auto_failure(state.provider_index, &state.model);

                if let Some(telemetry) = &state.owner.telemetry {
                    let provider = &state.owner.config.providers[state.provider_index].name;
                    telemetry.observe_health(
                        "chat",
                        provider,
                        &state.model,
                        "transport_error",
                        state.started_at.elapsed(),
                        &state.owner.metrics,
                    );
                }
                record_circuit_failure(&mut state.circuit_ticket, &state.owner.metrics);
                state.terminal_reported = true;
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
