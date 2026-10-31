//! Owns the Chat SSE streaming lifecycle, preflight and cancellation metrics.
//! No request routing or key retry policy: the single dispatch candidate walk
//! transfers ownership only after the first valid event is observed.
use super::{AppState, LiveRouteSnapshot, record_circuit_failure, record_circuit_success};
use crate::{
    circuit::Ticket,
    keypool::Lease,
    metrics::Metrics,
    native_responses_sse::NativeResponsesSse,
    outcome::Outcome,
    sse::{ChatSse, SseError},
};
use axum::{
    body::{Body, Bytes},
    response::Response,
};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::OwnedSemaphorePermit;

// Bound a single upstream read: even valid tiny events must not create an unbounded queue.
const MAX_STREAM_CHUNK_BYTES: usize = 2 * 1024 * 1024;
// SSE heartbeat comments keep the socket alive but are not model progress.
// Bound admission even if a provider sends one heartbeat every few seconds.
const MAX_STREAM_PROGRESS_IDLE: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PreflightError {
    InvalidStream,
    TransportFailure,
    Timeout,
    LocalLimit,
    UnsupportedProtocol,
}
impl PreflightError {
    pub(super) fn failure_type(self) -> &'static str {
        match self {
            Self::InvalidStream => "invalid_response",
            Self::TransportFailure => "transport_error",
            Self::Timeout => "timeout",
            Self::LocalLimit => "local_error",
            Self::UnsupportedProtocol => "capability_mismatch",
        }
    }

    pub(super) fn blame_provider(self) -> bool {
        matches!(self, Self::InvalidStream | Self::TransportFailure)
    }
}

pub(super) struct StreamRoute {
    pub(super) provider_index: usize,
    pub(super) endpoint: &'static str,
    pub(super) model: String,
    pub(super) telemetry_model: String,
    pub(super) live: Arc<LiveRouteSnapshot>,
    pub(super) request_id: String,
    pub(super) attempt_index: u16,
    pub(super) response_owner: Option<super::response_routes::NativeOwner>,
    pub(super) logical_model: String,
    pub(super) output_format: Option<crate::structured_output::OutputFormat>,
}

#[derive(Clone)]
pub(super) struct NativeResponseStream;

enum StreamParser {
    Chat(ChatSse),
    Native(NativeResponsesSse),
    Bridge(Box<super::chat_responses_stream::ChatResponsesStream>),
}

impl StreamParser {
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<Bytes>, SseError> {
        match self {
            Self::Chat(parser) => parser.push(chunk),
            Self::Native(parser) => parser.push(chunk),
            Self::Bridge(parser) => parser.push(chunk),
        }
    }
    fn is_done(&self) -> bool {
        match self {
            Self::Chat(parser) => parser.is_done(),
            Self::Native(parser) => parser.is_done(),
            Self::Bridge(parser) => parser.is_done(),
        }
    }
    fn take_progress(&mut self) -> bool {
        match self {
            Self::Chat(parser) => parser.take_progress(),
            Self::Native(parser) => parser.take_progress(),
            Self::Bridge(parser) => parser.take_progress(),
        }
    }
    fn hit_resource_limit(&self) -> bool {
        match self {
            Self::Chat(parser) => parser.hit_resource_limit(),
            Self::Native(parser) => parser.hit_resource_limit(),
            Self::Bridge(parser) => parser.hit_resource_limit(),
        }
    }
    fn finalize_on_eof(&mut self) -> Option<Bytes> {
        match self {
            Self::Chat(parser) => parser.finalize_on_eof(),
            Self::Native(parser) => parser.finalize_on_eof(),
            Self::Bridge(_) => None,
        }
    }
    fn is_completion_event(&self, event: &Bytes) -> bool {
        match self {
            Self::Chat(_) | Self::Bridge(_) => event.as_ref() == b"data: [DONE]\n\n",
            Self::Native(parser) => parser.is_completion_event(event),
        }
    }
    fn is_native(&self) -> bool {
        matches!(self, Self::Native(_))
    }
    fn is_failure_event(&self, event: &Bytes) -> bool {
        matches!(self,Self::Native(parser) if parser.is_failure_event(event))
    }
    fn incompatible(&self) -> bool {
        matches!(self,Self::Bridge(parser) if parser.incompatible())
    }
}
struct ChatStreamState {
    owner: Arc<AppState>,
    provider_index: usize,
    endpoint: &'static str,
    model: String,
    telemetry_model: String,
    live: Arc<LiveRouteSnapshot>,
    request_id: String,
    attempt_index: u16,
    response_owner: Option<super::response_routes::NativeOwner>,
    started_at: tokio::time::Instant,
    last_progress_at: tokio::time::Instant,
    lease: Lease,
    upstream: reqwest::Response,
    parser: StreamParser,
    pending: VecDeque<Bytes>,
    terminated: bool,
    failure_type: &'static str,
    terminal_reported: bool,
    _permit: Option<OwnedSemaphorePermit>,
    circuit_ticket: Option<Ticket>,
}

impl Drop for ChatStreamState {
    fn drop(&mut self) {
        // Dropping a partially consumed body must free the upstream request permit.
        if self._permit.is_some() && !self.terminal_reported {
            Metrics::inc(&self.owner.metrics.chat_stream_cancelled);
            // Client disconnect is a terminal observation, not an upstream
            // failure. Never punish the provider, key pool or circuit breaker.
            if let Some(telemetry) = &self.owner.telemetry {
                let provider = &self.owner.config.providers[self.provider_index].name;
                telemetry.observe_stream_health(
                    (self.endpoint, provider),
                    &self.telemetry_model,
                    "client_cancelled",
                    self.started_at.elapsed(),
                    (&self.request_id, self.attempt_index),
                    &self.owner.metrics,
                );
            }
        }
    }
}
fn stream_error(native: bool) -> Bytes {
    if native {
        Bytes::from_static(b"event: error\ndata: {\"type\":\"error\",\"code\":\"upstream_stream_incomplete\",\"message\":\"Upstream Responses stream ended without valid completion\"}\n\n")
    } else {
        Bytes::from_static(b"data: {\"error\":{\"type\":\"gateway_error\",\"code\":\"upstream_stream_incomplete\"}}\n\n")
    }
}

pub(super) async fn start(
    upstream: reqwest::Response,
    parser: ChatSse,
    owner: Arc<AppState>,
    route: StreamRoute,
    lease: Lease,
    permit: &mut Option<OwnedSemaphorePermit>,
    circuit_ticket: &mut Option<Ticket>,
) -> Result<Response, PreflightError> {
    start_with_parser(
        upstream,
        StreamParser::Chat(parser),
        owner,
        route,
        lease,
        permit,
        circuit_ticket,
    )
    .await
}

pub(super) async fn start_native(
    upstream: reqwest::Response,
    owner: Arc<AppState>,
    route: StreamRoute,
    lease: Lease,
    permit: &mut Option<OwnedSemaphorePermit>,
    circuit_ticket: &mut Option<Ticket>,
) -> Result<Response, PreflightError> {
    let parser = NativeResponsesSse::new()
        .with_logical_model(route.logical_model.clone())
        .with_output_contract(route.output_format.clone());
    let mut response = start_with_parser(
        upstream,
        StreamParser::Native(parser),
        owner,
        route,
        lease,
        permit,
        circuit_ticket,
    )
    .await?;
    response.extensions_mut().insert(NativeResponseStream);
    Ok(response)
}

pub(super) async fn start_bridge(
    upstream: reqwest::Response,
    parser: super::chat_responses_stream::ChatResponsesStream,
    owner: Arc<AppState>,
    route: StreamRoute,
    lease: Lease,
    permit: &mut Option<OwnedSemaphorePermit>,
    circuit_ticket: &mut Option<Ticket>,
) -> Result<Response, PreflightError> {
    start_with_parser(
        upstream,
        StreamParser::Bridge(Box::new(parser)),
        owner,
        route,
        lease,
        permit,
        circuit_ticket,
    )
    .await
}

async fn start_with_parser(
    upstream: reqwest::Response,
    parser: StreamParser,
    owner: Arc<AppState>,
    route: StreamRoute,
    lease: Lease,
    permit: &mut Option<OwnedSemaphorePermit>,
    circuit_ticket: &mut Option<Ticket>,
) -> Result<Response, PreflightError> {
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
        return Err(PreflightError::InvalidStream);
    }
    let mut state = ChatStreamState {
        owner,
        provider_index: route.provider_index,
        endpoint: route.endpoint,
        model: route.model,
        telemetry_model: route.telemetry_model,
        live: route.live,
        request_id: route.request_id,
        attempt_index: route.attempt_index,
        response_owner: route.response_owner,
        started_at: tokio::time::Instant::now(),
        last_progress_at: tokio::time::Instant::now(),
        lease,
        upstream,
        parser,
        pending: VecDeque::new(),
        terminated: false,
        failure_type: "invalid_response",
        terminal_reported: false,
        _permit: None,
        circuit_ticket: None,
    };
    preflight_stream(&mut state).await?;
    state.last_progress_at = tokio::time::Instant::now();
    if state.parser.take_progress()
        && let Some(ticket) = circuit_ticket.as_mut()
    {
        ticket.progress();
    }
    state._permit = permit.take();
    state.circuit_ticket = circuit_ticket.take();
    Ok(stream_response(state))
}

async fn preflight_stream(state: &mut ChatStreamState) -> Result<(), PreflightError> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let chunk = state
                .upstream
                .chunk()
                .await
                .map_err(|_| PreflightError::TransportFailure)?
                .ok_or(PreflightError::InvalidStream)?;
            if chunk.len() > MAX_STREAM_CHUNK_BYTES {
                return Err(PreflightError::LocalLimit);
            }
            let events = state.parser.push(&chunk).map_err(|_| {
                if state.parser.hit_resource_limit() {
                    PreflightError::LocalLimit
                } else if state.parser.incompatible() {
                    PreflightError::UnsupportedProtocol
                } else {
                    PreflightError::InvalidStream
                }
            })?;
            state.pending.extend(events);
            if !state.pending.is_empty() {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|_| PreflightError::Timeout)?
}

fn stream_response(state: ChatStreamState) -> Response {
    let output = futures_util::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                if state.parser.is_failure_event(&event) {
                    // A real upstream terminal failure is forwarded exactly
                    // once, never recorded as success or retried after output.
                    state.terminal_reported = true;
                    state.terminated = true;
                    Metrics::inc(&state.owner.metrics.chat_stream_failed);
                    if let Some(telemetry) = &state.owner.telemetry {
                        telemetry.observe_stream_health(
                            (
                                state.endpoint,
                                &state.owner.config.providers[state.provider_index].name,
                            ),
                            &state.telemetry_model,
                            "upstream_failed",
                            state.started_at.elapsed(),
                            (&state.request_id, state.attempt_index),
                            &state.owner.metrics,
                        );
                    }
                    return Some((Ok(event), state));
                }
                if state.parser.is_completion_event(&event) {
                    if let Some(origin) = &state.response_owner {
                        let data = event
                            .split(|b| *b == b'\n')
                            .find_map(|line| line.strip_prefix(b"data: "));
                        let value = data.and_then(|data| {
                            serde_json::from_slice::<serde_json::Value>(data).ok()
                        });
                        let binding = super::response_routes::Binding {
                            provider: state.provider_index,
                            credential: state.lease.env.clone(),
                            model: state.model.clone(),
                            logical_model: origin.logical_model.clone(),
                            background: origin.background,
                        };
                        if value.as_ref().is_none_or(|v| {
                            super::responses_lifecycle::remember(
                                &state.owner,
                                origin.owner,
                                &v["response"],
                                binding,
                                origin.stored,
                            )
                            .is_err()
                        }) {
                            state.terminal_reported = true;
                            state.terminated = true;
                            state.pending.clear();
                            Metrics::inc(&state.owner.metrics.chat_stream_failed);
                            return Some((Ok(stream_error(true)), state));
                        }
                    }
                    state.owner.pools[state.provider_index].record_success(&state.lease);
                    record_circuit_success(&mut state.circuit_ticket, &state.owner.metrics);
                    Metrics::inc(&state.owner.metrics.upstream_success);
                    state.owner.mark_auto_success_in(
                        &state.live,
                        state.provider_index,
                        &state.model,
                    );

                    if let Some(telemetry) = &state.owner.telemetry {
                        let provider = &state.owner.config.providers[state.provider_index].name;
                        telemetry.observe_stream_health(
                            (state.endpoint, provider),
                            &state.telemetry_model,
                            "success",
                            state.started_at.elapsed(),
                            (&state.request_id, state.attempt_index),
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
            // Both the transport read and meaningful-content idle clocks are
            // bounded. Frequent :heartbeats cannot indefinitely hold the
            // request permit or an upstream half-open recovery ticket.
            let now = tokio::time::Instant::now();
            let deadline = (now + Duration::from_secs(15))
                .min(state.last_progress_at + MAX_STREAM_PROGRESS_IDLE);
            match tokio::time::timeout_at(deadline, state.upstream.chunk()).await {
                Ok(Ok(Some(bytes))) => {
                    if bytes.len() > MAX_STREAM_CHUNK_BYTES {
                        state.failure_type = "local_error";
                        state.terminated = true;
                    } else {
                        match state.parser.push(&bytes) {
                            Ok(events) => {
                                if state.parser.take_progress() {
                                    state.last_progress_at = tokio::time::Instant::now();
                                    if let Some(ticket) = state.circuit_ticket.as_mut() {
                                        ticket.progress();
                                    }
                                }
                                state.pending.extend(events);
                            }
                            Err(_) => {
                                if state.parser.hit_resource_limit() {
                                    state.failure_type = "local_error";
                                } else if state.parser.incompatible() {
                                    state.failure_type = "capability_mismatch";
                                }
                                state.terminated = true;
                            }
                        }
                    }
                }
                Ok(Ok(None)) => {
                    if let Some(done) = state.parser.finalize_on_eof() {
                        state.pending.push_back(done);
                    } else {
                        state.terminated = true;
                    }
                }
                Ok(Err(_)) => {
                    state.failure_type = "transport_error";
                    state.terminated = true;
                }
                Err(_) => {
                    state.failure_type = "timeout";
                    state.terminated = true;
                }
            }
            if state.terminated {
                Metrics::inc(&state.owner.metrics.chat_stream_failed);
                // A bad SSE frame, premature EOF or oversized chunk is a
                // provider protocol failure, not evidence of a bad key.
                // A long generation timeout is likewise not credential
                // specific. Only a real body transport error quarantines.
                if state.failure_type == "transport_error" {
                    Metrics::inc(&state.owner.metrics.upstream_transport_failures);
                    if state.owner.pools[state.provider_index]
                        .record(&state.lease, Outcome::TransportFailure)
                    {
                        Metrics::inc(&state.owner.metrics.credential_quarantines);
                    }
                } else if state.failure_type == "invalid_response" {
                    Metrics::inc(&state.owner.metrics.upstream_invalid_responses);
                }
                // A local output policy (large frame or a long idle pause)
                // must not teach the breaker/AutoHealth that a healthy
                // provider is down. Protocol violations and actual I/O
                // failures still count against the provider.
                let blame_provider =
                    matches!(state.failure_type, "transport_error" | "invalid_response");
                if blame_provider {
                    state.owner.mark_auto_failure_in(
                        &state.live,
                        state.provider_index,
                        &state.model,
                    );
                }

                if let Some(telemetry) = &state.owner.telemetry {
                    let provider = &state.owner.config.providers[state.provider_index].name;
                    telemetry.observe_stream_health(
                        (state.endpoint, provider),
                        &state.telemetry_model,
                        state.failure_type,
                        state.started_at.elapsed(),
                        (&state.request_id, state.attempt_index),
                        &state.owner.metrics,
                    );
                }
                if blame_provider {
                    record_circuit_failure(&mut state.circuit_ticket, &state.owner.metrics);
                }
                state.terminal_reported = true;
                return Some((Ok(stream_error(state.parser.is_native())), state));
            }
        }
    });
    Response::builder()
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(Body::from_stream(output))
        .expect("static response headers")
}
