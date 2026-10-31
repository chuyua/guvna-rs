//! Authenticated native lifecycle operations use the recorded account only.
use super::{
    AppState, authenticated_body, dispatch, error,
    response_routes::{Binding, Context},
};
use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{Method, Request, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Arc, time::Duration};

fn state_error(status: StatusCode, code: &str, message: &str) -> Box<Response> {
    Box::new(error(status, code, message))
}
pub(super) fn context(
    state: &AppState,
    owner: usize,
    raw: &Value,
) -> Result<Context, Box<Response>> {
    let previous = raw.get("previous_response_id").filter(|v| !v.is_null());
    let conversation = raw.get("conversation").filter(|v| !v.is_null());
    if previous.is_some() && conversation.is_some() {
        return Err(state_error(
            StatusCode::BAD_REQUEST,
            "conflicting_response_state",
            "conversation and previous_response_id cannot be combined",
        ));
    }
    if conversation.is_some_and(|v| v.is_object() && v.as_object().is_some_and(|m| m.len() != 1)) {
        return Err(state_error(
            StatusCode::BAD_REQUEST,
            "invalid_conversation",
            "conversation must be an ID or an object containing only id",
        ));
    }
    let reference = if let Some(id) = previous {
        Some((false, id.as_str()))
    } else {
        conversation.map(|v| {
            (
                true,
                v.as_str().or_else(|| v.get("id").and_then(Value::as_str)),
            )
        })
    };
    let mut pin = if let Some((conversation, id)) = reference {
        let id = id
            .filter(|v| !v.is_empty() && v.len() <= 512)
            .ok_or_else(|| {
                state_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_response_reference",
                    "A nonempty state ID is required",
                )
            })?;
        let binding = state
            .response_routes
            .get(owner, conversation, id)
            .ok_or_else(|| {
                state_error(
                    StatusCode::NOT_FOUND,
                    "response_not_found",
                    "Response state is unknown, expired or unavailable after restart",
                )
            })?;
        if raw.get("model").and_then(Value::as_str) != Some(binding.logical_model.as_str()) {
            return Err(state_error(
                StatusCode::CONFLICT,
                "response_model_mismatch",
                "Continue using the original logical model",
            ));
        }
        Some(binding)
    } else {
        None
    };
    if let Some(items) = raw.get("input").and_then(Value::as_array) {
        for item in items {
            if item.get("encrypted_content").is_some_and(|v| !v.is_null()) {
                let id = item.get("id").and_then(Value::as_str).ok_or_else(|| {
                    state_error(
                        StatusCode::NOT_IMPLEMENTED,
                        "encrypted_item_owner_unknown",
                        "Encrypted replay requires an item ID previously returned by this gateway",
                    )
                })?;
                let origin = state.response_routes.get_item(owner, id).ok_or_else(|| {
                    state_error(
                        StatusCode::NOT_FOUND,
                        "encrypted_item_not_found",
                        "Encrypted item ownership is unknown or expired",
                    )
                })?;
                if raw.get("model").and_then(Value::as_str) != Some(origin.logical_model.as_str())
                    || pin.as_ref().is_some_and(|old| !old.same_origin(&origin))
                {
                    return Err(state_error(
                        StatusCode::CONFLICT,
                        "mixed_state_owners",
                        "Native state references must share the original account and model",
                    ));
                }
                pin = Some(origin);
            }
        }
    }
    Ok(Context { owner, pin })
}
pub(super) fn remember(
    state: &AppState,
    owner: usize,
    value: &Value,
    binding: Binding,
    stored: bool,
) -> Result<(), ()> {
    let id = value["id"].as_str().ok_or(())?;
    let conversation = value
        .get("conversation")
        .filter(|v| !v.is_null())
        .map(|v| v["id"].as_str().ok_or(()))
        .transpose()?;
    let items: Vec<_> = value
        .get("output")
        .and_then(Value::as_array)
        .ok_or(())?
        .iter()
        .filter(|item| item.get("encrypted_content").is_some_and(|v| !v.is_null()))
        .map(|item| item.get("id").and_then(Value::as_str).ok_or(()))
        .collect::<Result<_, _>>()?;
    state.response_routes.remember_response(
        owner,
        stored.then_some(id),
        conversation,
        &items,
        binding,
    )
}

pub(super) async fn retrieve(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    request: Request<Body>,
) -> Response {
    forward(state, id, request, Method::GET, "").await
}
pub(super) async fn delete(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    request: Request<Body>,
) -> Response {
    forward(state, id, request, Method::DELETE, "").await
}
pub(super) async fn cancel(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    request: Request<Body>,
) -> Response {
    forward(state, id, request, Method::POST, "cancel").await
}
pub(super) async fn input_items(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    request: Request<Body>,
) -> Response {
    forward(state, id, request, Method::GET, "input_items").await
}

async fn forward(
    state: Arc<AppState>,
    id: String,
    request: Request<Body>,
    method: Method,
    suffix: &str,
) -> Response {
    let Some(owner) = state.client_identity(request.headers()) else {
        return super::unauthorized();
    };
    let query = request.uri().query().map(str::to_owned);
    let (_body, _admission, _payload) = match authenticated_body(&state, request).await {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let Some(binding) = state.response_routes.get(owner, false, &id) else {
        return error(
            StatusCode::NOT_FOUND,
            "response_not_found",
            "Unknown or expired response state",
        );
    };
    if suffix == "cancel" && !binding.background {
        return error(
            StatusCode::BAD_REQUEST,
            "response_not_background",
            "Only responses created with background:true can be cancelled",
        );
    }
    let provider = &state.config.providers[binding.provider];
    let Some(lease) =
        state.pools[binding.provider].acquire_pinned(&HashSet::new(), Some(&binding.credential))
    else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "response_account_unavailable",
            "The original upstream account is temporarily unavailable",
        );
    };
    let mut url =
        reqwest::Url::parse(&provider.endpoint("/responses")).expect("validated provider");
    {
        let mut path = url.path_segments_mut().expect("http URL");
        path.push(&id);
        if !suffix.is_empty() {
            path.push(suffix);
        }
    }
    url.set_query(query.as_deref());
    if url
        .query_pairs()
        .any(|(k, v)| k == "stream" && v != "false")
    {
        return error(
            StatusCode::NOT_IMPLEMENTED,
            "retrieve_stream_unsupported",
            "Retrieve uses JSON; creation supports native streaming",
        );
    }
    let operation = async {
        let request = state
            .client
            .request(method.clone(), url)
            .bearer_auth(&lease.secret);
        let upstream = request.send().await.map_err(|_| ())?;
        let status = upstream.status();
        if !status.is_success() {
            let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            return Ok(error(
                status,
                "upstream_lifecycle_rejected",
                "The original upstream rejected this lifecycle operation",
            ));
        }
        if method == Method::DELETE && status == StatusCode::NO_CONTENT {
            state.response_routes.remove(owner, &id);
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
        let (mut value, lease) =
            dispatch::bounded_json(upstream, Arc::clone(&state.large_response_slots), None)
                .await
                .map_err(|_| ())?;
        let valid = if suffix == "input_items" {
            value["object"] == "list" && value["data"].is_array() && value["has_more"].is_boolean()
        } else if method == Method::DELETE {
            value["id"] == id && value["object"] == "response.deleted" && value["deleted"] == true
        } else {
            value["id"] == id
                && value["object"] == "response"
                && value["output"].is_array()
                && matches!(
                    value["status"].as_str(),
                    Some(
                        "queued"
                            | "in_progress"
                            | "completed"
                            | "incomplete"
                            | "failed"
                            | "cancelled"
                    )
                )
        };
        if !valid {
            return Err(());
        }
        if method == Method::DELETE {
            state.response_routes.remove(owner, &id);
        } else if suffix != "input_items" {
            value["model"] = json!(binding.logical_model);
        }
        Ok(dispatch::hold_buffered_response(
            (status, Json(value)).into_response(),
            lease,
        ))
    };
    match tokio::time::timeout(Duration::from_secs(90), operation).await {
        Ok(Ok(v)) => v,
        Ok(Err(())) => error(
            StatusCode::BAD_GATEWAY,
            "invalid_lifecycle_response",
            "Invalid upstream lifecycle response",
        ),
        Err(_) => error(
            StatusCode::GATEWAY_TIMEOUT,
            "lifecycle_timeout",
            "Upstream lifecycle operation timed out",
        ),
    }
}

pub(super) async fn input_tokens(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
) -> Response {
    let Some(owner) = state.client_identity(request.headers()) else {
        return super::unauthorized();
    };
    let (body, admission, payload) = match authenticated_body(&state, request).await {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let large = body.len() >= super::LARGE_REQUEST_BODY_BYTES;
    let raw: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                "Expected a JSON object",
            );
        }
    };
    drop(body);
    let _payload = if large { Some(payload) } else { None };
    if raw
        .get("model")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
        || !raw.is_object()
    {
        return error(
            StatusCode::BAD_REQUEST,
            "missing_model",
            "model is required for native token routing",
        );
    }
    if raw.get("stream").is_some_and(|v| v != &Value::Bool(false)) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_stream",
            "Input token counting is nonstreaming",
        );
    }
    let context = match context(&state, owner, &raw) {
        Ok(v) => v,
        Err(e) => return *e,
    };
    dispatch::dispatch_input_tokens(state, raw, admission, context, large).await
}
