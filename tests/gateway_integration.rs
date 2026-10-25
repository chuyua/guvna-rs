use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::State,
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::post,
};
use guvna::{
    config::Config,
    gateway::{AppState, app},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tower::ServiceExt;

#[derive(Clone)]
struct UpstreamState {
    calls: Arc<AtomicUsize>,
    status: StatusCode,
}

async fn upstream(State(state): State<UpstreamState>, request: Request<Body>) -> impl IntoResponse {
    state.calls.fetch_add(1, Ordering::SeqCst);
    if request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        == Some("Bearer bad")
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(axum::http::header::RETRY_AFTER, "3600")],
            axum::Json(json!({"error":"quota"})),
        )
            .into_response();
    }
    if state.status != StatusCode::OK {
        return (state.status, axum::Json(json!({"error":"unavailable"}))).into_response();
    }
    let body = to_bytes(request.into_body(), 1 << 20).await.unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    if value["messages"]
        .as_array()
        .is_some_and(|messages| messages.iter().any(|m| m["content"] == "parallelcheck"))
    {
        let messages = value["messages"].as_array().unwrap();
        let valid = messages.iter().any(|m| {
            m["role"] == "assistant"
                && m["tool_calls"]
                    .as_array()
                    .is_some_and(|calls| calls.len() == 2)
        }) && messages.iter().filter(|m| m["role"] == "tool").count() == 2;
        if !valid {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error":"parallel tool calls not grouped"})),
            )
                .into_response();
        }
    }
    if let Some(marker) = value["messages"][0]["content"]
        .as_str()
        .filter(|s| s.starts_with("invalidchat:"))
    {
        let mode = marker.trim_start_matches("invalidchat:");
        let mut completion = json!({"id":"mock","object":"chat.completion","model":value["model"],"choices":[{"index":0,"message":{"role":"assistant","content":"success"},"finish_reason":"stop"}],"usage":null});
        match mode {
            "nochoices" => {
                completion.as_object_mut().unwrap().remove("choices");
            }
            "empty" => completion["choices"] = json!([]),
            "multiple" => {
                completion["choices"] = json!([{"index":0,"message":{"role":"assistant","content":"success"},"finish_reason":"stop"},{"index":1,"message":{"role":"assistant","content":"extra"},"finish_reason":"stop"}])
            }
            "wrongindex" => completion["choices"][0]["index"] = json!(1),
            "wrongrole" => completion["choices"][0]["message"]["role"] = json!("user"),
            "noid" => {
                completion.as_object_mut().unwrap().remove("id");
            }
            "emptyid" => completion["id"] = json!(""),
            "negativecreated" => completion["created"] = json!(-1),
            "stringcreated" => completion["created"] = json!("yesterday"),
            "emptymodel" => completion["model"] = json!(""),
            "numbermodel" => completion["model"] = json!(123),
            "emptytoolcalls" => {
                completion["choices"][0]["finish_reason"] = json!("tool_calls");
                completion["choices"][0]["message"]["tool_calls"] = json!([]);
            }
            "missingtoolcalls" => completion["choices"][0]["finish_reason"] = json!("tool_calls"),
            "wrongtoolcalls" => completion["choices"][0]["message"]["tool_calls"] = json!("bad"),
            "nulltoolcalls" => completion["choices"][0]["message"]["tool_calls"] = Value::Null,
            _ => unreachable!(),
        }
        return (StatusCode::OK, axum::Json(completion)).into_response();
    }
    let model = value["model"].as_str().unwrap_or_default();
    (
        StatusCode::OK,
        axum::Json(json!({
            "id":"mock","object":"chat.completion","model":model,
            "choices":[{"index":0,"message":if value["messages"][0]["content"] == "calldup" { json!({"role":"assistant","content":null,"tool_calls":[{"id":"call_dup","type":"function","function":{"name":"lookup","arguments":"{}"}},{"id":"call_dup","type":"function","function":{"name":"lookup","arguments":"{}"}}]}) } else if value["messages"][0]["content"] == "callmixed" { json!({"role":"assistant","content":"I will look that up","tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}},{"id":"call_2","type":"function","function":{"name":"lookup","arguments":"{}"}}]}) } else if value["messages"][0]["content"] == "call" || value["messages"][0]["content"] == "callusage" { json!({"role":"assistant","content":null,"tool_calls":[{"id":"call_123","type":"function","function":{"name":"lookup","arguments":json!({"city":"Paris"}).to_string()}}]}) } else { json!({"role":"assistant","content":"success"}) },
            "finish_reason":if value["messages"][0]["content"] == "truncate" { "length" } else if value["messages"][0]["content"] == "call" || value["messages"][0]["content"] == "callusage" || value["messages"][0]["content"] == "callmixed" || value["messages"][0]["content"] == "calldup" { "tool_calls" } else { "stop" }}],
            "usage": if value["messages"][0]["content"] == "usage" || value["messages"][0]["content"] == "callusage" { json!({"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}) } else if value["messages"][0]["content"] == "badusage" { json!({"prompt_tokens":5,"completion_tokens":3,"total_tokens":7}) } else { Value::Null },
            "debug_seed": value["seed"],
            "debug_temperature": value["temperature"]
        })),
    )
        .into_response()
}

async fn start_upstream(status: StatusCode) -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let state = UpstreamState {
        calls: calls.clone(),
        status,
    };
    let routes = Router::new()
        .route("/v1/chat/completions", post(upstream))
        .with_state(state);
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(socket, routes).await.unwrap();
    });
    (format!("http://{addr}"), calls)
}

fn make_app(a: &str, b: &str) -> Router {
    make_app_with_circuit(a, b, false)
}

fn make_app_with_circuit(a: &str, b: &str, enabled: bool) -> Router {
    let yaml = format!(
        r#"
providers:
  - name: broken
    type: openai
    base_url: {a}
    key_env: FIRST
    circuit_breaker:
      enabled: {enabled}
      failure_threshold: 3
      cooldown: 100ms
      max_cooldown: 400ms
  - name: healthy
    type: openai
    base_url: {b}
    key_envs: [BAD, GOOD]
chains:
  - name: demo
    steps:
      - provider: broken
        model: one
      - provider: healthy
        model: two
"#
    );
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    app(AppState::new(
        cfg,
        HashMap::from([
            ("FIRST".into(), "first".into()),
            ("BAD".into(), "bad".into()),
            ("GOOD".into(), "good".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap())
}

#[tokio::test]
async fn online_chain_addition_is_recorded_as_verified_model_not_passthrough() {
    use guvna::gateway::app_with_state;
    use rusqlite::Connection;

    let dir = tempfile::tempdir().unwrap();
    let (url, hits) = start_upstream(StatusCode::OK).await;
    let base: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: p\n    type: openai\n    base_url: {url}\n    key_env: KEY\nchains:\n  - name: fixed\n    steps:\n      - provider: p\n        model: static\n"
    ))
    .unwrap();
    let mut state = AppState::new(
        base.clone(),
        HashMap::from([("KEY".into(), "secret".into())]),
        vec!["client".into()],
    )
    .unwrap();
    state.enable_telemetry(dir.path()).unwrap();
    let (router, running) = app_with_state(state);
    let next: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: p\n    type: openai\n    base_url: {url}\n    key_env: KEY\nchains:\n  - name: fixed\n    steps:\n      - provider: p\n        model: static\n  - name: dynamic\n    steps:\n      - provider: p\n        model: fresh\n"
    ))
    .unwrap();
    running.replace_route_snapshot(next).unwrap();
    let (status, result) = call(router.clone(), request("dynamic", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert!(running.flush_telemetry(std::time::Duration::from_secs(3)));
    let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
    let (model, verified): (String, i64) = db
        .query_row(
            "SELECT model, identity_verified FROM request_events WHERE endpoint='chat'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((model.as_str(), verified), ("fresh", 1));
    running.replace_route_snapshot(base).unwrap();
    let (status, result) = call(router, request("dynamic", "", "client")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{result}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn live_auto_candidate_add_and_delete_invalidates_empty_route_cache() {
    use guvna::gateway::app_with_state;

    let (url, hits) = start_upstream(StatusCode::OK).await;
    let base: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: p\n    type: openai\n    base_url: {url}\n    key_env: KEY\nauto:\n  enabled: true\n  discover_from_chains: true\n  candidates:\n    - provider: p\n      model: impossible\n      min_input_bytes: 9999999\n"
    ))
    .unwrap();
    let (router, state) = app_with_state(
        AppState::new(
            base.clone(),
            HashMap::from([("KEY".into(), "secret".into())]),
            vec!["client".into()],
        )
        .unwrap(),
    );
    let (status, _) = call(router.clone(), request("auto", "", "client")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    let mut added = base.clone();
    added.chains.push(
        serde_yaml::from_str("name: online\nsteps:\n  - provider: p\n    model: discovered\n")
            .unwrap(),
    );
    state.replace_route_snapshot(added).unwrap();
    assert_eq!(state.runtime_status()["auto_candidate_count"], 2);
    let (status, result) = call(router.clone(), request("auto", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(result["model"], "discovered");

    state.replace_route_snapshot(base).unwrap();
    assert_eq!(state.runtime_status()["auto_candidate_count"], 1);
    let (status, _) = call(router, request("auto", "", "client")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "deleted Auto candidate must never be retried from cache"
    );
}

#[tokio::test]
async fn failed_first_candidate_is_correlated_with_one_terminal_request_and_scoring() {
    use guvna::gateway::app_with_state;
    use rusqlite::{Connection, params};
    let dir = tempfile::tempdir().unwrap();
    let (bad, bad_calls) = start_upstream(StatusCode::INTERNAL_SERVER_ERROR).await;
    let (good, good_calls) = start_upstream(StatusCode::OK).await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: broken
    type: openai
    base_url: {bad}
    key_env: FIRST
  - name: healthy
    type: openai
    base_url: {good}
    key_env: SECOND
chains:
  - name: demo
    steps:
      - provider: broken
        model: one
      - provider: healthy
        model: two
"#
    ))
    .unwrap();
    let mut state = AppState::new(
        config,
        HashMap::from([
            ("FIRST".into(), "secret-one".into()),
            ("SECOND".into(), "secret-two".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap();
    state.enable_telemetry(dir.path()).unwrap();
    let (router, shared) = app_with_state(state);
    let (status, result) = call(router, request("demo", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(bad_calls.load(Ordering::SeqCst), 1);
    assert_eq!(good_calls.load(Ordering::SeqCst), 1);
    assert!(shared.flush_telemetry(std::time::Duration::from_secs(3)));
    let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
    let (request_id, attempt, stream): (String, i64, i64) = db
        .query_row(
            "SELECT request_id,attempt_index,stream_requested FROM request_events",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert!(!request_id.is_empty());
    assert_eq!((attempt, stream), (2, 0));
    let (fail_id, fail_attempt, fail_stream): (String, i64, i64) = db
        .query_row(
            "SELECT request_id,attempt_index,stream_requested FROM failure_events",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (fail_id, fail_attempt, fail_stream),
        (request_id.clone(), 1, 0)
    );
    let requests: i64 = db
        .query_row("SELECT COUNT(*) FROM request_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(requests, 1);
    let scoring = guvna::scoring::read_scoring(dir.path(), 1).unwrap();
    assert_eq!(scoring["request_metrics"]["traced_requests"], 1);
    assert_eq!(scoring["request_metrics"]["requests_with_fallback"], 1);
    assert_eq!(scoring["request_metrics"]["fallback_rate"], 1.0);
    assert_eq!(scoring["request_metrics"]["first_attempt_nonstream"], 1);
    assert_eq!(
        scoring["request_metrics"]["first_attempt_successes_nonstream"],
        0
    );
    assert_eq!(
        scoring["request_metrics"]["first_attempt_success_rate_nonstream"],
        0.0
    );
    // A trace is operational metadata, never a source for credentials or prompts.
    assert!(!request_id.contains("secret"));
    let unknown: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM request_events WHERE request_id=?1",
            params![request_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(unknown, 1);
    let (penalty, failures): (i64, i64) = db
        .query_row(
            "SELECT penalty,consecutive_failures FROM provider_health
             WHERE provider='broken' AND model='one'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        (penalty, failures),
        (35, 1),
        "a genuine upstream 5xx must still update persisted provider health"
    );
}

#[tokio::test]
async fn uncatalogued_prefixed_model_strips_only_the_gateway_namespace_on_wire() {
    let (upstream_url, calls) = start_upstream(StatusCode::OK).await;
    let config: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: bridge\n    type: openai\n    base_url: {upstream_url}\n    key_env: BRIDGE\n"
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([("BRIDGE".into(), "bridge-secret".into())]),
        vec!["client".into()],
    )
    .unwrap());
    let (status, result) = call(router, request("bridge/openai/gpt-4o", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["model"], "openai/gpt-4o");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn issue_132_namespaced_embedding_reaches_correct_upstream_without_hijacking_chat() {
    async fn echo_embedding_model(request: Request<Body>) -> impl IntoResponse {
        let body = to_bytes(request.into_body(), 1 << 20).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        (
            StatusCode::OK,
            axum::Json(json!({
                "object": "list",
                "model": payload["model"],
                "data": [{"object":"embedding","index":0,"embedding":[0.25,0.5]}]
            })),
        )
    }
    let (other_url, other_hits) = start_upstream(StatusCode::OK).await;
    let nvidia_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nvidia_url = format!("http://{}", nvidia_listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(
            nvidia_listener,
            Router::new().route("/v1/embeddings", post(echo_embedding_model)),
        )
        .await
        .unwrap();
    });
    let cfg: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: nvidia\n    type: openai\n    base_url: {nvidia_url}\n    key_env: NVIDIA\n    namespace_on_wire: true\n  - name: other\n    type: openai\n    base_url: {other_url}\n    key_env: OTHER\n"
    ))
    .unwrap();
    cfg.validate().unwrap();
    let gateway = app(AppState::new(
        cfg,
        HashMap::from([
            ("NVIDIA".into(), "nvidia-secret".into()),
            ("OTHER".into(), "other-secret".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap());
    let embeddings = Request::builder()
        .method("POST")
        .uri("/v1/embeddings")
        .header("authorization", "Bearer client")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"model":"nvidia/nemotron-3-embed-1b","input":"hello"}).to_string(),
        ))
        .unwrap();
    let (status, body) = call(gateway.clone(), embeddings).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["model"], "nvidia/nemotron-3-embed-1b");
    assert_eq!(body["data"][0]["embedding"], json!([0.25, 0.5]));

    let (status, chat) = call(gateway.clone(), request("other/a-model", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{chat}");
    assert_eq!(chat["model"], "a-model");
    assert_eq!(other_hits.load(Ordering::SeqCst), 1);

    let (status, unknown) = call(gateway, request("unconfigured/model", "", "client")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{unknown}");
    assert_eq!(other_hits.load(Ordering::SeqCst), 1);
}

async fn call(router: Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn request(model: &str, extra: &str, key: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", format!("Bearer {key}"))
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"model":"{model}","messages":[{{"role":"user","content":"hi"}}]{extra}}}"#
        )))
        .unwrap()
}

#[tokio::test]
async fn last_upstream_caller_rejection_preserves_client_status_without_key_leak() {
    let (upstream, hits) = start_upstream(StatusCode::UNPROCESSABLE_ENTITY).await;
    let router = make_app(&upstream, &upstream);
    let (status, body) = call(router, request("demo", "", "client")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "upstream_rejected");
    assert!(!body.to_string().contains("Bearer"));
    assert!(hits.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn upstream_4xx_error_messages_never_reflect_secret_or_prompt_content() {
    async fn secret_bearing_error() -> impl IntoResponse {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            axum::Json(json!({
                "error": {
                    "code": "invalid_request_error",
                    "message": "Bearer provider-secret: confidential prompt content"
                }
            })),
        )
    }
    let routes = Router::new().route("/v1/chat/completions", post(secret_bearing_error));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, routes).await.unwrap() });
    let url = format!("http://{addr}");
    let (status, body) = call(make_app(&url, &url), request("demo", "", "client")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        body["error"]["message"],
        "Upstream rejected request parameters"
    );
    assert!(!body.to_string().contains("provider-secret"));
    assert!(!body.to_string().contains("confidential prompt"));
}

#[tokio::test]
async fn failover_skips_bad_provider_and_key_and_preserves_large_integers() {
    let (a, first_calls) = start_upstream(StatusCode::SERVICE_UNAVAILABLE).await;
    let (b, next_calls) = start_upstream(StatusCode::OK).await;
    let router = make_app(&a, &b);
    let (status, body) = call(
        router,
        request("demo", r#","seed":12345678901234567890"#, "client"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["model"], "two");
    assert_eq!(
        body["debug_seed"].as_u64(),
        Some(12_345_678_901_234_567_890)
    );
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(next_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn candidate_payload_overrides_do_not_leak_to_fallback_steps() {
    let (first, first_calls) = start_upstream(StatusCode::SERVICE_UNAVAILABLE).await;
    let (second, second_calls) = start_upstream(StatusCode::OK).await;
    let config: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: first\n    type: openai\n    base_url: {first}\n    key_env: FIRST\n  - name: second\n    type: openai\n    base_url: {second}\n    key_env: SECOND\nchains:\n  - name: fallback\n    steps:\n      - provider: first\n        model: first-model\n        params:\n          temperature: 0.1\n      - provider: second\n        model: second-model\n"
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([
            ("FIRST".into(), "first-secret".into()),
            ("SECOND".into(), "second-secret".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap());
    let (status, result) = call(router, request("fallback", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["model"], "second-model");
    assert!(
        result["debug_temperature"].is_null(),
        "first-step defaults leaked: {result}"
    );
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn go_auto_discovered_chain_params_fill_missing_and_never_override_caller_values() {
    let (url, hits) = start_upstream(StatusCode::OK).await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: p
    type: openai
    base_url: {url}
    key_env: K
    models: [x]
chains:
  - name: primary
    steps:
      - provider: p
        model: x
        params: {{temperature: 0.25}}
default_chain: primary
auto:
  enabled: true
  discover_from_chains: true
"#
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([("K".into(), "secret".into())]),
        vec!["client".into()],
    )
    .unwrap());
    for (extra, expected) in [
        ("", json!(0.25)),
        (r#","temperature":0.75"#, json!(0.75)),
        (r#","temperature":null"#, Value::Null),
    ] {
        let (status, result) = call(router.clone(), request("auto", extra, "client")).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["model"], "x");
        assert_eq!(result["debug_temperature"], expected);
    }
    assert_eq!(hits.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn default_chain_routes_unknown_model_but_not_named_or_prefixed_models() {
    let (upstream_url, calls) = start_upstream(StatusCode::OK).await;
    let config: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: bridge\n    type: openai\n    base_url: {upstream_url}\n    key_env: BRIDGE\nchains:\n  - name: fallback\n    steps:\n      - provider: bridge\n        model: physical-default\ndefault_chain: fallback\n"
    )).unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([("BRIDGE".into(), "secret".into())]),
        vec!["client".into()],
    )
    .unwrap());
    let (status, result) = call(router.clone(), request("unknown-model", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["model"], "physical-default");
    let (status, result) = call(
        router.clone(),
        request("bridge/physical-direct", "", "client"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["model"], "physical-direct");
    let (status, result) = call(router.clone(), request("fallback", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["model"], "physical-default");
    let (status, result) = call(router, request("auto", "", "client")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{result}");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn reject_bad_key_and_invalid_stream_n_without_calling_upstream() {
    let (a, calls) = start_upstream(StatusCode::OK).await;
    let router = make_app(&a, &a);
    let (status, _) = call(router.clone(), request("demo", "", "wrong")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, result) = call(router, request("demo", r#","stream":true,"n":0"#, "client")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(result["error"]["code"], "invalid_n");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn retry_after_429_suppresses_early_retries_without_suppressing_healthy_keys() {
    let (url, calls) = start_upstream(StatusCode::OK).await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: only
    type: openai
    base_url: {url}
    key_envs: [BAD, GOOD]
    rotation: sequential
    quarantine:
      rate_limit: 100ms
chains:
  - name: demo
    steps:
      - provider: only
        model: one
"#
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([("BAD".into(), "bad".into()), ("GOOD".into(), "good".into())]),
        vec!["client".into()],
    )
    .unwrap());
    assert_eq!(
        call(router.clone(), request("demo", "", "client")).await.0,
        StatusCode::OK
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2); // BAD 429, then GOOD success

    // Longer than the configured 100ms, but much shorter than Retry-After: 3600.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(
        call(router.clone(), request("demo", "", "client")).await.0,
        StatusCode::OK
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3); // Only GOOD, no premature BAD retry
    let (_, metrics) = get_metrics(router, Some("client")).await;
    assert_eq!(metric_value(&metrics, "credential_quarantines_total"), 1);
    assert_eq!(metric_value(&metrics, "upstream_attempts_total"), 3);
}

#[tokio::test]
async fn all_rate_limited_credentials_return_bounded_retry_after_without_network_retry() {
    let (url, calls) = start_upstream(StatusCode::OK).await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: only
    type: openai
    base_url: {url}
    key_env: BAD
    quarantine:
      rate_limit: 100ms
chains:
  - name: demo
    steps:
      - provider: only
        model: one
auto:
  enabled: true
  candidates:
    - provider: only
      model: one
      priority: 1
"#
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([("BAD".into(), "bad".into())]),
        vec!["client".into()],
    )
    .unwrap());
    // Fresh 429 comes from the actual upstream. The longer 3600s
    // Retry-After beats the configured 100ms minimum key cooldown.
    let response = router
        .clone()
        .oneshot(request("demo", "", "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry = response.headers()["retry-after"]
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!((3599..=3600).contains(&retry), "{retry}");
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "providers_rate_limited");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // 24 concurrent Chat and Auto calls must get a bounded retry hint with
    // zero further upstream attempts and no credential re-quarantining.
    let responses = futures_util::future::join_all((0..24).map(|index| {
        let router = router.clone();
        async move {
            let request = if index % 2 == 0 {
                request("demo", "", "client")
            } else {
                auto_request(
                    "/v1/auto",
                    "client",
                    json!({"messages":[{"role":"user","content":"hello"}]}),
                )
            };
            router.oneshot(request).await.unwrap()
        }
    }))
    .await;
    for response in responses {
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let retry = response.headers()["retry-after"]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert!((3598..=3600).contains(&retry));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (_, metrics) = get_metrics(router, Some("client")).await;
    assert_eq!(metric_value(&metrics, "credential_quarantines_total"), 1);
    assert_eq!(metric_value(&metrics, "upstream_attempts_total"), 1);
    assert_eq!(metric_value(&metrics, "rate_limit_exhaustions_total"), 25);
}

#[tokio::test]
async fn mixed_rate_limit_and_provider_failure_do_not_misreport_global_429() {
    let (rate_url, rate_calls) = start_upstream(StatusCode::OK).await;
    let (down_url, down_calls) = start_upstream(StatusCode::SERVICE_UNAVAILABLE).await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: rate
    type: openai
    base_url: {rate_url}
    key_env: BAD
    quarantine:
      rate_limit: 100ms
  - name: unavailable
    type: openai
    base_url: {down_url}
    key_env: DOWN
chains:
  - name: demo
    steps:
      - provider: rate
        model: one
      - provider: unavailable
        model: two
"#
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([("BAD".into(), "bad".into()), ("DOWN".into(), "down".into())]),
        vec!["client".into()],
    )
    .unwrap());
    for _ in 0..2 {
        let response = router
            .clone()
            .oneshot(request("demo", "", "client"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(response.headers().get("retry-after").is_none());
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(body["error"]["code"], "upstream_unavailable");
    }
    assert_eq!(rate_calls.load(Ordering::SeqCst), 1);
    assert_eq!(down_calls.load(Ordering::SeqCst), 2);
    let (_, metrics) = get_metrics(router, Some("client")).await;
    assert_eq!(metric_value(&metrics, "rate_limit_exhaustions_total"), 0);
    assert_eq!(metric_value(&metrics, "candidate_fallbacks_total"), 2);
}

#[tokio::test]
async fn unauthenticated_posts_never_poll_unbounded_request_bodies() {
    let (url, calls) = start_upstream(StatusCode::OK).await;
    let router = make_app(&url, &url);
    for path in ["/v1/chat/completions", "/v1/embeddings", "/v1/responses"] {
        // A never-ending body used to hold the handler open before authentication.
        // The request must return 401 even if not a single byte arrives.
        let body =
            Body::from_stream(futures_util::stream::pending::<Result<Bytes, std::io::Error>>());
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("authorization", "Bearer invalid")
            .body(body)
            .unwrap();
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            router.clone().oneshot(request),
        )
        .await
        .expect("authorization must not wait for request body")
        .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn stalled_authenticated_uploads_cannot_bypass_payload_memory_gate() {
    let (url, calls) = start_upstream(StatusCode::OK).await;
    let router = make_app(&url, &url);
    let polls = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let polls = polls.clone();
        let body = Body::from_stream(futures_util::stream::poll_fn(move |_| {
            polls.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::<Option<Result<Bytes, std::io::Error>>>::Pending
        }));
        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("authorization", "Bearer client")
            .body(body)
            .unwrap();
        let router = router.clone();
        tasks.push(tokio::spawn(async move { router.oneshot(request).await }));
    }
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while polls.load(Ordering::SeqCst) < 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all stalled uploads must reach body polling");

    // A ninth request must fail before touching its own never-ending body.
    for path in ["/v1/chat/completions", "/v1/embeddings", "/v1/responses"] {
        let body =
            Body::from_stream(futures_util::stream::pending::<Result<Bytes, std::io::Error>>());
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("authorization", "Bearer client")
            .body(body)
            .unwrap();
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            router.clone().oneshot(request),
        )
        .await
        .expect("admission must reject before buffering another body")
        .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(response.headers()["retry-after"], "1");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    let (status, _) = call(router, request("demo", "", "client")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "cancelled uploads must free admission slots"
    );
}

#[tokio::test]
async fn authorized_posts_still_enforce_the_8_mib_body_limit() {
    let (url, calls) = start_upstream(StatusCode::OK).await;
    let router = make_app(&url, &url);
    let too_large = vec![b'x'; 8 * 1024 * 1024 + 1];
    for path in ["/v1/chat/completions", "/v1/embeddings", "/v1/responses"] {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("authorization", "Bearer client")
            .body(Body::from(too_large.clone()))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE, "{path}");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct SseUpstream {
    hits: Arc<AtomicUsize>,
    chunks: Vec<Bytes>,
}

async fn serve_sse(State(state): State<SseUpstream>) -> impl IntoResponse {
    state.hits.fetch_add(1, Ordering::SeqCst);
    let chunks = state.chunks.clone();
    let output = futures_util::stream::iter(chunks.into_iter().map(Ok::<_, std::io::Error>));
    (
        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
        Body::from_stream(output),
    )
}

async fn start_sse(chunks: Vec<&'static [u8]>) -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let state = SseUpstream {
        hits: hits.clone(),
        chunks: chunks.into_iter().map(Bytes::from_static).collect(),
    };
    let router = Router::new()
        .route("/v1/chat/completions", post(serve_sse))
        .with_state(state);
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(socket, router).await.unwrap();
    });
    (format!("http://{addr}"), hits)
}

const SSE_START: &[u8] = b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n";
const SSE_END: &[u8] =
    b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
const SSE_DONE: &[u8] = b"data: [DONE]\n\n";

#[tokio::test(flavor = "current_thread")]
async fn healthy_stream_survives_ninety_seconds_of_regular_progress() {
    use std::time::Duration;

    let upstream = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let chunks = futures_util::stream::unfold(0u8, |index| async move {
                match index {
                    0 => Some((Ok::<_, std::io::Error>(Bytes::from_static(SSE_START)), 1)),
                    1..=20 => {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        Some((
                            Ok(Bytes::from_static(b": upstream heartbeat\n\n")),
                            index + 1,
                        ))
                    }
                    21 => Some((Ok(Bytes::from_static(SSE_END)), 22)),
                    22 => Some((Ok(Bytes::from_static(SSE_DONE)), 23)),
                    _ => None,
                }
            });
            (
                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                Body::from_stream(chunks),
            )
        }),
    );
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(socket, upstream).await.unwrap() });
    let url = format!("http://{addr}");
    let response = make_app(&url, &url)
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Pause only after the first SSE event has passed preflight. Advance the
    // Tokio clock in small increments so each upstream heartbeat is consumed.
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let collected = tokio::spawn(async move { to_bytes(response.into_body(), 1 << 20).await });
    for _ in 0..60 {
        tokio::time::advance(Duration::from_secs(5)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        if collected.is_finished() {
            break;
        }
    }
    assert!(started.elapsed() > Duration::from_secs(90));
    tokio::time::resume();
    let bytes = tokio::time::timeout(Duration::from_secs(2), collected)
        .await
        .expect("healthy SSE stream should complete")
        .unwrap()
        .unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
    assert!(!text.contains("upstream_stream_incomplete"), "{text}");
}

#[tokio::test]
async fn chat_stream_fails_over_before_first_valid_event() {
    let (a, bad_hits) = start_sse(vec![b"data: {\"choices\":[]}\n\n"]).await;
    let (b, good_hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let router = make_app(&a, &b);
    let response = router
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
    assert_eq!(bad_hits.load(Ordering::SeqCst), 1);
    assert_eq!(good_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn incomplete_stream_never_fails_over_after_first_event() {
    let (a, first_hits) = start_sse(vec![SSE_START]).await;
    let (b, backup_hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let router = make_app(&a, &b);
    let response = router
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.contains("upstream_stream_incomplete"), "{text}");
    assert!(!text.contains("[DONE]"), "{text}");
    assert_eq!(first_hits.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn sse_duplicate_terminal_marker_is_never_relayed_twice() {
    let (upstream, hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE, SSE_DONE]).await;
    let router = make_app(&upstream, &upstream);
    let response = router
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert_eq!(text.matches("data: [DONE]").count(), 1, "{text}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sse_truncated_finish_event_never_claims_success() {
    let (upstream, hits) = start_sse(vec![
        SSE_START,
        b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]",
    ])
    .await;
    let router = make_app(&upstream, &upstream);
    let response = router
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.contains("upstream_stream_incomplete"), "{text}");
    assert!(!text.contains("data: [DONE]"), "{text}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn duplicate_choice_index_fails_over_before_first_event() {
    let duplicate = b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":null},{\"index\":0,\"delta\":{},\"finish_reason\":null}]}\n\n";
    let (a, bad_hits) = start_sse(vec![duplicate]).await;
    let (b, good_hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let response = make_app(&a, &b)
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
    assert_eq!(bad_hits.load(Ordering::SeqCst), 1);
    assert_eq!(good_hits.load(Ordering::SeqCst), 1);
}

// A stalled upstream keeps its response body alive until the downstream reader
// finishes or disconnects. Dropping that body must release the upstream stream.
struct DropSignal(Arc<AtomicUsize>);
impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn start_stalled_sse() -> (String, Arc<AtomicUsize>) {
    start_stalled_sse_frame(SSE_START).await
}

async fn start_stalled_sse_frame(frame: &'static [u8]) -> (String, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    let signal = drops.clone();
    let router = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let signal = signal.clone();
            async move {
                let output = futures_util::stream::unfold(
                    (false, DropSignal(signal)),
                    move |(sent, guard)| async move {
                        if sent {
                            std::future::pending::<
                                Option<(Result<Bytes, std::io::Error>, (bool, DropSignal))>,
                            >()
                            .await
                        } else {
                            Some((Ok(Bytes::from_static(frame)), (true, guard)))
                        }
                    },
                );
                (
                    [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                    Body::from_stream(output),
                )
            }
        }),
    );
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(socket, router).await.unwrap();
    });
    (format!("http://{addr}"), drops)
}

#[tokio::test]
async fn stalled_sse_emits_one_incomplete_error_without_failover() {
    let (a, drops) = start_stalled_sse().await;
    let (b, backup_hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let response = make_app(&a, &b)
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(19),
        to_bytes(response.into_body(), 1 << 20),
    )
    .await
    .expect("idle timeout should complete the response")
    .unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert_eq!(
        text.matches("upstream_stream_incomplete").count(),
        1,
        "{text}"
    );
    assert!(!text.contains("data: [DONE]"), "{text}");
    assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("upstream stream must be dropped");
}

#[tokio::test]
async fn downstream_disconnect_drops_stalled_upstream_promptly() {
    let (a, drops) = start_stalled_sse().await;
    let response = make_app(&a, &a)
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    use http_body_util::BodyExt;
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), body.frame())
        .await
        .expect("first frame timed out")
        .expect("body ended")
        .unwrap();
    assert!(first.into_data().unwrap().starts_with(b"data: "));
    drop(body);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("downstream cancellation should release upstream");
}

#[tokio::test]
async fn streamed_required_tool_choice_cannot_complete_plain_text() {
    let (upstream, hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let response = make_app(&upstream, &upstream)
        .oneshot(request(
            "demo",
            r#","stream":true,"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],"tool_choice":"required""#,
            "client",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.contains("upstream_stream_incomplete"), "{text}");
    assert!(!text.contains("data: [DONE]"), "{text}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn malformed_tool_arguments_cannot_complete_chat_stream() {
    let tool = b"data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"[1]\"}}]},\"finish_reason\":null}]}\n\n";
    let finish =
        b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n";
    let (upstream, hits) = start_sse(vec![tool, finish, SSE_DONE]).await;
    let response = make_app(&upstream, &upstream)
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.contains("upstream_stream_incomplete"), "{text}");
    assert!(!text.contains("data: [DONE]"), "{text}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn truncated_tool_stream_preserves_length_finish_without_replaying_partial_output() {
    let tool = b"data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"key\\\":\"}}]},\"finish_reason\":null}]}\n\n";
    let finish =
        b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}\n\n";
    let (primary, primary_hits) = start_sse(vec![tool, finish, SSE_DONE]).await;
    let (backup, backup_hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let response = make_app(&primary, &backup)
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    // The upstream's explicit "length" means the tool invocation was
    // truncated, not successfully executable. Forward that truthful terminal
    // outcome instead of replacing a valid incomplete response with an error.
    assert!(text.contains(r#""finish_reason":"length""#), "{text}");
    assert!(text.contains("data: [DONE]"), "{text}");
    assert!(!text.contains(r#""finish_reason":"tool_calls""#), "{text}");
    assert!(!text.contains("upstream_stream_incomplete"), "{text}");
    assert_eq!(primary_hits.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn malformed_sse_after_first_event_does_not_fail_over_or_claim_success() {
    let (a, first_hits) = start_sse(vec![SSE_START, b"data: not-json\n\n"]).await;
    let (b, backup_hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let response = make_app(&a, &b)
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert_eq!(
        text.matches("upstream_stream_incomplete").count(),
        1,
        "{text}"
    );
    assert!(!text.contains("data: [DONE]"), "{text}");
    assert_eq!(first_hits.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn multi_event_chunk_preserves_order_and_one_terminal_marker() {
    let mut combined = Vec::new();
    combined.extend_from_slice(SSE_START);
    combined.extend_from_slice(SSE_END);
    combined.extend_from_slice(SSE_DONE);
    let (upstream, hits) = start_sse(vec![Box::leak(combined.into_boxed_slice())]).await;
    let response = make_app(&upstream, &upstream)
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert_eq!(text.matches("data: ").count(), 3, "{text}");
    assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oversized_first_sse_chunk_fails_over_before_response_starts() {
    let oversized: &'static [u8] = Box::leak(vec![b'x'; 2 * 1024 * 1024 + 1].into_boxed_slice());
    let (a, bad_hits) = start_sse(vec![oversized]).await;
    let (b, good_hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let response = make_app(&a, &b)
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    assert!(bytes.ends_with(SSE_DONE));
    assert_eq!(bad_hits.load(Ordering::SeqCst), 1);
    assert_eq!(good_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oversized_later_sse_chunk_returns_incomplete_without_failover() {
    let oversized: &'static [u8] = Box::leak(vec![b'x'; 2 * 1024 * 1024 + 1].into_boxed_slice());
    let (a, first_hits) = start_sse(vec![SSE_START, oversized]).await;
    let (b, backup_hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let response = make_app(&a, &b)
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert_eq!(
        text.matches("upstream_stream_incomplete").count(),
        1,
        "{text}"
    );
    assert!(!text.contains("[DONE]"));
    assert_eq!(first_hits.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mixed_endpoint_slow_uploads_share_payload_admission_telemetry_and_recover() {
    let (url, calls) = start_upstream(StatusCode::OK).await;
    let router = make_app(&url, &url);
    let mut upload_tasks = Vec::with_capacity(8);
    // Exercise the payload-memory gate across all three body-reading routes.
    for index in 0..8 {
        let path = match index % 3 {
            0 => "/v1/chat/completions",
            1 => "/v1/embeddings",
            _ => "/v1/responses",
        };
        let req = Request::builder()
            .method("POST")
            .uri(path)
            .header("authorization", "Bearer client")
            .body(Body::from_stream(futures_util::stream::pending::<
                Result<Bytes, std::io::Error>,
            >()))
            .unwrap();
        let app = router.clone();
        upload_tasks.push(tokio::spawn(async move { app.oneshot(req).await }));
    }
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        loop {
            let (_, metrics) = get_metrics(router.clone(), Some("client")).await;
            if metric_value(&metrics, "active_requests") == 8 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all mixed uploads must reserve admission permits");
    for path in ["/v1/chat/completions", "/v1/embeddings", "/v1/responses"] {
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            router.clone().oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("authorization", "Bearer client")
                    .body(Body::from_stream(futures_util::stream::pending::<
                        Result<Bytes, std::io::Error>,
                    >()))
                    .unwrap(),
            ),
        )
        .await
        .expect("overload must not read another upload body")
        .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(response.headers()["retry-after"], "1");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let (_, metrics) = get_metrics(router.clone(), Some("client")).await;
    assert_eq!(metric_value(&metrics, "admission_rejections_total"), 3);
    assert_eq!(metric_value(&metrics, "active_requests"), 8);
    for task in upload_tasks {
        task.abort();
        let _ = task.await;
    }
    let (_, metrics) = get_metrics(router.clone(), Some("client")).await;
    assert_eq!(metric_value(&metrics, "active_requests"), 0);
    assert_eq!(
        call(router, request("demo", "", "client")).await.0,
        StatusCode::OK,
        "admission must recover after cancellation"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn admission_control_bounds_concurrent_streams_and_recovers_after_disconnect() {
    let (upstream, dropped) = start_stalled_sse().await;
    let router = make_app(&upstream, &upstream);
    let mut bodies = Vec::new();
    for _ in 0..128 {
        let response = router
            .clone()
            .oneshot(request("demo", r#","stream":true"#, "client"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        bodies.push(response.into_body());
    }
    let overloaded = router
        .clone()
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(overloaded.headers()["retry-after"], "1");
    let bytes = to_bytes(overloaded.into_body(), 1 << 20).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["error"]["code"],
        "gateway_overloaded"
    );
    drop(bodies);
    let recovered = router
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(recovered.status(), StatusCode::OK);
    drop(recovered);
    assert!(dropped.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn invalid_stream_and_empty_model_rejected_without_upstream_calls() {
    let (url, hits) = start_upstream(StatusCode::OK).await;
    let router = make_app(&url, &url);
    for (model, extra, code) in [
        ("demo", r#","stream":"true""#, "invalid_stream"),
        ("demo", r#","stream":null"#, "invalid_stream"),
        ("demo", r#","n":0"#, "invalid_n"),
        ("demo", r#","n":-1"#, "invalid_n"),
        ("demo", r#","n":"1""#, "invalid_n"),
        ("demo", r#","n":1.5"#, "invalid_n"),
        ("", "", "invalid_model"),
    ] {
        let (status, value) = call(router.clone(), request(model, extra, "client")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value["error"]["code"], code);
    }
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn model_catalog_and_detail_are_consistent_and_authorized() {
    let (url, hits) = start_upstream(StatusCode::OK).await;
    let router = make_app(&url, &url);
    let req = |uri: &str, key: &str| {
        Request::builder()
            .uri(uri)
            .header("authorization", format!("Bearer {key}"))
            .body(Body::empty())
            .unwrap()
    };
    let (status, catalog) = call(router.clone(), req("/v1/models", "client")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(catalog["data"][0]["id"], "demo");
    let (status, detail) = call(router.clone(), req("/v1/models/demo", "client")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail, catalog["data"][0]);
    let (status, missing) = call(router.clone(), req("/v1/models/missing", "client")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["error"]["code"], "model_not_found");
    let (status, _) = call(router, req("/v1/models/demo", "wrong")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn catalog_exposes_configured_provider_models_without_upstream_discovery() {
    let cfg: guvna::config::Config = serde_yaml::from_str(
        "providers:\n  - name: fast\n    type: openai\n    base_url: https://example.org\n    key_env: FAST\n    prefixes: [fast, openai]\n    models: [gpt-a, gpt-b]\nchains:\n  - name: demo\n    steps:\n      - provider: fast\n        model: gpt-a\n",
    ).unwrap();
    let router = app(AppState::new(
        cfg,
        HashMap::from([("FAST".into(), "secret".into())]),
        vec!["client".into()],
    )
    .unwrap());
    let req = |uri: &str| {
        Request::builder()
            .uri(uri)
            .header("authorization", "Bearer client")
            .body(Body::empty())
            .unwrap()
    };
    let (status, list) = call(router.clone(), req("/v1/models")).await;
    assert_eq!(status, StatusCode::OK);
    let data = list["data"].as_array().unwrap();
    let ids: Vec<_> = data
        .iter()
        .map(|model| model["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        vec![
            "demo",
            "fast/gpt-a",
            "fast/gpt-b",
            "openai/gpt-a",
            "openai/gpt-b"
        ]
    );
    let (status, detail) = call(router, req("/v1/models/openai%2Fgpt-b")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["owned_by"], "fast");
}

#[tokio::test]
async fn responses_text_bridge_and_unsupported_modes() {
    let (url, hits) = start_upstream(StatusCode::OK).await;
    let router = make_app(&url, &url);
    let req = |payload: Value| {
        Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("authorization", "Bearer client")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap()
    };
    let (status, value) = call(router.clone(), req(json!({"model":"demo","input":"hello"}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["object"], "response");
    assert_eq!(value["status"], "completed");
    assert_eq!(value["output"][0]["status"], "completed");
    assert!(value["incomplete_details"].is_null());
    assert_eq!(value["output"][0]["content"][0]["text"], "success");
    assert_eq!(value["output_text"], "success");
    assert_eq!(value["model"], "demo");
    assert!(value["usage"].is_null());
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    for mode in [
        "nochoices",
        "empty",
        "multiple",
        "wrongindex",
        "wrongrole",
        "noid",
        "emptyid",
        "negativecreated",
        "stringcreated",
        "emptymodel",
        "numbermodel",
        "emptytoolcalls",
        "missingtoolcalls",
        "wrongtoolcalls",
        "nulltoolcalls",
    ] {
        let (status, rejected) = call(
            router.clone(),
            req(json!({"model":"demo","input":format!("invalidchat:{mode}")})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{mode}");
        assert!(
            matches!(
                rejected["error"]["code"].as_str(),
                Some(
                    "invalid_chat_choices"
                        | "invalid_chat_id"
                        | "invalid_chat_created"
                        | "invalid_chat_model"
                        | "missing_tool_calls"
                        | "invalid_tool_calls"
                        | "upstream_unavailable"
                )
            ),
            "{mode}: {rejected}"
        );
    }
    // Responses-only validation now runs inside the candidate walker, so all
    // malformed completions can try the healthy fallback before giving up.
    assert_eq!(hits.load(Ordering::SeqCst), 32);
    let (status, converted) = call(router.clone(), req(json!({"model":"demo","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hel"},{"type":"input_text","text":"lo"}]}]}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(converted["output_text"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 33);
    let (status, instructed) = call(
        router.clone(),
        req(json!({"model":"demo","instructions":"Answer briefly","input":"hello"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(instructed["output_text"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 34);
    let (status, truncated) = call(
        router.clone(),
        req(json!({"model":"demo","input":"truncate"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(truncated["status"], "incomplete");
    assert_eq!(truncated["output"][0]["status"], "incomplete");
    assert_eq!(
        truncated["incomplete_details"]["reason"],
        "max_output_tokens"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 35);
    let (status, usage) = call(router.clone(), req(json!({"model":"demo","input":"usage"}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        usage["usage"],
        json!({"input_tokens":5,"output_tokens":3,"total_tokens":8})
    );
    let (status, invalid) = call(
        router.clone(),
        req(json!({"model":"demo","input":"badusage"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(invalid["error"]["code"], "upstream_unavailable");
    assert_eq!(hits.load(Ordering::SeqCst), 38);
    let (status, tool_result) = call(router.clone(), req(json!({"model":"demo","input":"call","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],"tool_choice":"auto"}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tool_result["output"][0]["type"], "function_call");
    assert_eq!(tool_result["output"][0]["name"], "lookup");
    assert_eq!(tool_result["output"][0]["call_id"], "call_123");
    assert_eq!(hits.load(Ordering::SeqCst), 39);
    let (status, call_usage) = call(router.clone(), req(json!({"model":"demo","input":"callusage","tools":[{"type":"function","name":"lookup"}]}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        call_usage["usage"],
        json!({"input_tokens":5,"output_tokens":3,"total_tokens":8})
    );
    assert!(call_usage["incomplete_details"].is_null());
    assert_eq!(hits.load(Ordering::SeqCst), 40);
    let (status, mixed) = call(router.clone(), req(json!({"model":"demo","input":"callmixed","tools":[{"type":"function","name":"lookup"}],"parallel_tool_calls":true}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(mixed["output_text"], "I will look that up");
    assert_eq!(mixed["output"].as_array().unwrap().len(), 3);
    assert_eq!(mixed["output"][0]["type"], "message");
    assert_eq!(mixed["output"][1]["call_id"], "call_1");
    assert_eq!(mixed["output"][2]["call_id"], "call_2");
    assert_eq!(hits.load(Ordering::SeqCst), 41);
    let (status, duplicate) = call(
        router.clone(),
        req(
            json!({"model":"demo","input":"calldup","tools":[{"type":"function","name":"lookup"}]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(duplicate["error"]["code"], "upstream_unavailable");
    assert_eq!(hits.load(Ordering::SeqCst), 43);
    let (status, continued) = call(router.clone(), req(json!({"model":"demo","input":[{"type":"function_call","call_id":"call_123","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"call_123","output":"Paris is in France"}]}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(continued["output_text"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 44);
    let (status, _) = call(router.clone(), req(json!({"model":"demo","input":[{"type":"function_call_output","call_id":"call_123","output":7}]}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(hits.load(Ordering::SeqCst), 44);
    let (status, parallel_history) = call(
        router.clone(),
        req(json!({"model":"demo","input":[
            {"role":"user","content":"parallelcheck"},
            {"type":"function_call","call_id":"p1","name":"lookup","arguments":"{}"},
            {"type":"function_call","call_id":"p2","name":"lookup","arguments":"{}"},
            {"type":"function_call_output","call_id":"p2","output":"second"},
            {"type":"function_call_output","call_id":"p1","output":"first"}
        ]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(parallel_history["output_text"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 45);
    let (status, assistant_text) = call(router.clone(), req(json!({"model":"demo","input":[{"role":"assistant","content":[{"type":"output_text","text":"previous response"}]},{"role":"user","content":"follow up"}]}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(assistant_text["output_text"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 46);
    for invalid_input in [
        json!([{"type":"function_call","call_id":"x","name":"lookup","arguments":"[]"},{"type":"function_call_output","call_id":"x","output":"ok"}]),
        json!([{"type":"function_call","call_id":"x","name":"lookup","arguments":"null"},{"type":"function_call_output","call_id":"x","output":"ok"}]),
        json!([{"type":"function_call","call_id":"x","name":"lookup","arguments":"{}","status":"in_progress"},{"type":"function_call_output","call_id":"x","output":"ok"}]),
        json!([{"type":"function_call","call_id":"x","name":"lookup","arguments":"{}","status":false},{"type":"function_call_output","call_id":"x","output":"ok"}]),
        json!([{"type":"function_call","call_id":"x","name":"lookup","arguments":"{}","id":12},{"type":"function_call_output","call_id":"x","output":"ok"}]),
        json!([{"type":"function_call","call_id":"x","name":"lookup","arguments":"{}","id":""},{"type":"function_call_output","call_id":"x","output":"ok"}]),
        json!([{"role":"user","content":[{"type":"output_text","text":"wrong role"}]}]),
        json!([{"type":"function_call_output","call_id":"orphan","output":"x"}]),
        json!([{"type":"function_call","call_id":"dup","name":"lookup","arguments":"{}"},{"type":"function_call","call_id":"dup","name":"lookup","arguments":"{}"}]),
        json!([{"type":"function_call","call_id":"missing","name":"lookup","arguments":"{}"}]),
        json!([{"type":"function_call","call_id":"id1","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"id2","output":"x"}]),
    ] {
        let (status, _) = call(
            router.clone(),
            req(json!({"model":"demo","input":invalid_input})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    assert_eq!(hits.load(Ordering::SeqCst), 46);
    for bad_request in [
        json!({"model":"demo","input":"hello","tool_choice":"required"}),
        json!({"model":"demo","input":"hello","tool_choice":{"type":"function","name":"lookup"}}),
        json!({"model":"demo","input":"hello","temperature":-0.1}),
        json!({"model":"demo","input":"hello","temperature":2.1}),
        json!({"model":"demo","input":"hello","top_p":-0.01}),
        json!({"model":"demo","input":"hello","top_p":1.1}),
        json!({"model":"demo","input":"hello","top_p":"0.9"}),
        json!({"model":"demo","input":"hello","max_output_tokens":0}),
        json!({"model":"demo","input":"hello","max_output_tokens":-1}),
        json!({"model":"demo","input":"hello","max_output_tokens":2.5}),
        json!({"model":"demo","input":"hello","user":true}),
        json!({"model":"demo","input":"hello","instructions":[]}),
        json!({"model":"demo","input":"hello","tools":[{"type":"function","name":"lookup"},{"type":"function","name":"lookup"}]}),
        json!({"model":"demo","input":"hello","tools":[{"type":"function","name":"lookup","parameters":7}]}),
        json!({"model":"demo","input":"hello","parallel_tool_calls":1}),
        json!({"model":"demo","input":"hello","tools":[{"type":"function","name":"lookup"}],"tool_choice":{"type":"function","name":"missing"}}),
        json!({"model":"demo","input":[{"type":"function_call","call_id":"dup","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"dup","output":"x"},{"type":"function_call","call_id":"dup","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"dup","output":"y"}]}),
        json!({"model":"demo","input":[{"type":"function_call","call_id":"id1","name":"lookup","arguments":"{}"},{"role":"user","content":"premature"},{"type":"function_call_output","call_id":"id1","output":"x"}]}),
    ] {
        let (status, _) = call(router.clone(), req(bad_request)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    assert_eq!(hits.load(Ordering::SeqCst), 46);
    for payload in [
        json!({"model":"demo","input":"hello","tools":[{"type":"web_search"}]}),
        json!({"model":"demo","input":"hello","n":2}),
        json!({"model":"demo","input":"hello","unknown_option":42}),
        json!({"model":"demo","input":"hello","messages":[]}),
        json!({"model":"demo","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.org/img.png"}]}]}),
        json!({"model":"demo","input":[{"role":"user","content":"hi","unknown":1}]}),
    ] {
        let (status, _) = call(router.clone(), req(payload)).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    }
    let (status, _) = call(
        router.clone(),
        req(json!({"model":"demo","input":"hello","instructions":[]})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = call(router.clone(), req(json!({"model":"demo"}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = call(router, req(json!({"input":"hello"}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(hits.load(Ordering::SeqCst), 46);
}

#[tokio::test]
async fn responses_reject_malformed_function_names_before_provider_dispatch() {
    let (url, hits) = start_upstream(StatusCode::OK).await;
    let router = make_app(&url, &url);
    let invalid_names = vec![
        "bad+name".into(),
        "bad name".into(),
        "bad.name".into(),
        "bad/name".into(),
        "bad\\nname".into(),
        "a".repeat(65),
    ];
    for name in &invalid_names {
        let payload = json!({
            "model": "demo", "input": "hello",
            "tools": [{"type": "function", "name": name}]
        });
        let (status, output) = call(router.clone(), responses_stream_req(payload)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name}: {output}");
        assert_eq!(output["error"]["code"], "invalid_function_name", "{name}");
    }
    for name in ["bad name".to_owned(), "bad.name".to_owned(), "a".repeat(65)] {
        let payload = json!({
            "model": "demo",
            "input": [
                {"type":"function_call", "call_id":"call_1", "name":name, "arguments":"{}"},
                {"type":"function_call_output", "call_id":"call_1", "output":"done"}
            ]
        });
        let (status, output) = call(router.clone(), responses_stream_req(payload)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name}: {output}");
        assert_eq!(output["error"]["code"], "invalid_function_name", "{name}");
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "invalid tool names must never reach providers"
    );
    for name in [
        "lookup".to_owned(),
        "find_data-v2".to_owned(),
        "A1".to_owned(),
        "a".repeat(64),
    ] {
        let payload = json!({
            "model":"demo", "input":"hello",
            "tools":[{"type":"function", "name":name}]
        });
        let (status, _) = call(router.clone(), responses_stream_req(payload)).await;
        assert_eq!(status, StatusCode::OK, "valid function name {name}");
    }
    assert_eq!(hits.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn responses_stateless_metadata_and_mode_contract() {
    let (url, hits) = start_upstream(StatusCode::OK).await;
    let router = make_app(&url, &url);
    let req = |payload: Value| {
        Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("authorization", "Bearer client")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap()
    };
    let (status, result) = call(router.clone(), req(json!({"model":"demo","input":"hello","metadata":{"job":"abc","tag":"small"},"store":false,"background":false,"truncation":"disabled"}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["metadata"], json!({"job":"abc","tag":"small"}));
    assert_eq!(result["output_text"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let (status, tool) = call(router.clone(), req(json!({"model":"demo","input":"call","metadata":{"job":"tools"},"store":false,"tools":[{"type":"function","name":"lookup"}]}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tool["metadata"]["job"], "tools");
    assert_eq!(tool["output"][0]["type"], "function_call");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    for payload in [
        json!({"store":true}),
        json!({"background":true}),
        json!({"truncation":"auto"}),
        json!({"metadata":[]}),
        json!({"metadata":{"a":12}}),
        json!({"metadata":serde_json::Map::from_iter((0..17).map(|n| (format!("key{n}"),json!("value"))))}),
        json!({"metadata":{"":"x"}}),
        json!({"metadata":{"x":"a".repeat(513)}}),
        json!({"metadata":{("x".repeat(65)):"a"}}),
    ] {
        let mut request = json!({"model":"demo","input":"hello"});
        request
            .as_object_mut()
            .unwrap()
            .extend(payload.as_object().unwrap().clone());
        let (status, result) = call(router.clone(), req(request)).await;
        assert!(
            matches!(
                status,
                StatusCode::BAD_REQUEST | StatusCode::NOT_IMPLEMENTED
            ),
            "{result}"
        );
    }
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

fn responses_stream_req(payload: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("authorization", "Bearer client")
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap()
}

const RESP_START_A: &[u8] = b"data: {\"id\":\"chat1\",\"created\":123,\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",";
const RESP_START_B: &[u8] = b"\"content\":\"hi\"},\"finish_reason\":null}]}\n\n";
const RESP_END: &[u8] = b"data: {\"id\":\"chat1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"!\"},\"finish_reason\":\"stop\"}]}\n\n";
const RESP_END_WITH_INLINE_USAGE: &[u8] = b"data: {\"id\":\"chat1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"!\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2,\"total_tokens\":6}}\n\n";
const RESP_LENGTH_END: &[u8] = b"data: {\"id\":\"chat1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"!\"},\"finish_reason\":\"length\"}]}\n\n";
const RESP_FILTER_END: &[u8] = b"data: {\"id\":\"chat1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"content_filter\"}]}\n\n";
const RESP_USAGE: &[u8] = b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}\n\n";

#[tokio::test]
async fn inline_usage_plus_final_trailer_succeeds_for_chat_and_responses() {
    let (url, upstream_hits) = start_sse(vec![
        RESP_START_A,
        RESP_START_B,
        RESP_END_WITH_INLINE_USAGE,
        RESP_USAGE,
        SSE_DONE,
    ])
    .await;
    let router = make_app(&url, &url);
    let chat_response = router
        .clone()
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(chat_response.status(), StatusCode::OK);
    let chat_text = String::from_utf8(
        to_bytes(chat_response.into_body(), 4 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(chat_text.contains("data: [DONE]"), "{chat_text}");
    let response = router
        .oneshot(responses_stream_req(json!({
            "model":"demo","input":"hello","stream":true
        })))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), 4 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(
        body.matches("event: response.completed").count(),
        1,
        "{body}"
    );
    assert!(!body.contains("event: response.failed"), "{body}");
    let completed: Value = serde_json::from_str(
        body.lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .next_back()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(completed["response"]["usage"]["total_tokens"], 7);
    assert_eq!(completed["response"]["output_text"], "hi!");
    assert_eq!(upstream_hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn responses_sse_real_upstream_fragmentation_metadata_usage() {
    let (url, hits) = start_sse(vec![
        RESP_START_A,
        RESP_START_B,
        RESP_END,
        RESP_USAGE,
        SSE_DONE,
    ])
    .await;
    let response = make_app(&url, &url)
        .oneshot(responses_stream_req(json!({"model":"demo","input":"hello",
            "stream":true,"metadata":{"job":"sse"},"store":false,"max_output_tokens":32})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let text = String::from_utf8(
        to_bytes(response.into_body(), 4 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(text.contains("event: response.created"), "{text}");
    assert!(text.contains("event: response.in_progress"), "{text}");
    assert!(text.contains("event: response.output_item.added"), "{text}");
    assert_eq!(text.matches("event: response.output_text.delta").count(), 2);
    assert_eq!(text.matches("event: response.completed").count(), 1);
    assert!(!text.contains("event: response.failed"), "{text}");
    assert!(!text.contains("data: [DONE]"));
    let events: Vec<Value> = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|json| serde_json::from_str(json).unwrap())
        .collect();
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event["sequence_number"], json!(index), "{event}");
    }
    let completed = events.last().unwrap();
    assert_eq!(completed["response"]["status"], "completed");
    assert_eq!(completed["response"]["model"], "demo");
    assert_eq!(completed["response"]["output_text"], "hi!");
    assert_eq!(completed["response"]["metadata"]["job"], "sse");
    assert_eq!(completed["response"]["usage"]["total_tokens"], 7);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn responses_sse_content_filter_preserves_incomplete_reason() {
    let (url, hits) = start_sse(vec![RESP_START_A, RESP_START_B, RESP_FILTER_END, SSE_DONE]).await;
    let response = make_app(&url, &url)
        .oneshot(responses_stream_req(
            json!({"model":"demo","input":"hello","stream":true}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = String::from_utf8(
        to_bytes(response.into_body(), 4 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(
        text.matches("event: response.incomplete").count(),
        1,
        "{text}"
    );
    assert!(text.contains("\"reason\":\"content_filter\""), "{text}");
    assert!(!text.contains("event: response.failed"), "{text}");
    assert!(!text.contains("event: response.completed"), "{text}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn responses_sse_length_incomplete_and_truncated_failed() {
    let (url, hits) = start_sse(vec![RESP_START_A, RESP_START_B, RESP_LENGTH_END, SSE_DONE]).await;
    let response = make_app(&url, &url)
        .oneshot(responses_stream_req(
            json!({"model":"demo","input":"hello","stream":true}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = String::from_utf8(
        to_bytes(response.into_body(), 4 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(text.contains("event: response.incomplete"), "{text}");
    assert!(text.contains("max_output_tokens"), "{text}");
    assert!(!text.contains("event: response.completed"), "{text}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    let (url, hits) = start_sse(vec![RESP_START_A, RESP_START_B]).await;
    let response = make_app(&url, &url)
        .oneshot(responses_stream_req(
            json!({"model":"demo","input":"hello","stream":true}),
        ))
        .await
        .unwrap();
    let text = String::from_utf8(
        to_bytes(response.into_body(), 4 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(text.matches("event: response.failed").count(), 1, "{text}");
    assert!(!text.contains("event: response.completed"), "{text}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn responses_sse_unsupported_tool_stream_fails_closed() {
    let tool_call = b"data: {\"id\":\"chat1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call1\"}]},\"finish_reason\":null}]}\n\n";
    let (url, hits) = start_sse(vec![RESP_START_A, RESP_START_B, tool_call, SSE_DONE]).await;
    let response = make_app(&url, &url)
        .oneshot(responses_stream_req(
            json!({"model":"demo","input":"hello","stream":true,
            "tools":[{"type":"function","name":"lookup"}]}),
        ))
        .await
        .unwrap();
    let text = String::from_utf8(
        to_bytes(response.into_body(), 4 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(text.matches("event: response.failed").count(), 1, "{text}");
    assert!(!text.contains("event: response.completed"), "{text}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn responses_sse_multiple_concurrent_clients_complete_without_mixups() {
    let (url, hits) = start_sse(vec![RESP_START_A, RESP_START_B, RESP_END, SSE_DONE]).await;
    let app = make_app(&url, &url);
    let tasks = (0..12).map(|index| {
        let app = app.clone();
        async move {
            let resp = app.oneshot(responses_stream_req(json!({
                "model":"demo","input":"hello","stream":true,"metadata":{"request":format!("{index}")}
            }))).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let text = String::from_utf8(to_bytes(resp.into_body(), 4 << 20).await.unwrap().to_vec()).unwrap();
            assert!(text.contains(&format!("\"request\":\"{index}\"")));
            assert_eq!(text.matches("event: response.completed").count(), 1);
            assert!(!text.contains("event: response.failed"));
        }
    });
    futures_util::future::join_all(tasks).await;
    assert_eq!(hits.load(Ordering::SeqCst), 12);
}

const TOOL_FIRST: &[u8] = b"data: {\"id\":\"chat1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"city\\\":\"}}]},\"finish_reason\":null}]}\n\n";
const TOOL_MORE: &[u8] = b"data: {\"id\":\"chat1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"Paris\\\"}\"}}]},\"finish_reason\":null}]}\n\n";
const TOOL_SECOND: &[u8] = b"data: {\"id\":\"chat1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"id\":\"call_2\",\"type\":\"function\",\"function\":{\"name\":\"weather\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n";
const TOOL_FINISH: &[u8] = b"data: {\"id\":\"chat1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n";
const TOOL_BAD_FINISH: &[u8] = b"data: {\"id\":\"chat1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";

#[tokio::test]
async fn responses_sse_function_calls_and_mixed_text_work() {
    let (url, hits) = start_sse(vec![
        RESP_START_A,
        RESP_START_B,
        TOOL_FIRST,
        TOOL_MORE,
        TOOL_SECOND,
        TOOL_FINISH,
        SSE_DONE,
    ])
    .await;
    let response = make_app(&url, &url)
        .oneshot(responses_stream_req(
            json!({"model":"demo","input":"hello","stream":true,
            "tools":[{"type":"function","name":"lookup"},{"type":"function","name":"weather"}],
            "parallel_tool_calls":true}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = String::from_utf8(
        to_bytes(response.into_body(), 4 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(
        text.matches("event: response.output_item.added").count(),
        3,
        "{text}"
    );
    assert_eq!(
        text.matches("event: response.function_call_arguments.delta")
            .count(),
        3,
        "{text}"
    );
    assert_eq!(
        text.matches("event: response.function_call_arguments.done")
            .count(),
        2,
        "{text}"
    );
    assert_eq!(
        text.matches("event: response.completed").count(),
        1,
        "{text}"
    );
    let response_event: Value = serde_json::from_str(
        text.lines()
            .rev()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap(),
    )
    .unwrap();
    let result = &response_event["response"];
    assert_eq!(result["output_text"], "hi");
    assert_eq!(result["output"].as_array().unwrap().len(), 3);
    assert_eq!(result["output"][0]["type"], "message");
    assert_eq!(result["output"][1]["type"], "function_call");
    assert_eq!(result["output"][1]["call_id"], "call_1");
    assert_eq!(result["output"][1]["arguments"], r#"{"city":"Paris"}"#);
    assert_eq!(result["output"][2]["name"], "weather");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn responses_sse_tool_only_and_invalid_tool_finish() {
    let (url, hits) = start_sse(vec![TOOL_FIRST, TOOL_MORE, TOOL_FINISH, SSE_DONE]).await;
    let response = make_app(&url, &url)
        .oneshot(responses_stream_req(
            json!({"model":"demo","input":"call","stream":true,
            "tools":[{"type":"function","name":"lookup"}]}),
        ))
        .await
        .unwrap();
    let text = String::from_utf8(
        to_bytes(response.into_body(), 4 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(!text.contains("event: response.output_text.done"), "{text}");
    assert_eq!(
        text.matches("event: response.completed").count(),
        1,
        "{text}"
    );
    let final_event: Value = serde_json::from_str(
        text.lines()
            .rev()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        final_event["response"]["output"][0]["type"],
        "function_call"
    );
    assert_eq!(
        final_event["response"]["output"][0]["arguments"],
        r#"{"city":"Paris"}"#
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    let (url, hits) = start_sse(vec![TOOL_FIRST, TOOL_MORE, TOOL_BAD_FINISH, SSE_DONE]).await;
    let response = make_app(&url, &url)
        .oneshot(responses_stream_req(
            json!({"model":"demo","input":"call","stream":true}),
        ))
        .await
        .unwrap();
    let text = String::from_utf8(
        to_bytes(response.into_body(), 4 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(text.matches("event: response.failed").count(), 1, "{text}");
    assert!(!text.contains("event: response.completed"), "{text}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

const RESP_SINGLE: &[u8] = b"data: {\"id\":\"stream_stalled\",\"created\":123,\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"start\"},\"finish_reason\":null}]}\n\n";

#[tokio::test]
async fn responses_sse_downstream_disconnect_drops_provider_stream() {
    let (url, drops) = start_stalled_sse_frame(RESP_SINGLE).await;
    let response = make_app(&url, &url)
        .oneshot(responses_stream_req(
            json!({"model":"demo","input":"hello","stream":true}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    use http_body_util::BodyExt;
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), body.frame())
        .await
        .expect("response did not start")
        .expect("response body ended")
        .unwrap();
    assert!(
        first
            .into_data()
            .unwrap()
            .starts_with(b"event: response.created")
    );
    drop(body);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnect did not release the upstream stream");
}

#[tokio::test]
async fn responses_sse_stalled_provider_reports_one_failure_not_success() {
    let (url, drops) = start_stalled_sse_frame(RESP_SINGLE).await;
    let response = make_app(&url, &url)
        .oneshot(responses_stream_req(
            json!({"model":"demo","input":"hello","stream":true}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(19),
        to_bytes(response.into_body(), 4 << 20),
    )
    .await
    .expect("stalled source was not timed out")
    .unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert_eq!(text.matches("event: response.failed").count(), 1, "{text}");
    assert!(!text.contains("event: response.completed"), "{text}");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed-out provider body was not dropped");
}

async fn get_metrics(router: Router, bearer: Option<&str>) -> (StatusCode, String) {
    let mut request = Request::builder().method("GET").uri("/metrics");
    if let Some(value) = bearer {
        request = request.header("authorization", format!("Bearer {value}"));
    }
    let response = router
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    if status == StatusCode::OK {
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/plain")
        );
    }
    let bytes = to_bytes(response.into_body(), 16 << 10).await.unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn metric_value(text: &str, name: &str) -> u64 {
    let prefix = format!("guvna_{name} ");
    let found: Vec<_> = text
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix))
        .collect();
    assert_eq!(found.len(), 1, "missing or duplicate {name} metric");
    found[0].parse::<u64>().expect("numeric metric")
}

#[tokio::test]
async fn metrics_auth_privacy_retry_fallback_and_credential_quarantine() {
    let (primary, primary_calls) = start_upstream(StatusCode::SERVICE_UNAVAILABLE).await;
    let (secondary, secondary_calls) = start_upstream(StatusCode::OK).await;
    let router = make_app(&primary, &secondary);
    let (status, _) = get_metrics(router.clone(), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = get_metrics(router.clone(), Some("invalid")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, before) = get_metrics(router.clone(), Some("client")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(metric_value(&before, "upstream_attempts_total"), 0);
    assert_eq!(metric_value(&before, "active_requests"), 0);
    assert!(!before.contains("first"));
    assert!(!before.contains("good"));
    assert!(!before.contains("model"));
    assert!(!before.contains("env"));

    let (status, response) = call(router.clone(), request("demo", "", "client")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["choices"][0]["message"]["content"], "success");
    assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
    assert_eq!(secondary_calls.load(Ordering::SeqCst), 2);

    let (status, after) = get_metrics(router.clone(), Some("client")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(metric_value(&after, "requests_dispatched_total"), 1);
    assert_eq!(metric_value(&after, "upstream_attempts_total"), 3);
    assert_eq!(metric_value(&after, "upstream_http_failures_total"), 2);
    assert_eq!(metric_value(&after, "credential_quarantines_total"), 1);
    assert_eq!(metric_value(&after, "candidate_fallbacks_total"), 1);
    assert_eq!(metric_value(&after, "upstream_success_total"), 1);
    assert_eq!(metric_value(&after, "active_requests"), 0);
    assert_eq!(metric_value(&after, "upstream_invalid_responses_total"), 0);
    assert!(!after.contains("secret"));
    assert!(!after.contains("BAD"));
    assert_eq!(after.matches("# TYPE guvna_").count(), 30);
    assert_eq!(metric_value(&after, "aggregation_failures_total"), 0);
    assert_eq!(metric_value(&after, "walk_deadline_exhaustions_total"), 0);
    assert_eq!(metric_value(&after, "circuit_open_providers"), 0);
    assert_eq!(metric_value(&after, "circuit_bypasses_total"), 0);
    assert_eq!(metric_value(&after, "circuit_openings_total"), 0);

    let (status, _) = call(router.clone(), request("demo", "", "client")).await;
    assert_eq!(status, StatusCode::OK);
    let (_, later) = get_metrics(router, Some("client")).await;
    assert_eq!(metric_value(&later, "upstream_attempts_total"), 5);
    assert_eq!(metric_value(&later, "credential_quarantines_total"), 1);
    assert_eq!(metric_value(&later, "upstream_success_total"), 2);
}

#[tokio::test]
async fn metrics_stream_success_failure_and_client_disconnect() {
    let (success_url, _) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let app = make_app(&success_url, &success_url);
    let response = app
        .clone()
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let (_, metrics) = get_metrics(app, Some("client")).await;
    assert_eq!(metric_value(&metrics, "chat_stream_completed_total"), 1);
    assert_eq!(metric_value(&metrics, "upstream_success_total"), 1);
    assert_eq!(metric_value(&metrics, "chat_stream_cancelled_total"), 0);
    assert_eq!(metric_value(&metrics, "active_requests"), 0);

    let (truncated_url, _) = start_sse(vec![SSE_START]).await;
    let app = make_app(&truncated_url, &truncated_url);
    let response = app
        .clone()
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let (_, metrics) = get_metrics(app, Some("client")).await;
    assert_eq!(metric_value(&metrics, "chat_stream_failed_total"), 1);
    assert_eq!(metric_value(&metrics, "chat_stream_cancelled_total"), 0);
    assert_eq!(metric_value(&metrics, "chat_stream_completed_total"), 0);
    assert_eq!(metric_value(&metrics, "active_requests"), 0);

    let (stalled_url, drops) = start_stalled_sse().await;
    let app = make_app(&stalled_url, &stalled_url);
    let response = app
        .clone()
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    let mut body = response.into_body();
    use http_body_util::BodyExt;
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(first.into_data().unwrap().starts_with(b"data: "));
    let (_, snapshot) = get_metrics(app.clone(), Some("client")).await;
    assert_eq!(metric_value(&snapshot, "active_requests"), 1);
    drop(body);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (_, metrics) = get_metrics(app, Some("client")).await;
    assert_eq!(metric_value(&metrics, "chat_stream_cancelled_total"), 1);
    assert_eq!(metric_value(&metrics, "chat_stream_completed_total"), 0);
    assert_eq!(metric_value(&metrics, "active_requests"), 0);
}

#[tokio::test]
async fn cancelled_chat_sse_persists_actual_terminal_with_same_trace_and_no_provider_failure() {
    use guvna::gateway::app_with_state;
    use http_body_util::BodyExt;
    use rusqlite::Connection;
    let dir = tempfile::tempdir().unwrap();
    let (url, drops) = start_stalled_sse().await;
    let config: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: p\n    type: openai\n    base_url: {url}\n    key_env: K\n\
         chains:\n  - name: demo\n    steps:\n      - provider: p\n        model: m\n"
    ))
    .unwrap();
    let mut state = AppState::new(
        config,
        HashMap::from([("K".into(), "secret".into())]),
        vec!["client".into()],
    )
    .unwrap();
    state.enable_telemetry(dir.path()).unwrap();
    let (router, shared) = app_with_state(state);
    let response = router
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(first.into_data().unwrap().starts_with(b"data: "));
    drop(body);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(shared.flush_telemetry(std::time::Duration::from_secs(3)));
    let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
    let (trace, attempt): (String, i64) = db.query_row(
        "SELECT request_id,attempt_index FROM request_events WHERE failure_type='stream_started'",
        [], |row| Ok((row.get(0)?,row.get(1)?)),
    ).unwrap();
    let (terminal_trace, terminal_attempt, outcome): (String, i64, String) = db
        .query_row(
            "SELECT request_id,attempt_index,outcome FROM stream_terminals",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((trace, attempt), (terminal_trace, terminal_attempt));
    assert_eq!(outcome, "client_cancelled");
    let failures: i64 = db
        .query_row("SELECT COUNT(*) FROM failure_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(failures, 0);
    let provider_health: i64 = db
        .query_row("SELECT COUNT(*) FROM provider_health", [], |row| row.get(0))
        .unwrap();
    assert_eq!(provider_health, 0);
}

#[tokio::test]
async fn metrics_responses_stream_terminal_outcomes() {
    let (url, _) = start_sse(vec![RESP_START_A, RESP_START_B, RESP_END, SSE_DONE]).await;
    let app = make_app(&url, &url);
    let response = app
        .clone()
        .oneshot(responses_stream_req(json!({
            "model":"demo","input":"hi","stream":true
        })))
        .await
        .unwrap();
    to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let (_, metrics) = get_metrics(app, Some("client")).await;
    assert_eq!(
        metric_value(&metrics, "responses_stream_completed_total"),
        1
    );
    assert_eq!(metric_value(&metrics, "responses_stream_failed_total"), 0);
    assert_eq!(
        metric_value(&metrics, "responses_stream_cancelled_total"),
        0
    );

    let (url, _) = start_sse(vec![RESP_START_A, RESP_START_B, RESP_LENGTH_END, SSE_DONE]).await;
    let app = make_app(&url, &url);
    let response = app
        .clone()
        .oneshot(responses_stream_req(json!({
            "model":"demo","input":"hi","stream":true
        })))
        .await
        .unwrap();
    to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let (_, metrics) = get_metrics(app, Some("client")).await;
    assert_eq!(
        metric_value(&metrics, "responses_stream_incomplete_total"),
        1
    );
    assert_eq!(
        metric_value(&metrics, "responses_stream_completed_total"),
        0
    );

    let (url, _) = start_sse(vec![RESP_START_A, RESP_START_B]).await;
    let app = make_app(&url, &url);
    let response = app
        .clone()
        .oneshot(responses_stream_req(json!({
            "model":"demo","input":"hi","stream":true
        })))
        .await
        .unwrap();
    to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let (_, metrics) = get_metrics(app, Some("client")).await;
    assert_eq!(metric_value(&metrics, "responses_stream_failed_total"), 1);
    assert_eq!(
        metric_value(&metrics, "responses_stream_cancelled_total"),
        0
    );
    assert_eq!(
        metric_value(&metrics, "responses_stream_completed_total"),
        0
    );

    let (url, drops) = start_stalled_sse_frame(RESP_SINGLE).await;
    let app = make_app(&url, &url);
    let response = app
        .clone()
        .oneshot(responses_stream_req(json!({
            "model":"demo","input":"hi","stream":true
        })))
        .await
        .unwrap();
    let mut body = response.into_body();
    use http_body_util::BodyExt;
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(first.into_data().unwrap().starts_with(b"event: "));
    let (_, snapshot) = get_metrics(app.clone(), Some("client")).await;
    assert_eq!(metric_value(&snapshot, "active_requests"), 1);
    drop(body);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (_, metrics) = get_metrics(app, Some("client")).await;
    assert_eq!(
        metric_value(&metrics, "responses_stream_cancelled_total"),
        1
    );
    assert_eq!(
        metric_value(&metrics, "responses_stream_completed_total"),
        0
    );
    assert_eq!(metric_value(&metrics, "active_requests"), 0);
}

#[derive(Clone)]
struct ChangingProvider {
    hits: Arc<AtomicUsize>,
    healthy: Arc<std::sync::atomic::AtomicBool>,
}

async fn changing_provider(State(state): State<ChangingProvider>) -> impl IntoResponse {
    state.hits.fetch_add(1, Ordering::SeqCst);
    // Keep the half-open probe active long enough to test concurrent callers.
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    if !state.healthy.load(Ordering::SeqCst) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({"error":"offline"})),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        axum::Json(json!({
            "id":"mock","object":"chat.completion",
            "choices":[{"index":0,"message":{"role":"assistant","content":"recovered"},
                "finish_reason":"stop"}]
        })),
    )
        .into_response()
}

async fn changing_upstream() -> (String, Arc<AtomicUsize>, Arc<std::sync::atomic::AtomicBool>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let router = Router::new()
        .route("/v1/chat/completions", post(changing_provider))
        .with_state(ChangingProvider {
            hits: hits.clone(),
            healthy: healthy.clone(),
        });
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(socket, router).await.unwrap() });
    (format!("http://{addr}"), hits, healthy)
}

#[tokio::test]
async fn circuit_breaker_skips_repeated_failed_upstream_then_single_probe_recovers() {
    let (down, down_hits, online) = changing_upstream().await;
    let (fallback, healthy_hits) = start_upstream(StatusCode::OK).await;
    let app = make_app_with_circuit(&down, &fallback, true);
    for _ in 0..3 {
        let (status, value) = call(app.clone(), request("demo", "", "client")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["choices"][0]["message"]["content"], "success");
    }
    assert_eq!(down_hits.load(Ordering::SeqCst), 3);
    let (_, metrics) = get_metrics(app.clone(), Some("client")).await;
    assert_eq!(metric_value(&metrics, "circuit_openings_total"), 1);
    assert_eq!(metric_value(&metrics, "circuit_open_providers"), 1);
    assert_eq!(metric_value(&metrics, "circuit_bypasses_total"), 0);
    // The provider is open, so multiple requests must bypass it entirely.
    let results = futures_util::future::join_all((0..12).map(|_| {
        let app = app.clone();
        async move { call(app, request("demo", "", "client")).await.0 }
    }))
    .await;
    assert!(results.iter().all(|status| *status == StatusCode::OK));
    assert_eq!(down_hits.load(Ordering::SeqCst), 3);
    let (_, metrics) = get_metrics(app.clone(), Some("client")).await;
    assert_eq!(metric_value(&metrics, "circuit_bypasses_total"), 12);
    assert_eq!(metric_value(&metrics, "upstream_success_total"), 15);
    // One-provider passthrough cannot fall back: fail fast with a Retry-After
    // instead of manufacturing a 502 or making an upstream HTTP call.
    let response = app
        .clone()
        .oneshot(request("broken/direct", "", "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "1");
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(payload["error"]["code"], "provider_circuit_open");
    assert_eq!(down_hits.load(Ordering::SeqCst), 3);

    tokio::time::sleep(std::time::Duration::from_millis(110)).await;
    online.store(true, Ordering::SeqCst);
    // 24 callers race after cooldown. Exactly one may probe the previously
    // failed provider; all others use the healthy fallback.
    let results = futures_util::future::join_all((0..24).map(|_| {
        let app = app.clone();
        async move { call(app, request("demo", "", "client")).await.0 }
    }))
    .await;
    assert!(results.iter().all(|status| *status == StatusCode::OK));
    assert_eq!(down_hits.load(Ordering::SeqCst), 4);
    let (_, metrics) = get_metrics(app.clone(), Some("client")).await;
    assert_eq!(metric_value(&metrics, "circuit_probe_success_total"), 1);
    assert_eq!(metric_value(&metrics, "circuit_probe_failures_total"), 0);
    assert_eq!(metric_value(&metrics, "circuit_open_providers"), 0);
    let (status, recovered) = call(app.clone(), request("demo", "", "client")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(recovered["choices"][0]["message"]["content"], "recovered");
    assert_eq!(down_hits.load(Ordering::SeqCst), 5);
    assert!(healthy_hits.load(Ordering::SeqCst) >= 15);
}

#[tokio::test]
async fn transient_5xx_quarantines_only_the_used_credential_for_configured_duration() {
    let (url, hits) = start_upstream(StatusCode::INTERNAL_SERVER_ERROR).await;
    let config: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: p\n    type: openai\n    base_url: {url}\n    key_env: KEY\n    quarantine:\n      transient: 30s\nchains:\n  - name: demo\n    steps:\n      - provider: p\n        model: provider-model\n"
    ))
    .unwrap();
    let app = app(AppState::new(
        config,
        HashMap::from([("KEY".into(), "secret".into())]),
        vec!["client".into()],
    )
    .unwrap());
    let (status, _) = call(app.clone(), request("demo", "", "client")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let (_, metrics) = get_metrics(app.clone(), Some("client")).await;
    assert_eq!(metric_value(&metrics, "credential_quarantines_total"), 1);

    let (status, _) = call(app, request("demo", "", "client")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "quarantined credentials must not be retried early"
    );
}

#[tokio::test]
async fn client_errors_never_open_provider_circuit_or_quarantine_credential() {
    let (down, down_hits) = start_upstream(StatusCode::BAD_REQUEST).await;
    let (fallback, _) = start_upstream(StatusCode::OK).await;
    let app = make_app_with_circuit(&down, &fallback, true);
    for _ in 0..10 {
        let (status, _) = call(app.clone(), request("demo", "", "client")).await;
        assert_eq!(status, StatusCode::OK);
    }
    assert_eq!(down_hits.load(Ordering::SeqCst), 10);
    let (_, metrics) = get_metrics(app, Some("client")).await;
    assert_eq!(metric_value(&metrics, "circuit_openings_total"), 0);
    assert_eq!(metric_value(&metrics, "circuit_bypasses_total"), 0);
    assert_eq!(metric_value(&metrics, "circuit_open_providers"), 0);
    assert_eq!(metric_value(&metrics, "credential_quarantines_total"), 1); // BAD fallback key only
}

#[tokio::test]
async fn broken_midstream_counts_as_provider_failure_without_replaying_partial_output() {
    let (down, down_hits) = start_sse(vec![SSE_START]).await;
    let (fallback, fallback_hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let app = make_app_with_circuit(&down, &fallback, true);
    for _ in 0..3 {
        let response = app
            .clone()
            .oneshot(request("demo", r#","stream":true"#, "client"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("upstream_stream_incomplete"), "{text}");
        assert!(!text.contains("data: [DONE]\n\n"), "{text}");
    }
    assert_eq!(down_hits.load(Ordering::SeqCst), 3);
    let (_, metrics) = get_metrics(app.clone(), Some("client")).await;
    assert_eq!(metric_value(&metrics, "circuit_openings_total"), 1);
    assert_eq!(metric_value(&metrics, "chat_stream_failed_total"), 3);
    let response = app
        .clone()
        .oneshot(request("demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    assert!(bytes.ends_with(b"data: [DONE]\n\n"));
    assert_eq!(down_hits.load(Ordering::SeqCst), 3);
    assert_eq!(fallback_hits.load(Ordering::SeqCst), 1);
    let (_, metrics) = get_metrics(app, Some("client")).await;
    assert_eq!(metric_value(&metrics, "circuit_bypasses_total"), 1);
    assert_eq!(metric_value(&metrics, "chat_stream_completed_total"), 1);
}

#[derive(Clone)]
struct FormatProbeState {
    seen: Arc<tokio::sync::Mutex<Vec<Value>>>,
    output: &'static str,
}

async fn format_probe(
    State(state): State<FormatProbeState>,
    request: Request<Body>,
) -> axum::response::Response {
    let bytes = to_bytes(request.into_body(), 32 << 20).await.unwrap();
    let payload: Value = serde_json::from_slice(&bytes).unwrap();
    state.seen.lock().await.push(payload.clone());
    if payload["stream"] == true {
        let stream_content = json!({
            "id":"format_probe_1",
            "choices":[{"index":0,"delta":{"role":"assistant","content":state.output},
                "finish_reason":null}]
        });
        let stream_finish = json!({
            "id":"format_probe_1",
            "choices":[{"index":0,"delta":{},"finish_reason":"stop"}]
        });
        return (
            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
            Body::from(format!(
                "data: {stream_content}\n\ndata: {stream_finish}\n\ndata: [DONE]\n\n"
            )),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        axum::Json(json!({
            "id":"format_probe_1",
            "object":"chat.completion",
            "choices":[{"index":0,"message":{"role":"assistant","content":state.output},
                "finish_reason":"stop"}],
            "usage":null,
        })),
    )
        .into_response()
}

async fn start_format_probe(output: &'static str) -> (String, Arc<tokio::sync::Mutex<Vec<Value>>>) {
    let seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let state = FormatProbeState {
        seen: seen.clone(),
        output,
    };
    let app = Router::new()
        .route("/v1/chat/completions", post(format_probe))
        .with_state(state);
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    (format!("http://{addr}"), seen)
}

#[tokio::test]
async fn responses_text_format_maps_all_supported_types_to_real_upstream_payloads() {
    let (url, seen) = start_format_probe(r#"{"ok":true}"#).await;
    let app = make_app(&url, &url);
    let schema = json!({
        "type":"json_schema",
        "name":"good_result",
        "schema":{"type":"object",
            "properties":{"large":{"const":12345678901234567890u64}},
            "required":["large"],"additionalProperties":false},
        "strict":true,
        "description":"Format data"
    });
    let cases = [
        (json!({}), Value::Null),
        (json!({"text":{}}), Value::Null),
        (
            json!({"text":{"format":{"type":"text"}}}),
            json!({"type":"text"}),
        ),
        (
            json!({"text":{"format":{"type":"json_object"}}}),
            json!({"type":"json_object"}),
        ),
        (
            json!({"text":{"format":schema.clone()}}),
            json!({"type":"json_schema","json_schema":{
                "name":"good_result",
                "schema":schema["schema"],
                "strict":true,
                "description":"Format data"
            }}),
        ),
    ];
    for (mut extras, expected) in cases {
        extras["model"] = json!("demo");
        extras["input"] = json!("hello");
        let (status, value) = call(app.clone(), responses_stream_req(extras)).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["output_text"], r#"{"ok":true}"#);
        let latest = seen.lock().await.last().unwrap().clone();
        assert_eq!(latest["response_format"], expected);
        assert!(
            latest.get("text").is_none(),
            "Responses text leaked: {latest}"
        );
        assert_eq!(latest["model"], "one");
        assert_eq!(latest["messages"][0]["content"], "hello");
    }
    assert_eq!(seen.lock().await.len(), 5);
}

#[tokio::test]
async fn responses_json_schema_and_tools_are_combined_without_loss_or_duplicate_routing() {
    let (url, seen) = start_format_probe(r#"{"ok":true}"#).await;
    let app = make_app(&url, &url);
    let schema = json!({"type":"json_schema","name":"result_7",
        "schema":{"type":"object","properties":{"ok":{"type":"boolean"}}},"strict":true});
    let input = json!({"model":"demo","input":"hello","stream":false,
        "metadata":{"job":"format"},
        "text":{"format":schema},
        "tools":[{"type":"function","name":"lookup",
            "parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}],
        "tool_choice":"auto",
        "max_output_tokens":256});
    let (status, output) = call(app, responses_stream_req(input)).await;
    assert_eq!(status, StatusCode::OK, "{output}");
    assert_eq!(output["metadata"]["job"], "format");
    assert_eq!(output["output_text"], r#"{"ok":true}"#);
    let latest = seen.lock().await.last().unwrap().clone();
    assert_eq!(latest["response_format"]["type"], "json_schema");
    assert_eq!(latest["response_format"]["json_schema"]["name"], "result_7");
    assert_eq!(latest["tools"][0]["function"]["name"], "lookup");
    assert_eq!(
        latest["tools"][0]["function"]["parameters"]["type"],
        "object"
    );
    assert_eq!(latest["tool_choice"], "auto");
    assert_eq!(latest["max_tokens"], 256);
    assert!(latest.get("max_output_tokens").is_none());
    assert!(latest.get("metadata").is_none());
}

#[tokio::test]
async fn responses_forced_tool_choice_ignoring_upstream_fails_over() {
    let (url, seen) = start_format_probe("ordinary text").await;
    let app = make_app(&url, &url);
    for choice in [
        json!("required"),
        json!({"type":"function","name":"lookup"}),
    ] {
        let (status, result) = call(
            app.clone(),
            responses_stream_req(json!({
                "model":"demo","input":"hello",
                "tools":[{"type":"function","name":"lookup"}],
                "tool_choice":choice
            })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{result}");
        assert_eq!(result["error"]["code"], "upstream_unavailable");
    }
    assert!(
        !seen.lock().await.is_empty(),
        "upstream was never exercised"
    );
}

#[tokio::test]
async fn responses_json_text_format_preserves_stream_lifecycle_and_no_false_terminals() {
    let (url, seen) = start_format_probe(r#"{"ok":true}"#).await;
    let app = make_app(&url, &url);
    for format in [
        json!({"type":"json_object"}),
        json!({"type":"json_schema","name":"stream_data",
            "schema":{"type":"object"},"strict":true}),
    ] {
        let response = app
            .clone()
            .oneshot(responses_stream_req(json!({
                "model":"demo","input":"hello","stream":true,
                "text":{"format":format}
            })))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        let bytes = to_bytes(response.into_body(), 2 << 20).await.unwrap();
        let frames = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(
            frames.matches("event: response.completed").count(),
            1,
            "{frames}"
        );
        assert!(!frames.contains("event: response.failed"), "{frames}");
        assert!(!frames.contains("data: [DONE]"), "{frames}");
        let last_event: Value = serde_json::from_str(
            frames
                .lines()
                .filter_map(|l| l.strip_prefix("data: "))
                .next_back()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(last_event["response"]["output_text"], r#"{"ok":true}"#);
        let seen_payload = seen.lock().await.last().unwrap().clone();
        assert_eq!(seen_payload["response_format"]["type"], format["type"]);
        assert_eq!(seen_payload["stream"], true);
        assert!(seen_payload.get("text").is_none());
    }
    assert_eq!(seen.lock().await.len(), 2);
}

#[tokio::test]
async fn unsupported_or_corrupt_format_is_rejected_without_any_provider_calls() {
    let (url, seen) = start_format_probe(r#"{"ok":true}"#).await;
    let app = make_app(&url, &url);
    let cases = [
        (
            json!({"text":null}),
            StatusCode::BAD_REQUEST,
            "invalid_text_format",
        ),
        (
            json!({"text":{"format":null}}),
            StatusCode::BAD_REQUEST,
            "invalid_text_format",
        ),
        (
            json!({"text":{"format":{"type":"json_schema","name":"wrong name","schema":{}}}}),
            StatusCode::BAD_REQUEST,
            "invalid_text_format",
        ),
        (
            json!({"text":{"format":{"type":"json_schema","name":"a","schema":[]}}}),
            StatusCode::BAD_REQUEST,
            "invalid_text_format",
        ),
        (
            json!({"text":{"format":{"type":"json_schema","name":"a","schema":{},"strict":1}}}),
            StatusCode::BAD_REQUEST,
            "invalid_text_format",
        ),
        (
            json!({"text":{"format":{"type":"json_schema","name":"a","schema":{},
            "description":null}}}),
            StatusCode::BAD_REQUEST,
            "invalid_text_format",
        ),
        (
            json!({"text":{"format":{"type":"json_schema","name":"a","schema":{},
            "hidden":"ignored"}}}),
            StatusCode::NOT_IMPLEMENTED,
            "responses_text_format_unsupported",
        ),
        (
            json!({"text":{"format":{"type":"audio"}}}),
            StatusCode::NOT_IMPLEMENTED,
            "responses_text_format_unsupported",
        ),
        (
            json!({"text":{"verbosity":"low"}}),
            StatusCode::NOT_IMPLEMENTED,
            "responses_text_format_unsupported",
        ),
        (
            json!({"text":{"format":{"type":"json_object","undocumented":true}}}),
            StatusCode::NOT_IMPLEMENTED,
            "responses_text_format_unsupported",
        ),
        (
            json!({"response_format":{"type":"json_object"}}),
            StatusCode::NOT_IMPLEMENTED,
            "responses_parameter_unsupported",
        ),
        (
            json!({"reasoning":{"effort":"high"}}),
            StatusCode::NOT_IMPLEMENTED,
            "responses_feature_unsupported",
        ),
    ];
    for (extra, expected_status, expected_code) in cases {
        let mut request = json!({"model":"demo","input":"hello"});
        request
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let (status, output) = call(app.clone(), responses_stream_req(request)).await;
        assert_eq!(status, expected_status, "{output}");
        assert_eq!(output["error"]["code"], expected_code, "{output}");
    }
    assert!(
        seen.lock().await.is_empty(),
        "invalid requests reached upstream"
    );
}

#[tokio::test]
async fn unparseable_json_mode_upstream_200_fails_over_to_valid_json_provider() {
    let (bad_url, bad_seen) = start_format_probe("prose instead of json").await;
    let (good_url, good_seen) = start_format_probe(r#"{"result":12345678901234567890}"#).await;
    let app = make_app(&bad_url, &good_url);
    let (status, result) = call(
        app.clone(),
        responses_stream_req(json!({
            "model":"demo","input":"hello","text":{"format":{"type":"json_object"}}
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["output_text"], r#"{"result":12345678901234567890}"#);
    assert_eq!(bad_seen.lock().await.len(), 1);
    assert_eq!(good_seen.lock().await.len(), 1);
    // When both candidates ignore json_object, no false HTTP 200 is returned.
    let only_bad = make_app(&bad_url, &bad_url);
    let (bad_status, bad_output) = call(
        only_bad,
        responses_stream_req(json!({
            "model":"demo","input":"hello","text":{"format":{"type":"json_object"}}
        })),
    )
    .await;
    assert_eq!(bad_status, StatusCode::BAD_GATEWAY, "{bad_output}");
    assert_eq!(bad_output["error"]["code"], "upstream_unavailable");
    assert_eq!(bad_seen.lock().await.len(), 3);
}

#[tokio::test]
async fn invalid_json_stream_must_never_emit_a_completed_response_or_replay() {
    let (url, seen) = start_format_probe("upstream ignored json output mode").await;
    let app = make_app(&url, &url);
    let response = app
        .clone()
        .oneshot(responses_stream_req(json!({
            "model":"demo","input":"hello","stream":true,
            "text":{"format":{"type":"json_object"}}
        })))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let frames = String::from_utf8(
        to_bytes(response.into_body(), 2 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(
        frames.matches("event: response.failed").count(),
        1,
        "{frames}"
    );
    assert_eq!(
        frames.matches("event: response.completed").count(),
        0,
        "{frames}"
    );
    assert_eq!(
        frames.matches("event: response.output_text.delta").count(),
        1
    );
    assert!(!frames.contains("data: [DONE]"), "{frames}");
    assert_eq!(
        seen.lock().await.len(),
        1,
        "never retry after emitting deltas"
    );
    let (_, metrics) = get_metrics(app, Some("client")).await;
    assert_eq!(metric_value(&metrics, "responses_stream_failed_total"), 1);
    assert_eq!(
        metric_value(&metrics, "responses_stream_completed_total"),
        0
    );
}

fn make_auto_app(broken: &str, healthy: &str) -> Router {
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: broken
    type: openai
    base_url: {broken}
    key_env: FIRST
  - name: healthy
    type: openai
    base_url: {healthy}
    key_env: GOOD
auto:
  enabled: true
  candidates:
    - provider: healthy
      model: fallback-low
      priority: 90
    - provider: broken
      model: fast-short
      priority: 1
      max_input_bytes: 130
    - provider: healthy
      model: long-context
      priority: 2
      min_input_bytes: 131
"#,
    ))
    .unwrap();
    app(AppState::new(
        config,
        HashMap::from([
            ("FIRST".into(), "first".into()),
            ("GOOD".into(), "good".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap())
}

fn auto_request(path: &str, bearer: &str, payload: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("authorization", format!("Bearer {bearer}"))
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap()
}

#[tokio::test]
async fn auto_chat_and_alias_share_fallback_with_priority_and_context_filter() {
    let (failed, failed_calls) = start_upstream(StatusCode::SERVICE_UNAVAILABLE).await;
    let (good, good_calls) = start_upstream(StatusCode::OK).await;
    let router = make_auto_app(&failed, &good);
    let (status, response) = call(
        router.clone(),
        auto_request(
            "/v1/auto",
            "client",
            json!({
                "messages":[{"role":"user","content":"hi"}]
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["model"], "fallback-low");
    assert_eq!(failed_calls.load(Ordering::SeqCst), 1);
    assert_eq!(good_calls.load(Ordering::SeqCst), 1);

    let (status, response) = call(
        router.clone(),
        auto_request(
            "/v1/chat/completions",
            "client",
            json!({
                "model":"auto","messages":[{"role":"user","content":"x".repeat(400)}]
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["model"], "long-context");
    assert_eq!(
        failed_calls.load(Ordering::SeqCst),
        1,
        "long context skips short-only provider"
    );
    assert_eq!(good_calls.load(Ordering::SeqCst), 2);

    let (status, model_list) = call_model_list(router.clone(), "client").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        model_list["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["id"] == "auto")
    );

    for payload in [
        json!({"model":"different","messages":[{"role":"user","content":"hi"}]}),
        json!({"model":123,"messages":[{"role":"user","content":"hi"}]}),
    ] {
        let (status, result) =
            call(router.clone(), auto_request("/v1/auto", "client", payload)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(result["error"]["code"], "invalid_auto_model");
    }
    let (status, _) = call(
        router.clone(),
        auto_request("/v1/auto", "bad", json!({"messages":[]})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(good_calls.load(Ordering::SeqCst), 2);
}

async fn call_model_list(router: Router, key: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("GET")
        .uri("/v1/models")
        .header("authorization", format!("Bearer {key}"))
        .body(Body::empty())
        .unwrap();
    call(router, request).await
}

#[tokio::test]
async fn auto_config_invalid_ranges_and_candidate_references_rejected() {
    let (good, _) = start_upstream(StatusCode::OK).await;
    let yaml = format!(
        r#"
providers:
  - name: healthy
    type: openai
    base_url: {good}
    key_env: GOOD
auto:
  enabled: true
  candidates:
    - provider: healthy
      model: x
      priority: 1
      min_input_bytes: 30
      max_input_bytes: 2
"#
    );
    let config: Config = serde_yaml::from_str(&yaml).unwrap();
    assert!(
        config
            .validate()
            .unwrap_err()
            .contains("input length range")
    );
    let config: Config = serde_yaml::from_str(
        &yaml
            .replace("max_input_bytes: 2", "max_input_bytes: 33")
            .replace(
                "provider: healthy\n      model: x",
                "provider: unknown\n      model: x",
            ),
    )
    .unwrap();
    assert!(config.validate().unwrap_err().contains("missing provider"));
    let config: Config = serde_yaml::from_str(
        &yaml
            .replace("max_input_bytes: 2", "max_input_bytes: 33")
            .replace("min_input_bytes: 30", "min_input_bytes: 0")
            .replace("  enabled: true", "  enabled: false"),
    )
    .unwrap();
    assert!(config.validate().unwrap_err().contains("enabled: true"));
}

/// Mock enforces that Go-only Auto selector fields never reach any provider.
async fn strict_auto_upstream(
    State(calls): State<Arc<AtomicUsize>>,
    axum::Json(request): axum::Json<Value>,
) -> impl IntoResponse {
    calls.fetch_add(1, Ordering::SeqCst);
    assert!(
        request.get("min_context").is_none(),
        "leaked Auto min_context"
    );
    assert!(
        request.get("max_context").is_none(),
        "leaked Auto max_context"
    );
    axum::Json(json!({
        "id":"mock-auto-context",
        "object":"chat.completion",
        "model":request["model"],
        "choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]
    }))
}

async fn start_strict_auto_upstream() -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/v1/chat/completions", post(strict_auto_upstream))
        .with_state(calls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}"), calls)
}

#[tokio::test]
async fn go_auto_context_filters_are_stripped_and_route_cache_isolated() {
    let (small, small_calls) = start_strict_auto_upstream().await;
    let (large, large_calls) = start_strict_auto_upstream().await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: small
    type: openai
    base_url: {small}
    key_env: SMALL_KEY
    models: [short]
  - name: large
    type: openai
    base_url: {large}
    key_env: LARGE_KEY
    models: [long]
auto:
  enabled: true
  candidates:
    - provider: small
      model: short
      max_context_tokens: 4096
    - provider: large
      model: long
      max_context_tokens: 32768
"#
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([
            ("SMALL_KEY".into(), "small-secret".into()),
            ("LARGE_KEY".into(), "large-secret".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap());
    let messages = json!([{"role":"user","content":"hello"}]);
    for (path, context, expected) in [
        ("/v1/auto", json!({"min_context":8192}), "long"),
        ("/v1/chat/completions", json!({"max_context":8192}), "short"),
        (
            "/v1/auto",
            json!({"min_context":0,"max_context":0}),
            "short",
        ),
        ("/v1/chat/completions", json!({"min_context":8192}), "long"),
        ("/v1/auto", json!({"min_context":-1}), "short"),
    ] {
        let mut payload = json!({"model":"auto","messages":messages});
        payload
            .as_object_mut()
            .unwrap()
            .extend(context.as_object().unwrap().clone());
        let (status, result) = call(router.clone(), auto_request(path, "client", payload)).await;
        assert_eq!(status, StatusCode::OK, "{path}: {result}");
        assert_eq!(result["model"], expected, "{path}: {result}");
    }
    assert_eq!(small_calls.load(Ordering::SeqCst), 3);
    assert_eq!(large_calls.load(Ordering::SeqCst), 2);
    for bounds in [
        json!({"min_context":1000,"max_context":500}),
        json!({"min_context":null}),
        json!({"min_context":"8192"}),
        json!({"max_context":1.5}),
        json!({"max_context":true}),
        json!({"min_context":[]}),
    ] {
        let mut payload = json!({"model":"auto","messages":messages});
        payload
            .as_object_mut()
            .unwrap()
            .extend(bounds.as_object().unwrap().clone());
        let (status, result) =
            call(router.clone(), auto_request("/v1/auto", "client", payload)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bounds}: {result}");
        assert_eq!(result["error"]["code"], "invalid_auto_context");
    }
    let (status, result) = call(
        router,
        auto_request(
            "/v1/auto",
            "client",
            json!({"messages":messages,"min_context":65536}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(result["error"]["code"], "auto_no_eligible_candidate");
    assert_eq!(small_calls.load(Ordering::SeqCst), 3);
    assert_eq!(large_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn go_auto_operator_context_floor_can_be_overridden_per_request_without_upstream_leak() {
    let (small, small_calls) = start_strict_auto_upstream().await;
    let (large, large_calls) = start_strict_auto_upstream().await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: small
    type: openai
    base_url: {small}
    key_env: SMALL_KEY
    models: [short]
  - name: large
    type: openai
    base_url: {large}
    key_env: LARGE_KEY
    models: [long]
auto:
  enabled: true
  default_min_context: 8192
  candidates:
    - provider: small
      model: short
      max_context_tokens: 4096
    - provider: large
      model: long
      max_context_tokens: 32768
"#
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([
            ("SMALL_KEY".into(), "small-secret".into()),
            ("LARGE_KEY".into(), "large-secret".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap());
    let messages = json!([{"role":"user","content":"hello"}]);
    for (path, bounds, expected) in [
        ("/v1/auto", json!({}), "long"),
        ("/v1/chat/completions", json!({"min_context":1024}), "short"),
        ("/v1/auto", json!({"min_context":0}), "long"),
        ("/v1/chat/completions", json!({"min_context":-1}), "long"),
    ] {
        let mut request = json!({"model":"auto","messages":messages});
        request
            .as_object_mut()
            .unwrap()
            .extend(bounds.as_object().unwrap().clone());
        let (status, response) = call(router.clone(), auto_request(path, "client", request)).await;
        assert_eq!(status, StatusCode::OK, "{path}: {response}");
        assert_eq!(response["model"], expected, "{path}: {response}");
    }
    for bounds in [
        json!({"max_context":4096}),
        json!({"min_context":8192,"max_context":4096}),
    ] {
        let mut request = json!({"model":"auto","messages":messages});
        request
            .as_object_mut()
            .unwrap()
            .extend(bounds.as_object().unwrap().clone());
        let (status, response) =
            call(router.clone(), auto_request("/v1/auto", "client", request)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bounds}: {response}");
        assert_eq!(response["error"]["code"], "invalid_auto_context");
    }
    assert_eq!(small_calls.load(Ordering::SeqCst), 1);
    assert_eq!(large_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn go_auto_operator_floor_degrades_without_weakening_explicit_bounds_or_capabilities() {
    let (url, hits) = start_strict_auto_upstream().await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: p
    type: openai
    base_url: {url}
    key_env: P_KEY
    models: [short]
auto:
  enabled: true
  default_min_context: 8192
  candidates:
    - provider: p
      model: short
      max_context_tokens: 4096
      supports_images: false
"#
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([("P_KEY".into(), "secret".into())]),
        vec!["client".into()],
    )
    .unwrap());
    for (path, bounds) in [
        ("/v1/auto", json!({})),
        ("/v1/chat/completions", json!({"min_context":0})),
        ("/v1/auto", json!({"max_context":0})),
        ("/v1/auto", json!({"min_context":1024})),
    ] {
        let mut payload = json!({"model":"auto","messages":[{"role":"user","content":"hello"}]});
        payload
            .as_object_mut()
            .unwrap()
            .extend(bounds.as_object().unwrap().clone());
        let (status, result) = call(router.clone(), auto_request(path, "client", payload)).await;
        assert_eq!(status, StatusCode::OK, "{path} {bounds}: {result}");
        assert_eq!(result["model"], "short");
    }
    assert_eq!(hits.load(Ordering::SeqCst), 4);
    for bounds in [
        json!({"min_context":8192}),
        json!({"max_context":8192}),
        json!({"min_context":8192,"max_context":8192}),
    ] {
        let mut payload = json!({"model":"auto","messages":[{"role":"user","content":"hello"}]});
        payload
            .as_object_mut()
            .unwrap()
            .extend(bounds.as_object().unwrap().clone());
        let (status, result) =
            call(router.clone(), auto_request("/v1/auto", "client", payload)).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{bounds}: {result}"
        );
        assert_eq!(result["error"]["code"], "auto_no_eligible_candidate");
    }
    let (status, result) = call(
        router.clone(),
        auto_request(
            "/v1/auto",
            "client",
            json!({
                "model":"auto",
                "messages":[{"role":"user","content":[
                    {"type":"text","text":"hello"},
                    {"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}
                ]}]
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{result}");
    assert_eq!(result["error"]["code"], "auto_no_eligible_candidate");
    assert_eq!(
        hits.load(Ordering::SeqCst),
        4,
        "rejected requests reached upstream"
    );
}

#[tokio::test]
async fn auto_without_eligible_candidate_returns_422_without_network_attempt() {
    let (url, hits) = start_upstream(StatusCode::OK).await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: p
    type: openai
    base_url: {url}
    key_env: FIRST
auto:
  enabled: true
  candidates:
    - provider: p
      model: x
      min_input_bytes: 8192
"#
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([("FIRST".into(), "first".into())]),
        vec!["client".into()],
    )
    .unwrap());
    let (status, body) = call(
        router.clone(),
        auto_request(
            "/v1/auto",
            "client",
            json!({
                "messages":[{"role":"user","content":"short"}]
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], "auto_no_eligible_candidate");
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    let (_, metrics) = get_metrics(router, Some("client")).await;
    assert_eq!(metric_value(&metrics, "upstream_attempts_total"), 0);
}

#[tokio::test]
async fn auto_is_disabled_by_default_and_keeps_old_routing_unchanged() {
    let (url, hits) = start_upstream(StatusCode::OK).await;
    let router = make_app(&url, &url);
    let (status, body) = call(
        router.clone(),
        auto_request(
            "/v1/auto",
            "client",
            json!({"messages":[{"role":"user","content":"hi"}]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "auto_disabled");
    let (status, body) = call(router, request("auto", "", "client")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "unknown_model");
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn auto_stream_reuses_existing_chat_sse_validation() {
    let (good, hits) = start_sse(vec![SSE_START, SSE_END, SSE_DONE]).await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: good
    type: openai
    base_url: {good}
    key_env: GOOD
auto:
  enabled: true
  candidates:
    - provider: good
      model: stream
      priority: 1
"#,
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([("GOOD".into(), "good".into())]),
        vec!["client".into()],
    )
    .unwrap());
    let response = router
        .oneshot(auto_request(
            "/v1/auto",
            "client",
            json!({
                "stream":true,
                "messages":[{"role":"user","content":"hi"}]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    assert!(bytes.ends_with(b"data: [DONE]\n\n"), "{bytes:?}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[derive(Clone)]
struct StreamOnlyFixture {
    hits: Arc<AtomicUsize>,
    streaming_flag: Arc<AtomicUsize>,
    chunks: Vec<Bytes>,
}

async fn strict_stream_only(
    State(state): State<StreamOnlyFixture>,
    request: Request<Body>,
) -> impl IntoResponse {
    state.hits.fetch_add(1, Ordering::SeqCst);
    let body = to_bytes(request.into_body(), 1 << 20).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    if payload["stream"] != true {
        return (StatusCode::BAD_REQUEST, "upstream requires stream:true").into_response();
    }
    state.streaming_flag.fetch_add(1, Ordering::SeqCst);
    let bytes = state.chunks.clone();
    let output = futures_util::stream::iter(bytes.into_iter().map(Ok::<Bytes, std::io::Error>));
    (
        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
        Body::from_stream(output),
    )
        .into_response()
}

async fn start_strict_stream_only(
    frames: Vec<Vec<u8>>,
) -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let streaming = Arc::new(AtomicUsize::new(0));
    let state = StreamOnlyFixture {
        hits: hits.clone(),
        streaming_flag: streaming.clone(),
        chunks: frames.into_iter().map(Bytes::from).collect(),
    };
    let router = Router::new()
        .route("/v1/chat/completions", post(strict_stream_only))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{addr}"), hits, streaming)
}

fn app_with_stream_only_and_auto(streaming_url: &str, healthy_url: &str, auto: bool) -> Router {
    app_with_stream_only_and_auto_walk(streaming_url, healthy_url, auto, "100s")
}

fn app_with_stream_only_and_auto_walk(
    streaming_url: &str,
    healthy_url: &str,
    auto: bool,
    walk: &str,
) -> Router {
    let auto_yaml = if auto {
        r#"
auto:
  enabled: true
  candidates:
    - provider: streaming
      model: stream-model
      priority: 1
    - provider: backup
      model: backup-model
      priority: 2
"#
    } else {
        ""
    };
    let yaml = format!(
        r#"
providers:
  - name: streaming
    type: openai
    base_url: {streaming_url}
    key_env: STREAM_KEY
    stream_only: true
  - name: backup
    type: openai
    base_url: {healthy_url}
    key_env: BACKUP_KEY
chains:
  - name: ordered
    steps:
      - provider: streaming
        model: stream-model
      - provider: backup
        model: backup-model
{auto_yaml}
timeout:
  walk: {walk}
"#
    );
    let config: Config = serde_yaml::from_str(&yaml).unwrap();
    app(AppState::new(
        config,
        HashMap::from([
            ("STREAM_KEY".into(), "stream-secret".into()),
            ("BACKUP_KEY".into(), "backup-secret".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap())
}

// Endless progress cannot evade the 100-second non-streaming walk budget.
// Healthy long-thinking streams are verified separately by the real-socket
// sixteen-second idle-gap and 20k-delta integration tests.
#[derive(Clone)]
struct ProgressingAggregation {
    hits: Arc<AtomicUsize>,
}

async fn serve_progressing_aggregation(
    State(state): State<ProgressingAggregation>,
) -> impl IntoResponse {
    state.hits.fetch_add(1, Ordering::SeqCst);
    let chunks = futures_util::stream::unfold(0usize, move |index| async move {
        match index {
            0 => Some((Ok::<_, std::io::Error>(Bytes::from_static(SSE_START)), 1)),
            _ => {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                Some((
                    Ok(Bytes::from_static(b": bounded stream heartbeat\n\n")),
                    index + 1,
                ))
            }
        }
    });
    (
        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
        Body::from_stream(chunks),
    )
}

async fn start_idle_gap_aggregation() -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let idle_wait_started = Arc::new(AtomicUsize::new(0));
    let signal = hits.clone();
    let idle_signal = idle_wait_started.clone();
    let router = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let signal = signal.clone();
            let idle_signal = idle_signal.clone();
            async move {
                signal.fetch_add(1, Ordering::SeqCst);
                let chunks = futures_util::stream::unfold(0u8, move |index| {
                    let idle_signal = idle_signal.clone();
                    async move {
                        match index {
                            0 => Some((Ok::<_, std::io::Error>(Bytes::from_static(SSE_START)), 1)),
                            1 => {
                                // Longer than the historical 15s timeout, shorter
                                // than the reasoning-aware 60s idle limit.
                                idle_signal.store(1, Ordering::SeqCst);
                                tokio::time::sleep(std::time::Duration::from_secs(16)).await;
                                Some((Ok(Bytes::from_static(SSE_END)), 2))
                            }
                            2 => Some((Ok(Bytes::from_static(SSE_DONE)), 3)),
                            _ => None,
                        }
                    }
                });
                (
                    [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                    Body::from_stream(chunks),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{addr}"), hits, idle_wait_started)
}

#[tokio::test(flavor = "current_thread")]
async fn sse_only_aggregation_tolerates_a_sixteen_second_reasoning_pause() {
    let (primary, primary_hits, idle_wait_started) = start_idle_gap_aggregation().await;
    let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
    let router = app_with_stream_only_and_auto_walk(&primary, &backup, false, "100s");
    let metrics_router = router.clone();
    let task = tokio::spawn(async move { call(router, request("ordered", "", "client")).await });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while idle_wait_started.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("upstream must enter the deliberate idle gap before virtual time advances");
    // Let the client consume the first frame and arm its idle watchdog while
    // the upstream is already blocked on the next frame.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    for _ in 0..40 {
        tokio::task::yield_now().await;
    }
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_secs(16)).await;
    for _ in 0..200 {
        tokio::task::yield_now().await;
        if task.is_finished() {
            break;
        }
    }
    tokio::time::resume();
    let (status, result) = tokio::time::timeout(std::time::Duration::from_secs(3), task)
        .await
        .expect("the long-thinking stream must finish within the walk budget")
        .unwrap();
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["choices"][0]["message"]["content"], "hi");
    assert_eq!(primary_hits.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
    let (_, metrics) = get_metrics(metrics_router, Some("client")).await;
    assert_eq!(metric_value(&metrics, "aggregation_completed_total"), 1);
    assert_eq!(metric_value(&metrics, "active_requests"), 0);
}

async fn start_progressing_aggregation() -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route("/v1/chat/completions", post(serve_progressing_aggregation))
        .with_state(ProgressingAggregation { hits: hits.clone() });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{addr}"), hits)
}

async fn drive_aggregating_progress(
    walk: &str,
) -> (
    StatusCode,
    Value,
    std::time::Duration,
    usize,
    usize,
    u64,
    u64,
) {
    let (primary, primary_hits) = start_progressing_aggregation().await;
    let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
    let app = app_with_stream_only_and_auto_walk(&primary, &backup, false, walk);
    let metrics_app = app.clone();
    let task = tokio::spawn(async move { call(app, request("ordered", "", "client")).await });
    // Keep real time running through the TCP connect and HTTP headers.
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if primary_hits.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("upstream should receive the first aggregation request");
    assert_eq!(primary_hits.load(Ordering::SeqCst), 1);
    for _ in 0..40 {
        tokio::task::yield_now().await;
    }
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    for _ in 0..160 {
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        for _ in 0..30 {
            tokio::task::yield_now().await;
        }
        if task.is_finished() {
            break;
        }
    }
    let elapsed = started.elapsed();
    tokio::time::resume();
    let (status, payload) = tokio::time::timeout(std::time::Duration::from_secs(3), task)
        .await
        .expect("progressing SSE aggregation must resolve within its bounded deadline")
        .unwrap();
    let (_, metrics) = get_metrics(metrics_app, Some("client")).await;
    (
        status,
        payload,
        elapsed,
        primary_hits.load(Ordering::SeqCst),
        backup_hits.load(Ordering::SeqCst),
        metric_value(&metrics, "walk_deadline_exhaustions_total"),
        metric_value(&metrics, "active_requests"),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn sse_only_infinite_heartbeat_ends_before_front_proxy_hard_timeout() {
    let (status, result, elapsed, primary, backup, deadline_exhaustions, active) =
        drive_aggregating_progress("12m").await;
    assert!(elapsed >= std::time::Duration::from_secs(95), "{elapsed:?}");
    assert!(
        elapsed <= std::time::Duration::from_secs(105),
        "{elapsed:?}"
    );
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{result}");
    assert_eq!(result["error"]["code"], "upstream_walk_timeout");
    assert_eq!(primary, 1, "no repeated attempts on a drip-fed primary");
    assert_eq!(
        backup, 0,
        "the exhausted budget must not start a late fallback"
    );
    assert_eq!(
        deadline_exhaustions, 1,
        "expected local 504 before front-proxy expiry"
    );
    assert_eq!(active, 0, "aggregation admission permit was not released");
}

// Issue #65's shared walk budget is authoritative even if an SSE-only
// upstream keeps sending heartbeats: it cannot silently monopolize a
// non-streaming slot beyond the front-proxy-safe aggregate cap.
#[tokio::test(flavor = "current_thread")]
async fn sse_only_progressing_heartbeat_cannot_bypass_shared_walk_budget() {
    let (status, result, elapsed, primary, backup, deadline_exhaustions, active) =
        drive_aggregating_progress("100s").await;
    assert!(elapsed >= std::time::Duration::from_secs(90), "{elapsed:?}");
    assert!(elapsed < std::time::Duration::from_secs(150), "{elapsed:?}");
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{result}");
    assert_eq!(result["error"]["code"], "upstream_walk_timeout");
    assert_eq!(primary, 1, "do not replay a drip-fed candidate");
    assert_eq!(backup, 0, "no fallback after exhausting the shared budget");
    assert_eq!(deadline_exhaustions, 1, "wrong total-walk deadline outcome");
    assert_eq!(active, 0, "aggregation admission permit was not released");
}

#[tokio::test]
async fn sse_only_large_usage_metadata_is_included_in_output_budget() {
    let mut frames = vec![];
    for _ in 0..2 {
        frames.push(aggregate_chat_frame(json!({
            "choices":[{"index":0, "delta":{"content":"x".repeat(800_000)},
                "finish_reason":null}]
        })));
    }
    frames.push(aggregate_chat_frame(json!({
        "choices":[],
        "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2,
            "provider_detail":"y".repeat(550_000)}
    })));
    frames.push(SSE_END.to_vec());
    frames.push(SSE_DONE.to_vec());
    let (primary, primary_hits, _) = start_strict_stream_only(frames).await;
    let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
    let app = app_with_stream_only_and_auto(&primary, &backup, false);
    let (status, payload) = call(app, request("ordered", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{payload}");
    assert_eq!(payload["choices"][0]["message"]["content"], "success");
    assert_eq!(primary_hits.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stream_only_provider_completes_nonstream_chat_with_fragmented_sse_and_tool_arguments() {
    let frames = vec![
        br#"data: {"id":"chat-123","model":"stream-model","created":45,"choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":null}]}

"#.to_vec(),
        br#"data: {"choices":[{"index":0,"delta":{"content":" there","tool_calls":[{"index":0,"id":"call_42","type":"function","function":{"name":"lookup","arguments":"{"}}]},"finish_reason":null}]}

"#.to_vec(),
        br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"q\":1}"}}]},"finish_reason":"tool_calls"}]}

"#.to_vec(),
        br#"data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}

data: [DONE]

"#.to_vec(),
    ];
    let (a, hits, streaming) = start_strict_stream_only(frames).await;
    let (b, backup) = start_upstream(StatusCode::OK).await;
    let router = app_with_stream_only_and_auto(&a, &b, false);
    let (status, result) = call(router, request("ordered", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["choices"][0]["message"]["content"], "hi there");
    assert_eq!(result["choices"][0]["finish_reason"], "tool_calls");
    assert_eq!(
        result["choices"][0]["message"]["tool_calls"][0]["id"],
        "call_42"
    );
    assert_eq!(
        result["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
        "lookup"
    );
    assert_eq!(
        result["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
        r#"{"q":1}"#
    );
    assert_eq!(result["usage"]["total_tokens"], 8);
    assert_eq!(result["id"], "chat-123");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(streaming.load(Ordering::SeqCst), 1);
    assert_eq!(backup.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn sse_only_aggregation_accepts_twenty_thousand_reasoning_deltas() {
    // Reproduce a normal reasoning workload whose event count exceeded the
    // former 16,384-frame cap, while keeping output and input under byte limits.
    let mut frames = Vec::with_capacity(22_004);
    frames.push(
        br#"data: {"id":"long-reasoning","model":"stream-model","created":45,"choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"r"},"finish_reason":null}]}

"#
        .to_vec(),
    );
    for _ in 0..19_999 {
        frames.push(
            br#"data: {"choices":[{"index":0,"delta":{"reasoning_content":"r"},"finish_reason":null}]}

"#
            .to_vec(),
        );
    }
    for _ in 0..2_000 {
        frames.push(
            br#"data: {"choices":[{"index":0,"delta":{"content":"c"},"finish_reason":null}]}

"#
            .to_vec(),
        );
    }
    frames.push(SSE_END.to_vec());
    frames.push(SSE_DONE.to_vec());
    let (primary, primary_hits, streaming) = start_strict_stream_only(frames).await;
    let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
    let router = app_with_stream_only_and_auto(&primary, &backup, false);
    let (status, result) = call(router, request("ordered", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(
        result["choices"][0]["message"]["reasoning_content"]
            .as_str()
            .unwrap()
            .len(),
        20_000
    );
    assert_eq!(
        result["choices"][0]["message"]["content"]
            .as_str()
            .unwrap()
            .len(),
        2_000
    );
    assert_eq!(result["choices"][0]["finish_reason"], "stop");
    assert_eq!(primary_hits.load(Ordering::SeqCst), 1);
    assert_eq!(streaming.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn stream_only_invalid_or_missing_done_falls_back_before_returning_json() {
    let invalid = vec![
        vec![SSE_START.to_vec()],
        vec![SSE_START.to_vec(), b"data: [DONE]\n\n".to_vec()],
        vec![SSE_START.to_vec(), SSE_END.to_vec(), b"data: {bad}\n\n".to_vec()],
        vec![b"data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"user\"},\"finish_reason\":null}]}\n\n".to_vec()],
    ];
    for frames in invalid {
        let (a, hits, streaming) = start_strict_stream_only(frames).await;
        let (b, backup_hits) = start_upstream(StatusCode::OK).await;
        let router = app_with_stream_only_and_auto(&a, &b, false);
        let (status, result) = call(router, request("ordered", "", "client")).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["choices"][0]["message"]["content"], "success");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(streaming.load(Ordering::SeqCst), 1);
        assert_eq!(backup_hits.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn chain_default_n_is_used_by_stream_and_stream_only_aggregation() {
    // The client omits n, but this chain explicitly requests two choices.
    // Both SSE validation and SSE-to-JSON synthesis must use the actual
    // per-candidate payload rather than the original client's default n=1.
    const TWO_START: &[u8] = b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"one\"},\"finish_reason\":null},{\"index\":1,\"delta\":{\"content\":\"two\"},\"finish_reason\":null}]}\n\n";
    const TWO_END: &[u8] = b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"},{\"index\":1,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
    let (url, hits) = start_sse(vec![TWO_START, TWO_END, SSE_DONE]).await;
    let config: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: stream\n    type: openai\n    base_url: {url}\n    key_env: STREAM_KEY\n    stream_only: true\nchains:\n  - name: two\n    steps:\n      - provider: stream\n        model: m\n        params:\n          n: 2\n"
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([("STREAM_KEY".into(), "test-secret".into())]),
        vec!["client".into()],
    )
    .unwrap());

    let streaming = router
        .clone()
        .oneshot(request("two", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(streaming.status(), StatusCode::OK);
    let bytes = to_bytes(streaming.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
    assert!(text.contains("\"index\":1"), "{text}");

    let (status, aggregated) = call(router, request("two", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{aggregated}");
    assert_eq!(aggregated["choices"].as_array().unwrap().len(), 2);
    assert_eq!(aggregated["choices"][1]["message"]["content"], "two");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn shadow_selector_backpressure_skips_unbounded_tasks_without_affecting_live_requests() {
    use guvna::gateway::app_with_state;
    use std::time::Duration;
    let (live, live_hits) = start_upstream(StatusCode::OK).await;
    let selector_hits = Arc::new(AtomicUsize::new(0));
    let released = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::clone(&selector_hits);
    let release = Arc::clone(&released);
    let selector = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let calls = Arc::clone(&calls);
            let release = Arc::clone(&release);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
                axum::Json(json!({"choices":[{"message":{"content":r#"{"order":[1,0]}"#}}]}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, selector).await.unwrap() });
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: live
    type: openai
    base_url: {live}
    key_env: LIVE
  - name: selector
    type: openai
    base_url: http://{address}
    key_env: SELECTOR
auto:
  enabled: true
  candidates:
    - provider: live
      model: first
    - provider: live
      model: second
  decider:
    enabled: true
    provider: selector
    model: ranker
    shadow_only: true
    timeout_ms: 2000
"#
    ))
    .unwrap();
    let (router, state) = app_with_state(
        AppState::new(
            config,
            HashMap::from([
                ("LIVE".into(), "live-key".into()),
                ("SELECTOR".into(), "selector-key".into()),
            ]),
            vec!["client".into()],
        )
        .unwrap(),
    );
    let payload = json!({"model":"auto","messages":[{"role":"user","content":"hello"}]});
    let (status, body) = call(
        router.clone(),
        auto_request("/v1/auto", "client", payload.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    tokio::time::timeout(Duration::from_secs(1), async {
        while selector_hits.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one selector should start");
    let tasks = (0..32).map(|_| {
        let router = router.clone();
        let payload = payload.clone();
        async move { call(router, auto_request("/v1/auto", "client", payload)).await }
    });
    let responses = tokio::time::timeout(
        Duration::from_secs(3),
        futures_util::future::join_all(tasks),
    )
    .await
    .expect("shadow backpressure must not block live traffic");
    for (status, body) in responses {
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    assert_eq!(selector_hits.load(Ordering::SeqCst), 1);
    assert_eq!(live_hits.load(Ordering::SeqCst), 33);
    let before = state.runtime_status();
    assert_eq!(before["auto_shadow_total"], 0);
    assert_eq!(before["auto_shadow_unavailable"], 32);
    released.notify_waiters();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.runtime_status()["auto_shadow_total"] != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first selector must finish and release the permit");
    assert_eq!(state.runtime_status()["auto_shadow_disagreements"], 1);
    server.abort();
}

#[tokio::test]
async fn streaming_auto_never_waits_for_a_cold_llm_decider() {
    let (live, hits, _) = start_strict_stream_only(vec![
        SSE_START.to_vec(),
        SSE_END.to_vec(),
        SSE_DONE.to_vec(),
    ])
    .await;
    let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
    let selector_hits = Arc::new(AtomicUsize::new(0));
    let selector_calls = Arc::clone(&selector_hits);
    let selector = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let selector_calls = Arc::clone(&selector_calls);
            async move {
                selector_calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
                axum::Json(json!({"choices":[{"message":{"content":r#"{"order":[1,0]}"#}}]}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let selector_addr = listener.local_addr().unwrap();
    let selector_task = tokio::spawn(async move {
        axum::serve(listener, selector).await.unwrap();
    });
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: live
    type: openai
    base_url: {live}
    key_env: LIVE
    stream_only: true
  - name: backup
    type: openai
    base_url: {backup}
    key_env: BACKUP
  - name: selector
    type: openai
    base_url: http://{selector_addr}
    key_env: SELECTOR
auto:
  enabled: true
  candidates:
    - provider: live
      model: primary
      priority: 1
    - provider: backup
      model: fallback
      priority: 2
  decider:
    enabled: true
    provider: selector
    model: ranker
    timeout_ms: 1500
    cache_ttl_secs: 30
"#
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([
            ("LIVE".into(), "live-key".into()),
            ("BACKUP".into(), "backup-key".into()),
            ("SELECTOR".into(), "selector-key".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap());
    let response = tokio::time::timeout(
        std::time::Duration::from_millis(700),
        router.oneshot(auto_request(
            "/v1/auto",
            "client",
            json!({"model":"auto","stream":true,"messages":[{"role":"user","content":"hello"}]}),
        )),
    )
    .await
    .expect("SSE headers must not wait for the 1100ms selector")
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("data: [DONE]"));
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
    assert_eq!(selector_hits.load(Ordering::SeqCst), 1);
    selector_task.abort();
}

#[tokio::test]
async fn auto_basic_uses_ordered_chain_without_remote_management_or_scoring() {
    let frames = vec![SSE_START.to_vec(), SSE_END.to_vec(), SSE_DONE.to_vec()];
    let (a, hits, streaming) = start_strict_stream_only(frames).await;
    let (b, backup) = start_upstream(StatusCode::OK).await;
    let router = app_with_stream_only_and_auto(&a, &b, true);
    let req = |path: &str, payload: Value| {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("authorization", "Bearer client")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap()
    };
    let (status, first) = call(
        router.clone(),
        req(
            "/v1/auto",
            json!({
                "messages":[{"role":"user","content":"test"}],
                "model":"auto"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["choices"][0]["message"]["content"], "hi");
    let (status, second) = call(
        router.clone(),
        req(
            "/v1/chat/completions",
            json!({
                "model":"auto","messages":[{"role":"user","content":"test"}]
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    assert_eq!(streaming.load(Ordering::SeqCst), 2);
    assert_eq!(backup.load(Ordering::SeqCst), 0);
    let (status, models) = call(
        router.clone(),
        Request::builder()
            .method("GET")
            .uri("/v1/models")
            .header("authorization", "Bearer client")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        models["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["id"] == "auto")
    );

    let noauto = app_with_stream_only_and_auto(&a, &b, false);
    let (status, output) = call(
        noauto,
        req(
            "/v1/auto",
            json!({
                "messages":[{"role":"user","content":"test"}]
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(output["error"]["code"], "auto_disabled");
}
async fn start_aggregation_stall() -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_for_server = hits.clone();
    let router = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let hits = hits_for_server.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                let output = futures_util::stream::unfold(0_u8, |index| async move {
                    if index == 0 {
                        Some((
                            Ok::<Bytes, std::io::Error>(Bytes::from_static(SSE_START)),
                            1,
                        ))
                    } else {
                        std::future::pending::<Option<(Result<Bytes, std::io::Error>, u8)>>().await
                    }
                });
                (
                    [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                    Body::from_stream(output),
                )
            }
        }),
    );
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(socket, router).await.unwrap() });
    (format!("http://{addr}"), hits)
}

#[tokio::test]
async fn four_aggregation_slots_bound_memory_and_busy_candidate_uses_backup() {
    let (a, hits) = start_aggregation_stall().await;
    let (b, backup_hits) = start_upstream(StatusCode::OK).await;
    let router = app_with_stream_only_and_auto(&a, &b, true);
    let mut held = Vec::new();
    for _ in 0..4 {
        let router = router.clone();
        held.push(tokio::spawn(async move {
            call(router, request("ordered", "", "client")).await
        }));
    }
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while hits.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("four requests should own four aggregation permits");

    // Another caller waits a bounded two seconds for space, then falls back.
    let (status, body) = tokio::time::timeout(
        std::time::Duration::from_secs(4),
        call(router.clone(), request("auto", "", "client")),
    )
    .await
    .expect("capacity must not cause indefinite stalls");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["choices"][0]["message"]["content"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 4);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 1);
    let (_, metrics) = get_metrics(router.clone(), Some("client")).await;
    assert_eq!(metric_value(&metrics, "aggregation_busy_total"), 1);

    // Releasing in-progress clients releases the semaphore reservations.
    for task in held {
        task.abort();
        let _ = task.await;
    }
    let handle = tokio::spawn({
        let router = router.clone();
        async move { call(router, request("ordered", "", "client")).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while hits.load(Ordering::SeqCst) < 5 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a new request must reacquire the released permit");
    handle.abort();
    let _ = handle.await;
    let (_, metrics) = get_metrics(router, Some("client")).await;
    assert_eq!(metric_value(&metrics, "active_requests"), 0);
}

fn aggregate_chat_frame(event: Value) -> Vec<u8> {
    format!("data: {event}\n\n").into_bytes()
}

#[tokio::test]
async fn sse_only_aggregate_preserves_reasoning_and_refusal_without_replaying_to_backup() {
    let scenarios = [
        (
            vec![
                aggregate_chat_frame(json!({"id":"reasoned","choices":[{"index":0,"delta":{
                    "role":"assistant","reasoning":"chain ", "reasoning_content":"first ", "content":"answer ", "audio":null
                },"finish_reason":null}]})),
                aggregate_chat_frame(json!({"id":"reasoned","choices":[{"index":0,"delta":{
                    "reasoning":"complete","reasoning_content":"second","content":"done"
                },"finish_reason":"stop"}]})),
            ],
            "answer done",
            Some("first second"),
            None,
        ),
        (
            vec![
                aggregate_chat_frame(json!({"id":"refused","choices":[{"index":0,"delta":{
                    "role":"assistant","refusal":"I cannot "
                },"finish_reason":null}]})),
                aggregate_chat_frame(json!({"id":"refused","choices":[{"index":0,"delta":{
                    "refusal":"help with that."
                },"finish_reason":"stop"}]})),
            ],
            "",
            None,
            Some("I cannot help with that."),
        ),
    ];
    for (mut frames, content, reasoning, refusal) in scenarios {
        frames.push(b"data: [DONE]\n\n".to_vec());
        let (primary, primary_hits, _) = start_strict_stream_only(frames).await;
        let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
        let router = app_with_stream_only_and_auto(&primary, &backup, false);
        let (status, output) = call(router, request("ordered", "", "client")).await;
        assert_eq!(status, StatusCode::OK, "{output}");
        let message = &output["choices"][0]["message"];
        assert_eq!(message["content"].as_str().unwrap_or_default(), content);
        assert_eq!(message["reasoning_content"].as_str(), reasoning);
        if reasoning.is_some() {
            assert_eq!(message["reasoning"].as_str(), Some("chain complete"));
        } else {
            assert!(message.get("reasoning").is_none());
        }
        assert_eq!(message["refusal"].as_str(), refusal);
        assert_eq!(primary_hits.load(Ordering::SeqCst), 1);
        assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn failed_stream_only_aggregation_does_not_repeat_same_model_with_param_variation() {
    let bad = aggregate_chat_frame(json!({"choices":[{"index":0,
        "delta":{"audio":{"data":"unsupported"}},"finish_reason":null}]}));
    let (primary, primary_hits, _) =
        start_strict_stream_only(vec![bad, b"data: [DONE]\n\n".to_vec()]).await;
    let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
    let yaml = format!(
        "providers:\n  - name: streaming\n    type: openai\n    base_url: {primary}\n    key_env: STREAM_KEY\n    stream_only: true\n  - name: backup\n    type: openai\n    base_url: {backup}\n    key_env: BACKUP_KEY\nchains:\n  - name: ordered\n    steps:\n      - provider: streaming\n        model: first-model\n      - provider: streaming\n        model: first-model\n        params:\n          temperature: 0.7\n      - provider: backup\n        model: backup-model\n"
    );
    let config: Config = serde_yaml::from_str(&yaml).unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([
            ("STREAM_KEY".into(), "stream-secret".into()),
            ("BACKUP_KEY".into(), "backup-secret".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap());
    let (status, output) = call(router, request("ordered", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{output}");
    assert_eq!(primary_hits.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn local_aggregation_failures_do_not_cascade_open_provider_circuit() {
    let invalid = aggregate_chat_frame(json!({"choices":[{"index":0,
        "delta":{"audio":{"data":"unsupported"}},"finish_reason":null}]}));
    let (primary, attempts, _) =
        start_strict_stream_only(vec![invalid, b"data: [DONE]\n\n".to_vec()]).await;
    let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: streaming
    type: openai
    base_url: {primary}
    key_env: STREAM_KEY
    stream_only: true
    circuit_breaker:
      enabled: true
      failure_threshold: 1
      cooldown: 500ms
      max_cooldown: 1s
  - name: backup
    type: openai
    base_url: {backup}
    key_env: BACKUP_KEY
chains:
  - name: ordered
    steps:
      - provider: streaming
        model: stream-model
      - provider: backup
        model: backup-model
"#
    ))
    .unwrap();
    let router = app(AppState::new(
        config,
        HashMap::from([
            ("STREAM_KEY".into(), "stream-secret".into()),
            ("BACKUP_KEY".into(), "backup-secret".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap());
    let runs = futures_util::future::join_all(
        (0..8).map(|_| call(router.clone(), request("ordered", "", "client"))),
    )
    .await;
    for (status, output) in &runs {
        assert_eq!(*status, StatusCode::OK, "{output}");
    }
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        8,
        "local aggregation failures incorrectly opened the shared breaker"
    );
    assert_eq!(backup_hits.load(Ordering::SeqCst), 8);
    let (_, metrics) = get_metrics(router, Some("client")).await;
    assert_eq!(metric_value(&metrics, "circuit_openings_total"), 0);
    assert_eq!(metric_value(&metrics, "aggregation_failures_total"), 8);
}

#[tokio::test]
async fn local_aggregation_policy_does_not_poison_auto_health_or_sqlite_restart() {
    use guvna::gateway::app_with_state;
    use rusqlite::Connection;

    let dir = tempfile::tempdir().unwrap();
    let invalid = aggregate_chat_frame(json!({"choices":[{"index":0,
        "delta":{"audio":{"data":"unsupported"}},"finish_reason":null}]}));
    let (primary, primary_hits, _) =
        start_strict_stream_only(vec![invalid, b"data: [DONE]\n\n".to_vec()]).await;
    let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
    let config: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: streaming
    type: openai
    base_url: {primary}
    key_env: STREAM_KEY
    stream_only: true
  - name: backup
    type: openai
    base_url: {backup}
    key_env: BACKUP_KEY
auto:
  enabled: true
  health_aware: true
  candidates:
    - provider: streaming
      model: stream-model
      priority: 1
    - provider: backup
      model: backup-model
      priority: 2
"#
    ))
    .unwrap();
    let credentials = HashMap::from([
        ("STREAM_KEY".into(), "stream-secret".into()),
        ("BACKUP_KEY".into(), "backup-secret".into()),
    ]);
    let mut state =
        AppState::new(config.clone(), credentials.clone(), vec!["client".into()]).unwrap();
    state.enable_telemetry(dir.path()).unwrap();
    let (router, shared) = app_with_state(state);
    for _ in 0..3 {
        let (status, output) = call(router.clone(), request("auto", "", "client")).await;
        assert_eq!(status, StatusCode::OK, "{output}");
    }
    assert_eq!(primary_hits.load(Ordering::SeqCst), 3);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 3);
    assert!(shared.flush_telemetry(std::time::Duration::from_secs(3)));

    let db = Connection::open(dir.path().join("telemetry/requests.sqlite3")).unwrap();
    let poisoned: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM provider_health WHERE provider='streaming' AND model='stream-model'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        poisoned, 0,
        "local SSE-to-JSON aggregation policy cannot silently demote a provider after restart"
    );
    let recorded: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM failure_events WHERE provider='streaming' AND model='stream-model'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(recorded, 3, "diagnostic failures still appear in telemetry");
    drop(db);

    let mut restarted = AppState::new(config, credentials, vec!["client".into()]).unwrap();
    restarted.enable_telemetry(dir.path()).unwrap();
    let (router, _) = app_with_state(restarted);
    let (status, output) = call(router, request("auto", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{output}");
    assert_eq!(
        primary_hits.load(Ordering::SeqCst),
        4,
        "SQLite restart must not reintroduce an unsupported aggregation health penalty"
    );
    assert_eq!(backup_hits.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn sse_only_aggregate_fails_over_for_unrepresentable_and_invalid_deltas() {
    let failures = vec![
        // Unsupported but syntactically valid extensions must never be
        // silently dropped from a successful synthesized Chat response.
        aggregate_chat_frame(json!({"choices":[{"index":0,"delta":{
            "role":"assistant","function_call":{"name":"legacy","arguments":"{}"}
        },"finish_reason":null}]})),
        aggregate_chat_frame(json!({"choices":[{"index":0,"delta":{
            "role":"assistant","image":{"url":"hidden-image-data"}
        },"finish_reason":null}]})),
        aggregate_chat_frame(json!({"choices":[{"index":0,"delta":{
            "role":"assistant","audio":{"data":"abcd"}
        },"finish_reason":null}]})),
        aggregate_chat_frame(json!({"choices":[{"index":0,"delta":{
            "role":"assistant","refusal":{"invalid":"object"}
        },"finish_reason":null}]})),
        aggregate_chat_frame(json!({"choices":[{"index":0,"delta":{
            "role":"assistant","reasoning_content":["invalid"]
        },"finish_reason":null}]})),
    ];
    for bad in failures {
        let (primary, hits, _) = start_strict_stream_only(vec![
            bad,
            aggregate_chat_frame(
                json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
            ),
            b"data: [DONE]\n\n".to_vec(),
        ])
        .await;
        let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
        let router = app_with_stream_only_and_auto(&primary, &backup, false);
        let (status, output) = call(router, request("ordered", "", "client")).await;
        assert_eq!(status, StatusCode::OK, "{output}");
        assert_eq!(output["choices"][0]["message"]["content"], "success");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(backup_hits.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn sse_only_aggregate_bounds_reasoning_and_refusal_combined_with_text() {
    let text = "r".repeat(680_000);
    let frames = vec![
        aggregate_chat_frame(json!({"choices":[{"index":0,"delta":{
            "role":"assistant","reasoning_content":text
        },"finish_reason":null}]})),
        aggregate_chat_frame(json!({"choices":[{"index":0,"delta":{
            "refusal":text
        },"finish_reason":null}]})),
        aggregate_chat_frame(json!({"choices":[{"index":0,"delta":{
            "reasoning_content":text
        },"finish_reason":null}]})),
        aggregate_chat_frame(json!({"choices":[{"index":0,"delta":{
            "content":text
        },"finish_reason":"stop"}]})),
        b"data: [DONE]\n\n".to_vec(),
    ];
    let (primary, hits, _) = start_strict_stream_only(frames).await;
    let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
    let router = app_with_stream_only_and_auto(&primary, &backup, false);
    let (status, output) = call(router, request("ordered", "", "client")).await;
    assert_eq!(status, StatusCode::OK, "{output}");
    assert_eq!(output["choices"][0]["message"]["content"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 1);
}

async fn start_bad_named_tool_then_stall() -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    let router = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let hits = counted.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                let first = aggregate_chat_frame(json!({"choices":[{
                    "index":0, "delta":{
                        "role":"assistant","tool_calls":[{
                            "index":0,"id":"call_bad","type":"function",
                            "function":{"name":"wrong_function","arguments":"{}"}
                        }]
                    }, "finish_reason":null
                }]}));
                let output =
                    futures_util::stream::unfold(Some(Bytes::from(first)), |state| async move {
                        if let Some(frame) = state {
                            Some((Ok::<Bytes, std::io::Error>(frame), None))
                        } else {
                            std::future::pending::<
                                Option<(Result<Bytes, std::io::Error>, Option<Bytes>)>,
                            >()
                            .await
                        }
                    });
                (
                    [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                    Body::from_stream(output),
                )
            }
        }),
    );
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(socket, router).await.unwrap() });
    (format!("http://{addr}"), hits)
}

#[tokio::test]
async fn sse_only_wrong_named_tool_fails_over_before_idle_timeout() {
    let (primary, hits) = start_bad_named_tool_then_stall().await;
    let (backup, fallback_hits) = start_upstream(StatusCode::OK).await;
    let router = app_with_stream_only_and_auto(&primary, &backup, false);
    // The healthy JSON fallback mock produces a lookup tool response when
    // the input says call, satisfying the original named-tool constraint.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer client")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "model": "ordered",
                "messages": [{"role": "user", "content": "call"}],
                "tool_choice": {"type": "function", "function": {"name": "lookup"}}
            })
            .to_string(),
        ))
        .unwrap();
    let (status, output) =
        tokio::time::timeout(std::time::Duration::from_secs(3), call(router, req))
            .await
            .expect("bad named tool must fall back immediately, not after 15-second idle");
    assert_eq!(status, StatusCode::OK, "{output}");
    assert_eq!(output["choices"][0]["finish_reason"], "tool_calls");
    assert_eq!(
        output["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
        "lookup"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(fallback_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sse_only_logprobs_request_never_returns_silently_lost_logprobs() {
    let (primary, primary_hits, _) = start_strict_stream_only(vec![
        SSE_START.to_vec(),
        SSE_END.to_vec(),
        SSE_DONE.to_vec(),
    ])
    .await;
    let (backup, backup_hits) = start_upstream(StatusCode::OK).await;
    let router = app_with_stream_only_and_auto(&primary, &backup, false);
    let (status, output) = call(router, request("ordered", r#","logprobs":true"#, "client")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{output}");
    assert_eq!(output["error"]["code"], "upstream_unavailable");
    assert_eq!(primary_hits.load(Ordering::SeqCst), 0);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 1);
}

async fn start_variable_choice_upstream() -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    let router = Router::new().route(
        "/v1/chat/completions",
        post(move |body: Bytes| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                let request: Value = serde_json::from_slice(&body).unwrap();
                let n = request.get("n").and_then(Value::as_u64).unwrap_or(1);
                let choices = (0..n)
                    .map(|index| {
                        json!({
                            "index":index,
                            "message":{"role":"assistant","content":"json fallback"},
                            "finish_reason":"stop"
                        })
                    })
                    .collect::<Vec<_>>();
                (
                    StatusCode::OK,
                    axum::Json(json!({"id":"fallback","object":"chat.completion",
                        "choices": choices})),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{addr}"), hits)
}

#[tokio::test]
async fn sse_only_aggregate_declines_unsupported_n_before_network_but_keeps_json_fallback() {
    let sse = vec![SSE_START.to_vec(), SSE_END.to_vec(), SSE_DONE.to_vec()];
    let (primary, streaming_hits, _) = start_strict_stream_only(sse).await;
    let (backup, backup_hits) = start_variable_choice_upstream().await;
    let router = app_with_stream_only_and_auto(&primary, &backup, true);

    for n in [0_u64, 65, 512] {
        // A direct passthrough has no other eligible candidate. Report the
        // stream aggregator's capability limit clearly without HTTP traffic.
        let (status, output) = call(
            router.clone(),
            request("streaming/model", &format!(",\"n\":{n}"), "client"),
        )
        .await;
        if n == 0 {
            // Invalid client input is rejected before any provider/aggregation
            // work; this protects provider health state from local 400s.
            assert_eq!(status, StatusCode::BAD_REQUEST, "{output}");
            assert_eq!(output["error"]["code"], "invalid_n");
        } else {
            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{output}");
            assert_eq!(output["error"]["code"], "aggregate_n_unsupported");
        }
        assert_eq!(streaming_hits.load(Ordering::SeqCst), 0);

        // The fallback JSON provider may support n > 64; do not reject the
        // entire chain merely because the SSE-only candidate cannot.
        if n > 64 {
            let (status, output) = call(
                router.clone(),
                request("ordered", &format!(",\"n\":{n}"), "client"),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{output}");
            assert_eq!(output["choices"].as_array().unwrap().len(), n as usize);
            assert_eq!(output["choices"][0]["message"]["content"], "json fallback");
            assert_eq!(streaming_hits.load(Ordering::SeqCst), 0);
        }
    }
    // Auto routes through precisely the same single dispatch loop and skips
    // the unsupported SSE-only candidate without consuming any credential.
    let auto_req = Request::builder()
        .method("POST")
        .uri("/v1/auto")
        .header("authorization", "Bearer client")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "model":"auto", "messages":[{"role":"user","content":"test"}], "n":65
            })
            .to_string(),
        ))
        .unwrap();
    let (status, output) = call(router.clone(), auto_req).await;
    assert_eq!(status, StatusCode::OK, "{output}");
    assert_eq!(output["choices"].as_array().unwrap().len(), 65);
    assert_eq!(streaming_hits.load(Ordering::SeqCst), 0);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 3);
    let (_, metrics) = get_metrics(router, Some("client")).await;
    assert_eq!(metric_value(&metrics, "upstream_attempts_total"), 3);
    assert_eq!(
        metric_value(&metrics, "upstream_invalid_responses_total"),
        0
    );
}

#[tokio::test]
async fn valid_single_choice_still_aggregates_from_stream_only_primary() {
    let (primary, primary_hits, _) = start_strict_stream_only(vec![
        SSE_START.to_vec(),
        SSE_END.to_vec(),
        SSE_DONE.to_vec(),
    ])
    .await;
    let (backup, backup_hits) = start_variable_choice_upstream().await;
    let router = app_with_stream_only_and_auto(&primary, &backup, false);
    let (status, output) = call(router, request("ordered", ",\"n\":1", "client")).await;
    assert_eq!(status, StatusCode::OK, "{output}");
    assert_eq!(output["choices"][0]["message"]["content"], "hi");
    assert_eq!(primary_hits.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn sse_only_aggregation_accepts_exactly_sixty_four_choices() {
    let choices = (0..64)
        .map(|index| {
            json!({
                "index": index,
                "delta": {"role":"assistant","content":format!("answer-{index}")},
                "finish_reason": "stop"
            })
        })
        .collect::<Vec<_>>();
    let primary_stream = vec![
        aggregate_chat_frame(json!({"id":"sixty-four","choices":choices})),
        b"data: [DONE]\n\n".to_vec(),
    ];
    let (primary, primary_hits, _) = start_strict_stream_only(primary_stream).await;
    let (backup, backup_hits) = start_variable_choice_upstream().await;
    let app = app_with_stream_only_and_auto(&primary, &backup, false);
    let (status, body) = call(app, request("ordered", ",\"n\":64", "client")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let completed = body["choices"].as_array().unwrap();
    assert_eq!(completed.len(), 64);
    for (i, choice) in completed.iter().enumerate() {
        assert_eq!(choice["index"], i);
        assert_eq!(choice["message"]["content"], format!("answer-{i}"));
        assert_eq!(choice["finish_reason"], "stop");
    }
    assert_eq!(primary_hits.load(Ordering::SeqCst), 1);
    assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct SlowWalkProvider {
    calls: Arc<AtomicUsize>,
    delay: std::time::Duration,
    status: StatusCode,
}

async fn slow_walk_upstream(
    State(state): State<SlowWalkProvider>,
    request: Request<Body>,
) -> impl IntoResponse {
    state.calls.fetch_add(1, Ordering::SeqCst);
    tokio::time::sleep(state.delay).await;
    if !state.status.is_success() {
        return (state.status, axum::Json(json!({"error":"injected"}))).into_response();
    }
    let bytes = to_bytes(request.into_body(), 1 << 20).await.unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    (
        StatusCode::OK,
        axum::Json(json!({
            "id":"slow-ok",
            "object":"chat.completion",
            "model":value["model"],
            "choices":[{"index":0,"message":{"role":"assistant","content":"late-success"},
                        "finish_reason":"stop"}]
        })),
    )
        .into_response()
}

async fn start_slow_walk_provider(
    delay: std::time::Duration,
    status: StatusCode,
) -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route("/v1/chat/completions", post(slow_walk_upstream))
        .with_state(SlowWalkProvider {
            calls: calls.clone(),
            delay,
            status,
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{addr}"), calls)
}

fn walk_budget_router(primary: &str, backup: &str, walk: &str, stream_only: bool) -> Router {
    let config: Config = serde_yaml::from_str(&format!(
        r#"
timeout:
  walk: {walk}
providers:
  - name: first
    type: openai
    base_url: {primary}
    key_env: FIRST
    stream_only: {stream_only}
  - name: backup
    type: openai
    base_url: {backup}
    key_env: BACKUP
chains:
  - name: budget
    steps:
      - provider: first
        model: one
      - provider: backup
        model: two
"#
    ))
    .unwrap();
    app(AppState::new(
        config,
        HashMap::from([
            ("FIRST".into(), "firstsecret".into()),
            ("BACKUP".into(), "backupsecret".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap())
}

#[tokio::test]
async fn issue65_total_walk_budget_cancels_slow_fallback_before_a_late_success() {
    use std::time::{Duration, Instant};
    // Prior behavior: both candidates each receive a fresh 90s deadline,
    // allowing a 190s+ walk through Cloudflare's 120s proxy. These scaled
    // delays deterministically prove that the SECOND attempt has only the
    // time remaining in the ONE shared walk budget.
    let (first, first_hits) =
        start_slow_walk_provider(Duration::from_millis(180), StatusCode::SERVICE_UNAVAILABLE).await;
    let (second, second_hits) =
        start_slow_walk_provider(Duration::from_millis(250), StatusCode::OK).await;
    let router = walk_budget_router(&first, &second, "270ms", false);
    let begin = Instant::now();
    let (status, body) = tokio::time::timeout(
        Duration::from_secs(3),
        call(router.clone(), request("budget", "", "client")),
    )
    .await
    .expect("non-streaming fallback must have a bounded lifetime");
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(body["error"]["code"], "upstream_walk_timeout");
    assert!(begin.elapsed() < Duration::from_secs(2));
    assert_eq!(first_hits.load(Ordering::SeqCst), 1);
    assert!(second_hits.load(Ordering::SeqCst) <= 1);
    let (_, metrics) = get_metrics(router, Some("client")).await;
    assert_eq!(metric_value(&metrics, "active_requests"), 0);
    assert_eq!(metric_value(&metrics, "walk_deadline_exhaustions_total"), 1);
    assert_eq!(
        metric_value(&metrics, "upstream_invalid_responses_total"),
        0
    );
}

#[tokio::test]
async fn issue65_healthy_fallback_finishes_if_total_budget_suffices() {
    use std::time::Duration;
    let (first, first_hits) =
        start_slow_walk_provider(Duration::from_millis(65), StatusCode::SERVICE_UNAVAILABLE).await;
    let (second, second_hits) =
        start_slow_walk_provider(Duration::from_millis(65), StatusCode::OK).await;
    let router = walk_budget_router(&first, &second, "2s", false);
    let (status, body) = call(router, request("budget", "", "client")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["choices"][0]["message"]["content"], "late-success");
    assert_eq!(first_hits.load(Ordering::SeqCst), 1);
    assert_eq!(second_hits.load(Ordering::SeqCst), 1);
}

async fn delayed_stream_upstream(State(calls): State<Arc<AtomicUsize>>) -> impl IntoResponse {
    calls.fetch_add(1, Ordering::SeqCst);
    let output = futures_util::stream::unfold(0u8, |index| async move {
        match index {
            0 => Some((
                Ok::<Bytes, std::io::Error>(Bytes::from_static(SSE_START)),
                1,
            )),
            1 => {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                Some((Ok(Bytes::from_static(SSE_END)), 2))
            }
            2 => Some((Ok(Bytes::from_static(SSE_DONE)), 3)),
            _ => None,
        }
    });
    (
        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
        Body::from_stream(output),
    )
        .into_response()
}

async fn start_delayed_stream() -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route("/v1/chat/completions", post(delayed_stream_upstream))
        .with_state(calls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{addr}"), calls)
}

#[tokio::test]
async fn issue65_sse_only_aggregation_obeys_walk_budget_but_client_sse_keeps_progress() {
    use std::time::Duration;
    let (first, calls) = start_delayed_stream().await;
    let router = walk_budget_router(&first, &first, "130ms", true);
    // JSON aggregation reads the SSE source; it MUST obey the total walk
    // budget even though the HTTP stream itself remains healthy.
    let (status, body) = tokio::time::timeout(
        Duration::from_secs(3),
        call(router.clone(), request("first/demo", "", "client")),
    )
    .await
    .expect("aggregation cannot run indefinitely");
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(body["error"]["code"], "upstream_walk_timeout");

    // The identical stream source via client-facing SSE must remain alive
    // well beyond timeout.walk as long as it keeps valid progress.
    let response = router
        .clone()
        .oneshot(request("first/demo", r#","stream":true"#, "client"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = tokio::time::timeout(
        Duration::from_secs(3),
        to_bytes(response.into_body(), 1 << 20),
    )
    .await
    .expect("healthy SSE should continue beyond the JSON-only deadline")
    .unwrap();
    assert!(
        bytes.ends_with(SSE_DONE),
        "{:?}",
        String::from_utf8_lossy(&bytes)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
