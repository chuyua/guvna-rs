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
            "debug_seed": value["seed"]
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
async fn authorized_posts_still_enforce_the_32_mib_body_limit() {
    let (url, calls) = start_upstream(StatusCode::OK).await;
    let router = make_app(&url, &url);
    let too_large = vec![b'x'; 32 * 1024 * 1024 + 1];
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
    assert_eq!(hits.load(Ordering::SeqCst), 23);
    let (status, converted) = call(router.clone(), req(json!({"model":"demo","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hel"},{"type":"input_text","text":"lo"}]}]}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(converted["output_text"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 24);
    let (status, instructed) = call(
        router.clone(),
        req(json!({"model":"demo","instructions":"Answer briefly","input":"hello"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(instructed["output_text"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 25);
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
    assert_eq!(hits.load(Ordering::SeqCst), 26);
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
    assert_eq!(invalid["error"]["code"], "invalid_upstream_usage");
    assert_eq!(hits.load(Ordering::SeqCst), 28);
    let (status, tool_result) = call(router.clone(), req(json!({"model":"demo","input":"call","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],"tool_choice":"auto"}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tool_result["output"][0]["type"], "function_call");
    assert_eq!(tool_result["output"][0]["name"], "lookup");
    assert_eq!(tool_result["output"][0]["call_id"], "call_123");
    assert_eq!(hits.load(Ordering::SeqCst), 29);
    let (status, call_usage) = call(router.clone(), req(json!({"model":"demo","input":"callusage","tools":[{"type":"function","name":"lookup"}]}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        call_usage["usage"],
        json!({"input_tokens":5,"output_tokens":3,"total_tokens":8})
    );
    assert!(call_usage["incomplete_details"].is_null());
    assert_eq!(hits.load(Ordering::SeqCst), 30);
    let (status, mixed) = call(router.clone(), req(json!({"model":"demo","input":"callmixed","tools":[{"type":"function","name":"lookup"}],"parallel_tool_calls":true}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(mixed["output_text"], "I will look that up");
    assert_eq!(mixed["output"].as_array().unwrap().len(), 3);
    assert_eq!(mixed["output"][0]["type"], "message");
    assert_eq!(mixed["output"][1]["call_id"], "call_1");
    assert_eq!(mixed["output"][2]["call_id"], "call_2");
    assert_eq!(hits.load(Ordering::SeqCst), 31);
    let (status, duplicate) = call(
        router.clone(),
        req(
            json!({"model":"demo","input":"calldup","tools":[{"type":"function","name":"lookup"}]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(duplicate["error"]["code"], "upstream_unavailable");
    assert_eq!(hits.load(Ordering::SeqCst), 33);
    let (status, continued) = call(router.clone(), req(json!({"model":"demo","input":[{"type":"function_call","call_id":"call_123","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"call_123","output":"Paris is in France"}]}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(continued["output_text"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 34);
    let (status, _) = call(router.clone(), req(json!({"model":"demo","input":[{"type":"function_call_output","call_id":"call_123","output":7}]}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(hits.load(Ordering::SeqCst), 34);
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
    assert_eq!(hits.load(Ordering::SeqCst), 35);
    let (status, assistant_text) = call(router.clone(), req(json!({"model":"demo","input":[{"role":"assistant","content":[{"type":"output_text","text":"previous response"}]},{"role":"user","content":"follow up"}]}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(assistant_text["output_text"], "success");
    assert_eq!(hits.load(Ordering::SeqCst), 36);
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
    assert_eq!(hits.load(Ordering::SeqCst), 36);
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
    assert_eq!(hits.load(Ordering::SeqCst), 36);
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
    assert_eq!(hits.load(Ordering::SeqCst), 36);
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
const RESP_LENGTH_END: &[u8] = b"data: {\"id\":\"chat1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"!\"},\"finish_reason\":\"length\"}]}\n\n";
const RESP_USAGE: &[u8] = b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}\n\n";

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
    assert_eq!(after.matches("# TYPE guvna_").count(), 22);
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
