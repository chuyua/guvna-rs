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
            axum::Json(json!({"error":"quota"})),
        )
            .into_response();
    }
    if state.status != StatusCode::OK {
        return (state.status, axum::Json(json!({"error":"unavailable"}))).into_response();
    }
    let body = to_bytes(request.into_body(), 1 << 20).await.unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    let model = value["model"].as_str().unwrap_or_default();
    (
        StatusCode::OK,
        axum::Json(json!({
            "id":"mock","object":"chat.completion","model":model,
            "choices":[{"index":0,"message":{"role":"assistant","content":"success"},
            "finish_reason":"stop"}],
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
    let yaml = format!(
        r#"
providers:
  - name: broken
    type: openai
    base_url: {a}
    key_env: FIRST
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
    let drops = Arc::new(AtomicUsize::new(0));
    let signal = drops.clone();
    let router = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let signal = signal.clone();
            async move {
                let output = futures_util::stream::unfold(
                    (false, DropSignal(signal)),
                    |(sent, guard)| async move {
                        if sent {
                            std::future::pending::<
                                Option<(Result<Bytes, std::io::Error>, (bool, DropSignal))>,
                            >()
                            .await
                        } else {
                            Some((Ok(Bytes::from_static(SSE_START)), (true, guard)))
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
