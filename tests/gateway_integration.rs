use axum::{
    Router,
    body::{Body, to_bytes},
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
async fn reject_bad_key_and_unsupported_stream_without_calling_upstream() {
    let (a, calls) = start_upstream(StatusCode::OK).await;
    let router = make_app(&a, &a);
    let (status, _) = call(router.clone(), request("demo", "", "wrong")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, result) = call(router, request("demo", r#","stream":true"#, "client")).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(result["error"]["code"], "migration_pending");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
