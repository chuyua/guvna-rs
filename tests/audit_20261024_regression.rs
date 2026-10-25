//! Regression coverage for the independently reproduced 2026-10-24 audit issues.
//! Local fake upstreams only; no real provider tokens or external endpoints.
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    routing::post,
};
use guvna::{
    config::Config,
    gateway::{AppState, app_with_state},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tower::ServiceExt;

fn completion() -> Value {
    json!({"id":"backup","created":1,"model":"m",
        "choices":[{"index":0,"message":{"role":"assistant","content":"backup"},
            "finish_reason":"stop"}]})
}

async fn fake_upstream(body: String, sse: bool) -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let router = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let body = body.clone();
            let seen = Arc::clone(&seen);
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                (
                    [(
                        "content-type",
                        if sse {
                            "text/event-stream"
                        } else {
                            "application/json"
                        },
                    )],
                    body,
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{addr}"), calls)
}

fn gateway(first: &str, backup: &str) -> Router {
    let config: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: p\n    type: openai\n    base_url: {first}\n    key_env: K\n    stream_only: true\n  - name: b\n    type: openai\n    base_url: {backup}\n    key_env: B\nchains:\n  - name: route\n    steps:\n      - provider: p\n        model: m\n      - provider: b\n        model: m\n"
    )).unwrap();
    app_with_state(
        AppState::new(
            config,
            HashMap::from([("K".into(), "local".into()), ("B".into(), "local".into())]),
            vec!["local".into()],
        )
        .unwrap(),
    )
    .0
}

fn request(model: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer local")
        .header("content-type", "application/json")
        .body(Body::from(json!({"model":model,"messages":[]}).to_string()))
        .unwrap()
}
fn sse(value: Value) -> String {
    format!("data: {value}\n\n")
}
async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
}

#[tokio::test]
async fn audit_f06_complete_eof_without_done_uses_first_upstream() {
    let stream = sse(json!({"id":"first","choices":[{"index":0,"delta":
        {"role":"assistant","content":"first"},"finish_reason":"stop"}]}));
    let (first, first_calls) = fake_upstream(stream, true).await;
    let (backup, backup_calls) = fake_upstream(completion().to_string(), false).await;
    let body = json_body(
        gateway(&first, &backup)
            .oneshot(request("route"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["choices"][0]["message"]["content"], "first", "{body}");
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(backup_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn audit_f06_partial_eof_must_fall_back() {
    let stream = sse(json!({"id":"first","choices":[{"index":0,"delta":
        {"content":"unfinished"},"finish_reason":null}]}));
    let (first, _) = fake_upstream(stream, true).await;
    let (backup, backup_calls) = fake_upstream(completion().to_string(), false).await;
    let body = json_body(
        gateway(&first, &backup)
            .oneshot(request("route"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["choices"][0]["message"]["content"], "backup", "{body}");
    assert_eq!(backup_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn audit_f07_provisional_usage_then_final_trailer_replaces_accounting() {
    let stream = sse(json!({"id":"first","choices":[{"index":0,"delta":
        {"content":"first"},"finish_reason":"stop"}],
        "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}))
        + &sse(json!({"choices":[],"usage":{
            "prompt_tokens":5,"completion_tokens":7,"total_tokens":12
        }}))
        + "data: [DONE]\n\n";
    let (first, _) = fake_upstream(stream, true).await;
    let (backup, backup_calls) = fake_upstream(completion().to_string(), false).await;
    let body = json_body(
        gateway(&first, &backup)
            .oneshot(request("route"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["choices"][0]["message"]["content"], "first", "{body}");
    assert_eq!(body["usage"]["total_tokens"], 12);
    assert_eq!(backup_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn audit_f07_duplicate_authoritative_trailer_must_fall_back() {
    let trailer = sse(json!({"choices":[],"usage":{
        "prompt_tokens":1,"completion_tokens":1,"total_tokens":2
    }}));
    let stream = sse(json!({"id":"first","choices":[{"index":0,"delta":
        {"content":"first"},"finish_reason":"stop"}]}))
        + &trailer
        + &trailer
        + "data: [DONE]\n\n";
    let (first, _) = fake_upstream(stream, true).await;
    let (backup, backup_calls) = fake_upstream(completion().to_string(), false).await;
    let body = json_body(
        gateway(&first, &backup)
            .oneshot(request("route"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["choices"][0]["message"]["content"], "backup", "{body}");
    assert_eq!(backup_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn audit_f05_upstream_4xx_body_must_respect_walk_deadline() {
    let upstream = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let chunks = futures_util::stream::unfold(0, |i| async move {
                match i {
                    0 => Some((
                        Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"{\"error\":")),
                        1,
                    )),
                    1 => {
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        Some((Ok(axum::body::Bytes::from_static(b"{}}")), 2))
                    }
                    _ => None,
                }
            });
            (StatusCode::BAD_REQUEST, Body::from_stream(chunks))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let config: Config = serde_yaml::from_str(&format!(
        "timeout:\n  walk: 100ms\nproviders:\n  - name: p\n    type: openai\n    base_url: http://{addr}\n    key_env: K\n"
    )).unwrap();
    let router = app_with_state(
        AppState::new(
            config,
            HashMap::from([("K".into(), "local".into())]),
            vec!["local".into()],
        )
        .unwrap(),
    )
    .0;
    let started = Instant::now();
    let response = router.oneshot(request("p/m")).await.unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "4xx body escaped 100ms walk budget: {:?}",
        started.elapsed()
    );
}
