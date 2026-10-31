//! Stacked #130 + #138 + #139 regression: a stream-only Auto primary
//! cannot commit an incomplete SSE as a successful Responses answer.
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
};
use tower::ServiceExt;

async fn fake_provider(response: String, content_type: &'static str) -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let hits = Arc::clone(&calls);
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let response = response.clone();
            let hits = Arc::clone(&hits);
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                ([(axum::http::header::CONTENT_TYPE, content_type)], response)
            }
        }),
    );
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    (format!("http://{address}"), calls)
}

#[tokio::test]
async fn auto_responses_incomplete_stream_fails_over_without_false_success() {
    let incomplete = concat!(
        "data: {\"id\":\"bad\",\"choices\":[{\"index\":0,\"delta\":",
        "{\"content\":\"unfinished\"},\"finish_reason\":null}]}\n\n"
    );
    let (first, first_calls) = fake_provider(incomplete.to_owned(), "text/event-stream").await;
    let backup_chat = json!({
        "id":"complete","created":1,"model":"m",
        "choices":[{
            "index":0,
            "message":{"role":"assistant","content":"completed by backup"},
            "finish_reason":"stop"
        }]
    });
    let (backup, backup_calls) = fake_provider(backup_chat.to_string(), "application/json").await;
    let cfg: Config = serde_yaml::from_str(&format!(
        r#"
providers:
  - name: first
    type: openai
    base_url: {first}
    key_env: F
    stream_only: true
  - name: backup
    type: openai
    base_url: {backup}
    key_env: B
auto:
  enabled: true
  candidates:
    - provider: first
      model: m
    - provider: backup
      model: m
"#
    ))
    .unwrap();
    cfg.validate().unwrap();
    let (app, state) = app_with_state(
        AppState::new(
            cfg,
            HashMap::from([
                ("F".into(), "fake-local".into()),
                ("B".into(), "fake-local".into()),
            ]),
            vec!["local-client".into()],
        )
        .unwrap(),
    );
    let req = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("authorization", "Bearer local-client")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"model":"auto","input":"hello"}).to_string(),
        ))
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["output_text"], "completed by backup", "{body}");
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(backup_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.runtime_status()["upstream_success"], 1);
}
