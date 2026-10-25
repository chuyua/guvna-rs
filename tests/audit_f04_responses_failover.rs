//! F04 regression: a syntactically valid Chat answer that cannot become a
//! Responses answer is not committed as provider success before failover.
use axum::{
    Json, Router,
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

fn chat() -> Value {
    json!({"id":"chat1","created":1,"model":"m","choices":[{
        "index":0,"message":{"role":"assistant","content":"from backup"},
        "finish_reason":"stop"}]})
}

async fn fake_chat(result: Value) -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let hits = Arc::clone(&calls);
    let router = Router::new().route(
        "/v1/chat/completions",
        post(move |Json(body): Json<Value>| {
            let result = result.clone();
            let hits = Arc::clone(&hits);
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                // The actual request must not use a chain's multi-choice n.
                assert_eq!(body["n"], 1);
                axum::Json(result)
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{addr}"), calls)
}

fn router(first: &str, backup: &str, chain_n: usize) -> (Router, Arc<AppState>) {
    let config: Config = serde_yaml::from_str(&format!(
        "providers:\n  - name: p\n    type: openai\n    base_url: {first}\n    key_env: K\n  - name: b\n    type: openai\n    base_url: {backup}\n    key_env: B\nchains:\n  - name: route\n    steps:\n      - provider: p\n        model: m\n        params:\n          n: {chain_n}\n      - provider: b\n        model: m\n"
    )).unwrap();
    app_with_state(
        AppState::new(
            config,
            HashMap::from([("K".into(), "local".into()), ("B".into(), "local".into())]),
            vec!["local".into()],
        )
        .unwrap(),
    )
}
fn request() -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("authorization", "Bearer local")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"model":"route","input":"hi"}).to_string(),
        ))
        .unwrap()
}
async fn body(result: axum::response::Response) -> Value {
    assert_eq!(result.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(result.into_body(), 2 << 20).await.unwrap()).unwrap()
}

#[tokio::test]
async fn invalid_chat_id_can_failover_and_is_not_marked_success() {
    let mut invalid = chat();
    invalid.as_object_mut().unwrap().remove("id");
    let (primary, _) = fake_chat(invalid).await;
    let (backup, backup_calls) = fake_chat(chat()).await;
    let (router, state) = router(&primary, &backup, 3);
    let result = body(router.oneshot(request()).await.unwrap()).await;
    assert_eq!(result["output_text"], "from backup");
    assert_eq!(backup_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.runtime_status()["upstream_success"], 1);
}

#[tokio::test]
async fn invalid_chat_usage_falls_back_without_poisoning_candidate_walk() {
    let mut invalid = chat();
    invalid["usage"] = json!({
        "prompt_tokens":1,"completion_tokens":3,"total_tokens":2
    });
    let (primary, _) = fake_chat(invalid).await;
    let (backup, backup_calls) = fake_chat(chat()).await;
    let (router, state) = router(&primary, &backup, 2);
    let result = body(router.oneshot(request()).await.unwrap()).await;
    assert_eq!(result["output_text"], "from backup");
    assert_eq!(backup_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.runtime_status()["upstream_success"], 1);
}
