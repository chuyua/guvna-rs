use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use guvna::{
    config::Config,
    gateway::{AppState, app},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tower::ServiceExt;

#[derive(Clone, Default)]
struct Mock {
    answer: Arc<Mutex<Option<String>>>,
    requests: Arc<Mutex<Vec<Value>>>,
    next: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}
fn native(id: &str, model: &str, text: &str) -> Value {
    json!({"id":id,"object":"response","created_at":123,"model":model,"status":"completed","error":null,
    "output":[{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]}],
    "usage":{"input_tokens":5,"output_tokens":3,"total_tokens":8,"input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":1}}})
}
fn frame(kind: &str, seq: usize, mut value: Value) -> String {
    value["type"] = json!(kind);
    value["sequence_number"] = json!(seq);
    format!("event: {kind}\ndata: {value}\n\n")
}
async fn create_native(State(mock): State<Mock>, req: Request<Body>) -> Response {
    let auth = req
        .headers()
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let mut value: Value =
        serde_json::from_slice(&to_bytes(req.into_body(), 8 * 1024 * 1024).await.unwrap()).unwrap();
    value["_auth"] = json!(auth);
    mock.requests.lock().unwrap().push(value.clone());
    if mock.fail.load(Ordering::SeqCst) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let id = format!("resp_{}", mock.next.fetch_add(1, Ordering::SeqCst));
    let answer = mock.answer.lock().unwrap().clone().unwrap_or_else(|| {
        if value["model"] == "selector" {
            r#"{"order":[1,0]}"#.into()
        } else {
            "hello".into()
        }
    });
    let mut result = native(&id, value["model"].as_str().unwrap(), &answer);
    if value["input"] == "encrypted" {
        result["output"].as_array_mut().unwrap().insert(0,json!({"id":"rs_1","type":"reasoning","summary":[],"encrypted_content":"opaque-provider-state"}));
    }
    if value["input"] == "conversation" || !value["conversation"].is_null() {
        result["conversation"] = json!({"id":"conv_1"});
    }
    if value["background"] == true {
        result["status"] = json!("queued");
        result["output"] = json!([]);
    }
    if value["stream"] == true {
        let frames = frame(
            "response.created",
            0,
            json!({"response":{"id":id,"object":"response","created_at":123,"model":value["model"],"status":"in_progress","output":[]}}),
        ) + &frame(
            "response.output_item.added",
            1,
            json!({"output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant"}}),
        ) + &frame(
            "response.output_text.delta",
            2,
            json!({"output_index":0,"content_index":0,"delta":answer}),
        ) + &frame("response.completed", 3, json!({"response":result}));
        return ([("content-type", "text/event-stream")], frames).into_response();
    }
    Json(result).into_response()
}
async fn chat(State(mock): State<Mock>, req: Request<Body>) -> Response {
    let value: Value =
        serde_json::from_slice(&to_bytes(req.into_body(), 8 * 1024 * 1024).await.unwrap()).unwrap();
    mock.requests.lock().unwrap().push(value.clone());
    if mock.fail.load(Ordering::SeqCst) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let answer = mock
        .answer
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| "hello".into());
    if value["stream"] == true {
        return ([("content-type","text/event-stream")],format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"id":"chat1","created":123,"model":value["model"],"choices":[{"index":0,"delta":{"role":"assistant","content":answer},"finish_reason":null}]}),
        json!({"id":"chat1","created":123,"model":value["model"],"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}))).into_response();
    }
    Json(json!({"id":"chat1","created":123,"object":"chat.completion","model":value["model"],"choices":[{"index":0,"message":{"role":"assistant","content":answer},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}})).into_response()
}
async fn retrieve(
    Path(id): Path<String>,
    State(mock): State<Mock>,
    req: Request<Body>,
) -> Response {
    mock.requests.lock().unwrap().push(json!({"_method":"get","_id":id,"_query":req.uri().query(),"_auth":req.headers()["authorization"].to_str().unwrap()}));
    let mut result = native(&id, "physical", "hello");
    if req
        .uri()
        .query()
        .is_some_and(|q| q.contains("encrypted_content"))
    {
        result["output"].as_array_mut().unwrap().insert(
            0,
            json!({
                "id": format!("rs_retrieved_{id}"), "type": "reasoning", "summary": [],
                "encrypted_content": "retrieved-provider-state"
            }),
        );
    }
    Json(result).into_response()
}
async fn delete(Path(id): Path<String>, State(mock): State<Mock>) -> Response {
    mock.requests
        .lock()
        .unwrap()
        .push(json!({"_method":"delete","_id":id}));
    StatusCode::NO_CONTENT.into_response()
}
async fn cancel(Path(id): Path<String>) -> Response {
    let mut value = native(&id, "physical", "hello");
    value["status"] = json!("cancelled");
    Json(value).into_response()
}
async fn items(Path(_id): Path<String>, req: Request<Body>) -> Response {
    Json(json!({"object":"list","data":[{"id":"item_1","type":"message","role":"user","content":[]}],"has_more":false,"first_id":"item_1","last_id":"item_1","query":req.uri().query()})).into_response()
}
async fn compact_mock(State(mock): State<Mock>, Json(_raw): Json<Value>) -> Response {
    let id = mock.next.fetch_add(1, Ordering::SeqCst);
    Json(json!({"id":format!("compact_{id}"),"object":"response.compaction","created_at":123,"output":[{"id":format!("cmp_{id}"),"type":"compaction","encrypted_content":"compacted-provider-state"}]})).into_response()
}
async fn tokens(State(mock): State<Mock>, req: Request<Body>) -> Response {
    let value: Value =
        serde_json::from_slice(&to_bytes(req.into_body(), 1024 * 1024).await.unwrap()).unwrap();
    mock.requests.lock().unwrap().push(value);
    Json(json!({"object":"response.input_tokens","input_tokens":17})).into_response()
}
async fn server() -> (String, Mock) {
    let mock = Mock::default();
    let routes = Router::new()
        .route("/v1/chat/completions", post(chat))
        .route("/v1/responses", post(create_native))
        .route("/v1/responses/compact", post(compact_mock))
        .route("/v1/responses/input_tokens", post(tokens))
        .route("/v1/responses/{id}", get(retrieve).delete(delete))
        .route("/v1/responses/{id}/cancel", post(cancel))
        .route("/v1/responses/{id}/input_items", get(items))
        .with_state(mock.clone());
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(socket, routes).await.unwrap();
    });
    (format!("http://{address}"), mock)
}
fn gateway(url: &str, protocol: &str, stream_only: bool) -> Router {
    let protocols = match protocol {
        "native" => "responses_protocol: native\n    chat_protocol: responses",
        "dual" => "responses_protocol: native",
        _ => "responses_protocol: chat",
    };
    let yaml = format!(
        "providers:\n  - name: upstream\n    type: openai\n    base_url: {url}\n    key_envs: [K1, K2]\n    {protocols}\n    stream_only: {stream_only}\nchains:\n  - name: demo\n    steps:\n      - provider: upstream\n        model: physical\n"
    );
    app(AppState::new(
        serde_yaml::from_str::<Config>(&yaml).unwrap(),
        HashMap::from([
            ("K1".into(), "first".into()),
            ("K2".into(), "second".into()),
        ]),
        vec!["client".into(), "other".into()],
    )
    .unwrap())
}
fn req(path: &str, value: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("authorization", "Bearer client")
        .header("content-type", "application/json")
        .body(Body::from(value.to_string()))
        .unwrap()
}
async fn json_body(response: Response) -> Value {
    let bytes = to_bytes(response.into_body(), 32 * 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn protocol_matrix_both_clients_three_provider_protocols_and_all_stream_modes() {
    for protocol in ["chat", "native", "dual"] {
        for stream_only in [false, true] {
            for stream in [false, true] {
                for endpoint in ["chat/completions", "responses"] {
                    let (url, mock) = server().await;
                    let app = gateway(&url, protocol, stream_only);
                    let mut value = json!({"model":"demo","stream":stream});
                    if endpoint == "responses" {
                        value["input"] = json!("hi");
                        value["store"] = json!(false);
                    } else {
                        value["messages"] = json!([{"role":"user","content":"hi"}]);
                    }
                    let response = app
                        .oneshot(req(&format!("/v1/{endpoint}"), value))
                        .await
                        .unwrap();
                    assert_eq!(
                        response.status(),
                        StatusCode::OK,
                        "{protocol} {stream_only} {stream} {endpoint}"
                    );
                    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
                    let text = String::from_utf8_lossy(&bytes);
                    if stream {
                        assert!(
                            text.contains(if endpoint == "responses" {
                                "response.completed"
                            } else {
                                "[DONE]"
                            }),
                            "{text}"
                        );
                        assert!(!text.contains("upstream_stream_incomplete"), "{text}");
                    } else {
                        let value: Value = serde_json::from_slice(&bytes).unwrap();
                        assert_eq!(
                            value["object"],
                            if endpoint == "responses" {
                                "response"
                            } else {
                                "chat.completion"
                            }
                        );
                    }
                    assert_eq!(mock.requests.lock().unwrap().len(), 1);
                }
            }
        }
    }
}

#[tokio::test]
async fn state_lifecycle_is_authenticated_pinned_to_account_and_deleted_on_real_success() {
    let (url, mock) = server().await;
    let app = gateway(&url, "native", false);
    let created = json_body(
        app.clone()
            .oneshot(req(
                "/v1/responses",
                json!({"model":"demo","input":"hi","store":true,"background":true}),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(created["status"], "queued");
    let id = created["id"].as_str().unwrap();
    let response = app
        .clone()
        .oneshot(req(
            "/v1/responses",
            json!({"model":"demo","input":"next","previous_response_id":id}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(mock.requests.lock().unwrap()[1]["_auth"], "Bearer first");
    for suffix in ["", "/input_items?limit=1&order=asc&after=item_0", "/cancel"] {
        let method = if suffix == "/cancel" { "POST" } else { "GET" };
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(format!("/v1/responses/{id}{suffix}"))
                    .header("authorization", "Bearer client")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value = json_body(response).await;
        if suffix.is_empty() {
            assert_eq!(value["model"], "demo");
        }
        if suffix.starts_with("/input_items") {
            assert!(value["query"].as_str().unwrap().contains("limit=1"));
        }
    }
    for token in [None, Some("other")] {
        let mut request = Request::builder().uri(format!("/v1/responses/{id}"));
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let result = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            result.status(),
            if token.is_none() {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::NOT_FOUND
            }
        );
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v1/responses/{id}"))
                .header("authorization", "Bearer client")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let count = mock.requests.lock().unwrap().len();
    let response = app
        .oneshot(req(
            "/v1/responses",
            json!({"model":"demo","input":"next","previous_response_id":id}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(mock.requests.lock().unwrap().len(), count);
}

#[tokio::test]
async fn background_retrieval_registers_new_encrypted_items_on_the_original_account() {
    let (url, mock) = server().await;
    let app = configured(&format!(
        "providers:\n  - name: p\n    type: openai\n    base_url: {url}\n    key_envs: [K1, K2]\n    responses_protocol: native\nchains:\n  - name: demo\n    steps:\n      - provider: p\n        model: physical\n"
    ));
    let created = json_body(
        app.clone()
            .oneshot(req(
                "/v1/responses",
                json!({
                    "model":"demo", "input":"hi", "store":true, "background":true
                }),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(created["status"], "queued");
    let id = created["id"].as_str().unwrap();
    let retrieved = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/responses/{id}?include%5B%5D=reasoning.encrypted_content"
                ))
                .header("authorization", "Bearer client")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(retrieved.status(), StatusCode::OK);
    let retrieved = json_body(retrieved).await;
    let replay = app
        .oneshot(req(
            "/v1/responses",
            json!({
                "model":"demo", "store":false, "input":[retrieved["output"][0].clone()]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for request in requests.iter() {
        assert_eq!(request["_auth"], "Bearer first");
    }
}

#[tokio::test]
async fn unknown_state_conflicts_and_chat_only_native_features_fail_before_network() {
    let (url, mock) = server().await;
    let app = gateway(&url, "chat", false);
    for (value, status) in [
        (
            json!({"previous_response_id":"external"}),
            StatusCode::NOT_FOUND,
        ),
        (
            json!({"previous_response_id":"x","conversation":"y"}),
            StatusCode::BAD_REQUEST,
        ),
        (json!({"store":true}), StatusCode::NOT_IMPLEMENTED),
        (
            json!({"tools":[{"type":"web_search"}]}),
            StatusCode::NOT_IMPLEMENTED,
        ),
        (
            json!({"context_management":[{"type":"compaction"}]}),
            StatusCode::NOT_IMPLEMENTED,
        ),
    ] {
        let mut input = json!({"model":"demo","input":"hi"});
        input
            .as_object_mut()
            .unwrap()
            .extend(value.as_object().unwrap().clone());
        let response = app
            .clone()
            .oneshot(req("/v1/responses", input))
            .await
            .unwrap();
        assert_eq!(response.status(), status);
    }
    assert!(mock.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn exact_input_tokens_use_native_route_and_chat_only_never_guesses() {
    for protocol in ["native", "chat"] {
        let (url, mock) = server().await;
        let app = gateway(&url, protocol, false);
        let response = app
            .oneshot(req(
                "/v1/responses/input_tokens",
                json!({"model":"demo","input":"hi"}),
            ))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if protocol == "native" {
                StatusCode::OK
            } else {
                StatusCode::NOT_IMPLEMENTED
            }
        );
        if protocol == "native" {
            assert_eq!(json_body(response).await["input_tokens"], 17);
            assert_eq!(mock.requests.lock().unwrap()[0]["model"], "physical");
        } else {
            assert!(mock.requests.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn reverse_bridge_preserves_tools_images_formats_and_rejects_unsupported_controls() {
    let (url, mock) = server().await;
    let app = gateway(&url, "native", false);
    let value = json!({"model":"demo","messages":[{"role":"system","content":"rules"},{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"https://example.org/a.png","detail":"high"}}]},
        {"role":"assistant","content":null,"tool_calls":[{"id":"call1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},
        {"role":"tool","tool_call_id":"call1","content":"answer"}],
        "tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"},"strict":true}}],"tool_choice":"auto","max_completion_tokens":32,"max_tokens":64,
        "response_format":{"type":"text"},"reasoning_effort":"low"});
    assert_eq!(
        app.clone()
            .oneshot(req("/v1/chat/completions", value))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let wire = mock.requests.lock().unwrap()[0].clone();
    assert_eq!(wire["max_output_tokens"], 32);
    assert_eq!(wire["input"][1]["content"][1]["type"], "input_image");
    assert_eq!(wire["input"][2]["type"], "function_call");
    assert_eq!(wire["input"][3]["type"], "function_call_output");
    assert_eq!(wire["tools"][0]["strict"], true);
    for extra in [
        json!({"n":2}),
        json!({"logprobs":true}),
        json!({"stop":["x"]}),
        json!({"audio":{"voice":"alloy"}}),
    ] {
        let mut value = json!({"model":"demo","messages":[{"role":"user","content":"hi"}]});
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert_eq!(
            app.clone()
                .oneshot(req("/v1/chat/completions", value))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_IMPLEMENTED
        );
    }
    assert_eq!(mock.requests.lock().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Requires official SDKs; run explicitly in CI after installing pinned versions"]
async fn official_python_and_node_sdk_roundtrips() {
    let (url, _mock) = server().await;
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(socket, gateway(&url, "native", false))
            .await
            .unwrap();
    });
    let output = tokio::process::Command::new(
        std::env::var("GUVNA_SDK_PYTHON").expect("SDK Python interpreter"),
    )
    .arg("scripts/sdk_smoke.py")
    .arg("--base-url")
    .arg(format!("http://{address}/v1"))
    .arg("--node-package")
    .arg(std::env::var("GUVNA_SDK_NODE_PACKAGE").expect("Node SDK package path"))
    .output()
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("{}", String::from_utf8_lossy(&output.stdout));
}

fn configured(yaml: &str) -> Router {
    app(AppState::new(
        serde_yaml::from_str::<Config>(yaml).unwrap(),
        HashMap::from([
            ("K1".into(), "first".into()),
            ("K2".into(), "second".into()),
        ]),
        vec!["client".into()],
    )
    .unwrap())
}

#[tokio::test]
async fn same_provider_models_have_independent_protocol_and_capability_contracts() {
    let (url, mock) = server().await;
    let yaml = format!(
        "providers:\n  - name: p\n    type: openai\n    base_url: {url}\n    key_env: K1\n    model_capabilities:\n      re:\n        chat_protocol: responses\n        responses_protocol: native\n        supports_images: true\n      text:\n        supports_images: false\n        context_tokens: 4096\nchains:\n  - name: demo\n    steps:\n      - provider: p\n        model: text\n      - provider: p\n        model: re\nauto:\n  enabled: true\n  candidates:\n    - provider: p\n      model: text\n      priority: 1\n    - provider: p\n      model: re\n      priority: 2\n"
    );
    let app = configured(&yaml);
    for route in ["/v1/chat/completions", "/v1/auto", "/v1/responses"] {
        let value = if route.ends_with("responses") {
            json!({"model":"demo","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.org/i.png"}]}]})
        } else {
            json!({"model":if route.ends_with("auto"){"auto"}else{"demo"},"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.org/i.png"}}]}]})
        };
        assert_eq!(
            app.clone()
                .oneshot(req(route, value))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for wire in requests.iter() {
        assert_eq!(wire["model"], "re");
        assert!(wire.get("input").is_some());
        assert!(wire.get("messages").is_none());
    }
}

#[tokio::test]
async fn native_defaults_map_aliases_and_caller_controls_win() {
    let (url, mock) = server().await;
    let app = configured(&format!(
        "providers:\n  - name: p\n    type: openai\n    base_url: {url}\n    key_env: K1\n    responses_protocol: native\nchains:\n  - name: demo\n    steps:\n      - provider: p\n        model: re\n        params:\n          max_completion_tokens: 64\n          reasoning_effort: low\n          response_format: {{type: text}}\n          verbosity: low\n"
    ));
    for controls in [
        json!({}),
        json!({"max_output_tokens":32,"text":{"format":{"type":"text"},"verbosity":"high"},"reasoning":{"effort":"high"}}),
    ] {
        let mut value = json!({"model":"demo","input":"hi","store":false});
        value
            .as_object_mut()
            .unwrap()
            .extend(controls.as_object().unwrap().clone());
        assert_eq!(
            app.clone()
                .oneshot(req("/v1/responses", value))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
    let seen = mock.requests.lock().unwrap();
    assert_eq!(seen[0]["max_output_tokens"], 64);
    assert_eq!(seen[0]["text"]["verbosity"], "low");
    assert!(seen[0].get("max_completion_tokens").is_none());
    assert_eq!(seen[1]["max_output_tokens"], 32);
    assert_eq!(seen[1]["reasoning"]["effort"], "high");
    assert_eq!(seen[1]["text"]["verbosity"], "high");
}

#[tokio::test]
async fn mixed_protocols_fallback_once_and_native_features_cannot_escape_selected_chain() {
    let (native_url, native) = server().await;
    native.fail.store(true, Ordering::SeqCst);
    let (chat_url, chat) = server().await;
    let app = configured(&format!(
        "providers:\n  - name: re\n    type: openai\n    base_url: {native_url}\n    key_env: K1\n    responses_protocol: native\n    chat_protocol: responses\n  - name: chat\n    type: openai\n    base_url: {chat_url}\n    key_env: K2\nchains:\n  - name: demo\n    steps:\n      - provider: re\n        model: re\n      - provider: chat\n        model: chat\n  - name: chat_only\n    steps:\n      - provider: chat\n        model: chat\n"
    ));
    for stream in [false, true] {
        for endpoint in ["chat/completions", "responses"] {
            let value = if endpoint == "responses" {
                json!({"model":"demo","input":"hi","stream":stream})
            } else {
                json!({"model":"demo","messages":[{"role":"user","content":"hi"}],"stream":stream})
            };
            let response = app
                .clone()
                .oneshot(req(&format!("/v1/{endpoint}"), value))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains("upstream_stream_incomplete"));
        }
    }
    assert_eq!(native.requests.lock().unwrap().len(), 4);
    assert_eq!(chat.requests.lock().unwrap().len(), 4);
    let result = app
        .oneshot(req(
            "/v1/responses",
            json!({"model":"chat_only","input":"hi","tools":[{"type":"web_search"}]}),
        ))
        .await
        .unwrap();
    assert_eq!(result.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(chat.requests.lock().unwrap().len(), 4);
    assert_eq!(native.requests.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn stateful_failure_cannot_fall_back_to_another_provider_or_credential() {
    let (first_url, first) = server().await;
    let (second_url, second) = server().await;
    let yaml = format!(
        "providers:\n  - name: first\n    type: openai\n    base_url: {first_url}\n    key_envs: [K1, K2]\n    responses_protocol: native\n  - name: second\n    type: openai\n    base_url: {second_url}\n    key_env: K1\n    responses_protocol: native\nchains:\n  - name: demo\n    steps:\n      - provider: first\n        model: first\n      - provider: second\n        model: second\n"
    );
    let app = configured(&yaml);
    let result = json_body(
        app.clone()
            .oneshot(req(
                "/v1/responses",
                json!({"model":"demo","input":"hi","store":true}),
            ))
            .await
            .unwrap(),
    )
    .await;
    let previous = result["id"].as_str().unwrap();
    first.fail.store(true, Ordering::SeqCst);
    let response = app
        .oneshot(req(
            "/v1/responses",
            json!({"model":"demo","input":"next","previous_response_id":previous}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(first.requests.lock().unwrap().len(), 2);
    assert_eq!(first.requests.lock().unwrap()[1]["_auth"], "Bearer first");
    assert!(second.requests.lock().unwrap().is_empty());
    let restarted = configured(&yaml);
    let response = restarted
        .oneshot(req(
            "/v1/responses",
            json!({"model":"demo","input":"next","previous_response_id":previous}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

async fn fixed_native(json: Value, events: String) -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let app = Router::new().route(
        "/v1/responses",
        post(move |Json(value): Json<Value>| {
            let events = events.clone();
            let json = json.clone();
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                if value["stream"] == true {
                    ([("content-type", "text/event-stream")], events).into_response()
                } else {
                    Json(json).into_response()
                }
            }
        }),
    );
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(socket, app).await.unwrap();
    });
    (format!("http://{address}"), calls)
}

#[tokio::test]
async fn native_stream_failure_is_preserved_without_false_success_or_state_pollution() {
    for failed in [false, true] {
        let created = frame(
            "response.created",
            0,
            json!({"response":{"id":"resp_failed","object":"response","created_at":123}}),
        );
        let end = if failed {
            frame(
                "response.failed",
                1,
                json!({"response":{"id":"resp_failed","object":"response","status":"failed","output":[],"error":{"code":"server_error","message":"failed"}}}),
            )
        } else {
            String::new()
        };
        let (url, calls) = fixed_native(Value::Null, created + &end).await;
        let app = gateway(&url, "native", false);
        let response = app
            .clone()
            .oneshot(req(
                "/v1/responses",
                json!({"model":"demo","input":"hi","stream":true,"store":true}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let text = String::from_utf8(
            to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(!text.contains("response.completed"));
        assert!(text.contains(if failed {
            "response.failed"
        } else {
            "upstream_stream_incomplete"
        }));
        let metrics = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header("authorization", "Bearer client")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let metrics = String::from_utf8(
            to_bytes(metrics.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(
            metrics.contains("guvna_upstream_success_total 0"),
            "{metrics}"
        );
        let response = app
            .oneshot(req(
                "/v1/responses",
                json!({"model":"demo","input":"next","previous_response_id":"resp_failed"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn reverse_bridge_streams_functions_refusals_and_incomplete_status() {
    for mode in ["tool", "refusal", "incomplete"] {
        let mut response = native("resp_result", "physical", "");
        let (item, delta) = if mode == "refusal" {
            (
                json!({"type":"message","role":"assistant","content":[{"type":"refusal","refusal":"declined"}]}),
                frame(
                    "response.refusal.delta",
                    2,
                    json!({"output_index":0,"delta":"declined"}),
                ),
            )
        } else {
            let args = if mode == "tool" { "{}" } else { "{" };
            (
                json!({"type":"function_call","id":"fc1","call_id":"call1","name":"lookup","arguments":args}),
                frame(
                    "response.function_call_arguments.delta",
                    2,
                    json!({"output_index":0,"delta":args}),
                ),
            )
        };
        response["output"] = json!([item]);
        if mode == "incomplete" {
            response["status"] = json!("incomplete");
            response["incomplete_details"] = json!({"reason":"max_output_tokens"});
        }
        let mut added = item.clone();
        if mode != "refusal" {
            added["arguments"] = json!("");
        }
        let events = frame(
            "response.created",
            0,
            json!({"response":{"id":"resp_result","object":"response","created_at":123}}),
        ) + &frame(
            "response.output_item.added",
            1,
            json!({"output_index":0,"item":added}),
        ) + &delta
            + &frame(
                if mode == "incomplete" {
                    "response.incomplete"
                } else {
                    "response.completed"
                },
                3,
                json!({"response":response}),
            );
        let (url, _) = fixed_native(response, events).await;
        let app = gateway(&url, "native", false);
        for stream in [false, true] {
            let response=app.clone().oneshot(req("/v1/chat/completions",json!({"model":"demo","messages":[{"role":"user","content":"hi"}],"stream":stream}))).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let text = String::from_utf8_lossy(&bytes);
            assert!(
                !text.contains("upstream_stream_incomplete"),
                "{mode} {text}"
            );
            assert!(
                text.contains(match mode {
                    "refusal" => "declined",
                    "incomplete" => "length",
                    _ => "tool_calls",
                }),
                "{text}"
            );
            if stream {
                assert!(text.contains("[DONE]"));
            }
        }
    }
}

#[tokio::test]
async fn encrypted_stateless_replay_and_conversation_preserve_original_account() {
    let (url, mock) = server().await;
    let app = gateway(&url, "native", false);
    let first = json_body(
        app.clone()
            .oneshot(req(
                "/v1/responses",
                json!({"model":"demo","input":"encrypted","store":false}),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        first["output"][0]["encrypted_content"],
        "opaque-provider-state"
    );
    let mut replay = first["output"].as_array().unwrap().clone();
    replay.push(json!({"role":"user","content":"next"}));
    let response = app
        .clone()
        .oneshot(req(
            "/v1/responses",
            json!({"model":"demo","input":replay,"store":false}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    {
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests[0]["_auth"], requests[1]["_auth"]);
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/responses/{}", first["id"].as_str().unwrap()))
                .header("authorization", "Bearer client")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let created = json_body(
        app.clone()
            .oneshot(req(
                "/v1/responses",
                json!({"model":"demo","input":"conversation"}),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(created["conversation"]["id"], "conv_1");
    for conversation in [json!("conv_1"), json!({"id":"conv_1"})] {
        let response = app
            .clone()
            .oneshot(req(
                "/v1/responses",
                json!({"model":"demo","input":"next","conversation":conversation}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let before = mock.requests.lock().unwrap().len();
    let response=app.oneshot(req("/v1/responses",json!({"model":"demo","input":[{"id":"foreign","type":"reasoning","summary":[],"encrypted_content":"foreign-state"}]}))).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(mock.requests.lock().unwrap().len(), before);
}

#[tokio::test]
async fn files_are_verified_on_same_account_before_native_generation() {
    let hits = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen = hits.clone();
    let app = Router::new().route(
        "/v1/files/{id}",
        get(move |Path(id): Path<String>, request: Request<Body>| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(format!(
                    "file:{}",
                    request.headers()["authorization"].to_str().unwrap()
                ));
                if id == "owned" && request.headers()["authorization"] == "Bearer second" {
                    Json(json!({"id":id,"object":"file","status":"processed"})).into_response()
                } else {
                    StatusCode::NOT_FOUND.into_response()
                }
            }
        }),
    );
    let seen = hits.clone();
    let app = app.route(
        "/v1/responses",
        post(move |request: Request<Body>| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(format!(
                    "generate:{}",
                    request.headers()["authorization"].to_str().unwrap()
                ));
                Json(native("resp_file", "physical", "hello"))
            }
        }),
    );
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(socket, app).await.unwrap();
    });
    let app = gateway(&format!("http://{address}"), "native", false);
    for (id, status) in [
        ("foreign", StatusCode::NOT_IMPLEMENTED),
        ("owned", StatusCode::OK),
    ] {
        let response=app.clone().oneshot(req("/v1/responses",json!({"model":"demo","input":[{"role":"user","content":[{"type":"input_file","file_id":id}]}]}))).await.unwrap();
        assert_eq!(response.status(), status);
    }
    let calls = hits.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .filter(|s| s.starts_with("generate:"))
            .collect::<Vec<_>>(),
        vec![&"generate:Bearer second".to_owned()]
    );
}

#[tokio::test]
async fn optional_auto_decider_can_use_re_only_provider_without_chat_dependency() {
    let (url, mock) = server().await;
    let app = configured(&format!(
        "providers:\n  - name: p\n    type: openai\n    base_url: {url}\n    key_env: K1\n    responses_protocol: native\n    chat_protocol: responses\nauto:\n  enabled: true\n  candidates:\n    - provider: p\n      model: first\n    - provider: p\n      model: second\n  decider:\n    enabled: true\n    provider: p\n    model: selector\n    timeout_ms: 1000\n"
    ));
    let response=app.oneshot(req("/v1/chat/completions",json!({"model":"auto","messages":[{"role":"user","content":"private text never sent to selector"}]}))).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["model"], "selector");
    assert!(!requests[0].to_string().contains("private text"));
    assert_eq!(requests[1]["model"], "second");
    assert!(requests.iter().all(|v| v.get("messages").is_none()));
}

#[tokio::test]
async fn caller_token_alias_prevents_larger_chain_default_on_both_wire_protocols() {
    for native in [false, true] {
        let (url, mock) = server().await;
        let protocol = if native {
            "responses_protocol: native\n    chat_protocol: responses"
        } else {
            "responses_protocol: chat"
        };
        let app = configured(&format!(
            "providers:\n  - name: p\n    type: openai\n    base_url: {url}\n    key_env: K1\n    {protocol}\nchains:\n  - name: demo\n    steps:\n      - provider: p\n        model: physical\n        params:\n          max_completion_tokens: 64000\n"
        ));
        let response = app
            .oneshot(req(
                "/v1/chat/completions",
                json!({"model":"demo","messages":[{"role":"user","content":"hi"}],"max_tokens":32}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let requests = mock.requests.lock().unwrap();
        assert!(requests[0].get("max_completion_tokens").is_none());
        assert_eq!(
            requests[0][if native {
                "max_output_tokens"
            } else {
                "max_tokens"
            }],
            32
        );
    }
}

#[tokio::test]
async fn native_json_failed_status_is_forwarded_without_success_or_retry() {
    let mut failed = native("resp_failed_json", "physical", "");
    failed["status"] = json!("failed");
    failed["error"] = json!({"code":"server_error","message":"upstream failed"});
    let (url, calls) = fixed_native(failed, String::new()).await;
    let app = gateway(&url, "native", false);
    let response = app
        .clone()
        .oneshot(req("/v1/responses", json!({"model":"demo","input":"hi"})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value = json_body(response).await;
    assert_eq!(value["status"], "failed");
    assert_eq!(value["error"]["code"], "server_error");
    assert_eq!(value["model"], "demo");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let metrics = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", "Bearer client")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&to_bytes(metrics.into_body(), 1024 * 1024).await.unwrap())
            .contains("guvna_upstream_success_total 0")
    );
}

#[tokio::test]
async fn valid_native_output_without_chat_equivalent_is_not_a_provider_failure() {
    let mut value = native("resp_image", "physical", "");
    value["output"] = json!([{"id":"img1","type":"image_generation_call","result":"YQ=="}]);
    let (url, calls) = fixed_native(value, String::new()).await;
    let app = gateway(&url, "native", false);
    let response = app
        .clone()
        .oneshot(req(
            "/v1/chat/completions",
            json!({"model":"demo","messages":[{"role":"user","content":"hi"}]}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let metrics = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", "Bearer client")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let metrics = String::from_utf8(
        to_bytes(metrics.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(
        metrics.contains("guvna_upstream_invalid_responses_total 0"),
        "{metrics}"
    );
    assert!(metrics.contains("guvna_upstream_success_total 0"));
}

#[tokio::test]
async fn all_responses_auxiliary_routes_require_auth_and_unknown_ids_never_probe_upstream() {
    let (url, mock) = server().await;
    let app = gateway(&url, "native", false);
    for (method, path) in [
        ("GET", "/v1/responses/unknown"),
        ("DELETE", "/v1/responses/unknown"),
        ("POST", "/v1/responses/unknown/cancel"),
        ("GET", "/v1/responses/unknown/input_items"),
        ("POST", "/v1/responses/input_tokens"),
        ("POST", "/v1/responses/compact"),
        ("GET", "/v1/responses"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
    }
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/responses/unknown")
                .header("authorization", "Bearer client")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(mock.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn native_extensions_are_forwarded_as_one_original_payload() {
    let (url, mock) = server().await;
    let app = gateway(&url, "native", false);
    let original = json!({"model":"demo","input":"hi","store":false,"include":["reasoning.encrypted_content"],
        "prompt":{"id":"prompt_external","version":"1","variables":{"x":"y"}},
        "context_management":[{"type":"compaction","compact_threshold":2048}],
        "reasoning":{"effort":"high","context":"all_turns","summary":"auto"},
        "tools":[{"type":"mcp","server_label":"local_test","server_url":"https://example.org/mcp","require_approval":"always"}],
        "provider_extension":{"nested":[true,1,"kept"]}});
    let response = app
        .oneshot(req("/v1/responses", original.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut wire = mock.requests.lock().unwrap()[0].clone();
    wire["model"] = json!("demo");
    wire.as_object_mut().unwrap().remove("_auth");
    assert_eq!(wire, original);
}

#[tokio::test]
async fn native_sse_identity_is_registered_after_terminal_and_cancelled_body_leaves_no_binding() {
    let (url, _mock) = server().await;
    let app = gateway(&url, "native", false);
    let response = app
        .clone()
        .oneshot(req(
            "/v1/responses",
            json!({"model":"demo","input":"hi","store":true,"stream":true}),
        ))
        .await
        .unwrap();
    let text = String::from_utf8(
        to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(text.contains("\"model\":\"demo\""));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/responses/resp_0")
                .header("authorization", "Bearer client")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let created = frame(
        "response.created",
        0,
        json!({"response":{"id":"resp_cancelled","object":"response","created_at":123}}),
    );
    let (url, _) = fixed_native(Value::Null, created).await;
    let app = gateway(&url, "native", false);
    let response = app
        .clone()
        .oneshot(req(
            "/v1/responses",
            json!({"model":"demo","input":"hi","store":true,"stream":true}),
        ))
        .await
        .unwrap();
    drop(response);
    let response = app
        .oneshot(req(
            "/v1/responses",
            json!({"model":"demo","input":"next","previous_response_id":"resp_cancelled"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn real_schema_mismatch_falls_back_for_all_json_and_stream_protocols() {
    for native in [false, true] {
        for stream in [false, true] {
            for endpoint in ["chat/completions", "responses"] {
                let (bad_url, bad) = server().await;
                let (good_url, good) = server().await;
                *bad.answer.lock().unwrap() = Some(r#"{"n":"2"}"#.into());
                *good.answer.lock().unwrap() = Some(r#"{"n":2}"#.into());
                let protocols = if native {
                    "responses_protocol: native\n    chat_protocol: responses"
                } else {
                    "responses_protocol: chat"
                };
                let app = configured(&format!(
                    "providers:\n  - name: a\n    type: openai\n    base_url: {bad_url}\n    key_env: K1\n    {protocols}\n  - name: b\n    type: openai\n    base_url: {good_url}\n    key_env: K2\n    {protocols}\nchains:\n  - name: demo\n    steps:\n      - provider: a\n        model: physical-a\n      - provider: b\n        model: physical-b\n"
                ));
                let schema = json!({"type":"object","properties":{"n":{"type":"integer","minimum":2}},"required":["n"],"additionalProperties":false});
                let value = if endpoint == "responses" {
                    json!({"model":"demo","input":"hi","store":false,"stream":stream,"text":{"format":{"type":"json_schema","name":"result","strict":true,"schema":schema}}})
                } else {
                    json!({"model":"demo","messages":[{"role":"user","content":"hi"}],"stream":stream,"response_format":{"type":"json_schema","json_schema":{"name":"result","strict":true,"schema":schema}}})
                };
                let response = app
                    .oneshot(req(&format!("/v1/{endpoint}"), value))
                    .await
                    .unwrap();
                assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "{native} {stream} {endpoint}"
                );
                let text = String::from_utf8(
                    to_bytes(response.into_body(), 1024 * 1024)
                        .await
                        .unwrap()
                        .to_vec(),
                )
                .unwrap();
                assert!(!text.contains("upstream_stream_incomplete"), "{text}");
                assert_eq!(bad.requests.lock().unwrap().len(), 1);
                assert_eq!(good.requests.lock().unwrap().len(), 1);
            }
        }
    }
}

#[tokio::test]
async fn malformed_or_external_output_schema_is_rejected_before_upstream() {
    let (url, mock) = server().await;
    let app = gateway(&url, "native", false);
    for schema in [
        json!({"type":42}),
        json!({"$ref":"https://example.org/schema.json"}),
        json!({"$ref":"file:///etc/passwd"}),
    ] {
        let response=app.clone().oneshot(req("/v1/responses",json!({"model":"demo","input":"hi","text":{"format":{"type":"json_schema","name":"x","strict":true,"schema":schema}}}))).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert!(mock.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn schema_slots_are_held_by_live_streams_and_released_on_cancel() {
    for protocol in ["chat", "native"] {
        let (url, mock) = server().await;
        *mock.answer.lock().unwrap() = Some(r#"{"n":2}"#.into());
        let app = gateway(&url, protocol, false);
        let value = json!({"model":"demo","messages":[{"role":"user","content":"hi"}],"stream":true,
            "response_format":{"type":"json_schema","json_schema":{"name":"result","strict":true,"schema":{"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}}}});
        let mut responses = Vec::new();
        for _ in 0..4 {
            let response = app
                .clone()
                .oneshot(req("/v1/chat/completions", value.clone()))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            responses.push(response);
        }
        let busy = app
            .clone()
            .oneshot(req("/v1/chat/completions", value.clone()))
            .await
            .unwrap();
        assert_eq!(busy.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mock.requests.lock().unwrap().len(), 4);
        drop(responses.pop());
        let recovered = app
            .oneshot(req("/v1/chat/completions", value))
            .await
            .unwrap();
        assert_eq!(recovered.status(), StatusCode::OK);
        assert_eq!(mock.requests.lock().unwrap().len(), 5);
    }
}
