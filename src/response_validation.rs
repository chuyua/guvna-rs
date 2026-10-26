//! Strict, endpoint-specific 2xx upstream payload checks.
//! No routing, retry, quarantine, metrics or HTTP actions happen here.
use super::dispatch::Endpoint;
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use std::collections::{BTreeMap, HashSet};

#[cfg(test)]
pub(super) fn valid_result(endpoint: Endpoint, request: &Value, response: &Value) -> bool {
    valid_result_with_params(endpoint, request, &BTreeMap::new(), response)
}

pub(super) fn valid_result_with_params(
    endpoint: Endpoint,
    request: &Value,
    params: &BTreeMap<String, Value>,
    response: &Value,
) -> bool {
    match endpoint {
        Endpoint::Chat => validate_chat_with_params(request, params, response),
        Endpoint::Responses => {
            validate_chat_with_params(request, params, response)
                && super::responses::translatable_chat_result(response)
        }
        Endpoint::Embeddings => validate_embeddings_with_params(request, params, response),
    }
}

// Request fields win over chain defaults, matching the dispatch payload merge
// without cloning a potentially large request merely to validate a response.
fn request_value<'a>(
    request: &'a Value,
    params: &'a BTreeMap<String, Value>,
    name: &str,
) -> Option<&'a Value> {
    request.get(name).or_else(|| params.get(name))
}

// A result is only usable when every choice completed; a partial 2xx is NOT success.
#[cfg(test)]
fn validate_chat(request: &Value, response: &Value) -> bool {
    validate_chat_with_params(request, &BTreeMap::new(), response)
}

fn validate_chat_with_params(
    request: &Value,
    params: &BTreeMap<String, Value>,
    response: &Value,
) -> bool {
    let Some(choices) = response.get("choices").and_then(Value::as_array) else {
        return false;
    };
    if choices.is_empty() {
        return false;
    }
    let expected = request_value(request, params, "n")
        .and_then(Value::as_u64)
        .unwrap_or(1);
    if choices.len() as u64 != expected {
        return false;
    }
    let mut indices = HashSet::new();
    let mut tool_call_ids = HashSet::new();
    let tool_choice = request_value(request, params, "tool_choice");
    let required_name = tool_choice
        .and_then(|choice| choice.pointer("/function/name"))
        .and_then(Value::as_str);
    for choice in choices {
        let index = match choice.get("index") {
            Some(index) => match index.as_u64() {
                Some(x) => x,
                None => return false,
            },
            None if choices.len() == 1 => 0,
            None => return false,
        };
        if index >= choices.len() as u64 || !indices.insert(index) {
            return false;
        }
        let Some(message) = choice.get("message").and_then(Value::as_object) else {
            return false;
        };
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            return false;
        }
        if !choice
            .get("finish_reason")
            .and_then(Value::as_str)
            .is_some_and(|x| ["stop", "length", "tool_calls", "content_filter"].contains(&x))
        {
            return false;
        }
        let end_reason = choice.get("finish_reason").and_then(Value::as_str);
        if matches!(end_reason, Some("stop" | "length"))
            && !message
                .get("content")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
            && !message
                .get("refusal")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
            && !message
                .get("tool_calls")
                .and_then(Value::as_array)
                .is_some_and(|c| !c.is_empty())
            && !message
                .get("reasoning")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
            && !message
                .get("reasoning_content")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
        {
            return false;
        }
        // Logprobs are a requested output, not optional metadata that can
        // disappear silently when the upstream ignores the flag. A native
        // JSON fallback must also honor it before becoming a valid success.
        if request_value(request, params, "logprobs") == Some(&Value::Bool(true))
            && !choice.get("logprobs").is_some_and(Value::is_object)
        {
            return false;
        }
        let calls = match message.get("tool_calls") {
            None => None,
            Some(value) => match value.as_array() {
                Some(calls) => Some(calls),
                None => return false,
            },
        };
        // JSON output is an opt-in upstream contract. A provider that ignores
        // response_format and returns ordinary text must not be marked healthy
        // and committed as a completed JSON result. Check before the shared
        // dispatcher accepts the provider so normal failover still works.
        // A max-token truncated output remains incomplete and is not required
        // to be parseable. Tool calls are separate from requested text output.
        if choice.get("finish_reason").and_then(Value::as_str) == Some("stop")
            && let Some(format) = request_value(request, params, "response_format")
                .and_then(|format| format.pointer("/type"))
                .and_then(Value::as_str)
            && matches!(format, "json_object" | "json_schema")
        {
            let valid_json = message
                .get("content")
                .and_then(Value::as_str)
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                .is_some_and(|parsed| format != "json_object" || parsed.is_object());
            let valid_refusal = message
                .get("refusal")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty());
            if !valid_json && !valid_refusal {
                return false;
            }
        }
        let finished_with_tools =
            choice.get("finish_reason").and_then(Value::as_str) == Some("tool_calls");
        let reason = choice.get("finish_reason").and_then(Value::as_str);
        let contains_tools = calls.is_some_and(|calls| !calls.is_empty());
        if (finished_with_tools && !contains_tools) || (reason == Some("stop") && contains_tools) {
            return false;
        }
        if (tool_choice.and_then(Value::as_str) == Some("none") && contains_tools)
            || ((tool_choice.and_then(Value::as_str) == Some("required")
                || required_name.is_some())
                && reason == Some("stop")
                && !finished_with_tools)
        {
            return false;
        }
        if let Some(calls) = calls {
            for call in calls {
                let Some(id) = call
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                else {
                    return false;
                };
                if !tool_call_ids.insert(id) {
                    return false;
                }
                let valid = call.get("type").and_then(Value::as_str) == Some("function")
                    && call
                        .pointer("/function/name")
                        .and_then(Value::as_str)
                        .is_some_and(|name| {
                            !name.is_empty()
                                && required_name.is_none_or(|required| required == name)
                        })
                    && call
                        .pointer("/function/arguments")
                        .and_then(Value::as_str)
                        .is_some_and(|s| {
                            matches!(reason, Some("length" | "content_filter"))
                                || s.is_empty()
                                || serde_json::from_str::<Value>(s)
                                    .is_ok_and(|args| args.is_object())
                        });
                if !valid {
                    return false;
                }
            }
        }
    }
    true
}

// Do not accept duplicate / missing embeddings, mixed dimensions or corrupt vectors.
#[cfg(test)]
fn validate_embeddings(request: &Value, response: &Value) -> bool {
    validate_embeddings_with_params(request, &BTreeMap::new(), response)
}

fn validate_embeddings_with_params(
    request: &Value,
    params: &BTreeMap<String, Value>,
    response: &Value,
) -> bool {
    let expected = match request_value(request, params, "input") {
        Some(Value::String(_)) => 1,
        Some(Value::Array(items)) if !items.is_empty() => {
            if items.iter().all(Value::is_number) {
                1
            } else {
                items.len()
            }
        }
        _ => return false,
    };
    let Some(data) = response.get("data").and_then(Value::as_array) else {
        return false;
    };
    if data.len() != expected {
        return false;
    }
    let desired_dim = request_value(request, params, "dimensions")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0);
    let encoding = request_value(request, params, "encoding_format").and_then(Value::as_str);
    let mut observed_dimension = None;
    let mut observed_format = None;
    let mut indices = HashSet::new();
    for entry in data {
        let index = match entry.get("index") {
            Some(index) => match index.as_u64() {
                Some(x) => x,
                None => return false,
            },
            None if expected == 1 => 0,
            None => return false,
        };
        if index >= expected as u64 || !indices.insert(index) {
            return false;
        }
        let Some(vector) = entry.get("embedding") else {
            return false;
        };
        let (format, dimension) = match vector {
            Value::Array(numbers) if !numbers.is_empty() => {
                if numbers
                    .iter()
                    .all(|n| n.as_f64().is_some_and(f64::is_finite))
                {
                    ("float", numbers.len())
                } else {
                    return false;
                }
            }
            Value::String(data) if !data.is_empty() => {
                use base64::{Engine, engine::general_purpose::STANDARD};
                let Ok(raw) = STANDARD.decode(data) else {
                    return false;
                };
                if raw.is_empty()
                    || raw.len() % 4 != 0
                    || !raw
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .all(|part| f32::from_le_bytes(*part).is_finite())
                {
                    return false;
                }
                ("base64", raw.len() / 4)
            }
            _ => return false,
        };
        if encoding.is_some_and(|e| e == "float" || e == "base64") && encoding != Some(format) {
            return false;
        }
        if desired_dim.is_some_and(|wanted| wanted != dimension) {
            return false;
        }
        if observed_dimension.is_some_and(|previous| previous != dimension)
            || observed_format.is_some_and(|previous| previous != format)
        {
            return false;
        }
        observed_dimension = Some(dimension);
        observed_format = Some(format);
    }
    true
}

#[cfg(test)]
mod response_contract_tests {
    use super::*;

    #[test]
    fn embedding_smoke_contract_stays_with_validator() {
        let request: Value = serde_json::from_str(r#"{"input":"hello"}"#).unwrap();
        assert!(valid_result(
            Endpoint::Embeddings,
            &request,
            &json!({"data":[{"embedding":[0.1,0.2],"index":0}]})
        ));
        assert!(!valid_result(
            Endpoint::Embeddings,
            &request,
            &json!({"data":[{"embedding":[]}]})
        ));
    }

    fn assistant(index: u64) -> Value {
        json!({"index":index,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"})
    }

    #[test]
    fn chat_requires_all_choices_and_unique_indices() {
        let request = json!({"n":2});
        let good = json!({"choices":[assistant(1),assistant(0)]});
        assert!(validate_chat(&request, &good));
        assert!(!validate_chat(&request, &json!({"choices":[assistant(0)]})));
        assert!(!validate_chat(
            &request,
            &json!({"choices":[assistant(0),assistant(0)]})
        ));
        assert!(!validate_chat(
            &request,
            &json!({"choices":[assistant(0),assistant(2)]})
        ));
        assert!(!validate_chat(
            &request,
            &json!({"choices":[{"index":0,"message":{},"finish_reason":null},assistant(1)]})
        ));
    }

    #[test]
    fn chat_logprobs_opt_in_requires_payload_from_upstream() {
        let request = json!({"logprobs": true});
        let missing = json!({"choices":[assistant(0)]});
        assert!(!validate_chat(&request, &missing));
        let mut valid = missing.clone();
        valid["choices"][0]["logprobs"] = json!({
            "content": [{"token":"ok","logprob":-0.02,"bytes":[111,107],"top_logprobs":[]}],
            "refusal": null
        });
        assert!(validate_chat(&request, &valid));
        // The opt-out response must retain preexisting compatibility.
        assert!(validate_chat(&json!({}), &missing));
    }

    #[test]
    fn chat_requires_exactly_one_choice_by_default() {
        let two = json!({"choices":[assistant(0),assistant(1)]});
        assert!(!validate_chat(&json!({}), &two));
        assert!(!validate_chat(&json!({"n":1}), &two));
        assert!(validate_chat(&json!({"n":2}), &two));
    }

    #[test]
    fn validation_reads_chain_defaults_without_cloning_the_request() {
        let request = json!({"model":"fallback"});
        let params = BTreeMap::from([
            ("n".to_owned(), json!(2)),
            ("tool_choice".to_owned(), json!("required")),
        ]);
        let response = json!({"choices":[
            {"index":0,"message":{"role":"assistant","tool_calls":[
                {"id":"call-a","type":"function","function":{"name":"lookup","arguments":"{}"}}
            ]},"finish_reason":"tool_calls"},
            {"index":1,"message":{"role":"assistant","tool_calls":[
                {"id":"call-b","type":"function","function":{"name":"lookup","arguments":"{}"}}
            ]},"finish_reason":"tool_calls"}
        ]});
        assert!(valid_result_with_params(
            Endpoint::Chat,
            &request,
            &params,
            &response
        ));
        assert!(!valid_result_with_params(
            Endpoint::Chat,
            &json!({"model":"fallback","n":1}),
            &params,
            &response,
        ));
    }

    #[test]
    fn chat_tool_finish_must_match_actual_calls() {
        let tool = json!({
            "id":"call_1", "type":"function",
            "function":{"name":"lookup","arguments":"{}"}
        });
        let with_calls = |reason: &str| {
            json!({"choices":[{
                "index":0,
                "message":{"role":"assistant","content":null,"tool_calls":[tool]},
                "finish_reason":reason
            }]})
        };
        let without_calls = |reason: &str| {
            json!({"choices":[{
                "index":0,
                "message":{"role":"assistant","content":"ok"},
                "finish_reason":reason
            }]})
        };
        assert!(validate_chat(&json!({}), &with_calls("tool_calls")));
        assert!(!validate_chat(&json!({}), &with_calls("stop")));
        assert!(!validate_chat(&json!({}), &without_calls("tool_calls")));
        assert!(!validate_chat(
            &json!({}),
            &json!({"choices":[{
                "index":0,
                "message":{"role":"assistant","tool_calls":[]},
                "finish_reason":"tool_calls"
            }]})
        ));
    }

    #[test]
    fn truncated_required_tools_and_zero_argument_calls_remain_deliverable() {
        let incomplete = json!({"choices":[{"index":0,
            "message":{"role":"assistant","content":"partial"},
            "finish_reason":"length"}]});
        assert!(validate_chat(
            &json!({"tool_choice":"required"}),
            &incomplete
        ));
        let tool = json!({"choices":[{"index":0,
            "message":{"role":"assistant","tool_calls":[{"id":"call_1",
                "type":"function","function":{"name":"empty","arguments":""}}]},
            "finish_reason":"tool_calls"}]});
        assert!(validate_chat(&json!({"tool_choice":"required"}), &tool));
        let mut truncated = tool.clone();
        truncated["choices"][0]["finish_reason"] = json!("length");
        truncated["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"] =
            json!("{unfinished");
        assert!(validate_chat(
            &json!({"tool_choice":"required"}),
            &truncated
        ));
        assert!(!validate_chat(&json!({"tool_choice":"none"}), &truncated));
        assert!(!validate_chat(
            &json!({"tool_choice":"required"}),
            &json!({"choices":[assistant(0)]})
        ));
    }

    #[test]
    fn chat_rejects_unsupported_legacy_function_finish() {
        let response = json!({"choices":[{
            "index":0,"message":{"role":"assistant","content":"ok"},
            "finish_reason":"function_call"
        }]});
        assert!(!validate_chat(&json!({}), &response));
    }

    #[test]
    fn chat_tool_choice_must_be_honored_by_upstream() {
        let tool = |name: &str| {
            json!({"choices":[{
                "index":0,
                "message":{"role":"assistant","content":null,"tool_calls":[{
                    "id":"call_1","type":"function","function":{"name":name,"arguments":"{}"}
                }]},
                "finish_reason":"tool_calls"
            }]})
        };
        let text = json!({"choices":[assistant(0)]});
        let named = json!({"tool_choice":{"type":"function","function":{"name":"lookup"}}});
        assert!(!validate_chat(&json!({"tool_choice":"required"}), &text));
        assert!(!validate_chat(
            &json!({"tool_choice":"none"}),
            &tool("lookup")
        ));
        assert!(!validate_chat(&named, &text));
        assert!(!validate_chat(&named, &tool("other")));
        assert!(validate_chat(&named, &tool("lookup")));
        assert!(validate_chat(
            &json!({"tool_choice":"required"}),
            &tool("lookup")
        ));
        assert!(validate_chat(&json!({"tool_choice":"auto"}), &text));
    }

    #[test]
    fn completed_tool_calls_require_valid_json_arguments() {
        let request = json!({"n":1});
        let item = |args: &str| {
            json!({"choices":[{
                "index":0,"message":{"role":"assistant","tool_calls":[{
                    "id":"call_1","type":"function","function":{"name":"read","arguments":args}
                }]},"finish_reason":"tool_calls"
            }]})
        };
        assert!(validate_chat(&request, &item(r#"{"path":"/tmp"}"#)));
        assert!(!validate_chat(&request, &item(r#"{"path":"#)));
        assert!(!validate_chat(&request, &item("null")));
        assert!(!validate_chat(&request, &item("[]")));
        assert!(!validate_chat(&request, &item("42")));
    }

    #[test]
    fn refuse_semantically_empty_chat_and_accept_one_tokenized_embedding() {
        let empty = json!({"choices":[{"index":0,"message":{"role":"assistant","content":null},
            "finish_reason":"stop"}]});
        assert!(!validate_chat(&json!({}), &empty));
        let refused = json!({"choices":[{"index":0,"message":{"role":"assistant",
            "content":null,"refusal":"Not allowed"},"finish_reason":"stop"}]});
        assert!(validate_chat(&json!({}), &refused));
        let embedding = json!({"data":[{"index":0,"embedding":[0.3,0.5]}]});
        assert!(validate_embeddings(&json!({"input":[1,2,3]}), &embedding));
        assert!(!validate_embeddings(&json!({"input":42}), &embedding));
    }

    #[test]
    fn chat_rejects_missing_or_non_assistant_role() {
        let request = json!({});
        for message in [
            json!({"content":"ok"}),
            json!({"role":"user","content":"ok"}),
            json!({"role":"tool","content":"ok"}),
            json!({"role":null,"content":"ok"}),
        ] {
            let response =
                json!({"choices":[{"index":0,"message":message,"finish_reason":"stop"}]});
            assert!(!validate_chat(&request, &response));
        }
        assert!(validate_chat(&request, &json!({"choices":[assistant(0)]})));
    }

    #[test]
    fn chat_rejects_duplicate_tool_call_ids_across_choices() {
        let tool_choice = |index| {
            json!({
                "index":index,
                "message":{"role":"assistant","tool_calls":[
                    {"id":"call_shared","type":"function","function":{"name":"read","arguments":"{}"}}
                ]},
                "finish_reason":"tool_calls"
            })
        };
        let request = json!({"n":2});
        assert!(!validate_chat(
            &request,
            &json!({"choices":[tool_choice(0),tool_choice(1)]})
        ));
        assert!(validate_chat(
            &request,
            &json!({"choices":[tool_choice(0),assistant(1)]})
        ));
    }

    #[test]
    fn json_output_format_requires_parseable_content_before_provider_success() {
        let chat = |content: &str, reason: &str| {
            json!({"choices":[{"index":0,"message":{"role":"assistant","content":content},
                "finish_reason":reason}]})
        };
        for format in ["json_object", "json_schema"] {
            let request = json!({"response_format":{"type":format}});
            assert!(validate_chat(&request, &chat(r#"{"ok":true}"#, "stop")));
            assert!(!validate_chat(&request, &chat("ordinary prose", "stop")));
            assert!(!validate_chat(&request, &chat("{broken", "stop")));
            // An explicit refusal is not a JSON-format compliance failure.
            assert!(validate_chat(
                &request,
                &json!({"choices":[{"index":0,
                "message":{"role":"assistant","content":null,"refusal":"I cannot help with that"},
                "finish_reason":"stop"}]})
            ));
            // A missing/empty refusal must not bypass JSON validation.
            assert!(!validate_chat(
                &request,
                &json!({"choices":[{"index":0,
                "message":{"role":"assistant","content":"ordinary prose","refusal":""},
                "finish_reason":"stop"}]})
            ));
            // The original meaning of length is incomplete output, not
            // a validation failure requiring replay of truncated tokens.
            assert!(validate_chat(&request, &chat("{", "length")));
        }
        let object_mode = json!({"response_format":{"type":"json_object"}});
        assert!(!validate_chat(&object_mode, &chat("[1,2]", "stop")));
        assert!(!validate_chat(&object_mode, &chat("null", "stop")));
        let schema_mode = json!({"response_format":{"type":"json_schema"}});
        assert!(validate_chat(&schema_mode, &chat("[1,2]", "stop")));
        assert!(validate_chat(&json!({}), &chat("ordinary prose", "stop")));
    }

    #[test]
    fn embeddings_require_complete_unique_finite_vectors_and_equal_dimensions() {
        let request = json!({"input":["a","b"],"dimensions":2,"encoding_format":"float"});
        let good = json!({"data":[
            {"index":1,"embedding":[0.1,0.2]},
            {"index":0,"embedding":[0.3,0.4]}
        ]});
        assert!(validate_embeddings(&request, &good));
        assert!(!validate_embeddings(
            &request,
            &json!({"data":[
                {"index":0,"embedding":[0.1,0.2]},
                {"index":0,"embedding":[0.3,0.4]}
            ]})
        ));
        assert!(!validate_embeddings(
            &request,
            &json!({"data":[
                {"index":0,"embedding":[0.1,0.2]},
                {"index":1,"embedding":[0.3]}
            ]})
        ));
        assert!(!validate_embeddings(
            &request,
            &json!({"data":[
                {"index":0,"embedding":[0.1,0.2]}
            ]})
        ));
    }

    #[test]
    fn base64_embeddings_reject_invalid_payload_and_nonfinite_float32() {
        use base64::{Engine, engine::general_purpose::STANDARD};
        let request = json!({"input":"a","dimensions":1,"encoding_format":"base64"});
        let good = STANDARD.encode(1.25_f32.to_le_bytes());
        assert!(validate_embeddings(
            &request,
            &json!({"data":[{"embedding":good}]})
        ));
        assert!(!validate_embeddings(
            &request,
            &json!({"data":[{"embedding":"not-base64!"}]})
        ));
        let nan = STANDARD.encode(f32::NAN.to_le_bytes());
        assert!(!validate_embeddings(
            &request,
            &json!({"data":[{"embedding":nan}]})
        ));
    }
}
