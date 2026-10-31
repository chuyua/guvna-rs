//! Lossless, deliberately bounded Chat -> Responses compatibility subset.
//! No network, routing, retry or provider accounting is implemented here.
use serde_json::{Value, json};

/// Translate only defaults with equivalent semantics. The caller's native
/// field wins even when its Chat alias appears in the chain configuration.
pub(super) fn native_defaults(
    raw: &mut Value,
    params: &std::collections::BTreeMap<String, Value>,
) -> Result<Vec<(String, Option<Value>)>, ()> {
    // Validate translations before mutating the request, so a rejected
    // candidate cannot leak partially-injected defaults to its fallback.
    let mut chat = json!({"model":"defaults","messages":[{"role":"user","content":"defaults"}]});
    for (key, target) in [
        ("tools", "tools"),
        ("tool_choice", "tool_choice"),
        ("response_format", "text"),
        ("verbosity", "text"),
    ] {
        let supplied = match key {
            "response_format" => raw.pointer("/text/format").is_some(),
            "verbosity" => raw.pointer("/text/verbosity").is_some(),
            _ => raw.get(target).is_some(),
        };
        if !supplied && let Some(value) = params.get(key) {
            chat[key] = value.clone();
        }
    }
    let translated = request(&chat)?;
    let object = raw.as_object_mut().ok_or(())?;
    let mut changes = Vec::new();
    let mut insert = |key: &str, value: Value| {
        if !object.contains_key(key) {
            changes.push((key.into(), None));
            object.insert(key.into(), value);
        }
    };
    if let Some(value) = params
        .get("max_output_tokens")
        .or_else(|| params.get("max_completion_tokens"))
        .or_else(|| params.get("max_tokens"))
    {
        insert("max_output_tokens", value.clone());
    }
    for key in [
        "temperature",
        "top_p",
        "service_tier",
        "parallel_tool_calls",
        "safety_identifier",
        "prompt_cache_key",
        "user",
        "metadata",
    ] {
        if let Some(value) = params.get(key) {
            insert(key, value.clone());
        }
    }
    if let Some(value) = params.get("reasoning_effort") {
        insert("reasoning", json!({"effort":value}));
    }
    for key in ["tools", "tool_choice"] {
        if let Some(value) = translated.get(key) {
            insert(key, value.clone());
        }
    }
    if let Some(text) = translated.get("text").and_then(Value::as_object) {
        let previous = object.get("text").cloned();
        let mut merged = previous.clone().unwrap_or_else(|| json!({}));
        if let Some(map) = merged.as_object_mut() {
            for (key, value) in text {
                map.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        if previous.as_ref() != Some(&merged) {
            changes.push(("text".into(), previous));
            object.insert("text".into(), merged);
        }
    }
    Ok(changes)
}

pub(super) fn request(chat: &Value) -> Result<Value, ()> {
    let object = chat.as_object().ok_or(())?;
    for (key, value) in object {
        if value.is_null() {
            continue;
        }
        if ![
            "model",
            "messages",
            "stream",
            "stream_options",
            "n",
            "max_tokens",
            "max_completion_tokens",
            "temperature",
            "top_p",
            "tools",
            "tool_choice",
            "parallel_tool_calls",
            "response_format",
            "reasoning_effort",
            "verbosity",
            "service_tier",
            "safety_identifier",
            "prompt_cache_key",
            "user",
            "store",
            "metadata",
        ]
        .contains(&key.as_str())
        {
            return Err(());
        }
    }
    if chat
        .get("n")
        .is_some_and(|n| !n.is_null() && n.as_u64() != Some(1))
        || chat.get("store") == Some(&Value::Bool(true))
        || chat.get("stream_options").is_some_and(|v| {
            !v.is_null()
                && v.as_object().is_none_or(|m| {
                    m.iter()
                        .any(|(k, v)| k != "include_usage" || !v.is_boolean())
                })
        })
    {
        return Err(());
    }
    let mut native = json!({"model":chat["model"],"store":false});
    for key in [
        "stream",
        "temperature",
        "top_p",
        "parallel_tool_calls",
        "service_tier",
        "safety_identifier",
        "prompt_cache_key",
        "user",
        "metadata",
    ] {
        if let Some(value) = chat.get(key).filter(|v| !v.is_null()) {
            native[key] = value.clone();
        }
    }
    if let Some(tokens) = chat
        .get("max_completion_tokens")
        .or_else(|| chat.get("max_tokens"))
        .filter(|v| !v.is_null())
    {
        if tokens.as_u64().is_none_or(|v| v < 16) {
            return Err(());
        }
        native["max_output_tokens"] = tokens.clone();
    }
    if let Some(effort) = chat.get("reasoning_effort").filter(|v| !v.is_null()) {
        native["reasoning"] = json!({"effort":effort});
    }
    if let Some(verbosity) = chat.get("verbosity").filter(|v| !v.is_null()) {
        native["text"]["verbosity"] = verbosity.clone();
    }
    if let Some(format) = chat.get("response_format").filter(|v| !v.is_null()) {
        native["text"]["format"] = match format["type"].as_str() {
            Some("text" | "json_object") if format.as_object().is_some_and(|m| m.len() == 1) => {
                format.clone()
            }
            Some("json_schema") if format.as_object().is_some_and(|m| m.len() == 2) => {
                let mut schema = format["json_schema"].as_object().ok_or(())?.clone();
                if schema
                    .keys()
                    .any(|k| !["name", "schema", "strict", "description"].contains(&k.as_str()))
                {
                    return Err(());
                }
                schema.insert("type".into(), json!("json_schema"));
                Value::Object(schema)
            }
            _ => return Err(()),
        };
    }
    if let Some(tools) = chat.get("tools").filter(|v| !v.is_null()) {
        let mut mapped = Vec::new();
        for tool in tools.as_array().ok_or(())? {
            if tool["type"] != "function" || tool.as_object().is_none_or(|m| m.len() != 2) {
                return Err(());
            }
            let mut function = tool["function"].as_object().ok_or(())?.clone();
            if function
                .keys()
                .any(|k| !["name", "description", "parameters", "strict"].contains(&k.as_str()))
            {
                return Err(());
            }
            function.insert("type".into(), json!("function"));
            mapped.push(Value::Object(function));
        }
        native["tools"] = Value::Array(mapped);
    }
    if let Some(choice) = chat.get("tool_choice").filter(|v| !v.is_null()) {
        native["tool_choice"] = if choice.is_string() {
            choice.clone()
        } else if choice["type"] == "function"
            && choice.as_object().is_some_and(|m| m.len() == 2)
            && choice["function"].as_object().is_some_and(|m| m.len() == 1)
        {
            json!({"type":"function","name":choice["function"]["name"]})
        } else {
            return Err(());
        };
    }
    let mut input = Vec::new();
    for message in chat["messages"].as_array().ok_or(())? {
        let role = message["role"].as_str().ok_or(())?;
        if message.as_object().is_none_or(|m| {
            m.keys().any(|k| {
                !["role", "content", "tool_calls", "tool_call_id", "refusal"].contains(&k.as_str())
            })
        }) {
            return Err(());
        }
        if role == "tool" {
            input.push(json!({"type":"function_call_output","call_id":message["tool_call_id"].as_str().ok_or(())?,"output":message["content"].as_str().ok_or(())?}));
            continue;
        }
        if !["system", "developer", "user", "assistant"].contains(&role) {
            return Err(());
        }
        let mut content = Vec::new();
        match &message["content"] {
            Value::String(text) => content.push(json!({"type":if role=="assistant" {"output_text"} else {"input_text"},"text":text})),
            Value::Array(parts) => for part in parts {
                content.push(match part["type"].as_str() {
                    Some("text") if part.as_object().is_some_and(|m|m.len()==2) => json!({"type":if role=="assistant" {"output_text"} else {"input_text"},"text":part["text"].as_str().ok_or(())?}),
                    Some("image_url") if role=="user" && part.as_object().is_some_and(|m|m.len()==2) => {
                        let image=part["image_url"].as_object().ok_or(())?;
                        if image.keys().any(|k|!["url","detail"].contains(&k.as_str())) { return Err(()); }
                        let mut image=json!({"type":"input_image","image_url":image.get("url").and_then(Value::as_str).ok_or(())?});
                        if let Some(detail)=part["image_url"].get("detail") { image["detail"]=detail.clone(); } image
                    }
                    Some("input_audio") if role=="user" && part.as_object().is_some_and(|m|m.len()==2) => json!({"type":"input_audio","input_audio":part["input_audio"]}),
                    Some("file") if role=="user" && part.as_object().is_some_and(|m|m.len()==2) => {
                        let file=part["file"].as_object().ok_or(())?;
                        if file.keys().any(|k|!["file_data","filename"].contains(&k.as_str())) { return Err(()); }
                        let mut file=file.clone(); file.insert("type".into(),json!("input_file")); Value::Object(file)
                    }
                    _=>return Err(()),
                });
            },
            Value::Null => {},
            _=>return Err(()),
        }
        if let Some(refusal) = message.get("refusal").filter(|v| !v.is_null()) {
            if role != "assistant" {
                return Err(());
            }
            content.push(json!({"type":"refusal","refusal":refusal.as_str().ok_or(())?}));
        }
        if !content.is_empty() {
            input.push(json!({"type":"message","role":role,"content":content}));
        }
        if let Some(calls) = message.get("tool_calls").filter(|v| !v.is_null()) {
            if role != "assistant" {
                return Err(());
            }
            for call in calls.as_array().ok_or(())? {
                if call["type"] != "function"
                    || call.as_object().is_none_or(|m| m.len() != 3)
                    || call["function"].as_object().is_none_or(|m| m.len() != 2)
                {
                    return Err(());
                }
                input.push(json!({"type":"function_call","call_id":call["id"].as_str().ok_or(())?,
                    "name":call["function"]["name"].as_str().ok_or(())?,"arguments":call["function"]["arguments"].as_str().ok_or(())?}));
            }
        }
    }
    if input.is_empty() {
        return Err(());
    }
    native["input"] = Value::Array(input);
    Ok(native)
}

pub(super) fn result(native: &Value, model: &str) -> Result<Value, ()> {
    if !super::response_validation::valid_native_responses(native) {
        return Err(());
    }
    let mut content = String::new();
    let mut refusal = String::new();
    let mut reasoning = String::new();
    let mut calls = Vec::new();
    for item in native["output"].as_array().ok_or(())? {
        match item["type"].as_str() {
            Some("message") if item["role"]=="assistant" => for part in item["content"].as_array().ok_or(())? {
                match part["type"].as_str() {
                    Some("output_text") => {
                        // Annotations/logprobs have no equivalent in this bridge.
                        if part.get("annotations").is_some_and(|v|v.as_array().is_none_or(|v|!v.is_empty()))
                            || part.get("logprobs").is_some_and(|v|!v.is_null() && v.as_array().is_none_or(|v|!v.is_empty())) { return Err(()); }
                        content.push_str(part["text"].as_str().ok_or(())?);
                    }
                    Some("refusal") => refusal.push_str(part["refusal"].as_str().ok_or(())?),
                    _=>return Err(()),
                }
            },
            Some("function_call") => calls.push(json!({"id":item["call_id"].as_str().ok_or(())?,"type":"function",
                "function":{"name":item["name"].as_str().ok_or(())?,"arguments":item["arguments"].as_str().ok_or(())?}})),
            Some("reasoning") if item.get("encrypted_content").is_none_or(Value::is_null) => {
                for part in item["summary"].as_array().ok_or(())? { if part["type"]!="summary_text" { return Err(()); } reasoning.push_str(part["text"].as_str().ok_or(())?); }
            }
            _=>return Err(()),
        }
    }
    let finish = if native["status"] == "incomplete" {
        match native["incomplete_details"]["reason"].as_str() {
            Some("max_output_tokens") => "length",
            Some("content_filter") => "content_filter",
            _ => return Err(()),
        }
    } else if !calls.is_empty() {
        "tool_calls"
    } else {
        "stop"
    };
    let mut message = json!({"role":"assistant","content":if content.is_empty() {Value::Null} else {Value::String(content)}});
    if !refusal.is_empty() {
        message["refusal"] = Value::String(refusal);
    }
    if !reasoning.is_empty() {
        message["reasoning_content"] = Value::String(reasoning);
    }
    if !calls.is_empty() {
        message["tool_calls"] = Value::Array(calls);
    }
    let mut chat = json!({"id":native["id"],"object":"chat.completion","created":native["created_at"],"model":model,
        "choices":[{"index":0,"message":message,"finish_reason":finish}]});
    if let Some(usage) = native.get("usage").filter(|v| !v.is_null()) {
        chat["usage"] = json!({"prompt_tokens":usage["input_tokens"],"completion_tokens":usage["output_tokens"],"total_tokens":usage["total_tokens"]});
        for (from, to) in [
            ("input_tokens_details", "prompt_tokens_details"),
            ("output_tokens_details", "completion_tokens_details"),
        ] {
            if let Some(details) = usage.get(from) {
                chat["usage"][to] = details.clone();
            }
        }
    }
    Ok(chat)
}
