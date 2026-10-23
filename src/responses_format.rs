//! Lossless Responses text.format to Chat Completions response_format mapping.
//! Input schemas are data, not executable schema validators; the upstream
//! model/provider is responsible for producing schema-conforming content.
use axum::http::StatusCode;
use serde_json::{Map, Value, json};

type Failure = (StatusCode, &'static str, &'static str);

fn malformed() -> Failure {
    (
        StatusCode::BAD_REQUEST,
        "invalid_text_format",
        "text.format requires a valid text, json_object or json_schema configuration",
    )
}
fn unsupported() -> Failure {
    (
        StatusCode::NOT_IMPLEMENTED,
        "responses_text_format_unsupported",
        "This Responses text format or option cannot be mapped to Chat Completions",
    )
}

pub(super) fn translate(text: Option<&Value>) -> Result<Option<Value>, Failure> {
    let Some(text) = text else {
        return Ok(None);
    };
    let Some(wrapper) = text.as_object() else {
        return Err(malformed());
    };
    // verbosity and other Responses-specific options must not vanish silently.
    if wrapper.keys().any(|key| key != "format") {
        return Err(unsupported());
    }
    let Some(raw) = wrapper.get("format") else {
        return Ok(None); // Explicit {} behaves like the default text format.
    };
    let Some(format) = raw.as_object() else {
        return Err(malformed());
    };
    let Some(kind) = format.get("type").and_then(Value::as_str) else {
        return Err(malformed());
    };
    match kind {
        "text" | "json_object" => {
            if format.len() != 1 {
                return Err(unsupported());
            }
            Ok(Some(json!({"type":kind})))
        }
        "json_schema" => {
            if format.keys().any(|key| {
                !["type", "name", "description", "schema", "strict"].contains(&key.as_str())
            }) {
                return Err(unsupported());
            }
            let Some(name) = format.get("name").and_then(Value::as_str) else {
                return Err(malformed());
            };
            if name.is_empty()
                || name.len() > 64
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(malformed());
            }
            let Some(_schema) = format.get("schema").filter(|schema| schema.is_object()) else {
                return Err(malformed());
            };
            if format
                .get("description")
                .is_some_and(|item| !item.is_string())
                || format.get("strict").is_some_and(|item| !item.is_boolean())
            {
                return Err(malformed());
            }
            let mut nested = Map::new();
            for key in ["name", "description", "schema", "strict"] {
                if let Some(value) = format.get(key) {
                    nested.insert(key.into(), value.clone());
                }
            }
            // Do not flatten or stringify the supplied schema: deep objects,
            // large JSON integer values and optional strict all retain type.
            Ok(Some(json!({"type":"json_schema","json_schema":nested})))
        }
        _ => Err(unsupported()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn equivalent_modes_map_without_dropping_schema_fields() {
        assert_eq!(translate(None), Ok(None));
        assert_eq!(translate(Some(&json!({}))), Ok(None));
        assert_eq!(
            translate(Some(&json!({"format":{"type":"text"}}))),
            Ok(Some(json!({"type":"text"})))
        );
        assert_eq!(
            translate(Some(&json!({"format":{"type":"json_object"}}))),
            Ok(Some(json!({"type":"json_object"})))
        );
        let input = json!({"format":{"type":"json_schema","name":"future_1",
            "description":"Schema for data","strict":true,
            "schema":{"type":"object","properties":{"id":{"const":12345678901234567890u64}}}
        }});
        let mapped = translate(Some(&input)).unwrap().unwrap();
        assert_eq!(
            mapped["json_schema"]["schema"]["properties"]["id"]["const"].as_u64(),
            Some(12345678901234567890u64)
        );
        assert_eq!(mapped["json_schema"]["strict"], true);
        assert_eq!(mapped["json_schema"]["description"], "Schema for data");
    }
    #[test]
    fn malformed_and_unsupported_modes_fail_closed() {
        for item in [
            json!(null),
            json!([]),
            json!({"format":null}),
            json!({"format":{"type":"json_schema","name":"invalid name","schema":{}}}),
            json!({"format":{"type":"json_schema","name":"a","schema":"{}"}}),
            json!({"format":{"type":"json_schema","name":"a","schema":{},"strict":"true"}}),
            json!({"format":{"type":"json_schema","name":"a","schema":{},"description":false}}),
        ] {
            assert!(translate(Some(&item)).is_err(), "{item}");
        }
        for item in [
            json!({"verbosity":"high"}),
            json!({"format":{"type":"image"}}),
            json!({"format":{"type":"json_object","schema":{}}}),
            json!({"format":{"type":"json_schema","name":"a","schema":{},"hidden":1}}),
        ] {
            let err = translate(Some(&item)).unwrap_err();
            assert_eq!(err.0, StatusCode::NOT_IMPLEMENTED, "{item}");
        }
    }
}
