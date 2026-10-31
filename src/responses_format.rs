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

#[cfg(test)]
fn translate(text: Option<&Value>) -> Result<Option<Value>, Failure> {
    translate_owned(text.cloned())
}

/// Move the Responses text.verbosity level to the identical Chat field.
pub(super) fn take_verbosity(text: &mut Option<Value>) -> Result<Option<Value>, Failure> {
    let Some(Value::Object(wrapper)) = text.as_mut() else {
        return Ok(None); // translate_owned validates non-object configurations.
    };
    match wrapper.remove("verbosity") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(level)) if matches!(level.as_str(), "low" | "medium" | "high") => {
            Ok(Some(Value::String(level)))
        }
        _ => Err(malformed()),
    }
}

/// The Responses handler owns its input tree. Move the JSON Schema into the
/// Chat envelope instead of cloning a potentially multi-megabyte schema.
pub(super) fn translate_owned(text: Option<Value>) -> Result<Option<Value>, Failure> {
    let Some(text) = text else {
        return Ok(None);
    };
    let Value::Object(mut wrapper) = text else {
        return Err(malformed());
    };
    // Other Responses-specific options must not vanish silently.
    if wrapper.keys().any(|key| key != "format") {
        return Err(unsupported());
    }
    let Some(raw) = wrapper.remove("format") else {
        return Ok(None); // Explicit {} behaves like the default text format.
    };
    let Value::Object(mut format) = raw else {
        return Err(malformed());
    };
    let Some(kind) = format
        .get("type")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return Err(malformed());
    };
    match kind.as_str() {
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
            format.remove("type");
            let nested = format;
            // Do not flatten or stringify the supplied schema: deep objects,
            // large JSON integer values and optional strict all retain type.
            // json! borrows nested Values when serializing and clones their
            // Strings. Build the two-field envelope by ownership transfer.
            let mut output = Map::new();
            output.insert("type".into(), Value::String("json_schema".into()));
            output.insert("json_schema".into(), Value::Object(nested));
            Ok(Some(Value::Object(output)))
        }
        _ => Err(unsupported()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owned_schema_translation_reuses_large_text_allocation() {
        let description = "schema".repeat(128 * 1024);
        let input = json!({
            "format": {"type":"json_schema","name":"result",
                "schema":{"type":"object","description":description},
                "strict":true}
        });
        let source = input["format"]["schema"]["description"].as_str().unwrap();
        let source_ptr = source.as_ptr();
        let result = translate_owned(Some(input)).unwrap().unwrap();
        let target = result["json_schema"]["schema"]["description"]
            .as_str()
            .unwrap();
        assert_eq!(target.as_ptr(), source_ptr);
        assert_eq!(target.len(), 6 * 128 * 1024);
        assert_eq!(result["json_schema"]["strict"], true);
    }

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
    fn text_verbosity_maps_with_existing_format() {
        for level in ["low", "medium", "high"] {
            let mut config = Some(json!({"verbosity":level,
                "format":{"type":"json_object"}}));
            assert_eq!(take_verbosity(&mut config).unwrap(), Some(json!(level)));
            assert_eq!(
                translate_owned(config),
                Ok(Some(json!({"type":"json_object"})))
            );
        }
        let mut nullable = Some(json!({"verbosity":null}));
        assert_eq!(take_verbosity(&mut nullable).unwrap(), None);
        assert_eq!(translate_owned(nullable), Ok(None));
        for invalid in [json!("verbose"), json!(4), json!(false)] {
            let mut config = Some(json!({"verbosity":invalid}));
            assert!(take_verbosity(&mut config).is_err());
        }
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
