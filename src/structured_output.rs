//! Actual JSON Schema validation without HTTP or filesystem reference access.
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct Schema(Arc<jsonschema::Validator>);
impl std::fmt::Debug for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CompiledOutputSchema")
    }
}
struct NoExternalReferences;
impl jsonschema::Retrieve for NoExternalReferences {
    fn retrieve(
        &self,
        _uri: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("External schema references are disabled".into())
    }
}
impl Schema {
    pub(crate) fn compile(format: Option<&Value>) -> Result<Option<Self>, ()> {
        let Some(format) = format.filter(|v| v["type"] == "json_schema") else {
            return Ok(None);
        };
        let schema = format.pointer("/json_schema/schema").ok_or(())?;
        Self::build(schema).map(Some)
    }
    fn build(schema: &Value) -> Result<Self, ()> {
        // Keep schema compilation bounded independently of the full request.
        // Description/enum strings may be large, but the structural tree must
        // remain small enough for synchronous validation on a single-user host.
        fn bounded(value: &Value, depth: usize, nodes: &mut usize) -> bool {
            *nodes = nodes.saturating_add(1);
            if depth > 64 || *nodes > 16_384 {
                return false;
            }
            match value {
                Value::Array(items) => items.iter().all(|v| bounded(v, depth + 1, nodes)),
                Value::Object(items) => items.values().all(|v| bounded(v, depth + 1, nodes)),
                _ => true,
            }
        }
        if crate::routing::auto_input_bytes(Some(schema)) > 1024 * 1024
            || !bounded(schema, 0, &mut 0)
        {
            return Err(());
        }
        jsonschema::options()
            .with_retriever(NoExternalReferences)
            .build(schema)
            .map(|validator| Self(Arc::new(validator)))
            .map_err(|_| ())
    }
    pub(crate) fn accepts(&self, text: &str) -> bool {
        serde_json::from_str::<Value>(text).is_ok_and(|value| self.0.is_valid(&value))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OutputFormat {
    object_only: bool,
    schema: Option<Schema>,
    _lease: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
}
impl OutputFormat {
    pub(crate) fn chat(format: Option<&Value>) -> Result<Option<Self>, ()> {
        let Some(format) = format else {
            return Ok(None);
        };
        match format["type"].as_str() {
            Some("json_object") => Ok(Some(Self {
                object_only: true,
                schema: None,
                _lease: None,
            })),
            Some("json_schema") => Ok(Some(Self {
                object_only: false,
                schema: Schema::compile(Some(format))?,
                _lease: None,
            })),
            _ => Ok(None),
        }
    }
    pub(crate) fn native(
        format: Option<&Value>,
        fallback: Option<&Value>,
    ) -> Result<Option<Self>, ()> {
        let Some(format) = format else {
            return Self::chat(fallback);
        };
        match format["type"].as_str() {
            Some("json_object") => Ok(Some(Self {
                object_only: true,
                schema: None,
                _lease: None,
            })),
            Some("json_schema") => Ok(Some(Self {
                object_only: false,
                schema: Some(Schema::build(format.get("schema").ok_or(())?)?),
                _lease: None,
            })),
            _ => Ok(None),
        }
    }
    pub(crate) fn with_lease(mut self, permit: tokio::sync::OwnedSemaphorePermit) -> Self {
        self._lease = Some(Arc::new(permit));
        self
    }
    pub(crate) fn accepts(&self, text: &str) -> bool {
        serde_json::from_str::<Value>(text).is_ok_and(|value| {
            (!self.object_only || value.is_object())
                && self
                    .schema
                    .as_ref()
                    .is_none_or(|schema| schema.0.is_valid(&value))
        })
    }
    pub(crate) fn accepts_native(&self, response: &Value) -> bool {
        if response["status"] != "completed" {
            return true;
        }
        let Some(items) = response["output"].as_array() else {
            return false;
        };
        if items.iter().any(|item| item["type"] == "function_call") {
            return true;
        }
        let mut text = String::new();
        for item in items.iter().filter(|item| item["type"] == "message") {
            let Some(parts) = item["content"].as_array() else {
                return false;
            };
            for part in parts {
                if part["type"] == "refusal" {
                    return true;
                }
                if part["type"] == "output_text" {
                    let Some(part) = part["text"].as_str() else {
                        return false;
                    };
                    text.push_str(part);
                }
            }
        }
        if text.is_empty() && items.iter().any(|item| item["type"] != "message") {
            return true;
        }
        self.accepts(&text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn validates_required_types_enum_refs_and_exact_large_numbers() {
        let format = json!({"type":"json_schema","json_schema":{"name":"x","strict":true,"schema":{
            "type":"object","properties":{"value":{"$ref":"#/$defs/item"}},"required":["value"],"additionalProperties":false,
            "$defs":{"item":{"type":"integer","minimum":9007199254740993_u64,"enum":[9007199254740993_u64]}}}}});
        let schema = Schema::compile(Some(&format)).unwrap().unwrap();
        assert!(schema.accepts(r#"{"value":9007199254740993}"#));
        for bad in [
            r#"{"value":9007199254740992}"#,
            r#"{"value":"9007199254740993"}"#,
            r#"{}"#,
            r#"{"value":9007199254740993,"extra":1}"#,
        ] {
            assert!(!schema.accepts(bad));
        }
    }
    #[test]
    fn compilation_never_reads_network_or_local_files() {
        for reference in ["https://example.org/schema.json", "file:///etc/passwd"] {
            let format = json!({"type":"json_schema","json_schema":{"name":"x","schema":{"$ref":reference}}});
            assert!(Schema::compile(Some(&format)).is_err());
        }
    }
}
