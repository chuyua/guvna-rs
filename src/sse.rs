//! Incremental streaming parser.
use bytes::Bytes;
use serde_json::Value;

/// Shared Chat/Responses JSON output-mode validation. Retain the strict Value
/// parser: IgnoredAny misses malformed UTF-16 escape sequences like an
/// unpaired surrogate, so it cannot be used as a compliance success gate.
pub(crate) fn valid_json_output(text: &str, object_required: bool) -> bool {
    serde_json::from_str::<Value>(text).is_ok_and(|value| !object_required || value.is_object())
}

const MAX_EVENT_BYTES: usize = 1024 * 1024;
// Track only tool calls, and cap the cumulative memory retained across frames.
// Each of up to 64 choices may contain 32 tool calls. Keep the global cap
// consistent with both advertised per-choice limits; argument bytes remain
// bounded independently.
const MAX_STREAM_TOOL_CALLS: usize = 64 * 32;
const MAX_STREAM_TOOL_ARGUMENTS: usize = 1024 * 1024;
const MAX_TOOL_IDENTIFIER_BYTES: usize = 256;
// JSON-mode streams retain only a bounded aggregate across all choices.
const MAX_STREAM_JSON_VALIDATION_BYTES: usize = 1024 * 1024;
const MAX_STREAM_METADATA_BYTES: usize = 1024;

#[derive(Debug)]
struct StreamToolCall {
    choice: u64,
    index: u64,
    id: Option<String>,
    name: Option<String>,
    function_type: bool,
    arguments: String,
    arguments_seen: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct SseError;

#[derive(Debug)]
enum ToolChoice {
    None,
    Required,
    Named(String),
}

#[derive(Debug, Default)]
pub struct ChatSse {
    line: Vec<u8>,
    data: Vec<u8>,
    // Streaming and SSE->JSON aggregation must agree on event identity.
    // An upstream cannot splice frames from another generation/model into
    // one successful Chat Completion. Retain only small metadata strings.
    stream_id: Option<String>,
    stream_model: Option<String>,
    stream_created: Option<u64>,
    progress_since_push: bool,
    finished_mask: u64,
    tool_calls_mask: u64,
    output_mask: u64,
    tool_calls: Vec<StreamToolCall>,
    tool_argument_bytes: usize,
    usage_seen: bool,
    tool_choice: Option<ToolChoice>,
    json_mode: Option<bool>,
    json_text: Vec<String>,
    json_text_bytes: usize,
    refusal_mask: u64,
    resource_limit: bool,
    expected: u64,
    done: bool,
}

impl ChatSse {
    pub fn new(expected: u64) -> Result<Self, SseError> {
        if !(1..=64).contains(&expected) {
            return Err(SseError);
        }
        Ok(Self {
            expected,
            ..Self::default()
        })
    }

    pub fn is_done(&self) -> bool {
        self.done
    }
    /// Only validated content/tool deltas count as useful upstream progress.
    pub fn take_progress(&mut self) -> bool {
        std::mem::take(&mut self.progress_since_push)
    }
    /// Distinguish local bounded-memory protection from malformed upstream
    /// semantics without changing the shared low-allocation error type.
    pub fn hit_resource_limit(&self) -> bool {
        self.resource_limit
    }
    /// Some OpenAI-compatible SSE providers close at EOF after every choice's
    /// finish_reason without sending the optional [DONE] sentinel. Only
    /// synthesize the terminal marker for a fully parsed, complete stream.
    pub fn finalize_on_eof(&mut self) -> Option<Bytes> {
        if self.done
            || !self.line.is_empty()
            || !self.data.is_empty()
            || self.finished_mask.count_ones() as u64 != self.expected
        {
            return None;
        }
        self.done = true;
        Some(Bytes::from_static(b"data: [DONE]\n\n"))
    }

    /// Preserve the caller's tool constraint through SSE validation. An
    /// upstream 200 cannot silently override a required or forbidden tool.
    pub fn with_tool_choice(mut self, choice: Option<&Value>) -> Self {
        self.tool_choice = match choice {
            Some(Value::String(value)) if value == "none" => Some(ToolChoice::None),
            Some(Value::String(value)) if value == "required" => Some(ToolChoice::Required),
            Some(Value::Object(_)) => choice
                .and_then(|value| value.pointer("/function/name"))
                .and_then(Value::as_str)
                .map(|name| ToolChoice::Named(name.to_owned())),
            _ => None,
        };
        self
    }

    /// Direct SSE JSON mode is an actual provider output contract.
    pub fn with_json_format(mut self, format: Option<&Value>) -> Self {
        self.json_mode = format
            .and_then(|format| format.get("type"))
            .and_then(Value::as_str)
            .and_then(|kind| match kind {
                "json_object" => Some(true),
                "json_schema" => Some(false),
                _ => None,
            });
        if self.json_mode.is_some() {
            self.json_text = vec![String::new(); self.expected as usize];
        }
        self
    }

    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Bytes>, SseError> {
        let mut emitted = Vec::new();
        if self.done && !chunk.is_empty() {
            return Err(SseError);
        }
        for &byte in chunk {
            if self.done {
                return Err(SseError);
            }
            if byte == b'\n' {
                let line = std::mem::take(&mut self.line);
                self.process_line(line.strip_suffix(b"\r").unwrap_or(&line), &mut emitted)?;
            } else {
                if self.line.len() >= MAX_EVENT_BYTES {
                    self.resource_limit = true;
                    return Err(SseError);
                }
                self.line.push(byte);
            }
        }
        Ok(emitted)
    }

    fn process_line(&mut self, line: &[u8], emitted: &mut Vec<Bytes>) -> Result<(), SseError> {
        if line.is_empty() {
            if self.data.is_empty() {
                return Ok(());
            }
            let data = std::mem::take(&mut self.data);
            if self.done {
                return Err(SseError);
            }
            if data == b"[DONE]" {
                if self.finished_mask.count_ones() as u64 != self.expected {
                    return Err(SseError);
                }
                self.done = true;
                emitted.push(Bytes::from_static(b"data: [DONE]\n\n"));
                return Ok(());
            }
            return self.process_event(data, emitted);
        }
        self.process_field(line)
    }

    fn process_event(&mut self, data: Vec<u8>, emitted: &mut Vec<Bytes>) -> Result<(), SseError> {
        let event: Value = serde_json::from_slice(&data).map_err(|_| SseError)?;
        if event.get("error").is_some_and(|error| !error.is_null()) {
            return Err(SseError);
        }
        for (key, cached) in [
            ("id", &mut self.stream_id),
            ("model", &mut self.stream_model),
        ] {
            if let Some(value) = event.get(key) {
                let value = value.as_str().filter(|s| !s.is_empty()).ok_or(SseError)?;
                if value.len() > MAX_STREAM_METADATA_BYTES {
                    self.resource_limit = true;
                    return Err(SseError);
                }
                if cached.as_deref().is_some_and(|previous| previous != value) {
                    return Err(SseError);
                }
                if cached.is_none() {
                    *cached = Some(value.to_owned());
                }
            }
        }
        if let Some(value) = event.get("created") {
            let value = value.as_u64().ok_or(SseError)?;
            if self
                .stream_created
                .is_some_and(|previous| previous != value)
            {
                return Err(SseError);
            }
            self.stream_created = Some(value);
        }
        let choices = event
            .get("choices")
            .and_then(Value::as_array)
            .ok_or(SseError)?;
        if choices.is_empty() {
            // Stream usage is optional, but if present it must be complete
            // and occur only once after every choice has finished. Otherwise
            // malformed accounting can be forwarded as a successful response.
            if self.finished_mask.count_ones() as u64 != self.expected || self.usage_seen {
                return Err(SseError);
            }
            let usage = event.get("usage").ok_or(SseError)?;
            let (Some(prompt), Some(completion), Some(total)) = (
                usage.get("prompt_tokens").and_then(Value::as_u64),
                usage.get("completion_tokens").and_then(Value::as_u64),
                usage.get("total_tokens").and_then(Value::as_u64),
            ) else {
                return Err(SseError);
            };
            if prompt.checked_add(completion) != Some(total) {
                return Err(SseError);
            }
            self.usage_seen = true;
        }
        // At most 64 choices are permitted, so a bitset avoids a hot-path allocation.
        let mut seen_indices = 0u64;
        let mut newly_finished = 0u64;
        let mut newly_tools = 0u64;
        for choice in choices {
            let (index, has_tool_delta) = self.process_choice(choice)?;
            let bit = 1u64 << index;
            if seen_indices & bit != 0 {
                return Err(SseError);
            }
            seen_indices |= bit;
            if has_tool_delta {
                newly_tools |= bit;
            }
            if choice
                .get("finish_reason")
                .is_some_and(|reason| !reason.is_null())
            {
                newly_finished |= bit;
            }
        }
        self.finished_mask |= newly_finished;
        self.tool_calls_mask |= newly_tools;

        let mut normalized = Vec::with_capacity(data.len() + 8);
        normalized.extend_from_slice(b"data: ");
        normalized.extend_from_slice(&data);
        normalized.extend_from_slice(b"\n\n");
        emitted.push(Bytes::from(normalized));
        Ok(())
    }

    fn process_choice(&mut self, choice: &Value) -> Result<(u64, bool), SseError> {
        let index = choice
            .get("index")
            .and_then(Value::as_u64)
            .ok_or(SseError)?;
        if index >= self.expected || (self.finished_mask & (1u64 << index) != 0) {
            return Err(SseError);
        }
        let empty_delta = serde_json::Map::new();
        let delta = match choice.get("delta") {
            Some(Value::Object(delta)) => delta,
            Some(Value::Null) if choice.get("finish_reason").is_some_and(|v| !v.is_null()) => {
                &empty_delta
            }
            _ => return Err(SseError),
        };
        if delta.get("role").is_some_and(|role| role != "assistant")
            || delta
                .get("content")
                .is_some_and(|content| !content.is_null() && !content.is_string())
            || ["reasoning", "reasoning_content", "refusal"]
                .iter()
                .any(|field| {
                    delta
                        .get(*field)
                        .is_some_and(|value| !value.is_null() && !value.is_string())
                })
            // The JSON aggregation path rejects unsupported non-null
            // extensions rather than silently losing them. Direct SSE must
            // enforce the same contract: a legacy function_call, multimodal
            // audio or vendor-specific output cannot become a reported
            // successful Chat Completion simply because it arrived as SSE.
            || delta.iter().any(|(field, value)| {
                !value.is_null()
                    && !matches!(
                        field.as_str(),
                        "role"
                            | "content"
                            | "reasoning"
                            | "reasoning_content"
                            | "refusal"
                            | "tool_calls"
                    )
            })
        {
            return Err(SseError);
        }
        let choice_bit = 1u64 << index;
        let has_refusal = delta
            .get("refusal")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty());
        if has_refusal {
            self.refusal_mask |= choice_bit;
        }
        let has_output = ["content", "reasoning", "reasoning_content"]
            .iter()
            .any(|field| {
                delta
                    .get(*field)
                    .and_then(Value::as_str)
                    .is_some_and(|text| !text.is_empty())
            });
        if has_output {
            self.output_mask |= choice_bit;
        }
        if self.json_mode.is_some()
            && let Some(fragment) = delta.get("content").and_then(Value::as_str)
        {
            if fragment.len()
                > MAX_STREAM_JSON_VALIDATION_BYTES.saturating_sub(self.json_text_bytes)
            {
                self.resource_limit = true;
                return Err(SseError);
            }
            self.json_text[index as usize].push_str(fragment);
            self.json_text_bytes += fragment.len();
        }
        let mut has_tool_delta = false;
        let mut tool_progress = false;
        if let Some(calls) = delta.get("tool_calls") {
            // Reject before forwarding: consumers may act on tool deltas
            // before an eventual finish event arrives.
            if matches!(self.tool_choice, Some(ToolChoice::None)) {
                return Err(SseError);
            }
            let calls = calls
                .as_array()
                .filter(|calls| !calls.is_empty())
                .ok_or(SseError)?;
            has_tool_delta = true;
            for call in calls {
                let call = call.as_object().ok_or(SseError)?;
                tool_progress |= self.process_tool_call(index, call)?;
            }
        }
        if tool_progress || has_output || has_refusal {
            self.progress_since_push = true;
        }
        if let Some(reason) = choice.get("finish_reason").filter(|v| !v.is_null()) {
            let reason = reason.as_str().ok_or(SseError)?;
            if !["stop", "length", "tool_calls", "content_filter"].contains(&reason) {
                return Err(SseError);
            }
            let has_tools = has_tool_delta || self.tool_calls_mask & (1u64 << index) != 0;
            // A truncated/filtered tool stream must never look like a
            // completed function invocation. Legacy function_call deltas
            // are unsupported, so only tool_calls may terminate tool data.
            if (reason == "tool_calls" && !has_tools) || (reason == "stop" && has_tools) {
                return Err(SseError);
            }
            // Do not report an empty, cleanly stopped stream as a valid
            // answer. Explicit refusals and reasoning-only completions are
            // legitimate; partial length/content_filter endings are unchanged.
            if reason == "stop"
                && !has_tools
                && self.output_mask & choice_bit == 0
                && self.refusal_mask & choice_bit == 0
            {
                return Err(SseError);
            }
            if reason == "stop"
                && !has_tools
                && self.refusal_mask & choice_bit == 0
                && let Some(object_only) = self.json_mode
                && !valid_json_output(&self.json_text[index as usize], object_only)
            {
                return Err(SseError);
            }
            match &self.tool_choice {
                Some(ToolChoice::None) if has_tools => return Err(SseError),
                Some(ToolChoice::Required)
                    if !has_tools && reason == "stop" && self.refusal_mask & choice_bit == 0 =>
                {
                    return Err(SseError);
                }
                Some(ToolChoice::Named(name))
                    if (!has_tools && reason == "stop" && self.refusal_mask & choice_bit == 0)
                        || self.tool_calls.iter().any(|tool| {
                            tool.choice == index && tool.name.as_deref() != Some(name.as_str())
                        }) =>
                {
                    return Err(SseError);
                }
                _ => {}
            }
            if reason == "tool_calls"
                && self
                    .tool_calls
                    .iter()
                    .filter(|tool| tool.choice == index)
                    .any(|tool| {
                        tool.id.is_none()
                            || tool.name.is_none()
                            || !tool.function_type
                            || !tool.arguments_seen
                            || (!tool.arguments.is_empty()
                                && !valid_json_output(&tool.arguments, true))
                    })
            {
                return Err(SseError);
            }
        }
        Ok((index, has_tool_delta))
    }

    fn process_tool_call(
        &mut self,
        choice: u64,
        call: &serde_json::Map<String, Value>,
    ) -> Result<bool, SseError> {
        let index = call.get("index").and_then(Value::as_u64).ok_or(SseError)?;
        if index >= 32 || call.get("type").is_some_and(|kind| kind != "function") {
            return Err(SseError);
        }
        let id = call
            .get("id")
            .map(|value| value.as_str().filter(|id| !id.is_empty()).ok_or(SseError))
            .transpose()?;
        let function = call
            .get("function")
            .map(|value| value.as_object().ok_or(SseError))
            .transpose()?;
        let name = function
            .and_then(|obj| obj.get("name"))
            .map(|value| {
                value
                    .as_str()
                    .filter(|name| !name.is_empty())
                    .ok_or(SseError)
            })
            .transpose()?;
        if let Some(ToolChoice::Named(required)) = &self.tool_choice
            && name.is_some_and(|name| name != required)
        {
            return Err(SseError);
        }
        let fragment = function
            .and_then(|obj| obj.get("arguments"))
            .map(|value| value.as_str().ok_or(SseError))
            .transpose()?;
        if id.is_some_and(|id| id.len() > MAX_TOOL_IDENTIFIER_BYTES)
            || name.is_some_and(|name| name.len() > MAX_TOOL_IDENTIFIER_BYTES)
        {
            return Err(SseError);
        }
        let position = self
            .tool_calls
            .iter()
            .position(|tool| tool.choice == choice && tool.index == index);
        let position = match position {
            Some(position) => position,
            None => {
                if self.tool_calls.len() >= MAX_STREAM_TOOL_CALLS {
                    self.resource_limit = true;
                    return Err(SseError);
                }
                if self
                    .tool_calls
                    .iter()
                    .filter(|tool| tool.choice == choice)
                    .count()
                    != index as usize
                {
                    return Err(SseError);
                }
                self.tool_calls.push(StreamToolCall {
                    choice,
                    index,
                    id: None,
                    name: None,
                    function_type: false,
                    arguments: String::new(),
                    arguments_seen: false,
                });
                self.tool_calls.len() - 1
            }
        };
        let tool = &self.tool_calls[position];
        if id.is_some_and(|id| {
            tool.id.as_deref().is_some_and(|old| old != id)
                || self
                    .tool_calls
                    .iter()
                    .enumerate()
                    .any(|(i, other)| i != position && other.id.as_deref() == Some(id))
        }) || name.is_some_and(|name| tool.name.as_deref().is_some_and(|old| old != name))
        {
            return Err(SseError);
        }
        if fragment.is_some_and(|part| {
            part.len() > MAX_STREAM_TOOL_ARGUMENTS.saturating_sub(self.tool_argument_bytes)
        }) {
            self.resource_limit = true;
            return Err(SseError);
        }
        // Identical ID/name echoes and empty argument fragments are legal
        // but carry no meaningful model progress. They must not renew an SSE
        // lease indefinitely when a provider is stalled.
        let progressed = (id.is_some() && tool.id.is_none())
            || (name.is_some() && tool.name.is_none())
            || fragment.is_some_and(|part| !part.is_empty());
        let tool = &mut self.tool_calls[position];
        if let Some(id) = id {
            tool.id.get_or_insert_with(|| id.to_owned());
        }
        if let Some(name) = name {
            tool.name.get_or_insert_with(|| name.to_owned());
        }
        tool.function_type |= call.get("type").and_then(Value::as_str) == Some("function");
        if let Some(part) = fragment {
            tool.arguments_seen = true;
            tool.arguments.push_str(part);
            self.tool_argument_bytes += part.len();
        }
        Ok(progressed)
    }

    fn process_field(&mut self, line: &[u8]) -> Result<(), SseError> {
        if self.done {
            return Err(SseError);
        }
        if line.starts_with(b":")
            || line.starts_with(b"event:")
            || line.starts_with(b"id:")
            || line.starts_with(b"retry:")
        {
            return Ok(());
        }
        if let Some(value) = line.strip_prefix(b"data:") {
            let value = value.strip_prefix(b" ").unwrap_or(value);
            if self.data.len() + value.len() + 1 > MAX_EVENT_BYTES {
                self.resource_limit = true;
                return Err(SseError);
            }
            if !self.data.is_empty() {
                self.data.push(b'\n');
            }
            self.data.extend_from_slice(value);
            return Ok(());
        }
        Err(SseError)
    }

    pub fn finish(&self) -> Result<(), SseError> {
        if self.done && self.line.is_empty() && self.data.is_empty() {
            Ok(())
        } else {
            Err(SseError)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn shared_json_validation_matches_the_original_value_parser_contract() {
        let cases = [
            "{}",
            "{\"nested\":[1,true,null,\"hello\"]}",
            "  {\"unicode\":\"中🌿文\"}\n",
            "[]",
            "[1,2,3]",
            "null",
            "true",
            "42",
            "-0.000001",
            "1e309",
            "\"escaped\\ntext\"",
            "{\"a\":1,\"a\":2}",
            "",
            "  ",
            "{}{}",
            "{bad}",
            "{\"a\":NaN}",
            "{\"a\":Infinity}",
            "{\"a\":1,}",
            "{\"a\":\"unfinished}",
            "[1,]",
            "/* comment */ {}",
            "\"\\uD800\"",
        ];
        for source in cases {
            let original = serde_json::from_str::<Value>(source).ok();
            for object_required in [false, true] {
                assert_eq!(
                    valid_json_output(source, object_required),
                    original
                        .as_ref()
                        .is_some_and(|value| !object_required || value.is_object()),
                    "validation parity lost for {source:?}, object_required={object_required}"
                );
            }
        }
    }

    const START: &[u8] =
        br#"data: {"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}

"#;
    const END: &[u8] = br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

"#;
    #[test]
    fn direct_json_mode_validates_each_choice_at_completion() {
        for (format, fragments, finish, accepted) in [
            ("json_object", vec!["{\"ok\":", "true}"], "stop", true),
            ("json_object", vec!["plain prose"], "stop", false),
            ("json_object", vec!["[1]"], "stop", false),
            ("json_schema", vec!["[1]"], "stop", true),
            ("json_schema", vec!["not JSON"], "stop", false),
            ("json_object", vec!["{\"unfinished\":"], "length", true),
        ] {
            let mut parser = ChatSse::new(1)
                .unwrap()
                .with_json_format(Some(&json!({"type":format})));
            let mut failed = false;
            for (i, part) in fragments.iter().enumerate() {
                let reason = if i + 1 == fragments.len() {
                    json!(finish)
                } else {
                    Value::Null
                };
                let event = format!(
                    "data: {}\n\n",
                    json!({"choices":[{"index":0,"delta":{"content":part},"finish_reason":reason}]})
                );
                if parser.push(event.as_bytes()).is_err() {
                    failed = true;
                    break;
                }
            }
            assert_eq!(!failed, accepted, "{format} {fragments:?} {finish}");
            if accepted {
                assert!(parser.push(b"data: [DONE]\n\n").is_ok());
            }
        }
        // A legitimate provider refusal is not an ordinary JSON answer.
        let mut refusal = ChatSse::new(1)
            .unwrap()
            .with_json_format(Some(&json!({"type":"json_object"})));
        let event = format!(
            "data: {}\n\n",
            json!({"choices":[{"index":0,
            "delta":{"content":"cannot assist","refusal":"cannot assist"},
            "finish_reason":"stop"}]})
        );
        refusal.push(event.as_bytes()).unwrap();
        refusal.push(b"data: [DONE]\n\n").unwrap();
    }

    #[test]
    fn json_mode_stream_retain_limit_is_bounded_across_deltas() {
        let mut parser = ChatSse::new(1)
            .unwrap()
            .with_json_format(Some(&json!({"type":"json_object"})));
        let large = "a".repeat(MAX_STREAM_JSON_VALIDATION_BYTES / 2 + 1);
        let event = format!(
            "data: {}\n\n",
            json!({"choices":[{"index":0,
            "delta":{"content":large},"finish_reason":null}]})
        );
        assert!(parser.push(event.as_bytes()).is_ok());
        assert!(parser.push(event.as_bytes()).is_err());
        assert!(parser.hit_resource_limit());
    }

    #[test]
    fn oversized_fragmented_sse_frame_is_local_limit_not_malformed_protocol() {
        let mut oversized = ChatSse::new(1).unwrap();
        let half = vec![b'x'; MAX_EVENT_BYTES / 2 + 1];
        assert!(oversized.push(&half).is_ok());
        assert!(oversized.push(&half).is_err());
        assert!(oversized.hit_resource_limit());

        let mut malformed = ChatSse::new(1).unwrap();
        assert!(malformed.push(b"data: not-json\n\n").is_err());
        assert!(!malformed.hit_resource_limit());
    }

    #[test]
    fn known_reasoning_and_refusal_fields_must_be_text_or_null() {
        for field in ["reasoning", "reasoning_content", "refusal"] {
            let mut parser = ChatSse::new(1).unwrap();
            let mut delta = serde_json::Map::new();
            delta.insert(field.to_owned(), json!({"bad":"object"}));
            let malformed = format!(
                "data: {}\n\n",
                json!({"choices":[{"index":0,"delta":delta,"finish_reason":null}]})
            );
            assert!(parser.push(malformed.as_bytes()).is_err(), "{field}");
            assert!(
                !parser.take_progress(),
                "{field}: malformed data renewed probe"
            );
        }
        let mut parser = ChatSse::new(1).unwrap();
        let valid = b"data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":null},\"finish_reason\":null}]}\n\n";
        assert!(parser.push(valid).is_ok());
        assert!(!parser.take_progress());
    }

    #[test]
    fn only_valid_content_or_tool_deltas_refresh_probe_progress() {
        let mut parser = ChatSse::new(1).unwrap();
        let role =
            br#"data: {"choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}

"#;
        parser.push(role).unwrap();
        assert!(
            !parser.take_progress(),
            "a role-only frame is not useful progress"
        );
        parser.push(START).unwrap();
        assert!(parser.take_progress());
        assert!(!parser.take_progress(), "progress must be consumed once");
        let reasoning = br#"data: {"choices":[{"index":0,"delta":{"reasoning_content":"still thinking"},"finish_reason":null}]}

"#;
        parser.push(reasoning).unwrap();
        assert!(
            parser.take_progress(),
            "a valid reasoning delta is useful progress"
        );
        parser.push(END).unwrap();
        assert!(
            !parser.take_progress(),
            "terminal bookkeeping cannot renew a probe"
        );
    }

    #[test]
    fn fragmented_frames_complete_only_with_finish_and_done() {
        let mut parser = ChatSse::new(1).unwrap();
        let mut out = Vec::new();
        for b in START.iter().chain(END).chain(b"data: [DONE]\n\n") {
            out.extend(parser.push(&[*b]).unwrap());
        }
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], Bytes::from_static(START));
        assert!(parser.finish().is_ok());
    }

    #[test]
    fn rejects_premature_done() {
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(START).unwrap();
        assert!(parser.push(b"data: [DONE]\n\n").is_err());
        assert!(parser.finish().is_err());
    }

    #[test]
    fn rejects_large_line() {
        let mut parser = ChatSse::new(1).unwrap();
        assert!(parser.push(&vec![b'x'; MAX_EVENT_BYTES + 1]).is_err());
    }

    #[test]
    fn duplicate_choice_index_in_single_event_is_rejected() {
        let mut parser = ChatSse::new(1).unwrap();
        let frame = br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":null},{"index":0,"delta":{},"finish_reason":null}]}

"#;
        assert!(parser.push(frame).is_err());
    }

    #[test]
    fn rejects_malformed_assistant_and_tool_deltas() {
        for delta in [
            serde_json::json!({"role":"user"}),
            serde_json::json!({"content":{"text":"not a string"}}),
            serde_json::json!({"tool_calls":"not an array"}),
            serde_json::json!({"tool_calls":[]}),
            serde_json::json!({"tool_calls":[{"index":"zero"}]}),
            serde_json::json!({"tool_calls":[{"index":0,"function":"not an object"}]}),
            serde_json::json!({"tool_calls":[{"index":0,"function":{"arguments":{}}}]}),
        ] {
            let mut parser = ChatSse::new(1).unwrap();
            let frame = format!(
                "data: {}\n\n",
                serde_json::json!({
                    "choices":[{"index":0,"delta":delta,"finish_reason":null}]
                })
            );
            assert!(parser.push(frame.as_bytes()).is_err(), "accepted {delta}");
        }
    }

    #[test]
    fn direct_sse_rejects_unsupported_extensions_before_forwarding_them() {
        for delta in [
            json!({"function_call":{"name":"legacy","arguments":"{}"}}),
            json!({"audio":{"id":"audio-1","data":"b64"}}),
            json!({"image":{"url":"https://example.test/image"}}),
            json!({"reasoning_details":[{"text":"secret"}]}),
            json!({"content":"hello","function_call":{"name":"legacy","arguments":"{}"}}),
        ] {
            let mut parser = ChatSse::new(1).unwrap();
            let frame = format!(
                "data: {}\n\n",
                json!({
                    "choices":[{"index":0,"delta":delta,"finish_reason":null}]
                })
            );
            assert!(parser.push(frame.as_bytes()).is_err(), "accepted {delta}");
        }
        // Optional null extensions contain no information to silently lose.
        let mut parser = ChatSse::new(1).unwrap();
        let frame = format!(
            "data: {}\n\n",
            json!({
                "choices":[{"index":0,"delta":{
                    "content":"supported","function_call":null,"audio":null
                },"finish_reason":"stop"}]
            })
        );
        assert!(parser.push(frame.as_bytes()).is_ok());
        assert!(parser.push(b"data: [DONE]\n\n").is_ok());
    }

    #[test]
    fn sse_model_id_created_and_error_are_bounded_and_consistent() {
        let first = json!({
            "id":"chatcmpl-original","model":"physical-model","created":100,
            "choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]
        });
        let terminal = json!({
            "id":"chatcmpl-original","model":"physical-model","created":100,
            "choices":[{"index":0,"delta":{},"finish_reason":"stop"}]
        });
        let mut healthy = ChatSse::new(1).unwrap();
        for event in [first.clone(), terminal] {
            assert!(
                healthy
                    .push(format!("data: {event}\n\n").as_bytes())
                    .is_ok()
            );
        }
        assert!(healthy.push(b"data: [DONE]\n\n").is_ok());
        for replacement in [
            json!({"id":"another-generation"}),
            json!({"model":"different-physical-model"}),
            json!({"created":101}),
            json!({"error":{"code":"model_failed"}}),
            json!({"id":null}),
            json!({"model":"m".repeat(MAX_STREAM_METADATA_BYTES + 1)}),
        ] {
            let mut parser = ChatSse::new(1).unwrap();
            parser
                .push(format!("data: {first}\n\n").as_bytes())
                .unwrap();
            let mut changed = first.clone();
            for (key, value) in replacement.as_object().unwrap() {
                changed[key] = value.clone();
            }
            assert!(
                parser
                    .push(format!("data: {changed}\n\n").as_bytes())
                    .is_err(),
                "accepted changed metadata {replacement}"
            );
            assert!(!parser.is_done());
        }
        // Providers may omit metadata from subsequent chunks or usage trailers.
        let mut parser = ChatSse::new(1).unwrap();
        parser
            .push(format!("data: {first}\n\n").as_bytes())
            .unwrap();
        parser.push(END).unwrap();
        parser.push(b"data: [DONE]\n\n").unwrap();
    }

    #[test]
    fn tool_finish_reason_must_match_streamed_tool_calls() {
        let tool_delta = br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":null}]}

"#;
        let tool_finish =
            br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}

"#;
        let text_finish = br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

"#;
        let mut parser = ChatSse::new(1).unwrap();
        assert!(parser.push(tool_finish).is_err());
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(tool_delta).unwrap();
        assert!(parser.push(text_finish).is_err());
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(tool_delta).unwrap();
        let continuation = br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":" "}}]},"finish_reason":null}]}

"#;
        parser.push(continuation).unwrap();
        parser.push(tool_finish).unwrap();
        parser.push(b"data: [DONE]\n\n").unwrap();
        assert!(parser.finish().is_ok());
    }

    #[test]
    fn incomplete_tool_calls_and_unsupported_legacy_finish_cannot_complete() {
        let tool_delta = serde_json::json!({"choices":[{"index":0,"delta":{"tool_calls":[
            {"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"key\":"}}
        ]},"finish_reason":null}]});
        for reason in ["length", "content_filter"] {
            let mut parser = ChatSse::new(1).unwrap();
            parser
                .push(format!("data: {tool_delta}\n\n").as_bytes())
                .unwrap();
            let finish =
                serde_json::json!({"choices":[{"index":0,"delta":{},"finish_reason":reason}]});
            parser
                .push(format!("data: {finish}\n\n").as_bytes())
                .unwrap();
            parser.push(b"data: [DONE]\n\n").unwrap();
            assert!(
                parser.finish().is_ok(),
                "a truncated tool response is incomplete, not invalid"
            );
        }
        let mut parser = ChatSse::new(1).unwrap();
        let legacy =
            serde_json::json!({"choices":[{"index":0,"delta":{},"finish_reason":"function_call"}]});
        assert!(
            parser
                .push(format!("data: {legacy}\n\n").as_bytes())
                .is_err(),
            "legacy function_call without a function payload must not claim completion"
        );
        for reason in ["length", "content_filter"] {
            let mut parser = ChatSse::new(1).unwrap();
            let text = serde_json::json!({"choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":reason}]});
            parser.push(format!("data: {text}\n\n").as_bytes()).unwrap();
            parser.push(b"data: [DONE]\n\n").unwrap();
            assert!(
                parser.finish().is_ok(),
                "rejected valid text finish {reason}"
            );
        }
    }

    #[test]
    fn terminal_null_delta_empty_arguments_and_required_length_are_compatible() {
        let terminal = serde_json::json!({"choices":[{"index":0,
            "delta":null,"finish_reason":"length"}]});
        let mut required = ChatSse::new(1)
            .unwrap()
            .with_tool_choice(Some(&serde_json::json!("required")));
        required
            .push(format!("data: {terminal}\n\n").as_bytes())
            .unwrap();
        required.push(b"data: [DONE]\n\n").unwrap();
        required.finish().unwrap();

        let mut tools = ChatSse::new(1).unwrap();
        let delta = serde_json::json!({"choices":[{"index":0,
            "delta":{"tool_calls":[{"index":0,"id":"zero","type":"function",
                "function":{"name":"no_params","arguments":""}}]},"finish_reason":null}]});
        tools.push(format!("data: {delta}\n\n").as_bytes()).unwrap();
        let finish = serde_json::json!({"choices":[{"index":0,
            "delta":null,"finish_reason":"tool_calls"}]});
        tools
            .push(format!("data: {finish}\n\n").as_bytes())
            .unwrap();
        tools.push(b"data: [DONE]\n\n").unwrap();
        tools.finish().unwrap();
    }

    #[test]
    fn repeated_empty_tool_call_deltas_cannot_refresh_stream_progress_forever() {
        let mut parser = ChatSse::new(1).unwrap();
        let initial = json!({"choices":[{"index":0,"delta":{"tool_calls":[
            {"index":0,"id":"call_1","type":"function",
             "function":{"name":"lookup","arguments":""}}
        ]},"finish_reason":null}]});
        parser
            .push(format!("data: {initial}\n\n").as_bytes())
            .unwrap();
        assert!(parser.take_progress(), "new tool identity is real progress");
        let repeated = json!({"choices":[{"index":0,"delta":{"tool_calls":[
            {"index":0,"id":"call_1","type":"function",
             "function":{"name":"lookup","arguments":""}}
        ]},"finish_reason":null}]});
        for _ in 0..4 {
            parser
                .push(format!("data: {repeated}\n\n").as_bytes())
                .unwrap();
            assert!(
                !parser.take_progress(),
                "identical empty tool fragments bypassed meaningful-progress timeout"
            );
        }
        let meaningful = json!({"choices":[{"index":0,"delta":{"tool_calls":[
            {"index":0,"function":{"arguments":"{\"query\":\"hi\"}"}}
        ]},"finish_reason":null}]});
        parser
            .push(format!("data: {meaningful}\n\n").as_bytes())
            .unwrap();
        assert!(
            parser.take_progress(),
            "new argument bytes were not counted"
        );
    }

    #[test]
    fn fragmented_tool_arguments_exceeding_cap_are_a_local_resource_limit() {
        let mut parser = ChatSse::new(1).unwrap();
        let part = "x".repeat(MAX_STREAM_TOOL_ARGUMENTS / 2 + 10);
        let first = json!({"choices":[{"index":0,"delta":{"tool_calls":[
            {"index":0,"id":"call_1","type":"function",
             "function":{"name":"query","arguments":part}}
        ]},"finish_reason":null}]});
        let first = format!("data: {first}\n\n");
        assert!(parser.push(first.as_bytes()).is_ok());
        let second = json!({"choices":[{"index":0,"delta":{"tool_calls":[
            {"index":0,"function":{"arguments":part}}
        ]},"finish_reason":null}]});
        let second = format!("data: {second}\n\n");
        assert!(parser.push(second.as_bytes()).is_err());
        assert!(parser.hit_resource_limit());
        // An invalid tool index is a protocol fault, not an OOM guard.
        let mut invalid = ChatSse::new(1).unwrap();
        let bad = json!({"choices":[{"index":0,"delta":{"tool_calls":[
            {"index":1,"type":"function","function":{"name":"query","arguments":"{}"}}
        ]},"finish_reason":null}]});
        assert!(invalid.push(format!("data: {bad}\n\n").as_bytes()).is_err());
        assert!(!invalid.hit_resource_limit());
    }

    #[test]
    fn multi_choice_tool_calls_do_not_hit_an_unadvertised_global_128_cap() {
        let mut parser = ChatSse::new(5).unwrap();
        for choice in 0..5u64 {
            for index in 0..26u64 {
                let delta = serde_json::json!({"choices":[{"index":choice,
                    "delta":{"tool_calls":[{"index":index,
                        "id":format!("call_{choice}_{index}"),"type":"function",
                        "function":{"name":"run","arguments":"{}"}}]},
                    "finish_reason":null}]});
                parser
                    .push(format!("data: {delta}\n\n").as_bytes())
                    .unwrap();
            }
            let finish = serde_json::json!({"choices":[{"index":choice,
                "delta":null,"finish_reason":"tool_calls"}]});
            parser
                .push(format!("data: {finish}\n\n").as_bytes())
                .unwrap();
        }
        parser.push(b"data: [DONE]\n\n").unwrap();
        assert_eq!(parser.tool_calls.len(), 130);
        parser.finish().unwrap();
    }

    #[test]
    fn completed_choices_at_eof_gain_one_valid_terminal_marker() {
        let mut parser = ChatSse::new(1).unwrap();
        parser
            .push(
                br#"data: {"choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}

"#,
            )
            .unwrap();
        assert_eq!(
            parser.finalize_on_eof().unwrap(),
            Bytes::from_static(b"data: [DONE]\n\n")
        );
        assert!(parser.finalize_on_eof().is_none());
        parser.finish().unwrap();

        let mut incomplete = ChatSse::new(1).unwrap();
        incomplete.push(br#"data: {"choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}

"#).unwrap();
        assert!(incomplete.finalize_on_eof().is_none());
    }

    #[test]
    fn usage_frames_must_be_complete_consistent_and_unique() {
        let finished =
            br#"data: {"choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}

"#;
        let valid_usage = br#"data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}

"#;
        for invalid in [
            r#"{"choices":[],"usage":{}}"#,
            r#"{"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":7}}"#,
            r#"{"choices":[],"usage":{"prompt_tokens":-1,"completion_tokens":3,"total_tokens":2}}"#,
            r#"{"choices":[],"usage":{"prompt_tokens":18446744073709551615,"completion_tokens":1,"total_tokens":0}}"#,
        ] {
            let mut parser = ChatSse::new(1).unwrap();
            parser.push(finished).unwrap();
            assert!(
                parser
                    .push(format!("data: {invalid}\n\n").as_bytes())
                    .is_err(),
                "invalid usage accepted: {invalid}"
            );
        }
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(finished).unwrap();
        parser.push(valid_usage).unwrap();
        assert!(
            parser.push(valid_usage).is_err(),
            "duplicate usage accepted"
        );

        let mut parser = ChatSse::new(1).unwrap();
        parser.push(finished).unwrap();
        parser.push(valid_usage).unwrap();
        parser.push(b"data: [DONE]\n\n").unwrap();
        assert!(parser.finish().is_ok());
    }

    #[test]
    fn streamed_tool_choice_must_be_honored() {
        let tool = serde_json::json!({"choices":[{"index":0,"delta":{"tool_calls":[{
            "index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}
        }]},"finish_reason":null}]});
        let tool_event = format!("data: {tool}\n\n");
        let finish = b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n";

        let mut required = ChatSse::new(1)
            .unwrap()
            .with_tool_choice(Some(&serde_json::json!("required")));
        required.push(START).unwrap();
        assert!(
            required.push(END).is_err(),
            "required tool choice accepted plain text"
        );

        let mut none = ChatSse::new(1)
            .unwrap()
            .with_tool_choice(Some(&serde_json::json!("none")));
        assert!(
            none.push(tool_event.as_bytes()).is_err(),
            "tool_choice none forwarded a forbidden tool delta"
        );

        let named = serde_json::json!({"type":"function","function":{"name":"weather"}});
        let mut wrong = ChatSse::new(1).unwrap().with_tool_choice(Some(&named));
        assert!(
            wrong.push(tool_event.as_bytes()).is_err(),
            "named tool choice forwarded another function"
        );

        let named = serde_json::json!({"type":"function","function":{"name":"lookup"}});
        let mut valid = ChatSse::new(1).unwrap().with_tool_choice(Some(&named));
        valid.push(tool_event.as_bytes()).unwrap();
        valid.push(finish).unwrap();
        valid.push(b"data: [DONE]\n\n").unwrap();
        assert!(valid.finish().is_ok());
    }

    #[test]
    fn explicit_refusal_satisfies_forced_tool_stream_without_allowing_plain_text() {
        let refusal = br#"data: {"choices":[{"index":0,"delta":{"role":"assistant","refusal":"Cannot help"},"finish_reason":null}]}

"#;
        let empty_refusal =
            br#"data: {"choices":[{"index":0,"delta":{"refusal":""},"finish_reason":null}]}

"#;
        let finish = br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

"#;
        for tool_choice in [
            serde_json::json!("required"),
            serde_json::json!({"type":"function","function":{"name":"lookup"}}),
        ] {
            let mut allowed = ChatSse::new(1)
                .unwrap()
                .with_tool_choice(Some(&tool_choice));
            allowed.push(refusal).unwrap();
            allowed.push(finish).unwrap();
            allowed.push(b"data: [DONE]\n\n").unwrap();
            allowed.finish().unwrap();

            for invalid_delta in [empty_refusal.as_slice(), START] {
                let mut rejected = ChatSse::new(1)
                    .unwrap()
                    .with_tool_choice(Some(&tool_choice));
                rejected.push(invalid_delta).unwrap();
                assert!(
                    rejected.push(finish).is_err(),
                    "non-refusal cannot bypass required tool"
                );
            }
        }
    }

    #[test]
    fn tool_finish_rejects_incomplete_identity_and_invalid_json() {
        for call in [
            serde_json::json!({"index":0}),
            serde_json::json!({"index":0,"id":"call_1","type":"function","function":{"name":"lookup"}}),
            serde_json::json!({"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":"[1]"}}),
            serde_json::json!({"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{bad"}}),
        ] {
            let mut parser = ChatSse::new(1).unwrap();
            let first = format!(
                "data: {}\n\n",
                serde_json::json!({
                    "choices":[{"index":0,"delta":{"tool_calls":[call]},"finish_reason":null}]
                })
            );
            parser.push(first.as_bytes()).unwrap();
            let finish =
                br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}

"#;
            assert!(
                parser.push(finish).is_err(),
                "accepted incomplete tool call"
            );
        }
    }

    #[test]
    fn tool_stream_rejects_duplicate_ids_and_unbounded_arguments() {
        let mut parser = ChatSse::new(1).unwrap();
        let first = serde_json::json!({"choices":[{"index":0,"delta":{"tool_calls":[
            {"index":0,"id":"same","type":"function","function":{"name":"a","arguments":"{}"}},
            {"index":1,"id":"same","type":"function","function":{"name":"b","arguments":"{}"}}
        ]},"finish_reason":null}]});
        assert!(
            parser
                .push(format!("data: {first}\n\n").as_bytes())
                .is_err()
        );

        let mut parser = ChatSse::new(1).unwrap();
        let first = serde_json::json!({"choices":[{"index":0,"delta":{"tool_calls":[
            {"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":""}}
        ]},"finish_reason":null}]});
        parser
            .push(format!("data: {first}\n\n").as_bytes())
            .unwrap();
        let fragment = "x".repeat(64 * 1024);
        let event = serde_json::json!({"choices":[{"index":0,"delta":{"tool_calls":[
            {"index":0,"function":{"arguments":fragment}}
        ]},"finish_reason":null}]});
        let frame = format!("data: {event}\n\n");
        let mut rejected = false;
        for _ in 0..20 {
            if parser.push(frame.as_bytes()).is_err() {
                rejected = true;
                break;
            }
        }
        assert!(rejected, "unbounded tool arguments accepted");
    }

    #[test]
    fn clean_stop_requires_text_reasoning_or_refusal() {
        for (delta, allowed) in [
            (json!({}), false),
            (json!({"role":"assistant"}), false),
            (
                json!({"content":"","reasoning_content":"","refusal":""}),
                false,
            ),
            (json!({"content":"ok"}), true),
            (json!({"reasoning_content":"thought"}), true),
            (json!({"reasoning":"thought"}), true),
            (json!({"refusal":"Cannot help"}), true),
        ] {
            let mut parser = ChatSse::new(1).unwrap();
            let frame = format!(
                "data: {}\n\n",
                json!({"choices":[{"index":0,"delta":delta,"finish_reason":"stop"}]})
            );
            assert_eq!(parser.push(frame.as_bytes()).is_ok(), allowed, "{frame}");
        }
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(br#"data: {"choices":[{"index":0,"delta":{"refusal":"Cannot help"},"finish_reason":null}]}

"#).unwrap();
        assert!(parser.take_progress());
        parser
            .push(
                br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":null}]}

"#,
            )
            .unwrap();
        assert!(
            !parser.take_progress(),
            "empty frame is not renewed progress"
        );
    }

    #[test]
    fn duplicate_done_is_rejected() {
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(START).unwrap();
        parser.push(END).unwrap();
        parser.push(b"data: [DONE]\n\n").unwrap();
        assert!(parser.push(b"data: [DONE]\n\n").is_err());
    }

    #[test]
    fn invalid_later_choice_cannot_commit_earlier_finish() {
        let mut parser = ChatSse::new(2).unwrap();
        let bad = br#"data: {"choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"},{"index":1,"delta":null,"finish_reason":null}]}

"#;
        assert!(parser.push(bad).is_err());
        assert_eq!(parser.finished_mask, 0);
    }

    #[test]
    fn rejects_trailing_bytes_after_done_in_same_and_later_chunk() {
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(START).unwrap();
        parser.push(END).unwrap();
        assert!(parser.push(b"data: [DONE]\n\nextra").is_err());
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(START).unwrap();
        parser.push(END).unwrap();
        parser.push(b"data: [DONE]\n\n").unwrap();
        assert!(parser.push(b"extra").is_err());
    }
    #[test]
    fn sixty_four_choices_finish_out_of_order_without_allocating_per_choice() {
        let mut parser = ChatSse::new(64).unwrap();
        let high: Vec<Value> = (32..64)
            .rev()
            .map(|index| serde_json::json!({"index":index,"delta":{"content":"ok"},"finish_reason":"stop"}))
            .collect();
        let low: Vec<Value> = (0..32)
            .rev()
            .map(|index| serde_json::json!({"index":index,"delta":{"content":"ok"},"finish_reason":"stop"}))
            .collect();
        assert_eq!(high.len(), 32);
        let first = format!("data: {}\n\n", serde_json::json!({"choices":high}));
        parser.push(first.as_bytes()).unwrap();
        assert_eq!(parser.finished_mask.count_ones(), 32);
        assert!(parser.push(b"data: [DONE]\n\n").is_err());
        let second = format!("data: {}\n\n", serde_json::json!({"choices":low}));
        parser.push(second.as_bytes()).unwrap();
        assert_eq!(parser.finished_mask, u64::MAX);
        parser.push(b"data: [DONE]\n\n").unwrap();
        assert!(parser.finish().is_ok());
    }
}
