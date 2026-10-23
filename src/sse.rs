//! Incremental streaming parser.
use bytes::Bytes;
use serde_json::Value;

const MAX_EVENT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct SseError;

#[derive(Debug, Default)]
pub struct ChatSse {
    line: Vec<u8>,
    data: Vec<u8>,
    finished_mask: u64,
    tool_calls_mask: u64,
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
        let choices = event
            .get("choices")
            .and_then(Value::as_array)
            .ok_or(SseError)?;
        if choices.is_empty()
            && (self.finished_mask.count_ones() as u64 != self.expected
                || !event.get("usage").is_some_and(Value::is_object))
        {
            return Err(SseError);
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

    fn process_choice(&self, choice: &Value) -> Result<(u64, bool), SseError> {
        let index = choice
            .get("index")
            .and_then(Value::as_u64)
            .ok_or(SseError)?;
        if index >= self.expected || (self.finished_mask & (1u64 << index) != 0) {
            return Err(SseError);
        }
        let delta = choice
            .get("delta")
            .and_then(Value::as_object)
            .ok_or(SseError)?;
        if delta.get("role").is_some_and(|role| role != "assistant")
            || delta
                .get("content")
                .is_some_and(|content| !content.is_null() && !content.is_string())
        {
            return Err(SseError);
        }
        let mut has_tool_delta = false;
        if let Some(calls) = delta.get("tool_calls") {
            let calls = calls
                .as_array()
                .filter(|calls| !calls.is_empty())
                .ok_or(SseError)?;
            has_tool_delta = true;
            for call in calls {
                let call = call.as_object().ok_or(SseError)?;
                if !call
                    .get("index")
                    .is_some_and(|index| index.as_u64().is_some())
                    || call
                        .get("id")
                        .is_some_and(|id| id.as_str().is_none_or(str::is_empty))
                    || call.get("type").is_some_and(|kind| kind != "function")
                {
                    return Err(SseError);
                }
                if let Some(function) = call.get("function") {
                    let function = function.as_object().ok_or(SseError)?;
                    if function
                        .get("name")
                        .is_some_and(|name| name.as_str().is_none_or(str::is_empty))
                        || function
                            .get("arguments")
                            .is_some_and(|args| !args.is_string())
                    {
                        return Err(SseError);
                    }
                }
            }
        }
        if let Some(reason) = choice.get("finish_reason").filter(|v| !v.is_null()) {
            let reason = reason.as_str().ok_or(SseError)?;
            if ![
                "stop",
                "length",
                "tool_calls",
                "function_call",
                "content_filter",
            ]
            .contains(&reason)
            {
                return Err(SseError);
            }
            let has_tools = has_tool_delta || self.tool_calls_mask & (1u64 << index) != 0;
            if (reason == "tool_calls" && !has_tools) || (reason == "stop" && has_tools) {
                return Err(SseError);
            }
        }
        Ok((index, has_tool_delta))
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
    const START: &[u8] =
        br#"data: {"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}

"#;
    const END: &[u8] = br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

"#;
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
    fn duplicate_done_is_rejected() {
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(END).unwrap();
        parser.push(b"data: [DONE]\n\n").unwrap();
        assert!(parser.push(b"data: [DONE]\n\n").is_err());
    }

    #[test]
    fn invalid_later_choice_cannot_commit_earlier_finish() {
        let mut parser = ChatSse::new(2).unwrap();
        let bad = br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":1,"delta":null,"finish_reason":null}]}

"#;
        assert!(parser.push(bad).is_err());
        assert_eq!(parser.finished_mask, 0);
    }

    #[test]
    fn rejects_trailing_bytes_after_done_in_same_and_later_chunk() {
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(END).unwrap();
        assert!(parser.push(b"data: [DONE]\n\nextra").is_err());
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(END).unwrap();
        parser.push(b"data: [DONE]\n\n").unwrap();
        assert!(parser.push(b"extra").is_err());
    }
    #[test]
    fn sixty_four_choices_finish_out_of_order_without_allocating_per_choice() {
        let mut parser = ChatSse::new(64).unwrap();
        let high: Vec<Value> = (32..64)
            .rev()
            .map(|index| serde_json::json!({"index":index,"delta":{},"finish_reason":"stop"}))
            .collect();
        let low: Vec<Value> = (0..32)
            .rev()
            .map(|index| serde_json::json!({"index":index,"delta":{},"finish_reason":"stop"}))
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
