//! Incremental streaming parser.
use bytes::Bytes;
use serde_json::Value;
use std::collections::HashSet;

const MAX_EVENT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct SseError;

#[derive(Debug, Default)]
pub struct ChatSse {
    line: Vec<u8>,
    data: Vec<u8>,
    finished: HashSet<u64>,
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
        for &byte in chunk {
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
                if self.finished.len() as u64 != self.expected {
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
            && (self.finished.len() as u64 != self.expected
                || !event.get("usage").is_some_and(Value::is_object))
        {
            return Err(SseError);
        }
        for choice in choices {
            self.process_choice(choice)?;
        }
        let mut normalized = Vec::with_capacity(data.len() + 8);
        normalized.extend_from_slice(b"data: ");
        normalized.extend_from_slice(&data);
        normalized.extend_from_slice(b"\n\n");
        emitted.push(Bytes::from(normalized));
        Ok(())
    }

    fn process_choice(&mut self, choice: &Value) -> Result<(), SseError> {
        let index = choice
            .get("index")
            .and_then(Value::as_u64)
            .ok_or(SseError)?;
        if index >= self.expected || self.finished.contains(&index) {
            return Err(SseError);
        }
        if !choice.get("delta").is_some_and(Value::is_object) {
            return Err(SseError);
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
            self.finished.insert(index);
        }
        Ok(())
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
    fn duplicate_done_is_rejected() {
        let mut parser = ChatSse::new(1).unwrap();
        parser.push(END).unwrap();
        parser.push(b"data: [DONE]\n\n").unwrap();
        assert!(parser.push(b"data: [DONE]\n\n").is_err());
    }
}
