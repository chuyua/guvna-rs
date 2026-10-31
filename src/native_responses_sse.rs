//! Bounded native Responses SSE framing and contract validation.
//! Unlike the Chat bridge, native output events are passed through unchanged.
use crate::sse::SseError;
use bytes::Bytes;
use serde_json::Value;

const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Default)]
pub(crate) struct NativeResponsesSse {
    line: Vec<u8>,
    data: Vec<u8>,
    event: Option<String>,
    response_id: Option<String>,
    seen_created: bool,
    done: bool,
    progress: bool,
    limit_hit: bool,
    last_sequence: Option<u64>,
}

impl NativeResponsesSse {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn hit_resource_limit(&self) -> bool {
        self.limit_hit
    }

    pub fn take_progress(&mut self) -> bool {
        std::mem::take(&mut self.progress)
    }

    pub fn finalize_on_eof(&mut self) -> Option<Bytes> {
        // A native stream needs an explicit terminal event. Never fabricate it.
        None
    }

    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Bytes>, SseError> {
        let mut emitted = Vec::new();
        if self.done && !chunk.is_empty() {
            return Err(SseError);
        }
        for &byte in chunk {
            if byte == b'\n' {
                let line = std::mem::take(&mut self.line);
                self.process_line(line.strip_suffix(b"\r").unwrap_or(&line), &mut emitted)?;
            } else {
                if self.line.len() >= MAX_FRAME_BYTES {
                    self.limit_hit = true;
                    return Err(SseError);
                }
                self.line.push(byte);
            }
        }
        Ok(emitted)
    }

    fn process_line(&mut self, line: &[u8], output: &mut Vec<Bytes>) -> Result<(), SseError> {
        if line.is_empty() {
            if self.data.is_empty() {
                self.event = None;
                return Ok(());
            }
            let data = std::mem::take(&mut self.data);
            let kind = self.event.take().ok_or(SseError)?;
            if self.done {
                if data == b"[DONE]" {
                    return Ok(());
                }
                return Err(SseError);
            }
            if data == b"[DONE]" {
                return Err(SseError);
            }
            let value: Value = serde_json::from_slice(&data).map_err(|_| SseError)?;
            if value.get("type").and_then(Value::as_str) != Some(kind.as_str()) {
                return Err(SseError);
            }
            if let Some(seq) = value.get("sequence_number") {
                let seq = seq.as_u64().ok_or(SseError)?;
                if self.last_sequence.is_some_and(|last| seq <= last) {
                    return Err(SseError);
                }
                self.last_sequence = Some(seq);
            }
            if !self.seen_created {
                if kind != "response.created" {
                    return Err(SseError);
                }
                let response = value.get("response").ok_or(SseError)?;
                let id = response
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or(SseError)?;
                if response.get("object").and_then(Value::as_str) != Some("response") {
                    return Err(SseError);
                }
                self.response_id = Some(id.to_owned());
                self.seen_created = true;
            } else if kind == "response.completed" || kind == "response.incomplete" {
                let response = value.get("response").ok_or(SseError)?;
                if !crate::gateway::valid_native_responses(response)
                    || response.get("status").and_then(Value::as_str)
                        != Some(if kind == "response.completed" {
                            "completed"
                        } else {
                            "incomplete"
                        })
                    || response.get("id").and_then(Value::as_str) != self.response_id.as_deref()
                {
                    return Err(SseError);
                }
                self.done = true;
            } else if kind == "response.failed" || kind == "error" || !kind.starts_with("response.")
            {
                return Err(SseError);
            }
            self.progress = true;
            let mut frame = Vec::with_capacity(data.len() + kind.len() + 20);
            frame.extend_from_slice(b"event: ");
            frame.extend_from_slice(kind.as_bytes());
            frame.extend_from_slice(b"\ndata: ");
            frame.extend_from_slice(&data);
            frame.extend_from_slice(b"\n\n");
            output.push(Bytes::from(frame));
            return Ok(());
        }
        if line.starts_with(b":") {
            return Ok(());
        }
        if let Some(event) = line.strip_prefix(b"event:") {
            let event = std::str::from_utf8(event).map_err(|_| SseError)?.trim();
            if event.is_empty() || event.len() > 128 || !event.is_ascii() {
                return Err(SseError);
            }
            self.event = Some(event.to_owned());
        } else if let Some(data) = line.strip_prefix(b"data:") {
            let data = data.strip_prefix(b" ").unwrap_or(data);
            if self.data.len().saturating_add(data.len() + 1) > MAX_FRAME_BYTES {
                self.limit_hit = true;
                return Err(SseError);
            }
            if !self.data.is_empty() {
                self.data.push(b'\n');
            }
            self.data.extend_from_slice(data);
        } else if !line.starts_with(b"id:") && !line.starts_with(b"retry:") {
            return Err(SseError);
        }
        Ok(())
    }

    pub fn is_completion_event(&self, chunk: &Bytes) -> bool {
        chunk.starts_with(b"event: response.completed\n")
            || chunk.starts_with(b"event: response.incomplete\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn frame(kind: &str, value: Value) -> Vec<u8> {
        format!("event: {kind}\ndata: {value}\n\n").into_bytes()
    }

    #[test]
    fn accepts_fragmented_native_stream_without_renaming_tool_items() {
        let mut stream = NativeResponsesSse::new();
        let created = frame(
            "response.created",
            json!({"type":"response.created",
            "sequence_number":0,"response":{"object":"response","id":"resp_1"}}),
        );
        let complete = frame(
            "response.completed",
            json!({"type":"response.completed",
            "sequence_number":1,"response":{"id":"resp_1","object":"response",
                "model":"physical","created_at":123,"status":"completed","error":null,
                "output":[{"type":"image_generation_call","result":"YQ=="}]}}),
        );
        let mut all = created;
        all.extend(complete);
        let mut outputs = Vec::new();
        for c in all.chunks(5) {
            outputs.extend(stream.push(c).unwrap());
        }
        assert_eq!(outputs.len(), 2);
        assert!(stream.is_completion_event(&outputs[1]));
        assert!(String::from_utf8_lossy(&outputs[1]).contains("image_generation_call"));
        assert!(stream.is_done());
    }

    #[test]
    fn missing_creation_and_false_completion_are_rejected() {
        let mut stream = NativeResponsesSse::new();
        assert!(
            stream
                .push(&frame(
                    "response.completed",
                    json!({"type":"response.completed","response":{}})
                ))
                .is_err()
        );
        let mut stream = NativeResponsesSse::new();
        let created = frame(
            "response.created",
            json!({"type":"response.created",
            "response":{"id":"resp_a","object":"response"}}),
        );
        stream.push(&created).unwrap();
        assert!(
            stream
                .push(&frame(
                    "response.failed",
                    json!({"type":"response.failed","response":{"id":"resp_a"}})
                ))
                .is_err()
        );
        assert!(!stream.is_done());
        assert!(stream.finalize_on_eof().is_none());
    }
}
