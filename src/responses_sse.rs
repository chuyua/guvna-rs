//! Bounded Chat Completions SSE to Responses SSE conversion.
//! The gateway upstream has already normalized and validated Chat SSE frames.
use crate::metrics::Metrics;
use axum::{
    body::{Body, Bytes},
    response::Response,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{collections::VecDeque, sync::Arc};

const MAX_FRAME: usize = 1024 * 1024 + 8;
const MAX_INPUT_CHUNK: usize = 2 * 1024 * 1024;
const MAX_TEXT: usize = 512 * 1024;
const MAX_TOOLS: usize = 32;
const MAX_TOOL_ARGUMENTS: usize = 256 * 1024;

struct Tool {
    id: String,
    name: String,
    arguments: String,
    output_index: usize,
}

struct StreamGuard {
    metrics: Arc<Metrics>,
    terminal_reported: bool,
}
impl StreamGuard {
    fn emitted(&mut self, frame: &[u8]) {
        if frame.starts_with(b"event: response.completed\n") {
            Metrics::inc(&self.metrics.responses_stream_completed);
            self.terminal_reported = true;
        } else if frame.starts_with(b"event: response.incomplete\n") {
            Metrics::inc(&self.metrics.responses_stream_incomplete);
            self.terminal_reported = true;
        } else if frame.starts_with(b"event: response.failed\n")
            || frame.starts_with(b"event: error\n")
        {
            Metrics::inc(&self.metrics.responses_stream_failed);
            self.terminal_reported = true;
        }
    }
}
impl Drop for StreamGuard {
    fn drop(&mut self) {
        if !self.terminal_reported {
            Metrics::inc(&self.metrics.responses_stream_cancelled);
        }
    }
}

pub(crate) fn wrap(
    response: Response,
    model: String,
    metadata: Value,
    metrics: Arc<Metrics>,
) -> Response {
    let source = response.into_body().into_data_stream();
    let translator = ResponsesSse::new(model, metadata);
    let guard = StreamGuard {
        metrics,
        terminal_reported: false,
    };
    let output = futures_util::stream::unfold(
        (source, translator, VecDeque::<Bytes>::new(), false, guard),
        |(mut source, mut t, mut pending, mut stopped, mut guard)| async move {
            loop {
                if let Some(next) = pending.pop_front() {
                    guard.emitted(&next);
                    return Some((
                        Ok::<Bytes, std::io::Error>(next),
                        (source, t, pending, stopped, guard),
                    ));
                }
                if stopped {
                    return None;
                }
                match source.next().await {
                    Some(Ok(chunk)) => {
                        if t.feed(&chunk, &mut pending).is_err() {
                            pending.push_back(t.failed());
                            stopped = true;
                        } else if t.done {
                            stopped = true;
                        }
                    }
                    _ => {
                        pending.push_back(t.failed());
                        stopped = true;
                    }
                }
            }
        },
    );
    Response::builder()
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("x-accel-buffering", "no")
        .body(Body::from_stream(output))
        .expect("static headers")
}

struct ResponsesSse {
    model: String,
    metadata: Value,
    buffer: Vec<u8>,
    scan_from: usize,
    id: Option<String>,
    created_at: Option<i64>,
    text: String,
    text_index: Option<usize>,
    next_output_index: usize,
    tools: Vec<Tool>,
    finish_reason: Option<String>,
    usage: Value,
    usage_seen: bool,
    sequence: u64,
    done: bool,
}
impl ResponsesSse {
    fn new(model: String, metadata: Value) -> Self {
        Self {
            model,
            metadata,
            buffer: Vec::new(),
            scan_from: 0,
            id: None,
            created_at: None,
            text: String::new(),
            text_index: None,
            next_output_index: 0,
            tools: Vec::new(),
            finish_reason: None,
            usage: Value::Null,
            usage_seen: false,
            sequence: 0,
            done: false,
        }
    }
    fn emit(&mut self, kind: &'static str, mut fields: Value) -> Bytes {
        fields["type"] = json!(kind);
        fields["sequence_number"] = json!(self.sequence);
        self.sequence += 1;
        Bytes::from(format!("event: {kind}\ndata: {fields}\n\n"))
    }
    fn message_id(&self) -> String {
        format!("msg_{}", self.id.as_deref().unwrap_or_default())
    }
    fn response(&self, status: &str) -> Value {
        let mut output = vec![Value::Null; self.next_output_index];
        if let Some(index) = self.text_index {
            output[index] = json!({"id":self.message_id(),"type":"message","status":status,
                "role":"assistant","content":[{"type":"output_text","text":self.text,"annotations":[]}]});
        }
        for tool in &self.tools {
            output[tool.output_index] = json!({"id":tool.id,"call_id":tool.id,
                "name":tool.name,"type":"function_call","status":status,
                "arguments":tool.arguments});
        }
        json!({"id":self.id,"object":"response","created_at":self.created_at,
            "model":self.model,"status":status,
            "incomplete_details": if status == "incomplete" {json!({"reason":"max_output_tokens"})} else {Value::Null},
            "output":output,
            "output_text":self.text,"usage":self.usage,"metadata":self.metadata})
    }
    fn begin(&mut self, event: &Value, pending: &mut VecDeque<Bytes>) -> Result<(), ()> {
        let id = event
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or(())?;
        self.id = Some(id.to_owned());
        self.created_at = event
            .get("created")
            .and_then(Value::as_i64)
            .filter(|ts| *ts >= 0);
        if self.created_at.is_none() && event.get("created").is_none() {
            // Timestamp the gateway Response if the upstream omits created.
            self.created_at = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| ())?
                    .as_secs()
                    .try_into()
                    .map_err(|_| ())?,
            );
        }
        if event.get("created").is_some() && self.created_at.is_none() {
            return Err(());
        }
        let first = self.response("in_progress");
        pending.push_back(self.emit("response.created", json!({"response":first})));
        let ongoing = self.response("in_progress");
        pending.push_back(self.emit("response.in_progress", json!({"response":ongoing})));
        Ok(())
    }

    fn begin_text(&mut self, pending: &mut VecDeque<Bytes>) {
        if self.text_index.is_some() {
            return;
        }
        let index = self.next_output_index;
        self.next_output_index += 1;
        self.text_index = Some(index);
        let message_id = self.message_id();
        pending.push_back(self.emit(
            "response.output_item.added",
            json!({
                "output_index":index,"item":{"id":message_id,"type":"message",
                    "status":"in_progress","role":"assistant","content":[]}
            }),
        ));
        let message_id = self.message_id();
        pending.push_back(self.emit(
            "response.content_part.added",
            json!({
                "item_id":message_id,"output_index":index,"content_index":0,
                "part":{"type":"output_text","text":"","annotations":[]}
            }),
        ));
    }

    fn tool_delta(&mut self, calls: &Value, pending: &mut VecDeque<Bytes>) -> Result<(), ()> {
        let calls = calls.as_array().ok_or(())?;
        if calls.is_empty() {
            return Err(());
        }
        for call in calls {
            let index = call.get("index").and_then(Value::as_u64).ok_or(())? as usize;
            if index > self.tools.len() || index >= MAX_TOOLS {
                return Err(());
            }
            if index == self.tools.len() {
                let id = call
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                    .ok_or(())?;
                let name = call
                    .pointer("/function/name")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                    .ok_or(())?;
                if call.get("type").is_some_and(|kind| kind != "function")
                    || self.tools.iter().any(|tool| tool.id == id)
                {
                    return Err(());
                }
                let output_index = self.next_output_index;
                self.next_output_index += 1;
                self.tools.push(Tool {
                    id: id.into(),
                    name: name.into(),
                    arguments: String::new(),
                    output_index,
                });
                pending.push_back(self.emit(
                    "response.output_item.added",
                    json!({
                        "output_index":output_index,
                        "item":{"id":id,"call_id":id,"type":"function_call",
                            "name":name,"status":"in_progress","arguments":""}
                    }),
                ));
            }
            let tool = &self.tools[index];
            if call.get("id").is_some_and(|value| value != &tool.id)
                || call.get("type").is_some_and(|value| value != "function")
                || call
                    .pointer("/function/name")
                    .is_some_and(|value| value != &tool.name)
            {
                return Err(());
            }
            let fragment = call
                .pointer("/function/arguments")
                .map(|v| v.as_str().ok_or(()))
                .transpose()?
                .unwrap_or("");
            let total = self
                .tools
                .iter()
                .map(|tool| tool.arguments.len())
                .sum::<usize>();
            if fragment.len() > MAX_TOOL_ARGUMENTS.saturating_sub(self.tools[index].arguments.len())
                || fragment.len() > MAX_TEXT.saturating_sub(total.saturating_add(self.text.len()))
            {
                return Err(());
            }
            self.tools[index].arguments.push_str(fragment);
            if !fragment.is_empty() {
                let id = self.tools[index].id.clone();
                let output_index = self.tools[index].output_index;
                pending.push_back(self.emit(
                    "response.function_call_arguments.delta",
                    json!({
                        "item_id":id,"output_index":output_index,"delta":fragment
                    }),
                ));
            }
        }
        Ok(())
    }
    fn feed(&mut self, chunk: &[u8], pending: &mut VecDeque<Bytes>) -> Result<(), ()> {
        // The Chat parser has already bounded upstream chunks to 2 MiB.
        // Several independent <=1 MiB events may legitimately share one chunk.
        if self.done || chunk.len() > MAX_INPUT_CHUNK {
            return Err(());
        }
        // Normal gateway operation produces exactly one normalized event per
        // chunk. Process it directly, avoiding any frame clone/buffer allocation.
        if self.buffer.is_empty()
            && chunk.len() <= MAX_FRAME
            && chunk.ends_with(b"\n\n")
            && chunk.windows(2).position(|pair| pair == b"\n\n") == Some(chunk.len() - 2)
        {
            return self.frame(chunk, pending);
        }
        self.buffer.extend_from_slice(chunk);
        let mut consumed = 0;
        let mut scan = self.scan_from;
        while let Some(offset) = self.buffer[scan..]
            .windows(2)
            .position(|pair| pair == b"\n\n")
        {
            let end = scan + offset + 2;
            if end - consumed > MAX_FRAME {
                return Err(());
            }
            // Never emit success if the same chunk has forbidden trailing data.
            // Check before frame() mutates the terminal state.
            if self.buffer[consumed..end] == *b"data: [DONE]\n\n" && end != self.buffer.len() {
                return Err(());
            }
            let frame = self.buffer[consumed..end].to_vec();
            self.frame(&frame, pending)?;
            consumed = end;
            scan = end;
            if self.done && end != self.buffer.len() {
                return Err(());
            }
        }
        // Drain once, not per event. Retain only the unfinished suffix.
        if consumed != 0 {
            self.buffer.drain(..consumed);
        }
        if self.buffer.len() > MAX_FRAME {
            return Err(());
        }
        // Previously checked all pairs before this index. A future chunk only
        // needs to inspect the last previous byte and its newly appended bytes.
        self.scan_from = self.buffer.len().saturating_sub(1);
        Ok(())
    }
    fn frame(&mut self, frame: &[u8], pending: &mut VecDeque<Bytes>) -> Result<(), ()> {
        let data = frame
            .strip_prefix(b"data: ")
            .and_then(|x| x.strip_suffix(b"\n\n"))
            .ok_or(())?;
        if data == b"[DONE]" {
            if self.finish_reason.is_none() || self.id.is_none() {
                return Err(());
            }
            self.complete(pending);
            self.done = true;
            return Ok(());
        }
        let event: Value = serde_json::from_slice(data).map_err(|_| ())?;
        let choices = event.get("choices").and_then(Value::as_array).ok_or(())?;
        if choices.is_empty() {
            if self.finish_reason.is_none() || self.usage_seen {
                return Err(());
            }
            let usage = event.get("usage").ok_or(())?;
            let (Some(input), Some(output), Some(total)) = (
                usage["prompt_tokens"].as_u64(),
                usage["completion_tokens"].as_u64(),
                usage["total_tokens"].as_u64(),
            ) else {
                return Err(());
            };
            if input.checked_add(output) != Some(total) {
                return Err(());
            }
            self.usage = json!({"input_tokens":input,"output_tokens":output,"total_tokens":total});
            self.usage_seen = true;
            return Ok(());
        }
        if choices.len() != 1 || choices[0]["index"] != 0 || self.finish_reason.is_some() {
            return Err(());
        }
        if let Some(id) = event.get("id") {
            let current = id.as_str().filter(|id| !id.is_empty()).ok_or(())?;
            if self.id.as_deref().is_some_and(|saved| saved != current) {
                return Err(());
            }
        }
        if self.id.is_none() {
            self.begin(&event, pending)?;
        }
        let choice = &choices[0];
        let delta = choice.get("delta").and_then(Value::as_object).ok_or(())?;
        if delta
            .keys()
            .any(|key| !["role", "content", "tool_calls"].contains(&key.as_str()))
            || delta.get("role").is_some_and(|role| role != "assistant")
        {
            return Err(());
        }
        if let Some(content) = delta.get("content").filter(|part| !part.is_null()) {
            let part = content.as_str().ok_or(())?;
            if !part.is_empty() {
                if !self.tools.is_empty() {
                    return Err(());
                }
                let used = self
                    .tools
                    .iter()
                    .map(|tool| tool.arguments.len())
                    .sum::<usize>();
                if part.len() > MAX_TEXT.saturating_sub(self.text.len().saturating_add(used)) {
                    return Err(());
                }
                self.begin_text(pending);
                self.text.push_str(part);
                let item_id = self.message_id();
                pending.push_back(self.emit(
                    "response.output_text.delta",
                    json!({
                        "item_id":item_id,"output_index":self.text_index,"content_index":0,
                        "delta":part,"logprobs":[]
                    }),
                ));
            }
        }
        if let Some(calls) = delta.get("tool_calls") {
            self.tool_delta(calls, pending)?;
        }
        if let Some(reason) = choice.get("finish_reason").filter(|v| !v.is_null()) {
            let reason = reason.as_str().ok_or(())?;
            match reason {
                "tool_calls" if !self.tools.is_empty() => {
                    if !self.tools.iter().all(|tool| {
                        serde_json::from_str::<Value>(&tool.arguments)
                            .is_ok_and(|value| value.is_object())
                    }) {
                        return Err(());
                    }
                }
                "stop" if self.tools.is_empty() => {}
                "length" if self.tools.is_empty() => {}
                _ => return Err(()),
            }
            self.finish_reason = Some(reason.to_owned());
        }
        Ok(())
    }
    fn complete(&mut self, pending: &mut VecDeque<Bytes>) {
        let status = if self.finish_reason.as_deref() == Some("length") {
            "incomplete"
        } else {
            "completed"
        };
        if self.text_index.is_none() && self.tools.is_empty() {
            self.begin_text(pending);
        }
        if let Some(index) = self.text_index {
            let item_id = self.message_id();
            pending.push_back(self.emit(
                "response.output_text.done",
                json!({
                    "item_id":item_id,"output_index":index,"content_index":0,
                    "text":self.text,"logprobs":[]
                }),
            ));
            let item_id = self.message_id();
            pending.push_back(self.emit(
                "response.content_part.done",
                json!({
                    "item_id":item_id,"output_index":index,"content_index":0,
                    "part":{"type":"output_text","text":self.text,"annotations":[]}
                }),
            ));
            let item = json!({"id":self.message_id(),"type":"message","status":status,
                "role":"assistant","content":[{"type":"output_text","text":self.text,"annotations":[]}]});
            pending.push_back(self.emit(
                "response.output_item.done",
                json!({
                    "output_index":index,"item":item
                }),
            ));
        }
        for index in 0..self.tools.len() {
            let tool = &self.tools[index];
            let (id, name, arguments, output_index) = (
                tool.id.clone(),
                tool.name.clone(),
                tool.arguments.clone(),
                tool.output_index,
            );
            pending.push_back(self.emit(
                "response.function_call_arguments.done",
                json!({
                    "item_id":id,"output_index":output_index,"arguments":arguments
                }),
            ));
            pending.push_back(self.emit(
                "response.output_item.done",
                json!({
                    "output_index":output_index,
                    "item":{"id":id,"call_id":id,"name":name,"arguments":arguments,
                        "type":"function_call","status":"completed"}
                }),
            ));
        }
        let kind = if status == "incomplete" {
            "response.incomplete"
        } else {
            "response.completed"
        };
        let response = self.response(status);
        pending.push_back(self.emit(kind, json!({"response":response})));
    }
    fn failed(&mut self) -> Bytes {
        if self.id.is_some() {
            let mut failed = self.response("failed");
            failed["error"] = json!({"code":"upstream_stream_error",
                "message":"Upstream stream ended or contained invalid data"});
            self.emit("response.failed", json!({"response":failed}))
        } else {
            self.emit(
                "error",
                json!({"code":"upstream_stream_error",
                "message":"Upstream stream ended before a valid response"}),
            )
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn frame(event: Value) -> Bytes {
        Bytes::from(format!("data: {event}\n\n"))
    }
    #[test]
    fn fragments_usage_and_success() {
        let mut t = ResponsesSse::new("logical".into(), json!({"trace":"x"}));
        let mut q = VecDeque::new();
        let start = frame(json!({"id":"chat1","created":123,
            "choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":null}]}));
        for byte in start {
            t.feed(&[byte], &mut q).unwrap();
        }
        assert_eq!(q.len(), 5);
        t.feed(&frame(json!({"id":"chat1","choices":[{"index":0,"delta":{"content":"!"},"finish_reason":"stop"}]})), &mut q).unwrap();
        t.feed(&frame(json!({"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}})), &mut q).unwrap();
        t.feed(b"data: [DONE]\n\n", &mut q).unwrap();
        assert!(t.done);
        assert_eq!(t.text, "hi!");
        let completed = String::from_utf8_lossy(q.back().unwrap());
        assert!(completed.contains("response.completed"));
        assert!(completed.contains("input_tokens"));
        assert!(completed.contains("trace"));
    }
    #[test]
    fn length_and_failed_streams_are_distinct() {
        let mut t = ResponsesSse::new("logical".into(), Value::Null);
        let mut q = VecDeque::new();
        t.feed(&frame(json!({"id":"x","choices":[{"index":0,"delta":{"content":"short"},"finish_reason":"length"}]})), &mut q).unwrap();
        t.feed(b"data: [DONE]\n\n", &mut q).unwrap();
        assert!(String::from_utf8_lossy(q.back().unwrap()).contains("response.incomplete"));
        let mut t = ResponsesSse::new("logical".into(), Value::Null);
        let mut q = VecDeque::new();
        t.feed(&frame(json!({"id":"x","choices":[{"index":0,"delta":{"content":"lost"},"finish_reason":null}]})), &mut q).unwrap();
        assert!(t.feed(b"data: [DONE]\n\n", &mut q).is_err());
        assert!(String::from_utf8_lossy(&t.failed()).contains("response.failed"));
    }
    #[test]
    fn bounded_malformed_and_tools() {
        let mut t = ResponsesSse::new("x".into(), Value::Null);
        let mut q = VecDeque::new();
        assert!(t.feed(&frame(json!({"id":"x","choices":[{"index":0,"delta":{"tool_calls":[]},"finish_reason":null}]})), &mut q).is_err());
        assert!(t.feed(&vec![b'x'; MAX_FRAME + 1], &mut q).is_err());
        let mut t = ResponsesSse::new("x".into(), Value::Null);
        t.feed(&frame(json!({"id":"x","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]})), &mut q).unwrap();
        assert!(t.feed(&frame(json!({"id":"changed","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})), &mut q).is_err());
    }
    #[test]
    fn input_and_output_are_bounded() {
        let mut t = ResponsesSse::new("x".into(), Value::Null);
        let mut q = VecDeque::new();
        let large = "x".repeat(MAX_TEXT);
        t.feed(
            &frame(json!({"id":"x","choices":[{"index":0,
            "delta":{"content":large},"finish_reason":null}]})),
            &mut q,
        )
        .unwrap();
        assert_eq!(t.text.len(), MAX_TEXT);
        assert!(
            t.feed(
                &frame(json!({"id":"x","choices":[{"index":0,
            "delta":{"content":"one extra byte"},"finish_reason":null}]})),
                &mut q
            )
            .is_err()
        );
        assert_eq!(t.text.len(), MAX_TEXT);

        let mut t = ResponsesSse::new("x".into(), Value::Null);
        let mut q = VecDeque::new();
        assert!(
            t.feed(
                &frame(json!({"id":"x","choices":[{"index":0,
            "delta":{"tool_calls":[{"index":0,"id":"call0","type":"function",
                "function":{"name":"search","arguments":"a".repeat(MAX_TOOL_ARGUMENTS + 1)}}]},
            "finish_reason":null}]})),
                &mut q
            )
            .is_err()
        );
    }

    #[test]
    fn invalid_usage_and_terminal_events_never_complete() {
        let mut t = ResponsesSse::new("x".into(), Value::Null);
        let mut q = VecDeque::new();
        t.feed(
            &frame(json!({"id":"x","choices":[{"index":0,
            "delta":{"content":"done"},"finish_reason":"stop"}]})),
            &mut q,
        )
        .unwrap();
        let usage = frame(json!({"choices":[],"usage":{
            "prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}));
        t.feed(&usage, &mut q).unwrap();
        assert!(t.feed(&usage, &mut q).is_err());
        assert!(!t.done);
        assert!(t.feed(b"data: [DONE]\n\ndata: extra\n\n", &mut q).is_err());
        assert!(
            !q.iter()
                .any(|event| event.starts_with(b"event: response.completed"))
        );
    }

    #[test]
    fn rejects_tool_index_gaps_and_too_many_parallel_tools() {
        let mut t = ResponsesSse::new("x".into(), Value::Null);
        let mut q = VecDeque::new();
        assert!(
            t.feed(
                &frame(json!({"id":"x","choices":[{"index":0,
            "delta":{"tool_calls":[{"index":1,"id":"skip","type":"function",
                "function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":null}]})),
                &mut q
            )
            .is_err()
        );
        let mut t = ResponsesSse::new("x".into(), Value::Null);
        let mut q = VecDeque::new();
        for index in 0..MAX_TOOLS {
            t.feed(
                &frame(json!({"id":"x","choices":[{"index":0,
                "delta":{"tool_calls":[{"index":index,"id":format!("call{index}"),
                    "type":"function","function":{"name":"lookup","arguments":"{}"}}]},
                "finish_reason":null}]})),
                &mut q,
            )
            .unwrap();
        }
        assert_eq!(t.tools.len(), MAX_TOOLS);
        assert!(
            t.feed(
                &frame(json!({"id":"x","choices":[{"index":0,
            "delta":{"tool_calls":[{"index":MAX_TOOLS,"id":"extra","type":"function",
                "function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":null}]})),
                &mut q
            )
            .is_err()
        );
    }
    #[test]
    fn single_complete_frames_do_not_allocate_an_intermediate_frame_buffer() {
        let mut translator = ResponsesSse::new("demo".into(), Value::Null);
        let mut output = VecDeque::new();
        let first = frame(json!({
            "id":"chat","choices":[{"index":0,
                "delta":{"content":"hello"},"finish_reason":"stop"}]
        }));
        translator.feed(&first, &mut output).unwrap();
        assert_eq!(translator.buffer.capacity(), 0);
        translator.feed(b"data: [DONE]\n\n", &mut output).unwrap();
        assert_eq!(translator.buffer.capacity(), 0);
        assert!(translator.done);
    }
    #[test]
    fn one_byte_chunks_have_linear_scanning_and_complete_once() {
        let mut translator = ResponsesSse::new("demo".into(), Value::Null);
        let mut output = VecDeque::new();
        let large = frame(json!({
            "id":"chat","padding":"a".repeat(128 * 1024),
            "choices":[{"index":0,"delta":{"role":"assistant","content":"hello"},
                "finish_reason":"stop"}]
        }));
        for byte in large {
            translator.feed(&[byte], &mut output).unwrap();
        }
        assert_eq!(translator.text, "hello");
        translator.feed(b"data: [DONE]\n\n", &mut output).unwrap();
        assert_eq!(
            output
                .iter()
                .filter(|event| event.starts_with(b"event: response.completed"))
                .count(),
            1
        );
        assert_eq!(translator.scan_from, 0);
        assert!(translator.buffer.is_empty());
    }

    #[test]
    fn coalesced_valid_frames_can_exceed_one_frame_limit() {
        let mut translator = ResponsesSse::new("demo".into(), Value::Null);
        let mut output = VecDeque::new();
        let first = frame(json!({
            "id":"chat","padding":"x".repeat(600 * 1024),
            "choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]
        }));
        let last = frame(json!({
            "id":"chat","padding":"y".repeat(600 * 1024),
            "choices":[{"index":0,"delta":{"content":"!"},"finish_reason":"stop"}]
        }));
        assert!(first.len() < MAX_FRAME);
        assert!(last.len() < MAX_FRAME);
        let mut together = first.to_vec();
        together.extend_from_slice(&last);
        assert!(together.len() > MAX_FRAME);
        assert!(together.len() < MAX_INPUT_CHUNK);
        translator.feed(&together, &mut output).unwrap();
        translator.feed(b"data: [DONE]\n\n", &mut output).unwrap();
        assert_eq!(translator.text, "hi!");
        assert!(translator.done);
        assert_eq!(
            output
                .iter()
                .filter(|event| event.starts_with(b"event: response.completed"))
                .count(),
            1
        );
    }

    #[test]
    fn oversized_frame_and_trailing_after_terminal_fail_closed() {
        let mut translator = ResponsesSse::new("demo".into(), Value::Null);
        let mut output = VecDeque::new();
        let oversized = frame(json!({
            "id":"chat","padding":"b".repeat(MAX_FRAME),
            "choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":"stop"}]
        }));
        assert!(oversized.len() > MAX_FRAME);
        assert!(oversized.len() < MAX_INPUT_CHUNK);
        assert!(translator.feed(&oversized, &mut output).is_err());
        assert!(output.is_empty());

        let mut translator = ResponsesSse::new("demo".into(), Value::Null);
        let mut output = VecDeque::new();
        translator
            .feed(
                &frame(json!({
                    "id":"chat","choices":[{"index":0,"delta":{"content":"hi"},
                        "finish_reason":"stop"}]
                })),
                &mut output,
            )
            .unwrap();
        assert!(
            translator
                .feed(b"data: [DONE]\n\ndata: extra\n\n", &mut output)
                .is_err()
        );
        assert!(
            !output
                .iter()
                .any(|event| event.starts_with(b"event: response.completed"))
        );
    }
}
