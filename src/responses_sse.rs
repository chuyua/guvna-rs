//! Bounded Chat Completions SSE to Responses SSE conversion.
//! The gateway upstream has already normalized and validated Chat SSE frames.
use crate::metrics::Metrics;
use axum::{
    body::{Body, Bytes},
    response::Response,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::VecDeque, sync::Arc};

const MAX_FRAME: usize = 1024 * 1024 + 8;
const MAX_INPUT_CHUNK: usize = 2 * 1024 * 1024;
const MAX_TEXT: usize = 512 * 1024;
const MAX_TOOLS: usize = 32;
const MAX_TOOL_ARGUMENTS: usize = 256 * 1024;
static SYNTHETIC_STREAM_IDS: AtomicU64 = AtomicU64::new(1);

struct Tool {
    id: String,
    name: String,
    arguments: String,
    arguments_seen: bool,
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
    json_mode: Option<bool>,
) -> Response {
    let source = response.into_body().into_data_stream();
    let mut translator = ResponsesSse::new(model, metadata);
    // Some(true) requires a JSON object; Some(false) requires valid JSON
    // for the configured schema mode. Omit this for default text/tool streams.
    translator.json_mode = json_mode;
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
    json_mode: Option<bool>,
    has_refusal: bool,
    buffer: Vec<u8>,
    scan_from: usize,
    id: Option<String>,
    source_id: Option<String>,
    created_at: Option<i64>,
    text: String,
    text_index: Option<usize>,
    text_closed: bool,
    text_part_index: Option<usize>,
    refusal: String,
    refusal_part_index: Option<usize>,
    reasoning_text: String,
    reasoning_index: Option<usize>,
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
            json_mode: None,
            has_refusal: false,
            buffer: Vec::new(),
            scan_from: 0,
            id: None,
            source_id: None,
            created_at: None,
            text: String::new(),
            text_index: None,
            text_closed: false,
            text_part_index: None,
            refusal: String::new(),
            refusal_part_index: None,
            reasoning_text: String::new(),
            reasoning_index: None,
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
    fn reasoning_id(&self) -> String {
        format!("rs_{}", self.id.as_deref().unwrap_or_default())
    }
    fn message_parts(&self) -> Vec<Value> {
        let count = usize::from(self.text_part_index.is_some())
            + usize::from(self.refusal_part_index.is_some());
        let mut parts = vec![Value::Null; count];
        if let Some(index) = self.text_part_index {
            parts[index] = json!({"type":"output_text","text":self.text,"annotations":[]});
        }
        if let Some(index) = self.refusal_part_index {
            parts[index] = json!({"type":"refusal","refusal":self.refusal});
        }
        parts
    }
    fn response(&self, status: &str) -> Value {
        let mut output = vec![Value::Null; self.next_output_index];
        if let Some(index) = self.text_index {
            output[index] = json!({"id":self.message_id(),"type":"message","status":status,
                "role":"assistant","content":self.message_parts()});
        }
        if let Some(index) = self.reasoning_index {
            output[index] = json!({"id":self.reasoning_id(),"type":"reasoning","status":status,
                "summary":[],"content":[{"type":"reasoning_text","text":self.reasoning_text}]});
        }
        for tool in &self.tools {
            output[tool.output_index] = json!({"id":tool.id,"call_id":tool.id,
                "name":tool.name,"type":"function_call","status":status,
                "arguments":tool.arguments});
        }
        json!({"id":self.id,"object":"response","created_at":self.created_at,
            "model":self.model,"status":status,
            "error":null, "store":false, "background":false,
            "previous_response_id":null, "truncation":"disabled",
            "incomplete_details": if status == "incomplete" {
                json!({"reason": if self.finish_reason.as_deref() == Some("content_filter") {
                    "content_filter"
                } else {
                    "max_output_tokens"
                }})
            } else {Value::Null},
            "output":output,
            "output_text":self.text,"usage":self.usage,"metadata":self.metadata})
    }
    fn begin(&mut self, event: &Value, pending: &mut VecDeque<Bytes>) -> Result<(), ()> {
        let id = event
            .get("id")
            .map(|id| id.as_str().filter(|value| !value.is_empty()).ok_or(()))
            .transpose()?
            .map(str::to_owned)
            .unwrap_or_else(|| {
                format!(
                    "chatcmpl-rs-{}-{}-{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis(),
                    std::process::id(),
                    SYNTHETIC_STREAM_IDS.fetch_add(1, Ordering::Relaxed)
                )
            });
        self.id = Some(id);
        self.created_at = event
            .get("created")
            .and_then(|ts| {
                ts.as_i64().or_else(|| {
                    ts.as_f64()
                        .filter(|n| {
                            n.is_finite() && *n >= 0.0 && *n < i64::MAX as f64 && n.fract() == 0.0
                        })
                        .map(|n| n as i64)
                })
            })
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

    fn begin_message(&mut self, pending: &mut VecDeque<Bytes>) {
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
    }

    fn begin_text(&mut self, pending: &mut VecDeque<Bytes>) {
        self.begin_message(pending);
        if self.text_part_index.is_some() {
            return;
        }
        let content_index = usize::from(self.refusal_part_index.is_some());
        self.text_part_index = Some(content_index);
        let message_id = self.message_id();
        pending.push_back(self.emit(
            "response.content_part.added",
            json!({
                "item_id":message_id,"output_index":self.text_index,"content_index":content_index,
                "part":{"type":"output_text","text":"","annotations":[]}
            }),
        ));
    }

    fn begin_refusal(&mut self, pending: &mut VecDeque<Bytes>) {
        self.begin_message(pending);
        if self.refusal_part_index.is_some() {
            return;
        }
        let content_index = usize::from(self.text_part_index.is_some());
        self.refusal_part_index = Some(content_index);
        let message_id = self.message_id();
        pending.push_back(self.emit(
            "response.content_part.added",
            json!({
                "item_id":message_id,"output_index":self.text_index,"content_index":content_index,
                "part":{"type":"refusal","refusal":""}
            }),
        ));
    }

    fn begin_reasoning(&mut self, pending: &mut VecDeque<Bytes>) {
        if self.reasoning_index.is_some() {
            return;
        }
        let index = self.next_output_index;
        self.next_output_index += 1;
        self.reasoning_index = Some(index);
        let item_id = self.reasoning_id();
        pending.push_back(self.emit(
            "response.output_item.added",
            json!({
                "output_index":index,
                "item":{"id":item_id,"type":"reasoning","status":"in_progress",
                    "summary":[],"content":[]}
            }),
        ));
        pending.push_back(self.emit(
            "response.content_part.added",
            json!({
                "item_id":item_id,"output_index":index,"content_index":0,
                "part":{"type":"reasoning_text","text":""}
            }),
        ));
    }

    fn tool_delta(&mut self, calls: &Value, pending: &mut VecDeque<Bytes>) -> Result<(), ()> {
        let calls = calls.as_array().ok_or(())?;
        if calls.is_empty() {
            return Err(());
        }
        // An output item must close before a later function_call item opens.
        // Do not delay a preceding message's .done events until [DONE].
        self.close_text(pending, "completed");
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
                    arguments_seen: false,
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
            let fragment_value = call.pointer("/function/arguments");
            let fragment = fragment_value
                .map(|v| v.as_str().ok_or(()))
                .transpose()?
                .unwrap_or("");
            let total = self
                .tools
                .iter()
                .map(|tool| tool.arguments.len())
                .sum::<usize>();
            if fragment.len() > MAX_TOOL_ARGUMENTS.saturating_sub(self.tools[index].arguments.len())
                || fragment.len()
                    > MAX_TEXT.saturating_sub(
                        total
                            .saturating_add(self.text.len())
                            .saturating_add(self.reasoning_text.len()),
                    )
            {
                return Err(());
            }
            self.tools[index].arguments.push_str(fragment);
            self.tools[index].arguments_seen |= fragment_value.is_some();
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
            if self.finish_reason.as_deref() == Some("stop")
                && !self.has_refusal
                && let Some(only_object) = self.json_mode
            {
                // A model may ignore response_format even after a valid 200
                // streaming handshake. Check the bounded assembled text
                // before ever emitting a completed terminal event. Once any
                // delta has been sent we cannot replay another provider.
                let valid = crate::sse::valid_json_output(&self.text, only_object);
                if !valid {
                    return Err(());
                }
            }
            self.complete(pending);
            self.done = true;
            return Ok(());
        }
        let event: Value = serde_json::from_slice(data).map_err(|_| ())?;
        let choices = event.get("choices").and_then(Value::as_array).ok_or(())?;
        // An inline usage sample is useful if no trailer follows. Preserve
        // it as a fallback and let an authoritative empty-choices trailer
        // replace it without counting that trailer more than once.
        if choices.is_empty() && (self.usage_seen || self.finish_reason.is_none()) {
            return Err(());
        }
        if event.get("usage").is_some_and(|usage| !usage.is_null()) {
            self.usage = crate::gateway::translated_responses_usage(&event)?;
        }
        if choices.is_empty() {
            if event.get("usage").is_none_or(Value::is_null) {
                return Err(());
            }
            self.usage_seen = true;
            return Ok(());
        }
        if choices.len() != 1 || choices[0]["index"] != 0 || self.finish_reason.is_some() {
            return Err(());
        }
        if let Some(id) = event.get("id") {
            let current = id.as_str().filter(|id| !id.is_empty()).ok_or(())?;
            if self
                .source_id
                .as_deref()
                .is_some_and(|saved| saved != current)
            {
                return Err(());
            }
            if self.source_id.is_none() {
                self.source_id = Some(current.to_owned());
            }
        }
        if self.id.is_none() {
            self.begin(&event, pending)?;
        }
        let choice = &choices[0];
        let empty_delta = serde_json::Map::new();
        let delta = match choice.get("delta") {
            Some(Value::Object(delta)) => delta,
            Some(Value::Null) if choice.get("finish_reason").is_some_and(|v| !v.is_null()) => {
                &empty_delta
            }
            _ => return Err(()),
        };
        if delta.keys().any(|key| {
            ![
                "role",
                "content",
                "tool_calls",
                "reasoning",
                "reasoning_content",
                "refusal",
            ]
            .contains(&key.as_str())
        }) || delta.get("role").is_some_and(|role| role != "assistant")
        {
            return Err(());
        }
        // Some compatible providers emit both fields in the same delta.
        // Preserve both fragments in order instead of failing a valid stream.
        for field in ["reasoning", "reasoning_content"] {
            let Some(part) = delta.get(field).filter(|value| !value.is_null()) else {
                continue;
            };
            if field == "reasoning_content" && delta.get("reasoning") == delta.get(field) {
                continue;
            }
            let part = part.as_str().ok_or(())?;
            if part.is_empty() {
                continue;
            }
            let used = self
                .tools
                .iter()
                .map(|tool| tool.arguments.len())
                .sum::<usize>();
            if part.len()
                > MAX_TEXT.saturating_sub(
                    self.text
                        .len()
                        .saturating_add(self.refusal.len())
                        .saturating_add(self.reasoning_text.len())
                        .saturating_add(used),
                )
            {
                return Err(());
            }
            self.begin_reasoning(pending);
            self.reasoning_text.push_str(part);
            let item_id = self.reasoning_id();
            let output_index = self.reasoning_index.ok_or(())?;
            pending.push_back(self.emit(
                "response.reasoning_text.delta",
                json!({
                    "item_id":item_id,"output_index":output_index,"content_index":0,
                    "delta":part
                }),
            ));
        }
        for field in ["content", "refusal"] {
            if let Some(content) = delta.get(field).filter(|part| !part.is_null()) {
                let part = content.as_str().ok_or(())?;
                if field == "refusal" && !part.is_empty() {
                    self.has_refusal = true;
                }
                // Some upstreams mirror the refusal in content in the SAME
                // delta. Emit only the typed refusal, never a duplicate text.
                if field == "content" && delta.get("refusal") == Some(content) && !part.is_empty() {
                    continue;
                }
                if !part.is_empty() {
                    if !self.tools.is_empty() {
                        return Err(());
                    }
                    let used = self
                        .tools
                        .iter()
                        .map(|tool| tool.arguments.len())
                        .sum::<usize>();
                    if part.len()
                        > MAX_TEXT.saturating_sub(
                            self.text
                                .len()
                                .saturating_add(self.refusal.len())
                                .saturating_add(self.reasoning_text.len())
                                .saturating_add(used),
                        )
                    {
                        return Err(());
                    }
                    let item_id = self.message_id();
                    if field == "refusal" {
                        self.begin_refusal(pending);
                        self.refusal.push_str(part);
                        pending.push_back(self.emit(
                            "response.refusal.delta",
                            json!({
                                "item_id":item_id,
                                "output_index":self.text_index,
                                "content_index":self.refusal_part_index,
                                "delta":part
                            }),
                        ));
                    } else {
                        self.begin_text(pending);
                        self.text.push_str(part);
                        pending.push_back(self.emit(
                            "response.output_text.delta",
                            json!({
                                "item_id":item_id,
                                "output_index":self.text_index,
                                "content_index":self.text_part_index,
                                "delta":part,"logprobs":[]
                            }),
                        ));
                    }
                }
            }
        } // content/refusal parts
        if let Some(calls) = delta.get("tool_calls") {
            self.tool_delta(calls, pending)?;
        }
        if let Some(reason) = choice.get("finish_reason").filter(|v| !v.is_null()) {
            let reason = reason.as_str().ok_or(())?;
            match reason {
                "tool_calls" if !self.tools.is_empty() => {
                    if !self.tools.iter().all(|tool| {
                        tool.arguments_seen
                            && (tool.arguments.is_empty()
                                || crate::sse::valid_json_output(&tool.arguments, true))
                    }) {
                        return Err(());
                    }
                    for tool in &mut self.tools {
                        if tool.arguments.is_empty() {
                            tool.arguments.push_str("{}");
                        }
                    }
                }
                "stop" if self.tools.is_empty() => {}
                "length" => {}
                "content_filter" => {}
                _ => return Err(()),
            }
            self.finish_reason = Some(reason.to_owned());
        }
        Ok(())
    }
    fn complete(&mut self, pending: &mut VecDeque<Bytes>) {
        let status = if matches!(
            self.finish_reason.as_deref(),
            Some("length" | "content_filter")
        ) {
            "incomplete"
        } else {
            "completed"
        };
        if self.text_index.is_none() && self.tools.is_empty() && self.reasoning_index.is_none() {
            self.begin_text(pending);
        }
        // Output-item terminal events must follow their assigned indices,
        // not a hard-coded reasoning/text/tool category order.
        for output_index in 0..self.next_output_index {
            if self.reasoning_index == Some(output_index) {
                let index = output_index;
                let item_id = self.reasoning_id();
                pending.push_back(self.emit(
                    "response.reasoning_text.done",
                    json!({
                        "item_id":item_id,"output_index":index,"content_index":0,
                        "text":self.reasoning_text
                    }),
                ));
                let item_id = self.reasoning_id();
                pending.push_back(self.emit(
                    "response.content_part.done",
                    json!({
                        "item_id":item_id,"output_index":index,"content_index":0,
                        "part":{"type":"reasoning_text","text":self.reasoning_text}
                    }),
                ));
                let item = json!({"id":self.reasoning_id(),"type":"reasoning","status":status,
                "summary":[],"content":[{"type":"reasoning_text","text":self.reasoning_text}]});
                pending.push_back(self.emit(
                    "response.output_item.done",
                    json!({"output_index":index,"item":item}),
                ));
            }
            if self.text_index == Some(output_index) {
                self.close_text(pending, status);
            }
            if let Some(index) = self
                .tools
                .iter()
                .position(|tool| tool.output_index == output_index)
            {
                let tool = &self.tools[index];
                let (id, name, arguments, output_index) = (
                    tool.id.clone(),
                    tool.name.clone(),
                    tool.arguments.clone(),
                    tool.output_index,
                );
                // A max-token/content-filter truncation can leave JSON
                // arguments unfinished. An arguments.done event tells a
                // consumer it may execute this call; do not emit it for
                // incomplete calls even though the output item itself must
                // terminate with an incomplete status.
                if status == "completed" {
                    pending.push_back(self.emit(
                        "response.function_call_arguments.done",
                        json!({
                            "item_id":id,"output_index":output_index,"arguments":arguments
                        }),
                    ));
                }
                pending.push_back(self.emit(
                    "response.output_item.done",
                    json!({
                        "output_index":output_index,
                        "item":{"id":id,"call_id":id,"name":name,"arguments":arguments,
                            "type":"function_call","status":status}
                    }),
                ));
            }
        }
        let kind = if status == "incomplete" {
            "response.incomplete"
        } else {
            "response.completed"
        };
        let response = self.response(status);
        pending.push_back(self.emit(kind, json!({"response":response})));
    }

    fn close_text(&mut self, pending: &mut VecDeque<Bytes>, status: &str) {
        if self.text_closed {
            return;
        }
        if let Some(index) = self.text_index {
            let parts = self.message_parts();
            for (content_index, part) in parts.iter().enumerate() {
                if self.text_part_index == Some(content_index) {
                    let item_id = self.message_id();
                    pending.push_back(self.emit(
                        "response.output_text.done",
                        json!({
                            "item_id":item_id,"output_index":index,"content_index":content_index,
                            "text":self.text,"logprobs":[]
                        }),
                    ));
                } else if self.refusal_part_index == Some(content_index) {
                    let item_id = self.message_id();
                    pending.push_back(self.emit(
                        "response.refusal.done",
                        json!({
                            "item_id":item_id,"output_index":index,"content_index":content_index,
                            "refusal":self.refusal
                        }),
                    ));
                }
                let item_id = self.message_id();
                pending.push_back(self.emit(
                    "response.content_part.done",
                    json!({
                        "item_id":item_id,"output_index":index,"content_index":content_index,
                        "part":part
                    }),
                ));
            }
            let item = json!({"id":self.message_id(),"type":"message","status":status,
                "role":"assistant","content":parts});
            pending.push_back(self.emit(
                "response.output_item.done",
                json!({"output_index":index,"item":item}),
            ));
            self.text_closed = true;
        }
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
    fn audit_f01_truncated_function_call_item_never_reports_completed() {
        for reason in ["length", "content_filter"] {
            let mut translator = ResponsesSse::new("m".into(), Value::Null);
            let mut frames = VecDeque::new();
            translator
                .feed(
                    &frame(json!({"id":"chat","choices":[{
                "index":0,"delta":{"tool_calls":[{"index":0,"id":"call1",
                    "type":"function","function":{"name":"lookup","arguments":"{\"city\":"}}]},
                "finish_reason":null}]})),
                    &mut frames,
                )
                .unwrap();
            translator
                .feed(
                    &frame(json!({"id":"chat","choices":[{
                "index":0,"delta":{},"finish_reason":reason}]})),
                    &mut frames,
                )
                .unwrap();
            translator.feed(b"data: [DONE]\n\n", &mut frames).unwrap();
            let events: Vec<Value> = frames
                .iter()
                .filter_map(|raw| {
                    std::str::from_utf8(raw)
                        .ok()?
                        .lines()
                        .find_map(|line| line.strip_prefix("data: "))
                        .and_then(|data| serde_json::from_str(data).ok())
                })
                .collect();
            let item = events
                .iter()
                .find(|v| {
                    v["type"] == "response.output_item.done" && v["item"]["type"] == "function_call"
                })
                .unwrap();
            assert_eq!(item["item"]["status"], "incomplete", "{reason}");
            let terminal = events
                .iter()
                .find(|v| v["type"] == "response.incomplete")
                .unwrap();
            assert_eq!(terminal["response"]["status"], "incomplete");
            assert_eq!(
                terminal["response"]["output"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|item| item["type"] == "function_call")
                    .unwrap()["status"],
                "incomplete"
            );
        }
    }
    #[test]
    fn mixed_text_then_reasoning_completes_in_output_index_order() {
        let mut translator = ResponsesSse::new("logical".into(), Value::Null);
        let mut events = VecDeque::new();
        translator
            .feed(
                &frame(json!({"id":"mixed","created":123,
            "choices":[{"index":0,"delta":{"content":"answer"},
                "finish_reason":null}]})),
                &mut events,
            )
            .unwrap();
        translator
            .feed(
                &frame(json!({"id":"mixed",
            "choices":[{"index":0,"delta":{"reasoning":"thought"},
                "finish_reason":"stop"}]})),
                &mut events,
            )
            .unwrap();
        translator.feed(b"data: [DONE]\n\n", &mut events).unwrap();
        let done: Vec<usize> = events
            .iter()
            .filter_map(|frame| {
                let text = std::str::from_utf8(frame).ok()?;
                if !text.starts_with("event: response.output_item.done\n") {
                    return None;
                }
                let data = text.lines().find_map(|line| line.strip_prefix("data: "))?;
                serde_json::from_str::<Value>(data).ok()?["output_index"]
                    .as_u64()
                    .and_then(|index| usize::try_from(index).ok())
            })
            .collect();
        assert_eq!(done, vec![0, 1], "out-of-order terminal output items");
    }

    #[test]
    fn absent_early_chat_id_is_synthesized_without_losing_later_identity_fencing() {
        let mut translator = ResponsesSse::new("logical".into(), Value::Null);
        let mut output = VecDeque::new();
        translator
            .feed(
                &frame(json!({"created":123,"choices":[{"index":0,
                    "delta":{"content":"hel"},"finish_reason":null}]})),
                &mut output,
            )
            .unwrap();
        let generated = translator.id.clone().unwrap();
        assert!(generated.starts_with("chatcmpl-rs-"), "{generated}");
        assert_eq!(translator.source_id, None);
        translator
            .feed(
                &frame(json!({"id":"provider-id","choices":[{"index":0,
                    "delta":{"content":"lo"},"finish_reason":"stop"}]})),
                &mut output,
            )
            .unwrap();
        assert_eq!(translator.source_id.as_deref(), Some("provider-id"));
        assert_eq!(translator.id.as_deref(), Some(generated.as_str()));
        translator.feed(b"data: [DONE]\n\n", &mut output).unwrap();
        let completed = String::from_utf8_lossy(output.back().unwrap());
        let data = completed
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap();
        let event: Value = serde_json::from_str(data).unwrap();
        assert_eq!(event["response"]["id"], generated);
        assert_eq!(event["response"]["output_text"], "hello");

        let mut conflicting = ResponsesSse::new("logical".into(), Value::Null);
        let mut events = VecDeque::new();
        conflicting
            .feed(
                &frame(json!({"choices":[{"index":0,"delta":{"content":"start"},
                    "finish_reason":null}]})),
                &mut events,
            )
            .unwrap();
        conflicting
            .feed(
                &frame(json!({"id":"source-a","choices":[{"index":0,
                    "delta":{"content":"middle"},"finish_reason":null}]})),
                &mut events,
            )
            .unwrap();
        assert!(
            conflicting
                .feed(
                    &frame(json!({"id":"source-b","choices":[{"index":0,
                    "delta":{"content":"end"},"finish_reason":"stop"}]})),
                    &mut events,
                )
                .is_err()
        );
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
    fn reasoning_and_reasoning_content_are_translated_as_reasoning_items() {
        let mut translator = ResponsesSse::new("logical".into(), Value::Null);
        let mut output = VecDeque::new();
        translator
            .feed(
                &frame(json!({"id":"reasoning1","created":123,
                    "choices":[{"index":0,"delta":{"reasoning_content":"thinking "},"finish_reason":null}]})),
                &mut output,
            )
            .unwrap();
        translator
            .feed(
                &frame(json!({"id":"reasoning1",
                    "choices":[{"index":0,"delta":{"reasoning":"continues"},"finish_reason":null}]})),
                &mut output,
            )
            .unwrap();
        translator
            .feed(
                &frame(json!({"id":"reasoning1",
                    "choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}]})),
                &mut output,
            )
            .unwrap();
        translator.feed(b"data: [DONE]\n\n", &mut output).unwrap();
        assert!(translator.done);
        assert_eq!(translator.reasoning_text, "thinking continues");
        let events = output
            .iter()
            .map(|event| String::from_utf8_lossy(event).into_owned())
            .collect::<Vec<_>>()
            .join("");
        assert!(events.contains("response.reasoning_text.delta"));
        assert!(events.contains("response.reasoning_text.done"));
        assert!(events.contains("\"type\":\"reasoning\""));
        assert!(events.contains("\"type\":\"reasoning_text\""));
        assert!(events.contains("response.completed"));

        let mut ambiguous = ResponsesSse::new("logical".into(), Value::Null);
        let mut output = VecDeque::new();
        ambiguous
            .feed(
                &frame(json!({"id":"ambiguous",
                    "choices":[{"index":0,"delta":{"reasoning":"a","reasoning_content":"b"},"finish_reason":null}]})),
                &mut output,
            )
            .unwrap();
        assert_eq!(ambiguous.reasoning_text, "ab");
    }

    #[test]
    fn chat_token_breakdown_survives_inline_then_final_sse_usage() {
        let mut translator = ResponsesSse::new("logical".into(), Value::Null);
        let mut events = VecDeque::new();
        translator
            .feed(
                &frame(json!({"id":"usage-details","created":123,
                "choices":[{"index":0,"delta":{"content":"answer"},
                    "finish_reason":"stop"}],
                "usage":{"prompt_tokens":12,"completion_tokens":8,"total_tokens":20,
                    "prompt_tokens_details":{"cached_tokens":9},
                    "completion_tokens_details":{"reasoning_tokens":5}}})),
                &mut events,
            )
            .unwrap();
        assert_eq!(translator.usage["input_tokens_details"]["cached_tokens"], 9);
        translator
            .feed(
                &frame(json!({"choices":[],"usage":{
                "prompt_tokens":12,"completion_tokens":8,"total_tokens":20,
                "prompt_tokens_details":{"cached_tokens":7},
                "completion_tokens_details":{"reasoning_tokens":6}}})),
                &mut events,
            )
            .unwrap();
        translator.feed(b"data: [DONE]\n\n", &mut events).unwrap();
        let done = String::from_utf8_lossy(events.back().unwrap());
        assert!(done.contains("event: response.completed"));
        assert!(done.contains("\"cached_tokens\":7"), "{done}");
        assert!(done.contains("\"reasoning_tokens\":6"), "{done}");
    }

    #[test]
    fn inline_only_usage_is_preserved_in_completed_response() {
        let mut translator = ResponsesSse::new("logical".into(), Value::Null);
        let mut output = VecDeque::new();
        translator
            .feed(
                &frame(json!({"id":"inline-only","created":123,
                    "choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}],
                    "usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}})),
                &mut output,
            )
            .unwrap();
        assert!(!translator.usage_seen);
        translator.feed(b"data: [DONE]\n\n", &mut output).unwrap();
        assert!(translator.done);
        let completed = String::from_utf8_lossy(output.back().unwrap());
        assert!(
            completed.contains("event: response.completed"),
            "{completed}"
        );
        let data = completed
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap();
        let event: Value = serde_json::from_str(data).unwrap();
        assert_eq!(
            event["response"]["usage"],
            json!({"input_tokens":4,"output_tokens":2,"total_tokens":6})
        );
    }

    #[test]
    fn content_frame_usage_does_not_conflict_with_authoritative_trailer() {
        let usage = json!({"prompt_tokens":2,"completion_tokens":3,"total_tokens":5});
        let mut translator = ResponsesSse::new("logical".into(), Value::Null);
        let mut output = VecDeque::new();
        translator
            .feed(
                &frame(json!({"id":"usage1","created":123,
                    "choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}],
                    "usage":usage.clone()})),
                &mut output,
            )
            .unwrap();
        assert!(!translator.usage_seen);
        assert_eq!(
            translator.usage,
            json!({"input_tokens":2,"output_tokens":3,"total_tokens":5})
        );
        translator
            .feed(
                &frame(json!({"choices":[],"usage":usage.clone()})),
                &mut output,
            )
            .unwrap();
        assert_eq!(
            translator.usage,
            json!({"input_tokens":2,"output_tokens":3,"total_tokens":5})
        );
        translator.feed(b"data: [DONE]\n\n", &mut output).unwrap();
        assert!(translator.done);
        assert_eq!(
            output
                .iter()
                .filter(|event| event.starts_with(b"event: response.completed"))
                .count(),
            1
        );
        let mut duplicate = ResponsesSse::new("logical".into(), Value::Null);
        let mut events = VecDeque::new();
        duplicate
            .feed(
                &frame(json!({"id":"id",
            "choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}]})),
                &mut events,
            )
            .unwrap();
        duplicate
            .feed(
                &frame(json!({"choices":[],"usage":usage.clone()})),
                &mut events,
            )
            .unwrap();
        assert!(
            duplicate
                .feed(&frame(json!({"choices":[],"usage":usage})), &mut events)
                .is_err()
        );
        assert!(!duplicate.done);
    }

    #[test]
    fn reasoning_text_shares_the_existing_bounded_output_budget() {
        let mut translator = ResponsesSse::new("logical".into(), Value::Null);
        let mut output = VecDeque::new();
        translator
            .feed(
                &frame(json!({"id":"bounded-reasoning",
                    "choices":[{"index":0,"delta":{"reasoning_content":"r".repeat(MAX_TEXT)},"finish_reason":null}]})),
                &mut output,
            )
            .unwrap();
        assert_eq!(translator.reasoning_text.len(), MAX_TEXT);
        assert!(translator
            .feed(
                &frame(json!({"id":"bounded-reasoning",
                    "choices":[{"index":0,"delta":{"content":"one extra byte"},"finish_reason":null}]})),
                &mut output,
            )
            .is_err());
    }

    #[test]
    fn mirrored_refusal_is_not_duplicated_or_rejected_by_json_mode() {
        for delta in [
            json!({"content":"I cannot assist","refusal":"I cannot assist"}),
            json!({"refusal":"I cannot assist"}),
        ] {
            let mut translator = ResponsesSse::new("logical".into(), Value::Null);
            translator.json_mode = Some(true);
            let mut pending = VecDeque::new();
            translator
                .feed(
                    &frame(json!({"id":"refusal","created":123,
                        "choices":[{"index":0,"delta":delta,"finish_reason":"stop"}]})),
                    &mut pending,
                )
                .unwrap();
            translator.feed(b"data: [DONE]\n\n", &mut pending).unwrap();
            assert!(translator.done);
            assert!(translator.has_refusal);
            assert_eq!(translator.text, "");
            assert_eq!(translator.refusal, "I cannot assist");
            let emitted = pending
                .iter()
                .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
                .collect::<Vec<_>>()
                .join("");
            assert_eq!(emitted.matches("event: response.refusal.delta").count(), 1);
            assert_eq!(emitted.matches("event: response.refusal.done").count(), 1);
            assert!(!emitted.contains("event: response.output_text.delta"));
            assert!(emitted.contains("event: response.completed"));
            let last: Value = serde_json::from_str(
                emitted
                    .split("event: response.completed\ndata: ")
                    .nth(1)
                    .unwrap()
                    .trim(),
            )
            .unwrap();
            assert_eq!(last["response"]["output_text"], "");
            assert_eq!(
                last["response"]["output"][0]["content"][0],
                json!({"type":"refusal","refusal":"I cannot assist"})
            );
        }
    }

    #[test]
    fn mixed_text_and_refusal_stream_keep_distinct_content_part_indexes() {
        let mut translator = ResponsesSse::new("logical".into(), Value::Null);
        let mut pending = VecDeque::new();
        translator
            .feed(
                &frame(json!({"id":"mixed-refusal","created":123,
                "choices":[{"index":0,"delta":{"content":"I can discuss safety."},
                    "finish_reason":null}]})),
                &mut pending,
            )
            .unwrap();
        translator
            .feed(
                &frame(json!({"id":"mixed-refusal",
                "choices":[{"index":0,"delta":{"refusal":"But I cannot do that."},
                    "finish_reason":"stop"}]})),
                &mut pending,
            )
            .unwrap();
        translator.feed(b"data: [DONE]\n\n", &mut pending).unwrap();
        let emitted = pending
            .iter()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .collect::<Vec<_>>();
        let final_event = emitted.last().unwrap();
        let result: Value =
            serde_json::from_str(final_event.split_once("data: ").unwrap().1.trim()).unwrap();
        assert_eq!(result["response"]["output_text"], "I can discuss safety.");
        assert_eq!(
            result["response"]["output"][0]["content"],
            json!([
                {"type":"output_text","text":"I can discuss safety.","annotations":[]},
                {"type":"refusal","refusal":"But I cannot do that."}
            ])
        );
        let refusal_delta = emitted
            .iter()
            .find(|chunk| chunk.starts_with("event: response.refusal.delta"))
            .unwrap();
        let refusal_json: Value =
            serde_json::from_str(refusal_delta.split_once("data: ").unwrap().1.trim()).unwrap();
        assert_eq!(refusal_json["content_index"], 1);
        assert_eq!(
            emitted
                .iter()
                .filter(
                    |chunk| chunk.starts_with("event: response.output_item.done")
                        && chunk.contains("\"type\":\"message\"")
                )
                .count(),
            1
        );
    }

    #[test]
    fn json_output_mode_must_not_complete_unparseable_upstream_text() {
        for (fragment, object_only, accepted) in [
            (r#"{"ok":true}"#, true, true),
            ("plain prose", true, false),
            ("[1]", true, false),
            ("[1]", false, true),
            ("not JSON", false, false),
        ] {
            let mut t = ResponsesSse::new("logical".into(), Value::Null);
            t.json_mode = Some(object_only);
            let mut q = VecDeque::new();
            t.feed(
                &frame(json!({"id":"t","choices":[{"index":0,
                    "delta":{"content":fragment},"finish_reason":"stop"}]})),
                &mut q,
            )
            .unwrap();
            let result = t.feed(b"data: [DONE]\n\n", &mut q);
            assert_eq!(result.is_ok(), accepted, "{fragment}");
            assert_eq!(t.done, accepted);
            assert_eq!(
                q.iter()
                    .any(|frame| frame.starts_with(b"event: response.completed")),
                accepted
            );
            if !accepted {
                assert!(String::from_utf8_lossy(&t.failed()).contains("response.failed"));
            }
        }
        // Truncated JSON is incomplete, not an erroneous completed event.
        let mut t = ResponsesSse::new("logical".into(), Value::Null);
        t.json_mode = Some(true);
        let mut q = VecDeque::new();
        t.feed(
            &frame(json!({"id":"t","choices":[{"index":0,
            "delta":{"content":"{"},"finish_reason":"length"}]})),
            &mut q,
        )
        .unwrap();
        t.feed(b"data: [DONE]\n\n", &mut q).unwrap();
        assert!(String::from_utf8_lossy(q.back().unwrap()).contains("response.incomplete"));
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
    fn content_filter_is_an_incomplete_response_not_a_stream_failure() {
        let mut t = ResponsesSse::new("logical".into(), Value::Null);
        let mut q = VecDeque::new();
        t.feed(
            &frame(json!({"id":"filtered","choices":[{"index":0,
                "delta":{"content":"partial"},"finish_reason":"content_filter"}]})),
            &mut q,
        )
        .unwrap();
        t.feed(b"data: [DONE]\n\n", &mut q).unwrap();
        let last = String::from_utf8_lossy(q.back().unwrap());
        assert!(last.contains("event: response.incomplete"), "{last}");
        assert!(last.contains("\"reason\":\"content_filter\""), "{last}");
        assert!(!last.contains("response.failed"), "{last}");
    }
    #[test]
    fn incomplete_stream_tool_item_never_claims_completed_arguments() {
        for (finish, expected_status) in [
            ("length", "incomplete"),
            ("content_filter", "incomplete"),
            ("tool_calls", "completed"),
        ] {
            let arguments = if finish == "tool_calls" {
                "{}"
            } else {
                "{\"unfinished\":"
            };
            let mut t = ResponsesSse::new("logical".into(), Value::Null);
            let mut pending = VecDeque::new();
            t.feed(
                &frame(json!({"id":"partial-call","created":123,
                    "choices":[{"index":0,
                        "delta":{"tool_calls":[{"index":0,"id":"call_partial",
                            "type":"function",
                            "function":{"name":"lookup","arguments":arguments}}]},
                        "finish_reason":null}]})),
                &mut pending,
            )
            .unwrap();
            t.feed(
                &frame(json!({"id":"partial-call",
                    "choices":[{"index":0,"delta":{},"finish_reason":finish}]})),
                &mut pending,
            )
            .unwrap();
            t.feed(b"data: [DONE]\n\n", &mut pending).unwrap();
            assert!(t.done);
            let events = pending
                .iter()
                .map(|frame| String::from_utf8_lossy(frame).into_owned())
                .collect::<Vec<_>>();
            assert_eq!(
                events
                    .iter()
                    .filter(
                        |frame| frame.starts_with("event: response.function_call_arguments.done\n")
                    )
                    .count(),
                usize::from(expected_status == "completed"),
                "{finish}: incomplete function arguments must never be signaled ready"
            );
            let done = events
                .iter()
                .find(|frame| {
                    frame.starts_with("event: response.output_item.done\n")
                        && frame.contains("\"type\":\"function_call\"")
                })
                .unwrap();
            let item_done: Value =
                serde_json::from_str(done.split_once("data: ").unwrap().1.trim()).unwrap();
            assert_eq!(item_done["item"]["status"], expected_status, "{finish}");
            assert_eq!(item_done["item"]["arguments"], arguments);
            let final_event = events.last().unwrap();
            let response: Value =
                serde_json::from_str(final_event.split_once("data: ").unwrap().1.trim()).unwrap();
            assert_eq!(response["response"]["status"], expected_status, "{finish}");
            assert_eq!(response["response"]["output"][0]["status"], expected_status);
        }
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
    fn mixed_text_message_closes_before_tool_item_opens_exactly_once() {
        let mut translator = ResponsesSse::new("demo".into(), Value::Null);
        let mut pending = VecDeque::new();
        translator
            .feed(
                &frame(json!({
                    "id":"chat","choices":[{"index":0,
                        "delta":{"content":"hello"},"finish_reason":null}]
                })),
                &mut pending,
            )
            .unwrap();
        translator
            .feed(
                &frame(json!({
                    "id":"chat","choices":[{"index":0,
                        "delta":{"tool_calls":[{"index":0,"id":"call_one","type":"function",
                            "function":{"name":"lookup","arguments":"{}"}}]},
                        "finish_reason":null}]
                })),
                &mut pending,
            )
            .unwrap();
        let events = pending
            .iter()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .collect::<Vec<_>>();
        let text_done = events
            .iter()
            .position(|event| event.starts_with("event: response.output_text.done"))
            .unwrap();
        let message_done = events
            .iter()
            .position(|event| {
                event.starts_with("event: response.output_item.done")
                    && event.contains("\"type\":\"message\"")
            })
            .unwrap();
        let tool_added = events
            .iter()
            .position(|event| {
                event.starts_with("event: response.output_item.added")
                    && event.contains("\"type\":\"function_call\"")
            })
            .unwrap();
        assert!(text_done < message_done && message_done < tool_added);
        translator
            .feed(
                &frame(json!({
                    "id":"chat","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]
                })),
                &mut pending,
            )
            .unwrap();
        translator.feed(b"data: [DONE]\n\n", &mut pending).unwrap();
        assert_eq!(
            pending
                .iter()
                .filter(|bytes| bytes.starts_with(b"event: response.output_text.done"))
                .count(),
            1
        );
        assert_eq!(
            pending
                .iter()
                .filter(|bytes| bytes.starts_with(b"event: response.completed"))
                .count(),
            1
        );
    }

    #[test]
    fn integral_float_timestamp_and_empty_function_arguments_complete() {
        let mut converter = ResponsesSse::new("demo".into(), Value::Null);
        let mut pending = VecDeque::new();
        converter
            .feed(
                &frame(json!({"id":"chat-1","created":1729600000.0,
            "choices":[{"index":0,"delta":{"tool_calls":[{"index":0,
                "id":"call_zero","type":"function",
                "function":{"name":"no_params","arguments":""}}]},
                "finish_reason":null}]})),
                &mut pending,
            )
            .unwrap();
        converter
            .feed(
                &frame(json!({"id":"chat-1","choices":[{"index":0,
            "delta":null,"finish_reason":"tool_calls"}]})),
                &mut pending,
            )
            .unwrap();
        converter.feed(b"data: [DONE]\n\n", &mut pending).unwrap();
        assert_eq!(converter.created_at, Some(1_729_600_000));
        assert!(converter.done);
        let all = pending
            .iter()
            .map(|e| String::from_utf8_lossy(e).into_owned())
            .collect::<Vec<_>>()
            .join("");
        assert!(all.contains("event: response.completed"));
        assert!(all.contains(r#""arguments":"{}""#), "{all}");
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
