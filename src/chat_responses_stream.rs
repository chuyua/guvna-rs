//! Native Responses framing followed by a validated single-choice Chat stream.
use super::{chat_responses, dispatch::Endpoint, response_validation};
use crate::{
    native_responses_sse::NativeResponsesSse,
    sse::{ChatSse, SseError},
};
use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(super) struct ChatResponsesStream {
    native: NativeResponsesSse,
    chat: ChatSse,
    request: Value,
    model: String,
    id: String,
    created: u64,
    content: String,
    refusal: String,
    reasoning: String,
    tools: BTreeMap<u64, Value>,
    bytes: usize,
    limit: bool,
    incompatible: bool,
    output_format: Option<crate::structured_output::OutputFormat>,
}
impl ChatResponsesStream {
    pub(super) fn new(
        request: Value,
        model: String,
        output_format: Option<crate::structured_output::OutputFormat>,
    ) -> Self {
        Self {
            native: NativeResponsesSse::new(),
            chat: ChatSse::new(1)
                .expect("one choice")
                .with_output_contract(output_format.clone()),
            request,
            model,
            id: String::new(),
            created: 0,
            content: String::new(),
            refusal: String::new(),
            reasoning: String::new(),
            tools: BTreeMap::new(),
            bytes: 0,
            limit: false,
            incompatible: false,
            output_format,
        }
    }
    pub(super) fn is_done(&self) -> bool {
        self.chat.is_done()
    }
    pub(super) fn take_progress(&mut self) -> bool {
        self.native.take_progress()
    }
    pub(super) fn hit_resource_limit(&self) -> bool {
        self.limit || self.native.hit_resource_limit() || self.chat.hit_resource_limit()
    }
    pub(super) fn incompatible(&self) -> bool {
        self.incompatible
    }
    fn unsupported(&mut self) -> SseError {
        self.incompatible = true;
        SseError
    }
    pub(super) fn push(&mut self, bytes: &[u8]) -> Result<Vec<Bytes>, SseError> {
        let events = self.native.push(bytes)?;
        let mut output = Vec::new();
        for event in events {
            let data = event
                .split(|b| *b == b'\n')
                .find_map(|l| l.strip_prefix(b"data: "))
                .ok_or(SseError)?;
            let event: Value = serde_json::from_slice(data).map_err(|_| SseError)?;
            let kind = event["type"].as_str().ok_or(SseError)?;
            let mut delta = None;
            match kind {
                "response.created" => {
                    self.id = event["response"]["id"].as_str().ok_or(SseError)?.into();
                    self.created = event["response"]["created_at"].as_u64().ok_or(SseError)?;
                    delta = Some(json!({"role":"assistant"}));
                }
                "response.output_item.added" => {
                    let item = &event["item"];
                    match item["type"].as_str() {
                        Some("function_call") => {
                            let index = event["output_index"].as_u64().ok_or(SseError)?;
                            if self.tools.contains_key(&index) || self.tools.len() >= 128 {
                                return Err(SseError);
                            }
                            let position = self.tools.len();
                            let tool = json!({"index":position,"id":item["call_id"].as_str().ok_or(SseError)?,"type":"function",
                                "function":{"name":item["name"].as_str().ok_or(SseError)?,"arguments":""}});
                            delta = Some(json!({"tool_calls":[tool]}));
                            self.tools.insert(index, tool);
                        }
                        Some("message") if item["role"] == "assistant" => {}
                        Some("reasoning")
                            if item.get("encrypted_content").is_none_or(Value::is_null) => {}
                        _ => return Err(self.unsupported()),
                    }
                }
                "response.output_text.delta"
                | "response.refusal.delta"
                | "response.reasoning_summary_text.delta" => {
                    let text = event["delta"].as_str().ok_or(SseError)?;
                    self.account(text.len())?;
                    let key = match kind {
                        "response.output_text.delta" => {
                            self.content.push_str(text);
                            "content"
                        }
                        "response.refusal.delta" => {
                            self.refusal.push_str(text);
                            "refusal"
                        }
                        _ => {
                            self.reasoning.push_str(text);
                            "reasoning_content"
                        }
                    };
                    delta = Some(json!({key:text}));
                }
                "response.function_call_arguments.delta" => {
                    let text = event["delta"].as_str().ok_or(SseError)?;
                    self.account(text.len())?;
                    let tool = self
                        .tools
                        .get_mut(&event["output_index"].as_u64().ok_or(SseError)?)
                        .ok_or(SseError)?;
                    let Value::String(mut arguments) = tool["function"]["arguments"].take() else {
                        return Err(SseError);
                    };
                    arguments.push_str(text);
                    tool["function"]["arguments"] = Value::String(arguments);
                    delta = Some(
                        json!({"tool_calls":[{"index":tool["index"],"function":{"arguments":text}}]}),
                    );
                }
                "response.completed" | "response.incomplete" => {
                    let final_chat = chat_responses::result(&event["response"], &self.model)
                        .map_err(|_| self.unsupported())?;
                    if !response_validation::valid_result_with_contract(
                        Endpoint::Chat,
                        &self.request,
                        &BTreeMap::new(),
                        &final_chat,
                        self.output_format.as_ref(),
                    ) {
                        return Err(SseError);
                    }
                    let message = &final_chat["choices"][0]["message"];
                    if message["content"].as_str().unwrap_or("") != self.content
                        || message["refusal"].as_str().unwrap_or("") != self.refusal
                        || message["reasoning_content"].as_str().unwrap_or("") != self.reasoning
                    {
                        return Err(SseError);
                    }
                    let calls = message["tool_calls"]
                        .as_array()
                        .map(Vec::as_slice)
                        .unwrap_or(&[]);
                    if calls.len() != self.tools.len()
                        || self.tools.values().zip(calls).any(|(actual, expected)| {
                            actual["id"] != expected["id"]
                                || actual["function"] != expected["function"]
                        })
                    {
                        return Err(SseError);
                    }
                    let mut final_chunk =
                        self.chunk(json!({}), final_chat["choices"][0]["finish_reason"].clone());
                    if self.request.pointer("/stream_options/include_usage")
                        == Some(&Value::Bool(true))
                        && let Some(usage) = final_chat.get("usage")
                    {
                        final_chunk["usage"] = usage.clone();
                    }
                    self.emit(final_chunk, &mut output)?;
                    output.extend(self.chat.push(b"data: [DONE]\n\n")?);
                }
                // These describe already emitted items and carry no new Chat payload.
                "response.in_progress"
                | "response.output_item.done"
                | "response.content_part.added"
                | "response.content_part.done"
                | "response.output_text.done"
                | "response.refusal.done"
                | "response.function_call_arguments.done"
                | "response.reasoning_summary_part.added"
                | "response.reasoning_summary_part.done"
                | "response.reasoning_summary_text.done" => {}
                _ => return Err(self.unsupported()),
            }
            if let Some(delta) = delta {
                self.emit(self.chunk(delta, Value::Null), &mut output)?;
            }
        }
        Ok(output)
    }
    fn account(&mut self, n: usize) -> Result<(), SseError> {
        self.bytes = self.bytes.saturating_add(n);
        if self.bytes > 2 * 1024 * 1024 {
            self.limit = true;
            return Err(SseError);
        }
        Ok(())
    }
    fn chunk(&self, delta: Value, finish: Value) -> Value {
        json!({"id":self.id,"object":"chat.completion.chunk","created":self.created,"model":self.model,"choices":[{"index":0,"delta":delta,"finish_reason":finish}]})
    }
    fn emit(&mut self, value: Value, out: &mut Vec<Bytes>) -> Result<(), SseError> {
        out.extend(self.chat.push(format!("data: {value}\n\n").as_bytes())?);
        Ok(())
    }
}
