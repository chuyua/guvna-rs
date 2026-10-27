//! Named chain and provider-prefixed passthrough resolution.
use crate::{
    config::{AutoCandidate, Config, Step},
    model_capabilities,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    hash::{DefaultHasher, Hasher},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const AUTO_CACHE_CAPACITY: usize = 128;
const AUTO_CACHE_TTL: Duration = Duration::from_secs(30);
const CONTEXT_SAFETY_MARGIN_PERCENT: usize = 20;

/// Only well-known Chat request features are examined; no text or image bytes are retained.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct AutoRequirements {
    tools: bool,
    images: bool,
    json_schema: bool,
    min_context: usize,
    max_context: Option<usize>,
}

impl AutoRequirements {
    pub fn from_signals(tools: bool, images: bool, json_schema: bool) -> Self {
        Self {
            tools,
            images,
            json_schema,
            ..Self::default()
        }
    }

    pub fn with_context_bounds(mut self, min_context: usize, max_context: Option<usize>) -> Self {
        self.min_context = min_context;
        self.max_context = max_context;
        self
    }

    fn requires_images(self) -> bool {
        self.images
    }
}

pub fn auto_requirements(request: &Value) -> AutoRequirements {
    let tools = request
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| !tools.is_empty())
        // A required/named tool choice is a hard capability constraint even
        // if the caller supplied no usable tools array.
        || request.get("tool_choice").is_some_and(|choice| match choice {
            Value::String(value) => !matches!(value.as_str(), "auto" | "none"),
            Value::Null => false,
            _ => true,
        })
        || request
            .get("messages")
            .and_then(Value::as_array)
            .is_some_and(|messages| {
                messages.iter().any(|message| {
                    message.get("tool_calls").is_some_and(|calls| match calls {
                        Value::Array(calls) => !calls.is_empty(),
                        Value::Null => false,
                        _ => true,
                    })
                        || message.get("role").and_then(Value::as_str) == Some("tool")
                })
            });
    let images = request
        .get("messages")
        .and_then(Value::as_array)
        .is_some_and(|messages| {
            messages.iter().any(|message| {
                message
                    .get("content")
                    .and_then(Value::as_array)
                    .is_some_and(|parts| {
                        parts.iter().any(|part| {
                            matches!(
                                part.get("type").and_then(Value::as_str),
                                Some("image_url" | "image" | "input_image")
                            )
                        })
                    })
            })
        });
    let json_schema = request
        .get("response_format")
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str)
        == Some("json_schema");
    AutoRequirements::from_signals(tools, images, json_schema)
}

/// Go-compatible Auto-only window filters. Zero/negative values mean
/// unspecified. Unknown window sizes must not satisfy an explicit bound.
/// The 512-token Go default is used only when the local registry is opted in:
/// existing Rust configs with unknown model windows must remain usable.
pub fn auto_context_bounds(
    request: &Value,
    use_model_registry: bool,
) -> Result<(usize, Option<usize>), &'static str> {
    auto_context_bounds_with_default(request, use_model_registry, None)
}

/// Request bounds override the service default; the default is not sent
/// upstream and cannot relax an explicit caller-specified lower bound.
pub fn auto_context_bounds_with_default(
    request: &Value,
    use_model_registry: bool,
    default_min_context: Option<usize>,
) -> Result<(usize, Option<usize>), &'static str> {
    fn bound(value: Option<&Value>) -> Result<Option<usize>, &'static str> {
        match value {
            None => Ok(None),
            Some(Value::Number(number)) => {
                if number.as_i64().is_some_and(|value| value <= 0) {
                    return Ok(None);
                }
                let positive = number.as_u64().ok_or("context bounds must be integers")?;
                usize::try_from(positive)
                    .map(Some)
                    .map_err(|_| "context bounds exceed platform range")
                    .map(|value| value.filter(|number| *number > 0))
            }
            _ => Err("context bounds must be integers"),
        }
    }
    let min_context = bound(request.get("min_context"))?
        .unwrap_or(default_min_context.unwrap_or(if use_model_registry { 512 } else { 0 }));
    let max_context = bound(request.get("max_context"))?;
    if max_context.is_some_and(|max| max < min_context) {
        return Err("max_context must be >= min_context");
    }
    Ok((min_context, max_context))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CacheKey {
    generation: u64,
    input_bytes: usize,
    context_estimate: usize,
    // Request-aware selection depends on the exact encoded body length, not
    // only on rounded context estimates. Legacy resolution uses None.
    request_bytes: Option<usize>,
    requirements: AutoRequirements,
    penalties: Vec<u8>,
}

#[derive(Clone, Copy)]
pub struct AutoRequestBudget {
    pub input_bytes: usize,
    pub request_bytes: usize,
    pub context_estimate: usize,
}

impl CacheKey {
    fn new(
        config: &Config,
        input_bytes: usize,
        context_estimate: usize,
        request_bytes: Option<usize>,
        requirements: AutoRequirements,
        penalties: &[u8],
        generation: u64,
    ) -> Self {
        Self {
            generation,
            input_bytes,
            context_estimate,
            request_bytes,
            requirements,
            penalties: if config.auto.health_aware {
                penalties.to_vec()
            } else {
                Vec::new()
            },
        }
    }
}
struct CacheEntry {
    key: CacheKey,
    expires_at: Instant,
    // A hit must not hold the routing cache mutex while cloning arbitrarily
    // large candidate defaults (e.g. JSON-schema tools). Copy the Arc under
    // lock, then clone the request-owned candidate list after releasing it.
    steps: Arc<Vec<ResolvedStep>>,
}
#[derive(Default)]
struct CacheState {
    entries: VecDeque<CacheEntry>,
}

impl CacheState {
    fn retain_valid(&mut self, now: Instant) {
        // Old in-flight requests may outlive a live config publish. Never
        // let one of them roll back a global "current generation" and flush
        // the new snapshot's hot entries. Keys already include generation;
        // bound coexistence with the same 128-entry LRU and 30s TTL.
        self.entries.retain(|entry| entry.expires_at > now);
    }

    /// Both cache-hit paths must update LRU order without inserting duplicates.
    fn touch(&mut self, key: &CacheKey) -> Option<&CacheEntry> {
        let index = self.entries.iter().position(|entry| &entry.key == key)?;
        let entry = self.entries.remove(index).expect("located cache entry");
        self.entries.push_back(entry);
        self.entries.back()
    }
}

/// Small process-local route cache. It stores route metadata only, never request content.
#[derive(Default)]
pub struct AutoRouteCache {
    state: Mutex<CacheState>,
}
impl AutoRouteCache {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(CacheState::default()),
        }
    }

    pub fn resolve(
        &self,
        config: &Config,
        input_bytes: usize,
        context_estimate: usize,
        requirements: AutoRequirements,
        penalties: &[u8],
        generation: u64,
    ) -> Result<Vec<ResolvedStep>, String> {
        self.resolve_cached(
            CacheKey::new(
                config,
                input_bytes,
                context_estimate,
                None,
                requirements,
                penalties,
                generation,
            ),
            || {
                resolve_auto_with_requirements(
                    config,
                    input_bytes,
                    context_estimate,
                    requirements,
                    penalties,
                )
            },
        )
    }

    /// Cached Auto selection must account for the actual physical model
    /// written on the wire. The incoming logical model is always "auto",
    /// but its candidate models may have very different serialized lengths.
    pub fn resolve_for_request(
        &self,
        config: &Config,
        budget: AutoRequestBudget,
        request: &Value,
        requirements: AutoRequirements,
        penalties: &[u8],
        generation: u64,
    ) -> Result<Vec<ResolvedStep>, String> {
        self.resolve_cached(
            CacheKey::new(
                config,
                budget.input_bytes,
                budget.context_estimate,
                Some(budget.request_bytes),
                requirements,
                penalties,
                generation,
            ),
            || {
                resolve_auto_for_request_sized(
                    config,
                    budget.input_bytes,
                    request,
                    budget.request_bytes,
                    requirements,
                    penalties,
                )
            },
        )
    }

    fn resolve_cached(
        &self,
        key: CacheKey,
        compute: impl FnOnce() -> Result<Vec<ResolvedStep>, String>,
    ) -> Result<Vec<ResolvedStep>, String> {
        let now = Instant::now();
        let cached = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.retain_valid(now);
            state.touch(&key).map(|entry| Arc::clone(&entry.steps))
        };
        if let Some(steps) = cached {
            return Ok((*steps).clone());
        }

        let steps = compute()?;
        let cached_copy = Arc::new(steps.clone());
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.retain_valid(Instant::now());
            // Concurrent cold misses for the same key must not evict others.
            if state.touch(&key).is_none() {
                if state.entries.len() >= AUTO_CACHE_CAPACITY {
                    state.entries.pop_front();
                }
                state.entries.push_back(CacheEntry {
                    key,
                    expires_at: Instant::now() + AUTO_CACHE_TTL,
                    steps: cached_copy,
                });
            }
        }
        Ok(steps)
    }
}

/// The fingerprint is computed when AppState is created. Replacing that state
/// resets the cache; passing a different generation clears old entries immediately.
pub fn config_generation(config: &Config) -> u64 {
    struct HashFmt<'a>(&'a mut DefaultHasher);
    impl std::fmt::Write for HashFmt<'_> {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            self.0.write(value.as_bytes());
            Ok(())
        }
    }
    let mut hasher = DefaultHasher::new();
    // The version changes when ANY route/config field changes. Streaming the
    // debug representation into the hasher avoids allocating a second full
    // config string during runtime chain changes with large tool schemas.
    std::fmt::write(&mut HashFmt(&mut hasher), format_args!("{config:?}"))
        .expect("in-memory hash sink cannot fail");
    // Preserve std::hash::Hash's str boundary terminator for stable keys.
    hasher.write_u8(0xff);
    hasher.finish()
}
#[derive(Clone, Debug)]
pub struct ResolvedStep {
    pub provider_index: usize,
    pub model: String,
    pub params: BTreeMap<String, Value>,
}

/// Count serialized JSON bytes without retaining a copy of request content.
fn encoded_json_bytes_of<T: serde::Serialize + ?Sized>(value: &T) -> usize {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(buf.len());
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    if serde_json::to_writer(&mut counter, value).is_err() {
        return usize::MAX;
    }
    counter.0
}

fn encoded_json_bytes(value: Option<&Value>) -> usize {
    value.map_or(0, encoded_json_bytes_of)
}

/// Keep the established min/max input-byte contract for messages.
pub fn auto_input_bytes(messages: Option<&Value>) -> usize {
    encoded_json_bytes(messages)
}

/// Shared legacy and request-aware safety margin; keep overflow behavior stable.
fn with_context_safety_margin(bytes: usize) -> usize {
    bytes.saturating_add(
        bytes
            .saturating_mul(CONTEXT_SAFETY_MARGIN_PERCENT)
            .div_ceil(100),
    )
}

/// Estimate the full serialized request budget, output allowance and 20% safety reserve.
/// This deliberately overestimates tokens (one byte per token) rather than
/// depending on a remote tokenizer or underestimating multilingual input.
pub fn auto_context_estimate(request: &Value) -> usize {
    let bytes = encoded_json_bytes(Some(request));
    auto_context_estimate_from_bytes(request, bytes)
}

pub fn auto_request_bytes_and_context_estimate(request: &Value) -> (usize, usize) {
    let bytes = encoded_json_bytes(Some(request));
    (bytes, auto_context_estimate_from_bytes(request, bytes))
}

fn auto_context_estimate_from_bytes(request: &Value, bytes: usize) -> usize {
    let output_budget = ["max_completion_tokens", "max_tokens"]
        .iter()
        .filter_map(|name| request.get(name).and_then(Value::as_u64))
        .map(|tokens| tokens.min(usize::MAX as u64) as usize)
        .max()
        .unwrap_or(1024);
    with_context_safety_margin(bytes).saturating_add(output_budget)
}
/// A small, transparent Auto V1: fixed configured priorities and a bounded
/// length heuristic, not a black-box performance claim or remote-model ranking.
/// The returned candidates use the SAME dispatch/failover loop as named chains.
pub fn resolve_auto(config: &Config, input_bytes: usize) -> Result<Vec<ResolvedStep>, String> {
    let context_estimate = with_context_safety_margin(input_bytes);
    resolve_auto_with_estimate(config, input_bytes, context_estimate, &[])
}

/// Scores are fixed-size in-memory hints supplied by the gateway; never SQL queries.
pub fn resolve_auto_with_health(
    config: &Config,
    input_bytes: usize,
    penalties: &[u8],
) -> Result<Vec<ResolvedStep>, String> {
    let context_estimate = with_context_safety_margin(input_bytes);
    resolve_auto_with_estimate(config, input_bytes, context_estimate, penalties)
}

pub fn resolve_auto_with_estimate(
    config: &Config,
    input_bytes: usize,
    context_estimate: usize,
    penalties: &[u8],
) -> Result<Vec<ResolvedStep>, String> {
    resolve_auto_with_requirements(
        config,
        input_bytes,
        context_estimate,
        AutoRequirements::default(),
        penalties,
    )
}

/// Evaluate all hard candidate constraints before applying an operator-only
/// context-floor fallback. Keep this shared with normal routing so the
/// fallback cannot accidentally bypass image, tool, schema or byte limits.
fn auto_candidate_eligible(
    config: &Config,
    candidate: &AutoCandidate,
    input_bytes: usize,
    context_estimate: usize,
    requirements: AutoRequirements,
) -> bool {
    let model_supported = config
        .providers
        .iter()
        .find(|provider| provider.name == candidate.provider)
        .is_some_and(|provider| {
            provider.models.is_empty()
                || provider
                    .models
                    .iter()
                    .any(|model| model == &candidate.model)
        });
    let registry = config.auto.use_model_registry;
    let overrides = config.auto.capability_overrides.get(&candidate.model);
    let context_limit = candidate
        .max_context_tokens
        .or_else(|| overrides.and_then(|entry| entry.context_tokens))
        .or_else(|| registry.then(|| model_capabilities::context_tokens(&candidate.model)));
    let image_support = candidate.supports_images.or_else(|| {
        overrides
            .and_then(|entry| entry.supports_images)
            .or_else(|| registry.then_some(!model_capabilities::known_text_only(&candidate.model)))
    });
    let meets_window_bounds = if requirements.min_context > 0 || requirements.max_context.is_some()
    {
        context_limit.is_some_and(|window| {
            window >= requirements.min_context
                && requirements.max_context.is_none_or(|max| window <= max)
        })
    } else {
        true
    };
    model_supported
        && meets_window_bounds
        && (!requirements.tools || candidate.supports_tools != Some(false))
        && (!requirements.requires_images() || image_support != Some(false))
        && (!requirements.json_schema || candidate.supports_json_schema != Some(false))
        && context_limit.is_none_or(|max| context_estimate <= max)
        && input_bytes >= candidate.min_input_bytes
        && candidate
            .max_input_bytes
            .is_none_or(|max| input_bytes <= max)
}

/// Evaluate the payload actually emitted for this candidate without copying
/// the caller's messages (which can be multi-megabyte). Unset default fields
/// contribute their serialized byte count; explicit caller fields always win.
fn auto_candidate_eligible_for_request(
    config: &Config,
    candidate: &AutoCandidate,
    input_bytes: usize,
    request: &Value,
    request_bytes: usize,
    requirements: AutoRequirements,
) -> bool {
    let effective = |name: &str| request.get(name).or_else(|| candidate.params.get(name));
    // Dispatch always writes the physical model, even when /v1/auto callers
    // omit model entirely. Account for that replacement in the same pass.
    let physical_model_bytes = encoded_json_bytes_of(candidate.model.as_str());
    let mut effective_request_bytes = if let Some(original) = request.get("model") {
        request_bytes
            .saturating_sub(encoded_json_bytes(Some(original)))
            .saturating_add(physical_model_bytes)
    } else {
        request_bytes
            .saturating_add(usize::from(
                request.as_object().is_some_and(|map| !map.is_empty()),
            ))
            .saturating_add(encoded_json_bytes_of("model"))
            .saturating_add(1)
            .saturating_add(physical_model_bytes)
    };
    let mut field_count = request.as_object().map_or(0, serde_json::Map::len);
    if request.get("model").is_none() {
        field_count += 1;
    }
    // Non-streaming callers of SSE-only Chat suppliers are transparently
    // upgraded to upstream stream:true by dispatch. The stream field is
    // reserved and cannot be set in candidate defaults.
    let force_upstream_stream = request.get("stream") != Some(&Value::Bool(true))
        && config
            .providers
            .iter()
            .find(|provider| provider.name == candidate.provider)
            .is_some_and(|provider| provider.stream_only);
    for (name, value) in &candidate.params {
        if name != "model"
            && !(force_upstream_stream && name == "stream_options")
            && request.get(name).is_none()
        {
            let separator = usize::from(field_count != 0);
            effective_request_bytes = effective_request_bytes
                .saturating_add(separator)
                .saturating_add(encoded_json_bytes_of(name.as_str()))
                .saturating_add(1)
                .saturating_add(encoded_json_bytes(Some(value)));
            field_count += 1;
        }
    }
    if force_upstream_stream {
        if let Some(original) = request.get("stream") {
            effective_request_bytes = effective_request_bytes
                .saturating_sub(encoded_json_bytes(Some(original)))
                .saturating_add(encoded_json_bytes_of(&true));
        } else {
            effective_request_bytes = effective_request_bytes
                .saturating_add(usize::from(field_count > 0))
                .saturating_add(encoded_json_bytes_of("stream"))
                .saturating_add(1)
                .saturating_add(encoded_json_bytes_of(&true));
        }
        // Non-streaming SSE aggregation also forces
        // stream_options.include_usage=true. Account for the exact merge.
        let forced_options_bytes =
            encoded_json_bytes_of(&serde_json::json!({"include_usage":true}));
        match request.get("stream_options") {
            None => {
                effective_request_bytes = effective_request_bytes
                    .saturating_add(usize::from(field_count > 0))
                    .saturating_add(encoded_json_bytes_of("stream_options"))
                    .saturating_add(1)
                    .saturating_add(forced_options_bytes);
            }
            Some(Value::Object(options)) => {
                if let Some(existing) = options.get("include_usage") {
                    effective_request_bytes = effective_request_bytes
                        .saturating_sub(encoded_json_bytes(Some(existing)))
                        .saturating_add(encoded_json_bytes_of(&true));
                } else {
                    effective_request_bytes = effective_request_bytes
                        .saturating_add(usize::from(!options.is_empty()))
                        .saturating_add(encoded_json_bytes_of("include_usage"))
                        .saturating_add(1)
                        .saturating_add(encoded_json_bytes_of(&true));
                }
            }
            Some(other) => {
                effective_request_bytes = effective_request_bytes
                    .saturating_sub(encoded_json_bytes(Some(other)))
                    .saturating_add(forced_options_bytes);
            }
        }
    }
    // Both aliases can be present when the caller uses one and a candidate
    // default supplies the other. They are sent upstream as distinct fields;
    // take the larger bound rather than assuming one silently overrides the
    // other, which could admit a candidate far beyond its context window.
    let budgets = ["max_completion_tokens", "max_tokens"]
        .iter()
        .filter_map(|name| effective(name).and_then(Value::as_u64))
        .map(|tokens| tokens.min(usize::MAX as u64) as usize);
    let output_budget = budgets.max().unwrap_or(1024);
    let context_estimate =
        with_context_safety_margin(effective_request_bytes).saturating_add(output_budget);
    let tools = requirements.tools
        || effective("tools")
            .and_then(Value::as_array)
            .is_some_and(|tools| !tools.is_empty())
        || effective("tool_choice").is_some_and(|choice| match choice {
            Value::String(value) => !matches!(value.as_str(), "auto" | "none"),
            Value::Null => false,
            _ => true,
        });
    let json_schema = requirements.json_schema
        || effective("response_format")
            .and_then(|format| format.get("type"))
            .and_then(Value::as_str)
            == Some("json_schema");
    auto_candidate_eligible(
        config,
        candidate,
        input_bytes,
        context_estimate,
        AutoRequirements {
            tools,
            json_schema,
            ..requirements
        },
    )
}

pub fn auto_operator_default_can_degrade_for_request(
    config: &Config,
    input_bytes: usize,
    request: &Value,
    requirements: AutoRequirements,
) -> bool {
    auto_operator_default_can_degrade_for_request_sized(
        config,
        input_bytes,
        request,
        encoded_json_bytes(Some(request)),
        requirements,
    )
}

pub(crate) fn auto_operator_default_can_degrade_for_request_sized(
    config: &Config,
    input_bytes: usize,
    request: &Value,
    request_bytes: usize,
    requirements: AutoRequirements,
) -> bool {
    if requirements.min_context == 0
        || requirements.max_context.is_some()
        || config.auto.candidates.is_empty()
    {
        return false;
    }
    let relaxed = requirements.with_context_bounds(0, None);
    config.auto.candidates.iter().any(|candidate| {
        auto_candidate_eligible_for_request(
            config,
            candidate,
            input_bytes,
            request,
            request_bytes,
            relaxed,
        )
    })
}

pub fn resolve_auto_for_request(
    config: &Config,
    input_bytes: usize,
    request: &Value,
    requirements: AutoRequirements,
    penalties: &[u8],
) -> Result<Vec<ResolvedStep>, String> {
    let request_bytes = encoded_json_bytes(Some(request));
    resolve_auto_for_request_sized(
        config,
        input_bytes,
        request,
        request_bytes,
        requirements,
        penalties,
    )
}

pub(crate) fn resolve_auto_for_request_sized(
    config: &Config,
    input_bytes: usize,
    request: &Value,
    request_bytes: usize,
    requirements: AutoRequirements,
    penalties: &[u8],
) -> Result<Vec<ResolvedStep>, String> {
    resolve_auto_filtered(config, penalties, |candidate| {
        auto_candidate_eligible_for_request(
            config,
            candidate,
            input_bytes,
            request,
            request_bytes,
            requirements,
        )
    })
}

/// Go's service-level min-context is a preference, not a caller constraint.
/// Relax it only when at least one candidate survives every OTHER hard
/// filter. A separate unsuitable candidate must not block that safe fallback.
/// The caller only reaches this function after a normal selection found no
/// candidates, and explicit request bounds are never relaxed.
pub fn auto_operator_default_can_degrade(
    config: &Config,
    input_bytes: usize,
    context_estimate: usize,
    requirements: AutoRequirements,
) -> bool {
    if requirements.min_context == 0
        || requirements.max_context.is_some()
        || config.auto.candidates.is_empty()
    {
        return false;
    }
    let relaxed = requirements.with_context_bounds(0, None);
    config.auto.candidates.iter().any(|candidate| {
        auto_candidate_eligible(config, candidate, input_bytes, context_estimate, relaxed)
    })
}

/// Hard-exclude candidates only for capabilities explicitly declared unsupported.
/// Unknown capability flags remain eligible for backward compatibility.
pub fn resolve_auto_with_requirements(
    config: &Config,
    input_bytes: usize,
    context_estimate: usize,
    requirements: AutoRequirements,
    penalties: &[u8],
) -> Result<Vec<ResolvedStep>, String> {
    resolve_auto_filtered(config, penalties, |candidate| {
        auto_candidate_eligible(
            config,
            candidate,
            input_bytes,
            context_estimate,
            requirements,
        )
    })
}

fn resolve_auto_filtered(
    config: &Config,
    penalties: &[u8],
    mut eligible: impl FnMut(&AutoCandidate) -> bool,
) -> Result<Vec<ResolvedStep>, String> {
    if !config.auto.enabled {
        return Err("auto routing is disabled".into());
    }
    let mut ranked = config
        .auto
        .candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| eligible(candidate))
        .collect::<Vec<_>>();
    // Stable tie-break preserves configured order, avoiding non-deterministic
    // model selection on restart or across concurrent requests.
    ranked.sort_by_key(|(position, candidate)| {
        let penalty = if config.auto.health_aware {
            penalties.get(*position).copied().unwrap_or(0)
        } else {
            0
        };
        (penalty, candidate.priority, *position)
    });
    ranked.truncate(config.auto.max_candidates.unwrap_or(32));
    ranked
        .into_iter()
        .map(|(_, candidate)| {
            let provider_index = config
                .providers
                .iter()
                .position(|p| p.name == candidate.provider)
                .ok_or_else(|| "auto candidate references missing provider".to_string())?;
            Ok(ResolvedStep {
                provider_index,
                model: candidate.model.clone(),
                params: candidate.params.clone(),
            })
        })
        .collect()
}

pub fn resolve(config: &Config, logical_model: &str) -> Result<Vec<ResolvedStep>, String> {
    if logical_model.is_empty() {
        return Err("missing model".into());
    }
    fn resolve_chain(
        config: &Config,
        chain: &crate::config::Chain,
    ) -> Result<Vec<ResolvedStep>, String> {
        chain
            .steps
            .iter()
            .map(|step: &Step| {
                let provider_index = config
                    .providers
                    .iter()
                    .position(|p| p.name == step.provider)
                    .ok_or_else(|| format!("missing provider for chain {}", chain.name))?;
                Ok(ResolvedStep {
                    provider_index,
                    model: step.model.clone(),
                    params: step.params.clone(),
                })
            })
            .collect()
    }
    if let Some(chain) = config.chains.iter().find(|c| c.name == logical_model) {
        return resolve_chain(config, chain);
    }
    let mut longest: Option<(usize, usize, String)> = None;
    for (i, provider) in config.providers.iter().enumerate() {
        for prefix in provider.prefixes() {
            let Some(suffix) = logical_model
                .strip_prefix(prefix)
                .and_then(|s| s.strip_prefix('/'))
            else {
                continue;
            };
            if suffix.is_empty() {
                return Err("empty passthrough model".into());
            }
            // Select the provider by the longest matching namespace FIRST.
            // The opt-in controls only the model sent to that provider, never
            // provider selection. Default behavior strips one gateway prefix;
            // namespaced upstreams (e.g. NVIDIA NIM) can forward the full ID.
            // Explicit chain steps remain untouched.
            if longest
                .as_ref()
                .is_none_or(|(size, _, _)| prefix.len() > *size)
            {
                let wire_model = if provider.namespace_on_wire {
                    logical_model.to_owned()
                } else {
                    suffix.to_owned()
                };
                longest = Some((prefix.len(), i, wire_model));
            }
        }
    }
    if let Some((_, provider_index, model)) = longest {
        return Ok(vec![ResolvedStep {
            provider_index,
            model,
            params: BTreeMap::new(),
        }]);
    }
    // Only truly unknown names use the explicitly opted-in fallback. Exact
    // chains and provider-prefixed direct models always take precedence.
    if logical_model != "auto"
        && !config.default_chain.is_empty()
        && let Some(chain) = config
            .chains
            .iter()
            .find(|c| c.name == config.default_chain)
    {
        return resolve_chain(config, chain);
    }
    Err(format!("unknown model or chain: {logical_model}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use serde_json::json;
    use std::hash::Hash;

    #[test]
    fn auto_stream_only_json_aggregation_budget_counts_forced_stream_field() {
        let request = json!({"model":"auto","messages":[{"role":"user","content":"hi"}],
            "max_tokens":80});
        let mut physical = request.clone();
        physical["model"] = json!("sse");
        let cap = auto_context_estimate(&physical) + 1;
        physical["stream"] = json!(true);
        assert!(auto_context_estimate(&physical) > cap);
        let config: Config = serde_yaml::from_str(&format!(
            "providers:\n  - name: stream\n    type: openai\n    base_url: https://stream.invalid\n    key_env: FIRST\n    stream_only: true\n  - name: json\n    type: openai\n    base_url: https://json.invalid\n    key_env: SECOND\nauto:\n  enabled: true\n  max_candidates: 1\n  candidates:\n    - provider: stream\n      model: sse\n      priority: 0\n      max_context_tokens: {cap}\n    - provider: json\n      model: short\n      priority: 10\n      max_context_tokens: 8192\n"
        ))
        .unwrap();
        config.validate().unwrap();
        let (request_bytes, context_estimate) = auto_request_bytes_and_context_estimate(&request);
        let budget = AutoRequestBudget {
            input_bytes: auto_input_bytes(request.get("messages")),
            request_bytes,
            context_estimate,
        };
        let cache = AutoRouteCache::new();
        for _ in 0..2 {
            assert_eq!(
                cache
                    .resolve_for_request(
                        &config,
                        budget,
                        &request,
                        auto_requirements(&request),
                        &[],
                        1,
                    )
                    .unwrap()[0]
                    .model,
                "short"
            );
        }
        // An explicit false stream field is replaced with true on the wire,
        // shrinking the encoded request by one byte. The exact context
        // boundary must still admit this candidate (as must native SSE).
        let mut explicit_off = request.clone();
        explicit_off["stream"] = json!(false);
        let mut actual = explicit_off.clone();
        actual["model"] = json!("sse");
        actual["stream"] = json!(true);
        actual["stream_options"] = json!({"include_usage":true});
        let mut exact = config.clone();
        exact.auto.candidates[0].max_context_tokens = Some(auto_context_estimate(&actual));
        for value in [false, true] {
            let mut candidate_request = request.clone();
            candidate_request["stream"] = json!(value);
            let (request_bytes, context_estimate) =
                auto_request_bytes_and_context_estimate(&candidate_request);
            let chosen = AutoRouteCache::new()
                .resolve_for_request(
                    &exact,
                    AutoRequestBudget {
                        input_bytes: auto_input_bytes(candidate_request.get("messages")),
                        request_bytes,
                        context_estimate,
                    },
                    &candidate_request,
                    auto_requirements(&candidate_request),
                    &[],
                    2,
                )
                .unwrap();
            assert_eq!(chosen[0].model, "sse", "caller stream={value}");
        }
    }

    #[test]
    fn auto_sse_only_budget_matches_forced_usage_options_without_false_admission() {
        let mut cfg: Config = serde_yaml::from_str(
            "providers:\n  - name: s\n    type: openai\n    base_url: https://example.invalid\n    key_env: KEY\n    stream_only: true\nauto:\n  enabled: true\n  candidates:\n    - provider: s\n      model: physical\n      max_context_tokens: 8192\n      params:\n        stream_options:\n          unused_default: ignored\n",
        ).unwrap();
        cfg.validate().unwrap();
        for stream_options in [
            None,
            Some(json!({})),
            Some(json!({"include_usage":false})),
            Some(json!({"include_usage":true,"trace":"hi"})),
            Some(json!({"trace":"unicode 文本","another":42})),
            Some(Value::Null),
            Some(json!("invalid")),
        ] {
            let mut request = json!({"model":"auto","stream":false,
                "messages":[{"role":"user","content":"hello"}],"max_tokens":64});
            if let Some(options) = stream_options {
                request["stream_options"] = options;
            }
            let mut actual = request.clone();
            actual["model"] = json!("physical");
            actual["stream"] = json!(true);
            let mut options = actual["stream_options"]
                .as_object()
                .cloned()
                .unwrap_or_default();
            options.insert("include_usage".into(), Value::Bool(true));
            actual["stream_options"] = Value::Object(options);
            let exact = auto_context_estimate(&actual);
            cfg.auto.candidates[0].max_context_tokens = Some(exact);
            let select = |cfg: &Config| {
                resolve_auto_for_request(
                    cfg,
                    auto_input_bytes(request.get("messages")),
                    &request,
                    auto_requirements(&request),
                    &[],
                )
                .unwrap()
            };
            assert_eq!(
                select(&cfg).len(),
                1,
                "exact wire budget rejected: {request}"
            );
            cfg.auto.candidates[0].max_context_tokens = Some(exact - 1);
            assert!(
                select(&cfg).is_empty(),
                "oversized wire accepted: {request}"
            );
        }
    }

    #[test]
    fn auto_cached_selection_accounts_for_physical_model_bytes_before_truncation() {
        let long_model = "physical-model-".repeat(45);
        let yaml = format!(
            "providers:\n  - name: p\n    type: openai\n    base_url: https://example.invalid\n    key_env: K\nauto:\n  enabled: true\n  max_candidates: 1\n  candidates:\n    - provider: p\n      model: {long_model}\n      priority: 0\n      max_context_tokens: 450\n    - provider: p\n      model: short\n      priority: 10\n      max_context_tokens: 450\n"
        );
        let config: Config = serde_yaml::from_str(&yaml).unwrap();
        config.validate().unwrap();
        let request = json!({"model":"auto","messages":[{"role":"user","content":"hi"}],
            "max_tokens":80});
        let budget = |request: &Value| {
            let (request_bytes, context_estimate) =
                auto_request_bytes_and_context_estimate(request);
            AutoRequestBudget {
                input_bytes: auto_input_bytes(request.get("messages")),
                request_bytes,
                context_estimate,
            }
        };
        let base = budget(&request);
        let requirements = auto_requirements(&request);
        assert!(
            base.context_estimate < 450,
            "incoming logical name still fits the advertised window"
        );
        assert_eq!(
            AutoRouteCache::new()
                .resolve(
                    &config,
                    base.input_bytes,
                    base.context_estimate,
                    requirements,
                    &[],
                    1,
                )
                .unwrap()[0]
                .model,
            long_model,
            "the old estimate missed the physical-model substitution"
        );
        let cache = AutoRouteCache::new();
        for _ in 0..3 {
            let eligible = cache
                .resolve_for_request(&config, base, &request, requirements, &[], 1)
                .unwrap();
            assert_eq!(eligible.len(), 1);
            assert_eq!(eligible[0].model, "short");
        }
        let oversized = json!({"model":"auto","messages":[{"role":"user","content":"z".repeat(1000)}],
            "max_tokens":80});
        assert!(
            cache
                .resolve_for_request(
                    &config,
                    budget(&oversized),
                    &oversized,
                    auto_requirements(&oversized),
                    &[],
                    1,
                )
                .unwrap()
                .is_empty(),
            "a cached short prompt must not admit an oversized request"
        );
        assert_eq!(
            cache
                .resolve_for_request(&config, base, &request, requirements, &[], 1)
                .unwrap()[0]
                .model,
            "short"
        );
    }

    #[test]
    fn direct_json_string_byte_count_matches_value_encoding_with_unicode_and_escapes() {
        let long = "x".repeat(4096);
        for text in [
            "auto",
            "a/b",
            "escaped \"\\ newline\n",
            "模型🐈‍⬛",
            "",
            long.as_str(),
        ] {
            let expected = encoded_json_bytes(Some(&Value::String(text.to_owned())));
            assert_eq!(encoded_json_bytes_of(text), expected, "{text:?}");
        }
    }

    #[test]
    fn request_aware_cache_distinguishes_equal_context_estimates_with_different_body_lengths() {
        let mut small = json!({
            "model":"auto",
            "messages":[{"role":"user","content":"hi"}],
            "max_tokens":3000,
            "user":"x"
        });
        let mut found = false;
        for length in 1..=5 {
            small["user"] = json!("x".repeat(length));
            if encoded_json_bytes(Some(&small)).is_multiple_of(5) {
                found = true;
                break;
            }
        }
        assert!(found);
        let mut large = small.clone();
        large["user"] = json!(format!("{}x", small["user"].as_str().unwrap()));
        large["max_tokens"] = json!(2998);
        let (small_bytes, common_estimate) = auto_request_bytes_and_context_estimate(&small);
        let (large_bytes, large_estimate) = auto_request_bytes_and_context_estimate(&large);
        assert_eq!(large_bytes, small_bytes + 1);
        assert_eq!(
            large_estimate, common_estimate,
            "the old cache key cannot distinguish these two real requests"
        );
        let cfg: Config = serde_yaml::from_str(&format!(
            "providers:\n  - name: p\n    type: openai\n    base_url: https://example.invalid\n    key_env: K\nauto:\n  enabled: true\n  candidates:\n    - provider: p\n      model: short\n      max_context_tokens: {}\n",
            common_estimate + 1
        ))
        .unwrap();
        cfg.validate().unwrap();
        let cache = AutoRouteCache::new();
        let pick = |req: &Value, bytes: usize| {
            cache
                .resolve_for_request(
                    &cfg,
                    AutoRequestBudget {
                        input_bytes: auto_input_bytes(req.get("messages")),
                        request_bytes: bytes,
                        context_estimate: common_estimate,
                    },
                    req,
                    auto_requirements(req),
                    &[],
                    1,
                )
                .unwrap()
        };
        // Replacing "auto" with "short" adds one JSON byte. The 20%
        // ceil margin makes the smaller body exceed the window by 1 more.
        assert_eq!(pick(&large, large_bytes)[0].model, "short");
        assert!(pick(&small, small_bytes).is_empty());
        assert_eq!(pick(&large, large_bytes)[0].model, "short");
        assert!(pick(&small, small_bytes).is_empty());
        assert_eq!(cache.state.lock().unwrap().entries.len(), 2);
    }

    #[test]
    fn allocation_free_config_generation_matches_original_fingerprint_contract() {
        let mut config: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://example.invalid
    key_env: K
auto:
  enabled: true
  candidates:
    - provider: p
      model: one
"#,
        )
        .unwrap();
        config.validate().unwrap();
        let baseline = config_generation(&config);
        for with_large_params in [false, true] {
            if with_large_params {
                config.auto.candidates[0].params.insert(
                    "tools".into(),
                    json!([{"type":"function","function":{"name":"search",
                        "description":"x".repeat(900_000)}}]),
                );
            }
            let mut original = DefaultHasher::new();
            format!("{config:?}").hash(&mut original);
            assert_eq!(
                config_generation(&config),
                original.finish(),
                "streaming generation changed the existing cache fingerprint"
            );
        }
        assert_ne!(baseline, config_generation(&config));
        let before = config_generation(&config);
        config.auto.candidates[0]
            .params
            .insert("temperature".into(), json!(0.7));
        assert_ne!(before, config_generation(&config));
    }
    #[test]
    fn dual_output_budget_fields_use_the_conservative_ceiling() {
        let request = json!({"messages":[{"role":"user","content":"hi"}],
            "max_tokens":10000,"max_completion_tokens":16});
        let bytes = encoded_json_bytes(Some(&request));
        assert_eq!(
            auto_context_estimate(&request),
            with_context_safety_margin(bytes).saturating_add(10000)
        );
    }
    #[test]
    fn auto_candidate_defaults_respect_actual_wire_budget_and_caller_precedence() {
        let yaml = r#"
providers:
  - name: p
    type: openai
    base_url: https://example.invalid
    key_env: K
    models: [default-large, fallback]
auto:
  enabled: true
  max_candidates: 1
  candidates:
    - provider: p
      model: default-large
      max_context_tokens: 4096
      params:
        max_tokens: 10000
    - provider: p
      model: fallback
      max_context_tokens: 4096
"#;
        let mut cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        let request = json!({"model":"auto","messages":[{"role":"user","content":"hi"}]});
        let choose = |cfg: &Config, request: &Value| {
            resolve_auto_for_request(
                cfg,
                auto_input_bytes(request.get("messages")),
                request,
                auto_requirements(request),
                &[],
            )
            .unwrap()
        };
        assert_eq!(choose(&cfg, &request)[0].model, "fallback");

        // The caller's explicit field, even if smaller than the candidate
        // default, must override it both for routing and eventual injection.
        let caller_budget =
            json!({"model":"auto","messages":[{"role":"user","content":"hi"}],"max_tokens":64});
        assert_eq!(choose(&cfg, &caller_budget)[0].model, "default-large");
        // An explicit *other* alias does not replace the candidate's
        // max_tokens default on the wire; conservative eligibility counts both.
        let other_alias = json!({"model":"auto","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":64});
        assert_eq!(choose(&cfg, &other_alias)[0].model, "fallback");

        cfg.auto.candidates[0].params.remove("max_tokens");
        cfg.auto.candidates[0].supports_tools = Some(false);
        cfg.auto.candidates[0].params.insert(
            "tools".into(),
            json!([{"type":"function","function":{"name":"noop"}}]),
        );
        assert_eq!(choose(&cfg, &request)[0].model, "fallback");

        cfg.auto.candidates[0].supports_tools = Some(true);
        assert_eq!(choose(&cfg, &request)[0].model, "default-large");

        // Serialization of large default tool schemas must count toward the
        // candidate context window, not just the caller's original message.
        cfg.auto.candidates[0].params.insert(
            "tools".into(),
            json!([{"type":"function","function":{"name":"noop","description":"x".repeat(6000)}}]),
        );
        assert_eq!(choose(&cfg, &request)[0].model, "fallback");
        // An explicit empty caller tool array wins over the long default.
        let caller_no_tools =
            json!({"model":"auto","messages":[{"role":"user","content":"hi"}],"tools":[]});
        assert_eq!(choose(&cfg, &caller_no_tools)[0].model, "default-large");

        cfg.auto.candidates[0].params.clear();
        cfg.auto.candidates[0].supports_json_schema = Some(false);
        cfg.auto.candidates[0].params.insert(
            "response_format".into(),
            json!({"type":"json_schema","json_schema":{"name":"strict","schema":{"type":"object"}}}),
        );
        assert_eq!(choose(&cfg, &request)[0].model, "fallback");
        let caller_plain = json!({"model":"auto","messages":[{"role":"user","content":"hi"}],
            "response_format":{"type":"text"}});
        assert_eq!(choose(&cfg, &caller_plain)[0].model, "default-large");
    }

    #[test]
    fn go_operator_floor_degrades_only_when_all_other_constraints_hold() {
        let yaml = r#"
providers:
  - name: p
    type: openai
    base_url: https://example.invalid
    key_env: K
    models: [short]
auto:
  enabled: true
  default_min_context: 8192
  candidates:
    - provider: p
      model: short
      max_context_tokens: 4096
"#;
        let mut config: Config = serde_yaml::from_str(yaml).unwrap();
        config.validate().unwrap();
        let default = AutoRequirements::default().with_context_bounds(8192, None);
        assert!(
            resolve_auto_with_requirements(&config, 100, 500, default, &[])
                .unwrap()
                .is_empty()
        );
        assert!(auto_operator_default_can_degrade(
            &config, 100, 500, default
        ));
        assert_eq!(
            resolve_auto_with_requirements(
                &config,
                100,
                500,
                default.with_context_bounds(0, None),
                &[],
            )
            .unwrap()
            .len(),
            1
        );
        assert!(!auto_operator_default_can_degrade(
            &config,
            100,
            500,
            default.with_context_bounds(8192, Some(8192)),
        ));
        config.auto.candidates[0].supports_images = Some(false);
        assert!(!auto_operator_default_can_degrade(
            &config,
            100,
            500,
            AutoRequirements::from_signals(false, true, false).with_context_bounds(8192, None),
        ));
        config.auto.candidates[0].supports_images = None;
        config.auto.candidates[0].min_input_bytes = 1024;
        assert!(!auto_operator_default_can_degrade(
            &config, 100, 500, default
        ));
        config.auto.candidates[0].min_input_bytes = 0;
        assert!(!auto_operator_default_can_degrade(
            &config, 100, 8192, default
        ));
        config.auto.candidates[0].supports_tools = Some(false);
        assert!(!auto_operator_default_can_degrade(
            &config,
            100,
            500,
            AutoRequirements::from_signals(true, false, false).with_context_bounds(8192, None),
        ));
    }

    #[test]
    fn operator_floor_fallback_uses_only_hard_eligible_candidates_in_mixed_pool() {
        let yaml = r#"
providers:
  - name: p
    type: openai
    base_url: https://example.invalid
    key_env: K
    models: [text-only, tools-small, oversized]
auto:
  enabled: true
  default_min_context: 8192
  candidates:
    - provider: p
      model: text-only
      priority: 1
      max_context_tokens: 4096
      supports_tools: false
    - provider: p
      model: tools-small
      priority: 10
      max_context_tokens: 4096
      supports_tools: true
      supports_images: false
    - provider: p
      model: oversized
      priority: 0
      max_context_tokens: 4096
      supports_tools: true
      min_input_bytes: 200
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        let required =
            AutoRequirements::from_signals(true, false, false).with_context_bounds(8192, None);
        let cache = AutoRouteCache::new();
        assert!(
            cache
                .resolve(&cfg, 100, 500, required, &[], 1)
                .unwrap()
                .is_empty(),
            "the operator floor initially excludes all candidates"
        );
        assert!(
            auto_operator_default_can_degrade(&cfg, 100, 500, required),
            "incompatible neighbors cannot veto an otherwise safe fallback"
        );
        let relaxed = required.with_context_bounds(0, None);
        let chosen = cache.resolve(&cfg, 100, 500, relaxed, &[], 1).unwrap();
        assert_eq!(chosen.len(), 1);
        assert_eq!(chosen[0].model, "tools-small");
        assert_eq!(
            cache.resolve(&cfg, 100, 500, relaxed, &[], 1).unwrap()[0].model,
            "tools-small"
        );

        let needs_images =
            AutoRequirements::from_signals(true, true, false).with_context_bounds(8192, None);
        assert!(!auto_operator_default_can_degrade(
            &cfg,
            100,
            500,
            needs_images
        ));
        assert!(
            resolve_auto_with_requirements(
                &cfg,
                100,
                500,
                needs_images.with_context_bounds(0, None),
                &[]
            )
            .unwrap()
            .is_empty(),
            "relaxing min_context must not bypass image and tool hard filters"
        );
        assert!(
            !auto_operator_default_can_degrade(&cfg, 100, 4097, required),
            "context fit remains a hard limit"
        );
        assert!(
            !auto_operator_default_can_degrade(
                &cfg,
                100,
                500,
                required.with_context_bounds(8192, Some(16384))
            ),
            "the presence of a caller max_context must prohibit relaxation"
        );
    }

    #[test]
    fn go_service_min_context_default_respects_request_override_and_max_filter() {
        let request = serde_json::json!({});
        assert_eq!(
            auto_context_bounds_with_default(&request, true, Some(262_144)),
            Ok((262_144, None))
        );
        assert_eq!(
            auto_context_bounds_with_default(
                &serde_json::json!({"min_context": 8_192, "max_context": 16_384}),
                true,
                Some(262_144)
            ),
            Ok((8_192, Some(16_384)))
        );
        assert!(
            auto_context_bounds_with_default(
                &serde_json::json!({"max_context": 16_384}),
                true,
                Some(262_144)
            )
            .is_err()
        );
        assert_eq!(
            auto_context_bounds_with_default(
                &serde_json::json!({"min_context": 0}),
                false,
                Some(2048)
            ),
            Ok((2048, None))
        );
    }

    #[test]
    fn go_auto_context_bound_parser_is_bounded_and_registry_opt_in() {
        assert_eq!(
            auto_context_bounds(&serde_json::json!({}), false),
            Ok((0, None))
        );
        assert_eq!(
            auto_context_bounds(&serde_json::json!({}), true),
            Ok((512, None))
        );
        assert_eq!(
            auto_context_bounds(
                &serde_json::json!({"min_context":8192,"max_context":16384}),
                false
            ),
            Ok((8192, Some(16384)))
        );
        assert_eq!(
            auto_context_bounds(&serde_json::json!({"min_context":-9,"max_context":0}), true),
            Ok((512, None))
        );
        for bad in [
            serde_json::json!({"min_context":1000,"max_context":999}),
            serde_json::json!({"max_context":200}),
            serde_json::json!({"min_context":"8192"}),
            serde_json::json!({"max_context":1.25}),
            serde_json::json!({"min_context":null}),
            serde_json::json!({"max_context":false}),
        ] {
            assert!(auto_context_bounds(&bad, true).is_err(), "{bad}");
        }
    }

    #[test]
    fn auto_context_size_matches_original_serialization_without_copying() {
        let samples = [
            serde_json::json!(null),
            serde_json::json!([]),
            serde_json::json!([{"role":"user","content":"中文 \\n \\\" \\u{1f642}"}]),
            serde_json::json!([{"role":"assistant","tool_calls":[{"arguments":"{\\\"x\\\":1}"}]}]),
            serde_json::json!([{"role":"user","content":"a".repeat(1024 * 1024)}]),
        ];
        assert_eq!(auto_input_bytes(None), 0);
        for sample in &samples {
            assert_eq!(auto_input_bytes(Some(sample)), sample.to_string().len());
        }
    }

    #[test]
    fn legacy_and_request_aware_context_margin_preserve_boundary_behavior() {
        for bytes in [0, 1, 4, 5, 99, 100, 101, usize::MAX / 20, usize::MAX] {
            let original = bytes.saturating_add(
                bytes
                    .saturating_mul(CONTEXT_SAFETY_MARGIN_PERCENT)
                    .div_ceil(100),
            );
            assert_eq!(with_context_safety_margin(bytes), original, "{bytes}");
        }
        let request =
            serde_json::json!({"messages":[{"role":"user","content":"你好"}], "max_tokens":256});
        let bytes = request.to_string().len();
        assert_eq!(
            auto_context_estimate(&request),
            with_context_safety_margin(bytes) + 256,
        );
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://p.example
    key_env: P
auto:
  enabled: true
  candidates:
    - provider: p
      model: m
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        for bytes in [0, 1, 99, 100, 101, usize::MAX] {
            let legacy = resolve_auto(&cfg, bytes).unwrap();
            let health = resolve_auto_with_health(&cfg, bytes, &[]).unwrap();
            let explicit =
                resolve_auto_with_estimate(&cfg, bytes, with_context_safety_margin(bytes), &[])
                    .unwrap();
            let names = |steps: Vec<ResolvedStep>| {
                steps.into_iter().map(|step| step.model).collect::<Vec<_>>()
            };
            assert_eq!(names(legacy), names(health), "{bytes}");
            assert_eq!(
                names(explicit),
                names(resolve_auto(&cfg, bytes).unwrap()),
                "{bytes}"
            );
        }
    }

    #[test]
    fn auto_boundary_and_priority_are_deterministic() {
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: primary
    type: openai
    base_url: https://primary.example
    key_env: P
    models: [alpha]
  - name: backup
    type: openai
    base_url: https://backup.example
    key_env: B
    models: [beta]
auto:
  enabled: true
  max_candidates: 1
  candidates:
    - provider: backup
      model: beta
      priority: 20
      min_input_bytes: 100
      max_input_bytes: 200
      max_context_tokens: 180
    - provider: primary
      model: alpha
      priority: 10
      min_input_bytes: 100
      max_input_bytes: 200
      max_context_tokens: 180
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert!(resolve_auto(&cfg, 99).unwrap().is_empty());
        assert!(resolve_auto(&cfg, 201).unwrap().is_empty());
        assert!(resolve_auto(&cfg, 181).unwrap().is_empty());
        for size in [100, 150] {
            let steps = resolve_auto(&cfg, size).unwrap();
            assert_eq!(steps.len(), 1);
            assert_eq!(steps[0].model, "alpha");
        }
    }
    #[test]
    fn context_estimate_includes_tools_and_reserves_twenty_percent() {
        let request = serde_json::json!({
            "messages": [{"role":"user","content":"hello"}],
            "tools": [{"type":"function","function":{"description":"x".repeat(1000)}}],
            "max_completion_tokens": 256
        });
        let message_bytes = auto_input_bytes(request.get("messages"));
        let estimate = auto_context_estimate(&request);
        let payload_bytes = request.to_string().len();
        assert!(estimate > message_bytes);
        assert_eq!(
            estimate,
            payload_bytes + payload_bytes.saturating_mul(20).div_ceil(100) + 256
        );
    }

    #[test]
    fn auto_hard_filters_models_outside_an_explicit_provider_catalog() {
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: fast
    type: openai
    base_url: https://fast.example
    key_env: F
    models: [known-fast]
  - name: backup
    type: openai
    base_url: https://backup.example
    key_env: B
    models: [known-backup]
auto:
  enabled: true
  candidates:
    - provider: fast
      model: not-in-catalog
      priority: 1
    - provider: backup
      model: known-backup
      priority: 2
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        let steps = resolve_auto(&cfg, 0).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].provider_index, 1);
        assert_eq!(steps[0].model, "known-backup");
    }

    #[test]
    fn auto_route_cache_reorders_when_live_health_changes() {
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: fast
    type: openai
    base_url: https://fast.example
    key_env: F
    models: [alpha]
  - name: backup
    type: openai
    base_url: https://backup.example
    key_env: B
    models: [beta]
auto:
  enabled: true
  health_aware: true
  candidates:
    - provider: fast
      model: alpha
      priority: 1
    - provider: backup
      model: beta
      priority: 2
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        let cache = AutoRouteCache::new();
        assert_eq!(
            cache
                .resolve(&cfg, 50, 60, AutoRequirements::default(), &[0, 0], 7)
                .unwrap()[0]
                .model,
            "alpha"
        );
        assert_eq!(
            cache
                .resolve(&cfg, 50, 60, AutoRequirements::default(), &[100, 0], 7)
                .unwrap()[0]
                .model,
            "beta"
        );
    }
    #[test]
    fn auto_health_isolates_models_sharing_one_provider() {
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: shared
    type: openai
    base_url: https://shared.example
    key_env: SHARED
    models: [unhealthy-model, healthy-model]
auto:
  enabled: true
  health_aware: true
  max_candidates: 1
  candidates:
    - provider: shared
      model: unhealthy-model
      priority: 1
    - provider: shared
      model: healthy-model
      priority: 2
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        let baseline = resolve_auto_with_health(&cfg, 42, &[0, 0]).unwrap();
        assert_eq!(baseline[0].model, "unhealthy-model");
        let demoted = resolve_auto_with_health(&cfg, 42, &[100, 0]).unwrap();
        assert_eq!(demoted[0].provider_index, 0);
        assert_eq!(demoted[0].model, "healthy-model");
        let recovered = resolve_auto_with_health(&cfg, 42, &[0, 0]).unwrap();
        assert_eq!(recovered[0].model, "unhealthy-model");
    }
    #[test]
    fn explicit_capability_exclusions_are_hard_filters_and_cache_keys_are_distinct() {
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: shared
    type: openai
    base_url: https://shared.example
    key_env: KEY
    models: [basic,rich]
auto:
  enabled: true
  max_candidates: 1
  candidates:
    - provider: shared
      model: basic
      priority: 1
      supports_tools: false
      supports_images: false
      supports_json_schema: false
    - provider: shared
      model: rich
      priority: 2
      supports_tools: true
      supports_images: true
      supports_json_schema: true
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        let image_request = serde_json::json!({
            "messages": [{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,YQ=="}}]}],
            "tools": [{"type":"function","function":{"name":"search"}}],
            "response_format": {"type":"json_schema","json_schema":{"name":"result","schema":{"type":"object"}}}
        });
        let features = auto_requirements(&image_request);
        assert_eq!(
            features,
            AutoRequirements {
                tools: true,
                images: true,
                json_schema: true,
                ..AutoRequirements::default()
            }
        );
        let cache = AutoRouteCache::new();
        let fallback = cache.resolve(&cfg, 50, 100, features, &[], 55).unwrap();
        assert_eq!(fallback[0].model, "rich");
        // Identical size and health data cannot reuse an incompatible cache entry.
        let plain = cache
            .resolve(&cfg, 50, 100, AutoRequirements::default(), &[], 55)
            .unwrap();
        assert_eq!(plain[0].model, "basic");
        let mut single = features;
        single.images = false;
        single.json_schema = false;
        assert_eq!(
            resolve_auto_with_requirements(&cfg, 50, 100, single, &[]).unwrap()[0].model,
            "rich"
        );
        assert_eq!(cache.state.lock().unwrap().entries.len(), 2);
    }

    #[test]
    fn tool_message_history_requires_tool_support_even_without_tools_declaration() {
        let request = serde_json::json!({
            "messages": [{"role":"tool","tool_call_id":"call_123","content":"ready"}]
        });
        assert!(auto_requirements(&request).tools);
        assert!(
            !auto_requirements(&serde_json::json!({"messages":[{"role":"user","content":"hi"}]}))
                .tools
        );
    }
    #[test]
    fn tool_choice_and_null_history_do_not_bypass_or_overconstrain_auto() {
        let null_history = serde_json::json!({"messages":[{"role":"assistant","tool_calls":null}]});
        let empty_history = serde_json::json!({"messages":[{"role":"assistant","tool_calls":[]}]});
        assert!(!auto_requirements(&null_history).tools);
        assert!(!auto_requirements(&empty_history).tools);

        for choice in [
            serde_json::json!("required"),
            serde_json::json!({"type":"function","function":{"name":"lookup"}}),
            serde_json::json!("unrecognized_choice"),
        ] {
            let request = serde_json::json!({"tool_choice":choice});
            assert!(auto_requirements(&request).tools);
        }
        for choice in ["none", "auto"] {
            assert!(!auto_requirements(&serde_json::json!({"tool_choice":choice})).tools);
        }
        let historical = serde_json::json!({"messages":[{"role":"assistant",
            "tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}]});
        assert!(auto_requirements(&historical).tools);
        assert!(
            auto_requirements(&serde_json::json!({
                "messages":[{"role":"assistant","tool_calls":"malformed"}]
            }))
            .tools
        );

        let config: Config = serde_yaml::from_str(
            "providers:\n  - name: p\n    type: openai\n    base_url: https://example.invalid\n    key_env: K\nauto:\n  enabled: true\n  candidates:\n    - provider: p\n      model: text\n      supports_tools: false\n    - provider: p\n      model: tool\n      supports_tools: true\n",
        )
        .unwrap();
        config.validate().unwrap();
        let selected = resolve_auto_with_requirements(
            &config,
            10,
            100,
            auto_requirements(&serde_json::json!({"tool_choice":"required"})),
            &[],
        )
        .unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].model, "tool");
        let selected =
            resolve_auto_with_requirements(&config, 10, 100, auto_requirements(&null_history), &[])
                .unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].model, "text");
    }
    #[test]
    fn optional_go_capability_registry_and_local_overrides_filter_safely() {
        let yaml = r#"
providers:
  - name: p
    type: openai
    base_url: https://provider.example
    key_env: K
    models: [DeepSeek-V4-Flash, Qwen3.8-27B]
auto:
  enabled: true
  use_model_registry: true
  candidates:
    - provider: p
      model: DeepSeek-V4-Flash
      priority: 1
    - provider: p
      model: Qwen3.8-27B
      priority: 2
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        let images = AutoRequirements::from_signals(false, true, false);
        let ranked = resolve_auto_with_requirements(&cfg, 20, 100, images, &[]).unwrap();
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].model, "Qwen3.8-27B");
        let huge =
            resolve_auto_with_requirements(&cfg, 20, 140_000, AutoRequirements::default(), &[])
                .unwrap();
        assert_eq!(huge.len(), 1);
        assert_eq!(huge[0].model, "DeepSeek-V4-Flash");

        let disabled: Config = serde_yaml::from_str(
            &yaml.replace("  use_model_registry: true", "  use_model_registry: false"),
        )
        .unwrap();
        let all = resolve_auto_with_requirements(&disabled, 20, 140_000, images, &[]).unwrap();
        assert_eq!(all.len(), 2);
        let overridden: Config = serde_yaml::from_str(&yaml.replace(
            "  use_model_registry: true",
            "  use_model_registry: true\n  capability_overrides:\n    Qwen3.8-27B:\n      context_tokens: 256\n      supports_images: false",
        )).unwrap();
        assert!(
            resolve_auto_with_requirements(&overridden, 20, 100, images, &[])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn auto_route_cache_is_bounded_ttl_limited_and_generation_fenced() {
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://p.example
    key_env: P
    models: [m]
auto:
  enabled: true
  candidates:
    - provider: p
      model: m
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        let cache = AutoRouteCache::new();
        for size in 0..(AUTO_CACHE_CAPACITY + 8) {
            cache
                .resolve(&cfg, size, size, AutoRequirements::default(), &[], 11)
                .unwrap();
        }
        {
            let state = cache.state.lock().unwrap();
            assert_eq!(state.entries.len(), AUTO_CACHE_CAPACITY);
            assert!(state.entries.iter().all(|entry| entry.key.generation == 11));
        }
        cache
            .resolve(&cfg, 1, 1, AutoRequirements::default(), &[], 12)
            .unwrap();
        {
            let mut state = cache.state.lock().unwrap();
            assert_eq!(state.entries.len(), AUTO_CACHE_CAPACITY);
            assert_eq!(state.entries.back().unwrap().key.generation, 12);
            state.entries.back_mut().unwrap().expires_at = Instant::now() - Duration::from_secs(1);
        }
        cache
            .resolve(&cfg, 1, 1, AutoRequirements::default(), &[], 12)
            .unwrap();
        let state = cache.state.lock().unwrap();
        assert!(state.entries.len() <= AUTO_CACHE_CAPACITY);
        assert!(
            state
                .entries
                .iter()
                .any(|entry| { entry.key.generation == 12 && entry.expires_at > Instant::now() })
        );
    }

    #[test]
    fn old_inflight_route_generation_cannot_evict_new_hot_generation() {
        let old: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://p.example
    key_env: P
auto:
  enabled: true
  candidates:
    - provider: p
      model: old-model
"#,
        )
        .unwrap();
        let mut new = old.clone();
        new.auto.candidates[0].model = "new-model".into();
        let generation_old = config_generation(&old);
        let generation_new = config_generation(&new);
        assert_ne!(generation_old, generation_new);
        let cache = AutoRouteCache::new();
        let lookup = |config, generation| {
            cache
                .resolve(config, 25, 30, AutoRequirements::default(), &[], generation)
                .unwrap()[0]
                .model
                .clone()
        };
        assert_eq!(lookup(&old, generation_old), "old-model");
        assert_eq!(lookup(&new, generation_new), "new-model");
        // An old in-flight request must not invalidate the new request's
        // cached candidate merely because it resolved after publication.
        for _ in 0..24 {
            assert_eq!(lookup(&old, generation_old), "old-model");
            assert_eq!(lookup(&new, generation_new), "new-model");
        }
        let state = cache.state.lock().unwrap();
        assert_eq!(state.entries.len(), 2);
        assert!(
            state
                .entries
                .iter()
                .any(|entry| entry.key.generation == generation_old)
        );
        assert!(
            state
                .entries
                .iter()
                .any(|entry| entry.key.generation == generation_new)
        );
    }

    #[test]
    fn auto_cache_concurrent_cold_misses_and_lru_hits_do_not_duplicate_entries() {
        use std::sync::{Arc, Barrier};
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://p.example
    key_env: P
auto:
  enabled: true
  candidates:
    - provider: p
      model: m
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        let cache = Arc::new(AutoRouteCache::new());
        let cfg = Arc::new(cfg);
        const WORKERS: usize = 16;
        let barrier = Arc::new(Barrier::new(WORKERS));
        std::thread::scope(|scope| {
            for _ in 0..WORKERS {
                let barrier = Arc::clone(&barrier);
                let cfg = Arc::clone(&cfg);
                let cache = Arc::clone(&cache);
                scope.spawn(move || {
                    barrier.wait();
                    assert_eq!(
                        cache
                            .resolve(&cfg, 7, 9, AutoRequirements::default(), &[], 77)
                            .unwrap()
                            .len(),
                        1
                    );
                });
            }
        });
        {
            let state = cache.state.lock().unwrap();
            assert_eq!(state.entries.len(), 1);
            assert!(state.entries.iter().all(|entry| entry.key.generation == 77));
        }
        for size in 0..AUTO_CACHE_CAPACITY {
            cache
                .resolve(&cfg, size, size, AutoRequirements::default(), &[], 78)
                .unwrap();
        }
        // Touch oldest key, then insert another: key zero survives, key one is evicted.
        cache
            .resolve(&cfg, 0, 0, AutoRequirements::default(), &[], 78)
            .unwrap();
        cache
            .resolve(
                &cfg,
                AUTO_CACHE_CAPACITY,
                AUTO_CACHE_CAPACITY,
                AutoRequirements::default(),
                &[],
                78,
            )
            .unwrap();
        let state = cache.state.lock().unwrap();
        assert_eq!(state.entries.len(), AUTO_CACHE_CAPACITY);
        assert!(state.entries.iter().all(|entry| entry.key.generation == 78));
        assert_eq!(
            state
                .entries
                .iter()
                .filter(|entry| entry.key.input_bytes == 0)
                .count(),
            1
        );
        assert!(!state.entries.iter().any(|entry| entry.key.input_bytes == 1));
    }
    #[test]
    fn auto_fallback_limit_rejects_zero_and_oversized_values() {
        let base = r#"
providers:
  - name: p
    type: openai
    base_url: https://p.example
    key_env: P
auto:
  enabled: true
  candidates:
    - provider: p
      model: m
"#;
        for invalid in [0, 33, usize::MAX] {
            let yaml = base.replace(
                "  enabled: true",
                &format!("  enabled: true\n  max_candidates: {invalid}"),
            );
            let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
            assert!(cfg.validate().is_err(), "{invalid} should fail");
        }
        let cfg: Config = serde_yaml::from_str(base).unwrap();
        cfg.validate().unwrap();
        assert_eq!(resolve_auto(&cfg, 0).unwrap().len(), 1);
    }
    #[test]
    fn auto_v2_demotes_unhealthy_provider_only_when_explicitly_enabled() {
        let yaml = r#"
providers:
  - name: fast
    type: openai
    base_url: https://fast.example
    key_env: F
    models: [alpha]
  - name: backup
    type: openai
    base_url: https://backup.example
    key_env: B
    models: [beta]
auto:
  enabled: true
  health_aware: true
  max_candidates: 1
  candidates:
    - provider: fast
      model: alpha
      priority: 1
    - provider: backup
      model: beta
      priority: 99
"#;
        let enabled: Config = serde_yaml::from_str(yaml).unwrap();
        enabled.validate().unwrap();
        assert_eq!(resolve_auto(&enabled, 50).unwrap()[0].model, "alpha");
        assert_eq!(
            resolve_auto_with_health(&enabled, 50, &[100, 0]).unwrap()[0].model,
            "beta"
        );

        let disabled: Config =
            serde_yaml::from_str(&yaml.replace("health_aware: true", "health_aware: false"))
                .unwrap();
        disabled.validate().unwrap();
        assert_eq!(
            resolve_auto_with_health(&disabled, 50, &[100, 0]).unwrap()[0].model,
            "alpha",
        );
    }
    #[test]
    fn namespace_on_wire_provider_forwards_full_id_issue_132() {
        // NVIDIA NIM ids carry the same namespace as the gateway prefix;
        // namespace_on_wire forwards the whole logical id verbatim (issue #132).
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: nvidia
    type: openai
    base_url: https://integrate.api.nvidia.com
    key_env: KEY
    models: [nvidia/nemotron-3-embed-1b, nvidia/nemotron-3.5-embed-1b]
    namespace_on_wire: true
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            resolve(&cfg, "nvidia/nemotron-3-embed-1b").unwrap()[0].model,
            "nvidia/nemotron-3-embed-1b"
        );
        assert_eq!(
            resolve(&cfg, "nvidia/nemotron-3.5-embed-1b").unwrap()[0].model,
            "nvidia/nemotron-3.5-embed-1b"
        );
    }

    #[test]
    fn namespace_on_wire_never_hijacks_other_providers_or_unknown_models_issue_132() {
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: nvidia
    type: openai
    base_url: https://nvidia.example
    key_env: NVIDIA_KEY
    namespace_on_wire: true
    prefixes: [nvidia, nim]
  - name: other
    type: openai
    base_url: https://other.example
    key_env: OTHER_KEY
    prefixes: [other, nvidia/nemotron]
chains:
  - name: exact
    steps:
      - provider: other
        model: physical-model
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        for (requested, index, wire) in [
            (
                "nvidia/nemotron-3-embed-1b",
                0,
                "nvidia/nemotron-3-embed-1b",
            ),
            ("nim/a-new-model", 0, "nim/a-new-model"),
            ("other/chat", 1, "chat"),
            ("nvidia/nemotron/nested", 1, "nested"),
            ("exact", 1, "physical-model"),
        ] {
            let steps = resolve(&cfg, requested).unwrap();
            assert_eq!(steps.len(), 1, "{requested}");
            assert_eq!(steps[0].provider_index, index, "{requested}");
            assert_eq!(steps[0].model, wire, "{requested}");
        }
        for unknown in ["nvidiax/model", "unconfigured/model", "other/", "auto"] {
            assert!(resolve(&cfg, unknown).is_err(), "{unknown}");
        }
    }

    #[test]
    fn longest_provider_namespace_wins_independent_of_yaml_order() {
        let config: Config = serde_yaml::from_str(
            r#"
providers:
  - name: broad
    type: openai
    base_url: https://first.example
    key_env: KEY1
    prefixes: [openai]
  - name: narrow
    type: openai
    base_url: https://second.example
    key_env: KEY2
    prefixes: [openai/gpt]
"#,
        )
        .unwrap();
        config.validate().unwrap();
        let routed = resolve(&config, "openai/gpt/4o").unwrap();
        assert_eq!(routed[0].provider_index, 1);
        assert_eq!(routed[0].model, "4o");
    }

    #[test]
    fn chain_and_namespaced_passthrough() {
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: fast
    type: openai
    base_url: https://fast.example
    key_env: F
    models: [model1]
  - name: bridge
    type: openai
    base_url: https://bridge.example
    key_env: B
    models: [gpt-something, openai/gpt-4o]
    prefixes: [openai, bridge]
chains:
  - name: mychain
    steps:
      - provider: fast
        model: model1
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(resolve(&cfg, "mychain").unwrap()[0].model, "model1");
        assert_eq!(resolve(&cfg, "fast/model1").unwrap()[0].model, "model1");
        assert_eq!(
            resolve(&cfg, "openai/gpt-something").unwrap()[0].model,
            "gpt-something"
        );
        assert_eq!(
            resolve(&cfg, "openai/gpt-uncatalogued").unwrap()[0].model,
            "gpt-uncatalogued"
        );
        assert_eq!(
            resolve(&cfg, "bridge/openai/gpt-4o").unwrap()[0].model,
            "openai/gpt-4o"
        );
        assert!(resolve(&cfg, "missing").is_err());
    }

    #[test]
    fn default_chain_applies_only_to_unknown_names_and_never_shadows_explicit_routes() {
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://p.example
    key_env: P
chains:
  - name: first
    steps:
      - provider: p
        model: first-physical
  - name: backup
    steps:
      - provider: p
        model: backup-physical
default_chain: backup
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            resolve(&cfg, "unknown").unwrap()[0].model,
            "backup-physical"
        );
        assert_eq!(resolve(&cfg, "first").unwrap()[0].model, "first-physical");
        assert_eq!(resolve(&cfg, "p/direct").unwrap()[0].model, "direct");
        assert!(
            resolve(&cfg, "auto").is_err(),
            "auto is reserved, not a fallback typo"
        );
        let no_default: Config = serde_yaml::from_str("providers: []").unwrap();
        assert!(resolve(&no_default, "unknown").is_err());
    }
}
