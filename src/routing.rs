//! Named chain and provider-prefixed passthrough resolution.
use crate::{
    config::{Config, Step},
    model_capabilities,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    sync::Mutex,
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
}

impl AutoRequirements {
    pub fn from_signals(tools: bool, images: bool, json_schema: bool) -> Self {
        Self {
            tools,
            images,
            json_schema,
        }
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
        || request
            .get("messages")
            .and_then(Value::as_array)
            .is_some_and(|messages| {
                messages.iter().any(|message| {
                    message.get("tool_calls").is_some()
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
    AutoRequirements {
        tools,
        images,
        json_schema,
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct CacheKey {
    generation: u64,
    input_bytes: usize,
    context_estimate: usize,
    requirements: AutoRequirements,
    penalties: Vec<u8>,
}
struct CacheEntry {
    key: CacheKey,
    expires_at: Instant,
    steps: Vec<ResolvedStep>,
}
#[derive(Default)]
struct CacheState {
    generation: Option<u64>,
    entries: VecDeque<CacheEntry>,
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
        let key = CacheKey {
            generation,
            input_bytes,
            context_estimate,
            requirements,
            penalties: if config.auto.health_aware {
                penalties.to_vec()
            } else {
                Vec::new()
            },
        };
        let now = Instant::now();
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.generation != Some(generation) {
                state.entries.clear();
                state.generation = Some(generation);
            }
            state.entries.retain(|entry| entry.expires_at > now);
            if let Some(index) = state.entries.iter().position(|entry| entry.key == key) {
                let entry = state.entries.remove(index).expect("located cache entry");
                let steps = entry.steps.clone();
                state.entries.push_back(entry);
                return Ok(steps);
            }
        }

        let steps = resolve_auto_with_requirements(
            config,
            input_bytes,
            context_estimate,
            requirements,
            penalties,
        )?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.generation != Some(generation) {
            state.entries.clear();
            state.generation = Some(generation);
        }
        state
            .entries
            .retain(|entry| entry.expires_at > Instant::now());
        // Concurrent cold misses for the same key must not evict other entries.
        if let Some(index) = state.entries.iter().position(|entry| entry.key == key) {
            let entry = state.entries.remove(index).expect("located cache entry");
            state.entries.push_back(entry);
            return Ok(steps);
        }
        if state.entries.len() >= AUTO_CACHE_CAPACITY {
            state.entries.pop_front();
        }
        state.entries.push_back(CacheEntry {
            key,
            expires_at: Instant::now() + AUTO_CACHE_TTL,
            steps: steps.clone(),
        });
        Ok(steps)
    }
}

/// The fingerprint is computed when AppState is created. Replacing that state
/// resets the cache; passing a different generation clears old entries immediately.
pub fn config_generation(config: &Config) -> u64 {
    let mut hasher = DefaultHasher::new();
    format!("{config:?}").hash(&mut hasher);
    hasher.finish()
}
#[derive(Clone, Debug)]
pub struct ResolvedStep {
    pub provider_index: usize,
    pub model: String,
    pub params: BTreeMap<String, Value>,
}

/// Count serialized JSON bytes without retaining a copy of request content.
fn encoded_json_bytes(value: Option<&Value>) -> usize {
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
    let Some(value) = value else {
        return 0;
    };
    let mut counter = Counter(0);
    if serde_json::to_writer(&mut counter, value).is_err() {
        return usize::MAX;
    }
    counter.0
}

/// Keep the established min/max input-byte contract for messages.
pub fn auto_input_bytes(messages: Option<&Value>) -> usize {
    encoded_json_bytes(messages)
}

/// Estimate the full serialized request budget, output allowance and 20% safety reserve.
/// This deliberately overestimates tokens (one byte per token) rather than
/// depending on a remote tokenizer or underestimating multilingual input.
pub fn auto_context_estimate(request: &Value) -> usize {
    let bytes = encoded_json_bytes(Some(request));
    let safety_margin = bytes
        .saturating_mul(CONTEXT_SAFETY_MARGIN_PERCENT)
        .div_ceil(100);
    let output_budget = request
        .get("max_completion_tokens")
        .or_else(|| request.get("max_tokens"))
        .and_then(Value::as_u64)
        .map(|tokens| tokens.min(usize::MAX as u64) as usize)
        .unwrap_or(1024);
    bytes
        .saturating_add(safety_margin)
        .saturating_add(output_budget)
}
/// A small, transparent Auto V1: fixed configured priorities and a bounded
/// length heuristic, not a black-box performance claim or remote-model ranking.
/// The returned candidates use the SAME dispatch/failover loop as named chains.
pub fn resolve_auto(config: &Config, input_bytes: usize) -> Result<Vec<ResolvedStep>, String> {
    let context_estimate = input_bytes.saturating_add(
        input_bytes
            .saturating_mul(CONTEXT_SAFETY_MARGIN_PERCENT)
            .div_ceil(100),
    );
    resolve_auto_with_estimate(config, input_bytes, context_estimate, &[])
}

/// Scores are fixed-size in-memory hints supplied by the gateway; never SQL queries.
pub fn resolve_auto_with_health(
    config: &Config,
    input_bytes: usize,
    penalties: &[u8],
) -> Result<Vec<ResolvedStep>, String> {
    let context_estimate = input_bytes.saturating_add(
        input_bytes
            .saturating_mul(CONTEXT_SAFETY_MARGIN_PERCENT)
            .div_ceil(100),
    );
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

/// Hard-exclude candidates only for capabilities explicitly declared unsupported.
/// Unknown capability flags remain eligible for backward compatibility.
pub fn resolve_auto_with_requirements(
    config: &Config,
    input_bytes: usize,
    context_estimate: usize,
    requirements: AutoRequirements,
    penalties: &[u8],
) -> Result<Vec<ResolvedStep>, String> {
    if !config.auto.enabled {
        return Err("auto routing is disabled".into());
    }
    let mut ranked = config
        .auto
        .candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| {
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
                    .or_else(|| {
                        registry.then_some(!model_capabilities::known_text_only(&candidate.model))
                    })
            });
            model_supported
                && (!requirements.tools || candidate.supports_tools != Some(false))
                && (!requirements.requires_images() || image_support != Some(false))
                && (!requirements.json_schema || candidate.supports_json_schema != Some(false))
                && context_limit.is_none_or(|max| context_estimate <= max)
                && input_bytes >= candidate.min_input_bytes
                && candidate
                    .max_input_bytes
                    .is_none_or(|max| input_bytes <= max)
        })
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
                params: BTreeMap::new(),
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
                json_schema: true
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
            assert_eq!(state.generation, Some(11));
        }
        cache
            .resolve(&cfg, 1, 1, AutoRequirements::default(), &[], 12)
            .unwrap();
        {
            let mut state = cache.state.lock().unwrap();
            assert_eq!(state.generation, Some(12));
            assert_eq!(state.entries.len(), 1);
            state.entries[0].expires_at = Instant::now() - Duration::from_secs(1);
        }
        cache
            .resolve(&cfg, 1, 1, AutoRequirements::default(), &[], 12)
            .unwrap();
        let state = cache.state.lock().unwrap();
        assert_eq!(state.entries.len(), 1);
        assert!(state.entries[0].expires_at > Instant::now());
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
