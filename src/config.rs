use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::Path,
    time::Duration,
};

pub const DEFAULT_PORT: u16 = 20128;
fn default_port() -> u16 {
    DEFAULT_PORT
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub providers: Vec<Provider>,
    #[serde(default)]
    pub chains: Vec<Chain>,
    #[serde(default)]
    pub default_chain: String,
    #[serde(default)]
    pub auto: AutoConfig,
    #[serde(default)]
    pub timeout: TimeoutConfig,
}

/// The walk budget applies only to client non-streaming requests. It is
/// independent from the per-attempt timeout and cannot terminate an active
/// client SSE stream.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TimeoutConfig {
    pub walk: String,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            walk: "100s".into(),
        }
    }
}

impl TimeoutConfig {
    pub fn walk_duration(&self) -> Result<Duration, String> {
        let duration = parse_duration(&self.walk)?;
        if duration < Duration::from_millis(100) || duration > Duration::from_secs(3600) {
            return Err("timeout.walk must be between 100ms and 1h".into());
        }
        Ok(duration)
    }
}

/// Deterministic, opt-in Auto routing. Its bounded cache stores route metadata only;
/// health hints remain explicit and never depend on request-path database reads.
#[derive(Clone, Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct AutoConfig {
    pub enabled: bool,
    pub candidates: Vec<AutoCandidate>,
    /// Bound the number of upstream candidates attempted per Auto request.
    pub max_candidates: Option<usize>,
    /// Opt-in live failure-weighted priority (defaults to V1 deterministic order).
    pub health_aware: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoCandidate {
    pub provider: String,
    pub model: String,
    #[serde(default = "default_auto_priority")]
    pub priority: u16,
    #[serde(default)]
    pub min_input_bytes: usize,
    pub max_input_bytes: Option<usize>,
    pub max_context_tokens: Option<usize>,
    /// Explicit negative declarations make incompatible requests ineligible.
    /// None means unknown and preserves legacy routing behavior.
    pub supports_tools: Option<bool>,
    pub supports_images: Option<bool>,
    pub supports_json_schema: Option<bool>,
}

fn default_auto_priority() -> u16 {
    100
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provider {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub base_url: String,
    #[serde(default)]
    pub key_env: String,
    #[serde(default)]
    pub key_envs: Vec<String>,
    #[serde(default)]
    pub rotation: String,
    #[serde(default)]
    pub quarantine: QuarantineConfig,
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default)]
    pub prefixes: Vec<String>,
    #[serde(default)]
    pub stream_only: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuarantineConfig {
    pub auth: Option<String>,
    pub rate_limit: Option<String>,
    pub transient: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    pub enabled: bool,
    pub failure_threshold: u32,
    pub cooldown: String,
    pub max_cooldown: String,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            failure_threshold: 3,
            cooldown: "2s".into(),
            max_cooldown: "30s".into(),
        }
    }
}

impl CircuitBreakerConfig {
    pub fn timings(&self) -> Result<(Duration, Duration), String> {
        if !(1..=32).contains(&self.failure_threshold) {
            return Err("circuit_breaker failure_threshold must be 1..32".into());
        }
        let cooldown = parse_duration(&self.cooldown)?;
        let max = parse_duration(&self.max_cooldown)?;
        if cooldown < Duration::from_millis(100)
            || max < cooldown
            || max > Duration::from_secs(3600)
        {
            return Err(
                "circuit_breaker cooldown must be >=100ms and max_cooldown >= cooldown and <=1h"
                    .into(),
            );
        }
        Ok((cooldown, max))
    }
}
impl QuarantineConfig {
    fn checked_duration(value: &str) -> Result<Duration, String> {
        let duration = parse_duration(value)?;
        if !(Duration::from_secs(1)..=Duration::from_secs(30 * 24 * 3600)).contains(&duration) {
            return Err("quarantine duration must be between 1s and 30d".into());
        }
        Ok(duration)
    }
    pub fn auth(&self) -> Result<Duration, String> {
        Self::checked_duration(self.auth.as_deref().unwrap_or("24h"))
    }
    pub fn rate_limit(&self) -> Result<Duration, String> {
        Self::checked_duration(self.rate_limit.as_deref().unwrap_or("60s"))
    }
    pub fn transient(&self) -> Result<Duration, String> {
        Self::checked_duration(self.transient.as_deref().unwrap_or("60s"))
    }
}

// Parse Go-style duration fragments without accepting silently truncated input.
pub fn parse_duration(input: &str) -> Result<Duration, String> {
    if input.is_empty() {
        return Err("empty duration".into());
    }
    let mut rest = input;
    let mut total = Duration::ZERO;
    while !rest.is_empty() {
        let digit_count = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digit_count == 0 {
            return Err(format!("invalid duration {input:?}"));
        }
        let number: u64 = rest[..digit_count]
            .parse()
            .map_err(|_| "duration overflow")?;
        rest = &rest[digit_count..];
        let units = [
            ("ms", 1_000_000_u64),
            ("us", 1_000_u64),
            ("ns", 1_u64),
            ("h", 3_600_000_000_000_u64),
            ("m", 60_000_000_000_u64),
            ("s", 1_000_000_000_u64),
        ];
        let (suffix, multiplier) = units
            .into_iter()
            .find(|(u, _)| rest.starts_with(u))
            .ok_or_else(|| format!("invalid duration unit {input:?}"))?;
        rest = &rest[suffix.len()..];
        total = total
            .checked_add(Duration::from_nanos(
                number
                    .checked_mul(multiplier)
                    .ok_or_else(|| format!("duration overflow: {input}"))?,
            ))
            .ok_or_else(|| format!("duration overflow: {input}"))?;
    }
    Ok(total)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Chain {
    pub name: String,
    pub steps: Vec<Step>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub params: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeChains {
    #[serde(default)]
    pub(crate) version: Option<u16>,
    #[serde(default)]
    pub(crate) chains: Vec<Chain>,
}

impl Provider {
    pub fn key_names(&self) -> Vec<String> {
        if !self.key_envs.is_empty() {
            self.key_envs.clone()
        } else if !self.key_env.is_empty() {
            vec![self.key_env.clone()]
        } else {
            Vec::new()
        }
    }

    pub fn prefixes(&self) -> Vec<&str> {
        if self.prefixes.is_empty() {
            vec![self.name.as_str()]
        } else {
            self.prefixes.iter().map(String::as_str).collect()
        }
    }

    pub fn endpoint(&self, path: &str) -> String {
        let prefix = if self.kind == "gemini" {
            "/v1beta/openai"
        } else {
            "/v1"
        };
        format!("{}{}{}", self.base_url.trim_end_matches('/'), prefix, path)
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, String> {
        // Gate G01: explicit path only, with no HOME fallback.
        let src =
            fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
        let cfg: Self =
            serde_yaml::from_str(&src).map_err(|e| format!("parsing {}: {e}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.port == 0 {
            return Err("port must be between 1 and 65535".into());
        }
        self.timeout.walk_duration()?;
        let mut providers = HashSet::new();
        let mut prefixes = HashSet::new();
        for p in &self.providers {
            if p.name.is_empty() || !providers.insert(p.name.as_str()) {
                return Err(format!("empty/duplicate provider {:?}", p.name));
            }
            if p.kind != "openai" && p.kind != "gemini" {
                return Err(format!("unsupported provider type {:?}", p.kind));
            }
            let url = reqwest::Url::parse(&p.base_url)
                .map_err(|e| format!("provider {} invalid URL: {e}", p.name))?;
            if !["http", "https"].contains(&url.scheme())
                || url.host_str().is_none()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(format!(
                    "provider {} requires absolute http(s) URL without query or fragment",
                    p.name
                ));
            }
            if p.key_env.is_empty() == p.key_envs.is_empty() {
                return Err(format!(
                    "provider {}: supply exactly one of key_env or key_envs",
                    p.name
                ));
            }
            let mut keys = HashSet::new();
            for name in p.key_names() {
                if name.is_empty() || !keys.insert(name.clone()) {
                    return Err(format!(
                        "provider {}: empty/duplicate key environment name",
                        p.name
                    ));
                }
            }
            if !["", "round_robin", "least_used", "sequential"].contains(&p.rotation.as_str()) {
                return Err(format!("provider {}: unsupported rotation", p.name));
            }
            p.quarantine.auth()?;
            p.quarantine.rate_limit()?;
            p.quarantine.transient()?;
            p.circuit_breaker.timings()?;
            for prefix in p.prefixes() {
                if prefix.is_empty() || !prefixes.insert(prefix.to_owned()) {
                    return Err(format!("duplicate/empty provider prefix: {prefix:?}"));
                }
            }
        }
        if self
            .auto
            .max_candidates
            .is_some_and(|limit| !(1..=32).contains(&limit))
        {
            return Err("auto max_candidates must be 1..=32".into());
        }
        if self.auto.health_aware && !self.auto.enabled {
            return Err("auto health_aware requires enabled: true".into());
        }
        if self.auto.enabled {
            if self.auto.candidates.is_empty() || self.auto.candidates.len() > 32 {
                return Err("auto enabled requires 1..32 configured candidates".into());
            }
            if self.chains.iter().any(|chain| chain.name == "auto") {
                return Err("configured chain name auto is reserved when Auto is enabled".into());
            }
        } else if !self.auto.candidates.is_empty() {
            return Err("auto candidates require enabled: true".into());
        }
        let mut auto_keys = HashSet::new();
        for candidate in &self.auto.candidates {
            if candidate.model.is_empty()
                || !self.providers.iter().any(|p| p.name == candidate.provider)
            {
                return Err("auto candidate has missing provider or empty model".into());
            }
            if candidate
                .max_input_bytes
                .is_some_and(|v| v < candidate.min_input_bytes)
                || candidate.max_input_bytes == Some(0)
                || candidate.max_context_tokens == Some(0)
            {
                return Err("auto candidate invalid input length range".into());
            }
            if !auto_keys.insert((candidate.provider.as_str(), candidate.model.as_str())) {
                return Err("auto has duplicate provider/model candidates".into());
            }
        }
        let mut names = HashSet::new();
        for chain in &self.chains {
            if chain.name.is_empty() || !names.insert(chain.name.as_str()) {
                return Err(format!("empty/duplicate chain {:?}", chain.name));
            }
            self.validate_chain(chain)?;
        }
        if !self.default_chain.is_empty() && !names.contains(self.default_chain.as_str()) {
            return Err(format!(
                "default_chain must reference a configured named chain: {:?}",
                self.default_chain
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_chain(&self, chain: &Chain) -> Result<(), String> {
        if chain.steps.is_empty() {
            return Err(format!("chain {} has no steps", chain.name));
        }
        let mut seen_steps = HashSet::new();
        for step in &chain.steps {
            let key = (
                step.provider.clone(),
                step.model.clone(),
                serde_json::to_string(&step.params).map_err(|e| e.to_string())?,
            );
            if !seen_steps.insert(key) {
                return Err(format!(
                    "chain {} contains a duplicate candidate step",
                    chain.name
                ));
            }
            if !self.providers.iter().any(|p| p.name == step.provider) || step.model.is_empty() {
                return Err(format!(
                    "chain {} references missing provider or empty model",
                    chain.name
                ));
            }
            for key in step.params.keys() {
                if key.is_empty()
                    || ["model", "stream", "messages"].contains(&key.as_str())
                    || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    return Err(format!(
                        "chain {}: forbidden param key {:?}",
                        chain.name, key
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn with_runtime_chains(mut self, data_dir: &Path) -> Result<Self, String> {
        let runtime = crate::chain_store::load_runtime(&self, data_dir)?;
        self.chains.extend(runtime.chains);
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fail_closed_invalid_ports_quarantine_and_duplicate_chain_steps() {
        let invalid_port: Config = serde_yaml::from_str("port: 0").unwrap();
        assert!(invalid_port.validate().is_err());
        let bad_quarantine: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://test.invalid
    key_env: K
    quarantine:
      auth: 0s
"#,
        )
        .unwrap();
        assert!(bad_quarantine.validate().is_err());
        let duplicates: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://test.invalid
    key_env: K
chains:
  - name: fallback
    steps:
      - provider: p
        model: m
      - provider: p
        model: m
"#,
        )
        .unwrap();
        assert!(duplicates.validate().is_err());
    }

    #[test]
    fn walk_budget_defaults_and_bounds_are_fail_closed() {
        assert_eq!(
            TimeoutConfig::default().walk_duration().unwrap(),
            Duration::from_secs(100)
        );
        for (value, allowed) in [
            ("100ms", true),
            ("1s", true),
            ("100s", true),
            ("1h", true),
            ("0s", false),
            ("99ms", false),
            ("1h1s", false),
            ("garbage", false),
            ("18446744073709551615h", false),
        ] {
            let config: Config =
                serde_yaml::from_str(&format!("providers: []\ntimeout:\n  walk: {value}\n"))
                    .unwrap();
            assert_eq!(config.validate().is_ok(), allowed, "{value}");
        }
        let invalid: Result<Config, _> =
            serde_yaml::from_str("providers: []\ntimeout:\n  walk: 100s\n  extra: ignored\n");
        assert!(
            invalid.is_err(),
            "unknown timeout keys cannot be silently accepted"
        );
    }

    #[test]
    fn parse_go_durations() {
        assert_eq!(parse_duration("1h2m3s").unwrap(), Duration::from_secs(3723));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        for invalid in ["", "1", "-1s", "8x", "30sbad"] {
            assert!(parse_duration(invalid).is_err());
        }
    }

    #[test]
    fn explicit_config_wins_and_large_number_preserved() {
        let path =
            std::env::temp_dir().join(format!("guvna-rs-config-{}.yaml", std::process::id()));
        std::fs::write(&path, "port: 23232\nproviders: []\n").unwrap();
        let cfg = Config::load(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(cfg.port, 23232);
        let mut value: Value =
            serde_json::from_str(r#"{"model":"logical","seed":12345678901234567890}"#).unwrap();
        value["model"] = Value::String("physical".into());
        assert_eq!(value["seed"].as_u64(), Some(12_345_678_901_234_567_890));
        assert!(value.to_string().contains("12345678901234567890"));
    }

    #[test]
    fn invalid_provider_and_reserved_params_rejected() {
        let config: Config = serde_yaml::from_str(
            r#"
port: 20128
providers:
  - name: demo
    type: openai
    base_url: https://example.test
    key_env: DEMO_KEY
chains:
  - name: x
    steps:
      - provider: demo
        model: m
        params:
          messages: 1
"#,
        )
        .unwrap();
        assert!(config.validate().unwrap_err().contains("forbidden"));
    }

    #[test]
    fn default_chain_must_exist_at_startup() {
        let cfg: Config = serde_yaml::from_str("providers: []\ndefault_chain: missing\n").unwrap();
        assert!(cfg.validate().unwrap_err().contains("default_chain"));
        let cfg: Config = serde_yaml::from_str("providers: []\n").unwrap();
        assert!(cfg.validate().is_ok(), "legacy config remains valid");
    }
}
