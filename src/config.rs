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
    pub fn auth(&self) -> Result<Duration, String> {
        parse_duration(self.auth.as_deref().unwrap_or("24h"))
    }
    pub fn rate_limit(&self) -> Result<Duration, String> {
        parse_duration(self.rate_limit.as_deref().unwrap_or("60s"))
    }
    pub fn transient(&self) -> Result<Duration, String> {
        parse_duration(self.transient.as_deref().unwrap_or("60s"))
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
        let mut names = HashSet::new();
        for chain in &self.chains {
            if chain.name.is_empty() || !names.insert(chain.name.as_str()) {
                return Err(format!("empty/duplicate chain {:?}", chain.name));
            }
            self.validate_chain(chain)?;
        }
        Ok(())
    }

    pub(crate) fn validate_chain(&self, chain: &Chain) -> Result<(), String> {
        if chain.steps.is_empty() {
            return Err(format!("chain {} has no steps", chain.name));
        }
        for step in &chain.steps {
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
        let path = data_dir.join("chains.yaml");
        let data = match fs::read_to_string(&path) {
            Ok(v) => v,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(self),
            Err(err) => return Err(format!("reading {}: {err}", path.display())),
        };
        // Gate G11: malformed files are preserved, never overwritten or ignored.
        let file: RuntimeChains = serde_yaml::from_str(&data)
            .map_err(|e| format!("invalid {} (not modified): {e}", path.display()))?;
        if file.version.unwrap_or(1) != 1 {
            return Err(format!(
                "unsupported chains.yaml version in {}",
                path.display()
            ));
        }
        for chain in file.chains {
            self.validate_chain(&chain)?;
            if !self
                .chains
                .iter()
                .any(|existing| existing.name == chain.name)
            {
                self.chains.push(chain);
            }
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
