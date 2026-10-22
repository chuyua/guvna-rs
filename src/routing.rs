//! Named chain and provider-prefixed passthrough resolution.
use crate::config::{Config, Step};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct ResolvedStep {
    pub provider_index: usize,
    pub model: String,
    pub params: BTreeMap<String, Value>,
}

pub fn resolve(config: &Config, logical_model: &str) -> Result<Vec<ResolvedStep>, String> {
    if logical_model.is_empty() {
        return Err("missing model".into());
    }
    if let Some(chain) = config.chains.iter().find(|c| c.name == logical_model) {
        return chain
            .steps
            .iter()
            .map(|step: &Step| {
                let provider_index = config
                    .providers
                    .iter()
                    .position(|p| p.name == step.provider)
                    .ok_or_else(|| format!("missing provider for chain {logical_model}"))?;
                Ok(ResolvedStep {
                    provider_index,
                    model: step.model.clone(),
                    params: step.params.clone(),
                })
            })
            .collect();
    }
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
            // Preserve nested model names unless the provider explicitly catalogues the suffix.
            let wire_model = if provider.models.iter().any(|model| model == suffix) {
                suffix.to_owned()
            } else {
                logical_model.to_owned()
            };
            return Ok(vec![ResolvedStep {
                provider_index: i,
                model: wire_model,
                params: BTreeMap::new(),
            }]);
        }
    }
    Err(format!("unknown model or chain: {logical_model}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
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
            "openai/gpt-something"
        );
        assert!(resolve(&cfg, "missing").is_err());
    }
}
