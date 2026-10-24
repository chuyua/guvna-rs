//! Conservative, explicitly opt-in model capability hints derived from Go.
//! This table is best-effort and never a substitute for upstream validation.

pub const UNKNOWN_CONTEXT_TOKENS: usize = 8_192;

pub fn context_tokens(model: &str) -> usize {
    let full = model.split(':').next().unwrap_or(model);
    let tail = full.rsplit('/').next().unwrap_or(full);
    match full {
        "nvidia/nemotron-3-ultra-550b-a55b" => 524_288,
        "nvidia/nemotron-3-super-120b-a12b" => 262_144,
        "nvidia/nemotron-3.5-lightning-30b-a3b" => 262_144,
        "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning" => 128_000,
        "nvidia/mistralai/mistral-nemotron" => 262_144,
        "nvidia/moonshotai/kimi-k3" => 262_144,
        "nvidia/z-ai/glm-5.3" | "nvidia/z-ai/glm-5.3-flash" => 204_800,
        "nvidia/deepseek-ai/deepseek-v4.1-flash" => 163_840,
        _ => match tail {
            "Qwen3.8-27B" | "Qwen3.8-Flash-Next" | "MinerU2.5-Pro" => 131_072,
            "DeepSeek-V4-Flash" | "DeepSeek-V4.1-Flash" => 163_840,
            "GLM-5.3" | "GLM-5.3-Flash" | "glm-5.3" | "glm-5.3-flash" => 204_800,
            "MiMo-V2.6-Flash" => 65_536,
            "MiniCPM5-2B" | "ling-3.0-flash" => 32_768,
            "nemotron-3-ultra-550b-a55b" => 524_288,
            "nemotron-3-super-120b-a12b"
            | "nemotron-3.5-lightning-30b-a3b"
            | "mistral-nemotron"
            | "kimi-k3" => 262_144,
            "nemotron-3-nano-omni-30b-a3b-reasoning" => 128_000,
            "deepseek-v4-flash" | "deepseek-v4.1-flash" => 163_840,
            _ => UNKNOWN_CONTEXT_TOKENS,
        },
    }
}

pub fn known_text_only(model: &str) -> bool {
    let base = model.split(':').next().unwrap_or(model);
    let tail = base.rsplit('/').next().unwrap_or(base);
    matches!(
        tail,
        "DeepSeek-V4-Flash"
            | "DeepSeek-V4.1-Flash"
            | "deepseek-v4-flash"
            | "deepseek-v4.1-flash"
            | "deepseek-v4-pro"
            | "mistral-nemotron"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn known_registry_values_and_namespaces_match_go_baseline() {
        assert_eq!(
            context_tokens("nvidia/deepseek-ai/deepseek-v4.1-flash"),
            163_840
        );
        assert_eq!(context_tokens("nvidia/z-ai/glm-5.3"), 204_800);
        assert_eq!(
            context_tokens("foo/unknown-future-model"),
            UNKNOWN_CONTEXT_TOKENS
        );
        assert_eq!(context_tokens("amd/Qwen3.8-27B"), 131_072);
        assert!(known_text_only("xkiro/deepseek/deepseek-v4.1-flash:free"));
        assert!(!known_text_only("unknown-new-model"));
    }
}
