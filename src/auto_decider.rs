//! Optional ranking hint: no prompt, API key, user text, or request body is sent.
use crate::{
    config::Config,
    routing::{AutoRequirements, ResolvedStep},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

const MAX_CACHE: usize = 32;
const MAX_REQUEST: usize = 8192;
const MAX_RESPONSE: usize = 4096;
const STALE_SECS: u64 = 120;

struct Cache {
    key: u64,
    order: Vec<usize>,
    fresh: Instant,
    stale: Instant,
}

/// One opt-in ranking service. No second routing/fallback state machine.
pub struct AutoDecider {
    client: reqwest::Client,
    endpoint: String,
    model: String,
    key: String,
    timeout: Duration,
    ttl: Duration,
    cache: Mutex<VecDeque<Cache>>,
    // Never hold the cache mutex across an upstream request. One permit
    // still bounds speculative and foreground selector traffic globally.
    refresh: Semaphore,
}

impl AutoDecider {
    pub fn from_config(
        cfg: &Config,
        credentials: &HashMap<String, String>,
        client: reqwest::Client,
    ) -> Result<Option<Self>, String> {
        let settings = &cfg.auto.decider;
        if !settings.enabled {
            return Ok(None);
        }
        let provider = cfg
            .providers
            .iter()
            .find(|p| p.name == settings.provider)
            .ok_or("Auto Decider provider is not configured")?;
        // The selector is optional. A temporarily missing selector key must
        // not prevent the otherwise healthy gateway from starting; ordinary
        // Auto routing continues with the deterministic candidate order.
        let Some(key) = provider.key_names().into_iter().find_map(|name| {
            credentials
                .get(&name)
                .filter(|secret| !secret.is_empty())
                .cloned()
        }) else {
            return Ok(None);
        };
        Ok(Some(Self {
            endpoint: provider.endpoint("/chat/completions"),
            model: settings.model.clone(),
            key,
            timeout: Duration::from_millis(settings.timeout_ms),
            ttl: Duration::from_secs(settings.cache_ttl_secs),
            client,
            cache: Mutex::new(VecDeque::new()),
            refresh: Semaphore::new(1),
        }))
    }

    /// SSE must never wait for a remote selector. Return a verified fresh or
    /// bounded stale ranking immediately, and signal whether background
    /// refresh is needed. A busy cache lock fails open without awaiting.
    pub fn cached_rank(
        &self,
        steps: &[ResolvedStep],
        input_bytes: usize,
        estimated_tokens: usize,
        requirements: AutoRequirements,
    ) -> (Option<Vec<ResolvedStep>>, bool) {
        if !(2..=32).contains(&steps.len()) {
            return (None, false);
        }
        let Ok(mut cache) = self.cache.try_lock() else {
            return (None, false);
        };
        let now = Instant::now();
        cache.retain(|entry| entry.stale > now);
        let key = ranking_key(steps, input_bytes, estimated_tokens, requirements);
        let can_refresh = self.refresh.available_permits() != 0;
        match cache.iter().find(|entry| entry.key == key) {
            Some(entry) => (
                apply_order(steps, &entry.order),
                entry.fresh <= now && can_refresh,
            ),
            None => (None, can_refresh),
        }
    }

    pub async fn rank(
        &self,
        cfg: &Config,
        steps: &[ResolvedStep],
        input_bytes: usize,
        estimated_tokens: usize,
        requirements: AutoRequirements,
    ) -> Option<Vec<ResolvedStep>> {
        if !(2..=32).contains(&steps.len()) {
            return None;
        }
        let key = ranking_key(steps, input_bytes, estimated_tokens, requirements);
        // A concurrent cold lookup fails open: it does not queue dozens of
        // requests behind a slow or inaccessible external model.
        let Ok(mut entries) = self.cache.try_lock() else {
            return None;
        };
        let now = Instant::now();
        entries.retain(|entry| entry.stale > now);
        let previous = entries.iter().find(|entry| entry.key == key);
        if let Some(entry) = previous
            && entry.fresh > now
        {
            return apply_order(steps, &entry.order);
        }
        let stale = previous.map(|entry| (entry.order.clone(), entry.stale));
        drop(entries);
        // Reject concurrent refreshes without queuing them. The cache remains
        // readable while an upstream model is slow or unresponsive.
        let Ok(_refresh) = self.refresh.try_acquire() else {
            return stale
                .filter(|(_, expiry)| *expiry > Instant::now())
                .and_then(|(order, _)| apply_order(steps, &order));
        };
        // Another request may have populated this key after our first read.
        let mut entries = self.cache.lock().await;
        entries.retain(|entry| entry.stale > Instant::now());
        if let Some(entry) = entries.iter().find(|entry| entry.key == key)
            && entry.fresh > Instant::now()
        {
            return apply_order(steps, &entry.order);
        }
        let stale = entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| (entry.order.clone(), entry.stale))
            .or(stale);
        drop(entries);
        if let Some(order) = self
            .fetch(cfg, steps, input_bytes, estimated_tokens, requirements)
            .await
        {
            let mut entries = self.cache.lock().await;
            entries.retain(|entry| entry.stale > Instant::now());
            entries.retain(|entry| entry.key != key);
            if entries.len() >= MAX_CACHE {
                entries.pop_front();
            }
            entries.push_back(Cache {
                key,
                order: order.clone(),
                fresh: Instant::now() + self.ttl,
                stale: Instant::now() + self.ttl + Duration::from_secs(STALE_SECS),
            });
            return apply_order(steps, &order);
        }
        stale
            .filter(|(_, expiry)| *expiry > Instant::now())
            .and_then(|(order, _)| apply_order(steps, &order))
    }

    async fn fetch(
        &self,
        cfg: &Config,
        steps: &[ResolvedStep],
        input_bytes: usize,
        estimated_tokens: usize,
        requirements: AutoRequirements,
    ) -> Option<Vec<usize>> {
        let candidates: Vec<_> = steps
            .iter()
            .enumerate()
            .map(|(index, step)| {
                let provider = cfg.providers.get(step.provider_index)?;
                Some(json!({"index":index,"provider":provider.name,"model":step.model}))
            })
            .collect::<Option<_>>()?;
        let summary = serde_json::to_string(&json!({
            "candidates":candidates,
            "input_bytes":input_bytes,
            "estimated_tokens":estimated_tokens,
            "features":format!("{requirements:?}")
        }))
        .ok()?;
        if summary.len() > MAX_REQUEST {
            return None;
        }
        let body = json!({
            "model": self.model, "stream":false, "temperature":0, "max_tokens":128,
            "messages": [
                {"role":"system","content":"Return only a JSON object containing an order array, a full permutation of the numeric candidate indexes. No explanation."},
                {"role":"user","content":summary}
            ]
        });
        // The summary is not the wire payload: system instructions and JSON
        // escaping add bytes. Enforce the cap on the final encoded request.
        let encoded = bounded_selector_body(&body)?;
        let req = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.key)
            .timeout(self.timeout)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(encoded);
        tokio::time::timeout(self.timeout, async {
            let mut response = req.send().await.ok()?;
            if !response.status().is_success()
                || response
                    .content_length()
                    .is_some_and(|v| v > MAX_RESPONSE as u64)
            {
                return None;
            }
            let mut data = Vec::new();
            while let Some(part) = response.chunk().await.ok()? {
                if data.len().saturating_add(part.len()) > MAX_RESPONSE {
                    return None;
                }
                data.extend_from_slice(&part);
            }
            let value: Value = serde_json::from_slice(&data).ok()?;
            let message = value.pointer("/choices/0/message/content")?.as_str()?;
            validate_order(&serde_json::from_str::<Value>(message).ok()?, steps.len())
        })
        .await
        .ok()
        .flatten()
    }
}

fn bounded_selector_body(body: &Value) -> Option<Vec<u8>> {
    let encoded = serde_json::to_vec(body).ok()?;
    (encoded.len() <= MAX_REQUEST).then_some(encoded)
}

fn validate_order(value: &Value, count: usize) -> Option<Vec<usize>> {
    let values = value.get("order")?.as_array()?;
    if values.len() != count || count > 32 {
        return None;
    }
    let mut used = HashSet::new();
    let mut result = Vec::with_capacity(count);
    for value in values {
        let index = usize::try_from(value.as_u64()?).ok()?;
        if index >= count || !used.insert(index) {
            return None;
        }
        result.push(index);
    }
    Some(result)
}

fn apply_order(steps: &[ResolvedStep], order: &[usize]) -> Option<Vec<ResolvedStep>> {
    validate_order(&json!({"order":order}), steps.len())?;
    Some(order.iter().map(|index| steps[*index].clone()).collect())
}

fn ranking_key(
    steps: &[ResolvedStep],
    bytes: usize,
    tokens: usize,
    features: AutoRequirements,
) -> u64 {
    struct HashWriter<'a>(&'a mut DefaultHasher);
    impl std::io::Write for HashWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.write(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut h = DefaultHasher::new();
    steps.len().hash(&mut h);
    for step in steps {
        step.provider_index.hash(&mut h);
        step.model.hash(&mut h);
        // Candidate defaults can include large tool JSON schemas. Hash the
        // canonical serialized fields directly instead of allocating a
        // potentially megabyte-sized Debug String on every request.
        serde_json::to_writer(HashWriter(&mut h), &step.params)
            .expect("validated JSON candidate params can be serialized");
    }
    // The selector receives exact estimates. Do not reuse a cached rank for
    // different context pressure just because both sizes share a KiB bucket.
    // The cache remains bounded and remote refreshes are semaphore-limited.
    bytes.hash(&mut h);
    tokens.hash(&mut h);
    features.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selector_request_cap_applies_to_complete_wire_json() {
        assert!(bounded_selector_body(&json!({"model":"ranker"})).is_some());
        // The inner summary fits but the complete wire request must not.
        let summary = "x".repeat(MAX_REQUEST - 40);
        assert!(summary.len() < MAX_REQUEST);
        assert!(
            bounded_selector_body(&json!({
                "model":"ranker", "messages":[
                    {"role":"system","content":"selector instructions"},
                    {"role":"user","content":summary}
                ]
            }))
            .is_none()
        );
        assert!(bounded_selector_body(&json!({"content":"\\".repeat(MAX_REQUEST / 2)})).is_none());
    }

    #[tokio::test]
    async fn slow_refresh_does_not_hide_stale_rank_or_queue_network_fetches() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::sync::Notify;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let server = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post({
                let calls = Arc::clone(&calls);
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                move || {
                    let calls = Arc::clone(&calls);
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    async move {
                        if calls.fetch_add(1, Ordering::SeqCst) != 0 {
                            entered.notify_one();
                            release.notified().await;
                        }
                        axum::Json(json!({"choices":[{"message":{"content":"{\"order\":[1,0]}"}}]}))
                    }
                }
            }),
        );
        let server_task = tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });
        let config: Config = serde_yaml::from_str(&format!(
            r#"
providers:
  - name: p
    type: openai
    base_url: http://{address}
    key_env: K
auto:
  enabled: true
  candidates:
    - provider: p
      model: one
    - provider: p
      model: two
  decider:
    enabled: true
    provider: p
    model: ranker
    timeout_ms: 2000
    cache_ttl_secs: 30
"#
        ))
        .unwrap();
        config.validate().unwrap();
        let config = Arc::new(config);
        let decider = Arc::new(
            AutoDecider::from_config(
                &config,
                &HashMap::from([("K".to_owned(), "fake-key".to_owned())]),
                reqwest::Client::new(),
            )
            .unwrap()
            .unwrap(),
        );
        let steps = vec![
            ResolvedStep {
                provider_index: 0,
                model: "one".into(),
                params: Default::default(),
            },
            ResolvedStep {
                provider_index: 0,
                model: "two".into(),
                params: Default::default(),
            },
        ];
        let features = AutoRequirements::default();
        assert_eq!(
            decider
                .rank(&config, &steps, 12, 23, features)
                .await
                .unwrap()[0]
                .model,
            "two"
        );
        {
            let mut cache = decider.cache.lock().await;
            cache.front_mut().unwrap().fresh = Instant::now() - Duration::from_millis(1);
        }
        let active_decider = Arc::clone(&decider);
        let active_config = Arc::clone(&config);
        let active_steps = steps.clone();
        let task = tokio::spawn(async move {
            active_decider
                .rank(&active_config, &active_steps, 12, 23, features)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("second fetch should reach mock upstream");
        let (cached, refresh) = decider.cached_rank(&steps, 12, 23, features);
        assert_eq!(cached.unwrap()[0].model, "two");
        assert!(!refresh, "in-flight request must not spawn duplicate fetch");
        let (unknown, refresh) = decider.cached_rank(
            &steps,
            12,
            23,
            AutoRequirements::from_signals(false, true, false),
        );
        assert!(unknown.is_none());
        assert!(!refresh, "another cache key must not queue a second fetch");
        let duplicate = tokio::time::timeout(
            Duration::from_millis(100),
            decider.rank(&config, &steps, 12, 23, features),
        )
        .await
        .expect("concurrent request should fail open using stale cache");
        assert_eq!(duplicate.unwrap()[0].model, "two");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        release.notify_one();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap()[0]
                .model,
            "two",
        );
        let (fresh, refresh) = decider.cached_rank(&steps, 12, 23, features);
        assert_eq!(fresh.unwrap()[0].model, "two");
        assert!(!refresh);
        server_task.abort();
    }

    #[tokio::test]
    async fn direct_decider_network_rank_is_cached_without_sending_prompts() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    let serialized = serde_json::to_string(&body).unwrap();
                    assert!(!serialized.contains("secret user question"));
                    assert!(!serialized.contains("test-secret"));
                    assert_eq!(body["model"], "ranker");
                    axum::Json(json!({"choices":[{"message":{"content":"{\"order\":[1,0]}"}}]}))
                }
            }),
        );
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config: Config = serde_yaml::from_str(&format!(
            r#"
providers:
  - name: p
    type: openai
    base_url: http://{address}
    key_env: K
auto:
  enabled: true
  candidates:
    - provider: p
      model: one
    - provider: p
      model: two
  decider:
    enabled: true
    provider: p
    model: ranker
    timeout_ms: 500
    cache_ttl_secs: 30
"#
        ))
        .unwrap();
        config.validate().unwrap();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let decider = AutoDecider::from_config(
            &config,
            &HashMap::from([("K".to_owned(), "test-secret".to_owned())]),
            client,
        )
        .unwrap()
        .unwrap();
        let steps = vec![
            ResolvedStep {
                provider_index: 0,
                model: "one".into(),
                params: Default::default(),
            },
            ResolvedStep {
                provider_index: 0,
                model: "two".into(),
                params: Default::default(),
            },
        ];
        let (cold, needs_refresh) =
            decider.cached_rank(&steps, 123, 234, AutoRequirements::default());
        assert!(cold.is_none());
        assert!(needs_refresh);
        let reordered = decider
            .rank(&config, &steps, 123, 234, AutoRequirements::default())
            .await
            .unwrap();
        assert_eq!(reordered[0].model, "two");
        let cached = decider
            .rank(&config, &steps, 123, 234, AutoRequirements::default())
            .await
            .unwrap();
        assert_eq!(cached[0].model, "two");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Previously both estimates rounded to the same 1-KiB bucket,
        // reusing a selector decision that was made for another request.
        assert!(
            decider
                .cached_rank(&steps, 124, 234, AutoRequirements::default())
                .0
                .is_none()
        );
        assert!(
            decider
                .cached_rank(&steps, 123, 235, AutoRequirements::default())
                .0
                .is_none()
        );
        assert!(
            decider
                .rank(&config, &steps, 124, 234, AutoRequirements::default())
                .await
                .is_some()
        );
        assert!(
            decider
                .rank(&config, &steps, 123, 235, AutoRequirements::default())
                .await
                .is_some()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let (warm, needs_refresh) =
            decider.cached_rank(&steps, 123, 234, AutoRequirements::default());
        assert_eq!(warm.unwrap()[0].model, "two");
        assert!(!needs_refresh);
        let (different_capability, refresh) = decider.cached_rank(
            &steps,
            123,
            234,
            AutoRequirements::from_signals(false, true, false),
        );
        assert!(different_capability.is_none());
        assert!(refresh);
        {
            let mut cache = decider.cache.lock().await;
            let entry = cache.front_mut().unwrap();
            entry.fresh = Instant::now() - Duration::from_millis(1);
            let (blocked, refresh) =
                decider.cached_rank(&steps, 123, 234, AutoRequirements::default());
            assert!(blocked.is_none());
            assert!(!refresh, "in-flight refresh must not create a convoy");
        }
        let (stale, refresh) = decider.cached_rank(&steps, 123, 234, AutoRequirements::default());
        assert_eq!(stale.unwrap()[0].model, "two");
        assert!(refresh, "stale ranking must schedule a background refresh");
        {
            let mut cache = decider.cache.lock().await;
            cache.front_mut().unwrap().stale = Instant::now() - Duration::from_millis(1);
        }
        let (expired, refresh) = decider.cached_rank(&steps, 123, 234, AutoRequirements::default());
        assert!(expired.is_none());
        assert!(refresh);
        task.abort();
    }

    #[test]
    fn rejects_partial_duplicate_out_of_range_and_non_integer_rankings() {
        assert_eq!(validate_order(&json!({"order":[1,0]}), 2), Some(vec![1, 0]));
        for bad in [
            json!({"order":[0,0]}),
            json!({"order":[0]}),
            json!({"order":[2,0]}),
            json!({"order":["0",1]}),
            json!({"order":[0,1,2]}),
            json!({"order":[]}),
            json!({"order":[-1,0]}),
            json!({"order":[0.5,1]}),
            json!({"order":[null,1]}),
            json!({"order":[true,1]}),
            json!({"order":[18446744073709551615_u64,0]}),
        ] {
            assert!(validate_order(&bad, 2).is_none(), "{bad}");
        }
        // Reject oversized permutations even if all indices are otherwise valid.
        let oversized = json!({"order": (0..33).collect::<Vec<_>>()});
        assert!(validate_order(&oversized, 33).is_none());
    }
    #[test]
    fn cache_key_fences_candidate_and_capability_changes() {
        let a = ResolvedStep {
            provider_index: 0,
            model: "a".into(),
            params: Default::default(),
        };
        let b = ResolvedStep {
            provider_index: 0,
            model: "b".into(),
            params: Default::default(),
        };
        let plain = AutoRequirements::default();
        let image = AutoRequirements::from_signals(false, true, false);
        let original = ranking_key(&[a.clone(), b.clone()], 2000, 3000, plain);
        assert_ne!(
            original,
            ranking_key(&[b.clone(), a.clone()], 2000, 3000, plain)
        );
        assert_ne!(
            original,
            ranking_key(&[a.clone(), b.clone()], 2000, 3000, image)
        );
        assert_ne!(
            original,
            ranking_key(&[a.clone(), b.clone()], 4000, 3000, plain)
        );
        assert_ne!(
            original,
            ranking_key(&[a.clone(), b.clone()], 2001, 3000, plain),
            "distinct byte estimates within the old bucket must not share a rank"
        );
        assert_ne!(
            original,
            ranking_key(&[a.clone(), b.clone()], 2000, 3001, plain),
            "distinct token estimates within the old bucket must not share a rank"
        );
        // A live chain edit can change the forwarded parameters without
        // changing the candidate order. Never reuse the old selector ranking.
        let mut changed = a.clone();
        changed.params.insert("temperature".into(), json!(0.75));
        assert_ne!(
            original,
            ranking_key(&[changed, b], 2000, 3000, plain),
            "changed chain parameters must invalidate the selector cache"
        );
        let mut large = a.clone();
        large.params.insert(
            "tools".into(),
            json!([{"type":"function","function":{"name":"search",
                "description":"x".repeat(900_000)}}]),
        );
        let large_key = ranking_key(&[large.clone()], 2000, 3000, plain);
        assert_eq!(
            large_key,
            ranking_key(&[large.clone()], 2000, 3000, plain),
            "streaming hash must be deterministic for large schemas"
        );
        large.params.insert("temperature".into(), json!(0.75));
        assert_ne!(
            large_key,
            ranking_key(&[large], 2000, 3000, plain),
            "even an additional small field must invalidate a large schema key"
        );
    }
}
