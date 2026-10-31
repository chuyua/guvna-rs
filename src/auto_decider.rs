//! Optional ranking hint: no prompt, API key, user text, or request body is sent.
use crate::{
    config::Config,
    routing::{AutoRequirements, ResolvedStep},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

const MAX_CACHE: usize = 32;
const MAX_REQUEST: usize = 8192;
const MAX_RESPONSE: usize = 4096;
const STALE_SECS: u64 = 120;
// A failed optional selector must not add a network timeout to every
// otherwise healthy Chat request. A brief, keyed negative entry is advisory;
// the normal deterministic candidate order remains available immediately.
const FAILURE_COOLDOWN: Duration = Duration::from_secs(5);

struct Cache {
    key: u64,
    order: Vec<usize>,
    fresh: Instant,
    stale: Instant,
}

/// The selector needs only physical candidate identity, never forwarded
/// tool schemas or request defaults. Static Chat requests can borrow their
/// resolved steps, while background tasks own just these tiny metadata views.
pub(crate) trait SelectorIdentity {
    fn provider_index(&self) -> usize;
    fn model(&self) -> &str;
}

impl SelectorIdentity for ResolvedStep {
    fn provider_index(&self) -> usize {
        self.provider_index
    }
    fn model(&self) -> &str {
        &self.model
    }
}

pub(crate) struct SelectorCandidate {
    provider_index: usize,
    model: String,
}

impl SelectorIdentity for SelectorCandidate {
    fn provider_index(&self) -> usize {
        self.provider_index
    }
    fn model(&self) -> &str {
        &self.model
    }
}

pub(crate) fn selector_candidates(steps: &[ResolvedStep]) -> Vec<SelectorCandidate> {
    steps
        .iter()
        .map(|step| SelectorCandidate {
            provider_index: step.provider_index,
            model: step.model.clone(),
        })
        .collect()
}

/// A frequently used ranking should not be discarded before an older,
/// unreferenced cache key. The bounded cache is tiny (32 entries), so a
/// VecDeque splice needs no map or second cache state machine.
fn touch_cache(entries: &mut VecDeque<Cache>, key: u64) -> Option<&Cache> {
    let position = entries.iter().position(|entry| entry.key == key)?;
    let entry = entries.remove(position)?;
    entries.push_back(entry);
    entries.back()
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

    /// SSE must never wait for a remote selector. Return only the cached
    /// permutation (at most 32 indexes), not a deep copy of every candidate's
    /// potentially multi-megabyte forwarded tool/schema defaults. The caller
    /// moves the existing steps in place. Busy locks still fail open.
    pub fn cached_rank(
        &self,
        steps: &[ResolvedStep],
        input_bytes: usize,
        estimated_tokens: usize,
        requirements: AutoRequirements,
        generation: u64,
    ) -> (Option<Vec<usize>>, bool) {
        if !(2..=32).contains(&steps.len()) {
            return (None, false);
        }
        // The immutable route snapshot already hashes all candidate defaults
        // ONCE when published. Do not re-serialize huge JSON tool schemas on
        // every live SSE request merely to look up a short selector ranking.
        let key = ranking_key(
            steps,
            input_bytes,
            estimated_tokens,
            requirements,
            generation,
        );
        let Ok(mut cache) = self.cache.try_lock() else {
            return (None, false);
        };
        let now = Instant::now();
        cache.retain(|entry| entry.stale > now);
        let can_refresh = self.refresh.available_permits() != 0;
        let cached =
            touch_cache(&mut cache, key).map(|entry| (entry.order.clone(), entry.fresh <= now));
        drop(cache);
        match cached {
            Some((order, expired)) => (
                valid_permutation(&order, steps.len()).then_some(order),
                expired && can_refresh,
            ),
            None => (None, can_refresh),
        }
    }

    /// The request walk can cancel the optional selector before its own
    /// timeout expires. A synchronous, fail-open cooldown keeps that
    /// cancellation from adding the same wait to every later Chat request.
    pub(crate) fn cooldown_cancelled<T: SelectorIdentity>(
        &self,
        steps: &[T],
        input_bytes: usize,
        estimated_tokens: usize,
        requirements: AutoRequirements,
        generation: u64,
    ) {
        if (2..=32).contains(&steps.len()) {
            self.record_failed_refresh(ranking_key(
                steps,
                input_bytes,
                estimated_tokens,
                requirements,
                generation,
            ));
        }
    }

    fn record_failed_refresh(&self, key: u64) {
        // Do not await the cache mutex from the critical request deadline path.
        let Ok(mut entries) = self.cache.try_lock() else {
            return;
        };
        let now = Instant::now();
        entries.retain(|entry| entry.stale > now);
        if let Some(index) = entries.iter().position(|entry| entry.key == key) {
            let mut entry = entries.remove(index).expect("matched cache entry");
            if entry.order.is_empty() {
                entry.fresh = now + FAILURE_COOLDOWN;
                entry.stale = entry.fresh;
            } else if entry.fresh <= now {
                // A verified ranking can remain a stale advisory, but do not
                // extend its original hard stale deadline. More importantly,
                // a cancelled older fetch must not shorten the freshness of
                // a newer successful ranking published by another request.
                entry.fresh = (now + FAILURE_COOLDOWN).min(entry.stale);
            }
            entries.push_back(entry);
        } else {
            if entries.len() >= MAX_CACHE {
                entries.pop_front();
            }
            entries.push_back(Cache {
                key,
                order: Vec::new(),
                fresh: now + FAILURE_COOLDOWN,
                stale: now + FAILURE_COOLDOWN,
            });
        }
    }

    pub(crate) async fn rank<T: SelectorIdentity>(
        &self,
        cfg: &Config,
        steps: &[T],
        input_bytes: usize,
        estimated_tokens: usize,
        requirements: AutoRequirements,
        generation: u64,
    ) -> Option<Vec<usize>> {
        if !(2..=32).contains(&steps.len()) {
            return None;
        }
        let key = ranking_key(
            steps,
            input_bytes,
            estimated_tokens,
            requirements,
            generation,
        );
        // A concurrent cold lookup fails open: it does not queue dozens of
        // requests behind a slow or inaccessible external model.
        let Ok(mut entries) = self.cache.try_lock() else {
            return None;
        };
        let now = Instant::now();
        entries.retain(|entry| entry.stale > now);
        let previous = touch_cache(&mut entries, key)
            .map(|entry| (entry.order.clone(), entry.fresh, entry.stale));
        drop(entries);
        if let Some((order, fresh, _)) = &previous
            && *fresh > now
        {
            return valid_permutation(order, steps.len()).then(|| order.clone());
        }
        let stale = previous.map(|(order, _, stale)| (order, stale));
        // Reject concurrent refreshes without queuing them. The cache remains
        // readable while an upstream model is slow or unresponsive.
        let Ok(_refresh) = self.refresh.try_acquire() else {
            return stale
                .filter(|(_, expiry)| *expiry > Instant::now())
                .and_then(|(order, _)| valid_permutation(&order, steps.len()).then_some(order));
        };
        // Another request may have populated this key after our first read.
        let mut entries = self.cache.lock().await;
        entries.retain(|entry| entry.stale > Instant::now());
        let cached = touch_cache(&mut entries, key)
            .map(|entry| (entry.order.clone(), entry.fresh, entry.stale));
        drop(entries);
        if let Some((order, fresh, _)) = &cached
            && *fresh > Instant::now()
        {
            return valid_permutation(order, steps.len()).then(|| order.clone());
        }
        let stale = cached.map(|(order, _, stale)| (order, stale)).or(stale);
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
            return Some(order);
        }
        let fallback = stale
            .filter(|(_, expiry)| *expiry > Instant::now())
            .and_then(|(order, _)| valid_permutation(&order, steps.len()).then_some(order));
        self.record_failed_refresh(key);
        fallback
    }

    async fn fetch<T: SelectorIdentity>(
        &self,
        cfg: &Config,
        steps: &[T],
        input_bytes: usize,
        estimated_tokens: usize,
        requirements: AutoRequirements,
    ) -> Option<Vec<usize>> {
        let candidates: Vec<_> = steps
            .iter()
            .enumerate()
            .map(|(index, step)| {
                let provider = cfg.providers.get(step.provider_index())?;
                Some(json!({"index":index,"provider":provider.name,"model":step.model()}))
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
    let mut result = Vec::with_capacity(count);
    for value in values {
        let index = usize::try_from(value.as_u64()?).ok()?;
        result.push(index);
    }
    valid_permutation(&result, count).then_some(result)
}

/// The selector has at most 32 candidates. A bitset validates both remote
/// proposals and cached rankings without allocating a JSON object or hash
/// table on every streaming request.
fn valid_permutation(order: &[usize], count: usize) -> bool {
    if order.len() != count || count == 0 || count > 32 {
        return false;
    }
    let mut seen = 0u32;
    for &index in order {
        if index >= count {
            return false;
        }
        let bit = 1u32 << index;
        if seen & bit != 0 {
            return false;
        }
        seen |= bit;
    }
    true
}

/// SSE hot-path: reorder metadata and JSON default ownership, never cloning
/// large per-candidate tool schemas. Invalid cached order leaves steps intact.
pub(crate) fn apply_cached_order(steps: &mut Vec<ResolvedStep>, order: &[usize]) -> bool {
    if !valid_permutation(order, steps.len()) {
        return false;
    }
    let mut originals: Vec<_> = std::mem::take(steps).into_iter().map(Some).collect();
    steps.extend(
        order
            .iter()
            .map(|&index| originals[index].take().expect("validated permutation")),
    );
    true
}

fn ranking_key<T: SelectorIdentity>(
    steps: &[T],
    bytes: usize,
    tokens: usize,
    features: AutoRequirements,
    generation: u64,
) -> u64 {
    let mut h = DefaultHasher::new();
    // A generation is the full Config fingerprint computed by
    // LiveRouteSnapshot::new, including every candidate's forwarded params.
    // A changed tool schema or chain param necessarily produces a new key.
    generation.hash(&mut h);
    steps.len().hash(&mut h);
    for step in steps {
        step.provider_index().hash(&mut h);
        step.model().hash(&mut h);
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
    const SELECTOR_GENERATION: u64 = 7;

    #[test]
    fn selector_permutation_bitset_rejects_duplicates_and_out_of_bounds() {
        assert!(valid_permutation(&[1, 0], 2));
        assert!(valid_permutation(&(0..32).rev().collect::<Vec<_>>(), 32));
        assert!(!valid_permutation(&[], 0));
        assert!(!valid_permutation(&[0, 0], 2));
        assert!(!valid_permutation(&[0, 2], 2));
        assert!(!valid_permutation(&[0], 2));
        assert!(!valid_permutation(&(0..33).collect::<Vec<_>>(), 33));
        let step = |name: &str| ResolvedStep {
            provider_index: 0,
            model: name.into(),
            params: Default::default(),
        };
        let steps = [step("one"), step("two")];
        let mut moving = steps.to_vec();
        assert!(apply_cached_order(&mut moving, &[1, 0]));
        assert_eq!(moving[0].model, "two");
        assert_eq!(moving[1].model, "one");
        let previous = moving.iter().map(|s| s.model.clone()).collect::<Vec<_>>();
        assert!(!apply_cached_order(&mut moving, &[0, 0]));
        assert_eq!(
            moving.iter().map(|s| s.model.clone()).collect::<Vec<_>>(),
            previous,
            "invalid order must leave all owned candidate data untouched"
        );
        for bad in [&[0, 0][..], &[0, 9][..], &[1][..]] {
            let mut candidate = steps.to_vec();
            assert!(!apply_cached_order(&mut candidate, bad), "{bad:?}");
        }
        assert_eq!(validate_order(&json!({"order":[1,0]}), 2), Some(vec![1, 0]));
        assert!(validate_order(&json!({"order":[1,1]}), 2).is_none());
    }

    #[test]
    fn cached_permutation_moves_large_schema_without_reallocating_payload() {
        let mut large = ResolvedStep {
            provider_index: 0,
            model: "large".into(),
            params: Default::default(),
        };
        large.params.insert(
            "tools".into(),
            json!([{"type":"function","function":{"description":"X".repeat(950_000)}}]),
        );
        let small = ResolvedStep {
            provider_index: 1,
            model: "small".into(),
            params: Default::default(),
        };
        let mut steps = vec![large, small];
        let address = steps[0].params["tools"][0]["function"]["description"]
            .as_str()
            .unwrap()
            .as_ptr();
        assert!(apply_cached_order(&mut steps, &[1, 0]));
        assert_eq!(steps[0].model, "small");
        assert_eq!(steps[1].model, "large");
        assert_eq!(
            address,
            steps[1].params["tools"][0]["function"]["description"]
                .as_str()
                .unwrap()
                .as_ptr(),
            "the 950 KiB schema buffer must move, not be copied"
        );
        // Maximal supported candidate count remains a real permutation,
        // regardless of the relative insertion positions.
        let mut full: Vec<_> = (0..32)
            .map(|i| ResolvedStep {
                provider_index: i,
                model: format!("model-{i}"),
                params: Default::default(),
            })
            .collect();
        let reverse: Vec<_> = (0..32).rev().collect();
        assert!(apply_cached_order(&mut full, &reverse));
        assert_eq!(full[0].model, "model-31");
        assert_eq!(full[31].model, "model-0");
    }

    #[test]
    fn background_selector_uses_tiny_identity_snapshot_without_schema_clones() {
        let mut large = ResolvedStep {
            provider_index: 3,
            model: "model-with-default-tools".into(),
            params: Default::default(),
        };
        large.params.insert(
            "tools".into(),
            json!([{"function":{"description":"S".repeat(900_000)}}]),
        );
        let steps = vec![
            large,
            ResolvedStep {
                provider_index: 1,
                model: "fallback".into(),
                params: Default::default(),
            },
        ];
        let snapshot = selector_candidates(&steps);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].model(), "model-with-default-tools");
        assert_eq!(snapshot[0].provider_index(), 3);
        let features = AutoRequirements::default();
        assert_eq!(
            ranking_key(&steps, 42, 4096, features, SELECTOR_GENERATION),
            ranking_key(&snapshot, 42, 4096, features, SELECTOR_GENERATION),
            "compact background input must use the exact cache identity"
        );
        assert_eq!(
            steps[0].params["tools"][0]["function"]["description"]
                .as_str()
                .unwrap()
                .len(),
            900_000,
            "the original candidate owns its large schema unchanged"
        );
    }

    #[test]
    fn selector_cache_hit_updates_recency_and_prevents_hot_key_eviction() {
        let now = Instant::now();
        let mut entries = VecDeque::new();
        for key in 0..MAX_CACHE as u64 {
            entries.push_back(Cache {
                key,
                order: vec![1, 0],
                fresh: now + Duration::from_secs(5),
                stale: now + Duration::from_secs(15),
            });
        }
        assert_eq!(touch_cache(&mut entries, 0).unwrap().order, vec![1, 0]);
        assert_eq!(entries.back().unwrap().key, 0);
        // Under new cache pressure the oldest untouched key (1) goes, not
        // the most frequently requested baseline rank (0, SELECTOR_GENERATION).
        entries.pop_front();
        entries.push_back(Cache {
            key: MAX_CACHE as u64,
            order: vec![0, 1],
            fresh: now + Duration::from_secs(5),
            stale: now + Duration::from_secs(15),
        });
        assert!(entries.iter().any(|entry| entry.key == 0));
        assert!(!entries.iter().any(|entry| entry.key == 1));
        assert_eq!(entries.len(), MAX_CACHE);
    }

    #[tokio::test]
    async fn selector_cache_large_schema_hit_and_busy_mutex_fail_open() {
        let decider = AutoDecider {
            client: reqwest::Client::new(),
            endpoint: String::new(),
            model: "ranker".into(),
            key: String::new(),
            timeout: Duration::from_secs(1),
            ttl: Duration::from_secs(20),
            cache: Mutex::new(VecDeque::new()),
            refresh: Semaphore::new(1),
        };
        let mut a = ResolvedStep {
            provider_index: 0,
            model: "first".into(),
            params: Default::default(),
        };
        a.params
            .insert("schema".into(), json!({"description":"x".repeat(900_000)}));
        let b = ResolvedStep {
            provider_index: 0,
            model: "second".into(),
            params: Default::default(),
        };
        let steps = [a, b];
        let requirements = AutoRequirements::default();
        let key = ranking_key(&steps, 12, 2000, requirements, SELECTOR_GENERATION);
        let mut locked = decider.cache.lock().await;
        locked.push_back(Cache {
            key,
            order: vec![1, 0],
            fresh: Instant::now() + Duration::from_secs(20),
            stale: Instant::now() + Duration::from_secs(60),
        });
        let (unavailable, refresh) =
            decider.cached_rank(&steps, 12, 2000, requirements, SELECTOR_GENERATION);
        assert!(
            unavailable.is_none(),
            "busy mutex must not await on the SSE path"
        );
        assert!(!refresh, "busy mutex cannot schedule speculative ranking");
        drop(locked);
        let (hit, refresh) =
            decider.cached_rank(&steps, 12, 2000, requirements, SELECTOR_GENERATION);
        assert_eq!(hit.unwrap(), vec![1, 0]);
        assert!(!refresh);
        // A different schema key must not inherit the cached result.
        let (miss, refresh) =
            decider.cached_rank(&steps, 13, 2000, requirements, SELECTOR_GENERATION);
        assert!(miss.is_none());
        assert!(refresh);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_large_selector_cache_hits_keep_order_and_capacity() {
        use std::sync::Arc;
        let decider = Arc::new(AutoDecider {
            client: reqwest::Client::new(),
            endpoint: String::new(),
            model: "ranker".into(),
            key: String::new(),
            timeout: Duration::from_secs(1),
            ttl: Duration::from_secs(30),
            cache: Mutex::new(VecDeque::new()),
            refresh: Semaphore::new(1),
        });
        let mut a = ResolvedStep {
            provider_index: 0,
            model: "first".into(),
            params: Default::default(),
        };
        a.params.insert(
            "tools".into(),
            json!([{"function":{"description":"p".repeat(800_000)}}]),
        );
        let steps = Arc::new(vec![
            a,
            ResolvedStep {
                provider_index: 0,
                model: "second".into(),
                params: Default::default(),
            },
        ]);
        let requirements = AutoRequirements::default();
        let key = ranking_key(&steps, 20, 30, requirements, SELECTOR_GENERATION);
        decider.cache.lock().await.push_back(Cache {
            key,
            order: vec![1, 0],
            fresh: Instant::now() + Duration::from_secs(20),
            stale: Instant::now() + Duration::from_secs(60),
        });
        let mut tasks = Vec::new();
        for _ in 0..12 {
            let decider = Arc::clone(&decider);
            let steps = Arc::clone(&steps);
            tasks.push(tokio::task::spawn_blocking(move || {
                let (ranked, refresh) =
                    decider.cached_rank(&steps, 20, 30, requirements, SELECTOR_GENERATION);
                if let Some(ranked) = ranked {
                    assert_eq!(ranked, vec![1, 0]);
                    assert_eq!(steps[0].model, "first");
                    assert!(!refresh);
                } else {
                    assert!(!refresh, "busy lock must fail open, not queue a refresh");
                }
            }));
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            for task in tasks {
                task.await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(decider.cache.lock().await.len(), 1);
    }

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
                .rank(&config, &steps, 12, 23, features, SELECTOR_GENERATION)
                .await
                .unwrap(),
            vec![1, 0]
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
                .rank(
                    &active_config,
                    &active_steps,
                    12,
                    23,
                    features,
                    SELECTOR_GENERATION,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("second fetch should reach mock upstream");
        let (cached, refresh) = decider.cached_rank(&steps, 12, 23, features, SELECTOR_GENERATION);
        assert_eq!(cached.unwrap(), vec![1, 0]);
        assert!(!refresh, "in-flight request must not spawn duplicate fetch");
        let (unknown, refresh) = decider.cached_rank(
            &steps,
            12,
            23,
            AutoRequirements::from_signals(false, true, false),
            SELECTOR_GENERATION,
        );
        assert!(unknown.is_none());
        assert!(!refresh, "another cache key must not queue a second fetch");
        let duplicate = tokio::time::timeout(
            Duration::from_millis(100),
            decider.rank(&config, &steps, 12, 23, features, SELECTOR_GENERATION),
        )
        .await
        .expect("concurrent request should fail open using stale cache");
        assert_eq!(duplicate.unwrap(), vec![1, 0]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        release.notify_one();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            vec![1, 0],
        );
        let (fresh, refresh) = decider.cached_rank(&steps, 12, 23, features, SELECTOR_GENERATION);
        assert_eq!(fresh.unwrap(), vec![1, 0]);
        assert!(!refresh);
        server_task.abort();
    }

    #[tokio::test]
    async fn late_cancelled_selector_cannot_shorten_newly_successful_rank() {
        let decider = AutoDecider {
            client: reqwest::Client::new(),
            endpoint: String::new(),
            model: String::new(),
            key: String::new(),
            timeout: Duration::from_secs(1),
            ttl: Duration::from_secs(30),
            cache: Mutex::new(VecDeque::new()),
            refresh: Semaphore::new(1),
        };
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
        let key = ranking_key(&steps, 10, 40, features, SELECTOR_GENERATION);
        let original_fresh = Instant::now() + Duration::from_secs(30);
        let original_stale = original_fresh + Duration::from_secs(STALE_SECS);
        decider.cache.lock().await.push_back(Cache {
            key,
            order: vec![1, 0],
            fresh: original_fresh,
            stale: original_stale,
        });
        // Simulates an old request's cancellation arriving after another
        // request has already published a fresh, successful ranking.
        decider.cooldown_cancelled(&steps, 10, 40, features, SELECTOR_GENERATION);
        let entries = decider.cache.lock().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].fresh, original_fresh);
        assert_eq!(entries[0].stale, original_stale);
        drop(entries);
        let (order, refresh) = decider.cached_rank(&steps, 10, 40, features, SELECTOR_GENERATION);
        assert_eq!(order.unwrap(), vec![1, 0]);
        assert!(!refresh);
    }

    #[tokio::test]
    async fn cancelled_selector_does_not_repeat_external_timeout_on_next_request() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let server = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post({
                let calls = Arc::clone(&calls);
                let entered = Arc::clone(&entered);
                move || {
                    let calls = Arc::clone(&calls);
                    let entered = Arc::clone(&entered);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        entered.notify_one();
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        axum::Json(json!({"choices":[{"message":{"content":"{\"order\":[1,0]}"}}]}))
                    }
                }
            }),
        );
        let server_task = tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });
        let cfg: Config = serde_yaml::from_str(
            "providers:\n  - name: p\n    type: openai\n    base_url: http://127.0.0.1\n    key_env: K\nauto:\n  enabled: true\n  candidates:\n    - provider: p\n      model: one\n    - provider: p\n      model: two\n",
        ).unwrap();
        cfg.validate().unwrap();
        let decider = Arc::new(AutoDecider {
            client: reqwest::Client::new(),
            endpoint: format!("http://{addr}/v1/chat/completions"),
            model: "ranker".into(),
            key: "fake-key".into(),
            timeout: Duration::from_secs(5),
            ttl: Duration::from_secs(30),
            cache: Mutex::new(VecDeque::new()),
            refresh: Semaphore::new(1),
        });
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
        let req = AutoRequirements::default();
        let running = {
            let decider = Arc::clone(&decider);
            tokio::spawn(async move { decider.rank(&cfg, &steps, 10, 40, req, 7).await })
        };
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .expect("slow selector should start before cancellation");
        running.abort();
        let _ = running.await;
        let steps = [
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
        decider.cooldown_cancelled(&steps, 10, 40, req, 7);
        let (order, refresh) = decider.cached_rank(&steps, 10, 40, req, 7);
        assert!(order.is_none());
        assert!(
            !refresh,
            "the cancelled selector gets a short keyed cooldown"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        {
            let mut entries = decider.cache.lock().await;
            entries[0].fresh = Instant::now() - Duration::from_secs(1);
            entries[0].stale = entries[0].fresh;
        }
        let (_, refresh) = decider.cached_rank(&steps, 10, 40, req, 7);
        assert!(refresh, "the next request may refresh after the cooldown");
        server_task.abort();
    }

    #[tokio::test]
    async fn failed_selector_is_cooldown_cached_and_retries_after_expiry() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let counter = Arc::clone(&counter);
                async move {
                    if counter.fetch_add(1, Ordering::SeqCst) != 1 {
                        (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            axum::Json(json!({"error":"temporary outage"})),
                        )
                    } else {
                        (
                            axum::http::StatusCode::OK,
                            axum::Json(
                                json!({"choices":[{"message":{"content":"{\"order\":[1,0]}"}}]}),
                            ),
                        )
                    }
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config: Config = serde_yaml::from_str(&format!(
            "providers:\n  - name: p\n    type: openai\n    base_url: http://{address}\n    key_env: K\nauto:\n  enabled: true\n  candidates:\n    - provider: p\n      model: one\n    - provider: p\n      model: two\n  decider:\n    enabled: true\n    provider: p\n    model: ranker\n    timeout_ms: 500\n    cache_ttl_secs: 30\n",
        )).unwrap();
        config.validate().unwrap();
        let decider = AutoDecider::from_config(
            &config,
            &HashMap::from([("K".to_owned(), "fake-key".to_owned())]),
            reqwest::Client::new(),
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
        let features = AutoRequirements::default();
        assert!(
            decider
                .rank(&config, &steps, 10, 40, features, 7)
                .await
                .is_none()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        for _ in 0..3 {
            let (cached, refresh) = decider.cached_rank(&steps, 10, 40, features, 7);
            assert!(cached.is_none());
            assert!(
                !refresh,
                "failed ranking is cooling down; do not queue a retry"
            );
            assert!(
                decider
                    .rank(&config, &steps, 10, 40, features, 7)
                    .await
                    .is_none()
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        {
            let mut entries = decider.cache.lock().await;
            assert_eq!(entries.len(), 1);
            entries[0].fresh = Instant::now() - Duration::from_secs(1);
            entries[0].stale = entries[0].fresh;
        }
        assert_eq!(
            decider
                .rank(&config, &steps, 10, 40, features, 7)
                .await
                .unwrap(),
            vec![1, 0],
            "recovered selector must replace the expired negative entry"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            decider
                .rank(&config, &steps, 10, 40, features, 7)
                .await
                .unwrap(),
            vec![1, 0]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let old_stale = {
            let mut entries = decider.cache.lock().await;
            entries[0].fresh = Instant::now() - Duration::from_secs(1);
            entries[0].stale
        };
        assert_eq!(
            decider
                .rank(&config, &steps, 10, 40, features, 7)
                .await
                .unwrap(),
            vec![1, 0],
            "a failed refresh must preserve an unexpired successful ranking"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let (cached, refresh) = decider.cached_rank(&steps, 10, 40, features, 7);
        assert_eq!(cached.unwrap(), vec![1, 0]);
        assert!(!refresh, "stale success must get short refresh cooldown");
        assert_eq!(
            decider
                .rank(&config, &steps, 10, 40, features, 7)
                .await
                .unwrap(),
            vec![1, 0]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            decider.cache.lock().await[0].stale,
            old_stale,
            "refresh failure cannot extend the hard stale deadline"
        );
        server.abort();
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
        let (cold, needs_refresh) = decider.cached_rank(
            &steps,
            123,
            234,
            AutoRequirements::default(),
            SELECTOR_GENERATION,
        );
        assert!(cold.is_none());
        assert!(needs_refresh);
        let reordered = decider
            .rank(
                &config,
                &steps,
                123,
                234,
                AutoRequirements::default(),
                SELECTOR_GENERATION,
            )
            .await
            .unwrap();
        assert_eq!(reordered, vec![1, 0]);
        let cached = decider
            .rank(
                &config,
                &steps,
                123,
                234,
                AutoRequirements::default(),
                SELECTOR_GENERATION,
            )
            .await
            .unwrap();
        assert_eq!(cached, vec![1, 0]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Previously both estimates rounded to the same 1-KiB bucket,
        // reusing a selector decision that was made for another request.
        assert!(
            decider
                .cached_rank(
                    &steps,
                    124,
                    234,
                    AutoRequirements::default(),
                    SELECTOR_GENERATION
                )
                .0
                .is_none()
        );
        assert!(
            decider
                .cached_rank(
                    &steps,
                    123,
                    235,
                    AutoRequirements::default(),
                    SELECTOR_GENERATION
                )
                .0
                .is_none()
        );
        assert!(
            decider
                .rank(
                    &config,
                    &steps,
                    124,
                    234,
                    AutoRequirements::default(),
                    SELECTOR_GENERATION
                )
                .await
                .is_some()
        );
        assert!(
            decider
                .rank(
                    &config,
                    &steps,
                    123,
                    235,
                    AutoRequirements::default(),
                    SELECTOR_GENERATION
                )
                .await
                .is_some()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let (warm, needs_refresh) = decider.cached_rank(
            &steps,
            123,
            234,
            AutoRequirements::default(),
            SELECTOR_GENERATION,
        );
        assert_eq!(warm.unwrap(), vec![1, 0]);
        assert!(!needs_refresh);
        let (different_capability, refresh) = decider.cached_rank(
            &steps,
            123,
            234,
            AutoRequirements::from_signals(false, true, false),
            SELECTOR_GENERATION,
        );
        assert!(different_capability.is_none());
        assert!(refresh);
        let selected_key = ranking_key(
            &steps,
            123,
            234,
            AutoRequirements::default(),
            SELECTOR_GENERATION,
        );
        {
            let mut cache = decider.cache.lock().await;
            // Touching keys now updates LRU order. Never assume the oldest
            // deque slot still contains this request's ranking.
            let entry = cache
                .iter_mut()
                .find(|entry| entry.key == selected_key)
                .unwrap();
            entry.fresh = Instant::now() - Duration::from_millis(1);
            let (blocked, refresh) = decider.cached_rank(
                &steps,
                123,
                234,
                AutoRequirements::default(),
                SELECTOR_GENERATION,
            );
            assert!(blocked.is_none());
            assert!(!refresh, "in-flight refresh must not create a convoy");
        }
        let (stale, refresh) = decider.cached_rank(
            &steps,
            123,
            234,
            AutoRequirements::default(),
            SELECTOR_GENERATION,
        );
        assert_eq!(stale.unwrap(), vec![1, 0]);
        assert!(refresh, "stale ranking must schedule a background refresh");
        {
            let mut cache = decider.cache.lock().await;
            cache
                .iter_mut()
                .find(|entry| entry.key == selected_key)
                .unwrap()
                .stale = Instant::now() - Duration::from_millis(1);
        }
        let (expired, refresh) = decider.cached_rank(
            &steps,
            123,
            234,
            AutoRequirements::default(),
            SELECTOR_GENERATION,
        );
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
        let original = ranking_key(
            &[a.clone(), b.clone()],
            2000,
            3000,
            plain,
            SELECTOR_GENERATION,
        );
        assert_ne!(
            original,
            ranking_key(
                &[b.clone(), a.clone()],
                2000,
                3000,
                plain,
                SELECTOR_GENERATION
            )
        );
        assert_ne!(
            original,
            ranking_key(
                &[a.clone(), b.clone()],
                2000,
                3000,
                image,
                SELECTOR_GENERATION
            )
        );
        assert_ne!(
            original,
            ranking_key(
                &[a.clone(), b.clone()],
                4000,
                3000,
                plain,
                SELECTOR_GENERATION
            )
        );
        assert_ne!(
            original,
            ranking_key(
                &[a.clone(), b.clone()],
                2001,
                3000,
                plain,
                SELECTOR_GENERATION
            ),
            "distinct byte estimates within the old bucket must not share a rank"
        );
        assert_ne!(
            original,
            ranking_key(
                &[a.clone(), b.clone()],
                2000,
                3001,
                plain,
                SELECTOR_GENERATION
            ),
            "distinct token estimates within the old bucket must not share a rank"
        );
        // A live chain edit changes the route snapshot's precomputed
        // generation even when the ordered physical candidates stay equal.
        // The caller passes that generation, so no hot-path schema hash is
        // required for invalidation.
        let mut changed = a.clone();
        changed.params.insert("temperature".into(), json!(0.75));
        assert_ne!(
            original,
            ranking_key(&[changed, b], 2000, 3000, plain, SELECTOR_GENERATION + 1),
            "changed chain parameters must invalidate the selector cache"
        );
        let mut large = a.clone();
        large.params.insert(
            "tools".into(),
            json!([{"type":"function","function":{"name":"search",
                "description":"x".repeat(900_000)}}]),
        );
        let large_key = ranking_key(&[large.clone()], 2000, 3000, plain, SELECTOR_GENERATION);
        assert_eq!(
            large_key,
            ranking_key(&[large.clone()], 2000, 3000, plain, SELECTOR_GENERATION),
            "candidate identity and published generation must be stable"
        );
        large.params.insert("temperature".into(), json!(0.75));
        assert_ne!(
            large_key,
            ranking_key(&[large], 2000, 3000, plain, SELECTOR_GENERATION + 1),
            "even an additional small field must invalidate a large schema key"
        );
        let cfg: Config = serde_yaml::from_str(
            r#"
providers:
  - name: p
    type: openai
    base_url: https://example.test
    key_env: KEY
auto:
  enabled: true
  candidates:
    - provider: p
      model: a
"#,
        )
        .unwrap();
        let before = crate::routing::config_generation(&cfg);
        let mut updated = cfg.clone();
        updated.auto.candidates[0]
            .params
            .insert("tools".into(), json!([{"description":"x".repeat(900_000)}]));
        let after = crate::routing::config_generation(&updated);
        assert_ne!(
            before, after,
            "published schema changes must invalidate rankings"
        );
        assert_ne!(
            ranking_key(std::slice::from_ref(&a), 100, 200, plain, before),
            ranking_key(&[a], 100, 200, plain, after),
            "identical candidate names cannot reuse a ranking from another config"
        );
    }
}
