//! Small key pool with lease-generation fencing. Do not leak credentials in Debug/telemetry.
use crate::{config::Provider, outcome::Outcome};
use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant},
};

struct Credential {
    env: String,
    secret: String,
    epoch: u64,
    blocked_until: Option<Instant>,
    blocked_for: Option<Outcome>,
    uses: u64,
}
struct Inner {
    credentials: Vec<Credential>,
    next: usize,
}

/// Lease captures the generation in which a request selected its credential.
#[derive(Clone)]
pub struct Lease {
    pub env: String,
    pub secret: String,
    index: usize,
    epoch: u64,
}

/// Aggregate counters only: no key names, values, providers or cooldown timestamps.
#[derive(Clone, Copy, Debug, Default, serde::Serialize, PartialEq, Eq)]
pub struct KeyHealthSnapshot {
    pub configured: usize,
    pub available: usize,
    pub auth_quarantined: usize,
    pub rate_limited: usize,
    pub transient_quarantined: usize,
    pub total_uses: u64,
}

impl std::ops::AddAssign for KeyHealthSnapshot {
    fn add_assign(&mut self, other: Self) {
        self.configured = self.configured.saturating_add(other.configured);
        self.available = self.available.saturating_add(other.available);
        self.auth_quarantined = self.auth_quarantined.saturating_add(other.auth_quarantined);
        self.rate_limited = self.rate_limited.saturating_add(other.rate_limited);
        self.transient_quarantined = self
            .transient_quarantined
            .saturating_add(other.transient_quarantined);
        self.total_uses = self.total_uses.saturating_add(other.total_uses);
    }
}

pub struct KeyPool {
    inner: Mutex<Inner>,
    rotation: String,
    auth_cooldown: Duration,
    rate_cooldown: Duration,
    transient_cooldown: Option<Duration>,
}

impl KeyPool {
    pub fn new(provider: &Provider, keys: &HashMap<String, String>) -> Result<Self, String> {
        // Different environment variable names can refer to the very same
        // provider credential. Sending it a second time after an HTTP 429 or
        // 401 is a guaranteed useless retry and overstates available quota.
        // Deduplicate only WITHIN one physical provider. Preserve the first
        // configured alias without ever logging or hashing keys externally.
        let mut seen_secrets = HashSet::new();
        let credentials = provider
            .key_names()
            .into_iter()
            .filter_map(|env| {
                let value = keys.get(&env)?;
                if value.is_empty() || !seen_secrets.insert(value.as_str()) {
                    return None;
                }
                Some(Credential {
                    env,
                    secret: value.clone(),
                    epoch: 0,
                    blocked_until: None,
                    blocked_for: None,
                    uses: 0,
                })
            })
            .collect();
        Ok(Self {
            inner: Mutex::new(Inner {
                credentials,
                next: 0,
            }),
            rotation: provider.rotation.clone(),
            auth_cooldown: provider.quarantine.auth()?,
            rate_cooldown: provider.quarantine.rate_limit()?,
            // Preserve historical behavior unless this formerly ignored
            // provider policy is explicitly configured.
            transient_cooldown: provider
                .quarantine
                .transient
                .as_ref()
                .map(|_| provider.quarantine.transient())
                .transpose()?,
        })
    }

    /// Local CLI status snapshot; a brief mutex read, never acquire a lease.
    /// Expired cooldowns are counted as available even before their next use.
    pub fn health_snapshot(&self) -> KeyHealthSnapshot {
        let now = Instant::now();
        let inner = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        let mut result = KeyHealthSnapshot {
            configured: inner.credentials.len(),
            ..KeyHealthSnapshot::default()
        };
        for key in &inner.credentials {
            result.total_uses = result.total_uses.saturating_add(key.uses);
            if key.blocked_until.is_none_or(|until| until <= now) {
                result.available += 1;
            } else {
                match key.blocked_for {
                    Some(Outcome::CredentialAuth) => result.auth_quarantined += 1,
                    Some(Outcome::RateLimited) => result.rate_limited += 1,
                    _ => result.transient_quarantined += 1,
                }
            }
        }
        result
    }

    pub fn acquire(&self, excluded: &HashSet<String>) -> Option<Lease> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        let count = inner.credentials.len();
        if count == 0 {
            return None;
        }
        let start = if self.rotation == "sequential" {
            0
        } else {
            inner.next % count
        };
        // Scan in rotation order without allocating or sorting an index vector per request.
        let mut selected: Option<usize> = None;
        for offset in 0..count {
            let index = (start + offset) % count;
            let key = &inner.credentials[index];
            if excluded.contains(&key.env) || key.blocked_until.is_some_and(|until| until > now) {
                continue;
            }
            if self.rotation != "least_used" {
                selected = Some(index);
                break;
            }
            if selected.is_none_or(|old| key.uses < inner.credentials[old].uses) {
                selected = Some(index);
            }
        }
        let index = selected?;
        inner.next = (index + 1) % count;
        let key = &mut inner.credentials[index];
        key.uses += 1;
        Some(Lease {
            env: key.env.clone(),
            secret: key.secret.clone(),
            index,
            epoch: key.epoch,
        })
    }

    /// Returns true only when this result actually changed the current key state.
    pub fn record(&self, lease: &Lease, outcome: Outcome) -> bool {
        self.record_with_retry_after(lease, outcome, None)
    }

    /// A valid server Retry-After may extend 429 isolation, but cannot shorten
    /// the configured quarantine or extend a key already fenced by another result.
    pub fn record_with_retry_after(
        &self,
        lease: &Lease,
        outcome: Outcome,
        retry_after: Option<Duration>,
    ) -> bool {
        let delay = match outcome {
            Outcome::CredentialAuth => self.auth_cooldown,
            Outcome::RateLimited => self.rate_cooldown.max(
                retry_after
                    .unwrap_or_default()
                    .min(Duration::from_secs(24 * 3600)),
            ),
            Outcome::ProviderTransient | Outcome::TransportFailure => {
                match self.transient_cooldown {
                    Some(duration) if !duration.is_zero() => duration,
                    _ => return false,
                }
            }
            _ => return false, // Caller rejections and cancellations never quarantine keys.
        };
        let mut inner = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        let Some(key) = inner.credentials.get_mut(lease.index) else {
            return false;
        };
        if key.env != lease.env || key.epoch != lease.epoch {
            return false; // Late in-flight result cannot escalate an already quarantined key.
        }
        key.epoch = key.epoch.wrapping_add(1);
        key.blocked_until = Instant::now().checked_add(delay);
        key.blocked_for = Some(outcome);
        true
    }

    pub fn record_success(&self, lease: &Lease) {
        let mut inner = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(key) = inner.credentials.get_mut(lease.index)
            && key.env == lease.env
            && key.epoch == lease.epoch
        {
            key.blocked_until = None;
            key.blocked_for = None;
        }
    }

    /// An advisory only when every usable credential is still cooling down
    /// due specifically to upstream 429, never because of 401/403 or no keys.
    /// The caller may safely map *all* eligible candidates to HTTP 429 only
    /// if this is true for each candidate, preserving conservative failures.
    pub fn all_keys_rate_limited_for(&self) -> Option<Duration> {
        let now = Instant::now();
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.credentials.is_empty() {
            return None;
        }
        let mut shortest = Duration::MAX;
        for key in &inner.credentials {
            if key.blocked_for != Some(Outcome::RateLimited) {
                return None;
            }
            let remaining = key.blocked_until?.checked_duration_since(now)?;
            if remaining.is_zero() {
                return None;
            }
            shortest = shortest.min(remaining);
        }
        Some(shortest)
    }

    pub fn configured_cooldown(&self, outcome: Outcome) -> Option<Duration> {
        match outcome {
            Outcome::CredentialAuth => Some(self.auth_cooldown),
            Outcome::RateLimited => Some(self.rate_cooldown),
            Outcome::ProviderTransient | Outcome::TransportFailure => self.transient_cooldown,
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Provider;
    fn provider() -> Provider {
        serde_yaml::from_str(
            r#"
name: p
type: openai
base_url: https://example.org
key_envs: [A, B]
quarantine:
  auth: 48h
  rate_limit: 2h
  transient: 30s
"#,
        )
        .unwrap()
    }

    #[test]
    fn local_health_snapshot_is_aggregate_and_respects_fenced_cooldowns() {
        let pool = KeyPool::new(
            &provider(),
            &HashMap::from([
                ("A".into(), "sensitive-alpha".into()),
                ("B".into(), "sensitive-beta".into()),
            ]),
        )
        .unwrap();
        assert_eq!(pool.health_snapshot().configured, 2);
        assert_eq!(pool.health_snapshot().available, 2);
        let first = pool.acquire(&HashSet::new()).unwrap();
        assert!(pool.record(&first, Outcome::CredentialAuth));
        let second = pool.acquire(&HashSet::new()).unwrap();
        assert!(pool.record(&second, Outcome::RateLimited));
        let snapshot = pool.health_snapshot();
        assert_eq!(snapshot.configured, 2);
        assert_eq!(snapshot.available, 0);
        assert_eq!(snapshot.auth_quarantined, 1);
        assert_eq!(snapshot.rate_limited, 1);
        assert_eq!(snapshot.transient_quarantined, 0);
        assert_eq!(snapshot.total_uses, 2);
        let serialized = serde_json::to_string(&snapshot).unwrap();
        for secret in ["sensitive-alpha", "sensitive-beta", "A", "B"] {
            assert!(!serialized.contains(secret), "key metadata leaked");
        }
        // Stale successful callbacks cannot remove quarantine or skew status.
        pool.record_success(&first);
        assert_eq!(pool.health_snapshot(), snapshot);
    }

    #[test]
    fn local_health_snapshot_does_not_report_missing_keys_as_available() {
        let pool = KeyPool::new(&provider(), &HashMap::new()).unwrap();
        let snapshot = pool.health_snapshot();
        assert_eq!(snapshot.configured, 0);
        assert_eq!(snapshot.available, 0);
        assert_eq!(snapshot.total_uses, 0);
    }

    #[test]
    fn duplicate_environment_aliases_do_not_retry_one_physical_credential() {
        let same_secret = "only-one-real-credential".to_owned();
        let pool = KeyPool::new(
            &provider(),
            &HashMap::from([
                ("A".into(), same_secret.clone()),
                ("B".into(), same_secret.clone()),
            ]),
        )
        .unwrap();
        assert_eq!(pool.health_snapshot().configured, 1);
        let first = pool.acquire(&HashSet::new()).unwrap();
        assert_eq!(first.env, "A");
        assert!(pool.record(&first, Outcome::RateLimited));
        assert!(
            pool.acquire(&HashSet::from([first.env.clone()])).is_none(),
            "another environment alias must not retry the same 429 credential"
        );
        assert!(pool.all_keys_rate_limited_for().is_some());
        assert_eq!(pool.health_snapshot().rate_limited, 1);
        let pool = KeyPool::new(
            &provider(),
            &HashMap::from([
                ("A".into(), same_secret),
                ("B".into(), "a-different-real-key".into()),
            ]),
        )
        .unwrap();
        assert_eq!(pool.health_snapshot().configured, 2);
    }

    #[test]
    fn cooldown_classes_are_not_capped_by_default_auth_window() {
        let pool = KeyPool::new(
            &provider(),
            &HashMap::from([
                ("A".into(), "a-secret".into()),
                ("B".into(), "b-secret".into()),
            ]),
        )
        .unwrap();
        assert_eq!(
            pool.configured_cooldown(Outcome::CredentialAuth),
            Some(Duration::from_secs(48 * 3600))
        );
        assert_eq!(
            pool.configured_cooldown(Outcome::RateLimited),
            Some(Duration::from_secs(2 * 3600))
        );
        assert_eq!(
            pool.configured_cooldown(Outcome::ProviderTransient),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            pool.configured_cooldown(Outcome::TransportFailure),
            Some(Duration::from_secs(30))
        );
        let a = pool.acquire(&HashSet::new()).unwrap();
        let a_late = a.clone();
        assert!(pool.record(&a, Outcome::RateLimited));
        assert!(!pool.record(&a_late, Outcome::RateLimited));
        assert!(pool.acquire(&HashSet::new()).is_some()); // other credential works
        assert!(!pool.record(&a_late, Outcome::ProviderTransient));
    }

    #[test]
    fn transient_failures_quarantine_credentials_and_keep_generation_fencing() {
        let mut provider = provider();
        provider.key_envs = vec!["A".into()];
        let keys = HashMap::from([("A".into(), "a-secret".into())]);
        for outcome in [Outcome::ProviderTransient, Outcome::TransportFailure] {
            let pool = KeyPool::new(&provider, &keys).unwrap();
            let lease = pool.acquire(&HashSet::new()).unwrap();
            let late = lease.clone();
            assert!(pool.record(&lease, outcome));
            assert!(!pool.record(&late, outcome));
            assert!(pool.acquire(&HashSet::new()).is_none());
        }
    }

    #[test]
    fn transient_is_opt_in_to_preserve_existing_circuit_breaker_behavior() {
        let mut provider = provider();
        provider.quarantine.transient = None;
        provider.key_envs = vec!["A".into()];
        let pool = KeyPool::new(
            &provider,
            &HashMap::from([("A".into(), "safe-test-secret".into())]),
        )
        .unwrap();
        let lease = pool.acquire(&HashSet::new()).unwrap();
        assert_eq!(pool.configured_cooldown(Outcome::ProviderTransient), None);
        assert!(!pool.record(&lease, Outcome::ProviderTransient));
        assert!(!pool.record(&lease, Outcome::TransportFailure));
        assert!(pool.acquire(&HashSet::new()).is_some());
    }

    #[test]
    fn exhausted_429_cooldown_requires_every_key_and_excludes_auth_or_missing() {
        let mut provider = provider();
        provider.quarantine.rate_limit = Some("5s".into());
        let keys = HashMap::from([
            ("A".to_owned(), "a-local".to_owned()),
            ("B".to_owned(), "b-local".to_owned()),
        ]);
        let pool = KeyPool::new(&provider, &keys).unwrap();
        assert!(pool.all_keys_rate_limited_for().is_none());
        let first = pool.acquire(&HashSet::new()).unwrap();
        assert!(pool.record(&first, Outcome::RateLimited));
        assert!(pool.all_keys_rate_limited_for().is_none());
        let second = pool.acquire(&HashSet::new()).unwrap();
        assert!(pool.record(&second, Outcome::RateLimited));
        let remaining = pool.all_keys_rate_limited_for().unwrap();
        assert!(remaining > Duration::from_secs(4));
        assert!(remaining <= Duration::from_secs(5));
        assert!(pool.acquire(&HashSet::new()).is_none());
        // A stale success cannot clear an earlier quarantine.
        pool.record_success(&first);
        assert!(pool.all_keys_rate_limited_for().is_some());

        let auth_pool = KeyPool::new(&provider, &keys).unwrap();
        let first = auth_pool.acquire(&HashSet::new()).unwrap();
        assert!(auth_pool.record(&first, Outcome::RateLimited));
        let second = auth_pool.acquire(&HashSet::new()).unwrap();
        assert!(auth_pool.record(&second, Outcome::CredentialAuth));
        assert!(auth_pool.all_keys_rate_limited_for().is_none());

        let empty = KeyPool::new(&provider, &HashMap::new()).unwrap();
        assert!(empty.all_keys_rate_limited_for().is_none());
    }

    #[test]
    fn stale_success_cannot_reopen_newer_quarantine() {
        let mut p = provider();
        p.key_envs = vec!["A".into()];
        let pool = KeyPool::new(&p, &HashMap::from([("A".into(), "secret".into())])).unwrap();
        let old = pool.acquire(&HashSet::new()).unwrap();
        assert!(pool.record(&old, Outcome::CredentialAuth));
        pool.record_success(&old);
        assert!(pool.acquire(&HashSet::new()).is_none());
    }

    #[test]
    fn retry_after_is_bounded_and_cannot_be_overridden_by_late_results() {
        let mut provider = provider();
        provider.quarantine.rate_limit = Some("100ms".into());
        provider.key_envs = vec!["A".into()];
        let credentials = HashMap::from([("A".into(), "safe-test-key".into())]);
        let pool = KeyPool::new(&provider, &credentials).unwrap();
        let lease = pool.acquire(&HashSet::new()).unwrap();
        assert!(pool.record_with_retry_after(
            &lease,
            Outcome::RateLimited,
            Some(Duration::from_secs(7200)),
        ));
        let remaining = pool.inner.lock().unwrap().credentials[0]
            .blocked_until
            .unwrap()
            .saturating_duration_since(Instant::now());
        assert!(remaining > Duration::from_secs(7190));
        assert!(!pool.record_with_retry_after(
            &lease,
            Outcome::RateLimited,
            Some(Duration::from_secs(86400 * 365)),
        ));
        assert!(pool.acquire(&HashSet::new()).is_none());

        let pool = KeyPool::new(&provider, &credentials).unwrap();
        let lease = pool.acquire(&HashSet::new()).unwrap();
        assert!(pool.record_with_retry_after(
            &lease,
            Outcome::RateLimited,
            Some(Duration::from_secs(86400 * 365)),
        ));
        let remaining = pool.inner.lock().unwrap().credentials[0]
            .blocked_until
            .unwrap()
            .saturating_duration_since(Instant::now());
        assert!(remaining <= Duration::from_secs(86400));
        assert!(remaining > Duration::from_secs(86390));
    }
}

#[cfg(test)]
mod contention_tests {
    use super::*;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    };
    use std::thread;

    fn one_key_pool() -> Arc<KeyPool> {
        let provider: Provider = serde_yaml::from_str(
            "name: p\ntype: openai\nbase_url: https://example.org\nkey_env: A\n",
        )
        .unwrap();
        Arc::new(
            KeyPool::new(
                &provider,
                &HashMap::from([("A".into(), "only-local-test-key".into())]),
            )
            .unwrap(),
        )
    }

    /// 128 in-flight failures for one lease MUST produce only one quarantine.
    /// Repeat to detect races rather than relying on one fortunate schedule.
    #[test]
    fn concurrent_late_failures_cannot_extend_quarantine() {
        for _ in 0..12 {
            let pool = one_key_pool();
            let lease = pool.acquire(&HashSet::new()).unwrap();
            let gate = Arc::new(Barrier::new(64));
            let accepted = Arc::new(AtomicUsize::new(0));
            thread::scope(|scope| {
                for _ in 0..64 {
                    let pool = pool.clone();
                    let gate = gate.clone();
                    let lease = lease.clone();
                    let accepted = accepted.clone();
                    scope.spawn(move || {
                        gate.wait();
                        if pool.record(&lease, Outcome::RateLimited) {
                            accepted.fetch_add(1, Ordering::SeqCst);
                        }
                        pool.record_success(&lease); // stale successes must never reopen
                    });
                }
            });
            assert_eq!(
                accepted.load(Ordering::SeqCst),
                1,
                "only one generation may apply quarantine"
            );
            assert!(pool.acquire(&HashSet::new()).is_none());
        }
    }

    #[test]
    fn ten_thousand_parallel_key_selections_do_not_lose_updates() {
        let p: Provider = serde_yaml::from_str(
            "name: p\ntype: openai\nbase_url: https://example.org\nkey_envs: [A, B, C, D]\nrotation: round_robin\n"
        ).unwrap();
        let mut inputs = HashMap::new();
        for key in ["A", "B", "C", "D"] {
            // Round-robin fairness is measured over distinct credentials.
            // Duplicate aliases are intentionally deduplicated on startup.
            inputs.insert(key.to_string(), format!("test-secret-{key}"));
        }
        let pool = Arc::new(KeyPool::new(&p, &inputs).unwrap());
        let counts = Arc::new(Mutex::new(HashMap::<String, usize>::new()));
        thread::scope(|scope| {
            for _ in 0..32 {
                let pool = pool.clone();
                let counts = counts.clone();
                scope.spawn(move || {
                    for _ in 0..320 {
                        let lease = pool.acquire(&HashSet::new()).unwrap();
                        *counts.lock().unwrap().entry(lease.env).or_default() += 1;
                    }
                });
            }
        });
        let counts = counts.lock().unwrap();
        assert_eq!(counts.values().sum::<usize>(), 10240);
        // 10,240 is divisible by four; serialized RR state must distribute exactly.
        assert!(counts.values().all(|c| *c == 2560), "{counts:?}");
    }
}
