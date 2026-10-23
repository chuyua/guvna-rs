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

pub struct KeyPool {
    inner: Mutex<Inner>,
    rotation: String,
    auth_cooldown: Duration,
    rate_cooldown: Duration,
}

impl KeyPool {
    pub fn new(provider: &Provider, keys: &HashMap<String, String>) -> Result<Self, String> {
        let credentials = provider
            .key_names()
            .into_iter()
            .filter_map(|env| {
                let value = keys.get(&env)?;
                if value.is_empty() {
                    return None;
                }
                Some(Credential {
                    env,
                    secret: value.clone(),
                    epoch: 0,
                    blocked_until: None,
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
        })
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
            _ => return false, // Never quarantine keys for 5xx, 400, DNS or cancellation.
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
        true
    }

    pub fn record_success(&self, lease: &Lease) {
        let mut inner = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(key) = inner.credentials.get_mut(lease.index)
            && key.env == lease.env
            && key.epoch == lease.epoch
        {
            key.blocked_until = None;
        }
    }

    pub fn configured_cooldown(&self, outcome: Outcome) -> Option<Duration> {
        match outcome {
            Outcome::CredentialAuth => Some(self.auth_cooldown),
            Outcome::RateLimited => Some(self.rate_cooldown),
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
"#,
        )
        .unwrap()
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
        let a = pool.acquire(&HashSet::new()).unwrap();
        let a_late = a.clone();
        assert!(pool.record(&a, Outcome::RateLimited));
        assert!(!pool.record(&a_late, Outcome::RateLimited));
        assert!(pool.acquire(&HashSet::new()).is_some()); // other credential works
        assert!(!pool.record(&a_late, Outcome::ProviderTransient));
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
            inputs.insert(key.to_string(), "fake".to_string());
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
