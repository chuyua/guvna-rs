//! Fixed-size, lock-free in-process Auto health hints. No provider labels in metrics,
//! no SQLite request-path reads, and no network polling or background timers.
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, AtomicU64, Ordering};
use std::time::Instant;

const DEMOTE_AT: u8 = 70;
const RECOVER_AT: u8 = 30;
const MIN_SAMPLES: u16 = 3;
const FRESH_SECS: u64 = 300;

#[derive(Default)]
struct ProviderScore {
    penalty: AtomicU8,
    samples: AtomicU16,
    demoted: AtomicBool,
    last_failure_secs: AtomicU64,
}

pub struct AutoHealth {
    started: Instant,
    providers: Vec<ProviderScore>,
}

impl AutoHealth {
    pub fn new(count: usize) -> Self {
        Self {
            started: Instant::now(),
            providers: (0..count).map(|_| ProviderScore::default()).collect(),
        }
    }

    pub fn mark_failure(&self, index: usize) {
        self.failure_at(index, self.started.elapsed().as_secs());
    }

    pub fn mark_success(&self, index: usize) {
        if let Some(score) = self.providers.get(index) {
            let updated = score
                .penalty
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_sub(20))
                })
                .unwrap_or(0)
                .saturating_sub(20);
            if updated <= RECOVER_AT {
                score.demoted.store(false, Ordering::Relaxed);
            }
            let _ = score
                .samples
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_add(1))
                });
        }
    }

    pub fn penalties(&self) -> Vec<u8> {
        self.penalties_at(self.started.elapsed().as_secs())
    }

    fn failure_at(&self, index: usize, now_secs: u64) {
        if let Some(score) = self.providers.get(index) {
            let updated = score
                .penalty
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_add(35).min(100))
                })
                .unwrap_or(0)
                .saturating_add(35)
                .min(100);
            let previous = score
                .samples
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_add(1))
                })
                .unwrap_or(0);
            score.last_failure_secs.store(now_secs, Ordering::Relaxed);
            if previous.saturating_add(1) >= MIN_SAMPLES && updated >= DEMOTE_AT {
                score.demoted.store(true, Ordering::Relaxed);
            }
        }
    }

    fn penalties_at(&self, now_secs: u64) -> Vec<u8> {
        self.providers
            .iter()
            .map(|score| {
                let last_failure = score.last_failure_secs.load(Ordering::Relaxed);
                if score.demoted.load(Ordering::Relaxed)
                    && now_secs.saturating_sub(last_failure) <= FRESH_SECS
                {
                    score.penalty.load(Ordering::Relaxed)
                } else {
                    0
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn needs_multiple_failures_and_requires_recovery_hysteresis() {
        let health = AutoHealth::new(2);
        health.failure_at(0, 1);
        assert_eq!(health.penalties_at(1), vec![0, 0]);
        health.failure_at(0, 2);
        assert_eq!(health.penalties_at(2), vec![0, 0]);
        health.failure_at(0, 3);
        assert_eq!(health.penalties_at(3), vec![100, 0]);
        health.mark_success(0);
        assert_eq!(health.penalties_at(3), vec![80, 0]);
        health.mark_success(0);
        assert_eq!(health.penalties_at(3), vec![60, 0]);
        health.mark_success(0);
        health.mark_success(0);
        assert_eq!(health.penalties_at(3), vec![0, 0]);
    }

    #[test]
    fn stale_failures_age_out_without_timers_or_disk() {
        let health = AutoHealth::new(1);
        for _ in 0..3 {
            health.failure_at(0, 2);
        }
        assert_eq!(health.penalties_at(302), vec![100]);
        assert_eq!(health.penalties_at(303), vec![0]);
    }

    #[test]
    fn missing_provider_index_is_a_noop() {
        let health = AutoHealth::new(1);
        health.mark_failure(usize::MAX);
        health.mark_success(usize::MAX);
        assert_eq!(health.penalties(), vec![0]);
    }
}
