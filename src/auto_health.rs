//! Bounded opt-in provider health hints, no network requests, user data or timers.
use std::{
    sync::atomic::{AtomicBool, AtomicU8, AtomicU16, AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

const DEMOTE_AT: u8 = 70;
const RECOVER_AT: u8 = 30;
const MIN_SAMPLES: u16 = 3;
const FRESH_SECS: u64 = 300;

#[derive(Default)]
struct ProviderScore {
    penalty: AtomicU8,
    consecutive_failures: AtomicU16,
    demoted: AtomicBool,
    last_failure_secs: AtomicU64,
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn update_penalty(counter: &AtomicU8, transform: impl Fn(u8) -> u8) -> u8 {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = transform(current);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => current = actual,
        }
    }
}

fn count_observation(counter: &AtomicU16) -> u16 {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(1);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => current = actual,
        }
    }
}

pub struct AutoHealth {
    providers: Vec<ProviderScore>,
}

impl AutoHealth {
    pub fn new(count: usize) -> Self {
        Self {
            providers: (0..count).map(|_| ProviderScore::default()).collect(),
        }
    }

    pub fn mark_failure(&self, index: usize) {
        self.failure_at(index, now_unix_secs());
    }

    pub fn mark_success(&self, index: usize) {
        if let Some(score) = self.providers.get(index) {
            let updated = update_penalty(&score.penalty, |value| value.saturating_sub(20));
            if updated <= RECOVER_AT {
                score.demoted.store(false, Ordering::Relaxed);
            }
            score.consecutive_failures.store(0, Ordering::Relaxed);
        }
    }

    /// Restore only the bounded provider summary persisted by SQLite at startup.
    pub fn restore(
        &self,
        index: usize,
        penalty: u8,
        consecutive_failures: u16,
        demoted: bool,
        last_failure_secs: u64,
    ) {
        self.restore_at(
            index,
            penalty,
            consecutive_failures,
            demoted,
            last_failure_secs,
            now_unix_secs(),
        );
    }

    fn restore_at(
        &self,
        index: usize,
        penalty: u8,
        consecutive_failures: u16,
        demoted: bool,
        last_failure_secs: u64,
        now_secs: u64,
    ) {
        if let Some(score) = self.providers.get(index) {
            let fresh = now_secs.saturating_sub(last_failure_secs) <= FRESH_SECS;
            score
                .penalty
                .store(if fresh { penalty.min(100) } else { 0 }, Ordering::Relaxed);
            score.consecutive_failures.store(
                if fresh { consecutive_failures } else { 0 },
                Ordering::Relaxed,
            );
            score.demoted.store(
                fresh && demoted && consecutive_failures >= MIN_SAMPLES && penalty >= DEMOTE_AT,
                Ordering::Relaxed,
            );
            score
                .last_failure_secs
                .store(last_failure_secs, Ordering::Relaxed);
        }
    }
    pub fn penalties(&self) -> Vec<u8> {
        self.penalties_at(now_unix_secs())
    }

    fn failure_at(&self, index: usize, now_secs: u64) {
        if let Some(score) = self.providers.get(index) {
            let updated = update_penalty(&score.penalty, |value| value.saturating_add(35).min(100));
            let failures = count_observation(&score.consecutive_failures);
            score.last_failure_secs.store(now_secs, Ordering::Relaxed);
            if failures >= MIN_SAMPLES && updated >= DEMOTE_AT {
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
    fn multiple_failures_and_recovery_hysteresis() {
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
    fn successful_request_resets_failure_streak_before_demoting() {
        let health = AutoHealth::new(1);
        health.failure_at(0, 1);
        health.failure_at(0, 2);
        health.mark_success(0);
        health.failure_at(0, 3);
        health.failure_at(0, 4);
        assert_eq!(health.penalties_at(4), vec![0]);
        health.failure_at(0, 5);
        assert_eq!(health.penalties_at(5), vec![100]);
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
    fn restores_recent_health_but_ignores_stale_history() {
        let health = AutoHealth::new(2);
        health.restore_at(0, 100, 3, true, 900, 1000);
        health.restore_at(1, 100, 3, true, 600, 1000);
        assert_eq!(health.penalties_at(1000), vec![100, 0]);
        assert_eq!(health.penalties_at(1201), vec![0, 0]);
    }
    #[test]
    fn missing_provider_index_is_a_noop() {
        let health = AutoHealth::new(1);
        health.mark_failure(usize::MAX);
        health.mark_success(usize::MAX);
        assert_eq!(health.penalties(), vec![0]);
    }

    #[test]
    fn failure_streak_counter_never_wraps() {
        let counter = AtomicU16::new(u16::MAX);
        assert_eq!(count_observation(&counter), u16::MAX);
        assert_eq!(counter.load(Ordering::Relaxed), u16::MAX);
    }
}
