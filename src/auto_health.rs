//! Bounded opt-in Auto candidate health hints, no network requests, user data or timers.
use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

const DEMOTE_AT: u8 = 70;
const RECOVER_AT: u8 = 30;
const MIN_SAMPLES: u16 = 3;
const FRESH_SECS: u64 = 300;

// All four fields participate in one hysteresis transition. Four atomics
// behind another mutex were redundant and obscured whether a reader could
// observe a mixed old/new state. A single small lock also works across live
// route generations: the Arc is remapped by the physical candidate identity.
#[derive(Default)]
struct ProviderScore {
    state: Mutex<ProviderState>,
}

#[derive(Default)]
struct ProviderState {
    penalty: u8,
    consecutive_failures: u16,
    demoted: bool,
    last_failure_secs: u64,
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
pub struct AutoHealth {
    providers: Vec<Arc<ProviderScore>>,
}

impl AutoHealth {
    pub fn new(count: usize) -> Self {
        Self {
            providers: (0..count)
                .map(|_| Arc::new(ProviderScore::default()))
                .collect(),
        }
    }

    /// Preserve health by exact physical identity, never by the position in
    /// a newly discovered Auto candidate list. In-flight observations on a
    /// retained candidate update the same score through its shared Arc.
    pub fn remap(
        previous: &Self,
        old: &[crate::config::AutoCandidate],
        next: &[crate::config::AutoCandidate],
    ) -> Self {
        Self {
            providers: next
                .iter()
                .map(|candidate| {
                    old.iter()
                        .position(|item| {
                            item.provider == candidate.provider && item.model == candidate.model
                        })
                        .and_then(|index| previous.providers.get(index).cloned())
                        .unwrap_or_else(|| Arc::new(ProviderScore::default()))
                })
                .collect(),
        }
    }

    pub fn mark_failure(&self, index: usize) {
        self.failure_at(index, now_unix_secs());
    }

    pub fn mark_success(&self, index: usize) {
        if let Some(score) = self.providers.get(index) {
            let mut state = score.state.lock().unwrap_or_else(|e| e.into_inner());
            state.penalty = state.penalty.saturating_sub(20);
            if state.penalty <= RECOVER_AT {
                state.demoted = false;
            }
            state.consecutive_failures = 0;
        }
    }

    /// Restore only a bounded provider/model candidate summary from SQLite at startup.
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
            let mut state = score.state.lock().unwrap_or_else(|e| e.into_inner());
            let fresh = last_failure_secs <= now_secs
                && now_secs.saturating_sub(last_failure_secs) <= FRESH_SECS;
            state.penalty = if fresh { penalty.min(100) } else { 0 };
            state.consecutive_failures = if fresh { consecutive_failures } else { 0 };
            state.demoted = fresh && demoted && penalty > RECOVER_AT;
            state.last_failure_secs = last_failure_secs;
        }
    }
    pub fn penalties(&self) -> Vec<u8> {
        self.penalties_at(now_unix_secs())
    }

    fn failure_at(&self, index: usize, now_secs: u64) {
        if let Some(score) = self.providers.get(index) {
            let mut state = score.state.lock().unwrap_or_else(|e| e.into_inner());
            let previous = std::mem::replace(&mut state.last_failure_secs, now_secs);
            if now_secs.saturating_sub(previous) > FRESH_SECS || now_secs < previous {
                // A stale outage is not fresh evidence. Serialize window
                // rollover with the full demotion transition.
                state.penalty = 0;
                state.consecutive_failures = 0;
                state.demoted = false;
            }
            state.penalty = state.penalty.saturating_add(35).min(100);
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            if state.consecutive_failures >= MIN_SAMPLES && state.penalty >= DEMOTE_AT {
                state.demoted = true;
            }
        }
    }

    fn penalties_at(&self, now_secs: u64) -> Vec<u8> {
        self.providers
            .iter()
            .map(|score| {
                let state = score.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.demoted
                    && now_secs >= state.last_failure_secs
                    && now_secs.saturating_sub(state.last_failure_secs) <= FRESH_SECS
                {
                    state.penalty
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
    fn stale_failure_streak_does_not_immediately_demote_on_new_outage() {
        let health = AutoHealth::new(1);
        for time in [1, 2, 3] {
            health.failure_at(0, time);
        }
        assert_eq!(health.penalties_at(3), vec![100]);
        assert_eq!(health.penalties_at(304), vec![0]);
        health.failure_at(0, 400);
        assert_eq!(health.penalties_at(400), vec![0]);
        health.failure_at(0, 401);
        assert_eq!(health.penalties_at(401), vec![0]);
        health.failure_at(0, 402);
        assert_eq!(health.penalties_at(402), vec![100]);
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
    fn concurrent_failure_window_resets_do_not_erase_new_demotion() {
        use std::sync::{Arc, Barrier};
        let health = Arc::new(AutoHealth::new(1));
        for time in [1, 2, 3] {
            health.failure_at(0, time);
        }
        // New failures after a stale window must serialize the complete
        // reset-plus-penalty transition under contention.
        health.failure_at(0, 1000);
        let barrier = Arc::new(Barrier::new(17));
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let health = Arc::clone(&health);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    health.failure_at(0, 1001);
                });
            }
            barrier.wait();
        });
        assert_eq!(health.penalties_at(1001), vec![100]);
        assert!(health.providers[0].state.lock().unwrap().demoted);
    }

    #[test]
    fn failure_streak_counter_never_wraps() {
        let health = AutoHealth::new(1);
        health.restore_at(0, 100, u16::MAX, true, 1000, 1000);
        health.failure_at(0, 1001);
        let state = health.providers[0].state.lock().unwrap();
        assert_eq!(state.consecutive_failures, u16::MAX);
        assert_eq!(state.penalty, 100);
        assert!(state.demoted);
    }

    #[test]
    fn remap_keeps_shared_health_identity_while_old_generation_is_still_in_flight() {
        use crate::config::AutoCandidate;
        let make = |model: &str| AutoCandidate {
            provider: "provider".into(),
            model: model.into(),
            priority: 100,
            min_input_bytes: 0,
            max_input_bytes: None,
            max_context_tokens: None,
            supports_tools: None,
            supports_images: None,
            supports_audio: None,
            supports_json_schema: None,
            params: Default::default(),
        };
        let old = vec![make("a"), make("b")];
        let next = vec![make("b"), make("a"), make("new")];
        let current = AutoHealth::new(2);
        for _ in 0..3 {
            current.failure_at(0, 100);
        }
        let remapped = AutoHealth::remap(&current, &old, &next);
        assert_eq!(remapped.penalties_at(100), vec![0, 100, 0]);
        // A response admitted under the previous snapshot still updates
        // the same physical candidate score after the route reorder.
        current.mark_success(0);
        assert_eq!(remapped.penalties_at(100), vec![0, 80, 0]);
        for _ in 0..4 {
            remapped.mark_success(1);
        }
        assert_eq!(current.penalties_at(100), vec![0, 0]);
        assert_eq!(remapped.penalties_at(100), vec![0, 0, 0]);
    }
}
