//! Optional, per-provider outage circuit breaker.
//! No background tasks, no per-key labels, no per-request heap collection.
//! A single half-open request probes recovery; abandoned probes cannot
//! permanently leave the circuit stuck open.
use crate::config::CircuitBreakerConfig;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

// Half-open probes have a renewable idle lease and a strict total lifetime.
const PROBE_IDLE_LEASE: Duration = Duration::from_secs(10);
const PROBE_MAX_LIFETIME: Duration = Duration::from_secs(120);

struct State {
    failures: u32,
    generation: u64,
    open_until: Option<Instant>,
    probing: bool,
    probe_deadline: Option<Instant>,
    probe_hard_deadline: Option<Instant>,
    next_cooldown: Duration,
}

pub struct Circuit {
    state: Mutex<State>,
    threshold: u32,
    base: Duration,
    max: Duration,
}

pub struct Ticket {
    circuit: Arc<Circuit>,
    generation: u64,
    probe: bool,
    settled: bool,
}

impl Circuit {
    pub fn new(config: &CircuitBreakerConfig) -> Result<Option<Arc<Self>>, String> {
        let (base, max) = config.timings()?;
        if !config.enabled {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self {
            state: Mutex::new(State {
                failures: 0,
                generation: 0,
                open_until: None,
                probing: false,
                probe_deadline: None,
                probe_hard_deadline: None,
                next_cooldown: base,
            }),
            threshold: config.failure_threshold,
            base,
            max,
        })))
    }

    pub fn admit(self: &Arc<Self>) -> Option<Ticket> {
        self.admit_at(Instant::now())
    }

    pub fn admit_at(self: &Arc<Self>, now: Instant) -> Option<Ticket> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.probing && state.probe_deadline.is_some_and(|until| now >= until) {
            // Fence off the outstanding long-lived SSE probe result.
            state.probing = false;
            state.probe_deadline = None;
            state.probe_hard_deadline = None;
            state.generation = state.generation.wrapping_add(1);
            state.open_until = now.checked_add(self.base);
            return None;
        }
        let probe = if let Some(until) = state.open_until {
            if now < until || state.probing {
                return None;
            }
            state.probing = true;
            state.probe_deadline = now.checked_add(PROBE_IDLE_LEASE);
            state.probe_hard_deadline = now.checked_add(PROBE_MAX_LIFETIME);
            true
        } else {
            false
        };
        Some(Ticket {
            circuit: Arc::clone(self),
            generation: state.generation,
            probe,
            settled: false,
        })
    }

    pub fn is_open(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.open_until.is_some_and(|until| Instant::now() < until)
    }

    fn report(&self, ticket: &Ticket, failure: bool, now: Instant) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.generation != ticket.generation {
            return false;
        }
        if !failure {
            state.failures = 0;
            state.probing = false;
            state.probe_deadline = None;
            state.probe_hard_deadline = None;
            state.open_until = None;
            state.next_cooldown = self.base;
            return false;
        }
        state.failures = state.failures.saturating_add(1);
        if !ticket.probe && state.failures < self.threshold {
            return false;
        }
        let delay = state.next_cooldown;
        state.open_until = now.checked_add(delay);
        state.next_cooldown = delay.saturating_mul(2).min(self.max);
        state.probing = false;
        state.probe_deadline = None;
        state.probe_hard_deadline = None;
        state.generation = state.generation.wrapping_add(1);
        state.failures = 0;
        true
    }

    fn probe_progress(&self, ticket: &Ticket, now: Instant) {
        if !ticket.probe {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.generation != ticket.generation || !state.probing {
            return;
        }
        let (Some(deadline), Some(hard_deadline)) =
            (state.probe_deadline, state.probe_hard_deadline)
        else {
            return;
        };
        if now >= deadline || now >= hard_deadline {
            return;
        }
        state.probe_deadline = Some(
            now.checked_add(PROBE_IDLE_LEASE)
                .unwrap_or(hard_deadline)
                .min(hard_deadline),
        );
    }

    fn abandoned_probe(&self, ticket: &Ticket) {
        if !ticket.probe {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.generation == ticket.generation && state.probing {
            state.probing = false;
            state.probe_deadline = None;
            state.probe_hard_deadline = None;
            // Client cancellation is neutral, not a provider failure. Delay
            // another probe briefly instead of unleashing a stampede.
            state.open_until = Instant::now().checked_add(self.base);
        }
    }
}

impl Ticket {
    pub fn is_probe(&self) -> bool {
        self.probe
    }
    /// Renew the idle lease without counting a partial stream as success.
    pub fn progress(&mut self) {
        self.progress_at(Instant::now());
    }

    pub fn progress_at(&mut self, now: Instant) {
        if !self.settled {
            self.circuit.probe_progress(self, now);
        }
    }
    /// True if the provider circuit was opened/reopened by this failure.
    pub fn failure(&mut self) -> bool {
        self.failure_at(Instant::now())
    }

    pub fn failure_at(&mut self, now: Instant) -> bool {
        if self.settled {
            return false;
        }
        self.settled = true;
        self.circuit.report(self, true, now)
    }

    pub fn success(&mut self) {
        if self.settled {
            return;
        }
        self.settled = true;
        self.circuit.report(self, false, Instant::now());
    }

    pub fn success_at(&mut self, now: Instant) {
        if self.settled {
            return;
        }
        self.settled = true;
        self.circuit.report(self, false, now);
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if !self.settled {
            self.circuit.abandoned_probe(self);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn circuit() -> Arc<Circuit> {
        Circuit::new(&CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 3,
            cooldown: "100ms".into(),
            max_cooldown: "800ms".into(),
        })
        .unwrap()
        .unwrap()
    }

    #[test]
    fn three_failures_open_then_one_probe_restores_and_resets_backoff() {
        let breaker = circuit();
        let now = Instant::now();
        for _ in 0..2 {
            let mut ticket = breaker.admit_at(now).unwrap();
            assert!(!ticket.failure_at(now));
        }
        let mut third = breaker.admit_at(now).unwrap();
        assert!(third.failure_at(now));
        assert!(breaker.admit_at(now + Duration::from_millis(99)).is_none());
        let mut probe = breaker.admit_at(now + Duration::from_millis(100)).unwrap();
        assert!(breaker.admit_at(now + Duration::from_millis(101)).is_none());
        probe.success_at(now + Duration::from_millis(102));
        assert!(breaker.admit_at(now + Duration::from_millis(103)).is_some());
        for _ in 0..3 {
            let mut t = breaker.admit_at(now + Duration::from_millis(104)).unwrap();
            t.failure_at(now + Duration::from_millis(104));
        }
        assert!(breaker.admit_at(now + Duration::from_millis(204)).is_some());
    }

    #[test]
    fn probe_failure_doubles_cooldown_bounded_and_stale_result_cannot_close() {
        let breaker = circuit();
        let now = Instant::now();
        let mut stale = breaker.admit_at(now).unwrap();
        for _ in 0..3 {
            let mut ticket = breaker.admit_at(now).unwrap();
            ticket.failure_at(now);
        }
        stale.success_at(now);
        assert!(breaker.admit_at(now + Duration::from_millis(99)).is_none());
        let mut probe = breaker.admit_at(now + Duration::from_millis(100)).unwrap();
        assert!(probe.failure_at(now + Duration::from_millis(100)));
        assert!(breaker.admit_at(now + Duration::from_millis(299)).is_none());
        let mut probe = breaker.admit_at(now + Duration::from_millis(300)).unwrap();
        assert!(probe.failure_at(now + Duration::from_millis(300)));
        assert!(breaker.admit_at(now + Duration::from_millis(699)).is_none());
        let mut probe = breaker.admit_at(now + Duration::from_millis(700)).unwrap();
        assert!(probe.failure_at(now + Duration::from_millis(700)));
        assert!(
            breaker
                .admit_at(now + Duration::from_millis(1499))
                .is_none()
        );
        assert!(
            breaker
                .admit_at(now + Duration::from_millis(1500))
                .is_some()
        );
    }

    #[test]
    fn failed_or_cancelled_probe_is_never_a_permanent_lock() {
        let breaker = circuit();
        let now = Instant::now();
        for _ in 0..3 {
            let mut t = breaker.admit_at(now).unwrap();
            t.failure_at(now);
        }
        {
            let _probe = breaker.admit_at(now + Duration::from_millis(100)).unwrap();
            assert!(breaker.admit_at(now + Duration::from_millis(200)).is_none());
        }
        assert!(breaker.admit().is_none());
        // Artificial-time tests may simulate expiry without sleeping.
        let recovered = breaker.admit_at(Instant::now() + Duration::from_millis(100));
        assert!(recovered.is_some());
    }

    #[test]
    fn stale_ten_second_probe_cannot_block_other_clients_forever() {
        let breaker = circuit();
        let start = Instant::now();
        for _ in 0..3 {
            let mut ticket = breaker.admit_at(start).unwrap();
            ticket.failure_at(start);
        }
        let mut stalled = breaker
            .admit_at(start + Duration::from_millis(100))
            .unwrap();
        assert!(stalled.is_probe());
        // The first arrival after expiry fences the stalled probe and
        // starts a short neutral backoff before another probe is permitted.
        assert!(breaker.admit_at(start + Duration::from_secs(11)).is_none());
        let retried = breaker.admit_at(start + Duration::from_secs(12));
        assert!(retried.is_some(), "expired probe did not unlock recovery");
        stalled.success_at(start + Duration::from_secs(12));
        assert!(breaker.admit_at(start + Duration::from_secs(12)).is_none());
    }

    #[test]
    fn productive_slow_probe_survives_concurrent_arrivals_and_recovers() {
        let breaker = circuit();
        let start = Instant::now();
        for round in 0..3 {
            let t = start + Duration::from_secs(round * 30);
            for _ in 0..3 {
                let mut ticket = breaker.admit_at(t).unwrap();
                ticket.failure_at(t);
            }
            let opened = t + Duration::from_millis(100);
            let mut probe = breaker.admit_at(opened).unwrap();
            for second in 1..=20 {
                probe.progress_at(opened + Duration::from_secs(second));
                if second == 11 {
                    assert!(
                        breaker
                            .admit_at(opened + Duration::from_secs(second))
                            .is_none()
                    );
                }
            }
            probe.success_at(opened + Duration::from_secs(20));
            assert!(breaker.admit_at(opened + Duration::from_secs(21)).is_some());
        }
    }

    #[test]
    fn endlessly_progressing_probe_still_has_hard_deadline_and_stale_fence() {
        let breaker = circuit();
        let start = Instant::now();
        for _ in 0..3 {
            let mut ticket = breaker.admit_at(start).unwrap();
            ticket.failure_at(start);
        }
        let opened = start + Duration::from_millis(100);
        let mut probe = breaker.admit_at(opened).unwrap();
        for second in 1..=120 {
            probe.progress_at(opened + Duration::from_secs(second));
        }
        assert!(
            breaker
                .admit_at(opened + Duration::from_secs(120))
                .is_none()
        );
        probe.success_at(opened + Duration::from_secs(121));
        assert!(
            breaker
                .admit_at(opened + Duration::from_secs(120) + Duration::from_millis(99))
                .is_none()
        );
        assert!(
            breaker
                .admit_at(opened + Duration::from_secs(120) + Duration::from_millis(100))
                .is_some()
        );
    }

    #[test]
    fn disabled_by_default_and_config_durations_are_validated() {
        assert!(
            Circuit::new(&CircuitBreakerConfig::default())
                .unwrap()
                .is_none()
        );
        for config in [
            CircuitBreakerConfig {
                failure_threshold: 0,
                ..Default::default()
            },
            CircuitBreakerConfig {
                failure_threshold: 33,
                ..Default::default()
            },
            CircuitBreakerConfig {
                cooldown: "0s".into(),
                ..Default::default()
            },
            CircuitBreakerConfig {
                max_cooldown: "1s".into(),
                cooldown: "2s".into(),
                ..Default::default()
            },
            CircuitBreakerConfig {
                max_cooldown: "2h".into(),
                ..Default::default()
            },
        ] {
            assert!(Circuit::new(&config).is_err());
        }
    }
    #[test]
    fn burst_of_one_hundred_twenty_eight_callers_allows_only_one_half_open_probe() {
        use std::sync::{
            Barrier,
            atomic::{AtomicUsize, Ordering},
        };
        use std::thread;
        let breaker = circuit();
        let now = Instant::now();
        for _ in 0..3 {
            let mut ticket = breaker.admit_at(now).unwrap();
            ticket.failure_at(now);
        }
        let barrier = Arc::new(Barrier::new(129));
        let probes = Arc::new(AtomicUsize::new(0));
        thread::scope(|scope| {
            for _ in 0..128 {
                let breaker = breaker.clone();
                let barrier = barrier.clone();
                let probes = probes.clone();
                scope.spawn(move || {
                    barrier.wait();
                    if let Some(_probe) = breaker.admit_at(now + Duration::from_millis(100)) {
                        probes.fetch_add(1, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(15));
                    }
                });
            }
            barrier.wait();
        });
        assert_eq!(probes.load(Ordering::SeqCst), 1);
        assert!(breaker.is_open());
        assert!(breaker.admit().is_none());
    }

    #[test]
    fn neutral_errors_do_not_trip_closed_breaker() {
        let breaker = circuit();
        let now = Instant::now();
        for _ in 0..128 {
            let ticket = breaker.admit_at(now).unwrap();
            drop(ticket);
        }
        assert!(!breaker.is_open());
        let mut ticket = breaker.admit_at(now).unwrap();
        ticket.success_at(now);
        assert!(breaker.admit_at(now).is_some());
    }
}
