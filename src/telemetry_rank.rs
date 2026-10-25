//! Approved, bounded telemetry hints. All SQLite reads run at startup or on
//! a background refresh; the request path only reads a short-lived snapshot.
use crate::{config::Config, routing::ResolvedStep, scoring, telemetry::Telemetry};
use rusqlite::{Connection, OpenFlags, params};
use std::{
    path::Path,
    sync::RwLock,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const MAX_SNAPSHOT_LIFETIME: Duration = Duration::from_secs(75);
const MIN_SAMPLES: i64 = 20;

#[derive(Debug)]
struct Snapshot {
    order: Vec<(String, String)>,
    loaded: Instant,
}

/// Never exposes SQLite or score-derived weights to an upstream model.
#[derive(Default)]
pub struct TelemetryRank {
    current: RwLock<Option<Snapshot>>,
}

impl TelemetryRank {
    pub fn new(order: Option<Vec<(String, String)>>) -> Self {
        let rank = Self::default();
        rank.set(order);
        rank
    }

    pub fn set(&self, order: Option<Vec<(String, String)>>) {
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = order.map(|order| Snapshot {
            order,
            loaded: Instant::now(),
        });
    }

    /// Fail open on stale/missing snapshots, changed candidates or a demoted
    /// live provider. Hard eligibility filtering always happens before this.
    pub fn rank(
        &self,
        config: &Config,
        steps: &mut [ResolvedStep],
        has_health_penalties: bool,
    ) -> bool {
        if has_health_penalties || steps.len() < 2 {
            return false;
        }
        let guard = self.current.read().unwrap_or_else(|e| e.into_inner());
        let Some(snapshot) = guard.as_ref() else {
            return false;
        };
        if snapshot.loaded.elapsed() > MAX_SNAPSHOT_LIFETIME {
            return false;
        }
        // An incomplete/mismatched snapshot must not change even one route.
        if steps.iter().any(|step| {
            let Some(provider) = config.providers.get(step.provider_index) else {
                return true;
            };
            !snapshot
                .order
                .iter()
                .any(|(name, model)| name == &provider.name && model == &step.model)
        }) {
            return false;
        }
        steps.sort_by_key(|step| {
            let name = &config.providers[step.provider_index].name;
            snapshot
                .order
                .iter()
                .position(|(provider, model)| provider == name && model == &step.model)
                .unwrap_or(usize::MAX)
        });
        true
    }

    pub fn active(&self) -> bool {
        self.current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|snapshot| snapshot.loaded.elapsed() <= MAX_SNAPSHOT_LIFETIME)
    }
}

pub fn refresh_interval() -> Duration {
    REFRESH_INTERVAL
}

/// The scorer's observed success/failure classes exclude caller errors,
/// key throttling and unverified SSE headers. Every configured candidate
/// needs at least 20 actual samples and a recent terminal observation.
/// Missing/corrupt/locked SQLite is a normal, safe fallback to static order.
pub fn load(data_dir: &Path, config: &Config) -> Option<Vec<(String, String)>> {
    let options = &config.auto.telemetry_ranking;
    if !options.enabled || !options.approved || config.auto.candidates.is_empty() {
        return None;
    }
    let summary = scoring::read_scoring(data_dir, options.window_hours).ok()?;
    if summary.get("enabled")?.as_bool() != Some(true) {
        return None;
    }
    let candidates = summary.get("candidates")?.as_array()?;
    let path = data_dir.join("telemetry").join("requests.sqlite3");
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
    conn.busy_timeout(Duration::from_millis(50)).ok()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs() as i64;
    let since = now.saturating_sub(options.max_snapshot_age_secs as i64);
    let mut scores = Vec::with_capacity(config.auto.candidates.len());
    for (index, candidate) in config.auto.candidates.iter().enumerate() {
        // Telemetry labels are bounded/sanitized. A changed or colliding label
        // cannot be trusted as an identity for automatic routing.
        if Telemetry::normalize_label(&candidate.provider) != candidate.provider
            || Telemetry::normalize_label(&candidate.model) != candidate.model
        {
            return None;
        }
        let record = candidates.iter().find(|row| {
            row["provider"].as_str() == Some(candidate.provider.as_str())
                && row["model"].as_str() == Some(candidate.model.as_str())
        })?;
        if record["qualified"].as_bool() != Some(true)
            || record["observed_samples"].as_i64()? < MIN_SAMPLES
        {
            return None;
        }
        let score = record["observed_success_rate"].as_f64()?;
        if !score.is_finite() || !(0.0..=1.0).contains(&score) {
            return None;
        }
        let recent: Option<i64> = conn
            .query_row(
                "SELECT MAX(ts) FROM request_events
             WHERE provider=?1 AND model=?2 AND ts BETWEEN ?3 AND ?4
             AND failure_type IN ('success','provider_5xx','transport_error',
                 'invalid_response','timeout','upstream_unavailable')",
                params![candidate.provider, candidate.model, since, now],
                |row| row.get(0),
            )
            .ok()?;
        recent?;
        scores.push((index, score));
    }
    scores.sort_by(|(index_a, score_a), (index_b, score_b)| {
        score_b
            .total_cmp(score_a)
            .then_with(|| index_a.cmp(index_b))
    });
    Some(
        scores
            .into_iter()
            .map(|(index, _)| {
                let candidate = &config.auto.candidates[index];
                (candidate.provider.clone(), candidate.model.clone())
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        let text = r#"
providers:
  - name: p
    type: openai
    base_url: https://upstream.example
    key_env: K
auto:
  enabled: true
  candidates:
    - provider: p
      model: first
    - provider: p
      model: second
  telemetry_ranking:
    enabled: true
    approved: true
    window_hours: 1
    max_snapshot_age_secs: 300
"#;
        let cfg: Config = serde_yaml::from_str(text).unwrap();
        cfg.validate().unwrap();
        cfg
    }

    #[test]
    fn approval_and_bounds_are_required() {
        let mut cfg = config();
        cfg.auto.telemetry_ranking.approved = false;
        assert!(cfg.validate().is_err());
        assert!(load(Path::new("."), &cfg).is_none());
        cfg.auto.telemetry_ranking.approved = true;
        cfg.auto.telemetry_ranking.window_hours = 2;
        assert!(cfg.validate().is_err());
        cfg.auto.telemetry_ranking.window_hours = 1;
        cfg.auto.telemetry_ranking.max_snapshot_age_secs = 1;
        assert!(cfg.validate().is_err());
        cfg.auto.telemetry_ranking.max_snapshot_age_secs = 300;
        cfg.auto.decider.enabled = true;
        cfg.auto.decider.provider = "p".into();
        cfg.auto.decider.model = "ranker".into();
        assert!(cfg.validate().is_err(), "two active rankers must not race");
        cfg.auto.decider.shadow_only = true;
        assert!(cfg.validate().is_ok(), "advisory shadow may coexist");
    }

    #[test]
    fn requires_all_candidates_fresh_and_qualified_before_reordering() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("telemetry")).unwrap();
        let path = dir.path().join("telemetry/requests.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE hourly_stats (hour_ts INTEGER,endpoint TEXT,provider TEXT,
              model TEXT,failure_type TEXT,request_count INTEGER,failure_count INTEGER,
              total_elapsed_ms INTEGER,max_elapsed_ms INTEGER);
             CREATE TABLE request_events (ts INTEGER,provider TEXT,model TEXT,
              elapsed_ms INTEGER,sse_headers INTEGER,failure_type TEXT);",
        )
        .unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let hour = now - now.rem_euclid(3600);
        for (model, successes, failures) in [("first", 4, 16), ("second", 19, 1)] {
            for (kind, count) in [("success", successes), ("provider_5xx", failures)] {
                conn.execute(
                    "INSERT INTO hourly_stats VALUES (?1,'chat','p',?2,?3,?4,?5,?6,?7)",
                    params![
                        hour,
                        model,
                        kind,
                        count,
                        if kind == "success" { 0 } else { count },
                        100 * count,
                        100
                    ],
                )
                .unwrap();
            }
            conn.execute(
                "INSERT INTO request_events VALUES (?1,'p',?2,10,0,'success')",
                params![now, model],
            )
            .unwrap();
        }
        let cfg = config();
        let order = load(dir.path(), &cfg).unwrap();
        assert_eq!(order[0].1, "second");
        let rank = TelemetryRank::new(Some(order));
        let mut steps = vec![
            ResolvedStep {
                provider_index: 0,
                model: "first".into(),
                params: Default::default(),
            },
            ResolvedStep {
                provider_index: 0,
                model: "second".into(),
                params: Default::default(),
            },
        ];
        assert!(rank.rank(&cfg, &mut steps, false));
        assert_eq!(steps[0].model, "second");
        steps.reverse();
        assert!(!rank.rank(&cfg, &mut steps, true));
        assert_eq!(steps[0].model, "first");
        // A refresh task that dies or stalls cannot leave old approval live.
        {
            let mut guard = rank.current.write().unwrap();
            guard.as_mut().unwrap().loaded =
                Instant::now() - MAX_SNAPSHOT_LIFETIME - Duration::from_secs(1);
        }
        assert!(!rank.active());
        assert!(!rank.rank(&cfg, &mut steps, false));
        rank.set(load(dir.path(), &cfg));
        assert!(rank.active());
        conn.execute(
            "UPDATE request_events SET ts=?1 WHERE model='second'",
            params![now - 1000],
        )
        .unwrap();
        assert!(load(dir.path(), &cfg).is_none());
        conn.execute(
            "UPDATE request_events SET ts=?1 WHERE model='second'",
            params![now],
        )
        .unwrap();
        conn.execute("UPDATE hourly_stats SET request_count=1 WHERE model='second' AND failure_type='success'",[]).unwrap();
        assert!(load(dir.path(), &cfg).is_none());
        rank.set(None);
        assert!(!rank.active());
        assert!(!rank.rank(&cfg, &mut steps, false));
    }

    #[test]
    fn missing_or_malformed_database_fails_open() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load(dir.path(), &config()).is_none());
        std::fs::create_dir(dir.path().join("telemetry")).unwrap();
        std::fs::write(
            dir.path().join("telemetry/requests.sqlite3"),
            b"not a sqlite database",
        )
        .unwrap();
        assert!(load(dir.path(), &config()).is_none());
    }
}
