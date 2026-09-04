// SPDX-License-Identifier: MIT

use chrono::Timelike;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{debug, error};

pub const DEFAULT_PRIOR_ALPHA: f64 = 1.0; // Neutral prior successes (50% mean with beta=1)
pub const DEFAULT_PRIOR_BETA: f64 = 1.0; // Prior failures
pub const DEFAULT_SHRINKAGE_WEIGHT: f64 = 3.0; // Weight of global prior when estimating hourly rate
pub const DEFAULT_MAX_SAMPLES: f64 = 20.0; // Hard ceiling on effective sample weight per bucket
pub const DEFAULT_HALF_LIFE_SECS: f64 = 7.0 * 24.0 * 3600.0; // 7-day half-life for time decay
pub const DEFAULT_BURST_WINDOW_SECS: i64 = 15; // Consecutive requests within 15s are damped

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatCounts {
    #[serde(default)]
    pub successes: f64,
    #[serde(default)]
    pub failures: f64,
    #[serde(default)]
    pub last_updated_sec: i64,
    #[serde(default)]
    pub burst_count: u32,
}

impl Default for StatCounts {
    fn default() -> Self {
        Self {
            successes: 0.0,
            failures: 0.0,
            last_updated_sec: 0,
            burst_count: 0,
        }
    }
}

impl StatCounts {
    #[inline]
    pub fn total(&self) -> f64 {
        self.successes + self.failures
    }

    /// Applies exponential time-decay: Weight(dt) = 2^(-dt / half_life).
    /// If counts decay below 0.01, they are pruned to 0.0.
    pub fn decay_to_time(&mut self, now_sec: i64, half_life_secs: f64) {
        if self.last_updated_sec <= 0 || now_sec <= self.last_updated_sec || half_life_secs <= 0.0 {
            return;
        }

        let dt = (now_sec - self.last_updated_sec) as f64;
        let factor = 2.0f64.powf(-dt / half_life_secs);

        self.successes *= factor;
        self.failures *= factor;

        if self.successes < 0.01 {
            self.successes = 0.0;
        }
        if self.failures < 0.01 {
            self.failures = 0.0;
        }

        self.last_updated_sec = now_sec;
    }

    /// Returns decayed (successes, failures) evaluated at `now_sec` without mutating self.
    pub fn decayed_counts_at(&self, now_sec: i64, half_life_secs: f64) -> (f64, f64) {
        if self.last_updated_sec <= 0 || now_sec <= self.last_updated_sec || half_life_secs <= 0.0 {
            return (self.successes, self.failures);
        }

        let dt = (now_sec - self.last_updated_sec) as f64;
        let factor = 2.0f64.powf(-dt / half_life_secs);

        let s = if self.successes * factor < 0.01 { 0.0 } else { self.successes * factor };
        let f = if self.failures * factor < 0.01 { 0.0 } else { self.failures * factor };

        (s, f)
    }

    /// Records a success with burst damping and sample capacity capping.
    /// Consecutive requests within `burst_window_secs` have diminishing marginal returns
    /// to prevent rapid-fire requests in a single agent turn from artificially inflating the score.
    pub fn record_success(
        &mut self,
        now_sec: i64,
        half_life_secs: f64,
        burst_window_secs: i64,
        max_samples: f64,
    ) {
        // Calculate dt before decay_to_time updates last_updated_sec
        let dt = if self.last_updated_sec > 0 {
            now_sec - self.last_updated_sec
        } else {
            burst_window_secs + 1
        };

        self.decay_to_time(now_sec, half_life_secs);

        // Burst damping: consecutive requests within burst_window_secs scale down
        let increment = if dt <= burst_window_secs && dt >= 0 {
            self.burst_count = self.burst_count.saturating_add(1);
            // 1st burst: 1 / (1 + 0.5 * 1) = 0.67, 2nd: 0.5, 3rd: 0.4 ...
            1.0 / (1.0 + 0.5 * (self.burst_count as f64))
        } else {
            self.burst_count = 0;
            1.0
        };

        self.successes += increment;
        self.last_updated_sec = now_sec;

        // Apply capacity ceiling (saturation cap)
        self.enforce_cap(max_samples);
    }

    /// Records a failure. Resets burst counter and adds 1.0 failure.
    pub fn record_failure(&mut self, now_sec: i64, half_life_secs: f64, max_samples: f64) {
        self.decay_to_time(now_sec, half_life_secs);
        self.burst_count = 0;
        self.failures += 1.0;
        self.last_updated_sec = now_sec;
        self.enforce_cap(max_samples);
    }

    fn enforce_cap(&mut self, max_samples: f64) {
        let total = self.total();
        if total > max_samples && total > 0.0 {
            let scale = max_samples / total;
            self.successes *= scale;
            self.failures *= scale;
        }
    }
}

fn default_hourly() -> [StatCounts; 24] {
    std::array::from_fn(|_| StatCounts::default())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeStats {
    #[serde(default)]
    pub overall: StatCounts,
    #[serde(default = "default_hourly")]
    pub hourly: [StatCounts; 24],
}

impl Default for NodeStats {
    fn default() -> Self {
        Self {
            overall: StatCounts::default(),
            hourly: default_hourly(),
        }
    }
}

impl NodeStats {
    pub fn record_success(
        &mut self,
        hour: u8,
        now_sec: i64,
        half_life_secs: f64,
        burst_window_secs: i64,
        max_samples: f64,
    ) {
        self.overall.record_success(now_sec, half_life_secs, burst_window_secs, max_samples);
        let h = (hour as usize) % 24;
        self.hourly[h].record_success(now_sec, half_life_secs, burst_window_secs, max_samples);
    }

    pub fn record_failure(
        &mut self,
        hour: u8,
        now_sec: i64,
        half_life_secs: f64,
        max_samples: f64,
    ) {
        self.overall.record_failure(now_sec, half_life_secs, max_samples);
        let h = (hour as usize) % 24;
        self.hourly[h].record_failure(now_sec, half_life_secs, max_samples);
    }

    /// Calculates the reliability score in range (0.0, 1.0) for a specific hour at `now_sec`.
    /// Uses hierarchical Empirical Bayes shrinkage with weakly optimistic prior.
    pub fn calculate_score(
        &self,
        hour: u8,
        now_sec: i64,
        half_life_secs: f64,
        prior_alpha: f64,
        prior_beta: f64,
        shrinkage_weight: f64,
    ) -> f64 {
        // 1. Decayed global prior rate for this node
        let (overall_s, overall_f) = self.overall.decayed_counts_at(now_sec, half_life_secs);
        let overall_total = overall_s + overall_f;
        let global_prior = (overall_s + prior_alpha) / (overall_total + prior_alpha + prior_beta);

        // 2. Decayed hourly score via Empirical Bayes shrinkage towards global prior
        let h = (hour as usize) % 24;
        let (hour_s, hour_f) = self.hourly[h].decayed_counts_at(now_sec, half_life_secs);
        let hour_total = hour_s + hour_f;

        (hour_s + shrinkage_weight * global_prior) / (hour_total + shrinkage_weight)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatsFile {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub nodes: HashMap<String, NodeStats>,
}

fn default_version() -> u32 {
    1
}

impl Default for StatsFile {
    fn default() -> Self {
        Self {
            version: 1,
            updated_at: chrono::Local::now().to_rfc3339(),
            nodes: HashMap::new(),
        }
    }
}

pub struct StatsManager {
    conn: Mutex<Option<Connection>>,
    file_path: Option<PathBuf>,
    enabled: bool,
    max_samples: f64,
    half_life_secs: f64,
    burst_window_secs: i64,
}

impl StatsManager {
    pub fn new(
        file_path: Option<PathBuf>,
        enabled: bool,
        max_samples: f64,
        half_life_secs: f64,
        burst_window_secs: i64,
    ) -> Arc<Self> {
        let conn = if enabled {
            match &file_path {
                Some(path) => {
                    if let Some(parent) = path.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    match Connection::open(path) {
                        Ok(c) => {
                            if let Err(e) = Self::setup_database(&c) {
                                error!("Failed to setup SQLite database at {}: {}", path.display(), e);
                            }
                            Some(c)
                        }
                        Err(e) => {
                            error!("Failed to open SQLite database at {}: {}", path.display(), e);
                            None
                        }
                    }
                }
                None => match Connection::open_in_memory() {
                    Ok(c) => {
                        let _ = Self::setup_database(&c);
                        Some(c)
                    }
                    Err(e) => {
                        error!("Failed to open in-memory SQLite database: {}", e);
                        None
                    }
                },
            }
        } else {
            None
        };

        Arc::new(Self {
            conn: Mutex::new(conn),
            file_path,
            enabled,
            max_samples,
            half_life_secs,
            burst_window_secs,
        })
    }

    fn setup_database(conn: &Connection) -> rusqlite::Result<()> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;
             CREATE TABLE IF NOT EXISTS meta (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS node_stats (
                 node_name TEXT NOT NULL,
                 hour INTEGER NOT NULL,
                 successes REAL NOT NULL DEFAULT 0.0,
                 failures REAL NOT NULL DEFAULT 0.0,
                 last_updated_sec INTEGER NOT NULL DEFAULT 0,
                 burst_count INTEGER NOT NULL DEFAULT 0,
                 PRIMARY KEY (node_name, hour)
             );"
        )?;
        Ok(())
    }


    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Gets current local hour (0..=23)
    #[inline]
    pub fn current_hour() -> u8 {
        chrono::Local::now().hour() as u8
    }

    /// Gets current unix timestamp in seconds
    #[inline]
    pub fn now_sec() -> i64 {
        chrono::Utc::now().timestamp()
    }

    fn fetch_counts(conn: &Connection, node: &str, hour: i32) -> rusqlite::Result<StatCounts> {
        let mut stmt = conn.prepare_cached(
            "SELECT successes, failures, last_updated_sec, burst_count
             FROM node_stats WHERE node_name = ?1 AND hour = ?2",
        )?;
        let mut rows = stmt.query(params![node, hour])?;
        if let Some(row) = rows.next()? {
            Ok(StatCounts {
                successes: row.get(0)?,
                failures: row.get(1)?,
                last_updated_sec: row.get(2)?,
                burst_count: row.get(3)?,
            })
        } else {
            Ok(StatCounts::default())
        }
    }

    fn save_counts(conn: &Connection, node: &str, hour: i32, counts: &StatCounts) -> rusqlite::Result<()> {
        let mut stmt = conn.prepare_cached(
            "INSERT INTO node_stats (node_name, hour, successes, failures, last_updated_sec, burst_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(node_name, hour) DO UPDATE SET
               successes = excluded.successes,
               failures = excluded.failures,
               last_updated_sec = excluded.last_updated_sec,
               burst_count = excluded.burst_count",
        )?;
        stmt.execute(params![
            node,
            hour,
            counts.successes,
            counts.failures,
            counts.last_updated_sec,
            counts.burst_count,
        ])?;
        Ok(())
    }

    fn update_node_record(
        conn: &Connection,
        node: &str,
        hour: i32,
        now: i64,
        half_life_secs: f64,
        burst_window_secs: i64,
        max_samples: f64,
        is_success: bool,
    ) -> rusqlite::Result<()> {
        let tx = conn.unchecked_transaction()?;

        let mut overall = Self::fetch_counts(&tx, node, -1)?;
        let mut hourly = Self::fetch_counts(&tx, node, hour)?;

        if is_success {
            overall.record_success(now, half_life_secs, burst_window_secs, max_samples);
            hourly.record_success(now, half_life_secs, burst_window_secs, max_samples);
        } else {
            overall.record_failure(now, half_life_secs, max_samples);
            hourly.record_failure(now, half_life_secs, max_samples);
        }

        Self::save_counts(&tx, node, -1, &overall)?;
        Self::save_counts(&tx, node, hour, &hourly)?;

        let now_str = chrono::Local::now().to_rfc3339();
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('updated_at', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![now_str],
        )?;

        tx.commit()?;

        debug!(
            "Recorded {} for node [{}] in hour {}. (hourly: {:.2}/{:.2}, overall: {:.2}/{:.2})",
            if is_success { "SUCCESS" } else { "FAILURE" },
            node,
            hour,
            hourly.successes,
            hourly.total(),
            overall.successes,
            overall.total()
        );
        Ok(())
    }

    /// Records a success for the specified node in current local hour.
    pub fn record_success(&self, node: &str, hour: u8) {
        if !self.enabled || node.is_empty() {
            return;
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return,
        };

        let now = Self::now_sec();
        let h = (hour as i32) % 24;

        if let Err(e) = Self::update_node_record(
            conn,
            node,
            h,
            now,
            self.half_life_secs,
            self.burst_window_secs,
            self.max_samples,
            true,
        ) {
            error!("Failed to record success for node [{}] to SQLite: {}", node, e);
        }
    }

    /// Records a failure for the specified node in current local hour.
    pub fn record_failure(&self, node: &str, hour: u8) {
        if !self.enabled || node.is_empty() {
            return;
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return,
        };

        let now = Self::now_sec();
        let h = (hour as i32) % 24;

        if let Err(e) = Self::update_node_record(
            conn,
            node,
            h,
            now,
            self.half_life_secs,
            self.burst_window_secs,
            self.max_samples,
            false,
        ) {
            error!("Failed to record failure for node [{}] to SQLite: {}", node, e);
        }
    }

    /// Ranks candidate nodes for the given hour by their reliability score descending.
    /// Returns a list of (node_name, score, hourly_counts, overall_counts).
    pub fn rank_nodes(
        &self,
        hour: u8,
        candidates: &[String],
    ) -> Vec<(String, f64, StatCounts, StatCounts)> {
        if candidates.is_empty() {
            return Vec::new();
        }

        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => {
                return candidates
                    .iter()
                    .map(|c| (c.clone(), 0.5, StatCounts::default(), StatCounts::default()))
                    .collect();
            }
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => {
                return candidates
                    .iter()
                    .map(|c| (c.clone(), 0.5, StatCounts::default(), StatCounts::default()))
                    .collect();
            }
        };

        let now = Self::now_sec();
        let h = (hour as i32) % 24;

        let mut ranked: Vec<(String, f64, StatCounts, StatCounts)> = candidates
            .iter()
            .map(|node| {
                let overall = Self::fetch_counts(conn, node, -1).unwrap_or_default();
                let hourly = Self::fetch_counts(conn, node, h).unwrap_or_default();

                if overall.total() > 0.0 || hourly.total() > 0.0 {
                    let mut node_stats = NodeStats {
                        overall: overall.clone(),
                        hourly: default_hourly(),
                    };
                    node_stats.hourly[h as usize] = hourly.clone();

                    let score = node_stats.calculate_score(
                        hour,
                        now,
                        self.half_life_secs,
                        DEFAULT_PRIOR_ALPHA,
                        DEFAULT_PRIOR_BETA,
                        DEFAULT_SHRINKAGE_WEIGHT,
                    );
                    let (hs, hf) = hourly.decayed_counts_at(now, self.half_life_secs);
                    let (os, of) = overall.decayed_counts_at(now, self.half_life_secs);
                    let hourly_c = StatCounts {
                        successes: hs,
                        failures: hf,
                        last_updated_sec: hourly.last_updated_sec,
                        burst_count: hourly.burst_count,
                    };
                    let overall_c = StatCounts {
                        successes: os,
                        failures: of,
                        last_updated_sec: overall.last_updated_sec,
                        burst_count: overall.burst_count,
                    };
                    (node.clone(), score, hourly_c, overall_c)
                } else {
                    let score = DEFAULT_PRIOR_ALPHA / (DEFAULT_PRIOR_ALPHA + DEFAULT_PRIOR_BETA);
                    (
                        node.clone(),
                        score,
                        StatCounts::default(),
                        StatCounts::default(),
                    )
                }
            })
            .collect();

        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        ranked
    }

    /// Selects the best candidate node for the given hour, excluding any nodes in `excluded`.
    /// If all candidate nodes are excluded, excludes only the first element (usually `now`)
    /// to avoid getting stuck on a dead node.
    pub fn select_best_node(
        &self,
        hour: u8,
        candidates: &[String],
        excluded: &[String],
    ) -> Option<String> {
        if candidates.is_empty() {
            return None;
        }

        let ranked = self.rank_nodes(hour, candidates);

        // 1. Try to find the top node that is not excluded
        for (node, _, _, _) in &ranked {
            if !excluded.iter().any(|ex| ex == node) {
                return Some(node.clone());
            }
        }

        // 2. All candidates were in excluded: fallback to excluding only the currently failing node (if present)
        let current_failing = excluded.first();
        for (node, _, _, _) in &ranked {
            if Some(node) != current_failing {
                return Some(node.clone());
            }
        }

        // 3. Fallback to top-ranked node overall
        ranked.first().map(|(n, _, _, _)| n.clone())
    }

    /// Checkpoints SQLite WAL data. Retained for backward compatibility.
    pub fn flush(&self) {
        if !self.enabled {
            return;
        }
        if let Ok(guard) = self.conn.lock() {
            if let Some(ref conn) = *guard {
                let _ = conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);");
                if let Some(ref path) = self.file_path {
                    debug!("Checkpointed SQLite WAL for {}", path.display());
                }
            }
        }
    }

    /// Snapshot copy of current statistics from SQLite for inspection/printing
    pub fn snapshot(&self) -> StatsFile {
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return StatsFile::default(),
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return StatsFile::default(),
        };

        let updated_at: String = conn
            .query_row("SELECT value FROM meta WHERE key = 'updated_at'", [], |r| r.get(0))
            .unwrap_or_else(|_| chrono::Local::now().to_rfc3339());

        let mut stmt = match conn.prepare(
            "SELECT node_name, hour, successes, failures, last_updated_sec, burst_count FROM node_stats"
        ) {
            Ok(s) => s,
            Err(_) => return StatsFile::default(),
        };

        let mut nodes: HashMap<String, NodeStats> = HashMap::new();
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i32>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, u32>(5)?,
            ))
        });

        if let Ok(rows) = rows {
            for item in rows.flatten() {
                let (node_name, hour, successes, failures, last_updated_sec, burst_count) = item;
                let entry = nodes.entry(node_name).or_default();
                let counts = StatCounts {
                    successes,
                    failures,
                    last_updated_sec,
                    burst_count,
                };
                if hour == -1 {
                    entry.overall = counts;
                } else if (0..24).contains(&hour) {
                    entry.hourly[hour as usize] = counts;
                }
            }
        }

        StatsFile {
            version: 1,
            updated_at,
            nodes,
        }
    }
}

/// Resolves the default path for `gyro.db`.
/// Prioritizes Antigravity CLI's configuration directory:
/// 1. `AGY_GYRO_STATS_FILE` env var if set.
/// 2. `%USERPROFILE%\.gemini\antigravity-cli\gyro.db` (Windows)
///    or `~/.gemini/antigravity-cli/gyro.db` (Unix).
/// 3. Fallback: `~/.agy-gyro/gyro.db`.
pub fn resolve_default_stats_path() -> PathBuf {
    if let Ok(env_path) = std::env::var("AGY_GYRO_STATS_FILE") {
        if !env_path.trim().is_empty() {
            return PathBuf::from(env_path);
        }
    }

    let home_dir: Option<PathBuf> = std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from);

    if let Some(home) = home_dir {
        // Preferred location: inside agy's configuration folder
        let agy_dir = home.join(".gemini").join("antigravity-cli");
        if agy_dir.is_dir() {
            return agy_dir.join("gyro.db");
        }
        // If agy directory doesn't exist yet, we still prefer putting it there if ~/.gemini exists
        let gemini_dir = home.join(".gemini");
        if gemini_dir.is_dir() {
            return agy_dir.join("gyro.db");
        }

        // Fallback to agy folder path directly so it will be created there
        return agy_dir.join("gyro.db");
    }

    std::env::temp_dir().join("gyro.db")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_burst_damping_limits_rapid_growth() {
        let mut counts = StatCounts::default();
        let start_time = 1000000;
        let half_life = 7.0 * 86400.0;
        let burst_window = 15;
        let max_samples = 20.0;

        // 30 rapid-fire requests in 5 seconds (interval = 0 or 1 sec)
        for i in 0..30 {
            let t = start_time + (i / 6); // 0 to 5 seconds
            counts.record_success(t, half_life, burst_window, max_samples);
        }

        // Due to burst damping (1/(1 + 0.5*burst)), 30 rapid-fire requests should NOT add 30.0!
        // It should be damped significantly below 10.0 effective samples.
        println!("Effective successes after 30 rapid requests: {:.2}", counts.successes);
        assert!(counts.successes < 10.0);
        assert!(counts.successes > 3.0);
    }

    #[test]
    fn test_half_life_time_decay() {
        let mut counts = StatCounts::default();
        let t0 = 1000000;
        let half_life = 7.0 * 86400.0; // 7 days in seconds

        counts.record_success(t0, half_life, 15, 20.0);
        counts.record_success(t0 + 20, half_life, 15, 20.0);
        assert!((counts.total() - 2.0).abs() < 1e-2);

        // Advance 7 days exactly (1 half-life)
        let t7 = t0 + 20 + (7 * 86400);
        let (s7, f7) = counts.decayed_counts_at(t7, half_life);
        assert!((s7 - 1.0).abs() < 1e-2, "After 1 half-life, 2.0 should decay to ~1.0");
        assert_eq!(f7, 0.0);

        // Advance 28 days (4 half-lives)
        let t28 = t0 + 20 + (28 * 86400);
        let (s28, _) = counts.decayed_counts_at(t28, half_life);
        // 2.0 * (1/16) = 0.125
        assert!((s28 - 0.125).abs() < 1e-2, "After 4 half-lives, should decay to ~0.125");
    }

    #[test]
    fn test_bayesian_single_failure_robustness() {
        let mut stats = NodeStats::default();
        let hour = 14;
        let t0 = 1000000;
        let half_life = 7.0 * 86400.0;

        // Brand new node: prior score 1/(1+1) = 0.50
        let initial_score = stats.calculate_score(
            hour,
            t0,
            half_life,
            DEFAULT_PRIOR_ALPHA,
            DEFAULT_PRIOR_BETA,
            DEFAULT_SHRINKAGE_WEIGHT,
        );
        assert!((initial_score - 0.50).abs() < 1e-6);

        // Record 15 spaced successes (e.g. every 20 seconds)
        for i in 0..15 {
            stats.record_success(hour, t0 + i * 20, half_life, 15, 20.0);
        }
        let score_after_success = stats.calculate_score(
            hour,
            t0 + 300,
            half_life,
            DEFAULT_PRIOR_ALPHA,
            DEFAULT_PRIOR_BETA,
            DEFAULT_SHRINKAGE_WEIGHT,
        );
        assert!(score_after_success > 0.85);

        // Now record ONE failure
        stats.record_failure(hour, t0 + 320, half_life, 20.0);
        let score_after_1_failure = stats.calculate_score(
            hour,
            t0 + 320,
            half_life,
            DEFAULT_PRIOR_ALPHA,
            DEFAULT_PRIOR_BETA,
            DEFAULT_SHRINKAGE_WEIGHT,
        );

        // One failure only drops score smoothly, remaining high
        assert!(score_after_1_failure > 0.80);
        assert!(score_after_1_failure < score_after_success);
        println!(
            "Score after successes: {:.4}, after 1 failure: {:.4}",
            score_after_success, score_after_1_failure
        );
    }

    #[test]
    fn test_ranking_with_session_exclusions() {
        let manager = StatsManager::new(None, true, 20.0, 7.0 * 86400.0, 15);
        let hour = 21;

        // Node A: reliable
        for _ in 0..10 {
            manager.record_success("node-a", hour);
        }
        // Node B: decent
        for _ in 0..5 {
            manager.record_success("node-b", hour);
        }
        manager.record_failure("node-b", hour);
        // Node C: broken (5 failures)
        for _ in 0..5 {
            manager.record_failure("node-c", hour);
        }
        // Node D: brand new

        let candidates = vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string(),
            "node-d".to_string(),
        ];

        let ranked = manager.rank_nodes(hour, &candidates);
        assert_eq!(ranked[0].0, "node-a"); // Top
        assert_eq!(ranked[1].0, "node-b"); // Second
        assert_eq!(ranked[2].0, "node-d"); // New node with prior (75%)
        assert_eq!(ranked[3].0, "node-c"); // Broken node

        // Exclude node-a -> chooses node-b
        let best = manager.select_best_node(hour, &candidates, &["node-a".to_string()]);
        assert_eq!(best, Some("node-b".to_string()));

        // Exclude node-a and node-b -> chooses node-d
        let best2 = manager.select_best_node(hour, &candidates, &["node-a".to_string(), "node-b".to_string()]);
        assert_eq!(best2, Some("node-d".to_string()));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let temp_dir = std::env::temp_dir().join(format!("gyro-test-{}", rand::random::<u32>()));
        let _ = std::fs::create_dir_all(&temp_dir);
        let file_path = temp_dir.join("stats.db");

        let manager = StatsManager::new(Some(file_path.clone()), true, 20.0, 7.0 * 86400.0, 15);
        manager.record_success("node-test", 10);
        manager.record_failure("node-test", 10);
        manager.flush();

        // Load again from disk
        let manager2 = StatsManager::new(Some(file_path), true, 20.0, 7.0 * 86400.0, 15);
        let snap = manager2.snapshot();
        let node_stats = snap.nodes.get("node-test").expect("node-test should exist");
        assert!(node_stats.overall.successes > 0.0);
        assert!(node_stats.overall.failures > 0.0);
        assert!(node_stats.hourly[10].successes > 0.0);
        assert!(node_stats.hourly[10].failures > 0.0);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
