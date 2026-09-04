// SPDX-License-Identifier: MIT

use chrono::Timelike;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tracing::{debug, error, info, warn};

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
    data: RwLock<StatsFile>,
    file_path: Option<PathBuf>,
    dirty: AtomicBool,
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
        let stats_file = if enabled {
            if let Some(ref path) = file_path {
                Self::load_from_disk(path).unwrap_or_else(|e| {
                    warn!(
                        "Failed to load stats from {}: {}. Initializing empty stats.",
                        path.display(),
                        e
                    );
                    StatsFile::default()
                })
            } else {
                StatsFile::default()
            }
        } else {
            StatsFile::default()
        };

        Arc::new(Self {
            data: RwLock::new(stats_file),
            file_path,
            dirty: AtomicBool::new(false),
            enabled,
            max_samples,
            half_life_secs,
            burst_window_secs,
        })
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

    /// Records a success for the specified node in current local hour.
    pub fn record_success(&self, node: &str, hour: u8) {
        if !self.enabled || node.is_empty() {
            return;
        }
        let now = Self::now_sec();
        if let Ok(mut guard) = self.data.write() {
            let (h_s, h_tot, o_s, o_tot) = {
                let entry = guard.nodes.entry(node.to_string()).or_default();
                entry.record_success(
                    hour,
                    now,
                    self.half_life_secs,
                    self.burst_window_secs,
                    self.max_samples,
                );
                (
                    entry.hourly[(hour as usize) % 24].successes,
                    entry.hourly[(hour as usize) % 24].total(),
                    entry.overall.successes,
                    entry.overall.total(),
                )
            };
            guard.updated_at = chrono::Local::now().to_rfc3339();
            self.dirty.store(true, Ordering::Release);
            debug!(
                "Recorded SUCCESS for node [{}] in hour {}. (hourly: {:.2}/{:.2}, overall: {:.2}/{:.2})",
                node, hour, h_s, h_tot, o_s, o_tot
            );
        }
    }

    /// Records a failure for the specified node in current local hour.
    pub fn record_failure(&self, node: &str, hour: u8) {
        if !self.enabled || node.is_empty() {
            return;
        }
        let now = Self::now_sec();
        if let Ok(mut guard) = self.data.write() {
            let (h_s, h_tot, o_s, o_tot) = {
                let entry = guard.nodes.entry(node.to_string()).or_default();
                entry.record_failure(hour, now, self.half_life_secs, self.max_samples);
                (
                    entry.hourly[(hour as usize) % 24].successes,
                    entry.hourly[(hour as usize) % 24].total(),
                    entry.overall.successes,
                    entry.overall.total(),
                )
            };
            guard.updated_at = chrono::Local::now().to_rfc3339();
            self.dirty.store(true, Ordering::Release);
            debug!(
                "Recorded FAILURE for node [{}] in hour {}. (hourly: {:.2}/{:.2}, overall: {:.2}/{:.2})",
                node, hour, h_s, h_tot, o_s, o_tot
            );
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

        let now = Self::now_sec();
        let guard = match self.data.read() {
            Ok(g) => g,
            Err(_) => {
                return candidates
                    .iter()
                    .map(|c| {
                        (
                            c.clone(),
                            0.5,
                            StatCounts::default(),
                            StatCounts::default(),
                        )
                    })
                    .collect();
            }
        };

        let mut ranked: Vec<(String, f64, StatCounts, StatCounts)> = candidates
            .iter()
            .map(|node| {
                if let Some(stats) = guard.nodes.get(node) {
                    let score = stats.calculate_score(
                        hour,
                        now,
                        self.half_life_secs,
                        DEFAULT_PRIOR_ALPHA,
                        DEFAULT_PRIOR_BETA,
                        DEFAULT_SHRINKAGE_WEIGHT,
                    );
                    let (hs, hf) = stats.hourly[(hour as usize) % 24]
                        .decayed_counts_at(now, self.half_life_secs);
                    let (os, of) = stats.overall.decayed_counts_at(now, self.half_life_secs);
                    let hourly_c = StatCounts {
                        successes: hs,
                        failures: hf,
                        last_updated_sec: stats.hourly[(hour as usize) % 24].last_updated_sec,
                        burst_count: stats.hourly[(hour as usize) % 24].burst_count,
                    };
                    let overall_c = StatCounts {
                        successes: os,
                        failures: of,
                        last_updated_sec: stats.overall.last_updated_sec,
                        burst_count: stats.overall.burst_count,
                    };
                    (node.clone(), score, hourly_c, overall_c)
                } else {
                    // New unobserved node: prior score
                    let score =
                        DEFAULT_PRIOR_ALPHA / (DEFAULT_PRIOR_ALPHA + DEFAULT_PRIOR_BETA);
                    (
                        node.clone(),
                        score,
                        StatCounts::default(),
                        StatCounts::default(),
                    )
                }
            })
            .collect();

        // Sort descending by score. Tie-break by node name for determinism.
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

    /// Flushes stats to disk if marked dirty.
    pub fn flush(&self) {
        if !self.enabled {
            return;
        }
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return;
        }

        if let Some(ref path) = self.file_path {
            if let Ok(guard) = self.data.read() {
                if let Err(e) = Self::write_to_disk(path, &guard) {
                    error!("Failed to write stats to {}: {}", path.display(), e);
                    self.dirty.store(true, Ordering::Release);
                } else {
                    info!(
                        "Successfully persisted node reliability stats to {}",
                        path.display()
                    );
                }
            }
        }
    }

    fn load_from_disk(path: &Path) -> std::io::Result<StatsFile> {
        if !path.exists() {
            return Ok(StatsFile::default());
        }
        let content = std::fs::read_to_string(path)?;
        let parsed: StatsFile = serde_json::from_str(&content).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        Ok(parsed)
    }

    fn write_to_disk(path: &Path, stats: &StatsFile) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json_str = serde_json::to_string_pretty(stats)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        std::fs::write(path, json_str)
    }

    /// Snapshot copy of current StatsFile for inspection/printing
    pub fn snapshot(&self) -> StatsFile {
        self.data.read().map(|g| g.clone()).unwrap_or_default()
    }
}

/// Resolves the default path for `gyro-stats.json`.
/// Prioritizes Antigravity CLI's configuration directory:
/// 1. `AGY_GYRO_STATS_FILE` env var if set.
/// 2. `%USERPROFILE%\.gemini\antigravity-cli\gyro-stats.json` (Windows)
///    or `~/.gemini/antigravity-cli/gyro-stats.json` (Unix).
/// 3. Fallback: `~/.agy-gyro/gyro-stats.json`.
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
            return agy_dir.join("gyro-stats.json");
        }
        // If agy directory doesn't exist yet, we still prefer putting it there if ~/.gemini exists
        let gemini_dir = home.join(".gemini");
        if gemini_dir.is_dir() {
            return agy_dir.join("gyro-stats.json");
        }

        // Fallback to agy folder path directly so it will be created there
        return agy_dir.join("gyro-stats.json");
    }

    std::env::temp_dir().join("gyro-stats.json")
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
        let file_path = temp_dir.join("stats.json");

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
