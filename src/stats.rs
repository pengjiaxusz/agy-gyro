// SPDX-License-Identifier: MIT

use chrono::Timelike;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info, warn};

pub const DEFAULT_PRIOR_ALPHA: f64 = 1.0; // Neutral prior successes (50% mean with beta=1)
pub const DEFAULT_PRIOR_BETA: f64 = 1.0; // Prior failures
pub const DEFAULT_SHRINKAGE_WEIGHT: f64 = 3.0; // Weight of global prior when estimating hourly rate
pub const DEFAULT_MAX_SAMPLES: f64 = 20.0; // Hard ceiling on effective sample weight per bucket
pub const DEFAULT_HALF_LIFE_SECS: f64 = 7.0 * 24.0 * 3600.0; // 7-day half-life for time decay
pub const DEFAULT_BURST_WINDOW_SECS: i64 = 15; // Consecutive requests within 15s are damped
pub const CURRENT_DB_VERSION: u32 = 2; // SQLite schema version

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

/// Returns true if a node is an informational, placeholder, or control entry in Clash rather than a usable proxy.
pub fn is_invalid_or_info_node(node: &str) -> bool {
    let trimmed = node.trim();
    if trimmed.is_empty() {
        return true;
    }

    let lower = trimmed.to_lowercase();

    // Standard control / policy keywords in Clash
    if matches!(
        lower.as_str(),
        "direct" | "reject" | "compatible" | "pass" | "global" | "proxy" | "auto" | "fallback"
    ) {
        return true;
    }

    // Chinese informational / metadata keywords common in subscriptions
    let info_keywords_zh = [
        "剩余流量",
        "到期时间",
        "官网",
        "重置",
        "通知",
        "套餐",
        "维护",
        "客服",
        "发布",
        "频道",
        "群组",
        "公告",
        "说明",
        "导航",
        "备用",
        "测速",
        "故障转移",
        "负载均衡",
        "自动选择",
        "更新订阅",
        "订阅信息",
    ];
    for kw in info_keywords_zh {
        if trimmed.contains(kw) {
            return true;
        }
    }

    // English info keywords
    let info_keywords_en = [
        "expire",
        "traffic",
        "reset",
        "website",
        "notice",
        "channel",
        "group",
        "telegram",
        "update",
        "load-balance",
    ];
    for kw in info_keywords_en {
        if lower.contains(kw) {
            return true;
        }
    }

    false
}

/// Returns true if a node belongs to a region unsupported by Gemini API (e.g. Hong Kong, China).
pub fn is_unsupported_region(node: &str) -> bool {
    let trimmed = node.trim();
    let lower = trimmed.to_lowercase();

    // Hong Kong is strictly unsupported by Gemini API
    if trimmed.contains("香港")
        || trimmed.contains("🇭🇰")
        || lower.contains("hong kong")
        || lower.contains("hongkong")
    {
        return true;
    }

    // Check for "hk" as a standalone token or standard prefix/suffix, e.g. "HK-01", "HK_01", "[HK]", "HK 01", "(HK)"
    for part in lower.split(|c: char| !c.is_alphanumeric()) {
        if part == "hk" {
            return true;
        }
    }

    // China / Mainland
    if trimmed.contains("中国")
        || trimmed.contains("国内")
        || trimmed.contains("🇨🇳")
        || lower.contains("china")
    {
        return true;
    }

    false
}

/// Checks whether a candidate node is valid for routing Gemini API traffic.
/// Excludes dummy/informational nodes and unsupported regions like Hong Kong.
pub fn is_valid_candidate_node(node: &str) -> bool {
    !is_invalid_or_info_node(node) && !is_unsupported_region(node)
}

/// Compares two region identifiers, matching canonical aliases (e.g. "美国" == "US", "日本" == "JP").
pub fn canonical_region_matches(region: &str, target: &str) -> bool {
    if region.eq_ignore_ascii_case(target) {
        return true;
    }
    let r_clean = region.trim();
    let t_clean = target.trim();
    if r_clean == t_clean {
        return true;
    }

    let is_us = |s: &str| {
        matches!(s.to_lowercase().as_str(), "美国" | "美" | "us" | "usa" | "united states" | "america" | "🇺🇸")
    };
    let is_jp = |s: &str| {
        matches!(s.to_lowercase().as_str(), "日本" | "日" | "jp" | "japan" | "🇯🇵")
    };
    let is_tw = |s: &str| {
        matches!(s.to_lowercase().as_str(), "台湾" | "台" | "tw" | "taiwan" | "🇹🇼")
    };
    let is_sg = |s: &str| {
        matches!(s.to_lowercase().as_str(), "新加坡" | "新" | "sg" | "singapore" | "🇸🇬" | "狮城")
    };
    let is_hk = |s: &str| {
        matches!(s.to_lowercase().as_str(), "香港" | "港" | "hk" | "hong kong" | "🇭🇰")
    };
    let is_kr = |s: &str| {
        matches!(s.to_lowercase().as_str(), "韩国" | "韩" | "kr" | "korea" | "🇰🇷")
    };
    let is_uk = |s: &str| {
        matches!(s.to_lowercase().as_str(), "英国" | "英" | "uk" | "gb" | "united kingdom" | "🇬🇧")
    };
    let is_de = |s: &str| {
        matches!(s.to_lowercase().as_str(), "德国" | "德" | "de" | "germany" | "🇩🇪")
    };

    (is_us(r_clean) && is_us(t_clean))
        || (is_jp(r_clean) && is_jp(t_clean))
        || (is_tw(r_clean) && is_tw(t_clean))
        || (is_sg(r_clean) && is_sg(t_clean))
        || (is_hk(r_clean) && is_hk(t_clean))
        || (is_kr(r_clean) && is_kr(t_clean))
        || (is_uk(r_clean) && is_uk(t_clean))
        || (is_de(r_clean) && is_de(t_clean))
}

/// Extracts or canonicalizes the region of a node based on emoji flags, prefixes, country names, and city keywords.
pub fn extract_region(node: &str, configured_regions: &[String]) -> String {
    let lower = node.to_lowercase();

    // 1. Check Hong Kong
    if node.contains("香港") || node.contains("🇭🇰") || lower.contains("hong kong") || lower.contains("hongkong") {
        return "香港".to_string();
    }
    for part in lower.split(|c: char| !c.is_alphanumeric()) {
        if part == "hk" {
            return "香港".to_string();
        }
    }

    // 2. Canonical regions detection
    let mut detected_canonical: Option<&'static str> = None;

    // US detection
    if node.contains("美国")
        || node.contains("🇺🇸")
        || node.contains("洛杉矶")
        || node.contains("硅谷")
        || node.contains("西雅图")
        || node.contains("芝加哥")
        || node.contains("纽约")
        || node.contains("凤凰城")
        || node.contains("波特兰")
        || node.contains("圣何塞")
        || node.contains("达拉斯")
        || node.contains("迈阿密")
        || lower.contains("united states")
        || lower.contains("los angeles")
        || lower.contains("silicon valley")
        || lower.contains("seattle")
        || lower.contains("chicago")
        || lower.contains("new york")
        || lower.contains("phoenix")
        || lower.contains("portland")
    {
        detected_canonical = Some("美国");
    } else {
        for part in lower.split(|c: char| !c.is_alphanumeric()) {
            if part == "us" || part == "usa" {
                detected_canonical = Some("美国");
                break;
            }
        }
    }

    // Japan detection
    if detected_canonical.is_none() {
        if node.contains("日本")
            || node.contains("🇯🇵")
            || node.contains("东京")
            || node.contains("大阪")
            || lower.contains("japan")
            || lower.contains("tokyo")
            || lower.contains("osaka")
        {
            detected_canonical = Some("日本");
        } else {
            for part in lower.split(|c: char| !c.is_alphanumeric()) {
                if part == "jp" {
                    detected_canonical = Some("日本");
                    break;
                }
            }
        }
    }

    // Taiwan detection
    if detected_canonical.is_none() {
        if node.contains("台湾")
            || node.contains("🇹🇼")
            || node.contains("台北")
            || node.contains("台中")
            || node.contains("新北")
            || lower.contains("taiwan")
            || lower.contains("taipei")
        {
            detected_canonical = Some("台湾");
        } else {
            for part in lower.split(|c: char| !c.is_alphanumeric()) {
                if part == "tw" {
                    detected_canonical = Some("台湾");
                    break;
                }
            }
        }
    }

    // Singapore detection
    if detected_canonical.is_none() {
        if node.contains("新加坡")
            || node.contains("🇸🇬")
            || node.contains("狮城")
            || lower.contains("singapore")
        {
            detected_canonical = Some("新加坡");
        } else {
            for part in lower.split(|c: char| !c.is_alphanumeric()) {
                if part == "sg" {
                    detected_canonical = Some("新加坡");
                    break;
                }
            }
        }
    }

    // Korea detection
    if detected_canonical.is_none() {
        if node.contains("韩国")
            || node.contains("🇰🇷")
            || node.contains("首尔")
            || lower.contains("korea")
            || lower.contains("seoul")
        {
            detected_canonical = Some("韩国");
        } else {
            for part in lower.split(|c: char| !c.is_alphanumeric()) {
                if part == "kr" {
                    detected_canonical = Some("韩国");
                    break;
                }
            }
        }
    }

    // UK detection
    if detected_canonical.is_none() {
        if node.contains("英国")
            || node.contains("🇬🇧")
            || node.contains("伦敦")
            || lower.contains("united kingdom")
            || lower.contains("london")
        {
            detected_canonical = Some("英国");
        } else {
            for part in lower.split(|c: char| !c.is_alphanumeric()) {
                if part == "uk" || part == "gb" {
                    detected_canonical = Some("英国");
                    break;
                }
            }
        }
    }

    // Germany detection
    if detected_canonical.is_none() {
        if node.contains("德国")
            || node.contains("🇩🇪")
            || node.contains("法兰克福")
            || lower.contains("germany")
            || lower.contains("frankfurt")
        {
            detected_canonical = Some("德国");
        } else {
            for part in lower.split(|c: char| !c.is_alphanumeric()) {
                if part == "de" {
                    detected_canonical = Some("德国");
                    break;
                }
            }
        }
    }

    // If a canonical region was detected, check if configured_regions has a matching alias
    if let Some(canonical) = detected_canonical {
        for cr in configured_regions {
            if canonical_region_matches(canonical, cr) {
                return cr.clone();
            }
        }
        return canonical.to_string();
    }

    // Fallback: match any substring from configured_regions
    for cr in configured_regions {
        if !cr.is_empty() && (node.contains(cr) || lower.contains(&cr.to_lowercase())) {
            return cr.clone();
        }
    }

    "其他".to_string()
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
    failure_cooldown_secs: i64,
    consecutive_failure_threshold: u32,
    region_priority: Vec<String>,
    region_consecutive_failure_threshold: u32,
    region_failure_cooldown_secs: i64,
    no_region_priority: bool,
}

impl StatsManager {
    pub fn new(
        file_path: Option<PathBuf>,
        enabled: bool,
        max_samples: f64,
        half_life_secs: f64,
        burst_window_secs: i64,
        failure_cooldown_secs: i64,
        consecutive_failure_threshold: u32,
        region_priority: Vec<String>,
        region_consecutive_failure_threshold: u32,
        region_failure_cooldown_secs: i64,
        no_region_priority: bool,
    ) -> Arc<Self> {
        let conn = if enabled {
            match &file_path {
                Some(path) => {
                    if let Some(parent) = path.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    let needs_rebuild = if path.exists() {
                        match Connection::open(path) {
                            Ok(c) => {
                                let ver = c
                                    .query_row(
                                        "SELECT value FROM meta WHERE key = 'schema_version'",
                                        [],
                                        |r| r.get::<_, String>(0),
                                    )
                                    .ok()
                                    .and_then(|v| v.parse::<u32>().ok());
                                ver != Some(CURRENT_DB_VERSION)
                            }
                            Err(_) => true,
                        }
                    } else {
                        false
                    };

                    if needs_rebuild {
                        warn!(
                            "Database schema version mismatch (expected {}). Dropping and recreating database at {}",
                            CURRENT_DB_VERSION,
                            path.display()
                        );
                        let _ = std::fs::remove_file(path);
                        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
                        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
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
            failure_cooldown_secs,
            consecutive_failure_threshold,
            region_priority,
            region_consecutive_failure_threshold,
            region_failure_cooldown_secs,
            no_region_priority,
        })
    }

    pub fn from_config(config: &crate::config::Config) -> Arc<Self> {
        Self::new(
            if config.no_stats {
                None
            } else {
                Some(config.resolved_stats_file())
            },
            !config.no_stats,
            config.stats_max_samples,
            config.stats_half_life_secs(),
            config.stats_burst_window_secs,
            config.failure_cooldown_duration_secs(),
            config.consecutive_failure_threshold,
            config.region_priority.clone(),
            config.region_consecutive_failure_threshold,
            config.region_failure_cooldown_duration_secs(),
            config.no_region_priority,
        )
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
             );
             CREATE TABLE IF NOT EXISTS node_quarantine (
                 node_name TEXT PRIMARY KEY,
                 quarantined_until_sec INTEGER NOT NULL,
                 reason TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS node_health (
                 node_name TEXT PRIMARY KEY,
                 consecutive_failures INTEGER NOT NULL DEFAULT 0,
                 cooldown_until_sec INTEGER NOT NULL DEFAULT 0,
                 last_success_sec INTEGER NOT NULL DEFAULT 0,
                 last_failure_sec INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS region_health (
                 region_name TEXT PRIMARY KEY,
                 consecutive_failures INTEGER NOT NULL DEFAULT 0,
                 cooldown_until_sec INTEGER NOT NULL DEFAULT 0,
                 last_success_sec INTEGER NOT NULL DEFAULT 0,
                 last_failure_sec INTEGER NOT NULL DEFAULT 0
             );
             INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', '2');"
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
        conn: &mut Connection,
        node: &str,
        hour: i32,
        now: i64,
        half_life_secs: f64,
        burst_window_secs: i64,
        max_samples: f64,
        failure_cooldown_secs: i64,
        consecutive_failure_threshold: u32,
        region_priority: &[String],
        region_consecutive_failure_threshold: u32,
        region_failure_cooldown_secs: i64,
        no_region_priority: bool,
        is_success: bool,
    ) -> rusqlite::Result<()> {
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        let mut overall = Self::fetch_counts(&tx, node, -1)?;
        let mut hourly = Self::fetch_counts(&tx, node, hour)?;

        if is_success {
            overall.record_success(now, half_life_secs, burst_window_secs, max_samples);
            hourly.record_success(now, half_life_secs, burst_window_secs, max_samples);

            // Reset consecutive failures and cooldown on success
            let _ = tx.execute(
                "INSERT INTO node_health (node_name, consecutive_failures, cooldown_until_sec, last_success_sec, last_failure_sec)
                 VALUES (?1, 0, 0, ?2, 0)
                 ON CONFLICT(node_name) DO UPDATE SET
                   consecutive_failures = 0,
                   cooldown_until_sec = 0,
                   last_success_sec = excluded.last_success_sec",
                params![node, now],
            );

            // Reset region health on success
            if !no_region_priority {
                let region = extract_region(node, region_priority);
                if !region.is_empty() && region != "其他" {
                    let _ = tx.execute(
                        "INSERT INTO region_health (region_name, consecutive_failures, cooldown_until_sec, last_success_sec, last_failure_sec)
                         VALUES (?1, 0, 0, ?2, 0)
                         ON CONFLICT(region_name) DO UPDATE SET
                           consecutive_failures = 0,
                           cooldown_until_sec = 0,
                           last_success_sec = excluded.last_success_sec",
                        params![region, now],
                    );
                }
            }

            let (os, of) = overall.decayed_counts_at(now, half_life_secs);
            let o_total = os + of;
            if o_total >= 3.0 && (os / o_total) >= 0.70 {
                let curr_anchor: Option<String> = tx
                    .query_row(
                        "SELECT value FROM meta WHERE key = 'consensus_anchor_node'",
                        [],
                        |r| r.get(0),
                    )
                    .ok();
                if curr_anchor.as_deref() != Some(node) {
                    tx.execute(
                        "INSERT INTO meta (key, value) VALUES ('consensus_anchor_node', ?1)
                         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                        params![node],
                    )?;
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    tx.execute(
                        "INSERT INTO meta (key, value) VALUES ('consensus_anchor_updated_ms', ?1)
                         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                        params![now_ms.to_string()],
                    )?;
                    info!(
                        "Promoted node [{}] to consensus anchor (reliability: {:.1}%, samples: {:.1})",
                        node,
                        (os / o_total) * 100.0,
                        o_total
                    );
                }
            }
        } else {
            overall.record_failure(now, half_life_secs, max_samples);
            hourly.record_failure(now, half_life_secs, max_samples);

            // Fetch current consecutive failures for node
            let curr_cf: u32 = tx
                .query_row(
                    "SELECT consecutive_failures FROM node_health WHERE node_name = ?1",
                    params![node],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            let new_cf = curr_cf + 1;
            let cooldown_until = if new_cf >= consecutive_failure_threshold {
                let cd = now + failure_cooldown_secs;
                warn!(
                    "Node [{}] reached {} consecutive failures! Cooling down for {:.0}s until timestamp {} to allow lower-priority nodes to be tried.",
                    node, new_cf, failure_cooldown_secs, cd
                );
                let curr_anchor: Option<String> = tx
                    .query_row(
                        "SELECT value FROM meta WHERE key = 'consensus_anchor_node'",
                        [],
                        |r| r.get(0),
                    )
                    .ok();
                if curr_anchor.as_deref() == Some(node) {
                    let _ = tx.execute(
                        "DELETE FROM meta WHERE key IN ('consensus_anchor_node', 'consensus_anchor_updated_ms')",
                        [],
                    );
                    info!("Cleared consensus anchor as [{}] entered consecutive failure cooldown", node);
                }
                cd
            } else {
                0
            };

            let _ = tx.execute(
                "INSERT INTO node_health (node_name, consecutive_failures, cooldown_until_sec, last_success_sec, last_failure_sec)
                 VALUES (?1, ?2, ?3, 0, ?4)
                 ON CONFLICT(node_name) DO UPDATE SET
                   consecutive_failures = excluded.consecutive_failures,
                   cooldown_until_sec = excluded.cooldown_until_sec,
                   last_failure_sec = excluded.last_failure_sec",
                params![node, new_cf, cooldown_until, now],
            );

            // Region failure tracking
            if !no_region_priority {
                let region = extract_region(node, region_priority);
                if !region.is_empty() && region != "其他" {
                    let curr_rcf: u32 = tx
                        .query_row(
                            "SELECT consecutive_failures FROM region_health WHERE region_name = ?1",
                            params![&region],
                            |r| r.get(0),
                        )
                        .unwrap_or(0);
                    let new_rcf = curr_rcf + 1;
                    let region_cd = if new_rcf >= region_consecutive_failure_threshold {
                        let rcd = now + region_failure_cooldown_secs;
                        warn!(
                            "Region [{}] reached {} consecutive failures! Cooling down for {:.0}s until timestamp {} to allow lower-priority regions to be tried.",
                            region, new_rcf, region_failure_cooldown_secs, rcd
                        );
                        // If current consensus anchor is in this region, clear it
                        let curr_anchor: Option<String> = tx
                            .query_row(
                                "SELECT value FROM meta WHERE key = 'consensus_anchor_node'",
                                [],
                                |r| r.get(0),
                            )
                            .ok();
                        if let Some(ref anchor) = curr_anchor {
                            if extract_region(anchor, region_priority) == region {
                                let _ = tx.execute(
                                    "DELETE FROM meta WHERE key IN ('consensus_anchor_node', 'consensus_anchor_updated_ms')",
                                    [],
                                );
                                info!("Cleared consensus anchor as its region [{}] entered cooldown", region);
                            }
                        }
                        rcd
                    } else {
                        0
                    };

                    let _ = tx.execute(
                        "INSERT INTO region_health (region_name, consecutive_failures, cooldown_until_sec, last_success_sec, last_failure_sec)
                         VALUES (?1, ?2, ?3, 0, ?4)
                         ON CONFLICT(region_name) DO UPDATE SET
                           consecutive_failures = excluded.consecutive_failures,
                           cooldown_until_sec = excluded.cooldown_until_sec,
                           last_failure_sec = excluded.last_failure_sec",
                        params![region, new_rcf, region_cd, now],
                    );
                }
            }
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
        let mut guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let conn = match guard.as_mut() {
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
            self.failure_cooldown_secs,
            self.consecutive_failure_threshold,
            &self.region_priority,
            self.region_consecutive_failure_threshold,
            self.region_failure_cooldown_secs,
            self.no_region_priority,
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
        let mut guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let conn = match guard.as_mut() {
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
            self.failure_cooldown_secs,
            self.consecutive_failure_threshold,
            &self.region_priority,
            self.region_consecutive_failure_threshold,
            self.region_failure_cooldown_secs,
            self.no_region_priority,
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

                let (cf, cooldown_until): (u32, i64) = conn
                    .query_row(
                        "SELECT consecutive_failures, cooldown_until_sec FROM node_health WHERE node_name = ?1",
                        params![node],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .unwrap_or((0, 0));

                let base_score = if overall.total() > 0.0 || hourly.total() > 0.0 {
                    let mut node_stats = NodeStats {
                        overall: overall.clone(),
                        hourly: default_hourly(),
                    };
                    node_stats.hourly[h as usize] = hourly.clone();

                    node_stats.calculate_score(
                        hour,
                        now,
                        self.half_life_secs,
                        DEFAULT_PRIOR_ALPHA,
                        DEFAULT_PRIOR_BETA,
                        DEFAULT_SHRINKAGE_WEIGHT,
                    )
                } else {
                    DEFAULT_PRIOR_ALPHA / (DEFAULT_PRIOR_ALPHA + DEFAULT_PRIOR_BETA)
                };

                // If in active cooldown: heavy penalty (0.05 * 0.5^cf) to ensure lower-priority and untried nodes (0.50) rank higher
                let score = if cooldown_until > now {
                    base_score * 0.05 * 0.5f64.powi(cf as i32)
                } else {
                    base_score
                };

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
            })
            .collect();

        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        ranked
    }

    /// Returns (last_switch_at_ms, last_switch_node) from shared SQLite metadata
    pub fn get_last_switch_info(&self) -> (i64, Option<String>) {
        if !self.enabled {
            return (0, None);
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return (0, None),
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return (0, None),
        };

        let switch_at: i64 = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'last_switch_at_ms'",
                [],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        let switch_node: Option<String> = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'last_switch_node'",
                [],
                |r| r.get(0),
            )
            .ok();

        (switch_at, switch_node)
    }

    /// Records a Clash switch event in shared SQLite metadata
    pub fn record_switch_event(
        &self,
        from_node: &str,
        to_node: &str,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return Ok(()),
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return Ok(()),
        };

        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('last_switch_at_ms', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![now_ms.to_string()],
        )?;
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('last_switch_node', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![to_node],
        )?;
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('last_switch_from', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![from_node],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Quarantines a node until `now_sec + duration_secs` with a given reason.
    pub fn quarantine_node(&self, node: &str, duration_secs: i64, reason: &str) {
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
        let until = now + duration_secs;

        let res: rusqlite::Result<()> = (|| {
            let mut stmt = conn.prepare_cached(
                "INSERT INTO node_quarantine (node_name, quarantined_until_sec, reason)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(node_name) DO UPDATE SET
                   quarantined_until_sec = excluded.quarantined_until_sec,
                   reason = excluded.reason",
            )?;
            stmt.execute(params![node, until, reason])?;

            // If the quarantined node is currently the consensus anchor, clear the anchor
            let curr_anchor: Option<String> = conn
                .query_row(
                    "SELECT value FROM meta WHERE key = 'consensus_anchor_node'",
                    [],
                    |r| r.get(0),
                )
                .ok();
            if curr_anchor.as_deref() == Some(node) {
                let _ = conn.execute(
                    "DELETE FROM meta WHERE key IN ('consensus_anchor_node', 'consensus_anchor_updated_ms')",
                    [],
                );
                info!("Cleared consensus anchor as [{}] was quarantined", node);
            }

            // If location blocked, also cool down the node's region
            if !self.no_region_priority && reason.contains("location") {
                let region = extract_region(node, &self.region_priority);
                if !region.is_empty() && region != "其他" {
                    let region_cd = now + self.region_failure_cooldown_secs;
                    let _ = conn.execute(
                        "INSERT INTO region_health (region_name, consecutive_failures, cooldown_until_sec, last_success_sec, last_failure_sec)
                         VALUES (?1, 1, ?2, 0, ?3)
                         ON CONFLICT(region_name) DO UPDATE SET
                           cooldown_until_sec = MAX(cooldown_until_sec, excluded.cooldown_until_sec),
                           last_failure_sec = excluded.last_failure_sec",
                        params![region, region_cd, now],
                    );
                    warn!("Region [{}] cooled down for {:.0}s due to node quarantine ({})", region, self.region_failure_cooldown_secs, reason);
                }
            }
            Ok(())
        })();

        if let Err(e) = res {
            error!("Failed to quarantine node [{}]: {}", node, e);
        } else {
            warn!(
                "Node [{}] quarantined for {:.1}h until timestamp {} ({})",
                node,
                duration_secs as f64 / 3600.0,
                until,
                reason
            );
        }
    }

    /// Checks whether a node is currently quarantined at `now_sec`.
    pub fn is_quarantined(&self, node: &str, now_sec: i64) -> bool {
        if !self.enabled || node.is_empty() {
            return false;
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return false,
        };

        conn.query_row(
            "SELECT quarantined_until_sec FROM node_quarantine WHERE node_name = ?1",
            params![node],
            |row| row.get::<_, i64>(0),
        )
        .map(|until| until > now_sec)
        .unwrap_or(false)
    }

    /// Filters candidate nodes to only those not currently quarantined.
    pub fn filter_quarantined(&self, candidates: &[String], now_sec: i64) -> Vec<String> {
        if !self.enabled || candidates.is_empty() {
            return candidates.to_vec();
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return candidates.to_vec(),
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return candidates.to_vec(),
        };

        let mut quarantined_set = std::collections::HashSet::new();
        if let Ok(mut stmt) = conn.prepare(
            "SELECT node_name FROM node_quarantine WHERE quarantined_until_sec > ?1",
        ) {
            if let Ok(rows) = stmt.query_map(params![now_sec], |row| row.get::<_, String>(0)) {
                for node in rows.flatten() {
                    quarantined_set.insert(node);
                }
            }
        }

        candidates
            .iter()
            .filter(|c| !quarantined_set.contains(*c))
            .cloned()
            .collect()
    }

    /// Returns all currently quarantined nodes with (node_name, remaining_secs, reason).
    pub fn get_quarantined_nodes(&self, now_sec: i64) -> Vec<(String, i64, String)> {
        if !self.enabled {
            return Vec::new();
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return Vec::new(),
        };

        let mut stmt = match conn.prepare(
            "SELECT node_name, quarantined_until_sec, reason
             FROM node_quarantine WHERE quarantined_until_sec > ?1
             ORDER BY quarantined_until_sec DESC",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let rows = stmt.query_map(params![now_sec], |row| {
            let node: String = row.get(0)?;
            let until: i64 = row.get(1)?;
            let reason: String = row.get(2)?;
            Ok((node, (until - now_sec).max(0), reason))
        });

        rows.map(|r| r.flatten().collect()).unwrap_or_default()
    }

    /// Clears quarantine for a specific node or all nodes if None
    pub fn clear_quarantine(&self, node: Option<&str>) {
        if !self.enabled {
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

        if let Some(n) = node {
            let _ = conn.execute("DELETE FROM node_quarantine WHERE node_name = ?1", params![n]);
        } else {
            let _ = conn.execute("DELETE FROM node_quarantine", []);
        }
    }

    /// Checks whether a node is currently in cooldown from consecutive failures at `now_sec`.
    pub fn is_cooling_down(&self, node: &str, now_sec: i64) -> bool {
        if !self.enabled || node.is_empty() {
            return false;
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return false,
        };

        conn.query_row(
            "SELECT cooldown_until_sec FROM node_health WHERE node_name = ?1",
            params![node],
            |row| row.get::<_, i64>(0),
        )
        .map(|until| until > now_sec)
        .unwrap_or(false)
    }

    /// Filters candidate nodes to only those not currently in cooldown.
    pub fn filter_cooling_down(&self, candidates: &[String], now_sec: i64) -> Vec<String> {
        if !self.enabled || candidates.is_empty() {
            return candidates.to_vec();
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return candidates.to_vec(),
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return candidates.to_vec(),
        };

        let mut cooling_set = std::collections::HashSet::new();
        if let Ok(mut stmt) = conn.prepare(
            "SELECT node_name FROM node_health WHERE cooldown_until_sec > ?1",
        ) {
            if let Ok(rows) = stmt.query_map(params![now_sec], |row| row.get::<_, String>(0)) {
                for node in rows.flatten() {
                    cooling_set.insert(node);
                }
            }
        }

        candidates
            .iter()
            .filter(|c| !cooling_set.contains(*c))
            .cloned()
            .collect()
    }

    /// Returns consecutive failure count for a node
    pub fn get_consecutive_failures(&self, node: &str) -> u32 {
        if !self.enabled || node.is_empty() {
            return 0;
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return 0,
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return 0,
        };

        conn.query_row(
            "SELECT consecutive_failures FROM node_health WHERE node_name = ?1",
            params![node],
            |row| row.get::<_, u32>(0),
        )
        .unwrap_or(0)
    }

    /// Returns all currently cooling-down nodes with (node_name, remaining_secs, consecutive_failures).
    pub fn get_cooling_down_nodes(&self, now_sec: i64) -> Vec<(String, i64, u32)> {
        if !self.enabled {
            return Vec::new();
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return Vec::new(),
        };

        let mut stmt = match conn.prepare(
            "SELECT node_name, cooldown_until_sec, consecutive_failures
             FROM node_health WHERE cooldown_until_sec > ?1
             ORDER BY cooldown_until_sec DESC",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let rows = stmt.query_map(params![now_sec], |row| {
            let node: String = row.get(0)?;
            let until: i64 = row.get(1)?;
            let cf: u32 = row.get(2)?;
            Ok((node, (until - now_sec).max(0), cf))
        });

        rows.map(|r| r.flatten().collect()).unwrap_or_default()
    }

    /// Gets current consensus anchor node from shared SQLite meta
    pub fn get_consensus_anchor(&self) -> Option<String> {
        if !self.enabled {
            return None;
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return None,
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return None,
        };

        conn.query_row(
            "SELECT value FROM meta WHERE key = 'consensus_anchor_node'",
            [],
            |r| r.get(0),
        )
        .ok()
        .filter(|s: &String| !s.trim().is_empty())
    }

    /// Sets consensus anchor node in shared SQLite meta
    pub fn set_consensus_anchor(&self, node: &str) {
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

        let now_ms = chrono::Utc::now().timestamp_millis();
        let _ = conn.execute(
            "INSERT INTO meta (key, value) VALUES ('consensus_anchor_node', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![node],
        );
        let _ = conn.execute(
            "INSERT INTO meta (key, value) VALUES ('consensus_anchor_updated_ms', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![now_ms.to_string()],
        );
        info!("Set consensus anchor node to [{}]", node);
    }

    /// Checks if a node is the current consensus anchor
    pub fn is_consensus_anchor(&self, node: &str) -> bool {
        if node.is_empty() {
            return false;
        }
        self.get_consensus_anchor().as_deref() == Some(node)
    }

    /// Checks whether a region is currently in cooldown from consecutive failures or location block at `now_sec`.
    pub fn is_region_cooling_down(&self, region: &str, now_sec: i64) -> bool {
        if !self.enabled || region.is_empty() {
            return false;
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return false,
        };

        conn.query_row(
            "SELECT cooldown_until_sec FROM region_health WHERE region_name = ?1",
            params![region],
            |row| row.get::<_, i64>(0),
        )
        .map(|until| until > now_sec)
        .unwrap_or(false)
    }

    /// Returns all currently cooling-down regions with (region_name, remaining_secs, consecutive_failures).
    pub fn get_cooling_down_regions(&self, now_sec: i64) -> Vec<(String, i64, u32)> {
        if !self.enabled {
            return Vec::new();
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return Vec::new(),
        };

        let mut stmt = match conn.prepare(
            "SELECT region_name, cooldown_until_sec, consecutive_failures
             FROM region_health WHERE cooldown_until_sec > ?1
             ORDER BY cooldown_until_sec DESC",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let rows = stmt.query_map(params![now_sec], |row| {
            let region: String = row.get(0)?;
            let until: i64 = row.get(1)?;
            let cf: u32 = row.get(2)?;
            Ok((region, (until - now_sec).max(0), cf))
        });

        rows.map(|r| r.flatten().collect()).unwrap_or_default()
    }

    /// Returns consecutive failure count for a region.
    pub fn get_region_consecutive_failures(&self, region: &str) -> u32 {
        if !self.enabled || region.is_empty() {
            return 0;
        }
        let guard = match self.conn.lock() {
            Ok(g) => g,
            Err(_) => return 0,
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return 0,
        };

        conn.query_row(
            "SELECT consecutive_failures FROM region_health WHERE region_name = ?1",
            params![region],
            |row| row.get::<_, u32>(0),
        )
        .unwrap_or(0)
    }

    pub fn region_priority(&self) -> &[String] {
        &self.region_priority
    }

    pub fn is_region_priority_enabled(&self) -> bool {
        !self.no_region_priority
    }

    /// Selects the best candidate node for the given hour, excluding any nodes in `excluded`.
    /// Excludes invalid/informational nodes and unsupported regions (such as Hong Kong).
    /// Uses two-tier priority (Region -> Node) unless disabled via config.
    pub fn select_best_node(
        &self,
        hour: u8,
        candidates: &[String],
        excluded: &[String],
        failing_node: Option<&str>,
    ) -> Option<String> {
        if candidates.is_empty() {
            return None;
        }

        // Filter out invalid/info nodes and unsupported regions (e.g. Hong Kong)
        let valid_candidates: Vec<String> = candidates
            .iter()
            .filter(|c| is_valid_candidate_node(c))
            .cloned()
            .collect();
        let pool = if valid_candidates.is_empty() {
            warn!("All candidate nodes filtered as invalid or unsupported! Falling back to full candidates");
            candidates.to_vec()
        } else {
            valid_candidates
        };

        let now = Self::now_sec();
        let unquarantined = self.filter_quarantined(&pool, now);
        let active_candidates = if unquarantined.is_empty() {
            warn!("All candidates are quarantined! Falling back to non-quarantined pool");
            &pool[..]
        } else {
            &unquarantined[..]
        };

        if self.no_region_priority {
            return self.select_best_node_flat(hour, active_candidates, excluded, failing_node, now);
        }

        self.select_best_node_two_tier(hour, active_candidates, excluded, failing_node, now)
    }

    /// Flat single-tier node selection fallback when no_region_priority is true.
    fn select_best_node_flat(
        &self,
        hour: u8,
        candidates: &[String],
        excluded: &[String],
        failing_node: Option<&str>,
        now_sec: i64,
    ) -> Option<String> {
        let non_cooling = self.filter_cooling_down(candidates, now_sec);
        let pool = if non_cooling.is_empty() {
            candidates
        } else {
            &non_cooling[..]
        };

        let ranked = self.rank_nodes(hour, pool);

        for (node, _, _, _) in &ranked {
            let is_failing = failing_node.map(|f| f == node).unwrap_or(false);
            if !is_failing && !excluded.iter().any(|ex| ex == node) {
                return Some(node.clone());
            }
        }

        if let Some(anchor) = self.get_consensus_anchor() {
            if pool.contains(&anchor) && failing_node != Some(&anchor) && !self.is_cooling_down(&anchor, now_sec) {
                info!("Gravity fallback: snapping back to consensus anchor [{}]", anchor);
                return Some(anchor);
            }
        }

        if let Some(failing) = failing_node {
            for (node, _, _, _) in &ranked {
                if node != failing {
                    return Some(node.clone());
                }
            }
        } else if let Some(last_excluded) = excluded.last() {
            for (node, _, _, _) in &ranked {
                if node != last_excluded {
                    return Some(node.clone());
                }
            }
        }

        ranked.first().map(|(n, _, _, _)| n.clone())
    }

    /// Two-tier node selection:
    /// Tier 1: Prioritize regions according to region_priority, region cooldown, and available healthy nodes.
    /// Tier 2: Within chosen region, prioritize nodes by 24h Bayesian reliability score and node cooldown.
    fn select_best_node_two_tier(
        &self,
        hour: u8,
        candidates: &[String],
        excluded: &[String],
        failing_node: Option<&str>,
        now_sec: i64,
    ) -> Option<String> {
        let mut region_map: HashMap<String, Vec<String>> = HashMap::new();
        for node in candidates {
            let region = extract_region(node, &self.region_priority);
            region_map.entry(region).or_default().push(node.clone());
        }

        let mut ordered_regions: Vec<String> = Vec::new();
        for r in &self.region_priority {
            for k in region_map.keys() {
                if canonical_region_matches(k, r) && !ordered_regions.contains(k) {
                    ordered_regions.push(k.clone());
                }
            }
        }
        for k in region_map.keys() {
            if k != "其他" && !ordered_regions.contains(k) {
                ordered_regions.push(k.clone());
            }
        }
        if region_map.contains_key("其他") && !ordered_regions.contains(&"其他".to_string()) {
            ordered_regions.push("其他".to_string());
        }

        // Consensus Anchor Snap-Back check:
        // Only snap back to anchor if:
        // 1. Anchor is not excluded in the current retry session
        // 2. Anchor's region is NOT cooling down
        // 3. Anchor is not cooling down, not quarantined, and not failing
        // 4. No higher-priority region has available healthy nodes
        if let Some(anchor) = self.get_consensus_anchor() {
            let anchor_region = extract_region(&anchor, &self.region_priority);
            let anchor_region_cooling = self.is_region_cooling_down(&anchor_region, now_sec);
            let anchor_cooling = self.is_cooling_down(&anchor, now_sec);
            let anchor_quarantined = self.is_quarantined(&anchor, now_sec);
            let is_failing = failing_node.map(|f| f == anchor).unwrap_or(false);
            let is_excluded = excluded.iter().any(|ex| ex == &anchor);

            if !is_excluded
                && !anchor_region_cooling
                && !anchor_cooling
                && !anchor_quarantined
                && !is_failing
                && candidates.contains(&anchor)
            {
                let anchor_region_idx = ordered_regions
                    .iter()
                    .position(|r| r == &anchor_region)
                    .unwrap_or(usize::MAX);
                let higher_region_available = ordered_regions
                    .iter()
                    .take(anchor_region_idx)
                    .any(|r| {
                        !self.is_region_cooling_down(r, now_sec)
                            && region_map.get(r).map(|nodes| {
                                nodes.iter().any(|n| {
                                    !self.is_cooling_down(n, now_sec)
                                        && Some(n.as_str()) != failing_node
                                        && !excluded.iter().any(|ex| ex == n)
                                })
                            }).unwrap_or(false)
                    });

                if !higher_region_available {
                    info!(
                        "Gravity fallback: snapping back to consensus anchor [{}] in region [{}]",
                        anchor, anchor_region
                    );
                    return Some(anchor);
                }
            }
        }

        // Tier 1: Look for highest priority region that is NOT cooling down and has available healthy nodes
        for region in &ordered_regions {
            if self.is_region_cooling_down(region, now_sec) {
                continue;
            }
            if let Some(nodes) = region_map.get(region) {
                let available_nodes: Vec<String> = nodes
                    .iter()
                    .filter(|n| {
                        !self.is_cooling_down(n, now_sec)
                            && Some(n.as_str()) != failing_node
                            && !excluded.iter().any(|ex| ex == *n)
                    })
                    .cloned()
                    .collect();

                if !available_nodes.is_empty() {
                    let ranked = self.rank_nodes(hour, &available_nodes);
                    if let Some((top_node, _, _, _)) = ranked.first() {
                        return Some(top_node.clone());
                    }
                }
            }
        }

        // Tier 1 Escalation: All priority regions are either cooling down or all their nodes are cooling down.
        // Try regions not in region cooldown, even if nodes are cooling down (Bayesian ranking selects least penalized)
        for region in &ordered_regions {
            if self.is_region_cooling_down(region, now_sec) {
                continue;
            }
            if let Some(nodes) = region_map.get(region) {
                let candidates_in_region: Vec<String> = nodes
                    .iter()
                    .filter(|n| {
                        Some(n.as_str()) != failing_node
                            && !excluded.iter().any(|ex| ex == *n)
                    })
                    .cloned()
                    .collect();
                if !candidates_in_region.is_empty() {
                    let ranked = self.rank_nodes(hour, &candidates_in_region);
                    if let Some((top_node, _, _, _)) = ranked.first() {
                        return Some(top_node.clone());
                    }
                }
            }
        }

        // Absolute Fallback: All candidate nodes across all regions have been excluded or failing!
        // 1. Try consensus anchor if it is not the failing node
        if let Some(anchor) = self.get_consensus_anchor() {
            if candidates.contains(&anchor) && failing_node != Some(&anchor) {
                return Some(anchor);
            }
        }

        // 2. Rank all candidates by rank_nodes across all regions and pick top non-failing node
        let ranked_all = self.rank_nodes(hour, candidates);
        for (node, _, _, _) in &ranked_all {
            if Some(node.as_str()) != failing_node && !excluded.iter().any(|ex| ex == node) {
                return Some(node.clone());
            }
        }
        for (node, _, _, _) in &ranked_all {
            if Some(node.as_str()) != failing_node {
                return Some(node.clone());
            }
        }

        ranked_all.first().map(|(n, _, _, _)| n.clone())
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
        let manager = StatsManager::new(
            None,
            true,
            20.0,
            7.0 * 86400.0,
            15,
            180,
            2,
            vec!["美国".to_string(), "日本".to_string(), "台湾".to_string(), "新加坡".to_string()],
            3,
            300,
            false,
        );
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
        assert_eq!(ranked[2].0, "node-d"); // New node with prior (50%)
        assert_eq!(ranked[3].0, "node-c"); // Broken node

        // Exclude node-a -> chooses node-b
        let best = manager.select_best_node(hour, &candidates, &["node-a".to_string()], Some("node-a"));
        assert_eq!(best, Some("node-b".to_string()));

        // Exclude node-a and node-b -> chooses node-d
        let best2 = manager.select_best_node(hour, &candidates, &["node-a".to_string(), "node-b".to_string()], Some("node-b"));
        assert_eq!(best2, Some("node-d".to_string()));

        // When ALL candidates are in excluded and failing_node is node-a -> chooses highest-scoring non-failing node (node-b)
        let all_excluded = candidates.clone();
        let fallback = manager.select_best_node(hour, &candidates, &all_excluded, Some("node-a"));
        assert_eq!(fallback, Some("node-b".to_string()));

        // When ALL candidates are in excluded and failing_node is node-b -> chooses node-a
        let fallback2 = manager.select_best_node(hour, &candidates, &all_excluded, Some("node-b"));
        assert_eq!(fallback2, Some("node-a".to_string()));
    }

    #[test]
    fn test_consecutive_failures_demotes_top_node_to_allow_lower_priority_exploration() {
        let manager = StatsManager::new(
            None,
            true,
            20.0,
            7.0 * 86400.0,
            15,
            180,
            2,
            vec!["美国".to_string(), "日本".to_string(), "台湾".to_string(), "新加坡".to_string()],
            3,
            300,
            false,
        );
        let hour = 14;

        // Node-Top has 20 successes (historical favorite)
        for _ in 0..20 {
            manager.record_success("node-top", hour);
        }
        // Node-Mid has 5 successes
        for _ in 0..5 {
            manager.record_success("node-mid", hour);
        }
        // Node-Untried has 0 requests

        let candidates = vec![
            "node-top".to_string(),
            "node-mid".to_string(),
            "node-untried".to_string(),
        ];

        // Initially, node-top is ranked #1
        let ranked = manager.rank_nodes(hour, &candidates);
        assert_eq!(ranked[0].0, "node-top");
        assert_eq!(ranked[1].0, "node-mid");
        assert_eq!(ranked[2].0, "node-untried");

        // First failure on node-top: penalized but still alive
        manager.record_failure("node-top", hour);
        assert_eq!(manager.get_consecutive_failures("node-top"), 1);
        assert!(!manager.is_cooling_down("node-top", StatsManager::now_sec()));

        // Second consecutive failure on node-top: triggers 180s cooldown!
        manager.record_failure("node-top", hour);
        assert_eq!(manager.get_consecutive_failures("node-top"), 2);
        assert!(manager.is_cooling_down("node-top", StatsManager::now_sec()));

        // Rank nodes: node-top drops to the bottom because of cooldown!
        let ranked_after_2_cf = manager.rank_nodes(hour, &candidates);
        assert_eq!(ranked_after_2_cf[0].0, "node-mid");
        assert_eq!(ranked_after_2_cf[1].0, "node-untried");
        assert_eq!(ranked_after_2_cf[2].0, "node-top");

        // select_best_node should automatically choose node-mid without needing manual exclusions
        let chosen = manager.select_best_node(hour, &candidates, &[], None);
        assert_eq!(chosen, Some("node-mid".to_string()));

        // Now suppose node-mid ALSO fails 2 times consecutively!
        manager.record_failure("node-mid", hour);
        manager.record_failure("node-mid", hour);
        assert!(manager.is_cooling_down("node-mid", StatsManager::now_sec()));

        // select_best_node now must explore node-untried!
        let chosen_untried = manager.select_best_node(hour, &candidates, &[], None);
        assert_eq!(chosen_untried, Some("node-untried".to_string()));

        // When node-untried succeeds, it stays healthy
        manager.record_success("node-untried", hour);
        assert_eq!(manager.get_consecutive_failures("node-untried"), 0);
        assert!(!manager.is_cooling_down("node-untried", StatsManager::now_sec()));
    }

    #[test]
    fn test_quarantine_and_consensus_anchor() {
        let manager = StatsManager::new(
            None,
            true,
            20.0,
            7.0 * 86400.0,
            15,
            180,
            2,
            vec!["美国".to_string(), "日本".to_string(), "台湾".to_string(), "新加坡".to_string()],
            3,
            300,
            false,
        );
        let now = StatsManager::now_sec();

        // 1. Anchor consensus promotion (needs >= 3.0 effective samples)
        for _ in 0..15 {
            manager.record_success("node-anchor", 10);
        }
        assert_eq!(manager.get_consensus_anchor(), Some("node-anchor".to_string()));
        assert!(manager.is_consensus_anchor("node-anchor"));

        // 2. Quarantine node-anchor for 12 hours
        manager.quarantine_node("node-anchor", 12 * 3600, "location_blocked_400");
        assert!(manager.is_quarantined("node-anchor", now));

        // Anchor should be cleared when quarantined!
        assert_eq!(manager.get_consensus_anchor(), None);

        // filter_quarantined should remove node-anchor
        let candidates = vec!["node-anchor".to_string(), "node-free".to_string()];
        let filtered = manager.filter_quarantined(&candidates, now);
        assert_eq!(filtered, vec!["node-free".to_string()]);

        // get_quarantined_nodes returns node-anchor
        let q_list = manager.get_quarantined_nodes(now);
        assert_eq!(q_list.len(), 1);
        assert_eq!(q_list[0].0, "node-anchor");
        assert_eq!(q_list[0].2, "location_blocked_400");

        // Clear quarantine
        manager.clear_quarantine(Some("node-anchor"));
        assert!(!manager.is_quarantined("node-anchor", now));
    }

    #[test]
    fn test_switch_metadata_persistence() {
        let temp_dir = std::env::temp_dir().join(format!("gyro-test-switch-{}", rand::random::<u32>()));
        let _ = std::fs::create_dir_all(&temp_dir);
        let file_path = temp_dir.join("stats.db");

        let manager = StatsManager::new(
            Some(file_path.clone()),
            true,
            20.0,
            7.0 * 86400.0,
            15,
            180,
            2,
            vec!["美国".to_string(), "日本".to_string(), "台湾".to_string(), "新加坡".to_string()],
            3,
            300,
            false,
        );
        let (initial_ms, initial_node) = manager.get_last_switch_info();
        assert_eq!(initial_ms, 0);
        assert_eq!(initial_node, None);

        let now_ms = 1725500000000;
        manager.record_switch_event("node-from", "node-to", now_ms).unwrap();

        let (switched_ms, switched_node) = manager.get_last_switch_info();
        assert_eq!(switched_ms, now_ms);
        assert_eq!(switched_node, Some("node-to".to_string()));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let temp_dir = std::env::temp_dir().join(format!("gyro-test-{}", rand::random::<u32>()));
        let _ = std::fs::create_dir_all(&temp_dir);
        let file_path = temp_dir.join("stats.db");

        let manager = StatsManager::new(
            Some(file_path.clone()),
            true,
            20.0,
            7.0 * 86400.0,
            15,
            180,
            2,
            vec!["美国".to_string(), "日本".to_string(), "台湾".to_string(), "新加坡".to_string()],
            3,
            300,
            false,
        );
        manager.record_success("node-test", 10);
        manager.record_failure("node-test", 10);
        manager.flush();

        // Load again from disk
        let manager2 = StatsManager::new(
            Some(file_path),
            true,
            20.0,
            7.0 * 86400.0,
            15,
            180,
            2,
            vec!["美国".to_string(), "日本".to_string(), "台湾".to_string(), "新加坡".to_string()],
            3,
            300,
            false,
        );
        let snap = manager2.snapshot();
        let node_stats = snap.nodes.get("node-test").expect("node-test should exist");
        assert!(node_stats.overall.successes > 0.0);
        assert!(node_stats.overall.failures > 0.0);
        assert!(node_stats.hourly[10].successes > 0.0);
        assert!(node_stats.hourly[10].failures > 0.0);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_invalid_and_unsupported_node_filtering() {
        // Invalid or informational nodes
        let invalid_nodes = vec![
            "DIRECT",
            "REJECT",
            "GLOBAL",
            "PROXY",
            "PASS",
            "剩余流量: 100GB",
            "到期时间: 2026-12-31",
            "官网: https://example.com",
            "官方发布频道 @clash",
            "Traffic Remaining: 50GB",
            "Reset Day: 10",
        ];
        for node in invalid_nodes {
            assert!(is_invalid_or_info_node(node), "Node [{}] should be identified as invalid/info", node);
            assert!(!is_valid_candidate_node(node), "Node [{}] should not be a valid candidate", node);
        }

        // Unsupported regions (Hong Kong & China)
        let unsupported_nodes = vec![
            "🇭🇰 香港 01 [x1.0]",
            "HK 02",
            "HongKong 03 HighSpeed",
            "香港专线",
            "🇨🇳 中国上海 01",
            "China Beijing",
        ];
        for node in unsupported_nodes {
            assert!(is_unsupported_region(node), "Node [{}] should be identified as unsupported region", node);
            assert!(!is_valid_candidate_node(node), "Node [{}] should not be a valid candidate", node);
        }

        // Valid candidate nodes
        let valid_nodes = vec![
            "🇺🇸 美国 01 [x1.0]",
            "US - Los Angeles 02",
            "🇯🇵 日本 01 高速",
            "JP Tokyo 03",
            "🇹🇼 台湾 01",
            "🇸🇬 新加坡 01",
            "🇩🇪 德国 01",
            "Custom-Node-Good",
        ];
        for node in valid_nodes {
            assert!(!is_invalid_or_info_node(node), "Node [{}] should not be invalid/info", node);
            assert!(!is_unsupported_region(node), "Node [{}] should not be unsupported", node);
            assert!(is_valid_candidate_node(node), "Node [{}] should be a valid candidate", node);
        }
    }

    #[test]
    fn test_extract_region_various_formats() {
        let priorities = vec![
            "美国".to_string(),
            "日本".to_string(),
            "台湾".to_string(),
            "新加坡".to_string(),
        ];

        // US
        assert_eq!(extract_region("🇺🇸 美国 01 [x1.0]", &priorities), "美国");
        assert_eq!(extract_region("US - Los Angeles 02", &priorities), "美国");
        assert_eq!(extract_region("United States Silicon Valley", &priorities), "美国");
        assert_eq!(extract_region("America Phoenix 03", &priorities), "美国");

        // JP
        assert_eq!(extract_region("🇯🇵 日本 01", &priorities), "日本");
        assert_eq!(extract_region("JP Tokyo 03", &priorities), "日本");
        assert_eq!(extract_region("Japan Osaka 05", &priorities), "日本");

        // TW
        assert_eq!(extract_region("🇹🇼 台湾 01", &priorities), "台湾");
        assert_eq!(extract_region("TW Taipei 02", &priorities), "台湾");
        assert_eq!(extract_region("Taiwan 03", &priorities), "台湾");

        // SG
        assert_eq!(extract_region("🇸🇬 新加坡 01", &priorities), "新加坡");
        assert_eq!(extract_region("SG Singapore 02", &priorities), "新加坡");
        assert_eq!(extract_region("狮城 03", &priorities), "新加坡");

        // Other supported region
        assert_eq!(extract_region("🇩🇪 德国 01", &priorities), "德国");
        assert_eq!(extract_region("UK London 01", &priorities), "英国");

        // Unknown
        assert_eq!(extract_region("Node-XYZ-Custom", &priorities), "其他");
    }

    #[test]
    fn test_two_tier_region_priority_selection() {
        let manager = StatsManager::new(
            None,
            true,
            20.0,
            7.0 * 86400.0,
            15,
            180,
            2,
            vec!["美国".to_string(), "日本".to_string(), "台湾".to_string(), "新加坡".to_string()],
            2, // Region consecutive failure threshold: 2
            300, // Region failure cooldown: 300s
            false,
        );
        let hour = 12;

        let candidates = vec![
            "🇺🇸 美国 01".to_string(),
            "🇺🇸 美国 02".to_string(),
            "🇯🇵 日本 01".to_string(),
            "🇯🇵 日本 02".to_string(),
            "🇸🇬 新加坡 01".to_string(),
        ];

        // 1. Initial selection: "美国" is priority #1, so an US node must be chosen
        let best = manager.select_best_node(hour, &candidates, &[], None);
        assert!(best.as_deref().unwrap().contains("美国"), "Expected US node initially, got {:?}", best);

        // 2. Both US nodes succeed or fail: let's record 1 failure on US 01
        manager.record_failure("🇺🇸 美国 01", hour);
        assert_eq!(manager.get_region_consecutive_failures("美国"), 1);
        assert!(!manager.is_region_cooling_down("美国", StatsManager::now_sec()));

        // Since US is not in cooldown and US 02 is healthy, US 02 should be selected
        let next_us = manager.select_best_node(hour, &candidates, &["🇺🇸 美国 01".to_string()], Some("🇺🇸 美国 01"));
        assert_eq!(next_us, Some("🇺🇸 美国 02".to_string()));

        // 3. Now US 02 ALSO fails! That makes 2 consecutive failures for region "美国"
        manager.record_failure("🇺🇸 美国 02", hour);
        assert_eq!(manager.get_region_consecutive_failures("美国"), 2);
        assert!(manager.is_region_cooling_down("美国", StatsManager::now_sec()));

        // 4. Since "美国" is now cooling down, Tier 1 escalates to priority #2: "日本"!
        let best_after_us_cooldown = manager.select_best_node(hour, &candidates, &[], None);
        assert!(
            best_after_us_cooldown.as_deref().unwrap().contains("日本"),
            "Expected Japan node after US cooldown, got {:?}",
            best_after_us_cooldown
        );

        // 5. If Japan also experiences 2 consecutive failures, it escalates to Singapore!
        manager.record_failure("🇯🇵 日本 01", hour);
        manager.record_failure("🇯🇵 日本 02", hour);
        assert!(manager.is_region_cooling_down("日本", StatsManager::now_sec()));

        let best_after_jp_cooldown = manager.select_best_node(hour, &candidates, &[], None);
        assert_eq!(
            best_after_jp_cooldown,
            Some("🇸🇬 新加坡 01".to_string()),
            "Expected Singapore node after JP cooldown"
        );
    }

    #[test]
    fn test_db_version_mismatch_auto_rebuild() {
        let temp_dir = std::env::temp_dir().join(format!("gyro-test-dbver-{}", rand::random::<u32>()));
        let _ = std::fs::create_dir_all(&temp_dir);
        let file_path = temp_dir.join("stats.db");

        // 1. Manually create an older schema DB (version 1) with an old table
        {
            let conn = Connection::open(&file_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta (key, value) VALUES ('schema_version', '1');
                 CREATE TABLE legacy_data (id INTEGER PRIMARY KEY, info TEXT);
                 INSERT INTO legacy_data (id, info) VALUES (1, 'old-data');",
            ).unwrap();
        }

        // 2. Open with StatsManager (requires CURRENT_DB_VERSION = 2)
        let manager = StatsManager::new(
            Some(file_path.clone()),
            true,
            20.0,
            7.0 * 86400.0,
            15,
            180,
            2,
            vec!["美国".to_string(), "日本".to_string()],
            3,
            300,
            false,
        );

        // Record some data to verify functionality
        manager.record_success("test-node-us", 10);

        // 3. Inspect database on disk: legacy_data must be gone, meta.schema_version must be '2', region_health exists
        {
            let conn = Connection::open(&file_path).unwrap();
            let ver: String = conn
                .query_row(
                    "SELECT value FROM meta WHERE key = 'schema_version'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(ver, "2");

            // region_health table exists
            let has_region_health: bool = conn
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='region_health'",
                    [],
                    |_| Ok(true),
                )
                .unwrap_or(false);
            assert!(has_region_health);

            // legacy_data is dropped
            let has_legacy_table: bool = conn
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='legacy_data'",
                    [],
                    |_| Ok(true),
                )
                .unwrap_or(false);
            assert!(!has_legacy_table, "legacy_data table should have been dropped on version mismatch rebuild");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
