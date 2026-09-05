// SPDX-License-Identifier: MIT

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "agy-gyro",
    author,
    version,
    about = "Gemini API retry proxy and wrapper for Antigravity CLI (agy)"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    #[command(flatten)]
    pub wrapper_args: WrapperArgs,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Run standalone proxy server in the foreground
    Server(ServerArgs),
    /// Run agy CLI wrapped with the local proxy (default mode)
    Run(WrapperArgs),
    /// Show node reliability priority rankings and statistics
    Stats(StatsArgs),
}

#[derive(Args, Debug, Clone)]
pub struct StatsArgs {
    #[command(flatten)]
    pub config: Config,

    /// Specific hour (0-23) to inspect (default: current local hour)
    #[arg(long)]
    pub hour: Option<u8>,

    /// Show detailed rankings for all 24 hours
    #[arg(long, default_value_t = false)]
    pub all_hours: bool,
}

#[derive(Args, Debug, Clone)]
pub struct ServerArgs {
    #[command(flatten)]
    pub config: Config,
}

impl ServerArgs {
    pub fn resolved_port(&self) -> u16 {
        self.config.port.unwrap_or(8080)
    }
}

#[derive(Args, Debug, Clone)]
pub struct WrapperArgs {
    #[command(flatten)]
    pub config: Config,

    /// Executable path or command name for Antigravity CLI
    #[arg(long, env = "AGY_GYRO_AGY_PATH", default_value = "agy")]
    pub agy_path: String,

    /// Optional log file path for agy-gyro proxy tracing logs
    #[arg(long, env = "AGY_GYRO_LOG_FILE")]
    pub log_file: Option<PathBuf>,

    /// Raw arguments passed through directly to agy
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub agy_args: Vec<String>,
}

impl WrapperArgs {
    pub fn resolved_port(&self) -> u16 {
        self.config.port.unwrap_or(0)
    }
}

#[derive(Args, Debug, Clone)]
pub struct Config {
    /// Host address to bind the proxy server to
    #[arg(short = 'H', long, env = "AGY_GYRO_HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Port to listen on (default: dynamic free port in wrapper mode, 8080 in server mode)
    #[arg(short = 'p', long, env = "AGY_GYRO_PORT")]
    pub port: Option<u16>,

    /// Upstream Gemini API base URL
    #[arg(
        short = 'u',
        long,
        env = "AGY_GYRO_UPSTREAM_URL",
        default_value = "https://generativelanguage.googleapis.com"
    )]
    pub upstream: String,

    /// Upstream Cloud Code API base URL (for Antigravity OAuth mode)
    #[arg(
        long,
        env = "AGY_GYRO_CLOUDCODE_URL",
        default_value = "https://daily-cloudcode-pa.googleapis.com"
    )]
    pub cloudcode_upstream: String,

    /// Maximum retry attempts on retriable errors (0 = unlimited)
    #[arg(long, env = "AGY_GYRO_MAX_RETRIES", default_value_t = 10000)]
    pub max_retries: u32,

    /// Initial retry backoff delay in milliseconds
    #[arg(long, env = "AGY_GYRO_INITIAL_DELAY_MS", default_value_t = 200)]
    pub initial_delay_ms: u64,

    /// Maximum retry backoff delay in milliseconds
    #[arg(long, env = "AGY_GYRO_MAX_DELAY_MS", default_value_t = 3000)]
    pub max_delay_ms: u64,

    /// Disable jitter in exponential backoff calculation
    #[arg(long, env = "AGY_GYRO_NO_JITTER", default_value_t = false)]
    pub no_jitter: bool,

    /// Disable full stream chunk buffering (stream chunks immediately to client after first chunk)
    #[arg(long, env = "AGY_GYRO_NO_BUFFER", default_value_t = false)]
    pub no_buffer: bool,

    /// Client request timeout in seconds (for long thinking / generation requests)
    #[arg(long, env = "AGY_GYRO_REQUEST_TIMEOUT_SECS", default_value_t = 600)]
    pub request_timeout_secs: u64,

    /// Redirect model requests in format "FROM:TO" (e.g. "gemini-3.7-flash:gemini-3.8-flash")
    #[arg(long, env = "AGY_GYRO_REDIRECT_MODEL", value_delimiter = ',')]
    pub redirect_model: Vec<String>,

    /// Clash API base URL for auto-switch on retry (e.g. http://127.0.0.1:9097)
    #[arg(long, env = "AGY_GYRO_CLASH_API", default_value = "http://127.0.0.1:9097")]
    pub clash_api: String,

    /// Clash external-controller secret
    #[arg(long, env = "AGY_GYRO_CLASH_SECRET", default_value = "set-your-secret")]
    pub clash_secret: String,

    /// Clash proxy group to rotate inside (e.g. Proxy)
    #[arg(long, env = "AGY_GYRO_CLASH_GROUP", default_value = "Proxy")]
    pub clash_group: String,

    /// Clash parent selector to ensure it points to clash_group (e.g. GLOBAL, optional, empty by default)
    #[arg(long, env = "AGY_GYRO_CLASH_PARENT", default_value = "")]
    pub clash_parent: String,

    /// Comma-separated two-tier region priority list (e.g. "美国,日本,台湾,新加坡")
    #[arg(
        long,
        env = "AGY_GYRO_REGION_PRIORITY",
        value_delimiter = ',',
        default_value = "美国,日本,台湾,新加坡"
    )]
    pub region_priority: Vec<String>,

    /// Consecutive failure threshold for an entire region before cooling it down (default: 3)
    #[arg(long, env = "AGY_GYRO_REGION_CONSECUTIVE_FAILURE_THRESHOLD", default_value_t = 3)]
    pub region_consecutive_failure_threshold: u32,

    /// Cooldown duration in seconds for a region after reaching failure threshold (default: 300.0s / 5 min)
    #[arg(long, env = "AGY_GYRO_REGION_FAILURE_COOLDOWN_SECS", default_value_t = 300.0)]
    pub region_failure_cooldown_secs: f64,

    /// Disable two-tier region priority switching (flat node priority fallback)
    #[arg(long, env = "AGY_GYRO_NO_REGION_PRIORITY", default_value_t = false)]
    pub no_region_priority: bool,

    /// Disable Clash auto-switch on retry
    #[arg(long, env = "AGY_GYRO_NO_CLASH_SWITCH", default_value_t = false)]
    pub no_clash_switch: bool,

    /// Retry on all non-2xx responses (including 400/401/403) with Clash switch. By default only 429/5xx/408 and location-block 400 are retried.
    #[arg(long, env = "AGY_GYRO_RETRY_ALL", default_value_t = false)]
    pub retry_all: bool,

    /// Optional path to node reliability statistics file (defaults to ~/.gemini/antigravity-cli/gyro.db)
    #[arg(long, env = "AGY_GYRO_STATS_FILE")]
    pub stats_file: Option<PathBuf>,

    /// Disable node reliability statistics and priority-based switching
    #[arg(long, env = "AGY_GYRO_NO_STATS", default_value_t = false)]
    pub no_stats: bool,

    /// Maximum effective sample capacity per bucket
    #[arg(long, env = "AGY_GYRO_STATS_MAX_SAMPLES", default_value_t = 20.0)]
    pub stats_max_samples: f64,

    /// Exponential half-life in days for time decay
    #[arg(long, env = "AGY_GYRO_STATS_HALF_LIFE_DAYS", default_value_t = 7.0)]
    pub stats_half_life_days: f64,

    /// Burst window in seconds to damp rapid-fire consecutive requests
    #[arg(long, env = "AGY_GYRO_STATS_BURST_WINDOW_SECS", default_value_t = 15)]
    pub stats_burst_window_secs: i64,

    /// Cooldown window in seconds between Clash proxy node switches to prevent multi-instance switching storms
    #[arg(long, env = "AGY_GYRO_CLASH_SWITCH_COOLDOWN_SECS", default_value_t = 5.0)]
    pub clash_switch_cooldown_secs: f64,

    /// Duration in hours to quarantine nodes that return 400 location block (default: 12.0 hours)
    #[arg(long, env = "AGY_GYRO_QUARANTINE_HOURS", default_value_t = 12.0)]
    pub node_quarantine_hours: f64,

    /// Disable fast pre-flight probing of candidate nodes before exposing user requests
    #[arg(long, env = "AGY_GYRO_NO_PREFLIGHT_PROBE", default_value_t = false)]
    pub no_preflight_probe: bool,

    /// Maximum local retries on the consensus anchor node before switching Clash proxy (default: 5)
    #[arg(long, env = "AGY_GYRO_ANCHOR_HYSTERESIS_RETRIES", default_value_t = 5)]
    pub anchor_hysteresis_retries: u32,

    /// Number of consecutive failures before temporarily cooling down a node to let lower-priority nodes be tried (default: 2)
    #[arg(long, env = "AGY_GYRO_CONSECUTIVE_FAILURE_THRESHOLD", default_value_t = 2)]
    pub consecutive_failure_threshold: u32,

    /// Duration in seconds to cool down a node after exceeding consecutive failure threshold (default: 180.0s / 3 min)
    #[arg(long, env = "AGY_GYRO_FAILURE_COOLDOWN_SECS", default_value_t = 180.0)]
    pub failure_cooldown_secs: f64,
}

impl Config {
    pub fn is_jitter_enabled(&self) -> bool {
        !self.no_jitter
    }

    pub fn is_buffer_enabled(&self) -> bool {
        !self.no_buffer
    }

    pub fn is_preflight_probe_enabled(&self) -> bool {
        !self.no_preflight_probe
    }

    pub fn node_quarantine_secs(&self) -> i64 {
        (self.node_quarantine_hours * 3600.0).max(0.0) as i64
    }

    pub fn failure_cooldown_duration_secs(&self) -> i64 {
        self.failure_cooldown_secs.max(0.0) as i64
    }

    pub fn clash_switch_cooldown_ms(&self) -> i64 {
        (self.clash_switch_cooldown_secs * 1000.0).max(0.0) as i64
    }

    pub fn model_redirects(&self) -> Vec<(&str, &str)> {
        self.redirect_model
            .iter()
            .filter_map(|s| s.split_once(':'))
            .collect()
    }

    pub fn resolved_stats_file(&self) -> PathBuf {
        self.stats_file
            .clone()
            .unwrap_or_else(crate::stats::resolve_default_stats_path)
    }

    pub fn region_failure_cooldown_duration_secs(&self) -> i64 {
        self.region_failure_cooldown_secs.max(0.0) as i64
    }

    pub fn is_region_priority_enabled(&self) -> bool {
        !self.no_region_priority
    }

    pub fn stats_half_life_secs(&self) -> f64 {
        self.stats_half_life_days * 86400.0
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: None,
            upstream: "https://generativelanguage.googleapis.com".to_string(),
            cloudcode_upstream: "https://daily-cloudcode-pa.googleapis.com".to_string(),
            max_retries: 10000,
            initial_delay_ms: 200,
            max_delay_ms: 3000,
            no_jitter: false,
            no_buffer: false,
            request_timeout_secs: 600,
            redirect_model: Vec::new(),
            clash_api: "http://127.0.0.1:9097".to_string(),
            clash_secret: "set-your-secret".to_string(),
            clash_group: "Proxy".to_string(),
            clash_parent: "".to_string(),
            region_priority: vec![
                "美国".to_string(),
                "日本".to_string(),
                "台湾".to_string(),
                "新加坡".to_string(),
            ],
            region_consecutive_failure_threshold: 3,
            region_failure_cooldown_secs: 300.0,
            no_region_priority: false,
            no_clash_switch: false,
            retry_all: false,
            stats_file: None,
            no_stats: false,
            stats_max_samples: 20.0,
            stats_half_life_days: 7.0,
            stats_burst_window_secs: 15,
            clash_switch_cooldown_secs: 5.0,
            node_quarantine_hours: 12.0,
            no_preflight_probe: false,
            anchor_hysteresis_retries: 5,
            consecutive_failure_threshold: 2,
            failure_cooldown_secs: 180.0,
        }
    }
}
