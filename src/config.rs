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

    /// Maximum retry attempts on retriable errors
    #[arg(long, env = "AGY_GYRO_MAX_RETRIES", default_value_t = 15)]
    pub max_retries: u32,

    /// Initial retry backoff delay in milliseconds
    #[arg(long, env = "AGY_GYRO_INITIAL_DELAY_MS", default_value_t = 1000)]
    pub initial_delay_ms: u64,

    /// Maximum retry backoff delay in milliseconds
    #[arg(long, env = "AGY_GYRO_MAX_DELAY_MS", default_value_t = 60000)]
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

    /// Clash proxy group to rotate inside (e.g. 台美新日)
    #[arg(long, env = "AGY_GYRO_CLASH_GROUP", default_value = "台美新日")]
    pub clash_group: String,

    /// Clash parent selector to ensure it points to clash_group (e.g. GLOBAL)
    #[arg(long, env = "AGY_GYRO_CLASH_PARENT", default_value = "GLOBAL")]
    pub clash_parent: String,

    /// Disable Clash auto-switch on retry
    #[arg(long, env = "AGY_GYRO_NO_CLASH_SWITCH", default_value_t = false)]
    pub no_clash_switch: bool,
}

impl Config {
    pub fn is_jitter_enabled(&self) -> bool {
        !self.no_jitter
    }

    pub fn is_buffer_enabled(&self) -> bool {
        !self.no_buffer
    }

    pub fn model_redirects(&self) -> Vec<(&str, &str)> {
        self.redirect_model
            .iter()
            .filter_map(|s| s.split_once(':'))
            .collect()
    }
}
