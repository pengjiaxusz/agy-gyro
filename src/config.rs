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

    /// Client request timeout in seconds (for long thinking / generation requests)
    #[arg(long, env = "AGY_GYRO_REQUEST_TIMEOUT_SECS", default_value_t = 600)]
    pub request_timeout_secs: u64,
}

impl Config {
    pub fn is_jitter_enabled(&self) -> bool {
        !self.no_jitter
    }
}
