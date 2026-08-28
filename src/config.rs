// SPDX-License-Identifier: MIT

use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "agy-gyro",
    author,
    version,
    about = "Gemini API retry proxy with exponential backoff for Antigravity CLI (agy)"
)]
pub struct Config {
    /// Host address to bind the proxy server to
    #[arg(short = 'H', long, env = "HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Port to listen on
    #[arg(short = 'p', long, env = "PORT", default_value_t = 8080)]
    pub port: u16,

    /// Upstream Gemini API base URL
    #[arg(
        short = 'u',
        long,
        env = "UPSTREAM_URL",
        default_value = "https://generativelanguage.googleapis.com"
    )]
    pub upstream: String,

    /// Maximum retry attempts on retriable errors
    #[arg(long, env = "MAX_RETRIES", default_value_t = 15)]
    pub max_retries: u32,

    /// Initial retry backoff delay in milliseconds
    #[arg(long, env = "INITIAL_DELAY_MS", default_value_t = 1000)]
    pub initial_delay_ms: u64,

    /// Maximum retry backoff delay in milliseconds
    #[arg(long, env = "MAX_DELAY_MS", default_value_t = 60000)]
    pub max_delay_ms: u64,

    /// Disable jitter in exponential backoff calculation
    #[arg(long, env = "NO_JITTER", default_value_t = false)]
    pub no_jitter: bool,

    /// Client request timeout in seconds (for long thinking / generation requests)
    #[arg(long, env = "REQUEST_TIMEOUT_SECS", default_value_t = 600)]
    pub request_timeout_secs: u64,
}

impl Config {
    pub fn is_jitter_enabled(&self) -> bool {
        !self.no_jitter
    }
}
