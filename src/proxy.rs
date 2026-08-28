// SPDX-License-Identifier: MIT

use crate::config::Config;
use crate::retry::{
    calculate_backoff, is_retriable_error, is_retriable_status, parse_in_stream_error,
    parse_retry_after,
};
use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, OriginalUri, State},
    http::{HeaderMap, HeaderName, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

pub struct ProxyState {
    pub config: Config,
    pub client: reqwest::Client,
    pub upstream_base: String,
}

impl ProxyState {
    pub fn new(config: Config, client: reqwest::Client) -> Self {
        let upstream_base = config.upstream.trim_end_matches('/').to_string();
        Self {
            config,
            client,
            upstream_base,
        }
    }
}

/// Builds a tuned reqwest HTTP client from the proxy configuration.
pub fn build_http_client(config: &Config) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .pool_max_idle_per_host(32)
        .tcp_keepalive(Duration::from_secs(60))
        .http2_keep_alive_interval(Duration::from_secs(15))
        .http2_keep_alive_while_idle(true)
        .tcp_nodelay(true)
        .build()
}


pub fn create_router(state: Arc<ProxyState>) -> Router {
    Router::new()
        .route("/", any(proxy_handler))
        .route("/{*path}", any(proxy_handler))
        .layer(DefaultBodyLimit::disable())
        .with_state(state)
}

fn is_hop_by_hop(header_name: &HeaderName) -> bool {
    matches!(
        header_name.as_str(),
        "host"
            | "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
            | "content-encoding"
    )
}

pub async fn proxy_handler(
    State(state): State<Arc<ProxyState>>,
    method: Method,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path_and_query = original_uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");

    let target_url_str = format!("{}{}", state.upstream_base, path_and_query);
    let target_url = match reqwest::Url::parse(&target_url_str) {
        Ok(url) => url,
        Err(err) => {
            error!("Failed to parse target URL {}: {}", target_url_str, err);
            return (
                StatusCode::BAD_REQUEST,
                format!("Invalid target URL: {}", err),
            )
                .into_response();
        }
    };

    let initial_delay = Duration::from_millis(state.config.initial_delay_ms);
    let max_delay = Duration::from_millis(state.config.max_delay_ms);
    let with_jitter = state.config.is_jitter_enabled();
    let max_retries = state.config.max_retries;

    let mut attempt = 0;

    loop {
        debug!(
            "Forwarding request {} {} (attempt {}/{})",
            method, target_url, attempt, max_retries
        );

        let mut req_builder = state.client.request(method.clone(), target_url.clone());

        // Forward headers
        for (header_name, header_val) in headers.iter() {
            if !is_hop_by_hop(header_name) {
                req_builder = req_builder.header(header_name, header_val);
            }
        }

        // Set request body
        req_builder = req_builder.body(body.clone());

        match req_builder.send().await {
            Ok(mut upstream_res) => {
                let status = upstream_res.status();

                if is_retriable_status(status) {
                    if attempt < max_retries {
                        let retry_after_duration = upstream_res
                            .headers()
                            .get(reqwest::header::RETRY_AFTER)
                            .and_then(|val| val.to_str().ok())
                            .and_then(parse_retry_after);

                        let delay = calculate_backoff(
                            attempt,
                            initial_delay,
                            max_delay,
                            with_jitter,
                            retry_after_duration,
                        );

                        let mut snippet = String::new();
                        if let Ok(Some(chunk)) = upstream_res.chunk().await {
                            let bytes = &chunk[..chunk.len().min(200)];
                            snippet = String::from_utf8_lossy(bytes).trim().to_string();
                            if chunk.len() > 200 {
                                snippet.push_str("...");
                            }
                        }

                        warn!(
                            "Upstream returned retriable status {} for {} {}. Retrying in {:?} (attempt {}/{}). Response: {}",
                            status,
                            method,
                            target_url,
                            delay,
                            attempt + 1,
                            max_retries,
                            snippet
                        );

                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    } else {
                        warn!(
                            "Max retries ({}) reached for status {}. Forwarding upstream error to client.",
                            max_retries, status
                        );
                    }
                } else if status.is_success() {
                    if attempt > 0 {
                        info!(
                            "Request {} {} succeeded after {} retries (status {})",
                            method, target_url, attempt, status
                        );
                    } else {
                        debug!("Request {} {} succeeded (status {})", method, target_url, status);
                    }
                } else {
                    debug!(
                        "Request {} {} returned non-retriable status {}",
                        method, target_url, status
                    );
                }

                // Build client response builder
                let mut client_res_builder = Response::builder().status(status.as_u16());

                for (name, val) in upstream_res.headers() {
                    if !is_hop_by_hop(name) {
                        client_res_builder = client_res_builder.header(name, val);
                    }
                }

                let mut stream = upstream_res.bytes_stream();

                // Peek at the initial stream chunk to catch in-stream 503 / 429 errors early
                match stream.next().await {
                    Some(Ok(first_chunk)) => {
                        if let Some((in_stream_status, err_msg)) =
                            parse_in_stream_error(&first_chunk)
                        {
                            if is_retriable_status(in_stream_status) {
                                if attempt < max_retries {
                                    let delay = calculate_backoff(
                                        attempt,
                                        initial_delay,
                                        max_delay,
                                        with_jitter,
                                        None,
                                    );

                                    warn!(
                                        "Upstream returned in-stream retriable error {} ({}) for {} {}. Retrying in {:?} (attempt {}/{})...",
                                        in_stream_status,
                                        err_msg,
                                        method,
                                        target_url,
                                        delay,
                                        attempt + 1,
                                        max_retries
                                    );

                                    tokio::time::sleep(delay).await;
                                    attempt += 1;
                                    continue;
                                } else {
                                    warn!(
                                        "Max retries ({}) reached for in-stream error {}. Forwarding stream to client.",
                                        max_retries, in_stream_status
                                    );
                                }
                            }
                        }

                        // Stream is healthy or retries exhausted: chain first chunk with remainder
                        let full_stream = futures_util::stream::once(async move {
                            Ok::<_, reqwest::Error>(first_chunk)
                        })
                        .chain(stream);

                        let body = Body::from_stream(full_stream);

                        return match client_res_builder.body(body) {
                            Ok(res) => res,
                            Err(err) => {
                                error!("Failed to build proxy response: {}", err);
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    format!("Failed to build proxy response: {}", err),
                                )
                                    .into_response()
                            }
                        };
                    }
                    Some(Err(err)) => {
                        if is_retriable_error(&err) && attempt < max_retries {
                            let delay = calculate_backoff(
                                attempt,
                                initial_delay,
                                max_delay,
                                with_jitter,
                                None,
                            );

                            warn!(
                                "Upstream stream error on initial chunk for {} {}: {}. Retrying in {:?} (attempt {}/{})...",
                                method, target_url, err, delay, attempt + 1, max_retries
                            );

                            tokio::time::sleep(delay).await;
                            attempt += 1;
                            continue;
                        } else {
                            error!(
                                "Upstream error reading initial stream chunk for {} {}: {}",
                                method, target_url, err
                            );
                            return (
                                StatusCode::BAD_GATEWAY,
                                format!("Bad Gateway: upstream stream read failed: {}", err),
                            )
                                .into_response();
                        }
                    }
                    None => {
                        // Empty response body
                        return match client_res_builder.body(Body::empty()) {
                            Ok(res) => res,
                            Err(err) => (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("Failed to build proxy response: {}", err),
                            )
                                .into_response(),
                        };
                    }
                }
            }
            Err(err) => {
                if is_retriable_error(&err) && attempt < max_retries {
                    let delay =
                        calculate_backoff(attempt, initial_delay, max_delay, with_jitter, None);

                    warn!(
                        "Upstream network error for {} {}: {}. Retrying in {:?} (attempt {}/{})...",
                        method,
                        target_url,
                        err,
                        delay,
                        attempt + 1,
                        max_retries
                    );

                    tokio::time::sleep(delay).await;
                    attempt += 1;
                    continue;
                } else {
                    error!(
                        "Upstream error for {} {}: {} (retries exhausted or non-retriable)",
                        method, target_url, err
                    );
                    return (
                        StatusCode::BAD_GATEWAY,
                        format!("Bad Gateway: upstream request failed: {}", err),
                    )
                        .into_response();
                }
            }
        }
    }
}

