// SPDX-License-Identifier: MIT

use crate::config::Config;
use crate::retry::{calculate_backoff, is_retriable_error, is_retriable_status, parse_retry_after};
use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, State},
    http::{HeaderMap, HeaderName, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

pub struct ProxyState {
    pub config: Config,
    pub client: reqwest::Client,
}

pub fn create_router(state: Arc<ProxyState>) -> Router {
    Router::new()
        .route("/", any(proxy_handler))
        .route("/{*path}", any(proxy_handler))
        .with_state(state)
}

fn is_hop_by_hop(header_name: &HeaderName) -> bool {
    matches!(
        header_name.as_str().to_lowercase().as_str(),
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

    let target_url = format!(
        "{}{}",
        state.config.upstream.trim_end_matches('/'),
        path_and_query
    );

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

        let mut req_builder = state.client.request(method.clone(), &target_url);

        // Forward headers
        for (header_name, header_val) in headers.iter() {
            if !is_hop_by_hop(header_name) {
                req_builder = req_builder.header(header_name, header_val);
            }
        }

        // Set request body
        req_builder = req_builder.body(body.clone());

        match req_builder.send().await {
            Ok(upstream_res) => {
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

                        let error_body = upstream_res.text().await.unwrap_or_default();
                        let snippet = if error_body.len() > 200 {
                            format!("{}...", &error_body[..200])
                        } else {
                            error_body
                        };

                        warn!(
                            "Upstream returned retriable status {} for {} {}. Retrying in {:?} (attempt {}/{}). Response: {}",
                            status,
                            method,
                            target_url,
                            delay,
                            attempt + 1,
                            max_retries,
                            snippet.trim()
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

                // Build client response
                let mut client_res_builder = Response::builder().status(status.as_u16());

                for (name, val) in upstream_res.headers() {
                    let h_name = HeaderName::from_bytes(name.as_str().as_bytes());
                    if let Ok(valid_name) = h_name {
                        if !is_hop_by_hop(&valid_name) {
                            client_res_builder = client_res_builder.header(valid_name, val);
                        }
                    }
                }

                let stream = upstream_res.bytes_stream();
                let body = Body::from_stream(stream);

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
