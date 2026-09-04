// SPDX-License-Identifier: MIT

use crate::config::Config;
use crate::retry::{
    calculate_backoff, is_location_block_error, is_retriable_error, is_retriable_status,
    parse_in_stream_error, parse_retry_after,
};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, OriginalUri, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
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

async fn trigger_clash_switch(state: &Arc<ProxyState>) {
    if state.config.no_clash_switch {
        return;
    }
    let api = state.config.clash_api.trim_end_matches('/');
    let secret = state.config.clash_secret.clone();
    let group = state.config.clash_group.clone();
    let parent = state.config.clash_parent.clone();
    // Use a fresh client without proxy to talk to local Clash API
    let client = match reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build() {
        Ok(c) => c,
        Err(e) => {
            warn!("Clash switch: failed to build client: {}", e);
            return;
        }
    };
    let auth = if secret.is_empty() {
        None
    } else {
        Some(format!("Bearer {}", secret))
    };
    // 1. Ensure parent points to group
    let parent_url = format!("{}/proxies/{}", api, parent);
    let group_url = format!("{}/proxies/{}", api, group);
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = &auth {
        if let Ok(val) = token.parse::<HeaderValue>() {
            headers.insert(reqwest::header::AUTHORIZATION, val);
        }
    }
    // Best-effort: switch parent
    match client.get(&parent_url).headers(headers.clone()).send().await {
        Ok(resp) if resp.status().is_success() => {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                let now = json.get("now").and_then(|v| v.as_str()).unwrap_or("");
                if now != group {
                    let res = client
                        .put(&parent_url)
                        .headers(headers.clone())
                        .json(&serde_json::json!({"name": group}))
                        .send()
                        .await;
                    match res {
                        Ok(r) if r.status().is_success() => {
                            info!("Clash switch: {}: {} -> {}", parent, now, group)
                        }
                        Ok(r) => warn!("Clash switch parent failed: {} {}", r.status(), r.text().await.unwrap_or_default()),
                        Err(e) => warn!("Clash switch parent error: {}", e),
                    }
                }
            }
        }
        Ok(r) => warn!("Clash get parent failed: {}", r.status()),
        Err(e) => warn!("Clash get parent error: {}", e),
    }
    // 2. Rotate inside group
    match client.get(&group_url).headers(headers.clone()).send().await {
        Ok(resp) if resp.status().is_success() => {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                let all = json.get("all").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                let now = json.get("now").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let all_str: Vec<String> = all.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                if all_str.is_empty() {
                    warn!("Clash switch: group {} has no nodes", group);
                    return;
                }
                let idx = all_str.iter().position(|n| n == &now).map(|i| (i + 1) % all_str.len()).unwrap_or(0);
                let nxt = &all_str[idx];
                if nxt == &now {
                    info!("Clash switch: {} already at {}", group, now);
                    return;
                }
                let res = client
                    .put(&group_url)
                    .headers(headers)
                    .json(&serde_json::json!({"name": nxt}))
                    .send()
                    .await;
                match res {
                    Ok(r) if r.status().is_success() => info!("Clash switch: {}: {} -> {}", group, now, nxt),
                    Ok(r) => warn!("Clash switch group failed: {} {}", r.status(), r.text().await.unwrap_or_default()),
                    Err(e) => warn!("Clash switch group error: {}", e),
                }
            }
        }
        Ok(r) => warn!("Clash get group failed: {}", r.status()),
        Err(e) => warn!("Clash get group error: {}", e),
    }
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

/// Rewrites the model identifier in Gemini API paths if it matches any configured redirect rule.
pub fn rewrite_model_path(path_and_query: &str, redirects: &[(&str, &str)]) -> String {
    if redirects.is_empty() {
        return path_and_query.to_string();
    }

    if let Some(models_idx) = path_and_query.find("/models/") {
        let prefix_end = models_idx + "/models/".len();
        let prefix = &path_and_query[..prefix_end];
        let rest = &path_and_query[prefix_end..];

        let model_end = rest.find([':', '/', '?']).unwrap_or(rest.len());

        if model_end > 0 {
            let model_name = &rest[..model_end];
            for &(from, to) in redirects {
                if model_name == from {
                    let remainder = &rest[model_end..];
                    return format!("{}{}{}", prefix, to, remainder);
                }
            }
        }
    }
    path_and_query.to_string()
}

pub async fn proxy_handler(
    State(state): State<Arc<ProxyState>>,
    method: Method,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let raw_path_and_query = original_uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");

    let redirects = state.config.model_redirects();
    let effective_path = if !redirects.is_empty() {
        let rewritten = rewrite_model_path(raw_path_and_query, &redirects);
        if rewritten != raw_path_and_query {
            debug!(
                "Redirecting model in request path: {} -> {}",
                raw_path_and_query, rewritten
            );
        }
        rewritten
    } else {
        raw_path_and_query.to_string()
    };

    let target_url_str = format!("{}{}", state.upstream_base, effective_path);
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

                        trigger_clash_switch(&state).await;
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    } else {
                        warn!(
                            "Max retries ({}) reached for status {}. Forwarding upstream error to client.",
                            max_retries, status
                        );
                    }
                } else if status == StatusCode::BAD_REQUEST {
                    // Handle 400 location block as retriable with Clash switch
                    let mut snippet = String::new();
                    if let Ok(Some(chunk)) = upstream_res.chunk().await {
                        let bytes = &chunk[..chunk.len().min(512)];
                        snippet = String::from_utf8_lossy(bytes).trim().to_string();
                        if chunk.len() > 512 {
                            snippet.push_str("...");
                        }
                    }
                    if is_location_block_error(status, &snippet) {
                        if attempt < max_retries {
                            let delay = calculate_backoff(
                                attempt,
                                initial_delay,
                                max_delay,
                                with_jitter,
                                None,
                            );
                            warn!(
                                "Upstream returned location-block 400 for {} {}. Snippet: {}. Retrying in {:?} (attempt {}/{}) with Clash switch",
                                method, target_url, snippet, delay, attempt + 1, max_retries
                            );
                            trigger_clash_switch(&state).await;
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                            continue;
                        } else {
                            warn!(
                                "Max retries ({}) reached for location-block 400. Forwarding to client.",
                                max_retries
                            );
                        }
                    } else {
                        debug!(
                            "Request {} {} returned non-retriable 400: {}",
                            method, target_url, snippet
                        );
                    }
                    // Forward the 400 to client if not retried
                    let mut client_res_builder = Response::builder().status(status.as_u16());
                    for (name, val) in upstream_res.headers() {
                        if !is_hop_by_hop(name) {
                            client_res_builder = client_res_builder.header(name, val);
                        }
                    }
                    let body = if snippet.is_empty() {
                        Body::empty()
                    } else {
                        Body::from(snippet)
                    };
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
                } else if status.is_success() {
                    if attempt > 0 {
                        info!(
                            "Request {} {} succeeded after {} retries (status {})",
                            method, target_url, attempt, status
                        );
                    } else {
                        debug!(
                            "Request {} {} succeeded (status {})",
                            method, target_url, status
                        );
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

                if state.config.is_buffer_enabled() {
                    let mut buffered_chunks: Vec<Bytes> = Vec::new();
                    let mut stream_error = None;
                    let mut in_stream_err_details = None;
                    let mut chunk_index = 0;

                    while let Some(chunk_res) = stream.next().await {
                        match chunk_res {
                            Ok(chunk) => {
                                if let Some((in_stream_status, err_msg)) =
                                    parse_in_stream_error(&chunk).filter(|(status, msg)| {
                                        is_retriable_status(*status)
                                            || is_location_block_error(*status, msg)
                                    })
                                {
                                    in_stream_err_details =
                                        Some((chunk_index, in_stream_status, err_msg));
                                    buffered_chunks.push(chunk);
                                    break;
                                }
                                buffered_chunks.push(chunk);
                                chunk_index += 1;
                            }
                            Err(err) => {
                                stream_error = Some((chunk_index, err));
                                break;
                            }
                        }
                    }

                    if let Some((chunk_idx, in_stream_status, err_msg)) = in_stream_err_details {
                        if attempt < max_retries {
                            let delay = calculate_backoff(
                                attempt,
                                initial_delay,
                                max_delay,
                                with_jitter,
                                None,
                            );

                            warn!(
                                "Upstream returned in-stream retriable error {} ({}) at chunk #{} for {} {}. Retrying in {:?} (attempt {}/{})...",
                                in_stream_status,
                                err_msg,
                                chunk_idx + 1,
                                method,
                                target_url,
                                delay,
                                attempt + 1,
                                max_retries
                            );

                            trigger_clash_switch(&state).await;
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                            continue;
                        } else {
                            warn!(
                                "Max retries ({}) reached for in-stream error {}. Forwarding buffered stream to client.",
                                max_retries, in_stream_status
                            );
                        }
                    } else if let Some((chunk_idx, err)) = stream_error {
                        if is_retriable_error(&err) && attempt < max_retries {
                            let delay = calculate_backoff(
                                attempt,
                                initial_delay,
                                max_delay,
                                with_jitter,
                                None,
                            );

                            warn!(
                                "Upstream stream error at chunk #{} for {} {}: {}. Retrying in {:?} (attempt {}/{})...",
                                chunk_idx + 1,
                                method,
                                target_url,
                                err,
                                delay,
                                attempt + 1,
                                max_retries
                            );

                            trigger_clash_switch(&state).await;
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                            continue;
                        } else {
                            error!(
                                "Upstream error reading stream at chunk #{} for {} {}: {}",
                                chunk_idx + 1,
                                method,
                                target_url,
                                err
                            );
                            return (
                                StatusCode::BAD_GATEWAY,
                                format!("Bad Gateway: upstream stream read failed: {}", err),
                            )
                                .into_response();
                        }
                    }

                    // All chunks buffered cleanly (or retries exhausted) - stream buffered chunks to client
                    let body_stream = futures_util::stream::iter(
                        buffered_chunks
                            .into_iter()
                            .map(Ok::<_, std::convert::Infallible>),
                    );
                    let body = Body::from_stream(body_stream);

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

                // Passthrough mode (--no-buffer): Peek at the initial stream chunk to catch in-stream 503 / 429 errors early
                match stream.next().await {
                    Some(Ok(first_chunk)) => {
                        if let Some((in_stream_status, err_msg)) =
                            parse_in_stream_error(&first_chunk).filter(|(status, msg)| {
                                is_retriable_status(*status)
                                    || is_location_block_error(*status, msg)
                            })
                        {
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

                                trigger_clash_switch(&state).await;
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
                                method,
                                target_url,
                                err,
                                delay,
                                attempt + 1,
                                max_retries
                            );

                            trigger_clash_switch(&state).await;
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

                    trigger_clash_switch(&state).await;
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
