// SPDX-License-Identifier: MIT

use crate::config::Config;
use crate::retry::{
    calculate_backoff, is_empty_candidate_response, is_location_block_error,
    is_retriable_aggressive, is_retriable_error, is_retriable_in_stream_with_flag,
    is_retriable_status, parse_in_stream_error, parse_retry_after,
};
use crate::stats::StatsManager;
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
    pub cloudcode_base: String,
    pub stats_manager: Arc<StatsManager>,
    pub active_clash_node: Arc<tokio::sync::RwLock<Option<String>>>,
}

impl ProxyState {
    pub fn new(config: Config, client: reqwest::Client) -> Self {
        let upstream_base = config.upstream.trim_end_matches('/').to_string();
        let cloudcode_base = config
            .cloudcode_upstream
            .trim_end_matches('/')
            .to_string();
        let stats_manager = StatsManager::new(
            if config.no_stats {
                None
            } else {
                Some(config.resolved_stats_file())
            },
            !config.no_stats,
            config.stats_max_samples,
            config.stats_half_life_secs(),
            config.stats_burst_window_secs,
        );
        let active_clash_node = Arc::new(tokio::sync::RwLock::new(None));

        Self {
            config,
            client,
            upstream_base,
            cloudcode_base,
            stats_manager,
            active_clash_node,
        }
    }

    /// Select upstream based on path: Cloud Code API (v1internal) vs Gemini API
    pub fn select_upstream(&self, effective_path: &str) -> &str {
        if effective_path.contains("v1internal") {
            &self.cloudcode_base
        } else {
            &self.upstream_base
        }
    }

    /// Gets cached active Clash node or fetches it from Clash API
    pub async fn get_or_fetch_active_node(&self) -> String {
        {
            let guard = self.active_clash_node.read().await;
            if let Some(ref node) = *guard {
                if !node.is_empty() {
                    return node.clone();
                }
            }
        }
        let node = self.fetch_current_clash_node().await;
        if !node.is_empty() {
            let mut guard = self.active_clash_node.write().await;
            *guard = Some(node.clone());
        }
        node
    }

    /// Fetches the current node from Clash API
    pub async fn fetch_current_clash_node(&self) -> String {
        if self.config.no_clash_switch {
            return String::new();
        }
        let api = self.config.clash_api.trim_end_matches('/');
        let secret = &self.config.clash_secret;
        let group = &self.config.clash_group;
        let client = match reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
        {
            Ok(c) => c,
            Err(_) => return String::new(),
        };
        let mut req = client.get(format!("{}/proxies/{}", api, group));
        if !secret.is_empty() {
            req = req.header(reqwest::header::AUTHORIZATION, format!("Bearer {}", secret));
        }
        if let Ok(resp) = req.send().await {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(now) = json.get("now").and_then(|v| v.as_str()) {
                    return now.to_string();
                }
            }
        }
        String::new()
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

async fn trigger_clash_priority_switch(
    state: &Arc<ProxyState>,
    excluded_nodes: &[String],
) -> Option<String> {
    if state.config.no_clash_switch {
        return None;
    }
    let api = state.config.clash_api.trim_end_matches('/');
    let secret = state.config.clash_secret.clone();
    let group = state.config.clash_group.clone();
    let parent = state.config.clash_parent.clone();
    // Use a fresh client without proxy to talk to local Clash API
    let client = match reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build() {
        Ok(c) => c,
        Err(e) => {
            error!("Clash switch: failed to build client: {} (check AGY_GYRO_CLASH_API)", e);
            return None;
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
    // Best-effort: switch parent with verification
    if let Ok(resp) = client.get(&parent_url).headers(headers.clone()).send().await {
        if resp.status().is_success() {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                let now = json.get("now").and_then(|v| v.as_str()).unwrap_or("");
                if now != group {
                    let _ = client
                        .put(&parent_url)
                        .headers(headers.clone())
                        .json(&serde_json::json!({"name": group}))
                        .send()
                        .await;
                    info!("Clash switch: {}: {} -> {}", parent, now, group);
                } else {
                    debug!("Clash parent {} already at {}", parent, group);
                }
            }
        }
    }

    // 2. Query nodes in group
    let group_resp = match client.get(&group_url).headers(headers.clone()).send().await {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            error!("Clash get group failed: {} (group={}, api={})", r.status(), group, api);
            return None;
        }
        Err(e) => {
            error!("Clash get group error: {} (group={}, api={} - check Clash running?)", e, group, api);
            return None;
        }
    };

    let group_json = match group_resp.json::<serde_json::Value>().await {
        Ok(j) => j,
        Err(e) => {
            warn!("Clash get group json parse failed: {} (api={})", e, api);
            return None;
        }
    };

    let all = group_json.get("all").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let now = group_json.get("now").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let all_str: Vec<String> = all.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
    if all_str.is_empty() {
        error!("Clash switch: group {} has no nodes (api={})", group, api);
        return None;
    }

    let hour = StatsManager::current_hour();

    // 3. Select next node: priority-based if stats enabled, else legacy round-robin
    let nxt = if state.stats_manager.is_enabled() {
        let mut combined_excluded = excluded_nodes.to_vec();
        if !now.is_empty() && !combined_excluded.contains(&now) {
            combined_excluded.push(now.clone());
        }
        state
            .stats_manager
            .select_best_node(hour, &all_str, &combined_excluded)
            .unwrap_or_else(|| all_str[0].clone())
    } else {
        let idx = all_str.iter().position(|n| n == &now).map(|i| (i + 1) % all_str.len()).unwrap_or(0);
        all_str[idx].clone()
    };

    if nxt == now && all_str.len() == 1 {
        info!("Clash switch: {} only one node {}, staying", group, now);
        return Some(now);
    }

    let res = client
        .put(&group_url)
        .headers(headers.clone())
        .json(&serde_json::json!({"name": nxt}))
        .send()
        .await;

    match res {
        Ok(r) if r.status().is_success() => {
            // Update cached active node
            {
                let mut guard = state.active_clash_node.write().await;
                *guard = Some(nxt.clone());
            }

            if state.stats_manager.is_enabled() {
                let ranked = state.stats_manager.rank_nodes(hour, &all_str);
                let score = ranked
                    .iter()
                    .find(|(n, _, _, _)| n == &nxt)
                    .map(|(_, s, _, _)| *s)
                    .unwrap_or(0.5);
                info!(
                    "Clash priority switch: {}: [{}] -> [{}] (hour {} reliability: {:.1}%)",
                    group, now, nxt, hour, score * 100.0
                );
            } else {
                info!("Clash switch: {}: {} -> {}", group, now, nxt);
            }

            // Verify switch fast (40ms, with auth)
            tokio::time::sleep(Duration::from_millis(40)).await;
            if let Ok(verify) = client.get(&group_url).headers(headers.clone()).send().await {
                if let Ok(vjson) = verify.json::<serde_json::Value>().await {
                    let verified = vjson.get("now").and_then(|v| v.as_str()).unwrap_or("");
                    if verified != nxt {
                        warn!("Clash switch verify failed: expected {} but got {}", nxt, verified);
                    }
                }
            }
            Some(nxt)
        }
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            error!("Clash switch group failed: {} {} (group={}, api={})", status, body, group, api);
            None
        }
        Err(e) => {
            error!("Clash switch group error: {} (group={}, api={})", e, group, api);
            None
        }
    }
}

pub fn create_router(state: Arc<ProxyState>) -> Router {
    Router::new()
        .route("/", any(proxy_handler))
        .route("/{*path}", any(proxy_handler))
        .layer(DefaultBodyLimit::disable())
        .with_state(state)
}

#[inline]
fn is_generation_path(path: &str) -> bool {
    path.contains("streamGenerateContent")
        || path.contains("generateContent")
        || path.contains("countTokens")
        || path.contains("embedContent")
}

#[inline]
fn should_switch_for_attempt(attempt: u32) -> bool {
    // 3 tries per node: switch on attempt 2,5,8... (0-indexed)
    attempt % 3 == 2
}

async fn handle_failure_and_switch(
    state: &Arc<ProxyState>,
    active_node: &mut String,
    tried_nodes: &mut Vec<String>,
    do_switch: bool,
) {
    let hour = StatsManager::current_hour();
    if !active_node.is_empty() {
        state.stats_manager.record_failure(active_node, hour);
    }
    if do_switch {
        if let Some(new_node) = trigger_clash_priority_switch(state, tried_nodes).await {
            *active_node = new_node.clone();
            if !tried_nodes.contains(&new_node) {
                tried_nodes.push(new_node);
            }
        }
    }
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

    // Select upstream: Gemini vs Cloud Code (Antigravity OAuth)
    let upstream_base = state.select_upstream(&effective_path);
    let target_url_str = format!("{}{}", upstream_base, effective_path);
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

    // Entry log at INFO: one line per incoming client request (before forwarding)
    info!(
        "REQ {} {} -> {} body={}B redirects={:?}",
        method,
        raw_path_and_query,
        target_url,
        body.len(),
        redirects
    );

    let mut tried_nodes: Vec<String> = Vec::new();
    let mut active_node = state.get_or_fetch_active_node().await;
    if !active_node.is_empty() {
        tried_nodes.push(active_node.clone());
    }

    let mut attempt = 0;

    loop {
        info!(
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
                    if max_retries == 0 || attempt < max_retries {
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

                        let do_switch = is_generation_path(&effective_path) && should_switch_for_attempt(attempt);
                        warn!(
                            "Upstream returned retriable status {} for {} {}. Retrying in {:?} (attempt {}/{}, switch={}) . Response: {}",
                            status,
                            method,
                            target_url,
                            delay,
                            attempt + 1,
                            max_retries,
                            do_switch,
                            snippet
                        );

                        handle_failure_and_switch(&state, &mut active_node, &mut tried_nodes, do_switch).await;
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
                    let is_loc = is_location_block_error(status, &snippet);
                    let is_aggressive = state.config.retry_all && is_retriable_aggressive(status, &snippet);
                    if is_loc || is_aggressive {
                        if max_retries == 0 || attempt < max_retries {
                            let delay = calculate_backoff(
                                attempt,
                                initial_delay,
                                max_delay,
                                with_jitter,
                                None,
                            );
                            let kind = if is_loc { "location-block" } else { "aggressive" };
                            // Switch immediately on attempt 0 for location block since retrying on same node is futile
                            let do_switch = is_generation_path(&effective_path) && (is_loc || should_switch_for_attempt(attempt));
                            warn!(
                                "Upstream returned {} 400 for {} {}. Snippet: {}. Retrying in {:?} (attempt {}/{}, switch={}, retry_all={})",
                                kind, method, target_url, snippet, delay, attempt + 1, max_retries, do_switch, state.config.retry_all
                            );
                            handle_failure_and_switch(&state, &mut active_node, &mut tried_nodes, do_switch).await;
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                            continue;
                        } else {
                            warn!(
                                "Max retries ({}) reached for {} 400. Forwarding to client. Snippet: {}",
                                max_retries, if is_loc { "location-block" } else { "aggressive" }, snippet
                            );
                        }
                    } else {
                        info!(
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
                } else if state.config.retry_all && !status.is_success() {
                    // Aggressive mode: retry on any 4xx/5xx (e.g. 401/403/404 and expanded location-block) with Clash switch
                    let mut snippet = String::new();
                    if let Ok(Some(chunk)) = upstream_res.chunk().await {
                        let bytes = &chunk[..chunk.len().min(512)];
                        snippet = String::from_utf8_lossy(bytes).trim().to_string();
                        if chunk.len() > 512 {
                            snippet.push_str("...");
                        }
                    }
                    // is_retriable_aggressive already covers 4xx/5xx + location-block; for retry_all we retry all non-2xx
                    let is_aggressive_retriable = is_retriable_aggressive(status, &snippet);
                    if is_aggressive_retriable {
                        if max_retries == 0 || attempt < max_retries {
                            let delay = calculate_backoff(
                                attempt,
                                initial_delay,
                                max_delay,
                                with_jitter,
                                None,
                            );
                            let do_switch = is_generation_path(&effective_path) && should_switch_for_attempt(attempt);
                            warn!(
                                "Upstream returned aggressive-retriable status {} for {} {}. Snippet: {}. Retrying in {:?} (attempt {}/{}, switch={}) with Clash switch",
                                status, method, target_url, snippet, delay, attempt + 1, max_retries, do_switch
                            );
                            handle_failure_and_switch(&state, &mut active_node, &mut tried_nodes, do_switch).await;
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                            continue;
                        } else {
                            warn!(
                                "Max retries ({}) reached for aggressive status {}. Forwarding to client. Snippet: {}",
                                max_retries, status, snippet
                            );
                        }
                    } else {
                        debug!(
                            "Request {} {} returned non-retriable status {} (aggressive check false): {}",
                            method, target_url, status, snippet
                        );
                    }
                    // Forward the error response to client (after retries exhausted or not retriable)
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
                        info!(
                            "Request {} {} succeeded (status {})",
                            method, target_url, status
                        );
                    }
                } else {
                    info!(
                        "Request {} {} returned non-retriable status {} (retry_all={})",
                        method, target_url, status, state.config.retry_all
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
                                        is_retriable_in_stream_with_flag(
                                            *status,
                                            msg,
                                            state.config.retry_all,
                                        )
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
                        if max_retries == 0 || attempt < max_retries {
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

                            let do_switch = is_generation_path(&effective_path) && should_switch_for_attempt(attempt);
                            handle_failure_and_switch(&state, &mut active_node, &mut tried_nodes, do_switch).await;
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
                        if is_retriable_error(&err) && (max_retries == 0 || attempt < max_retries) {
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

                            let do_switch = is_generation_path(&effective_path) && should_switch_for_attempt(attempt);
                            handle_failure_and_switch(&state, &mut active_node, &mut tried_nodes, do_switch).await;
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
                    } else {
                        // Detect empty candidate response (200 but no meaningful content) - treat as retriable
                        let combined_len: usize = buffered_chunks.iter().map(|c| c.len()).sum();
                        let should_check_empty = if buffered_chunks.is_empty() {
                            // No chunks at all - check if status was 2xx and path is generateContent
                            true
                        } else {
                            combined_len > 0
                        };
                        if should_check_empty {
                            let combined_bytes = if buffered_chunks.is_empty() {
                                Vec::new()
                            } else {
                                let mut v = Vec::with_capacity(combined_len);
                                for c in &buffered_chunks {
                                    v.extend_from_slice(c);
                                }
                                v
                            };
                            // Empty body with success status is retriable (Gemini blank response bug)
                            let is_empty = if buffered_chunks.is_empty() {
                                true
                            } else {
                                is_empty_candidate_response(&combined_bytes)
                            };
                            if is_empty {
                                // Only retry for model generation endpoints (avoid retrying unrelated 200 empty like /models list)
                                let is_gen_path = effective_path.contains(":generateContent")
                                    || effective_path.contains(":streamGenerateContent")
                                    || effective_path.contains(":countTokens")
                                    || effective_path.contains(":embedContent");
                                if is_gen_path {
                                    if max_retries == 0 || attempt < max_retries {
                                        let delay = calculate_backoff(
                                            attempt,
                                            initial_delay,
                                            max_delay,
                                            with_jitter,
                                            None,
                                        );
                                        warn!(
                                            "Upstream returned empty candidate response ({} bytes, {} chunks) for {} {}. Retrying in {:?} (attempt {}/{}) with Clash switch...",
                                            combined_len,
                                            buffered_chunks.len(),
                                            method,
                                            target_url,
                                            delay,
                                            attempt + 1,
                                            max_retries
                                        );
                                        let do_switch = is_generation_path(&effective_path) && should_switch_for_attempt(attempt);
                                        handle_failure_and_switch(&state, &mut active_node, &mut tried_nodes, do_switch).await;
                                        tokio::time::sleep(delay).await;
                                        attempt += 1;
                                        continue;
                                    } else {
                                        warn!(
                                            "Max retries ({}) reached for empty candidate response. Forwarding to client.",
                                            max_retries
                                        );
                                    }
                                } else {
                                    debug!("Empty 200 response for non-generation path {}, forwarding", effective_path);
                                }
                            }
                        }
                    }

                    // All chunks buffered cleanly (or retries exhausted) - stream buffered chunks to client
                    if status.is_success() && !active_node.is_empty() {
                        state.stats_manager.record_success(&active_node, StatsManager::current_hour());
                    }

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

                // Passthrough mode (--no-buffer): Peek at the initial stream chunk to catch in-stream 503 / 429 errors early + empty candidate
                // Note: Only check the FIRST SSE event in the chunk to avoid coalescing mid-stream errors into first_chunk.
                match stream.next().await {
                    Some(Ok(first_chunk)) => {
                        let first_event_err = {
                            // Extract first data: line only
                            let text = String::from_utf8_lossy(&first_chunk);
                            let mut first_data: Option<String> = None;
                            for line in text.lines() {
                                let trimmed = line.trim();
                                if let Some(stripped) = trimmed.strip_prefix("data:") {
                                    let json_str = stripped.trim();
                                    if json_str.starts_with('{') || json_str.starts_with('[') {
                                        first_data = Some(json_str.to_string());
                                        break;
                                    }
                                }
                            }
                            first_data
                                .as_deref()
                                .and_then(|s| crate::retry::parse_in_stream_error(s.as_bytes()))
                                .filter(|(st, msg)| {
                                    is_retriable_in_stream_with_flag(
                                        *st,
                                        msg,
                                        state.config.retry_all,
                                    )
                                })
                        };
                        if let Some((in_stream_status, err_msg)) = first_event_err
                        {
                            if max_retries == 0 || attempt < max_retries {
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

                                let do_switch = is_generation_path(&effective_path) && should_switch_for_attempt(attempt);
                                handle_failure_and_switch(&state, &mut active_node, &mut tried_nodes, do_switch).await;
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

                        // Detect empty candidate on first chunk (Gemini blank response bug) - retry before streaming
                        // Only inspect first SSE event to avoid coalesced mid-stream false positive
                        let first_chunk_is_empty = {
                            let text = String::from_utf8_lossy(&first_chunk);
                            let mut first_data: Option<Vec<u8>> = None;
                            for line in text.lines() {
                                let trimmed = line.trim();
                                if let Some(stripped) = trimmed.strip_prefix("data:") {
                                    let json_str = stripped.trim();
                                    if json_str.starts_with('{') || json_str.starts_with('[') {
                                        first_data = Some(json_str.as_bytes().to_vec());
                                        break;
                                    }
                                }
                                if !trimmed.is_empty() && !trimmed.starts_with(':') {
                                    // Raw JSON without data: prefix
                                    if trimmed.starts_with('{') || trimmed.starts_with('[') {
                                        first_data = Some(trimmed.as_bytes().to_vec());
                                        break;
                                    }
                                }
                            }
                            if let Some(d) = first_data {
                                is_empty_candidate_response(&d)
                            } else {
                                is_empty_candidate_response(&first_chunk)
                            }
                        };
                        if first_chunk_is_empty {
                            let is_gen_path = effective_path.contains(":generateContent")
                                || effective_path.contains(":streamGenerateContent");
                            if is_gen_path && (max_retries == 0 || attempt < max_retries) {
                                let delay = calculate_backoff(
                                    attempt,
                                    initial_delay,
                                    max_delay,
                                    with_jitter,
                                    None,
                                );
                                warn!(
                                    "Upstream returned empty candidate in first chunk for {} {}. Retrying in {:?} (attempt {}/{}) with Clash switch...",
                                    method, target_url, delay, attempt + 1, max_retries
                                );
                                let do_switch = is_generation_path(&effective_path) && should_switch_for_attempt(attempt);
                                handle_failure_and_switch(&state, &mut active_node, &mut tried_nodes, do_switch).await;
                                tokio::time::sleep(delay).await;
                                attempt += 1;
                                continue;
                            }
                        }

                        // Stream is healthy or retries exhausted: chain first chunk with remainder
                        if status.is_success() && !active_node.is_empty() {
                            state.stats_manager.record_success(&active_node, StatsManager::current_hour());
                        }

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
                        if is_retriable_error(&err) && (max_retries == 0 || attempt < max_retries) {
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

                            let do_switch = is_generation_path(&effective_path) && should_switch_for_attempt(attempt);
                            handle_failure_and_switch(&state, &mut active_node, &mut tried_nodes, do_switch).await;
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
                        // Empty response body - treat as retriable for generation endpoints (Gemini blank bug)
                        let is_gen_path = effective_path.contains(":generateContent")
                            || effective_path.contains(":streamGenerateContent");
                        if is_gen_path && (max_retries == 0 || attempt < max_retries) {
                            let delay = calculate_backoff(
                                attempt,
                                initial_delay,
                                max_delay,
                                with_jitter,
                                None,
                            );
                            warn!(
                                "Upstream returned empty body (no chunks) for {} {}. Retrying in {:?} (attempt {}/{}) with Clash switch...",
                                method, target_url, delay, attempt + 1, max_retries
                            );
                            let do_switch = is_generation_path(&effective_path) && should_switch_for_attempt(attempt);
                            handle_failure_and_switch(&state, &mut active_node, &mut tried_nodes, do_switch).await;
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                            continue;
                        }
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
                if is_retriable_error(&err) && (max_retries == 0 || attempt < max_retries) {
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

                    let do_switch = is_generation_path(&effective_path) && should_switch_for_attempt(attempt);
                    handle_failure_and_switch(&state, &mut active_node, &mut tried_nodes, do_switch).await;
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
