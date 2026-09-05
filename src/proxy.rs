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
use fs2::FileExt;
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
    pub active_clash_node: Arc<tokio::sync::RwLock<(Option<String>, std::time::Instant)>>,
    pub resolved_clash_group: Arc<tokio::sync::RwLock<Option<String>>>,
}

impl ProxyState {
    pub fn new(config: Config, client: reqwest::Client) -> Self {
        let upstream_base = config.upstream.trim_end_matches('/').to_string();
        let cloudcode_base = config
            .cloudcode_upstream
            .trim_end_matches('/')
            .to_string();
        let stats_manager = StatsManager::from_config(&config);
        let active_clash_node = Arc::new(tokio::sync::RwLock::new((None, std::time::Instant::now())));
        let resolved_clash_group = Arc::new(tokio::sync::RwLock::new(None));

        Self {
            config,
            client,
            upstream_base,
            cloudcode_base,
            stats_manager,
            active_clash_node,
            resolved_clash_group,
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

    /// Gets cached active Clash node or fetches it from Clash API (refreshing if older than 2s)
    pub async fn get_or_fetch_active_node(&self) -> String {
        if self.config.no_clash_switch {
            return String::new();
        }
        {
            let guard = self.active_clash_node.read().await;
            if let Some(ref node) = guard.0 {
                if !node.is_empty() && guard.1.elapsed() < Duration::from_millis(2000) {
                    return node.clone();
                }
            }
        }
        let node = self.fetch_current_clash_node().await;
        if !node.is_empty() {
            let mut guard = self.active_clash_node.write().await;
            *guard = (Some(node.clone()), std::time::Instant::now());
        }
        node
    }

    /// Sets the cached active Clash node
    pub async fn set_active_node(&self, node: String) {
        let mut guard = self.active_clash_node.write().await;
        *guard = (Some(node), std::time::Instant::now());
    }

    /// Fetches the current node from Clash API
    pub async fn fetch_current_clash_node(&self) -> String {
        if self.config.no_clash_switch {
            return String::new();
        }
        let api = self.config.clash_api.trim_end_matches('/');
        let secret = &self.config.clash_secret;
        let auth = if secret.is_empty() {
            None
        } else {
            Some(format!("Bearer {}", secret))
        };
        let client = match reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
        {
            Ok(c) => c,
            Err(_) => return String::new(),
        };
        let cached = self.resolved_clash_group.read().await.clone();
        if let Ok(info) = fetch_clash_group_info(&client, api, auth.as_deref(), &self.config.clash_group, cached).await {
            if !info.real_group.is_empty() {
                let mut guard = self.resolved_clash_group.write().await;
                *guard = Some(info.real_group.clone());
            }
            return info.now;
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

#[derive(Debug, PartialEq)]
enum ProbeResult {
    Supported,
    LocationBlocked(String),
    Unreachable(String),
}

async fn probe_candidate_node(
    client: &reqwest::Client,
    upstream_base: &str,
    headers: Option<&HeaderMap>,
) -> ProbeResult {
    let probe_url = format!("{}/v1beta/models?pageSize=1", upstream_base.trim_end_matches('/'));
    let mut req = client.get(&probe_url).timeout(Duration::from_millis(1500));
    if let Some(hdrs) = headers {
        if let Some(key) = hdrs.get("x-goog-api-key") {
            req = req.header("x-goog-api-key", key);
        }
        if let Some(auth) = hdrs.get(reqwest::header::AUTHORIZATION) {
            req = req.header(reqwest::header::AUTHORIZATION, auth);
        }
    }
    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            if status == StatusCode::BAD_REQUEST {
                let snippet = resp.text().await.unwrap_or_default();
                if is_location_block_error(status, &snippet) {
                    ProbeResult::LocationBlocked(snippet)
                } else {
                    ProbeResult::Supported
                }
            } else if status.is_server_error() {
                ProbeResult::Unreachable(format!("server status {}", status))
            } else {
                ProbeResult::Supported
            }
        }
        Err(e) => ProbeResult::Unreachable(e.to_string()),
    }
}

/// URL-encodes a path component safely (e.g. for Chinese or special characters in proxy group names).
pub fn encode_uri_component(input: &str) -> String {
    let mut encoded = String::with_capacity(input.len() * 3);
    for byte in input.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    encoded
}

#[derive(Debug, Clone)]
pub struct ClashGroupInfo {
    pub real_group: String,
    pub now: String,
    pub candidates: Vec<String>,
}

/// Helper to query Clash proxy group info, auto-resolving case differences (e.g. PROXY -> Proxy)
/// and filtering out sub-groups (Selector/URLTest/etc.) so only real leaf nodes remain.
pub async fn fetch_clash_group_info(
    client: &reqwest::Client,
    api: &str,
    auth: Option<&str>,
    preferred_group: &str,
    cached_group: Option<String>,
) -> Result<ClashGroupInfo, String> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = auth {
        if let Ok(val) = token.parse::<HeaderValue>() {
            headers.insert(reqwest::header::AUTHORIZATION, val);
        }
    }

    // 1. Try full /proxies listing first to discover exact group names, types, and leaf nodes
    let all_proxies_url = format!("{}/proxies", api);
    if let Ok(resp) = client.get(&all_proxies_url).headers(headers.clone()).send().await {
        if resp.status().is_success() {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(proxies_map) = json.get("proxies").and_then(|v| v.as_object()) {
                    // Identify all non-leaf groups (Selector, URLTest, Fallback, etc.)
                    let mut non_leaf_groups = std::collections::HashSet::new();
                    for (k, v) in proxies_map {
                        let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
                        if matches!(
                            ty,
                            "Selector" | "URLTest" | "Fallback" | "LoadBalance" | "Direct" | "Reject" | "Compatible"
                        ) {
                            non_leaf_groups.insert(k.clone());
                        }
                    }

                    // Find target group name:
                    // 1) Cached group name (if still present)
                    // 2) Exact match
                    // 3) Case-insensitive match (e.g. "PROXY" == "Proxy")
                    // 4) Fallback to selector with most valid candidates
                    let target_key = cached_group
                        .as_deref()
                        .filter(|c| proxies_map.contains_key(*c))
                        .map(|c| c.to_string())
                        .or_else(|| {
                            if proxies_map.contains_key(preferred_group) {
                                Some(preferred_group.to_string())
                            } else {
                                proxies_map
                                    .keys()
                                    .find(|k| k.eq_ignore_ascii_case(preferred_group))
                                    .cloned()
                            }
                        })
                        .or_else(|| {
                            // If preferred is "Proxy" or "PROXY", look for any selector group containing candidate nodes
                            if preferred_group.eq_ignore_ascii_case("proxy") {
                                proxies_map
                                    .iter()
                                    .filter(|(_, v)| {
                                        v.get("type").and_then(|t| t.as_str()) == Some("Selector")
                                    })
                                    .max_by_key(|(_, v)| {
                                        v.get("all")
                                            .and_then(|a| a.as_array())
                                            .map(|arr| {
                                                arr.iter()
                                                    .filter_map(|x| x.as_str())
                                                    .filter(|s| crate::stats::is_valid_candidate_node(s))
                                                    .count()
                                            })
                                            .unwrap_or(0)
                                    })
                                    .map(|(k, _)| k.clone())
                            } else {
                                None
                            }
                        });

                    if let Some(real_name) = target_key {
                        if let Some(group_obj) = proxies_map.get(&real_name) {
                            let now = group_obj
                                .get("now")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let all = group_obj
                                .get("all")
                                .and_then(|v| v.as_array())
                                .cloned()
                                .unwrap_or_default();

                            let candidates: Vec<String> = all
                                .iter()
                                .filter_map(|v| v.as_str())
                                .filter(|s| !non_leaf_groups.contains(*s)) // Exclude sub-groups like "台美新日", "全部自动"
                                .filter(|s| crate::stats::is_valid_candidate_node(s))
                                .map(|s| s.to_string())
                                .collect();

                            return Ok(ClashGroupInfo {
                                real_group: real_name,
                                now,
                                candidates,
                            });
                        }
                    }
                }
            }
        }
    }

    // 2. Fallback: Query single group directly (e.g. for wiremock tests where only /proxies/GROUP is mocked)
    let group_to_query = cached_group.as_deref().unwrap_or(preferred_group);
    let single_url = format!("{}/proxies/{}", api, encode_uri_component(group_to_query));
    let group_resp = client
        .get(&single_url)
        .headers(headers)
        .send()
        .await
        .map_err(|e| format!("Clash get group error: {} (group={}, api={})", e, group_to_query, api))?;

    if !group_resp.status().is_success() {
        return Err(format!(
            "Clash get group failed: {} (group={}, api={})",
            group_resp.status(),
            group_to_query,
            api
        ));
    }

    let group_json = group_resp
        .json::<serde_json::Value>()
        .await
        .map_err(|e| format!("Clash get group json parse failed: {} (api={})", e, api))?;

    let now = group_json
        .get("now")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let all = group_json
        .get("all")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let candidates: Vec<String> = all
        .iter()
        .filter_map(|v| v.as_str())
        .filter(|s| crate::stats::is_valid_candidate_node(s))
        .map(|s| s.to_string())
        .collect();

    Ok(ClashGroupInfo {
        real_group: group_to_query.to_string(),
        now,
        candidates,
    })
}

async fn trigger_clash_priority_switch(
    state: &Arc<ProxyState>,
    failing_node: &str,
    excluded_nodes: &[String],
    client_headers: Option<&HeaderMap>,
) -> Option<String> {
    if state.config.no_clash_switch {
        return None;
    }

    // Acquire cross-process switch lock to prevent concurrent instances from colliding
    let lock_path = std::env::temp_dir().join("agy-gyro-clash-switch.lock");
    let _lock_guard = tokio::task::spawn_blocking(move || {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        f.lock_exclusive()?;
        Ok::<_, std::io::Error>(f)
    })
    .await
    .ok()
    .and_then(|r| r.ok());

    let api = state.config.clash_api.trim_end_matches('/');
    let secret = state.config.clash_secret.clone();
    let configured_group = state.config.clash_group.clone();
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

    let cached_group = state.resolved_clash_group.read().await.clone();
    let group_info = match fetch_clash_group_info(&client, api, auth.as_deref(), &configured_group, cached_group).await {
        Ok(info) => {
            if info.real_group != configured_group {
                info!(
                    "Clash proxy group resolved: [{}] (configured: [{}])",
                    info.real_group, configured_group
                );
            }
            let mut guard = state.resolved_clash_group.write().await;
            *guard = Some(info.real_group.clone());
            info
        }
        Err(err) => {
            error!("{}", err);
            return None;
        }
    };

    let group = &group_info.real_group;
    let now = group_info.now;
    let all_str = group_info.candidates;

    // 1. Ensure parent points to group (only if clash_parent is configured)
    if !parent.is_empty() && !parent.eq_ignore_ascii_case(group) {
        let parent_url = format!("{}/proxies/{}", api, encode_uri_component(&parent));
        let mut parent_headers = reqwest::header::HeaderMap::new();
        if let Some(token) = &auth {
            if let Ok(val) = token.parse::<HeaderValue>() {
                parent_headers.insert(reqwest::header::AUTHORIZATION, val);
            }
        }
        // Best-effort: check if parent exists and contains real_group
        if let Ok(resp) = client.get(&parent_url).headers(parent_headers.clone()).send().await {
            if resp.status().is_success() {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    let p_now = json.get("now").and_then(|v| v.as_str()).unwrap_or("");
                    let all = json.get("all").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                    let contains_group = all.iter().any(|v| {
                        v.as_str().map(|s| s.eq_ignore_ascii_case(group)).unwrap_or(false)
                    });

                    if contains_group && !p_now.eq_ignore_ascii_case(group) {
                        let put_res = client
                            .put(&parent_url)
                            .headers(parent_headers.clone())
                            .json(&serde_json::json!({"name": group}))
                            .send()
                            .await;
                        match put_res {
                            Ok(r) if r.status().is_success() => {
                                info!("Clash switch parent: {}: {} -> {}", parent, p_now, group);
                            }
                            Ok(r) => {
                                warn!("Clash switch parent {} to {} failed: {}", parent, group, r.status());
                            }
                            Err(e) => {
                                warn!("Clash switch parent {} to {} error: {}", parent, group, e);
                            }
                        }
                    } else if !contains_group {
                        debug!("Clash parent [{}] does not contain group [{}], skipping parent switch", parent, group);
                    } else {
                        debug!("Clash parent {} already at {}", parent, group);
                    }
                }
            }
        }
    }

    if all_str.is_empty() {
        error!("Clash switch: group {} has no valid nodes after filtering invalid/unsupported entries (api={})", group, api);
        return None;
    }

    let group_url = format!("{}/proxies/{}", api, encode_uri_component(group));
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = &auth {
        if let Ok(val) = token.parse::<HeaderValue>() {
            headers.insert(reqwest::header::AUTHORIZATION, val);
        }
    }

    let now_sec = StatsManager::now_sec();
    let hour = StatsManager::current_hour();

    // Desynchronization check: If Clash was already switched away from failing_node by another process
    if !failing_node.is_empty() && !now.is_empty() && now != failing_node {
        if !state.stats_manager.is_quarantined(&now, now_sec) {
            info!(
                "Clash switch skipped: Clash already switched from [{}] to [{}] by another process/request",
                failing_node, now
            );
            state.set_active_node(now.clone()).await;
            return Some(now);
        }
    }

    // Cooldown check: Check if a switch occurred recently across any agy-gyro instance
    let now_ms = chrono::Utc::now().timestamp_millis();
    let (last_switch_ms, last_switch_node) = state.stats_manager.get_last_switch_info();
    let cooldown_ms = state.config.clash_switch_cooldown_ms();
    if cooldown_ms > 0 && last_switch_ms > 0 && now_ms.saturating_sub(last_switch_ms) < cooldown_ms {
        // If another process/request just switched Clash to a healthy target node, adopt it immediately!
        if let Some(ref target) = last_switch_node {
            if !target.is_empty() && target != failing_node && !state.stats_manager.is_quarantined(target, now_sec) {
                let remaining_s = (cooldown_ms - (now_ms - last_switch_ms)) as f64 / 1000.0;
                info!(
                    "Clash switch cooldown active ({:.1}s remaining): adopting recent switch target [{}]",
                    remaining_s, target
                );
                state.set_active_node(target.clone()).await;
                return Some(target.clone());
            }
        }
        if !state.stats_manager.is_quarantined(&now, now_sec) && !now.is_empty() {
            let remaining_s = (cooldown_ms - (now_ms - last_switch_ms)) as f64 / 1000.0;
            info!(
                "Clash switch cooldown active ({:.1}s remaining): staying on node [{}]",
                remaining_s, now
            );
            state.set_active_node(now.clone()).await;
            return Some(now);
        }
    }

    // Filter candidate nodes against quarantine
    let unquarantined = state.stats_manager.filter_quarantined(&all_str, now_sec);
    let non_quarantined_pool = if unquarantined.is_empty() {
        warn!("All {} nodes in group {} are quarantined! Falling back to full list", all_str.len(), group);
        all_str.clone()
    } else {
        unquarantined
    };

    // Filter candidate nodes against consecutive failure cooldown (forces exploration of untried/lower nodes)
    let non_cooling = state.stats_manager.filter_cooling_down(&non_quarantined_pool, now_sec);
    let pool = if non_cooling.is_empty() {
        warn!("All unquarantined nodes in group {} are cooling down! Falling back to non-quarantined pool", group);
        non_quarantined_pool
    } else {
        non_cooling
    };

    if pool.len() == 1 && pool[0] == now {
        info!("Clash switch: {} only one available node {}, staying", group, now);
        return Some(now);
    }

    // 3. Fast Pre-Flight Probing (screen candidate nodes before exposing user request)
    if state.config.is_preflight_probe_enabled() && pool.len() > 1 {
        let mut candidates_to_probe: Vec<String> = Vec::new();
        let mut probe_excluded: Vec<String> = Vec::new();
        if !now.is_empty() {
            probe_excluded.push(now.clone());
        }
        for _ in 0..3 {
            if let Some(cand) = state.stats_manager.select_best_node(hour, &pool, &probe_excluded, Some(failing_node)) {
                if !candidates_to_probe.contains(&cand) {
                    probe_excluded.push(cand.clone());
                    candidates_to_probe.push(cand);
                }
            }
        }

        for cand in candidates_to_probe {
            // Switch Clash to candidate
            let put_res = client
                .put(&group_url)
                .headers(headers.clone())
                .json(&serde_json::json!({"name": cand}))
                .send()
                .await;

            if let Err(e) = put_res {
                warn!("Clash switch to candidate [{}] failed: {}", cand, e);
                continue;
            }

            tokio::time::sleep(Duration::from_millis(30)).await;

            let probe_res = probe_candidate_node(&state.client, &state.upstream_base, client_headers).await;
            match probe_res {
                ProbeResult::Supported => {
                    info!("Pre-flight probe passed for node [{}]. Set as consensus anchor.", cand);
                    state.stats_manager.set_consensus_anchor(&cand);
                    let _ = state.stats_manager.record_switch_event(&now, &cand, now_ms);
                    state.set_active_node(cand.clone()).await;
                    return Some(cand);
                }
                ProbeResult::LocationBlocked(snippet) => {
                    warn!(
                        "Pre-flight probe BLOCKED for candidate [{}] (User location unsupported). Quarantined for {:.1}h. Snippet: {}",
                        cand, state.config.node_quarantine_hours, snippet
                    );
                    state.stats_manager.quarantine_node(
                        &cand,
                        state.config.node_quarantine_secs(),
                        "probe_location_blocked",
                    );
                }
                ProbeResult::Unreachable(err) => {
                    warn!("Pre-flight probe UNREACHABLE for candidate [{}]: {}. Trying next.", cand, err);
                    state.stats_manager.record_failure(&cand, hour);
                }
            }
        }
    }

    // 4. If preflight probe did not return a node (disabled or all probed candidates failed),
    // select best node via two-tier priority
    let nxt = {
        let mut combined_excluded = excluded_nodes.to_vec();
        if !now.is_empty() && !combined_excluded.contains(&now) {
            combined_excluded.push(now.clone());
        }
        state
            .stats_manager
            .select_best_node(hour, &pool, &combined_excluded, Some(&now))
            .unwrap_or_else(|| pool[0].clone())
    };

    if nxt == now && pool.len() > 1 {
        if let Some(alt) = pool.iter().find(|n| *n != &now) {
            let _ = client
                .put(&group_url)
                .headers(headers.clone())
                .json(&serde_json::json!({"name": alt}))
                .send()
                .await;
            // Record switch event in shared SQLite metadata
            let _ = state.stats_manager.record_switch_event(&now, alt, now_ms);
            state.set_active_node(alt.clone()).await;
            return Some(alt.clone());
        }
    }

    if nxt == now && pool.len() == 1 {
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
            // Record switch event in shared SQLite metadata
            let _ = state.stats_manager.record_switch_event(&now, &nxt, now_ms);
            // Update cached active node
            state.set_active_node(nxt.clone()).await;

            if state.stats_manager.is_enabled() {
                let ranked = state.stats_manager.rank_nodes(hour, &pool);
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
    failing_node: &str,
    active_node: &mut String,
    tried_nodes: &mut Vec<String>,
    do_switch: bool,
    record_failure: bool,
    client_headers: Option<&HeaderMap>,
) {
    let hour = StatsManager::current_hour();
    if record_failure && !failing_node.is_empty() {
        state.stats_manager.record_failure(failing_node, hour);
    }
    if !failing_node.is_empty() && !tried_nodes.contains(&failing_node.to_string()) {
        tried_nodes.push(failing_node.to_string());
    }
    if tried_nodes.len() > 8 {
        tried_nodes.remove(0);
    }
    if do_switch {
        if let Some(new_node) = trigger_clash_priority_switch(state, failing_node, tried_nodes, client_headers).await {
            *active_node = new_node;
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
    let mut attempt = 0;

    loop {
        let request_node = state.get_or_fetch_active_node().await;
        let mut active_node = request_node.clone();

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

                        let is_429 = status == StatusCode::TOO_MANY_REQUESTS;
                        let record_failure = !is_429;
                        let is_anchor = state.stats_manager.is_consensus_anchor(&request_node);
                        let do_switch = if is_429 {
                            is_generation_path(&effective_path) && attempt >= 5 && should_switch_for_attempt(attempt)
                        } else if is_anchor && attempt < state.config.anchor_hysteresis_retries {
                            info!(
                                "Anchor hysteresis: node [{}] is consensus anchor, staying on anchor with backoff (attempt {}/{})",
                                request_node, attempt + 1, state.config.anchor_hysteresis_retries
                            );
                            false
                        } else {
                            is_generation_path(&effective_path) && should_switch_for_attempt(attempt)
                        };
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

                        handle_failure_and_switch(&state, &request_node, &mut active_node, &mut tried_nodes, do_switch, record_failure, Some(&headers)).await;
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
                    if is_loc && !request_node.is_empty() {
                        state.stats_manager.quarantine_node(
                            &request_node,
                            state.config.node_quarantine_secs(),
                            "400_location_blocked",
                        );
                    }
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
                            handle_failure_and_switch(&state, &request_node, &mut active_node, &mut tried_nodes, do_switch, true, Some(&headers)).await;
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
                            let is_anchor = state.stats_manager.is_consensus_anchor(&request_node);
                            let do_switch = if is_anchor && attempt < state.config.anchor_hysteresis_retries {
                                false
                            } else {
                                is_generation_path(&effective_path) && should_switch_for_attempt(attempt)
                            };
                            warn!(
                                "Upstream returned aggressive-retriable status {} for {} {}. Snippet: {}. Retrying in {:?} (attempt {}/{}, switch={}) with Clash switch",
                                status, method, target_url, snippet, delay, attempt + 1, max_retries, do_switch
                            );
                            handle_failure_and_switch(&state, &request_node, &mut active_node, &mut tried_nodes, do_switch, true, Some(&headers)).await;
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

                            let is_loc = is_location_block_error(in_stream_status, &err_msg);
                            if is_loc && !request_node.is_empty() {
                                state.stats_manager.quarantine_node(
                                    &request_node,
                                    state.config.node_quarantine_secs(),
                                    "in_stream_location_blocked",
                                );
                            }
                            let is_429 = in_stream_status == StatusCode::TOO_MANY_REQUESTS;
                            let record_failure = !is_429;
                            let is_anchor = state.stats_manager.is_consensus_anchor(&request_node);
                            let do_switch = if is_loc {
                                is_generation_path(&effective_path)
                            } else if is_429 {
                                is_generation_path(&effective_path) && attempt >= 5 && should_switch_for_attempt(attempt)
                            } else if is_anchor && attempt < state.config.anchor_hysteresis_retries {
                                false
                            } else {
                                is_generation_path(&effective_path) && should_switch_for_attempt(attempt)
                            };
                            handle_failure_and_switch(&state, &request_node, &mut active_node, &mut tried_nodes, do_switch, record_failure, Some(&headers)).await;
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

                            let is_anchor = state.stats_manager.is_consensus_anchor(&request_node);
                            let do_switch = if is_anchor && attempt < state.config.anchor_hysteresis_retries {
                                false
                            } else {
                                is_generation_path(&effective_path) && should_switch_for_attempt(attempt)
                            };
                            handle_failure_and_switch(&state, &request_node, &mut active_node, &mut tried_nodes, do_switch, true, Some(&headers)).await;
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
                                        let is_anchor = state.stats_manager.is_consensus_anchor(&request_node);
                                        let do_switch = if is_anchor && attempt < state.config.anchor_hysteresis_retries {
                                            false
                                        } else {
                                            is_generation_path(&effective_path) && should_switch_for_attempt(attempt)
                                        };
                                        handle_failure_and_switch(&state, &request_node, &mut active_node, &mut tried_nodes, do_switch, false, Some(&headers)).await;
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
                    if status.is_success() && !request_node.is_empty() {
                        state.stats_manager.record_success(&request_node, StatsManager::current_hour());
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

                                let is_loc = is_location_block_error(in_stream_status, &err_msg);
                                if is_loc && !request_node.is_empty() {
                                    state.stats_manager.quarantine_node(
                                        &request_node,
                                        state.config.node_quarantine_secs(),
                                        "in_stream_location_blocked",
                                    );
                                }
                                let is_429 = in_stream_status == StatusCode::TOO_MANY_REQUESTS;
                                let record_failure = !is_429;
                                let is_anchor = state.stats_manager.is_consensus_anchor(&request_node);
                                let do_switch = if is_loc {
                                    is_generation_path(&effective_path)
                                } else if is_429 {
                                    is_generation_path(&effective_path) && attempt >= 5 && should_switch_for_attempt(attempt)
                                } else if is_anchor && attempt < state.config.anchor_hysteresis_retries {
                                    false
                                } else {
                                    is_generation_path(&effective_path) && should_switch_for_attempt(attempt)
                                };
                                handle_failure_and_switch(&state, &request_node, &mut active_node, &mut tried_nodes, do_switch, record_failure, Some(&headers)).await;
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
                                let is_anchor = state.stats_manager.is_consensus_anchor(&request_node);
                                let do_switch = if is_anchor && attempt < state.config.anchor_hysteresis_retries {
                                    false
                                } else {
                                    is_generation_path(&effective_path) && should_switch_for_attempt(attempt)
                                };
                                handle_failure_and_switch(&state, &request_node, &mut active_node, &mut tried_nodes, do_switch, false, Some(&headers)).await;
                                tokio::time::sleep(delay).await;
                                attempt += 1;
                                continue;
                            }
                        }

                        // Stream is healthy or retries exhausted: chain first chunk with remainder
                        if status.is_success() && !request_node.is_empty() {
                            state.stats_manager.record_success(&request_node, StatsManager::current_hour());
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

                            let is_anchor = state.stats_manager.is_consensus_anchor(&request_node);
                            let do_switch = if is_anchor && attempt < state.config.anchor_hysteresis_retries {
                                false
                            } else {
                                is_generation_path(&effective_path) && should_switch_for_attempt(attempt)
                            };
                            handle_failure_and_switch(&state, &request_node, &mut active_node, &mut tried_nodes, do_switch, true, Some(&headers)).await;
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
                            let is_anchor = state.stats_manager.is_consensus_anchor(&request_node);
                            let do_switch = if is_anchor && attempt < state.config.anchor_hysteresis_retries {
                                false
                            } else {
                                is_generation_path(&effective_path) && should_switch_for_attempt(attempt)
                            };
                            handle_failure_and_switch(&state, &request_node, &mut active_node, &mut tried_nodes, do_switch, false, Some(&headers)).await;
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

                    let is_anchor = state.stats_manager.is_consensus_anchor(&request_node);
                    let do_switch = if is_anchor && attempt < state.config.anchor_hysteresis_retries {
                        false
                    } else {
                        is_generation_path(&effective_path) && should_switch_for_attempt(attempt)
                    };
                    handle_failure_and_switch(&state, &request_node, &mut active_node, &mut tried_nodes, do_switch, true, Some(&headers)).await;
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
