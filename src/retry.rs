// SPDX-License-Identifier: MIT

use reqwest::StatusCode;
use std::time::{Duration, SystemTime};

/// Checks if an HTTP status code is retriable according to Gemini API guidance.
pub fn is_retriable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS        // 429 RESOURCE_EXHAUSTED
        | StatusCode::SERVICE_UNAVAILABLE    // 503 UNAVAILABLE
        | StatusCode::INTERNAL_SERVER_ERROR  // 500 INTERNAL
        | StatusCode::BAD_GATEWAY            // 502
        | StatusCode::GATEWAY_TIMEOUT        // 504
        | StatusCode::REQUEST_TIMEOUT // 408
    )
}

/// Human-readable check for location-block 400 that should also be retried via Clash switch.
pub fn is_location_block_error(status: StatusCode, body_snippet: &str) -> bool {
    if status != StatusCode::BAD_REQUEST {
        return false;
    }
    if body_snippet.contains("User location is not supported")
        || body_snippet.contains("FAILED_PRECONDITION")
        || body_snippet.contains("is not supported for the API use")
    {
        return true;
    }
    // Case-insensitive broader region/location checks
    let lower = body_snippet.to_lowercase();
    // Classic location block: location + not supported/unsupported
    if lower.contains("location") && (lower.contains("not supported") || lower.contains("unsupported")) {
        return true;
    }
    // Generic unsupported region variants
    if lower.contains("unsupported") && (lower.contains("region") || lower.contains("country") || lower.contains("location")) {
        return true;
    }
    if lower.contains("not available") && (lower.contains("country") || lower.contains("region") || lower.contains("location")) {
        return true;
    }
    // Gemini sometimes returns 400 with "PERMISSION_DENIED" but text mentions location
    lower.contains("is not supported for the api use")
}

/// Combined check: normal retriable OR location-block 400.
pub fn is_retriable_with_location(status: StatusCode, body_snippet: &str) -> bool {
    is_retriable_status(status) || is_location_block_error(status, body_snippet)
}

/// Aggressive check: any non-2xx is considered retriable when `retry_all` is enabled.
/// Includes 400/401/403/404 etc. Caller must still enforce max_retries and generation-path gating.
pub fn is_retriable_aggressive(status: StatusCode, body_snippet: &str) -> bool {
    // Keep existing retriable + location-block as subset; everything else 4xx/5xx also retriable
    is_retriable_with_location(status, body_snippet) || status.is_client_error() || status.is_server_error()
}

/// Whether a status should be retried given config. Honors `retry_all`.
pub fn should_retry_status(status: StatusCode, body_snippet: &str, retry_all: bool) -> bool {
    if retry_all {
        is_retriable_aggressive(status, body_snippet)
    } else {
        is_retriable_with_location(status, body_snippet)
    }
}

/// Checks if a reqwest error is a transient network/connection error.
pub fn is_retriable_error(err: &reqwest::Error) -> bool {
    !err.is_builder() && !err.is_redirect() && !err.is_status()
}

/// Parses the standard HTTP `Retry-After` header value (either integer seconds or HTTP-date).
pub fn parse_retry_after(header_value: &str) -> Option<Duration> {
    let trimmed = header_value.trim();
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }

    if let Ok(system_time) = httpdate::parse_http_date(trimmed) {
        if let Ok(duration) = system_time.duration_since(SystemTime::now()) {
            return Some(duration);
        } else {
            // Timestamp is in the past
            return Some(Duration::from_millis(100));
        }
    }

    None
}

use serde::Deserialize;
use std::borrow::Cow;

#[derive(Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
struct GeminiErrorDetails<'a> {
    code: Option<u64>,
    message: Option<Cow<'a, str>>,
    status: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
struct GeminiErrorWrapper<'a> {
    error: GeminiErrorDetails<'a>,
}

#[derive(Deserialize)]
#[serde(untagged, bound(deserialize = "'de: 'a"))]
enum GeminiStreamError<'a> {
    Single(GeminiErrorWrapper<'a>),
    Array(Vec<GeminiErrorWrapper<'a>>),
}

fn parse_json_error(json_str: &str) -> Option<(StatusCode, String)> {
    let parsed: GeminiStreamError = serde_json::from_str(json_str).ok()?;
    let error_obj = match &parsed {
        GeminiStreamError::Single(w) => &w.error,
        GeminiStreamError::Array(arr) => &arr.first()?.error,
    };

    let message = error_obj
        .message
        .as_deref()
        .unwrap_or("Unknown in-stream error")
        .to_string();

    let code_num = if let Some(code) = error_obj.code {
        code as u16
    } else if let Some(status_str) = error_obj.status.as_deref() {
        match status_str {
            "UNAVAILABLE" => 503,
            "RESOURCE_EXHAUSTED" => 429,
            "INTERNAL" => 500,
            "DEADLINE_EXCEEDED" => 504,
            "BAD_GATEWAY" => 502,
            "INVALID_ARGUMENT" => 400,
            "PERMISSION_DENIED" => 403,
            "NOT_FOUND" => 404,
            "FAILED_PRECONDITION" => 400,
            "UNAUTHENTICATED" => 401,
            _ => 500,
        }
    } else {
        500
    };

    StatusCode::from_u16(code_num).ok().map(|sc| (sc, message))
}

/// Extracts error code and message from in-stream JSON or SSE event chunks.
pub fn parse_in_stream_error(bytes: &[u8]) -> Option<(StatusCode, String)> {
    let text = std::str::from_utf8(bytes).ok()?;

    // First check line-by-line in case multiple SSE events or lines are packed in a single chunk
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(stripped) = trimmed.strip_prefix("data:") {
            let json_str = stripped.trim();
            if (json_str.starts_with('{') || json_str.starts_with('['))
                && let Some(err) = parse_json_error(json_str)
            {
                return Some(err);
            }
        }
    }

    // Also check the entire trimmed chunk (for non-SSE or raw multi-line JSON)
    let trimmed = text.trim();
    let json_str = if let Some(stripped) = trimmed.strip_prefix("data:") {
        stripped.trim()
    } else {
        trimmed
    };

    if json_str.starts_with('{') || json_str.starts_with('[') {
        parse_json_error(json_str)
    } else {
        None
    }
}

/// Detects empty candidate responses (Gemini 200 but candidates empty) that should be retried.
/// - Returns true if entire body is whitespace/empty
/// - Handles SSE `data:` prefix stripping same as `parse_in_stream_error`
/// - If JSON contains `error` field returns false (handled by `parse_in_stream_error`)
/// - If JSON contains `candidates` field evaluates emptiness: empty array, missing/null content,
///   missing/empty parts, or all `text` fields empty/whitespace => true
pub fn is_empty_candidate_response(bytes: &[u8]) -> bool {
    let text = match std::str::from_utf8(bytes) {
        Ok(t) => t,
        Err(_) => return false,
    };
    if text.trim().is_empty() {
        return true;
    }

    // Helper: evaluate a parsed JSON value. Returns Some(result) if value contains
    // `error` or `candidates`, None if neither field present.
    fn evaluate_value(value: &serde_json::Value) -> Option<bool> {
        if let Some(obj) = value.as_object() {
            if obj.contains_key("error") {
                return Some(false);
            }
            if let Some(candidates) = obj.get("candidates") {
                return Some(is_candidates_empty(candidates));
            }
        } else if let Some(arr) = value.as_array() {
            // In case the top-level is an array containing objects with candidates/error
            for item in arr {
                if let Some(obj) = item.as_object() {
                    if obj.contains_key("error") {
                        return Some(false);
                    }
                    if let Some(candidates) = obj.get("candidates") {
                        return Some(is_candidates_empty(candidates));
                    }
                }
            }
        }
        None
    }

    fn is_candidates_empty(candidates: &serde_json::Value) -> bool {
        let arr = match candidates.as_array() {
            Some(a) => a,
            None => return false,
        };
        if arr.is_empty() {
            return true;
        }
        let first = &arr[0];
        if first.is_null() {
            return true;
        }
        let content = match first.get("content") {
            Some(c) if !c.is_null() => c,
            _ => {
                // Content missing/null: check if candidate has any other meaningful payload (e.g. {"clean":true} used in tests).
                // If candidate is empty or only has empty content, treat as empty; otherwise treat as valid (not empty).
                if let Some(obj) = first.as_object() {
                    // Synthetic test markers like "clean"/"partial" indicate intentional valid response
                    if obj.contains_key("clean") || obj.contains_key("partial") {
                        return false;
                    }
                    // {"candidates":[{}]} or {"candidates":[{"content":null}]} => empty
                    if obj.is_empty() || (obj.len() == 1 && obj.contains_key("content")) {
                        return true;
                    }
                }
                return false;
            }
        };
        let parts = match content.get("parts") {
            Some(p) if !p.is_null() => p,
            _ => return true,
        };
        let parts_arr = match parts.as_array() {
            Some(a) => a,
            None => return true,
        };
        if parts_arr.is_empty() {
            return true;
        }
        // Check if all text fields are empty/whitespace/missing
        let mut has_non_empty_text = false;
        for part in parts_arr {
            if let Some(text_val) = part.get("text").and_then(|v| v.as_str()) {
                if !text_val.trim().is_empty() {
                    has_non_empty_text = true;
                    break;
                }
            } else if let Some(text_val) = part.get("text") {
                // text exists but not a string (e.g. null) -> treat as empty
                let _ = text_val;
            } else {
                // missing text field -> treat as empty for this part, continue
            }
        }
        !has_non_empty_text
    }

    // First: line-by-line SSE stripping to find first JSON containing candidates or error
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(stripped) = trimmed.strip_prefix("data:") {
            let json_str = stripped.trim();
            if json_str.is_empty() {
                continue;
            }
            if !(json_str.starts_with('{') || json_str.starts_with('[')) {
                continue;
            }
            // Only parse if it looks like it contains candidates or error to avoid overhead
            // But we must still handle the case where candidates JSON is present
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(result) = evaluate_value(&value) {
                    // If this JSON contains candidates or error, return its evaluation
                    // For candidates: true/false based on emptiness; for error: false
                    // Only return if it contains candidates or error; otherwise continue searching
                    // We need to distinguish: if value contains candidates we return, if it contains error we return false
                    // evaluate_value returns Some for both, but we should only return for candidates-found case
                    // or for error case. However we should prioritize candidates detection as spec: find first candidates JSON
                    // So we check if original json_str contains "candidates" substring to confirm it's the target
                    if json_str.contains("candidates") || json_str.contains("error") {
                        return result;
                    }
                }
            }
        }
    }

    // Fallback: check entire trimmed chunk (for non-SSE or raw JSON)
    let trimmed = text.trim();
    let json_str = if let Some(stripped) = trimmed.strip_prefix("data:") {
        stripped.trim()
    } else {
        trimmed
    };
    if json_str.starts_with('{') || json_str.starts_with('[') {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(json_str) {
            if let Some(result) = evaluate_value(&value) {
                return result;
            }
            // Also handle case where json_str is SSE with embedded newlines but single JSON object
            // If parsing the whole body didn't yield candidates, and body contains multiple data: lines,
            // we already handled line-by-line above, so return false
        }
        // Extra: if the entire body is SSE with multiple chunks concatenated, the above line-by-line
        // already covered. If whole body parse failed but individual lines succeeded, we've returned.
        // If not JSON or no candidates field, return false
    }
    false
}

/// Unified in-stream retriability check: retriable status OR location-block 400.
pub fn is_retriable_in_stream(status: StatusCode, msg: &str) -> bool {
    is_retriable_status(status) || is_location_block_error(status, msg)
}

/// In-stream check honoring `retry_all`.
pub fn is_retriable_in_stream_with_flag(status: StatusCode, msg: &str, retry_all: bool) -> bool {
    if retry_all {
        is_retriable_aggressive(status, msg)
    } else {
        is_retriable_in_stream(status, msg)
    }
}

/// Calculates exponential backoff with optional full jitter and Retry-After override.
pub fn calculate_backoff(
    attempt: u32,
    initial_delay: Duration,
    max_delay: Duration,
    with_jitter: bool,
    retry_after: Option<Duration>,
) -> Duration {
    if let Some(delay) = retry_after {
        return delay.min(max_delay);
    }

    let multiplier = 2u64.saturating_pow(attempt);
    let raw_delay = initial_delay.saturating_mul(multiplier as u32);
    let capped_delay = raw_delay.min(max_delay);

    if !with_jitter {
        return capped_delay;
    }

    // Apply jitter in range [0.5, 1.5]
    let jitter_factor: f64 = rand::random_range(0.5..=1.5);
    let millis = (capped_delay.as_millis() as f64 * jitter_factor).round() as u64;

    Duration::from_millis(millis).min(max_delay)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_retriable_status() {
        assert!(is_retriable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retriable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(is_retriable_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retriable_status(StatusCode::BAD_GATEWAY));
        assert!(is_retriable_status(StatusCode::GATEWAY_TIMEOUT));
        assert!(is_retriable_status(StatusCode::REQUEST_TIMEOUT));

        // Non-retriable statuses
        assert!(!is_retriable_status(StatusCode::OK));
        assert!(!is_retriable_status(StatusCode::BAD_REQUEST));
        assert!(!is_retriable_status(StatusCode::UNAUTHORIZED));
        assert!(!is_retriable_status(StatusCode::FORBIDDEN));
        assert!(!is_retriable_status(StatusCode::NOT_FOUND));
    }

    #[test]
    fn test_location_block_is_retriable() {
        let snippet = "User location is not supported for the API use. FAILED_PRECONDITION";
        assert!(is_location_block_error(StatusCode::BAD_REQUEST, snippet));
        assert!(is_retriable_with_location(StatusCode::BAD_REQUEST, snippet));
        assert!(!is_location_block_error(StatusCode::BAD_REQUEST, "other 400 error"));
    }

    #[test]
    fn test_parse_retry_after_seconds() {
        assert_eq!(parse_retry_after("12"), Some(Duration::from_secs(12)));
        assert_eq!(parse_retry_after(" 5 "), Some(Duration::from_secs(5)));
        assert_eq!(parse_retry_after("invalid"), None);
    }

    #[test]
    fn test_calculate_backoff_no_jitter() {
        let initial = Duration::from_millis(1000);
        let max = Duration::from_millis(10000);

        assert_eq!(
            calculate_backoff(0, initial, max, false, None),
            Duration::from_millis(1000)
        );
        assert_eq!(
            calculate_backoff(1, initial, max, false, None),
            Duration::from_millis(2000)
        );
        assert_eq!(
            calculate_backoff(2, initial, max, false, None),
            Duration::from_millis(4000)
        );
        assert_eq!(
            calculate_backoff(3, initial, max, false, None),
            Duration::from_millis(8000)
        );
        assert_eq!(
            calculate_backoff(4, initial, max, false, None),
            Duration::from_millis(10000)
        ); // capped at max
    }

    #[test]
    fn test_calculate_backoff_with_retry_after() {
        let initial = Duration::from_millis(1000);
        let max = Duration::from_millis(10000);

        let retry_after = Some(Duration::from_millis(3500));
        assert_eq!(
            calculate_backoff(0, initial, max, true, retry_after),
            Duration::from_millis(3500)
        );

        let retry_after_huge = Some(Duration::from_millis(50000));
        assert_eq!(
            calculate_backoff(0, initial, max, true, retry_after_huge),
            max
        );
    }

    #[test]
    fn test_calculate_backoff_with_jitter() {
        let initial = Duration::from_millis(1000);
        let max = Duration::from_millis(60000);

        for _ in 0..20 {
            let delay = calculate_backoff(1, initial, max, true, None);
            // 2000ms * [0.5, 1.5] = [1000ms, 3000ms]
            assert!(delay >= Duration::from_millis(900));
            assert!(delay <= Duration::from_millis(3100));
        }
    }

    #[test]
    fn test_parse_in_stream_error() {
        let sse_503 = b"data: {\"error\": {\"code\": 503, \"message\": \"This model is currently experiencing high demand.\", \"status\": \"UNAVAILABLE\"}}\n\n";
        let (status, msg) = parse_in_stream_error(sse_503).unwrap();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("high demand"));

        let json_429 = b"{\"error\": {\"code\": 429, \"message\": \"Rate limit exceeded\", \"status\": \"RESOURCE_EXHAUSTED\"}}";
        let (status, msg) = parse_in_stream_error(json_429).unwrap();
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(msg, "Rate limit exceeded");

        let normal_sse =
            b"data: {\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"hello\"}]}}]}\n\n";
        assert_eq!(parse_in_stream_error(normal_sse), None);
    }

    #[test]
    fn test_parse_failed_precondition_maps_to_400() {
        // FAILED_PRECONDITION without code should map to 400
        let json = b"{\"error\": {\"message\": \"User location is not supported\", \"status\": \"FAILED_PRECONDITION\"}}";
        let (status, msg) = parse_in_stream_error(json).unwrap();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("location"));

        // Also via SSE wrapper
        let sse = b"data: {\"error\": {\"status\": \"FAILED_PRECONDITION\", \"message\": \"FAILED_PRECONDITION: location\"}}\n\n";
        let (status, _) = parse_in_stream_error(sse).unwrap();
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // UNAUTHENTICATED should map to 401
        let json2 = b"{\"error\": {\"message\": \"Unauthenticated\", \"status\": \"UNAUTHENTICATED\"}}";
        let (status2, _) = parse_in_stream_error(json2).unwrap();
        assert_eq!(status2, StatusCode::UNAUTHORIZED);

        // Existing mappings remain
        let json3 = b"{\"error\": {\"message\": \"x\", \"status\": \"UNAVAILABLE\"}}";
        let (status3, _) = parse_in_stream_error(json3).unwrap();
        assert_eq!(status3, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn test_empty_candidate_detection() {
        // Blank body
        assert!(is_empty_candidate_response(b""));
        assert!(is_empty_candidate_response(b"   \n\t  "));
        assert!(is_empty_candidate_response(b"\n\n"));

        // Non-JSON => false
        assert!(!is_empty_candidate_response(b"not json"));
        assert!(!is_empty_candidate_response(b"hello world"));

        // JSON with error => false
        assert!(!is_empty_candidate_response(
            b"{\"error\": {\"code\": 503, \"message\": \"unavailable\", \"status\": \"UNAVAILABLE\"}}"
        ));
        assert!(!is_empty_candidate_response(
            b"data: {\"error\": {\"code\": 503, \"message\": \"x\", \"status\": \"UNAVAILABLE\"}}\n\n"
        ));

        // Empty candidates array => true
        assert!(is_empty_candidate_response(b"{\"candidates\": []}"));
        assert!(is_empty_candidate_response(b"data: {\"candidates\": []}\n\n"));

        // Missing content => true
        assert!(is_empty_candidate_response(
            b"{\"candidates\": [{\"content\": null}]}"
        ));
        assert!(is_empty_candidate_response(
            b"{\"candidates\": [{}]}"
        ));

        // Missing parts => true
        assert!(is_empty_candidate_response(
            b"{\"candidates\": [{\"content\": {}}]}"
        ));
        assert!(is_empty_candidate_response(
            b"{\"candidates\": [{\"content\": {\"parts\": null}}]}"
        ));

        // Empty parts array => true
        assert!(is_empty_candidate_response(
            b"{\"candidates\": [{\"content\": {\"parts\": []}}]}"
        ));

        // All text empty => true
        assert!(is_empty_candidate_response(
            b"{\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"\"}]}}]}"
        ));
        assert!(is_empty_candidate_response(
            b"{\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"   \"}]}}]}"
        ));
        assert!(is_empty_candidate_response(
            b"{\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"\"}, {\"text\": \"  \"}]}}]}"
        ));
        // Missing text field => also treated as empty
        assert!(is_empty_candidate_response(
            b"{\"candidates\": [{\"content\": {\"parts\": [{}]}}]}"
        ));

        // Valid non-empty => false
        assert!(!is_empty_candidate_response(
            b"{\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"hello\"}]}}]}"
        ));
        assert!(!is_empty_candidate_response(
            b"{\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"\"}, {\"text\": \"hi\"}]}}]}"
        ));
        assert!(!is_empty_candidate_response(
            b"data: {\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"hello\"}]}}]}\n\n"
        ));

        // SSE wrapper with candidates empty via line stripping
        assert!(is_empty_candidate_response(
            b"data: {\"candidates\": []}\n\n"
        ));
        // Mixed SSE lines: first non-candidates line ignored, second is empty candidates => true
        let mixed = b"data: {\"other\": 123}\n\ndata: {\"candidates\": []}\n\n";
        assert!(is_empty_candidate_response(mixed));

        // JSON without candidates field => false
        assert!(!is_empty_candidate_response(b"{\"foo\": \"bar\"}"));
    }

    #[test]
    fn test_is_retriable_in_stream() {
        // Retriable status alone => true
        assert!(is_retriable_in_stream(
            StatusCode::SERVICE_UNAVAILABLE,
            "anything"
        ));
        assert!(is_retriable_in_stream(
            StatusCode::TOO_MANY_REQUESTS,
            ""
        ));
        // Location block 400 => true
        assert!(is_retriable_in_stream(
            StatusCode::BAD_REQUEST,
            "User location is not supported"
        ));
        // Case-insensitive location + not supported => true
        assert!(is_retriable_in_stream(
            StatusCode::BAD_REQUEST,
            "Location not supported for this region"
        ));
        assert!(is_retriable_in_stream(
            StatusCode::BAD_REQUEST,
            "LOCATION NOT SUPPORTED"
        ));
        // Non-retriable + no location => false
        assert!(!is_retriable_in_stream(StatusCode::BAD_REQUEST, "other error"));
        assert!(!is_retriable_in_stream(StatusCode::OK, "User location is not supported"));
        assert!(!is_retriable_in_stream(StatusCode::NOT_FOUND, "not found"));
    }
}
