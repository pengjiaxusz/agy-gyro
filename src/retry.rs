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

/// Checks if a reqwest error is a transient network/connection error.
pub fn is_retriable_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request() || err.is_body()
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

/// Extracts error code and message from in-stream JSON or SSE event chunks.
pub fn parse_in_stream_error(bytes: &[u8]) -> Option<(StatusCode, String)> {
    let text = std::str::from_utf8(bytes).ok()?;
    let trimmed = text.trim();
    let json_str = if let Some(stripped) = trimmed.strip_prefix("data:") {
        stripped.trim()
    } else {
        trimmed
    };

    if json_str.is_empty() || (!json_str.starts_with('{') && !json_str.starts_with('[')) {
        return None;
    }

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
            _ => 500,
        }
    } else {
        500
    };

    StatusCode::from_u16(code_num).ok().map(|sc| (sc, message))
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
}
