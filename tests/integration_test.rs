// SPDX-License-Identifier: MIT

use agy_gyro::config::Config;
use agy_gyro::proxy::{ProxyState, create_router};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Helper function to start a test proxy pointing to the given upstream URL
async fn spawn_test_proxy(upstream_url: String, max_retries: u32) -> String {
    spawn_test_proxy_opts(upstream_url, max_retries, false).await
}

/// Helper function to start a test proxy with configurable buffering
async fn spawn_test_proxy_opts(upstream_url: String, max_retries: u32, no_buffer: bool) -> String {
    spawn_test_proxy_full(upstream_url, max_retries, no_buffer, Vec::new()).await
}

/// Helper function to start a test proxy with full configuration options
async fn spawn_test_proxy_full(
    upstream_url: String,
    max_retries: u32,
    no_buffer: bool,
    redirect_model: Vec<String>,
) -> String {
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: Some(0), // OS assigns random available port
        upstream: upstream_url.clone(),
        cloudcode_upstream: upstream_url,
        max_retries,
        initial_delay_ms: 10, // fast retries in tests
        max_delay_ms: 100,
        no_jitter: true,
        no_buffer,
        request_timeout_secs: 10,
        redirect_model,
        clash_api: "http://127.0.0.1:9097".to_string(),
        clash_secret: "set-your-secret".to_string(),
        clash_group: "台美新日".to_string(),
        clash_parent: "GLOBAL".to_string(),
        no_clash_switch: true,
        retry_all: false,
        stats_file: None,
        no_stats: true,
        stats_max_samples: 20.0,
        stats_half_life_days: 7.0,
        stats_burst_window_secs: 15,
        clash_switch_cooldown_secs: 5.0,
        ..Config::default()
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .unwrap();

    let state = Arc::new(ProxyState::new(config, client));

    let app = create_router(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    format!("http://{}", local_addr)
}

/// Helper to spawn proxy with retry_all enabled for aggressive retry tests
async fn spawn_test_proxy_retry_all(upstream_url: String, max_retries: u32) -> String {
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: Some(0),
        upstream: upstream_url.clone(),
        cloudcode_upstream: upstream_url,
        max_retries,
        initial_delay_ms: 10,
        max_delay_ms: 100,
        no_jitter: true,
        no_buffer: false,
        request_timeout_secs: 10,
        redirect_model: Vec::new(),
        clash_api: "http://127.0.0.1:9097".to_string(),
        clash_secret: "set-your-secret".to_string(),
        clash_group: "台美新日".to_string(),
        clash_parent: "GLOBAL".to_string(),
        no_clash_switch: true,
        retry_all: true,
        stats_file: None,
        no_stats: true,
        stats_max_samples: 20.0,
        stats_half_life_days: 7.0,
        stats_burst_window_secs: 15,
        clash_switch_cooldown_secs: 5.0,
        ..Config::default()
    };
    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .unwrap();
    let state = Arc::new(ProxyState::new(config, client));
    let app = create_router(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{}", local_addr)
}

#[tokio::test]
async fn test_successful_request_forwarding() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .and(header("x-goog-api-key", "test-key-123"))
        .and(query_param("alt", "json"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({
                    "candidates": [{
                        "content": { "parts": [{ "text": "Hello world" }] }
                    }]
                }))
                .insert_header("content-type", "application/json"),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy(mock_server.uri(), 3).await;
    let client = Client::new();

    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:generateContent?alt=json",
            proxy_url
        ))
        .header("x-goog-api-key", "test-key-123")
        .header("content-type", "application/json")
        .body(r#"{"contents":[{"parts":[{"text":"Hi"}]}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(
        body["candidates"][0]["content"]["parts"][0]["text"],
        "Hello world"
    );
}

#[tokio::test]
async fn test_retry_on_429_resource_exhausted_until_success() {
    let mock_server = MockServer::start().await;

    // First two requests return 429 Too Many Requests
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:streamGenerateContent"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string("RESOURCE_EXHAUSTED: Rate limit exceeded")
                .insert_header("retry-after", "0"),
        )
        .up_to_n_times(2)
        .expect(2)
        .mount(&mock_server)
        .await;

    // Third request succeeds with 200 OK
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:streamGenerateContent"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("data: {\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"Recovered from 429\"}]}}]}\n\n")
                .insert_header("content-type", "text/event-stream"),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy(mock_server.uri(), 5).await;
    let client = Client::new();

    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:streamGenerateContent",
            proxy_url
        ))
        .header("x-goog-api-key", "test-key")
        .body(r#"{"contents":[{"parts":[{"text":"Stream"}]}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    assert!(text.contains("Recovered from 429"));
}

#[tokio::test]
async fn test_retry_on_503_service_unavailable() {
    let mock_server = MockServer::start().await;

    // First request returns 503
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .respond_with(ResponseTemplate::new(503).set_body_string("Service Unavailable"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&mock_server)
        .await;

    // Second request returns 200
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_string("OK After 503"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy(mock_server.uri(), 3).await;
    let client = Client::new();

    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:generateContent",
            proxy_url
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    assert_eq!(text, "OK After 503");
}

#[tokio::test]
async fn test_non_retriable_400_bad_request_passed_through_immediately() {
    let mock_server = MockServer::start().await;

    // Upstream returns 400 Bad Request
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(serde_json::json!({
                    "error": { "code": 400, "message": "Invalid JSON payload" }
                }))
                .insert_header("content-type", "application/json"),
        )
        .expect(1) // must only be called ONCE (no retry)
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy(mock_server.uri(), 5).await;
    let client = Client::new();

    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:generateContent",
            proxy_url
        ))
        .header("content-type", "application/json")
        .body("invalid-json")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], 400);
}

#[tokio::test]
async fn test_non_retriable_403_forbidden_passed_through_immediately() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1beta/models"))
        .respond_with(
            ResponseTemplate::new(403).set_body_string("PERMISSION_DENIED: API key not valid"),
        )
        .expect(1) // No retry
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy(mock_server.uri(), 5).await;
    let client = Client::new();

    let response = client
        .get(format!("{}/v1beta/models", proxy_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 403);
    let text = response.text().await.unwrap();
    assert!(text.contains("PERMISSION_DENIED"));
}

#[tokio::test]
async fn test_exhausted_max_retries_returns_upstream_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string("RESOURCE_EXHAUSTED: Persistent quota error"),
        )
        .expect(3) // 1 initial + 2 retries = 3 attempts total
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy(mock_server.uri(), 2).await;
    let client = Client::new();

    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:generateContent",
            proxy_url
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 429);
    let text = response.text().await.unwrap();
    assert!(text.contains("RESOURCE_EXHAUSTED: Persistent quota error"));
}

#[tokio::test]
async fn test_large_request_body_exceeding_default_limit() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_string("Large body received OK"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy(mock_server.uri(), 3).await;
    let client = Client::new();

    // Create a 5MB payload (which exceeds Axum's default 2MB limit)
    let large_payload = vec![b'a'; 5 * 1024 * 1024];

    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:generateContent",
            proxy_url
        ))
        .body(large_payload)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    assert_eq!(text, "Large body received OK");
}

#[tokio::test]
async fn test_in_stream_503_error_retried_until_success() {
    let mock_server = MockServer::start().await;

    // First attempt returns 200 OK with an in-stream 503 error SSE payload
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:streamGenerateContent"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(
                    "data: {\"error\": {\"code\": 503, \"message\": \"This model is currently experiencing high demand.\", \"status\": \"UNAVAILABLE\"}}\n\n",
                ),
        )
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Second attempt returns 200 OK with valid candidates SSE payload
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:streamGenerateContent"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(
                    "data: {\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"Streaming success!\"}]}}]}\n\n",
                ),
        )
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy(mock_server.uri(), 5).await;
    let client = Client::new();

    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse",
            proxy_url
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    assert!(text.contains("Streaming success!"));
}

#[test]
fn test_cli_parse_server_command() {
    use agy_gyro::config::{Cli, Commands};
    use clap::Parser;

    let cli = Cli::parse_from(["agy-gyro", "server", "-p", "9090"]);
    assert!(matches!(cli.command, Some(Commands::Server(_))));
    if let Some(Commands::Server(server_args)) = cli.command {
        assert_eq!(server_args.resolved_port(), 9090);
    }
}

#[test]
fn test_cli_parse_wrapper_default_and_passthrough() {
    use agy_gyro::config::Cli;
    use clap::Parser;

    let cli = Cli::parse_from([
        "agy-gyro",
        "--log-file",
        "/tmp/test.log",
        "--",
        "--model",
        "gemini-2.5-pro",
    ]);

    assert!(cli.command.is_none());
    assert_eq!(
        cli.wrapper_args
            .log_file
            .as_deref()
            .unwrap()
            .to_str()
            .unwrap(),
        "/tmp/test.log"
    );
    assert_eq!(cli.wrapper_args.agy_args, vec!["--model", "gemini-2.5-pro"]);
}

#[tokio::test]
async fn test_buffered_mode_recovers_from_midstream_error() {
    use axum::response::sse::{Event, Sse};
    use axum::routing::post;
    use futures_util::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let call_count = Arc::new(AtomicUsize::new(0));
    let count_clone = call_count.clone();

    let app = axum::Router::new().route(
        "/v1beta/models/gemini-2.5-pro:streamGenerateContent",
        post(move || {
            let count = count_clone.clone();
            async move {
                let attempt = count.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    // First attempt: Chunk 1 valid candidates, Chunk 2 in-stream 503 error
                    let s = stream::iter(vec![
                        Ok::<_, std::convert::Infallible>(
                            Event::default().data("{\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"Thinking...\"}]}}]}")
                        ),
                        Ok::<_, std::convert::Infallible>(
                            Event::default().data("{\"error\": {\"code\": 503, \"message\": \"Mid-stream overload\", \"status\": \"UNAVAILABLE\"}}")
                        ),
                    ]);
                    Sse::new(s)
                } else {
                    // Second attempt: Clean valid chunks
                    let s = stream::iter(vec![
                        Ok::<_, std::convert::Infallible>(
                            Event::default().data("{\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"Thinking completed. \"}]}}]}")
                        ),
                        Ok::<_, std::convert::Infallible>(
                            Event::default().data("{\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"Here is the full answer!\"}]}}]}")
                        ),
                    ]);
                    Sse::new(s)
                }
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mock_upstream = format!("http://{}", addr);

    // Default mode is buffered (no_buffer = false)
    let proxy_url = spawn_test_proxy(mock_upstream, 5).await;
    let client = Client::new();

    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:streamGenerateContent",
            proxy_url
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    // Proxy must have retried after chunk 2 error, giving the complete recovered stream without error
    assert!(!text.contains("Mid-stream overload"));
    assert!(text.contains("Thinking completed."));
    assert!(text.contains("Here is the full answer!"));
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_buffered_mode_recovers_from_midstream_connection_drop() {
    use axum::response::sse::{Event, Sse};
    use axum::routing::post;
    use futures_util::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let call_count = Arc::new(AtomicUsize::new(0));
    let count_clone = call_count.clone();

    let app = axum::Router::new().route(
        "/v1beta/models/gemini-2.5-pro:streamGenerateContent",
        post(move || {
            let count = count_clone.clone();
            async move {
                let attempt = count.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    // Send 1 chunk then simulate IO stream error / drop
                    let s = stream::iter(vec![
                        Ok(Event::default().data("{\"candidates\": [{\"partial\": true}]}")),
                        Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "simulated stream disconnect",
                        )),
                    ]);
                    Sse::new(s)
                } else {
                    let s = stream::iter(vec![Ok(
                        Event::default().data("{\"candidates\": [{\"clean\": true}]}")
                    )]);
                    Sse::new(s)
                }
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mock_upstream = format!("http://{}", addr);

    let proxy_url = spawn_test_proxy(mock_upstream, 5).await;
    let client = Client::new();

    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:streamGenerateContent",
            proxy_url
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    assert!(text.contains("clean"));
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_no_buffer_mode_passes_through_midstream_error() {
    use axum::response::sse::{Event, Sse};
    use axum::routing::post;
    use futures_util::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let call_count = Arc::new(AtomicUsize::new(0));
    let count_clone = call_count.clone();

    let app = axum::Router::new().route(
        "/v1beta/models/gemini-2.5-pro:streamGenerateContent",
        post(move || {
            let count = count_clone.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                let s = stream::iter(vec![
                    Ok::<_, std::convert::Infallible>(
                        Event::default().data("{\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"Partial text...\"}]}}]}")
                    ),
                    Ok::<_, std::convert::Infallible>(
                        Event::default().data("{\"error\": {\"code\": 503, \"message\": \"Mid-stream overload\", \"status\": \"UNAVAILABLE\"}}")
                    ),
                ]);
                Sse::new(s)
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mock_upstream = format!("http://{}", addr);

    // Start proxy with no_buffer = true
    let proxy_url = spawn_test_proxy_opts(mock_upstream, 5, true).await;
    let client = Client::new();

    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:streamGenerateContent",
            proxy_url
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    // In passthrough mode, client receives the partial text followed by the mid-stream error without retry
    assert!(text.contains("Partial text..."));
    assert!(text.contains("Mid-stream overload"));
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_run_wrapper_executes_child_and_propagates_exit_code() {
    use agy_gyro::config::{Config, WrapperArgs};
    use agy_gyro::runner::run_wrapper;

    // Cross-platform: use `sh` on Unix, `cmd` on Windows
    let (agy_path, agy_args) = if cfg!(unix) {
        (
            "sh".to_string(),
            vec![
                "-c".to_string(),
                r#"test -n "$GOOGLE_GEMINI_BASE_URL" && exit 42"#.to_string(),
            ],
        )
    } else {
        (
            "cmd".to_string(),
            vec![
                "/C".to_string(),
                "if defined GOOGLE_GEMINI_BASE_URL exit 42".to_string(),
            ],
        )
    };

    let wrapper_args = WrapperArgs {
        config: Config {
            host: "127.0.0.1".to_string(),
            port: Some(0),
            upstream: "https://generativelanguage.googleapis.com".to_string(),
            cloudcode_upstream: "https://daily-cloudcode-pa.googleapis.com".to_string(),
            max_retries: 3,
            initial_delay_ms: 10,
            max_delay_ms: 100,
            no_jitter: true,
            no_buffer: false,
            request_timeout_secs: 10,
            redirect_model: Vec::new(),
            clash_api: "http://127.0.0.1:9097".to_string(),
            clash_secret: "set-your-secret".to_string(),
            clash_group: "台美新日".to_string(),
            clash_parent: "GLOBAL".to_string(),
            no_clash_switch: true,
            retry_all: false,
            stats_file: None,
            no_stats: true,
            stats_max_samples: 20.0,
            stats_half_life_days: 7.0,
            stats_burst_window_secs: 15,
            clash_switch_cooldown_secs: 5.0,
            ..Config::default()
        },
        agy_path,
        log_file: None,
        agy_args,
    };

    let exit_code = run_wrapper(wrapper_args).await.unwrap();
    assert_eq!(exit_code, 42);
}

#[test]
fn test_rewrite_model_path_unit() {
    use agy_gyro::proxy::rewrite_model_path;

    let redirects = vec![
        ("gemini-3.7-flash", "gemini-3.8-flash"),
        ("gemini-3.5-flash", "gemini-3.8-flash"),
    ];

    // Standard streaming generateContent - matched rule
    assert_eq!(
        rewrite_model_path(
            "/v1beta/models/gemini-3.7-flash:streamGenerateContent?alt=sse",
            &redirects
        ),
        "/v1beta/models/gemini-3.8-flash:streamGenerateContent?alt=sse"
    );

    // Standard generateContent - second matched rule
    assert_eq!(
        rewrite_model_path(
            "/v1beta/models/gemini-3.5-flash:generateContent",
            &redirects
        ),
        "/v1beta/models/gemini-3.8-flash:generateContent"
    );

    // Model path with query param - matched rule
    assert_eq!(
        rewrite_model_path("/v1/models/gemini-3.7-flash?key=abc", &redirects),
        "/v1/models/gemini-3.8-flash?key=abc"
    );

    // Exact model endpoint - matched rule
    assert_eq!(
        rewrite_model_path("/v1beta/models/gemini-3.7-flash", &redirects),
        "/v1beta/models/gemini-3.8-flash"
    );

    // Unmatched model remains untouched
    assert_eq!(
        rewrite_model_path("/v1beta/models/gemini-2.5-pro:generateContent", &redirects),
        "/v1beta/models/gemini-2.5-pro:generateContent"
    );

    // Non-model paths remain untouched
    assert_eq!(
        rewrite_model_path("/v1beta/models", &redirects),
        "/v1beta/models"
    );
    assert_eq!(
        rewrite_model_path("/v1internal:fetchAvailableModels", &redirects),
        "/v1internal:fetchAvailableModels"
    );

    // Empty redirects list leaves everything untouched
    assert_eq!(
        rewrite_model_path("/v1beta/models/gemini-3.7-flash:streamGenerateContent", &[]),
        "/v1beta/models/gemini-3.7-flash:streamGenerateContent"
    );
}

#[tokio::test]
async fn test_redirect_model_rewrites_matched_and_ignores_unmatched() {
    let mock_server = MockServer::start().await;

    // Matched model: gemini-3.7-flash -> gemini-3.8-flash
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-3.8-flash:streamGenerateContent"))
        .and(query_param("alt", "sse"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string("data: {\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"Response from 3.8-flash\"}]}}]}\n\n"),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Unmatched model: gemini-2.5-pro remains untouched
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_json(serde_json::json!({
                    "candidates": [{ "content": { "parts": [{ "text": "Response from 2.5-pro" }] } }]
                })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy_full(
        mock_server.uri(),
        3,
        false,
        vec!["gemini-3.7-flash:gemini-3.8-flash".to_string()],
    )
    .await;

    let client = Client::new();

    // 1. Request for gemini-3.7-flash (should be rewritten to gemini-3.8-flash)
    let res1 = client
        .post(format!(
            "{}/v1beta/models/gemini-3.7-flash:streamGenerateContent?alt=sse",
            proxy_url
        ))
        .header("content-type", "application/json")
        .body(r#"{"contents":[{"parts":[{"text":"Hello"}]}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(res1.status(), 200);
    let text1 = res1.text().await.unwrap();
    assert!(text1.contains("Response from 3.8-flash"));

    // 2. Request for gemini-2.5-pro (should stay gemini-2.5-pro)
    let res2 = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:generateContent",
            proxy_url
        ))
        .header("content-type", "application/json")
        .body(r#"{"contents":[{"parts":[{"text":"Hello"}]}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(res2.status(), 200);
    let body2: serde_json::Value = res2.json().await.unwrap();
    assert_eq!(
        body2["candidates"][0]["content"]["parts"][0]["text"],
        "Response from 2.5-pro"
    );
}

#[test]
fn test_cli_parse_env_vars() {
    use agy_gyro::config::Cli;
    use clap::Parser;

    // Set AGY_GYRO_ environment variables
    unsafe {
        std::env::set_var("AGY_GYRO_MAX_RETRIES", "42");
        std::env::set_var("AGY_GYRO_HOST", "0.0.0.0");
        std::env::set_var("AGY_GYRO_AGY_PATH", "/custom/agy");
        std::env::set_var("AGY_GYRO_NO_BUFFER", "true");
        std::env::set_var(
            "AGY_GYRO_REDIRECT_MODEL",
            "gemini-3.7-flash:gemini-3.8-flash,gemini-3.5-flash:gemini-3.8-flash",
        );
    }

    let cli = Cli::parse_from(["agy-gyro"]);

    assert_eq!(cli.wrapper_args.config.max_retries, 42);
    assert_eq!(cli.wrapper_args.config.host, "0.0.0.0");
    assert_eq!(cli.wrapper_args.agy_path, "/custom/agy");
    assert!(cli.wrapper_args.config.no_buffer);
    assert!(!cli.wrapper_args.config.is_buffer_enabled());
    assert_eq!(
        cli.wrapper_args.config.redirect_model,
        vec![
            "gemini-3.7-flash:gemini-3.8-flash".to_string(),
            "gemini-3.5-flash:gemini-3.8-flash".to_string()
        ]
    );
    assert_eq!(
        cli.wrapper_args.config.model_redirects(),
        vec![
            ("gemini-3.7-flash", "gemini-3.8-flash"),
            ("gemini-3.5-flash", "gemini-3.8-flash")
        ]
    );

    // Clean up env vars
    unsafe {
        std::env::remove_var("AGY_GYRO_MAX_RETRIES");
        std::env::remove_var("AGY_GYRO_HOST");
        std::env::remove_var("AGY_GYRO_AGY_PATH");
        std::env::remove_var("AGY_GYRO_NO_BUFFER");
        std::env::remove_var("AGY_GYRO_REDIRECT_MODEL");
    }
}

#[test]
fn test_cli_parse_redirect_model_flags() {
    use agy_gyro::config::Cli;
    use clap::Parser;

    let cli = Cli::parse_from([
        "agy-gyro",
        "--redirect-model",
        "gemini-3.7-flash:gemini-3.8-flash",
        "--redirect-model",
        "gemini-3.1-pro:gemini-3.1-pro-preview",
    ]);

    assert_eq!(
        cli.wrapper_args.config.model_redirects(),
        vec![
            ("gemini-3.7-flash", "gemini-3.8-flash"),
            ("gemini-3.1-pro", "gemini-3.1-pro-preview")
        ]
    );
}

#[tokio::test]
async fn test_retry_on_location_block_400_until_success() {
    let mock_server = MockServer::start().await;

    // First two attempts: 400 User location is not supported (FAILED_PRECONDITION)
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": { "code": 400, "message": "User location is not supported for the API use.", "status": "FAILED_PRECONDITION" }
            })),
        )
        .up_to_n_times(2)
        .expect(2)
        .mount(&mock_server)
        .await;

    // Third attempt succeeds
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_string("OK after location block"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy(mock_server.uri(), 5).await;
    let client = Client::new();
    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:generateContent",
            proxy_url
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "OK after location block");
}

#[tokio::test]
async fn test_retry_all_retries_on_403_and_400_generic() {
    let mock_server = MockServer::start().await;

    // 403 should be retried only when retry_all=true (covers generic 400 too)
    Mock::given(method("GET"))
        .and(path("/v1beta/models"))
        .respond_with(ResponseTemplate::new(403).set_body_string("PERMISSION_DENIED: API key not valid"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1beta/models"))
        .respond_with(ResponseTemplate::new(200).set_body_string("Recovered via retry_all"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy_retry_all(mock_server.uri(), 5).await;
    let client = Client::new();
    let response = client
        .get(format!("{}/v1beta/models", proxy_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "Recovered via retry_all");

    // Also verify generic 400 (non-location) is retried with retry_all
    let mock_server2 = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": { "code": 400, "message": "Invalid argument: generic bad request" }
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&mock_server2)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_string("Recovered generic 400"))
        .expect(1)
        .mount(&mock_server2)
        .await;

    let proxy_url2 = spawn_test_proxy_retry_all(mock_server2.uri(), 5).await;
    let response2 = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:generateContent",
            proxy_url2
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response2.status(), 200);
    assert_eq!(response2.text().await.unwrap(), "Recovered generic 400");
}

#[tokio::test]
async fn test_non_retry_all_generic_400_not_retried() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-pro:generateContent"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": { "code": 400, "message": "Invalid JSON payload" }
            })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let proxy_url = spawn_test_proxy(mock_server.uri(), 5).await;
    let client = Client::new();
    let response = client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-pro:generateContent",
            proxy_url
        ))
        .body("invalid")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn test_stats_path_resolution_targets_gemini_folder() {
    let path = agy_gyro::stats::resolve_default_stats_path();
    let path_str = path.to_string_lossy();
    assert!(
        path_str.ends_with("gyro.db"),
        "Path should end with gyro.db: {}",
        path_str
    );
    assert!(
        path_str.contains(".gemini") || path_str.contains(".agy-gyro") || path_str.contains("gyro.db"),
        "Path should be inside user gemini or agy folder: {}",
        path_str
    );
}

#[tokio::test]
async fn test_priority_clash_switch_selects_highest_score_node() {
    // 1. Mock Clash API
    let mock_clash = MockServer::start().await;

    // Parent group
    Mock::given(method("GET"))
        .and(path("/proxies/GLOBAL"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "PROXY"
        })))
        .mount(&mock_clash)
        .await;

    // Target group: Node-Failing is current active node
    Mock::given(method("GET"))
        .and(path("/proxies/PROXY"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "Node-Failing",
            "all": ["Node-Failing", "Node-LowScore", "Node-HighScore"]
        })))
        .mount(&mock_clash)
        .await;

    // We expect Clash switch to select Node-HighScore (highest reliability)
    Mock::given(method("PUT"))
        .and(path("/proxies/PROXY"))
        .and(wiremock::matchers::body_json(serde_json::json!({
            "name": "Node-HighScore"
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock_clash)
        .await;

    // 2. Mock upstream Gemini
    let mock_upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(ResponseTemplate::new(503).set_body_string("Overloaded"))
        .up_to_n_times(3)
        .mount(&mock_upstream)
        .await;

    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "candidates": [{"content": {"parts": [{"text": "Success on Node-HighScore"}]}}]
        })))
        .mount(&mock_upstream)
        .await;

    // 3. Spawn proxy with Clash switch enabled and pre-recorded stats
    let temp_stats_dir = std::env::temp_dir().join(format!("gyro-test-priority-{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_stats_dir);
    let stats_path = temp_stats_dir.join("stats.json");

    let config = Config {
        host: "127.0.0.1".to_string(),
        port: Some(0),
        upstream: mock_upstream.uri(),
        cloudcode_upstream: mock_upstream.uri(),
        max_retries: 5,
        initial_delay_ms: 10,
        max_delay_ms: 50,
        no_jitter: true,
        no_buffer: false,
        request_timeout_secs: 10,
        redirect_model: Vec::new(),
        clash_api: mock_clash.uri(),
        clash_secret: "".to_string(),
        clash_group: "PROXY".to_string(),
        clash_parent: "GLOBAL".to_string(),
        no_clash_switch: false,
        retry_all: false,
        stats_file: Some(stats_path.clone()),
        no_stats: false,
        stats_max_samples: 20.0,
        stats_half_life_days: 7.0,
        stats_burst_window_secs: 15,
        clash_switch_cooldown_secs: 0.0,
        ..Config::default()
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .unwrap();
    let state = Arc::new(ProxyState::new(config, client));

    // Pre-populate stats:
    // Node-HighScore: 15 successes, 0 failures (high score ~95%)
    // Node-LowScore: 1 success, 10 failures (low score ~15%)
    let hour = agy_gyro::stats::StatsManager::current_hour();
    for _ in 0..15 {
        state.stats_manager.record_success("Node-HighScore", hour);
    }
    state.stats_manager.record_success("Node-LowScore", hour);
    for _ in 0..10 {
        state.stats_manager.record_failure("Node-LowScore", hour);
    }

    let app = create_router(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let proxy_url = format!("http://{}", local_addr);
    let req_client = Client::new();
    let resp = req_client
        .post(format!(
            "{}/v1beta/models/gemini-2.5-flash:generateContent",
            proxy_url
        ))
        .body("prompt")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("Success on Node-HighScore"));

    // Verify stats were updated: Node-HighScore should have recorded success
    let snap = state.stats_manager.snapshot();
    let high_score_stats = snap.nodes.get("Node-HighScore").expect("Node-HighScore should exist");
    assert!(high_score_stats.overall.successes > 0.0);

    let failing_stats = snap.nodes.get("Node-Failing").expect("Node-Failing should exist");
    assert!(failing_stats.overall.failures > 0.0);

    let _ = std::fs::remove_dir_all(&temp_stats_dir);
}

#[test]
fn test_sole_instance_truncates_log_and_concurrent_instance_appends() {
    use std::io::Write;
    let temp_dir = std::env::temp_dir().join(format!("gyro-log-test-{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);
    let log_path = temp_dir.join("test.log");

    // Pre-populate old log content
    std::fs::write(&log_path, "old historical log content from yesterday\n").unwrap();

    // 1. Instance 1 starts: it is the sole instance -> should TRUNCATE
    let (mut log_file_1, lock_1, is_sole_1) =
        agy_gyro::runner::open_log_file_internal(&log_path).unwrap();
    assert!(is_sole_1, "First instance must be sole instance");
    writeln!(log_file_1, "instance 1 first line").unwrap();
    drop(log_file_1);

    let content_after_1 = std::fs::read_to_string(&log_path).unwrap();
    assert!(
        !content_after_1.contains("old historical log content"),
        "Historical log must be truncated by sole instance"
    );
    assert!(content_after_1.contains("instance 1 first line"));

    // 2. Instance 2 starts while Instance 1's lock_1 is still held -> should APPEND
    let (mut log_file_2, lock_2, is_sole_2) =
        agy_gyro::runner::open_log_file_internal(&log_path).unwrap();
    assert!(!is_sole_2, "Second instance while lock_1 held must not be sole instance");
    writeln!(log_file_2, "instance 2 appended line").unwrap();
    drop(log_file_2);

    let content_after_2 = std::fs::read_to_string(&log_path).unwrap();
    assert!(
        content_after_2.contains("instance 1 first line"),
        "Instance 1 log must be preserved"
    );
    assert!(
        content_after_2.contains("instance 2 appended line"),
        "Instance 2 log must be appended"
    );

    // 3. Both instances exit: drop both locks
    drop(lock_1);
    drop(lock_2);

    // 4. Instance 3 starts fresh -> should TRUNCATE again
    let (mut log_file_3, _lock_3, is_sole_3) =
        agy_gyro::runner::open_log_file_internal(&log_path).unwrap();
    assert!(is_sole_3, "Instance starting with no active locks must be sole instance");
    writeln!(log_file_3, "instance 3 brand new session").unwrap();
    drop(log_file_3);

    let content_after_3 = std::fs::read_to_string(&log_path).unwrap();
    assert!(
        !content_after_3.contains("instance 1"),
        "Logs from previous session should be truncated"
    );
    assert!(
        !content_after_3.contains("instance 2"),
        "Logs from previous session should be truncated"
    );
    assert!(content_after_3.contains("instance 3 brand new session"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_sqlite_multi_instance_concurrent_writes() {
    let temp_dir = std::env::temp_dir().join(format!("gyro-db-concurrency-{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);
    let db_path = temp_dir.join("gyro.db");

    let manager_1 = agy_gyro::stats::StatsManager::new(
        Some(db_path.clone()),
        true,
        20.0,
        7.0 * 86400.0,
        15,
        180,
        2,
        vec!["美国".to_string(), "日本".to_string(), "台湾".to_string(), "新加坡".to_string()],
        3,
        300,
        false,
    );
    let manager_2 = agy_gyro::stats::StatsManager::new(
        Some(db_path.clone()),
        true,
        20.0,
        7.0 * 86400.0,
        15,
        180,
        2,
        vec!["美国".to_string(), "日本".to_string(), "台湾".to_string(), "新加坡".to_string()],
        3,
        300,
        false,
    );

    let hour = 15;
    // Concurrently write from two different instances pointing to the same SQLite DB file
    let m1 = manager_1.clone();
    let m2 = manager_2.clone();

    let t1 = std::thread::spawn(move || {
        for _ in 0..10 {
            m1.record_success("node-shared-a", hour);
        }
    });

    let t2 = std::thread::spawn(move || {
        for _ in 0..10 {
            m2.record_success("node-shared-b", hour);
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    // Verify both managers and a third fresh manager see the exact shared data
    let manager_3 = agy_gyro::stats::StatsManager::new(
        Some(db_path),
        true,
        20.0,
        7.0 * 86400.0,
        15,
        180,
        2,
        vec!["美国".to_string(), "日本".to_string(), "台湾".to_string(), "新加坡".to_string()],
        3,
        300,
        false,
    );
    let snap = manager_3.snapshot();
    let a = snap.nodes.get("node-shared-a").expect("node-shared-a should exist");
    let b = snap.nodes.get("node-shared-b").expect("node-shared-b should exist");

    assert!(a.overall.successes > 0.0);
    assert!(b.overall.successes > 0.0);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_clash_switch_cooldown_prevents_thrashing() {
    let mock_clash = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/proxies/GLOBAL"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "PROXY"
        })))
        .mount(&mock_clash)
        .await;

    Mock::given(method("GET"))
        .and(path("/proxies/PROXY"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "Node-Failing",
            "all": ["Node-Failing", "Node-Good"]
        })))
        .mount(&mock_clash)
        .await;

    // PUT to switch to Node-Good should be called strictly ONCE because cooldown is 10s
    Mock::given(method("PUT"))
        .and(path("/proxies/PROXY"))
        .and(wiremock::matchers::body_json(serde_json::json!({
            "name": "Node-Good"
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock_clash)
        .await;

    let mock_upstream = MockServer::start().await;

    // First request fails 3 times on generation path -> triggers Clash switch
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(ResponseTemplate::new(503).set_body_string("Overloaded"))
        .up_to_n_times(3)
        .mount(&mock_upstream)
        .await;

    // Fourth request (and subsequent) succeeds
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "candidates": [{"content": {"parts": [{"text": "Success"}]}}]
        })))
        .mount(&mock_upstream)
        .await;

    let temp_dir = std::env::temp_dir().join(format!("gyro-test-cd-{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);
    let stats_path = temp_dir.join("stats.db");

    let config = Config {
        host: "127.0.0.1".to_string(),
        port: Some(0),
        upstream: mock_upstream.uri(),
        cloudcode_upstream: mock_upstream.uri(),
        max_retries: 5,
        initial_delay_ms: 10,
        max_delay_ms: 50,
        no_jitter: true,
        no_buffer: false,
        request_timeout_secs: 10,
        redirect_model: Vec::new(),
        clash_api: mock_clash.uri(),
        clash_secret: "".to_string(),
        clash_group: "PROXY".to_string(),
        clash_parent: "GLOBAL".to_string(),
        no_clash_switch: false,
        retry_all: false,
        stats_file: Some(stats_path.clone()),
        no_stats: false,
        stats_max_samples: 20.0,
        stats_half_life_days: 7.0,
        stats_burst_window_secs: 15,
        clash_switch_cooldown_secs: 10.0,
        ..Config::default()
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .unwrap();
    let state = Arc::new(ProxyState::new(config, client));
    let app = create_router(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let http_client = Client::new();
    let res = http_client
        .post(format!(
            "http://{}/v1beta/models/gemini-2.5-flash:generateContent",
            bound_addr
        ))
        .body("test payload")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_http_429_does_not_penalize_node_reliability() {
    let mock_upstream = MockServer::start().await;

    // Upstream returns 429 on first try, then 200 on second try
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(ResponseTemplate::new(429).set_body_json(serde_json::json!({
            "error": {
                "code": 429,
                "message": "Resource exhausted",
                "status": "RESOURCE_EXHAUSTED"
            }
        })))
        .up_to_n_times(1)
        .mount(&mock_upstream)
        .await;

    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "candidates": [{"content": {"parts": [{"text": "Success after 429"}]}}]
        })))
        .mount(&mock_upstream)
        .await;

    let temp_dir = std::env::temp_dir().join(format!("gyro-test-429-{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);
    let stats_path = temp_dir.join("stats.db");

    let config = Config {
        host: "127.0.0.1".to_string(),
        port: Some(0),
        upstream: mock_upstream.uri(),
        cloudcode_upstream: mock_upstream.uri(),
        max_retries: 5,
        initial_delay_ms: 10,
        max_delay_ms: 50,
        no_jitter: true,
        no_buffer: false,
        request_timeout_secs: 10,
        redirect_model: Vec::new(),
        clash_api: "http://127.0.0.1:9097".to_string(),
        clash_secret: "".to_string(),
        clash_group: "PROXY".to_string(),
        clash_parent: "GLOBAL".to_string(),
        no_clash_switch: true, // Disable clash switch for isolated 429 check
        retry_all: false,
        stats_file: Some(stats_path.clone()),
        no_stats: false,
        stats_max_samples: 20.0,
        stats_half_life_days: 7.0,
        stats_burst_window_secs: 15,
        clash_switch_cooldown_secs: 5.0,
        ..Config::default()
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .unwrap();
    let state = Arc::new(ProxyState::new(config, client));
    let app = create_router(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let http_client = Client::new();
    let res = http_client
        .post(format!(
            "http://{}/v1beta/models/gemini-2.5-flash:generateContent",
            bound_addr
        ))
        .body("payload")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);

    // Verify stats: 429 should NOT record failure
    let snap = state.stats_manager.snapshot();
    for (_node, s) in &snap.nodes {
        assert_eq!(s.overall.failures, 0.0, "429 should not count as failure");
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_concurrent_instances_avoid_duplicate_clash_switch() {
    let mock_clash = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/proxies/GLOBAL"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "PROXY"
        })))
        .mount(&mock_clash)
        .await;

    Mock::given(method("GET"))
        .and(path("/proxies/PROXY"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "Node-Failing",
            "all": ["Node-Failing", "Node-Good"]
        })))
        .mount(&mock_clash)
        .await;

    // Both concurrent requests fail on Node-Failing, but Clash switch should only be called ONCE
    Mock::given(method("PUT"))
        .and(path("/proxies/PROXY"))
        .and(wiremock::matchers::body_json(serde_json::json!({
            "name": "Node-Good"
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock_clash)
        .await;

    let mock_upstream = MockServer::start().await;

    // First attempt returns location block 400
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": { "code": 400, "message": "User location is not supported", "status": "FAILED_PRECONDITION" }
            }))
        )
        .up_to_n_times(2)
        .mount(&mock_upstream)
        .await;

    // Subsequent attempts succeed
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "candidates": [{"content": {"parts": [{"text": "Success on Node-Good"}]}}]
        })))
        .mount(&mock_upstream)
        .await;

    let temp_dir = std::env::temp_dir().join(format!("gyro-test-conc-{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);
    let stats_path = temp_dir.join("stats.db");

    let config = Config {
        host: "127.0.0.1".to_string(),
        port: Some(0),
        upstream: mock_upstream.uri(),
        cloudcode_upstream: mock_upstream.uri(),
        max_retries: 5,
        initial_delay_ms: 10,
        max_delay_ms: 50,
        no_jitter: true,
        no_buffer: false,
        request_timeout_secs: 10,
        redirect_model: Vec::new(),
        clash_api: mock_clash.uri(),
        clash_secret: "".to_string(),
        clash_group: "PROXY".to_string(),
        clash_parent: "GLOBAL".to_string(),
        no_clash_switch: false,
        retry_all: false,
        stats_file: Some(stats_path.clone()),
        no_stats: false,
        stats_max_samples: 20.0,
        stats_half_life_days: 7.0,
        stats_burst_window_secs: 15,
        clash_switch_cooldown_secs: 5.0,
        ..Config::default()
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .unwrap();
    let state = Arc::new(ProxyState::new(config, client));
    let app = create_router(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let http_client1 = Client::new();
    let http_client2 = Client::new();

    let req1 = http_client1
        .post(format!(
            "http://{}/v1beta/models/gemini-2.5-flash:generateContent",
            bound_addr
        ))
        .body("payload 1")
        .send();

    let req2 = http_client2
        .post(format!(
            "http://{}/v1beta/models/gemini-2.5-flash:generateContent",
            bound_addr
        ))
        .body("payload 2")
        .send();

    let (res1, res2) = tokio::join!(req1, req2);
    assert_eq!(res1.unwrap().status(), 200);
    assert_eq!(res2.unwrap().status(), 200);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_consecutive_failures_demotes_top_node_to_allow_lower_priority_exploration() {
    let mock_clash = MockServer::start().await;
    let mock_upstream = MockServer::start().await;

    // Clash group has 3 nodes: Node-TopA, Node-TopB, and Node-LowerC
    Mock::given(method("GET"))
        .and(path("/proxies/PROXY"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "Node-TopA",
            "all": ["Node-TopA", "Node-TopB", "Node-LowerC"]
        })))
        .mount(&mock_clash)
        .await;

    Mock::given(method("GET"))
        .and(path("/proxies/GLOBAL"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "PROXY",
            "all": ["PROXY"]
        })))
        .mount(&mock_clash)
        .await;

    Mock::given(method("PUT"))
        .and(path("/proxies/PROXY"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock_clash)
        .await;

    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "candidates": [{"content": {"parts": [{"text": "Success"}]}}]
        })))
        .mount(&mock_upstream)
        .await;

    let temp_stats_dir = std::env::temp_dir().join(format!("gyro-test-cf-{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_stats_dir);
    let stats_path = temp_stats_dir.join("stats.db");

    let config = Config {
        host: "127.0.0.1".to_string(),
        port: Some(0),
        upstream: mock_upstream.uri(),
        cloudcode_upstream: mock_upstream.uri(),
        max_retries: 5,
        initial_delay_ms: 10,
        max_delay_ms: 50,
        no_jitter: true,
        no_buffer: false,
        request_timeout_secs: 10,
        redirect_model: Vec::new(),
        clash_api: mock_clash.uri(),
        clash_secret: "".to_string(),
        clash_group: "PROXY".to_string(),
        clash_parent: "GLOBAL".to_string(),
        no_clash_switch: false,
        retry_all: false,
        stats_file: Some(stats_path.clone()),
        no_stats: false,
        stats_max_samples: 20.0,
        stats_half_life_days: 7.0,
        stats_burst_window_secs: 15,
        clash_switch_cooldown_secs: 0.0,
        consecutive_failure_threshold: 2,
        failure_cooldown_secs: 180.0,
        ..Config::default()
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .unwrap();
    let state = Arc::new(ProxyState::new(config, client));

    let hour = agy_gyro::stats::StatsManager::current_hour();
    // Pre-populate stats:
    // Node-TopA: 20 successes (historical favorite)
    // Node-TopB: 15 successes (historical second)
    // Node-LowerC: 0 requests (untried, score 50%)
    for _ in 0..20 {
        state.stats_manager.record_success("Node-TopA", hour);
    }
    for _ in 0..15 {
        state.stats_manager.record_success("Node-TopB", hour);
    }

    let candidates = vec![
        "Node-TopA".to_string(),
        "Node-TopB".to_string(),
        "Node-LowerC".to_string(),
    ];

    // Initially, Node-TopA is #1, Node-TopB is #2, Node-LowerC is #3
    let ranked_init = state.stats_manager.rank_nodes(hour, &candidates);
    assert_eq!(ranked_init[0].0, "Node-TopA");
    assert_eq!(ranked_init[1].0, "Node-TopB");
    assert_eq!(ranked_init[2].0, "Node-LowerC");

    // Both Node-TopA and Node-TopB suffer 2 consecutive failures
    state.stats_manager.record_failure("Node-TopA", hour);
    state.stats_manager.record_failure("Node-TopA", hour);
    state.stats_manager.record_failure("Node-TopB", hour);
    state.stats_manager.record_failure("Node-TopB", hour);

    let now_sec = agy_gyro::stats::StatsManager::now_sec();
    assert!(state.stats_manager.is_cooling_down("Node-TopA", now_sec));
    assert!(state.stats_manager.is_cooling_down("Node-TopB", now_sec));

    // After cooling down, rank_nodes demotes both Node-TopA and Node-TopB below Node-LowerC!
    let ranked_after = state.stats_manager.rank_nodes(hour, &candidates);
    assert_eq!(ranked_after[0].0, "Node-LowerC");

    // select_best_node automatically chooses Node-LowerC without ping-ponging!
    let chosen = state.stats_manager.select_best_node(hour, &candidates, &[], None);
    assert_eq!(chosen, Some("Node-LowerC".to_string()));

    let _ = std::fs::remove_dir_all(&temp_stats_dir);
}

#[tokio::test]
async fn test_location_block_quarantined_for_12_hours() {
    let mock_clash = MockServer::start().await;
    let mock_upstream = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/proxies/PROXY"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "Node-HK",
            "all": ["Node-HK", "Node-US"]
        })))
        .mount(&mock_clash)
        .await;

    Mock::given(method("GET"))
        .and(path("/proxies/GLOBAL"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "PROXY",
            "all": ["PROXY"]
        })))
        .mount(&mock_clash)
        .await;

    Mock::given(method("PUT"))
        .and(path("/proxies/PROXY"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock_clash)
        .await;

    // Node-HK returns 400 location block
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": { "code": 400, "message": "User location is not supported", "status": "FAILED_PRECONDITION" }
            }))
        )
        .up_to_n_times(1)
        .mount(&mock_upstream)
        .await;

    // Node-US succeeds
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "candidates": [{"content": {"parts": [{"text": "Success on Node-US"}]}}]
        })))
        .mount(&mock_upstream)
        .await;

    let temp_stats_dir = std::env::temp_dir().join(format!("gyro-test-quarantine-{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_stats_dir);
    let stats_path = temp_stats_dir.join("stats.db");

    let config = Config {
        host: "127.0.0.1".to_string(),
        port: Some(0),
        upstream: mock_upstream.uri(),
        cloudcode_upstream: mock_upstream.uri(),
        max_retries: 5,
        initial_delay_ms: 10,
        max_delay_ms: 50,
        no_jitter: true,
        no_buffer: false,
        request_timeout_secs: 10,
        redirect_model: Vec::new(),
        clash_api: mock_clash.uri(),
        clash_secret: "".to_string(),
        clash_group: "PROXY".to_string(),
        clash_parent: "GLOBAL".to_string(),
        no_clash_switch: false,
        retry_all: false,
        stats_file: Some(stats_path.clone()),
        no_stats: false,
        stats_max_samples: 20.0,
        stats_half_life_days: 7.0,
        stats_burst_window_secs: 15,
        clash_switch_cooldown_secs: 0.0,
        node_quarantine_hours: 12.0,
        no_preflight_probe: true, // test direct switch behavior
        ..Config::default()
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .unwrap();
    let state = Arc::new(ProxyState::new(config, client));
    let app = create_router(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let http_client = Client::new();
    let resp = http_client
        .post(format!(
            "http://{}/v1beta/models/gemini-2.5-flash:generateContent",
            bound_addr
        ))
        .body("test payload")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("Success on Node-US"));

    // Verify Node-HK is quarantined for ~12 hours
    let now_sec = agy_gyro::stats::StatsManager::now_sec();
    assert!(state.stats_manager.is_quarantined("Node-HK", now_sec));
    let q_nodes = state.stats_manager.get_quarantined_nodes(now_sec);
    assert!(q_nodes.iter().any(|(n, rem, _)| n == "Node-HK" && *rem > 40000));

    let _ = std::fs::remove_dir_all(&temp_stats_dir);
}

#[tokio::test]
async fn test_anchor_hysteresis_survives_transient_503() {
    let mock_clash = MockServer::start().await;
    let mock_upstream = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/proxies/PROXY"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "Node-Anchor",
            "all": ["Node-Anchor", "Node-Other"]
        })))
        .mount(&mock_clash)
        .await;

    Mock::given(method("GET"))
        .and(path("/proxies/GLOBAL"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "now": "PROXY",
            "all": ["PROXY"]
        })))
        .mount(&mock_clash)
        .await;

    // Clash PUT should NEVER be called because hysteresis retries absorb the transient 503 errors!
    Mock::given(method("PUT"))
        .and(path("/proxies/PROXY"))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&mock_clash)
        .await;

    // First 2 requests return 503, 3rd succeeds
    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(ResponseTemplate::new(503).set_body_string("Service Unavailable"))
        .up_to_n_times(2)
        .mount(&mock_upstream)
        .await;

    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "candidates": [{"content": {"parts": [{"text": "Anchor Succeeded"}]}}]
        })))
        .mount(&mock_upstream)
        .await;

    let temp_stats_dir = std::env::temp_dir().join(format!("gyro-test-anchor-{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_stats_dir);
    let stats_path = temp_stats_dir.join("stats.db");

    let config = Config {
        host: "127.0.0.1".to_string(),
        port: Some(0),
        upstream: mock_upstream.uri(),
        cloudcode_upstream: mock_upstream.uri(),
        max_retries: 5,
        initial_delay_ms: 10,
        max_delay_ms: 50,
        no_jitter: true,
        no_buffer: false,
        request_timeout_secs: 10,
        redirect_model: Vec::new(),
        clash_api: mock_clash.uri(),
        clash_secret: "".to_string(),
        clash_group: "PROXY".to_string(),
        clash_parent: "GLOBAL".to_string(),
        no_clash_switch: false,
        retry_all: false,
        stats_file: Some(stats_path.clone()),
        no_stats: false,
        stats_max_samples: 20.0,
        stats_half_life_days: 7.0,
        stats_burst_window_secs: 15,
        clash_switch_cooldown_secs: 5.0,
        anchor_hysteresis_retries: 5,
        ..Config::default()
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .unwrap();
    let state = Arc::new(ProxyState::new(config, client));

    // Designate Node-Anchor as the consensus anchor
    state.stats_manager.set_consensus_anchor("Node-Anchor");
    state.set_active_node("Node-Anchor".to_string()).await;

    let app = create_router(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let http_client = Client::new();
    let resp = http_client
        .post(format!(
            "http://{}/v1beta/models/gemini-2.5-flash:generateContent",
            bound_addr
        ))
        .body("test payload")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("Anchor Succeeded"));

    let _ = std::fs::remove_dir_all(&temp_stats_dir);
}


