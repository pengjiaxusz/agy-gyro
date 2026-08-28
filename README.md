<p align="center">
  <img src="assets/logo.svg" alt="agy-gyro logo" width="200" />
</p>

# agy-gyro

`agy-gyro` is a high-performance, lightweight local retry proxy written in Rust designed to stabilize the Antigravity CLI (`agy`) against transient Google Gemini API errors.

Just like a mechanical gyroscope provides stabilization to keep systems balanced through turbulence, `agy-gyro` acts as a stabilizer for `agy`. Whenever `agy` hits transient API turbulence (such as rate limits or server errors), `agy-gyro` absorbs the shock and keeps the interactive session smoothly on course.

## Motivation

When interacting with the Google Gemini API directly using an API key, requests occasionally fail due to transient infrastructure errors or capacity limits:
- **HTTP 429 (`RESOURCE_EXHAUSTED`)**: Rate limits (RPM/TPM) or temporary backend capacity spikes.
- **HTTP 503 (`UNAVAILABLE`)**: Model servers temporarily overloaded.
- **HTTP 500 / 502 / 504**: Transient backend gateway or server errors.
- **HTTP 408 / Transport Drops**: Network timeouts or aborted socket connections.

By default, the Antigravity CLI (`agy`) does not implement automatic retry loops with exponential backoff. When an error status code is encountered, `agy` terminates the active session immediately, disrupting the agentic workflow.

<p align="center">
  <img src="assets/error.png" alt="The dreaded &quot;Agent execution terminated&quot; error" width="650" /><br/>
  <em>The dreaded "Agent execution terminated" error</em>
</p>

`agy-gyro` solves this by acting as a transparent local HTTP proxy between `agy` and the upstream Gemini API (`generativelanguage.googleapis.com`). It catches transient errors, retries the requests internally using exponential backoff with jitter (following the [Gemini API Troubleshooting Guide](https://ai.google.dev/gemini-api/docs/troubleshooting)), and only returns the final result to `agy` once resolved (or when retry attempts are exhausted).

## Technical Design

```
+------------------+         +---------------------+         +-------------------------------------+
| Antigravity CLI  |  HTTP   |      agy-gyro       |  HTTPS  |          Google Gemini API          |
|      (agy)       | ------> | (127.0.0.1:8080)    | ------> | (generativelanguage.googleapis.com) |
|                  |         |                     |         |                                     |
| Sends prompt or  |         | 1. Buffers req body |         | Evaluates request:                  |
| stream request   |         | 2. Forwards request |         | - 429 / 503 / 500 / 504             |
|                  |         | 3. Retries on error | <------ |   -> Proxy handles backoff + retry  |
| Receives 200 OK  | <------ | 4. Streams response | <------ | - 200 OK                            |
+------------------+         +---------------------+         +-------------------------------------+
```

### 1. Request Forwarding & Header Management
- Preserves all HTTP methods, request paths (e.g. `/v1beta/models/...`), query parameters (`?alt=sse`), and authentication headers (`x-goog-api-key`, `Authorization`).
- Strips hop-by-hop and transport headers (`Connection`, `Keep-Alive`, `Transfer-Encoding`, `TE`, `Upgrade`, `Proxy-Authenticate`, `Proxy-Authorization`, `Trailers`, `Content-Length`, `Content-Encoding`, `Host`) to prevent upstream TLS and decompression conflicts.

### 2. Large Request Payload Buffering
- Disables Axum's default 2MB request body limit (`DefaultBodyLimit::disable()`).
- Buffers full request payloads in memory so multi-file codebases, diffs, and images can be replayed across retries.

### 3. Transient Error Classification
- **Retriable Errors**: `429` (Quota/Rate Limit), `503` (Service Unavailable), `500` (Internal Error), `502` (Bad Gateway), `504` (Gateway Timeout), `408` (Request Timeout), and network connection drops.
- **Fast-Fail Errors**: Client errors like `400` (Bad Request), `401` (Unauthorized), `403` (Forbidden), and `404` (Not Found) bypass retries and return immediately to `agy`.

### 4. Exponential Backoff with Jitter
- **Exponential Scaling**: Doubles the wait delay after each failed attempt up to a configurable cap (`--max-delay-ms`, default 60s).
- **Randomized Jitter**: Applies a random scaling factor ($0.5\times$ to $1.5\times$) to prevent thundering herd contention across concurrent sessions.
- **Upstream `Retry-After`**: Automatically respects HTTP `Retry-After` headers (seconds or HTTP date) returned by Gemini.

### 5. In-Stream SSE Error Interception
- For streaming requests (`streamGenerateContent`), `agy-gyro` inspects the initial SSE chunk before committing response headers to `agy`.
- If an in-stream error frame (e.g. `503 UNAVAILABLE` high-demand message) is detected inside an `HTTP 200 OK` stream, the proxy suppresses it and retries the request.
- Once a valid generation chunk arrives, `agy-gyro` pipes the response stream directly to `agy` with zero additional latency.

## Installation & Building

Prerequisites: Rust toolchain (`rustc` and `cargo` 1.80+).

```bash
# Clone the repository and build release binary
cargo build --release

# The compiled binary will be located at:
# ./target/release/agy-gyro
```

Or install it directly to your Cargo binary path:

```bash
cargo install --path .
```

## Configuration & Modes

`agy-gyro` operates in two modes: **Wrapper Mode** (default) and **Standalone Server Mode**.

### Mode 1: Wrapper Mode (Default)

Running `agy-gyro` without subcommands automatically starts an in-process proxy server on a dynamic TCP port (`127.0.0.1:0`), sets `GOOGLE_GEMINI_BASE_URL` automatically, and launches `agy`.

```bash
# Simply launch agy wrapped with agy-gyro proxy:
agy-gyro

# Pass arguments directly through to agy:
agy-gyro -- --model gemini-2.5-pro

# Optionally write proxy logs to a file:
agy-gyro --log-file /tmp/gyro.log
```

### Mode 2: Standalone Server Mode (`agy-gyro server`)

To run `agy-gyro` as a persistent background daemon or standalone server:

```bash
agy-gyro server --port 8080
```

### Options & Flags

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `-H`, `--host` | `AGY_GYRO_HOST` | `127.0.0.1` | Host address to bind the proxy server |
| `-p`, `--port` | `AGY_GYRO_PORT` | `0` (wrapper) / `8080` (server) | Port to listen on (`0` = OS-assigned free port) |
| `-u`, `--upstream` | `AGY_GYRO_UPSTREAM_URL` | `https://generativelanguage.googleapis.com` | Target upstream Gemini API base URL |
| `--agy-path` | `AGY_GYRO_AGY_PATH` | `agy` | Executable path for Antigravity CLI |
| `--log-file` | `AGY_GYRO_LOG_FILE` | *None* | Path to log file for proxy tracing logs in wrapper mode |
| `--max-retries` | `AGY_GYRO_MAX_RETRIES` | `15` | Maximum number of retry attempts for retriable errors |
| `--initial-delay-ms` | `AGY_GYRO_INITIAL_DELAY_MS` | `1000` | Initial retry backoff delay in milliseconds |
| `--max-delay-ms` | `AGY_GYRO_MAX_DELAY_MS` | `60000` | Maximum backoff delay cap in milliseconds |
| `--no-jitter` | `AGY_GYRO_NO_JITTER` | `false` | Disable randomized jitter in backoff calculation |
| `--request-timeout-secs` | `AGY_GYRO_REQUEST_TIMEOUT_SECS` | `600` | Timeout per attempt in seconds |

## Quick Start with Antigravity (`agy`)

### Step 1: Configure `agy` Settings

Ensure Antigravity is configured to use Gemini API key mode:
- **macOS / Linux**: `~/.gemini/antigravity-cli/settings.json`
- **Windows**: `%USERPROFILE%\.gemini\antigravity-cli\settings.json`

```json
{
  "modelProvider": "gemini"
}
```

### Step 2: Launch `agy` using `agy-gyro`

Set your Gemini API key and run `agy-gyro`:

```bash
export GEMINI_API_KEY="your-gemini-api-key"

agy-gyro
```

`agy-gyro` handles local proxy startup, dynamic port binding, environment variable setup (`GOOGLE_GEMINI_BASE_URL`), and signal forwarding seamlessly!

## License

This project is licensed under the [MIT License](LICENSE).
