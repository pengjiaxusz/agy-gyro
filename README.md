<p align="center">
  <img src="assets/logo.svg" alt="agy-gyro logo" width="200" />
</p>

# agy-gyro

> **Disclaimer**: `agy-gyro` is an independent open-source project and is not affiliated with, endorsed by, or sponsored by Google or the Antigravity team.

`agy-gyro` is a high-performance, lightweight local retry proxy written in Rust designed to stabilize the Antigravity CLI (`agy`) against transient Google Gemini API errors.


## Motivation

### 1. Transient API Errors & Lack of Retries

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

### 2. Access to Newer / Unlisted Gemini Models

When configured in Gemini API key mode (`"modelProvider": "gemini"`), `agy` uses a static, built-in model registry to validate and present available models. When Google releases newer models or previews on the Gemini API (such as `gemini-3.8-flash`), users cannot select them in `agy` until a new CLI release is published.

Because Gemini REST API schemas (including prompt structures, tool call definitions, and reasoning `thinkingConfig` parameters) are shared across model generations, requests differ primarily by the model identifier in the REST URL. `agy-gyro` enables instant access to new models via transparent model remapping (`--redirect-model`), redirecting outbound requests at the proxy layer without requiring changes to `agy`.

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

### 5. Full-Stream Caching & Error Interception

- **Full-Stream Buffering (Default)**: By default, `agy-gyro` buffers every stream chunk until completion to verify the entire stream is error-free before committing headers or data to `agy`. If an in-stream error or connection drop occurs at any point (chunk 1, chunk 2, or later), `agy-gyro` discards the buffered chunks and replays the request with backoff. Zero bytes are leaked to `agy`, providing 100% resilience against mid-stream failures.
- **Passthrough Mode (`--no-buffer`)**: If immediate chunk streaming is desired for lowest time-to-first-token latency, passing `--no-buffer` peeks at the initial stream chunk (Chunk 1) and immediately pipes subsequent chunks directly to `agy`.

### 6. Dynamic Model Redirection & Remapping

- **URL-Level Remapping**: Rewrites model path segments matching `FROM:TO` rules (e.g. `/v1beta/models/gemini-3.7-flash:streamGenerateContent` -> `/v1beta/models/gemini-3.8-flash:streamGenerateContent`).
- **Preserves Full Context**: Transparently preserves the request body, including tool call declarations, workspace context, and reasoning configurations (`thinkingConfig` / `thinkingBudget`).
- **Multi-Rule Support**: Supports mapping multiple models simultaneously using repeated flags or comma-separated environment variables (e.g., remapping Flash and Pro independently).
- **Zero Overhead on Unmatched Traffic**: Requests that do not match any redirect rule (or non-model management endpoints) pass through without modification.

## Installation

Install `agy-gyro` using `cargo`:

```bash
cargo install agy-gyro
```

Prebuilt executables for Linux, macOS, and Windows are also available in the [latest GitHub release](https://github.com/topjohnwu/agy-gyro/releases/latest).

## Configuration & Modes

`agy-gyro` operates in two modes: **Wrapper Mode** (default) and **Standalone Server Mode**.

### Mode 1: Wrapper Mode (Default)

Running `agy-gyro` without subcommands automatically starts an in-process proxy server on a dynamic TCP port (`127.0.0.1:0`), sets `GOOGLE_GEMINI_BASE_URL` automatically, and launches `agy`.

```bash
# Simply launch agy wrapped with agy-gyro proxy:
agy-gyro

# Pass arguments directly through to agy:
agy-gyro -- --model gemini-3.7-flash

# Redirect requests from one model to another (e.g. gemini-3.7-flash -> gemini-3.8-flash):
agy-gyro --redirect-model gemini-3.7-flash:gemini-3.8-flash

# Optionally write proxy logs to a file:
agy-gyro --log-file /tmp/gyro.log
```

### Mode 2: Standalone Server Mode (`agy-gyro server`)

To run `agy-gyro` as a persistent background daemon or standalone server:

```bash
agy-gyro server --port 8080
```

### Options & Flags

| Flag                     | Environment Variable            | Default                                     | Description                                             |
| :----------------------- | :------------------------------ | :------------------------------------------ | :------------------------------------------------------ |
| `-H`, `--host`           | `AGY_GYRO_HOST`                 | `127.0.0.1`                                 | Host address to bind the proxy server                   |
| `-p`, `--port`           | `AGY_GYRO_PORT`                 | `0` (wrapper) / `8080` (server)             | Port to listen on (`0` = OS-assigned free port)         |
| `-u`, `--upstream`       | `AGY_GYRO_UPSTREAM_URL`         | `https://generativelanguage.googleapis.com` | Target upstream Gemini API base URL                     |
| `--agy-path`             | `AGY_GYRO_AGY_PATH`             | `agy`                                       | Executable path for Antigravity CLI                     |
| `--log-file`             | `AGY_GYRO_LOG_FILE`             | _None_                                      | Path to log file for proxy tracing logs in wrapper mode |
| `--max-retries`          | `AGY_GYRO_MAX_RETRIES`          | `15`                                        | Maximum number of retry attempts for retriable errors   |
| `--initial-delay-ms`     | `AGY_GYRO_INITIAL_DELAY_MS`     | `1000`                                      | Initial retry backoff delay in milliseconds             |
| `--max-delay-ms`         | `AGY_GYRO_MAX_DELAY_MS`         | `60000`                                     | Maximum backoff delay cap in milliseconds               |
| `--no-jitter`            | `AGY_GYRO_NO_JITTER`            | `false`                                     | Disable randomized jitter in backoff calculation        |
| `--no-buffer`            | `AGY_GYRO_NO_BUFFER`            | `false`                                     | Disable full stream buffering (stream chunks immediately)|
| `--request-timeout-secs` | `AGY_GYRO_REQUEST_TIMEOUT_SECS` | `600`                                       | Timeout per attempt in seconds                          |
| `--redirect-model`       | `AGY_GYRO_REDIRECT_MODEL`       | _None_                                      | Redirect model requests in `FROM:TO` format             |

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

### Step 3: Access Newer Gemini Models (Optional)

To use newer or experimental Gemini models (such as `gemini-3.8-flash`) not yet selectable in `agy`'s static model selector:

```bash
# Redirect all requests for gemini-3.7-flash to gemini-3.8-flash:
agy-gyro --redirect-model gemini-3.7-flash:gemini-3.8-flash

# Multiple models can be redirected simultaneously:
agy-gyro \
  --redirect-model gemini-3.7-flash:gemini-3.8-flash \
  --redirect-model gemini-3.5-flash:gemini-3.8-flash
```

## Streaming & Error Recovery Behavior

### Default Mode: Full Stream Buffering

In default mode, `agy-gyro` buffers all incoming stream chunks before delivering them to `agy`:
1. **Full Verification**: Ensures every chunk in the response is error-free and that the upstream stream completes cleanly.
2. **Zero-Byte Leakage**: If an error (e.g. `503 UNAVAILABLE`, `429 RESOURCE_EXHAUSTED`, or network disconnect) occurs at chunk 1 or mid-stream, `agy-gyro` discards the buffered chunks and replays the request from scratch.
3. **Flawless Delivery**: `agy` only receives verified, complete streams without partial output or syntax errors.

### Passthrough Mode (`--no-buffer`)

When running with `--no-buffer`, `agy-gyro` verifies Chunk 1 and immediately streams subsequent tokens to minimize time-to-first-token latency:
- **Chunk 1 Error**: Retried seamlessly before sending headers to `agy`.
- **Mid-Stream Error (Chunk $N > 1$)**: Forwarded downstream to prevent duplicated tokens.

### Summary Matrix

| Error Stage                          | Default Buffering Mode                                                       | Passthrough Mode (`--no-buffer`)                                                     |
| :----------------------------------- | :--------------------------------------------------------------------------- | :----------------------------------------------------------------------------------- |
| **HTTP Status Error**                | **Retried**: Replays request with exponential backoff & jitter               | **Retried**: Replays request with exponential backoff & jitter                       |
| **Chunk 1 In-Stream Error**          | **Retried**: Suppresses error chunk & replays request cleanly                | **Retried**: Suppresses error chunk & replays request cleanly                        |
| **Mid-Stream Error (Chunk $N > 1$)** | **Retried**: Discards partial chunks & replays request cleanly from scratch  | **Forwarded**: Emits error to client to avoid duplicating already-streamed tokens   |
| **Mid-Stream Connection Drop**       | **Retried**: Discards partial chunks & replays request cleanly from scratch  | **Failed**: Stream ends prematurely with network disconnect                          |

## License

This project is licensed under the [MIT License](LICENSE).
