// SPDX-License-Identifier: MIT

use crate::config::WrapperArgs;
use crate::proxy::{ProxyState, build_http_client, create_router};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Initializes the tracing subscriber for wrapper mode.
///
/// In wrapper mode, to enforce the strict Zero-TTY Output Rule:
/// - If `log_file` is provided, logs are written exclusively to that file.
/// - If `log_file` is `None`, logs are written to a default file at
///   `%TEMP%/agy-gyro.log` (Windows) or `$TMPDIR/agy-gyro.log` (Unix),
///   i.e. `std::env::temp_dir().join("agy-gyro.log")`.
pub fn init_wrapper_tracing(log_file: Option<&Path>) -> Result<(), Box<dyn std::error::Error>> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,agy_gyro=debug"));

    let resolved_path: Option<std::path::PathBuf> = match log_file {
        Some(p) => Some(p.to_path_buf()),
        None => Some(std::env::temp_dir().join("agy-gyro.log")),
    };

    if let Some(path) = resolved_path {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // Best-effort: if default path fails (e.g. permission), fall back to sink instead of crashing wrapper
        match std::fs::OpenOptions::new().create(true).append(true).open(&path) {
            Ok(file) => {
                let _ = tracing_subscriber::registry()
                    .with(env_filter)
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_writer(file)
                            .with_ansi(false),
                    )
                    .try_init();
                // Hint log location on stderr without breaking Zero-TTY stream (one line to aid debugging)
                eprintln!("agy-gyro: logging to {}", path.display());
            }
            Err(e) => {
                eprintln!("agy-gyro: failed to open log file {}: {} (logging silenced)", path.display(), e);
                let _ = tracing_subscriber::registry()
                    .with(env_filter)
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_writer(std::io::sink)
                            .with_ansi(false),
                    )
                    .try_init();
            }
        }
    } else {
        let _ = tracing_subscriber::registry()
            .with(env_filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::sink)
                    .with_ansi(false),
            )
            .try_init();
    }

    Ok(())
}

/// Executes agy wrapped with an in-process agy-gyro proxy server and returns the process exit code.
pub async fn run_wrapper(wrapper_args: WrapperArgs) -> Result<i32, Box<dyn std::error::Error>> {
    init_wrapper_tracing(wrapper_args.log_file.as_deref())?;

    let mut config = wrapper_args.config.clone();
    let port = wrapper_args.resolved_port();
    config.port = Some(port);

    // Bind listener to dynamic port (port 0 by default) or user-specified port
    let addr = format!("{}:{}", config.host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let bound_addr = listener.local_addr()?;

    info!("agy-gyro wrapper proxy listening on http://{}", bound_addr);

    let client = build_http_client(&config)?;
    let state = Arc::new(ProxyState::new(config, client));
    let app = create_router(Arc::clone(&state));

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let proxy_url = format!("http://{}", bound_addr);

    let mut cmd = tokio::process::Command::new(&wrapper_args.agy_path);
    cmd.args(&wrapper_args.agy_args)
        .env("GOOGLE_GEMINI_BASE_URL", &proxy_url)
        .env("CLOUD_CODE_URL", &proxy_url)
        .env("GOOGLE_CLOUD_CODE_URL", &proxy_url)
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            error!(
                "Failed to execute target binary '{}': {}",
                wrapper_args.agy_path, err
            );
            let _ = shutdown_tx.send(());
            let _ = server_handle.await;
            eprintln!(
                "agy-gyro: failed to execute '{}': {}",
                wrapper_args.agy_path, err
            );
            return Ok(127);
        }
    };

    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;

    #[cfg(windows)]
    let mut ctrl_c = tokio::signal::windows::ctrl_c()?;

    let exit_status = tokio::select! {
        res = child.wait() => {
            res?
        }
        _sig = async {
            #[cfg(unix)]
            tokio::select! {
                _ = sigterm.recv() => libc::SIGTERM,
                _ = sighup.recv() => libc::SIGHUP,
            }
            #[cfg(windows)]
            {
                let _ = ctrl_c.recv().await;
                0
            }
            #[cfg(not(any(unix, windows)))]
            std::future::pending::<i32>().await
        } => {
            #[cfg(unix)]
            if let Some(pid) = child.id() {
                unsafe {
                    libc::kill(pid as libc::pid_t, _sig);
                }
            }
            #[cfg(windows)]
            {
                let _ = child.kill().await;
            }

            match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
                Ok(res) => res?,
                Err(_) => {
                    let _ = child.kill().await;
                    child.wait().await?
                }
            }
        }
    };

    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
    state.stats_manager.flush();

    let exit_code = exit_status.code().unwrap_or_else(|| {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if let Some(sig) = exit_status.signal() {
                return 128 + sig;
            }
        }
        1
    });

    Ok(exit_code)
}
