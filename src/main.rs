use agy_gyro::config::{Cli, Commands, ServerArgs, StatsArgs};
use agy_gyro::proxy::{ProxyState, create_router};
use agy_gyro::runner::run_wrapper;
use agy_gyro::stats::StatsManager;
use clap::Parser;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Server(server_args)) => run_server(server_args).await,
        Some(Commands::Run(wrapper_args)) => {
            let code = run_wrapper(wrapper_args).await?;
            std::process::exit(code);
        }
        Some(Commands::Stats(stats_args)) => {
            run_stats(stats_args)?;
            Ok(())
        }
        None => {
            let code = run_wrapper(cli.wrapper_args).await?;
            std::process::exit(code);
        }
    }
}

async fn run_server(server_args: ServerArgs) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber with env filter
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,agy_gyro=debug"));

    let _ = tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .try_init();

    let port = server_args.resolved_port();
    let mut config = server_args.config;
    config.port = Some(port);

    info!("Starting agy-gyro proxy server");
    info!("Upstream URL: {}", config.upstream);
    info!(
        "Retry configuration: max_retries={}, initial_delay_ms={}ms, max_delay_ms={}ms, jitter={}",
        config.max_retries,
        config.initial_delay_ms,
        config.max_delay_ms,
        config.is_jitter_enabled()
    );

    let stats_path = config.resolved_stats_file();
    info!("Node reliability stats file: {}", stats_path.display());

    let client = agy_gyro::proxy::build_http_client(&config)?;
    let state = Arc::new(ProxyState::new(config.clone(), client));

    let app = create_router(Arc::clone(&state));

    let addr = format!("{}:{}", config.host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    info!("agy-gyro proxy listening on http://{}", addr);
    info!(
        "To use with Antigravity CLI, run:\n  export GOOGLE_GEMINI_BASE_URL=http://{}\n",
        addr
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    state.stats_manager.flush();
    info!("agy-gyro server stopped gracefully.");
    Ok(())
}

fn run_stats(args: StatsArgs) -> Result<(), Box<dyn std::error::Error>> {
    let stats_file_path = args.config.resolved_stats_file();
    let manager = StatsManager::new(
        Some(stats_file_path.clone()),
        true,
        args.config.stats_max_samples,
        args.config.stats_half_life_secs(),
        args.config.stats_burst_window_secs,
    );

    let snapshot = manager.snapshot();
    let node_names: Vec<String> = snapshot.nodes.keys().cloned().collect();

    println!("==========================================================================");
    println!(" agy-gyro Node Reliability Priority Statistics");
    println!(" Stats file: {}", stats_file_path.display());
    println!(
        " Settings: half-life={:.1} days, sample-cap={:.1}, burst-window={}s",
        args.config.stats_half_life_days,
        args.config.stats_max_samples,
        args.config.stats_burst_window_secs
    );
    println!(" Last updated: {}", snapshot.updated_at);
    println!(" Total tracked nodes: {}", node_names.len());
    println!("==========================================================================");

    if node_names.is_empty() {
        println!("No node statistics recorded yet.");
        println!("Nodes will be automatically tracked as requests are proxied.");
        return Ok(());
    }

    let hours_to_show: Vec<u8> = if args.all_hours {
        (0..24).collect()
    } else {
        vec![args.hour.unwrap_or_else(StatsManager::current_hour)]
    };

    let current_h = StatsManager::current_hour();

    for h in hours_to_show {
        let is_current = h == current_h;
        let marker = if is_current { " [CURRENT LOCAL HOUR]" } else { "" };
        println!("\n--- Hour {:02}:00 - {:02}:59{} ---", h, h, marker);
        println!(
            "{:<4} | {:<28} | {:<8} | {:<16} | {:<16}",
            "Rank", "Node Name", "Score", "Hourly (S/F)", "Overall (S/F)"
        );
        println!("{:-<4}-+-{:-<28}-+-{:-<8}-+-{:-<16}-+-{:-<16}", "", "", "", "", "");

        let ranked = manager.rank_nodes(h, &node_names);
        for (i, (node, score, hourly, overall)) in ranked.iter().enumerate() {
            let hourly_str = format!("{:.1} / {:.1}", hourly.successes, hourly.failures);
            let overall_str = format!("{:.1} / {:.1}", overall.successes, overall.failures);
            println!(
                "{:>4} | {:<28} | {:>6.1}% | {:<16} | {:<16}",
                i + 1,
                node,
                score * 100.0,
                hourly_str,
                overall_str
            );
        }
    }
    println!();

    Ok(())
}


async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received, shutting down gracefully...");
}
