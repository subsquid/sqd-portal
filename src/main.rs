use std::sync::Arc;

use clap::Parser;
use cli::Cli;
use controller::task_manager::TaskManager;
use http_server::run_server;
use network::NetworkClient;
use tokio_util::sync::CancellationToken;

mod cli;
mod controller;
mod http_server;
mod metrics;
mod network;
mod types;
mod utils;

fn setup_tracing(json: bool) -> anyhow::Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(
        std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
            .unwrap_or(format!("info,{}=debug", std::env!("CARGO_CRATE_NAME"))),
    );

    if json {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .json()
            .with_span_list(false)
            .flatten_event(true)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .compact()
            .init();
    };
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args = Cli::parse();
    setup_tracing(args.json_log)?;
    let config = Arc::new(args.config);
    let mut metrics_registry = Default::default();
    metrics::register_metrics(&mut metrics_registry);
    subsquid_network_transport::metrics::register_metrics(&mut metrics_registry);
    let cancellation_token = CancellationToken::new();

    let network_client =
        Arc::new(NetworkClient::new(args.transport, args.logs_collector_id, config.clone()).await?);
    tracing::info!("Network client initialized");
    let task_manager = Arc::new(TaskManager::new(network_client.clone()));

    let (res, ()) = tokio::join!(
        run_server(
            task_manager,
            network_client.clone(),
            metrics_registry,
            &args.http_listen,
            config
        ),
        network_client.run(cancellation_token),
    );
    res?;

    Ok(())
}
