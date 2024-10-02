use std::borrow::Cow;
use std::sync::Arc;

use crate::network::datasets_load;
use clap::Parser;
use cli::Cli;
use controller::task_manager::TaskManager;
use http_server::run_server;
use network::NetworkClient;
use prometheus_client::registry::Registry;
use tokio_util::sync::CancellationToken;

mod api_types;
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
    let mut args = Cli::parse();
    setup_tracing(args.json_log)?;

    if let Ok(datasets) = datasets_load(&args.config).await {
        args.config.available_datasets = datasets
    };

    let config = Arc::new(args.config);
    let network_client =
        Arc::new(NetworkClient::new(args.transport, args.logs_collector_id, config.clone()).await?);

    let mut metrics_registry = Registry::with_labels(
        vec![(
            Cow::Borrowed("portal_id"),
            Cow::Owned(network_client.peer_id().to_string()),
        )]
        .into_iter(),
    );
    metrics::register_metrics(metrics_registry.sub_registry_with_prefix("portal"));
    sqd_network_transport::metrics::register_metrics(
        metrics_registry.sub_registry_with_prefix("transport"),
    );

    tracing::info!("Network client initialized");
    let task_manager = Arc::new(TaskManager::new(
        network_client.clone(),
        config.max_parallel_streams,
    ));

    let cancellation_token = CancellationToken::new();
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
