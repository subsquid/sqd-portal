use std::borrow::Cow;
use std::sync::Arc;

use crate::datasets::DatasetsMapping;
use clap::Parser;
use cli::Cli;
use controller::task_manager::TaskManager;
use http_server::run_server;
use network::NetworkClient;
use parking_lot::RwLock;
use prometheus_client::registry::Registry;
use tokio_util::sync::CancellationToken;

mod cli;
mod controller;
mod datasets;
mod http_server;
mod metrics;
mod network;
mod types;
mod utils;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn setup_tracing(json: bool) {
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args = Cli::parse();
    setup_tracing(args.json_log);

    let datasets = Arc::new(RwLock::new(DatasetsMapping::load(&args.config).await?));

    let config = Arc::new(args.config);
    let network_client =
        Arc::new(NetworkClient::new(args.transport, config.clone(), datasets.clone()).await?);

    let mut metrics_registry = Registry::with_labels(
        vec![(
            Cow::Borrowed("portal_id"),
            Cow::Owned(network_client.get_peer_id().to_string()),
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
    let (server_res, (), ()) = tokio::try_join!(
        tokio::spawn(run_server(
            task_manager,
            network_client.clone(),
            metrics_registry,
            args.http_listen,
            config.clone(),
            datasets.clone(),
        )),
        tokio::spawn(DatasetsMapping::run_updates(
            datasets,
            config.datasets_update_interval,
            config,
        )),
        network_client.run(cancellation_token),
    )?;
    server_res?;

    Ok(())
}
