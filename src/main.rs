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

fn setup_tracing() -> anyhow::Result<()> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Layer;

    let fmt = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());
    tracing_subscriber::registry().with(fmt).try_init()?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    setup_tracing()?;
    let args = Cli::parse();
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
