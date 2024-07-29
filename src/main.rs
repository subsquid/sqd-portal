use std::sync::Arc;

use clap::Parser;
use cli::Cli;
use http_server::run_server;
use network::NetworkClient;
use task_manager::TaskManager;
use tokio_util::sync::CancellationToken;

mod cli;
mod http_server;
mod network;
mod stream;
mod task_manager;
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
    let cancellation_token = CancellationToken::new();

    let network_client =
        Arc::new(NetworkClient::new(args.transport, args.logs_collector_id, config.clone()).await?);
    tracing::info!("Network client initialized");
    let task_manager = Arc::new(TaskManager::new(network_client.clone(), args.buffer_size));

    let (res, ()) = tokio::join!(
        run_server(
            task_manager,
            network_client.clone(),
            &args.http_listen,
            config
        ),
        network_client.run(cancellation_token),
    );
    res?;

    Ok(())
}
