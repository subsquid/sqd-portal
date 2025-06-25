use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use config::Config;
use controller::task_manager::TaskManager;
use datasets::Datasets;
use http_server::run_server;
use network::NetworkClient;
use prometheus_client::registry::Registry;
use sqd_network_transport::TransportArgs;
use tokio_util::sync::CancellationToken;

mod config;
mod controller;
mod datasets;
mod hotblocks;
mod http_server;
mod metrics;
mod network;
mod types;
mod utils;

#[derive(Parser)]
#[command(version)]
pub struct Cli {
    #[command(flatten)]
    pub transport: TransportArgs,

    /// HTTP server listen addr
    #[arg(long, env = "HTTP_LISTEN_ADDR", default_value = "0.0.0.0:8000")]
    pub http_listen: SocketAddr,

    /// Path to config file
    #[arg(long, env, value_parser = Config::read)]
    pub config: Config,

    /// Whether the logs should be structured in JSON format
    #[arg(long, env)]
    pub json_log: bool,
}

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

use crate::utils::RwLock;

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

    let datasets = Arc::new(RwLock::new(Datasets::load(&args.config).await?, "datasets"));

    let config = Arc::new(args.config);
    let hotblocks = hotblocks::build_server(&config).await?.map(Arc::new);
    let network_client =
        NetworkClient::new(args.transport, config.clone(), datasets.clone()).await?;

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
    if let Some(hotblocks) = &hotblocks {
        hotblocks::register_metrics(
            metrics_registry.sub_registry_with_prefix("portal_hotblocks"),
            hotblocks.clone(),
            config.clone(),
        );
    }

    tracing::info!("Network client initialized");
    let task_manager = Arc::new(TaskManager::new(
        network_client.clone(),
        config.max_parallel_streams,
    ));

    let cancellation_token = CancellationToken::new();
    let (server_res, ()) = tokio::try_join!(
        tokio::spawn(run_server(
            task_manager,
            network_client.clone(),
            metrics_registry,
            args.http_listen,
            config.clone(),
            hotblocks,
        )),
        network_client.run(cancellation_token),
    )?;
    server_res?;

    Ok(())
}
