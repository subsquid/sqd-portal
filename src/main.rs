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

use crate::utils::RwLock;

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

    #[arg(long, env, hide(true))]
    pub log_span_durations: bool,
}

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn setup_sentry(config: &Config, args: &Cli) -> sentry::ClientInitGuard {
    let environment = args.transport.rpc.network.to_string();
    sentry::init((
        config.sentry_dsn.as_str(),
        sentry::ClientOptions {
            release: sentry::release_name!(),
            environment: Some(environment.into()),
            traces_sample_rate: config.sentry_sampling_rate,
            attach_stacktrace: true,
            send_default_pii: true,
            ..Default::default()
        },
    ))
}

fn setup_tracing(json: bool, log_span_durations: bool) {
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Layer;

    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(
        std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
            .unwrap_or(format!("info,{}=debug", std::env!("CARGO_CRATE_NAME"))),
    );

    let fmt_layer = if json {
        tracing_subscriber::fmt::layer()
            .with_target(false)
            .json()
            .with_span_list(false)
            .flatten_event(true)
            .boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .with_target(false)
            .with_span_events(if log_span_durations {
                FmtSpan::CLOSE
            } else {
                FmtSpan::NONE
            })
            .compact()
            .boxed()
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(sentry::integrations::tracing::layer())
        .init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args = Cli::parse();

    // Initialize Sentry before tracing to integrate them
    let _sentry_guard = setup_sentry(&args.config, &args);
    setup_tracing(args.json_log, args.log_span_durations);

    let datasets = Arc::new(RwLock::new(Datasets::load(&args.config).await?, "datasets"));

    let config = Arc::new(args.config);
    let hotblocks = Arc::new(hotblocks::build_server(&config).await?);
    let network_client_builder = NetworkClient::builder(
        args.transport,
        config.clone(),
        datasets.clone(),
        hotblocks.clone(),
    )
    .await?;

    let peer_id = network_client_builder.peer_id();
    sentry::configure_scope(|scope| {
        scope.set_user(Some(sentry::User {
            id: Some(peer_id.to_string()),
            ..Default::default()
        }));
    });

    let mut metrics_registry = Registry::with_labels(
        vec![(
            Cow::Borrowed("portal_id"),
            Cow::Owned(peer_id.to_string()),
        )]
        .into_iter(),
    );
    metrics::register_metrics(metrics_registry.sub_registry_with_prefix("portal"));
    sqd_network_transport::metrics::register_metrics(
        metrics_registry.sub_registry_with_prefix("transport"),
    );

    // This should be done only when metrics are registered
    let network_client = network_client_builder.build()?;
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
