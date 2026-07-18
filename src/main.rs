use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use prometheus_client::registry::Registry;
use sqd_network_transport::TransportArgs;
use sqd_portal::config::Config;
use sqd_portal::controller::task_manager::TaskManager;
use sqd_portal::datasets::Datasets;
use sqd_portal::http_server::run_server;
use sqd_portal::network::NetworkClient;
use sqd_portal::utils::RwLock;
use tokio_util::sync::CancellationToken;

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

    /// Show endpoints marked internal (operations carrying the `x-internal: true`
    /// OpenAPI extension) in the docs. By default such endpoints are stripped from
    /// `/docs` and `/api-docs/openapi.json`.
    #[arg(long, env = "SHOW_INTERNAL_DOCS")]
    pub show_internal_docs: bool,
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
            .with_span_list(true)
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
    let _sentry_guard = args
        .config
        .sentry_is_enabled
        .then(|| setup_sentry(&args.config, &args));

    setup_tracing(args.json_log, args.log_span_durations);

    let datasets = Arc::new(RwLock::new(Datasets::load(&args.config).await?, "datasets"));

    let config = Arc::new(args.config);
    let hotblocks = Arc::new(sqd_portal::hotblocks::build_client(&config).await?);
    let network_client_builder =
        NetworkClient::builder(args.transport, config.clone(), datasets.clone()).await?;

    let peer_id = network_client_builder.peer_id();
    sentry::configure_scope(|scope| {
        scope.set_user(Some(sentry::User {
            id: Some(peer_id.to_string()),
            ..Default::default()
        }));
    });

    let mut metrics_registry = Registry::with_labels(
        vec![(Cow::Borrowed("portal_id"), Cow::Owned(peer_id.to_string()))].into_iter(),
    );
    sqd_portal::metrics::register_metrics(metrics_registry.sub_registry_with_prefix("portal"));
    sqd_network_transport::metrics::register_metrics(
        metrics_registry.sub_registry_with_prefix("transport"),
    );

    // This should be done only when metrics are registered
    let network_client = network_client_builder.build()?;
    tracing::info!("Network client initialized");

    let task_manager = Arc::new(TaskManager::new(
        network_client.clone(),
        config.max_parallel_streams,
        config.congestion.headroom_threshold,
        config.congestion.priority_stride,
    ));

    let cancellation_token = CancellationToken::new();
    let shutting_down = Arc::new(AtomicBool::new(false));
    let sigterm = {
        use anyhow::Context;
        use tokio::signal::unix::{signal, SignalKind};
        signal(SignalKind::terminate()).context("install SIGTERM handler")?
    };

    tokio::spawn(watch_shutdown_signal(
        sigterm,
        shutting_down.clone(),
        cancellation_token.clone(),
        config.pre_drain_grace_period,
    ));

    let (server_res, ()) = tokio::try_join!(
        tokio::spawn(run_server(
            task_manager,
            network_client.clone(),
            metrics_registry,
            args.http_listen,
            config.clone(),
            hotblocks,
            shutting_down,
            cancellation_token.clone(),
            args.show_internal_docs,
        )),
        network_client.run(cancellation_token),
    )?;
    server_res?;

    Ok(())
}

/// Awaits SIGTERM and runs the two-phase shutdown sequence.
///
/// See [ADR-005](../spec/decisions/ADR-005-two-phase-shutdown.md) for the two-phase
/// shutdown decision — lifecycle, timing rationale, and non-goals.
async fn watch_shutdown_signal(
    mut sigterm: tokio::signal::unix::Signal,
    shutting_down: Arc<AtomicBool>,
    cancel: CancellationToken,
    pre_drain_grace: Duration,
) {
    sigterm.recv().await;
    tracing::info!("SIGTERM received; starting shutdown sequence");
    run_shutdown_sequence(shutting_down, cancel, pre_drain_grace).await;
}

async fn run_shutdown_sequence(
    shutting_down: Arc<AtomicBool>,
    cancel: CancellationToken,
    pre_drain_grace: Duration,
) {
    shutting_down.store(true, Ordering::Relaxed);
    tracing::info!("/ready will return 503 for {pre_drain_grace:?} before draining");
    tokio::time::sleep(pre_drain_grace).await;
    tracing::info!("Pre-drain grace elapsed; starting HTTP drain and stopping background tasks");
    cancel.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn shutdown_sequence_flips_flag_then_cancels_after_grace() {
        let shutting_down = Arc::new(AtomicBool::new(false));
        let cancel = CancellationToken::new();
        let grace = Duration::from_secs(5);

        let task = tokio::spawn(run_shutdown_sequence(
            shutting_down.clone(),
            cancel.clone(),
            grace,
        ));

        // Yield to let the spawned task run up to the `sleep`.
        tokio::task::yield_now().await;
        assert!(
            shutting_down.load(Ordering::Relaxed),
            "flag flips immediately"
        );
        assert!(!cancel.is_cancelled(), "cancel held until grace elapses");

        tokio::time::advance(grace - Duration::from_millis(1)).await;
        assert!(
            !cancel.is_cancelled(),
            "cancel still pending just before grace"
        );

        tokio::time::advance(Duration::from_millis(2)).await;
        task.await.expect("shutdown sequence completes");
        assert!(cancel.is_cancelled(), "cancel fires after grace");
    }
}
