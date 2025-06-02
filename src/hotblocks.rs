use std::{sync::Arc, time::Duration};

use anyhow::Context;
use prometheus_client::{
    encoding::{DescriptorEncoder, EncodeMetric},
    metrics::{family::Family, gauge::Gauge},
    registry::Registry,
};
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_hotblocks::{DBRef, Node as HotblocksServer, NodeBuilder as HotblocksServerBuilder};
use sqd_storage::db::DatabaseSettings;

use crate::config::Config;

pub struct HotblocksHandle {
    pub server: HotblocksServer,
    pub db: DBRef,
    pub datasets: Vec<sqd_storage::db::DatasetId>,
}

pub async fn build_server(config: &Config) -> anyhow::Result<Option<HotblocksHandle>> {
    let has_sources = config.datasets.iter().any(|(_, d)| d.real_time.is_some());
    if !has_sources {
        return Ok(None);
    }

    tracing::info!("Initializing hotblocks storage");
    let hotblocks_config = config
        .hotblocks
        .as_ref()
        .expect("Hotblocks database path not specified");
    let mut settings = DatabaseSettings::default()
        .with_data_cache_size(hotblocks_config.data_cache_mb)
        .with_direct_io(hotblocks_config.direct_io)
        .with_rocksdb_stats(true);
    if let Some(size) = hotblocks_config.chunk_cache_mb {
        settings = settings.with_chunk_cache_size(size);
    }
    let db = Arc::new(
        settings
            .open(&hotblocks_config.db)
            .context("failed to open hotblocks database")?,
    );

    tokio::spawn(run_db_cleanup(db.clone()));

    let mut builder = HotblocksServerBuilder::new(db.clone());

    let http_client = sqd_data_client::reqwest::default_http_client();
    let mut datasets = Vec::new();
    for (default_name, dataset) in config.datasets.iter() {
        if let Some(hotblocks) = &dataset.real_time {
            let data_sources = hotblocks
                .data_sources
                .iter()
                .map(|url| ReqwestDataClient::new(http_client.clone(), url.clone()))
                .collect();
            let dataset_id = default_name.parse().map_err(|s| anyhow::anyhow!("{}", s))?;
            builder.add_dataset(
                dataset_id,
                hotblocks.kind,
                data_sources,
                hotblocks.retention.clone(),
            );
            datasets.push(dataset_id);
        }
    }

    Ok(Some(HotblocksHandle {
        server: builder.build().await?,
        db,
        datasets,
    }))
}

async fn run_db_cleanup(db: sqd_hotblocks::DBRef) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let db = db.clone();
        let result = tokio::task::spawn_blocking(move || db.cleanup()).await;
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => tracing::error!(error =? err, "database cleanup task failed"),
            Err(_) => tracing::error!("database cleanup task panicked"),
        }
    }
}

pub fn register_metrics(registry: &mut Registry, hotblocks: Arc<HotblocksHandle>) {
    let collector = Box::new(MetricsCollector { hotblocks });
    registry.register_collector(collector);
}

struct MetricsCollector {
    hotblocks: Arc<HotblocksHandle>,
}

impl MetricsCollector {
    fn collect(&self) -> Metrics {
        let metrics = Metrics::default();

        for dataset in &self.hotblocks.datasets {
            let labels = [("dataset_name", dataset.as_str().to_owned())];

            let head = self.hotblocks.server.get_head(*dataset).unwrap();
            let finalized_head = self.hotblocks.server.get_finalized_head(*dataset).unwrap();

            let first_block = self
                .hotblocks
                .db
                .snapshot()
                .get_first_chunk(*dataset)
                .inspect_err(|e| tracing::warn!(error = ?e, dataset = %dataset, "failed to get first chunk from hotblocks DB"))
                .ok()
                .flatten()
                .map(|chunk| chunk.first_block());

            if let Some(head) = head {
                metrics.head.get_or_create(&labels).set(head.number as i64);
            }
            if let Some(finalized_head) = finalized_head {
                metrics
                    .finalized_head
                    .get_or_create(&labels)
                    .set(finalized_head.number as i64);
            }
            if let Some(first_block) = first_block {
                metrics
                    .first_block
                    .get_or_create(&labels)
                    .set(first_block as i64);
            }
        }

        metrics
    }
}

impl prometheus_client::collector::Collector for MetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let metrics = self.collect();

        metrics.head.encode(encoder.encode_descriptor(
            "head",
            "The last block number in the hotblocks storage",
            None,
            metrics.head.metric_type(),
        )?)?;
        metrics.finalized_head.encode(encoder.encode_descriptor(
            "finalized_head",
            "The last finalized block number in the hotblocks storage",
            None,
            metrics.finalized_head.metric_type(),
        )?)?;
        metrics.first_block.encode(encoder.encode_descriptor(
            "first_block",
            "The first block existing in the hotblocks storage",
            None,
            metrics.first_block.metric_type(),
        )?)?;

        Ok(())
    }
}

type Labels = [(&'static str, String); 1];

#[derive(Default)]
struct Metrics {
    head: Family<Labels, Gauge>,
    finalized_head: Family<Labels, Gauge>,
    first_block: Family<Labels, Gauge>,
}

impl std::fmt::Debug for MetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsCollector").finish()
    }
}
