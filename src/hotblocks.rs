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
use crate::metrics::IngestionTimestampClient;

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
        .with_cache_index_and_filter_blocks(hotblocks_config.cache_index_and_filter_blocks)
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

pub fn register_metrics(registry: &mut Registry, hotblocks: Arc<HotblocksHandle>, config: Arc<Config>) {
    let mut timestamp_clients = Vec::new();
    
    for dataset_id in &hotblocks.datasets {
        let dataset_name = dataset_id.as_str();

        if let Some(dataset_config) = config.datasets.get(dataset_name) {
            if let Some(real_time) = &dataset_config.real_time {
                for url in &real_time.data_sources {
                    tracing::info!(
                        dataset = %dataset_name,
                        url = %url,
                        "Using data source for metrics collection"
                    );
                    
                    let timestamp_client = Arc::new(IngestionTimestampClient::new(url.to_string()));
                    timestamp_clients.push((*dataset_id, url.to_string(), timestamp_client));
                }
            }
        }
    }
    
    for (dataset, url, client) in &timestamp_clients {
        let dataset_str = dataset.as_str().to_string();
        let network = "mainnet".to_string();
        let client = client.clone();
        let url = url.clone();
        let hotblocks = Arc::clone(&hotblocks);
        let dataset_id = *dataset;
        
        tracing::info!(
            dataset = %dataset_str,
            source = %url,
            "Starting block processing time measurement for head updates"
        );
        
        let mut head_receiver = hotblocks.server.subscribe_head_updates(dataset_id)
            .expect("Dataset should exist since it is from the same list used to build the server");
        
        tokio::spawn(async move {
            while head_receiver.changed().await.is_ok() {
                let current_head = head_receiver.borrow().clone();
                if let Some(head) = current_head {
                    let block_number = head.number;
                    let block_hash = head.hash;
                    
                    tracing::debug!(
                        dataset = %dataset_str,
                        source = %url,
                        block = %block_number,
                        "Measuring block processing time for new head"
                    );

                    crate::metrics::report_block_available(
                        &client,
                        &dataset_str,
                        &network, 
                        block_number,
                        &block_hash,
                    ).await;
                }
            }
            
            tracing::warn!(
                dataset = %dataset_str,
                source = %url,
                "Head update subscription ended"
            );
        });
    }
    
    let collector = Box::new(MetricsCollector { 
        hotblocks,
    });
    
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
            
            if let Some(head) = head {
                metrics.head.get_or_create(&labels).set(head.number as i64);
            }
            
            if let Some(finalized_head) = finalized_head {
                metrics
                    .finalized_head
                    .get_or_create(&labels)
                    .set(finalized_head.number as i64);
            }

            let snapshot = self.hotblocks.db.snapshot();
            let first_chunk = snapshot
                .get_first_chunk(*dataset)
                .inspect_err(|e| {
                    tracing::warn!(error = ?e, dataset = %dataset, "failed to get first chunk from hotblocks DB")
                })
                .ok()
                .flatten();
            let first_block = first_chunk.as_ref().map(|chunk| chunk.first_block());
            let first_block_timestamp = first_chunk.and_then(|chunk| chunk.first_block_time());

            let last_chunk = snapshot
                .get_last_chunk(*dataset)
                .inspect_err(|e| {
                    tracing::warn!(error = ?e, dataset = %dataset, "failed to get last chunk from hotblocks DB")
                })
                .ok()
                .flatten();
            let last_block_timestamp = last_chunk.and_then(|chunk| chunk.last_block_time());
            
            if let Some(first_block) = first_block {
                metrics
                    .first_block
                    .get_or_create(&labels)
                    .set(first_block as i64);
            }
            if let Some(timestamp) = first_block_timestamp {
                metrics
                    .first_block_timestamp
                    .get_or_create(&labels)
                    .set(timestamp as i64);
            }
            if let Some(timestamp) = last_block_timestamp {
                metrics
                    .last_block_timestamp
                    .get_or_create(&labels)
                    .set(timestamp as i64);
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
        metrics
            .first_block_timestamp
            .encode(encoder.encode_descriptor(
                "first_block_timestamp",
                "The timestamp of the first block in the hotblocks storage",
                None,
                metrics.first_block_timestamp.metric_type(),
            )?)?;
        metrics
            .last_block_timestamp
            .encode(encoder.encode_descriptor(
                "last_block_timestamp",
                "The timestamp of the last block in the hotblocks storage",
                None,
                metrics.last_block_timestamp.metric_type(),
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
    first_block_timestamp: Family<Labels, Gauge>,
    last_block_timestamp: Family<Labels, Gauge>,
}

impl std::fmt::Debug for MetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsCollector").finish()
    }
}
