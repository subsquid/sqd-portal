use std::{io::Read, sync::Arc, time::{Duration, Instant}};

use anyhow::Context;
use flate2::bufread::GzDecoder;
use serde::Deserialize;
use sqd_primitives::{sid::SID, BlockRef};
use parking_lot::Mutex;
use prometheus_client::{
    encoding::{DescriptorEncoder, EncodeMetric},
    metrics::{family::Family, gauge::Gauge},
    registry::{Registry, Unit},
};
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_hotblocks::{
    DatabaseSettings, Node as HotblocksServer, NodeBuilder as HotblocksServerBuilder, Query,
};

use crate::config::Config;

pub async fn build_server(config: &Config) -> anyhow::Result<Option<HotblocksServer>> {
    let has_sources = config.datasets.iter().any(|(_, d)| d.real_time.is_some());
    if !has_sources {
        return Ok(None);
    }

    tracing::info!("Initializing hotblocks storage");
    let path = config
        .hotblocks_db_path
        .as_ref()
        .expect("Hotblocks database path not specified");
    let db = Arc::new(
        DatabaseSettings::default()
            .with_data_cache_size(config.hotblocks_data_cache_mb)
            .with_rocksdb_stats(true)
            .open(path)
            .context("failed to open hotblocks database")?,
    );

    tokio::spawn(run_db_cleanup(db.clone()));

    let mut builder = HotblocksServerBuilder::new(db);

    for (default_name, dataset) in config.datasets.iter() {
        if let Some(hotblocks) = &dataset.real_time {
            let http_client = sqd_data_client::reqwest::default_http_client();
            let data_sources = hotblocks
                .data_sources
                .iter()
                .map(|url| ReqwestDataClient::new(http_client.clone(), url.clone()))
                .collect();
            builder.add_dataset(
                default_name.parse().map_err(|s| anyhow::anyhow!("{}", s))?,
                hotblocks.kind,
                data_sources,
                hotblocks.retention.clone(),
            );
        }
    }

    Ok(Some(builder.build().await?))
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

pub fn register_metrics(registry: &mut Registry) {
    let collector = Box::new(MetricsCollector { });
    registry.register_collector(collector);
}

struct MetricsCollector {
}

impl prometheus_client::collector::Collector for MetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let metrics = HOTBLOCKS_MONITOR.read().unwrap_or_default();

        metrics.head.encode(encoder.encode_descriptor(
            "head",
            "The last block number in the hotblocks storage",
            None,
            metrics.head.metric_type(),
        )?)?;
        metrics.head_timestamp.encode(encoder.encode_descriptor(
            "head_timestamp",
            "Timestamp of the last block in the hotblocks storage",
            None,
            metrics.head_timestamp.metric_type(),
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

        metrics.memtables_size.encode(encoder.encode_descriptor(
            "memtables_size",
            "The approximate size of memtables in hotblocks storage",
            Some(&Unit::Bytes),
            metrics.memtables_size.metric_type(),
        )?)?;
        metrics.sst_files_size.encode(encoder.encode_descriptor(
            "sst_files_size",
            "The total size of all SST files in the hotblocks storage",
            Some(&Unit::Bytes),
            metrics.sst_files_size.metric_type(),
        )?)?;
        metrics.num_files.encode(encoder.encode_descriptor(
            "num_files",
            "The number of files in the hotblocks storage",
            None,
            metrics.num_files.metric_type(),
        )?)?;
        metrics.num_keys.encode(encoder.encode_descriptor(
            "num_keys",
            "The estimated number of keys in the hotblocks storage",
            None,
            metrics.num_keys.metric_type(),
        )?)?;
        metrics.time_spent.encode(encoder.encode_descriptor(
            "time_spent",
            "Time spent on hotplocks metrics collection in milliseconds",
            None,
            metrics.time_spent.metric_type(),
        )?)?;

        Ok(())
    }
}

type Labels = [(&'static str, String); 1];

#[derive(Default, Clone)]
pub struct Metrics {
    head: Family<Labels, Gauge>,
    head_timestamp: Family<Labels, Gauge>,
    finalized_head: Family<Labels, Gauge>,
    first_block: Family<Labels, Gauge>,
    memtables_size: Family<Labels, Gauge>,
    sst_files_size: Family<Labels, Gauge>,
    num_files: Family<Labels, Gauge>,
    num_keys: Family<Labels, Gauge>,
    time_spent: Gauge,
}

impl std::fmt::Debug for MetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsCollector").finish()
    }
}

pub struct HotblocksMonitor {
    metrics: Mutex<Option<Metrics>>
}

#[derive(Deserialize)]
struct TSHeader {
    timestamp: u64,
}

#[derive(Deserialize)]
struct TSResponse {
    header: TSHeader
}

impl HotblocksMonitor {
    pub fn new() -> Self {
        HotblocksMonitor { metrics: Mutex::new(None) }
    }

    async fn extract_timestamp(hotblocks: &Arc<HotblocksServer>, id: SID<48>, head: &BlockRef) -> Result<u64, anyhow::Error> {
        let num = head.number;
        let str_query = format!("{{\"fromBlock\":{num},\"toBlock\":{num},\"type\":\"solana\",\"fields\":{{\"block\":{{\"timestamp\":true}} }} }}");
        let query = Query::from_json_bytes(str_query.as_bytes()).unwrap();
        let mut result = hotblocks.query(id, query).await?;

        let bytes = result.next_bytes().await?.unwrap_or_default();
        let qqq = bytes.to_vec();
        let mut d = GzDecoder::new(qqq.as_slice());
        let mut s = String::new();
        d.read_to_string(&mut s).unwrap();
        let res = serde_json::from_str::<TSResponse>(&s)?;
        Ok(res.header.timestamp)
    }

    pub async fn collect(&self, hotblocks: Option<Arc<HotblocksServer>>) {
        if let Some(hotblocks) = hotblocks {
            let metrics = Metrics::default();
            let start = Instant::now();

            match hotblocks.get_db_metrics() {
                Ok(db_metrics) => {
                    for (cf, cf_metrics) in db_metrics.cf_metrics {
                        let labels = [("column_family", cf)];
                        if let Some(memtables_size) = cf_metrics.memtables_size {
                            metrics
                                .memtables_size
                                .get_or_create(&labels)
                                .set(memtables_size as i64);
                        }
                        if let Some(num_keys) = cf_metrics.num_keys {
                            metrics.num_keys.get_or_create(&labels).set(num_keys as i64);
                        }
                        metrics
                            .sst_files_size
                            .get_or_create(&labels)
                            .set(cf_metrics.sst_files_size as i64);
                        metrics
                            .num_files
                            .get_or_create(&labels)
                            .set(cf_metrics.file_count as i64);
                    }
                }
                Err(err) => {
                    tracing::error!(error = ?err, "failed to get hotblocks database metrics");
                }
            }
    
            for id in hotblocks.get_all_datasets() {
                let labels = [("dataset_name", id.as_str().to_owned())];
    
                let head = hotblocks.get_head(id).unwrap();
                let finalized_head = hotblocks.get_finalized_head(id).unwrap();
                let first_block = hotblocks
                    .get_first_block(id)
                    .expect("First block should be read successfully from the hotblocks storage");
    
                if let Some(head) = head {
                    if let Ok(timestamp) = Self::extract_timestamp(&hotblocks, id, &head).await {
                        metrics.head_timestamp.get_or_create(&labels).set(timestamp as i64);
                    }
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
            let duration = start.elapsed();
            metrics.time_spent.set(duration.subsec_millis().into());
            *self.metrics.lock() = Some(metrics);
        }
    }

    pub fn read(&self) -> Option<Metrics> {
        let metrics = self.metrics.lock();
        (*metrics).clone()
    }
}

lazy_static::lazy_static! {
    pub static ref HOTBLOCKS_MONITOR: HotblocksMonitor = HotblocksMonitor::new();
}