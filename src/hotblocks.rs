use std::{sync::Arc, time::Duration};

use anyhow::Context;

use crate::cli;

pub fn build_server(config: &cli::Config) -> anyhow::Result<Option<sqd_node::Node>> {
    let has_sources = config.datasets.iter().any(|d| d.hotblocks.is_some());
    if !has_sources {
        return Ok(None);
    }

    tracing::info!("Initializing hotblocks storage");
    let path = config
        .hotblocks_db_path
        .as_ref()
        .expect("Hotblocks database path not specified");
    let db = Arc::new(
        sqd_storage::db::DatabaseSettings::default()
            .set_data_cache_size(config.hotblocks_data_cache_mb)
            .open(path)
            .context("failed to open hotblocks database")?,
    );

    tokio::spawn(run_db_cleanup(db.clone()));

    let mut builder = sqd_node::NodeBuilder::new(db);

    for dataset in config.datasets.iter() {
        if let Some(hotblocks) = &dataset.hotblocks {
            let ds = builder.add_dataset(
                hotblocks.kind,
                dataset
                    .default_name
                    .as_str()
                    .try_into()
                    .map_err(|s| anyhow::anyhow!("{}", s))?,
                hotblocks.retention.clone(),
            );
            for url in &hotblocks.data_sources {
                ds.add_data_source(url.clone());
            }
        }
    }

    Ok(Some(builder.build()))
}

async fn run_db_cleanup(db: sqd_node::DBRef) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let db = db.clone();
        let result = tokio::task::spawn_blocking(move || db.cleanup()).await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::error!(error =? err, "database cleanup task failed"),
            Err(_) => tracing::error!("database cleanup task panicked"),
        }
    }
}
