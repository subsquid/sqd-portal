use std::{sync::Arc, time::Duration};

use anyhow::Context;

use crate::cli;

const SOLANA_FIRST_BLOCK: u64 = 250_000_000;

pub fn build_server(config: &cli::Config) -> anyhow::Result<Option<sqd_node::Node>> {
    let Some(path) = &config.hotblocks_db_path else {
        return Ok(None);
    };

    let db = Arc::new(
        sqd_storage::db::DatabaseSettings::default()
            .set_data_cache_size(config.hotblocks_data_cache_mb)
            .open(&path)
            .context("failed to open hotblocks database")?,
    );

    tokio::spawn(run_db_cleanup(db.clone()));

    let mut builder = sqd_node::NodeBuilder::new(db);

    if let Some(urls) = &config.solana_hotblocks_urls {
        let ds = builder.add_dataset(
            sqd_node::DatasetKind::Solana,
            "solana".try_into().unwrap(),
            sqd_node::RetentionStrategy::FromBlock(SOLANA_FIRST_BLOCK),
        );
        for url in urls {
            ds.add_data_source(url);
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
