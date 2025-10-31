use std::{collections::BTreeMap, time::Duration};

use sqd_primitives::BlockNumber;

use crate::config::Config;

pub struct HotblocksHandle {
    pub client: reqwest::Client,
    // Datasets are referenced by their default name in the config
    pub urls: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamMode {
    Finalized,
    RealTime,
}

#[derive(thiserror::Error, Debug)]
pub enum HotblocksErr {
    #[error("Dataset not configured")]
    UnknownDataset,
    #[error("Request to hotblocks database failed: {0}")]
    Request(#[from] reqwest::Error),
}

pub async fn build_server(config: &Config) -> anyhow::Result<HotblocksHandle> {
    tracing::info!("Initializing hotblocks storage");

    let mut urls = BTreeMap::new();
    for (default_name, dataset) in config.datasets.iter() {
        if let Some(hotblocks) = &dataset.real_time {
            let remote_name = hotblocks.dataset.as_ref().unwrap_or(default_name);
            let url = hotblocks
                .url
                .join("datasets/")
                .unwrap()
                .join(remote_name)
                .unwrap();
            urls.insert(default_name.clone(), url.into());
        }
    }

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_gzip()
        .no_deflate()
        .no_brotli()
        .no_zstd()
        .connect_timeout(Duration::from_secs(1))
        .build()?;

    Ok(HotblocksHandle { client, urls })
}

impl HotblocksHandle {
    pub async fn request_head(&self, dataset: &str) -> Result<reqwest::Response, HotblocksErr> {
        let Some(url) = self.urls.get(dataset) else {
            return Err(HotblocksErr::UnknownDataset);
        };

        let response = self.client.get(format!("{url}/head")).send().await?;

        Ok(response)
    }

    pub async fn _get_head(&self, dataset: &str) -> Result<sqd_primitives::BlockRef, HotblocksErr> {
        let result = self
            .request_head(dataset)
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(result)
    }

    pub async fn request_finalized_head(
        &self,
        dataset: &str,
    ) -> Result<reqwest::Response, HotblocksErr> {
        let Some(url) = self.urls.get(dataset) else {
            return Err(HotblocksErr::UnknownDataset);
        };

        let response = self
            .client
            .get(format!("{url}/finalized-head"))
            .send()
            .await?;

        Ok(response)
    }

    pub async fn _get_finalized_head(
        &self,
        dataset: &str,
    ) -> Result<sqd_primitives::BlockRef, HotblocksErr> {
        let result = self
            .request_finalized_head(dataset)
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(result)
    }

    pub async fn stream(
        &self,
        dataset: &str,
        query: &str,
        mode: StreamMode,
    ) -> Result<reqwest::Response, HotblocksErr> {
        let Some(url) = self.urls.get(dataset) else {
            return Err(HotblocksErr::UnknownDataset);
        };

        let endpoint = match mode {
            StreamMode::Finalized => "finalized-stream",
            StreamMode::RealTime => "stream",
        };

        let response = self
            .client
            .post(format!("{url}/{endpoint}"))
            .header("Content-Type", "application/json")
            .body(query.to_string())
            .send()
            .await?;

        Ok(response)
    }

    pub async fn retain(&self, dataset: &str, from_block: BlockNumber) -> Result<(), HotblocksErr> {
        let Some(url) = self.urls.get(dataset) else {
            return Err(HotblocksErr::UnknownDataset);
        };

        self.client
            .post(format!("{url}/retention"))
            .json(&serde_json::json!({"FromBlock": {"number": from_block}}))
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    pub async fn retain_with_retries(
        &self,
        dataset: &str,
        from_block: BlockNumber,
    ) -> Result<(), HotblocksErr> {
        if self.urls.get(dataset).is_none() {
            return Err(HotblocksErr::UnknownDataset);
        };

        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY: Duration = Duration::from_secs(2);

        for attempt in 1..=MAX_RETRIES {
            match self.retain(dataset, from_block).await {
                Ok(_) => return Ok(()),
                Err(e) if attempt < MAX_RETRIES => {
                    tracing::debug!(
                        "Retain request failed for {}: {}. Retrying in {:?}...",
                        dataset,
                        e,
                        RETRY_DELAY
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(e) => return Err(e),
            }
        }

        unreachable!()
    }
}
