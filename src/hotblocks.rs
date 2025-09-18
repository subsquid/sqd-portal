use std::{collections::BTreeMap, time::Duration};

use sqd_primitives::BlockNumber;

use crate::config::Config;

pub struct HotblocksHandle {
    pub client: reqwest::Client,
    // Datasets are referenced by their default name in the config
    pub urls: BTreeMap<String, String>,
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
        .read_timeout(Duration::from_secs(1))
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
    ) -> Result<reqwest::Response, HotblocksErr> {
        let Some(url) = self.urls.get(dataset) else {
            return Err(HotblocksErr::UnknownDataset);
        };

        let response = self
            .client
            .post(format!("{url}/stream"))
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
}
