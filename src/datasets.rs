use crate::cli::DatasetConfig;
use crate::cli::{Config, DatasetSourceConfig};
use crate::types::DatasetId;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;

pub struct Datasets {
    available: Vec<DatasetConfig>,
}

impl Datasets {
    pub async fn load(config: &Config) -> Self {
        datasets_load(config).await
    }

    pub fn available(&self) -> Vec<DatasetConfig> {
        self.available.clone()
    }

    pub fn find_dataset(&self, slug: &str) -> Option<&DatasetConfig> {
        self.available.iter().find(|d| {
            if d.slug == slug {
                return true;
            }

            if let Some(ref aliases) = d.aliases {
                return aliases.contains(&slug.to_string());
            }

            false
        })
    }

    pub fn dataset_id(&self, slug: &str) -> Option<DatasetId> {
        self.find_dataset(slug)
            .and_then(|dataset| dataset.network_dataset_id())
    }

    pub fn dataset_ids(&self) -> impl Iterator<Item = DatasetId> + '_ {
        self.available.iter().filter_map(|d| d.network_dataset_id())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct NetworkDataset {
    id: String,
    name: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct DatasetList {
    sqd_network_datasets: Vec<NetworkDataset>,
}

async fn fetch_remote_file(url: &str) -> anyhow::Result<DatasetList> {
    tracing::debug!("Fetching remote file from {}", url);

    let response = reqwest::get(url).await?;
    let text = response.text().await?;

    serde_yaml::from_str(&text).with_context(|| format!("failed to parse dataset {}", url))
}

async fn load_local_file(url: &str) -> anyhow::Result<DatasetList> {
    let full_path = url.replace("file:/", "");

    tracing::debug!("Loading local file from {}", full_path);

    let file = File::open(full_path.clone())
        .with_context(|| format!("failed to open file {}", full_path))?;
    let reader = BufReader::new(file);

    serde_yaml::from_reader(reader)
        .with_context(|| format!("failed to parse dataset {}", full_path))
}

async fn datasets_load(config: &Config) -> Datasets {
    if let Ok(file) = load_file(config).await {
        tracing::debug!(
            "Datasets file loaded, {} datasets found",
            file.sqd_network_datasets.len()
        );

        let available = file
            .sqd_network_datasets
            .iter()
            .map(|d| DatasetConfig {
                slug: d.name.clone(),
                aliases: None,
                data_sources: vec![DatasetSourceConfig {
                    kind: "sqd_network".into(),
                    name_ref: d.name.clone(),
                    id: d.id.clone(),
                }],
            })
            .collect();

        Datasets { available }
    } else {
        panic!(
            "Datasets file {} can't be loaded",
            config.sqd_network.datasets
        )
    }
}

async fn load_file(config: &Config) -> anyhow::Result<DatasetList> {
    let url = config.sqd_network.datasets.clone();

    if url.starts_with("file://") {
        load_local_file(&url).await
    } else {
        fetch_remote_file(&url).await
    }
}
