use crate::cli::{Config, DatasetConfig, DatasetSourceConfig};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;

#[derive(Serialize, Deserialize, Debug)]
struct Dataset {
    pub id: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DatasetList {
    #[serde(rename = "sqd-network-datasets")]
    sqd_network_datasets: Vec<Dataset>,
}

async fn fetch_remote_file(url: &str) -> anyhow::Result<DatasetList> {
    tracing::debug!("Fetching remote file from {}", url);

    let response = reqwest::get(url).await?;
    let text = response.text().await?;

    let parser =
        serde_yaml::from_str(&text).with_context(|| format!("failed to parse dataset {}", url));

    Ok(parser?)
}

async fn load_local_file(url: &str) -> anyhow::Result<DatasetList> {
    let full_path = url.replace("file:/", "");

    tracing::debug!("Loading local file from {}", full_path);

    let file = File::open(full_path.clone())
        .with_context(|| format!("failed to open file {}", full_path))?;
    let reader = BufReader::new(file);

    let parser = serde_yaml::from_reader(reader)
        .with_context(|| format!("failed to parse dataset {}", full_path));

    Ok(parser?)
}

pub async fn datasets_load(config: &Config) -> Vec<DatasetConfig> {
    if let Ok(file) = load_file(config).await {
        tracing::debug!(
            "Datasets file loaded, {} datasets found",
            file.sqd_network_datasets.len()
        );

        let loaded = file
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

        loaded
    } else {
        panic!(
            "Datasets file {} can't be loaded",
            config.sqd_network.datasets
        )
    }
}

pub async fn load_file(config: &Config) -> anyhow::Result<DatasetList> {
    let url = config.sqd_network.datasets.clone();

    if url.starts_with("file://") {
        load_local_file(&url).await
    } else {
        fetch_remote_file(&url).await
    }
}
