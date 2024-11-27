use crate::cli::Config;
use crate::types::DatasetId;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;

pub struct DatasetsMapping {
    available: Vec<DatasetConfig>,
    network_ids: BTreeMap<String, DatasetId>,
    default_names: BTreeMap<DatasetId, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetConfig {
    pub slug: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub data_sources: Vec<DatasetSourceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSourceConfig {
    pub kind: DataSourceKind,
    pub name_ref: String,

    #[serde(skip_deserializing)]
    pub id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DataSourceKind {
    #[default]
    SqdNetwork,
}

impl DatasetsMapping {
    pub async fn load(config: &Config) -> anyhow::Result<Self> {
        let file = load_networks(config).await?;
        let result = Self::parse(file)?;
        tracing::debug!(
            "Datasets file loaded, {} datasets found",
            result.available.len()
        );
        Ok(result)
    }

    fn parse(file: NetworkDatasets) -> anyhow::Result<Self> {
        let mut available = Vec::with_capacity(file.sqd_network_datasets.len());
        let mut network_ids = BTreeMap::new();
        let mut default_names = BTreeMap::new();
        for dataset in file.sqd_network_datasets {
            let dataset_id = DatasetId::from_url(&dataset.id);
            network_ids.insert(dataset.name.clone(), dataset_id.clone());
            default_names.insert(dataset_id, dataset.name.clone());
            let config = DatasetConfig {
                slug: dataset.name.clone(),
                aliases: Default::default(),
                data_sources: vec![DatasetSourceConfig {
                    kind: DataSourceKind::SqdNetwork,
                    name_ref: dataset.name,
                    id: dataset.id,
                }],
            };
            available.push(config);
        }

        Ok(Self {
            available,
            network_ids,
            default_names,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = &DatasetConfig> {
        self.available.iter()
    }

    pub fn find_dataset(&self, slug: &str) -> Option<&DatasetConfig> {
        self.available.iter().find(|d| {
            if d.slug == slug {
                return true;
            }

            d.aliases.contains(&slug.to_string())
        })
    }

    pub fn dataset_id(&self, slug: &str) -> Option<DatasetId> {
        self.network_ids.get(slug).cloned()
    }

    pub fn dataset_ids(&self) -> impl Iterator<Item = &DatasetId> + '_ {
        self.network_ids.values()
    }

    pub fn dataset_default_name(&self, id: &DatasetId) -> Option<&str> {
        self.default_names.get(id).map(|s| s.as_str())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct NetworkDataset {
    id: String,
    name: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct NetworkDatasets {
    sqd_network_datasets: Vec<NetworkDataset>,
}

async fn load_networks(config: &Config) -> anyhow::Result<NetworkDatasets> {
    let url = &config.sqd_network.datasets_url;

    if url.starts_with("file://") {
        load_local_file(url).await
    } else {
        fetch_remote_file(url).await
    }
}

async fn fetch_remote_file(url: &str) -> anyhow::Result<NetworkDatasets> {
    tracing::debug!("Fetching remote file from {}", url);

    let response = reqwest::get(url).await?;
    let text = response.text().await?;

    serde_yaml::from_str(&text).with_context(|| format!("failed to parse dataset {}", url))
}

async fn load_local_file(url: &str) -> anyhow::Result<NetworkDatasets> {
    let full_path = url
        .strip_prefix("file:/")
        .expect("Local file path must start with file://");

    tracing::debug!("Loading local file from {}", full_path);

    let file =
        File::open(full_path).with_context(|| format!("failed to open file {}", full_path))?;
    let reader = BufReader::new(file);

    serde_yaml::from_reader(reader)
        .with_context(|| format!("failed to parse dataset {}", full_path))
}
