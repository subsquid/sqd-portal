use crate::types::{DatasetId, DatasetRef};
use anyhow::Context;
use bimap::BiBTreeMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;

pub struct DatasetsMapping {
    datasets: BiBTreeMap<String, DatasetId>,
}

impl DatasetsMapping {
    pub async fn load(url: &str) -> anyhow::Result<Self> {
        let file = load_networks(url).await?;
        let result = Self::parse(file)?;
        tracing::debug!(
            "Datasets file loaded, {} datasets found",
            result.datasets.len()
        );
        Ok(result)
    }

    #[cfg(test)]
    pub fn new(datasets: BiBTreeMap<String, DatasetId>) -> Self {
        Self { datasets }
    }

    pub async fn update(handle: &RwLock<Self>, url: &str) -> anyhow::Result<()> {
        tracing::info!("Updating datasets mapping");
        let mapping = Self::load(url).await?;
        *handle.write() = mapping;
        Ok(())
    }

    pub fn resolve(&self, dataset_ref: DatasetRef) -> Option<DatasetId> {
        match dataset_ref {
            DatasetRef::Name(name) => self.datasets.get_by_left(&name).cloned(),
            DatasetRef::Id(id) => self.datasets.contains_right(&id).then_some(id),
        }
    }

    pub fn default_name(&self, id: &DatasetId) -> Option<&str> {
        self.datasets.get_by_right(id).map(String::as_str)
    }

    pub fn dataset_ids(&self) -> impl Iterator<Item = &DatasetId> {
        self.datasets.right_values()
    }

    pub fn inner(&self) -> &BiBTreeMap<String, DatasetId> {
        &self.datasets
    }

    fn parse(file: NetworkDatasets) -> anyhow::Result<Self> {
        let mut datasets = BiBTreeMap::new();
        for dataset in file.sqd_network_datasets {
            let dataset_id = DatasetId::from_url(&dataset.id);
            datasets.insert(dataset.name, dataset_id);
        }

        Ok(Self { datasets })
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct NetworkDatasets {
    sqd_network_datasets: Vec<NetworkDataset>,
}

#[derive(Serialize, Deserialize, Debug)]
struct NetworkDataset {
    id: String,
    name: String,
}

async fn load_networks(url: &str) -> anyhow::Result<NetworkDatasets> {
    if let Some(path) = url.strip_prefix("file://") {
        load_local_file(path)
    } else {
        fetch_remote_file(url).await
    }
}

async fn fetch_remote_file(url: &str) -> anyhow::Result<NetworkDatasets> {
    tracing::debug!("Fetching remote file from {}", url);

    let response = reqwest::get(url).await?;
    let text = response.text().await?;

    serde_yaml::from_str(&text).with_context(|| format!("failed to parse dataset {url}"))
}

fn load_local_file(full_path: &str) -> anyhow::Result<NetworkDatasets> {
    tracing::debug!("Loading local file from {}", full_path);

    let file = File::open(full_path).with_context(|| format!("failed to open file {full_path}"))?;
    let reader = BufReader::new(file);

    serde_yaml::from_reader(reader).with_context(|| format!("failed to parse dataset {full_path}"))
}
