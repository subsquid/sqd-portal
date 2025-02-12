use crate::types::{api_types::AvailableDatasetApiResponse, BlockNumber, DatasetRef};
use serde::{Deserialize, Serialize};
use sqd_node as sqd_hotblocks;
use std::collections::BTreeMap;
use url::Url;

#[derive(Debug, Clone)]
pub struct DatasetsConfig {
    datasets: Vec<DatasetConfig>,
    name_to_index: BTreeMap<String, usize>,
}

#[derive(Debug, Clone)]
pub struct DatasetConfig {
    pub default_name: String,
    pub aliases: Vec<String>,
    pub network_ref: Option<DatasetRef>,
    pub hotblocks: Option<RealTimeConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct RealTimeConfig {
    pub kind: sqd_hotblocks::DatasetKind,
    pub data_sources: Vec<Url>,
    pub first_block: BlockNumber,
}

impl DatasetsConfig {
    fn from_model(model: DatasetsConfigModel) -> Self {
        let mut datasets = Vec::with_capacity(model.len());
        let mut name_to_index = BTreeMap::new();
        for (name, dataset) in model {
            let index = datasets.len();
            name_to_index.insert(name.clone(), index);
            for alias in &dataset.aliases {
                name_to_index.insert(alias.clone(), index);
            }
            if let Some(DatasetRef::Name(network_name)) = &dataset.sqd_network {
                assert!(name == *network_name, "Dataset name must match SQD network name");
            }
            let config = DatasetConfig {
                default_name: name.clone(),
                aliases: dataset.aliases,
                network_ref: dataset.sqd_network,
                hotblocks: dataset.real_time,
            };
            datasets.push(config);
        }

        Self {
            datasets,
            name_to_index,
        }
    }

    pub fn get_by_name(&self, dataset: &str) -> Option<&DatasetConfig> {
        self.name_to_index
            .get(dataset)
            .and_then(|&index| self.datasets.get(index))
    }

    pub fn network_ref(&self, dataset: &str) -> Option<&DatasetRef> {
        self.get_by_name(dataset)
            .and_then(|d| d.network_ref.as_ref())
    }

    pub fn iter(&self) -> impl Iterator<Item = &DatasetConfig> {
        self.datasets.iter()
    }

    pub fn len(&self) -> usize {
        self.datasets.len()
    }
}

impl From<DatasetConfig> for AvailableDatasetApiResponse {
    fn from(dataset: DatasetConfig) -> Self {
        Self {
            dataset: dataset.default_name,
            aliases: dataset.aliases,
            real_time: dataset.hotblocks.is_some(),
        }
    }
}

type DatasetsConfigModel = BTreeMap<String, DatasetConfigModel>;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
struct DatasetConfigModel {
    #[serde(default)]
    aliases: Vec<String>,
    sqd_network: Option<DatasetRef>,
    real_time: Option<RealTimeConfig>,
}

impl<'de> Deserialize<'de> for DatasetsConfig {
    fn deserialize<D>(
        deserializer: D,
    ) -> Result<DatasetsConfig, <D as serde::Deserializer<'de>>::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let model = DatasetsConfigModel::deserialize(deserializer)?;
        Ok(DatasetsConfig::from_model(model))
    }
}
