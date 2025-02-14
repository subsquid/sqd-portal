use crate::{
    network::DatasetsMapping,
    types::{BlockNumber, DatasetId, DatasetRef},
};
use serde::{Deserialize, Serialize};
use sqd_node as sqd_hotblocks;
use std::{borrow::Cow, collections::BTreeMap};
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

/// Struct built from merging the static config with datasets mapping
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DatasetMetadata<'c> {
    pub default_name: Cow<'c, str>,
    pub aliases: Cow<'c, [String]>,
    pub real_time: bool,
    pub dataset_id: Option<DatasetId>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServeMode {
    #[default]
    All,
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
                assert!(
                    name == *network_name,
                    "Dataset name must match SQD network name"
                );
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

    pub fn metadata<'r, 's: 'r, 'd: 'r>(
        &'s self,
        dataset: &'d str,
        network_mapping: &DatasetsMapping,
        serve: &ServeMode,
    ) -> Option<DatasetMetadata<'r>> {
        let config = self.get_by_name(dataset);
        let ds_ref = match serve {
            ServeMode::All => match config {
                Some(DatasetConfig {
                    network_ref: Some(ds_ref),
                    ..
                }) => ds_ref.clone(),
                Some(DatasetConfig { default_name, .. }) => DatasetRef::Name(default_name.clone()),
                None => DatasetRef::Name(dataset.to_owned()),
            },
        };
        match (config, network_mapping.resolve(ds_ref)) {
            (Some(config), dataset_id) => Some(DatasetMetadata {
                default_name: Cow::Borrowed(&config.default_name),
                aliases: Cow::Borrowed(&config.aliases),
                real_time: config.hotblocks.is_some(),
                dataset_id,
            }),
            (None, Some(dataset_id)) => Some(DatasetMetadata {
                default_name: Cow::Borrowed(dataset),
                aliases: Vec::new().into(),
                real_time: false,
                dataset_id: Some(dataset_id),
            }),
            (None, None) => None,
        }
    }

    pub fn all_dataset_names(
        &self,
        network_mapping: &DatasetsMapping,
        serve: &ServeMode,
    ) -> impl Iterator<Item = String> {
        match serve {
            ServeMode::All => (),
            // Other modes require different implementation
        };
        let mut network_datasets = network_mapping.inner().clone();
        let mut from_config = Vec::with_capacity(network_datasets.len() + self.datasets.len());
        for ds in self.datasets.iter() {
            match ds.network_ref {
                Some(DatasetRef::Name(ref name)) => {
                    network_datasets.remove_by_left(name);
                }
                Some(DatasetRef::Id(ref id)) => {
                    network_datasets.remove_by_right(id);
                }
                None => {
                    network_datasets.remove_by_left(&ds.default_name);
                }
            }
            from_config.push(ds.default_name.clone());
        }
        from_config
            .into_iter()
            .chain(network_datasets.into_iter().map(|(name, _)| name))
    }

    pub fn iter(&self) -> impl Iterator<Item = &DatasetConfig> {
        self.datasets.iter()
    }
}

type DatasetsConfigModel = BTreeMap<String, DatasetConfigModel>;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
struct DatasetConfigModel {
    #[serde(default)]
    aliases: Vec<String>,
    #[serde(default)]
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

impl<'c> From<&'c DatasetConfig> for DatasetMetadata<'c> {
    fn from(config: &'c DatasetConfig) -> Self {
        Self {
            default_name: Cow::Borrowed(&config.default_name),
            aliases: Cow::Borrowed(&config.aliases),
            real_time: config.hotblocks.is_some(),
            dataset_id: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use bimap::BiBTreeMap;

    use super::*;
    use crate::network::DatasetsMapping;

    #[test]
    fn test_datasets_config() {
        let json = serde_json::json!({
            "ethereum-mainnet": {
                "aliases": ["eth-main"],
            },
            "solana-mainnet": {
                "aliases": ["solana"],
                "sqd-network": {
                    "dataset-name": "solana-mainnet"
                },
                "real-time": {
                    "kind": "solana",
                    "data-sources": ["http://localhost:8080"],
                    "first-block": 250000000
                }
            },
            "local": {
                "real-time": {
                    "kind": "evm",
                    "data-sources": ["http://localhost:8081"],
                    "first-block": 0
                }
            },
            "custom": {
                "sqd-network": {
                    "dataset-id": "s3://solana-mainnet"
                }
            },
            "empty": {}
        });
        let model = serde_json::from_value::<DatasetsConfigModel>(json).unwrap();
        let config = DatasetsConfig::from_model(model);
        let mapping = DatasetsMapping::new(
            [
                (
                    "ethereum-mainnet".to_owned(),
                    DatasetId::from_url("s3://ethereum-mainnet-1"),
                ),
                (
                    "solana-mainnet".to_owned(),
                    DatasetId::from_url("s3://solana-mainnet"),
                ),
                (
                    "arbitrum-one".to_owned(),
                    DatasetId::from_url("s3://arbitrum-one"),
                ),
            ]
            .into_iter()
            .collect::<BiBTreeMap<_, _>>(),
        );
        let serve = ServeMode::All;

        let sol_meta = config.metadata("solana", &mapping, &serve).unwrap();
        let eth_meta = config
            .metadata("ethereum-mainnet", &mapping, &serve)
            .unwrap();
        assert_eq!(
            config.metadata("eth-main", &mapping, &serve).as_ref(),
            Some(&eth_meta)
        );
        let local_meta = config.metadata("local", &mapping, &serve).unwrap();
        let custom_meta = config.metadata("custom", &mapping, &serve).unwrap();
        let empty_meta = config.metadata("empty", &mapping, &serve).unwrap();
        assert_eq!(config.metadata("unknown", &mapping, &serve), None);

        assert_eq!(
            sol_meta,
            DatasetMetadata {
                default_name: "solana-mainnet".into(),
                aliases: (&["solana".to_owned()]).into(),
                real_time: true,
                dataset_id: Some(DatasetId::from_url("s3://solana-mainnet")),
            }
        );

        assert_eq!(
            eth_meta,
            DatasetMetadata {
                default_name: "ethereum-mainnet".into(),
                aliases: (&["eth-main".to_owned()]).into(),
                real_time: false,
                dataset_id: Some(DatasetId::from_url("s3://ethereum-mainnet-1")),
            }
        );

        assert_eq!(
            local_meta,
            DatasetMetadata {
                default_name: "local".into(),
                aliases: (&[]).into(),
                real_time: true,
                dataset_id: None,
            }
        );

        assert_eq!(
            custom_meta,
            DatasetMetadata {
                default_name: "custom".into(),
                aliases: (&[]).into(),
                real_time: false,
                dataset_id: Some(DatasetId::from_url("s3://solana-mainnet")),
            }
        );

        assert_eq!(
            empty_meta,
            DatasetMetadata {
                default_name: "empty".into(),
                aliases: (&[]).into(),
                real_time: false,
                dataset_id: None,
            }
        );

        assert_eq!(
            config
                .all_dataset_names(&mapping, &serve)
                .collect::<Vec<_>>(),
            vec![
                "custom",
                "empty",
                "ethereum-mainnet",
                "local",
                "solana-mainnet",
                "arbitrum-one"
            ]
        )
    }
}
