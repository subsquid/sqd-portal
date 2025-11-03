use crate::{
    config::{Config, RealTimeConfig, ServeMode},
    types::{DatasetId, DatasetRef},
    utils::RwLock,
};
use anyhow::Context;
use bimap::BiBTreeMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    fs::File,
    io::BufReader,
};

pub struct Datasets {
    datasets: Vec<DatasetConfig>,
    alias_to_index: BTreeMap<String, usize>,
    id_to_default_name: BTreeMap<DatasetId, String>,
}

#[derive(Debug, Clone)]
pub struct DatasetConfig {
    pub default_name: String,
    pub aliases: Vec<String>,
    pub network_id: Option<DatasetId>,
    pub hotblocks: Option<RealTimeConfig>,
}

type DatasetsMapping = BiBTreeMap<String, DatasetId>;

impl Datasets {
    pub async fn load(config: &Config) -> anyhow::Result<Self> {
        let mapping = load_mapping(&config.sqd_network.datasets_url).await?;
        Self::parse(config, mapping)
    }

    fn parse(config: &Config, mapping: BiBTreeMap<String, DatasetId>) -> anyhow::Result<Self> {
        let mut datasets: Vec<DatasetConfig> = Vec::new();
        let mut alias_to_index = BTreeMap::new();
        let mut id_to_default_name = BTreeMap::new();
        for (default_name, ds) in config.datasets.clone() {
            let network_id = match ds.sqd_network {
                Some(DatasetRef::Name(name)) => mapping.get_by_left(&name).cloned(),
                Some(DatasetRef::Id(id)) => Some(id),
                None => mapping.get_by_left(&default_name).cloned(),
            };

            let index = datasets.len();
            for alias in ds.aliases.iter().chain(Some(&default_name)) {
                let prev = alias_to_index.insert(alias.clone(), index);
                if let Some(prev) = prev {
                    anyhow::bail!(
                        "Alias {} is already used by dataset {}",
                        alias,
                        datasets[prev].default_name
                    );
                }
            }

            if let Some(id) = &network_id {
                id_to_default_name
                    .insert(id.clone(), default_name.clone())
                    .inspect(|prev| {
                        tracing::warn!(
                            "Datasets '{}' and '{}' point to the same network ID '{}'",
                            default_name,
                            prev,
                            id.to_string()
                        );
                    });
            }

            datasets.push(DatasetConfig {
                default_name,
                aliases: ds.aliases,
                network_id,
                hotblocks: ds.real_time,
            });
        }

        if let ServeMode::All = config.sqd_network.serve {
            // Add all datasets from the mapping that are not already present
            for (name, id) in mapping {
                if let Entry::Vacant(entry) = alias_to_index.entry(name.clone()) {
                    entry.insert(datasets.len());
                    datasets.push(DatasetConfig {
                        default_name: name.clone(),
                        aliases: Vec::new(),
                        network_id: Some(id.clone()),
                        hotblocks: None,
                    });
                    if let Entry::Vacant(entry) = id_to_default_name.entry(id) {
                        entry.insert(name);
                    }
                }
            }
        }

        datasets.shrink_to_fit();
        Ok(Self {
            datasets,
            alias_to_index,
            id_to_default_name,
        })
    }

    pub fn get(&self, alias: &str) -> Option<&DatasetConfig> {
        self.alias_to_index
            .get(alias)
            .and_then(|&index| self.datasets.get(index))
    }

    pub fn iter(&self) -> impl Iterator<Item = &DatasetConfig> {
        self.datasets.iter()
    }

    #[cfg(test)]
    pub fn network_datasets(&self) -> impl Iterator<Item = (&DatasetId, &str)> {
        self.id_to_default_name
            .iter()
            .map(|(id, name)| (id, name.as_str()))
    }

    pub fn default_name(&self, id: &DatasetId) -> Option<&str> {
        self.id_to_default_name.get(id).map(String::as_str)
    }

    pub async fn update(handle: &RwLock<Self>, config: &Config) -> anyhow::Result<()> {
        tracing::info!("Updating datasets mapping");
        let datasets = Self::load(config).await?;
        *handle.write() = datasets;
        Ok(())
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

async fn load_mapping(url: &str) -> anyhow::Result<DatasetsMapping> {
    let file = load_networks(url).await?;
    let result = parse_mapping(file)?;
    tracing::debug!("Datasets file loaded, {} datasets found", result.len());
    Ok(result)
}

fn parse_mapping(file: NetworkDatasets) -> anyhow::Result<DatasetsMapping> {
    let mut datasets = BiBTreeMap::new();
    for dataset in file.sqd_network_datasets {
        let dataset_id = DatasetId::from_url(&dataset.id);
        datasets.insert(dataset.name, dataset_id);
    }
    Ok(datasets)
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

#[cfg(test)]
mod tests {
    use bimap::BiBTreeMap;

    use super::*;

    impl PartialEq for DatasetConfig {
        fn eq(&self, other: &Self) -> bool {
            self.default_name == other.default_name
                && self.aliases == other.aliases
                && self.hotblocks.is_some() == other.hotblocks.is_some()
                && self.network_id == other.network_id
        }
    }
    impl Eq for DatasetConfig {}

    #[test]
    fn test_datasets_config() {
        let json = serde_json::json!({
            "hostname": "http://localhost:8000",
            "sqd_network": {
                "datasets": "file://datasets.yaml",
                "serve": "all"
            },
            "datasets": {
                "ethereum-mainnet": {
                    "aliases": ["eth-main"],
                },
                "solana-mainnet": {
                    "aliases": ["solana"],
                    "sqd_network": {
                        "dataset_name": "solana-mainnet"
                    },
                    "real_time": {
                        "url": "http://localhost:8080"
                    }
                },
                "local": {
                    "real_time": {
                        "url": "http://localhost:8081"
                    }
                },
                "custom": {
                    "sqd_network": {
                        "dataset_id": "s3://solana-mainnet"
                    }
                },
                "beta": {
                    "sqd_network": {
                        "dataset_id": "s3://solana-mainnet-2" // not in mapping
                    }
                },
                "empty": {}
            }
        });
        let config = serde_json::from_value::<Config>(json).unwrap();
        let mapping = [
            (
                "ethereum-mainnet".to_owned(),
                DatasetId::from_url("s3://ethereum-mainnet-1"),
            ),
            (
                "eth-main".to_owned(),
                DatasetId::from_url("s3://ethereum-mainnet-2"),
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
        .collect::<BiBTreeMap<_, _>>();
        let datasets = Datasets::parse(&config, mapping).unwrap();

        let beta_meta = datasets.get("beta").unwrap();
        let sol_meta = datasets.get("solana").unwrap();
        let eth_meta = datasets.get("ethereum-mainnet").unwrap();
        assert_eq!(datasets.get("eth-main"), Some(eth_meta));
        let local_meta = datasets.get("local").unwrap();
        let custom_meta = datasets.get("custom").unwrap();
        let empty_meta = datasets.get("empty").unwrap();
        assert!(datasets.get("unknown").is_none());

        let hotblocks_config = sol_meta.hotblocks.clone().unwrap();

        assert_eq!(
            beta_meta,
            &DatasetConfig {
                default_name: "beta".into(),
                aliases: vec![],
                hotblocks: None,
                network_id: Some(DatasetId::from_url("s3://solana-mainnet-2")),
            }
        );

        assert_eq!(
            sol_meta,
            &DatasetConfig {
                default_name: "solana-mainnet".into(),
                aliases: vec!["solana".to_owned()],
                hotblocks: Some(hotblocks_config.clone()),
                network_id: Some(DatasetId::from_url("s3://solana-mainnet")),
            }
        );

        assert_eq!(
            eth_meta,
            &DatasetConfig {
                default_name: "ethereum-mainnet".into(),
                aliases: vec!["eth-main".to_owned()],
                hotblocks: None,
                network_id: Some(DatasetId::from_url("s3://ethereum-mainnet-1")),
            }
        );

        assert_eq!(
            local_meta,
            &DatasetConfig {
                default_name: "local".into(),
                aliases: vec![],
                hotblocks: Some(hotblocks_config),
                network_id: None,
            }
        );

        assert_eq!(
            custom_meta,
            &DatasetConfig {
                default_name: "custom".into(),
                aliases: vec![],
                hotblocks: None,
                network_id: Some(DatasetId::from_url("s3://solana-mainnet")),
            }
        );

        assert_eq!(
            empty_meta,
            &DatasetConfig {
                default_name: "empty".into(),
                aliases: vec![],
                hotblocks: None,
                network_id: None,
            }
        );

        assert_eq!(
            datasets
                .iter()
                .map(|d| d.default_name.clone())
                .collect::<Vec<_>>(),
            vec![
                "beta",
                "custom",
                "empty",
                "ethereum-mainnet",
                "local",
                "solana-mainnet",
                "arbitrum-one"
            ]
        );

        assert_eq!(
            datasets.network_datasets().collect::<Vec<_>>(),
            vec![
                (&DatasetId::from_url("s3://arbitrum-one"), "arbitrum-one"),
                (
                    &DatasetId::from_url("s3://ethereum-mainnet-1"),
                    "ethereum-mainnet"
                ),
                (
                    &DatasetId::from_url("s3://solana-mainnet"),
                    "solana-mainnet"
                ),
                (&DatasetId::from_url("s3://solana-mainnet-2"), "beta"),
                // ethereum-mainnet-2 has been overwritten by eth-main alias
            ]
        );
    }

    #[test]
    fn test_datasets_config_manual_mode() {
        let json = serde_json::json!({
            "hostname": "http://localhost:8000",
            "sqd_network": {
                "datasets": "file://datasets.yaml",
                "serve": "manual"
            },
            "datasets": {
                "ethereum-mainnet": {
                    "aliases": ["eth-main"],
                },
                "custom": {
                    "sqd_network": {
                        "dataset_id": "s3://solana-mainnet"
                    }
                }
            }
        });
        let config = serde_json::from_value::<Config>(json).unwrap();
        let mapping = [
            (
                "ethereum-mainnet".to_owned(),
                DatasetId::from_url("s3://ethereum-mainnet-1"),
            ),
            (
                "arbitrum-one".to_owned(),
                DatasetId::from_url("s3://arbitrum-one"),
            ),
        ]
        .into_iter()
        .collect::<BiBTreeMap<_, _>>();
        let datasets = Datasets::parse(&config, mapping).unwrap();

        // In manual mode, only explicitly configured datasets should be available
        assert!(datasets.get("ethereum-mainnet").is_some());
        assert!(datasets.get("eth-main").is_some());
        assert!(datasets.get("custom").is_some());

        // arbitrum-one should NOT be available since it's not in the config
        assert!(datasets.get("arbitrum-one").is_none());

        // Verify the dataset list only contains configured datasets
        assert_eq!(
            datasets
                .iter()
                .map(|d| d.default_name.clone())
                .collect::<Vec<_>>(),
            vec!["custom", "ethereum-mainnet"]
        );
    }
}
