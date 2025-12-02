use crate::{
    config::{Config, RealTimeConfig, ServeMode},
    types::{DatasetId, DatasetRef},
    utils::RwLock,
};
use anyhow::Context;
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
    pub kind: String,
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
struct NetDataset {
    dataset_id: DatasetId,
    kind: String,
}

type DatasetsMapping = BTreeMap<String, NetDataset>;

impl Datasets {
    pub async fn load(config: &Config) -> anyhow::Result<Self> {
        let mapping = load_mapping(&config.sqd_network.datasets_url).await?;
        Self::parse(config, mapping)
    }

    fn parse(config: &Config, mapping: DatasetsMapping) -> anyhow::Result<Self> {
        let mut datasets: Vec<DatasetConfig> = Vec::new();
        let mut alias_to_index = BTreeMap::new();
        let mut id_to_default_name = BTreeMap::new();
        for (default_name, ds) in config.datasets.clone() {
            let (network_id, ds_summary) =
                netdata_from_mapping(ds.sqd_network, &default_name, &mapping);
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

            let kind = ds
                .kind
                .or_else(|| ds_summary.as_ref().map(|s| s.kind.clone()))
                .ok_or_else(|| anyhow::anyhow!("Unknown kind for dataset {default_name}"))?;

            datasets.push(DatasetConfig {
                default_name,
                aliases: ds.aliases,
                network_id,
                hotblocks: ds.real_time,
                kind: kind,
            });
        }

        if let ServeMode::All = config.sqd_network.serve {
            // Add all datasets from the mapping that are not already present
            for (name, ds) in mapping {
                if let Entry::Vacant(entry) = alias_to_index.entry(name.clone()) {
                    entry.insert(datasets.len());

                    datasets.push(DatasetConfig {
                        default_name: name.clone(),
                        aliases: Vec::new(),
                        network_id: Some(ds.dataset_id.clone()),
                        hotblocks: None,
                        kind: ds.kind.clone(),
                    });

                    if let Entry::Vacant(entry) = id_to_default_name.entry(ds.dataset_id) {
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

fn netdata_from_mapping<'a>(
    dref: Option<DatasetRef>,
    default_name: &'a str,
    mapping: &'a DatasetsMapping,
) -> (Option<DatasetId>, Option<&'a NetDataset>) {
    let (tmp, key) = match &dref {
        Some(DatasetRef::Id(id)) => (Some(id.clone()), default_name),
        Some(DatasetRef::Name(name)) => (None, name.as_str()),
        None => (None, default_name),
    };

    let entry = mapping.get(key);
    let id = tmp.or_else(|| entry.map(|m| m.dataset_id.clone()));

    (id, entry)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "kebab-case")]
struct NetworkDatasets {
    sqd_network_datasets: Vec<NetworkDataset>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct NetworkDataset {
    id: String,
    name: String,
    kind: String,
}

async fn load_mapping(url: &str) -> anyhow::Result<DatasetsMapping> {
    let file = load_networks(url).await?;
    let mapping = parse_network_datasets(file.clone())?;
    tracing::debug!("Datasets file loaded, {} datasets found", mapping.len());
    Ok(mapping)
}

fn parse_network_datasets(file: NetworkDatasets) -> anyhow::Result<DatasetsMapping> {
    let mut datasets = BTreeMap::new();
    for dataset in file.sqd_network_datasets {
        let summary = NetDataset {
            dataset_id: DatasetId::from_url(&dataset.id),
            kind: dataset.kind.to_string(),
        };
        datasets.insert(dataset.name.clone(), summary);
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
                    "kind": "evm"
                },
                "solana-mainnet": {
                    "aliases": ["solana"],
                    "kind": "solana",
                    "sqd_network": {
                        "dataset_name": "solana-mainnet"
                    },
                    "real_time": {
                        "url": "http://localhost:8080"
                    }
                },
                "local": {
                    "kind": "solana",
                    "real_time": {
                        "url": "http://localhost:8081"
                    }
                },
                "custom": {
                    "kind": "solana",
                    "sqd_network": {
                        "dataset_id": "s3://solana-mainnet"
                    }
                },
                "beta": {
                    "kind": "solana",
                    "sqd_network": {
                        "dataset_id": "s3://solana-mainnet-2" // not in mapping
                    }
                },
                "empty": {
                    "kind": "empty",
                },
            }
        });
        let config = serde_json::from_value::<Config>(json).unwrap();
        let mapping = [
            (
                "ethereum-mainnet".to_owned(),
                NetDataset {
                    dataset_id: DatasetId::from_url("s3://ethereum-mainnet-1"),
                    kind: "shall_be_overwritten_by_config".to_string(),
                },
            ),
            (
                "eth-main".to_owned(),
                NetDataset {
                    dataset_id: DatasetId::from_url("s3://ethereum-mainnet-2"),
                    kind: "evm".to_string(),
                },
            ),
            (
                "solana-mainnet".to_owned(),
                NetDataset {
                    dataset_id: DatasetId::from_url("s3://solana-mainnet"),
                    kind: "solana".to_string(),
                },
            ),
            (
                "arbitrum-one".to_owned(),
                NetDataset {
                    dataset_id: DatasetId::from_url("s3://arbitrum-one"),
                    kind: "evm".to_string(),
                },
            ),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();

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
                kind: "solana".to_string(),
            }
        );

        assert_eq!(
            sol_meta,
            &DatasetConfig {
                default_name: "solana-mainnet".into(),
                aliases: vec!["solana".to_owned()],
                hotblocks: Some(hotblocks_config.clone()),
                network_id: Some(DatasetId::from_url("s3://solana-mainnet")),
                kind: "solana".to_string(),
            }
        );

        assert_eq!(
            eth_meta,
            &DatasetConfig {
                default_name: "ethereum-mainnet".into(),
                aliases: vec!["eth-main".to_owned()],
                hotblocks: None,
                network_id: Some(DatasetId::from_url("s3://ethereum-mainnet-1")),
                kind: "evm".to_string(),
            }
        );

        assert_eq!(
            local_meta,
            &DatasetConfig {
                default_name: "local".into(),
                aliases: vec![],
                hotblocks: Some(hotblocks_config),
                network_id: None,
                kind: "solana".to_string(),
            }
        );

        assert_eq!(
            custom_meta,
            &DatasetConfig {
                default_name: "custom".into(),
                aliases: vec![],
                hotblocks: None,
                network_id: Some(DatasetId::from_url("s3://solana-mainnet")),
                kind: "solana".to_string(),
            }
        );

        assert_eq!(
            empty_meta,
            &DatasetConfig {
                default_name: "empty".into(),
                aliases: vec![],
                hotblocks: None,
                network_id: None,
                kind: "empty".to_string(),
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
                    "kind": "evm",
                },
                "custom": {
                    "kind": "solana",
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
                NetDataset {
                    dataset_id: DatasetId::from_url("s3://ethereum-mainnet-1"),
                    kind: "evm".to_string(),
                },
            ),
            (
                "arbitrum-one".to_owned(),
                NetDataset {
                    dataset_id: DatasetId::from_url("s3://arbitrum-one"),
                    kind: "evm".to_string(),
                },
            ),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();

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
