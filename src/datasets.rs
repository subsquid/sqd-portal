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
    pub metadata: serde_json::Value,
}

type DatasetsMapping = BTreeMap<String, DatasetId>;

impl Datasets {
    pub async fn load(config: &Config) -> anyhow::Result<Self> {
        let (mapping, metadata) = tokio::join!(
            load_mapping(&config.sqd_network.datasets_url),
            load_metadata(config.sqd_network.metadata_url.as_deref()),
        );
        Self::parse(config, mapping?, metadata?)
    }

    fn parse(
        config: &Config,
        mapping: DatasetsMapping,
        mut metadata: MetadataMapping,
    ) -> anyhow::Result<Self> {
        let mut datasets: Vec<DatasetConfig> = Vec::new();
        let mut alias_to_index = BTreeMap::new();
        let mut id_to_default_name = BTreeMap::new();
        for (default_name, ds) in config.datasets.clone() {
            let network_id = match &ds.sqd_network {
                Some(DatasetRef::Id(id)) => Some(id.clone()),
                Some(DatasetRef::Name(name)) => mapping.get(name).cloned(),
                None => mapping.get(&default_name).cloned(),
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

            let ds_metadata = metadata
                .remove(&default_name)
                .unwrap_or(serde_json::Value::Null);
            let metadata_kind = ds_metadata
                .as_object()
                .and_then(|o| o.get("kind"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let kind = ds
                .kind
                .or_else(|| ds.real_time.as_ref().and_then(|r| r.kind.clone()))
                .or(metadata_kind)
                .ok_or_else(|| anyhow::anyhow!("Unknown kind for dataset {default_name}"))?;

            datasets.push(DatasetConfig {
                default_name,
                aliases: ds.aliases,
                network_id,
                hotblocks: ds.real_time,
                kind,
                metadata: ds_metadata,
            });
        }

        if let ServeMode::All = config.sqd_network.serve {
            // Add all datasets from the mapping that are not already present
            for (name, dataset_id) in mapping {
                if let Entry::Vacant(entry) = alias_to_index.entry(name.clone()) {
                    entry.insert(datasets.len());

                    let ds_metadata = metadata.remove(&name).unwrap_or(serde_json::Value::Null);
                    let kind = ds_metadata
                        .as_object()
                        .and_then(|o| o.get("kind"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .ok_or_else(|| anyhow::anyhow!("Unknown kind for dataset {name}"))?;

                    datasets.push(DatasetConfig {
                        default_name: name.clone(),
                        aliases: Vec::new(),
                        network_id: Some(dataset_id.clone()),
                        hotblocks: None,
                        kind,
                        metadata: ds_metadata,
                    });

                    if let Entry::Vacant(entry) = id_to_default_name.entry(dataset_id) {
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

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "kebab-case")]
struct NetworkDatasets {
    sqd_network_datasets: Vec<NetworkDataset>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct NetworkDataset {
    id: String,
    name: String,
}

async fn load_yaml<T: serde::de::DeserializeOwned>(url: &str) -> anyhow::Result<T> {
    if let Some(path) = url.strip_prefix("file://") {
        tracing::debug!("Loading local file from {}", path);
        let file =
            File::open(path).with_context(|| format!("failed to open file {path}"))?;
        let reader = BufReader::new(file);
        serde_yaml::from_reader(reader).with_context(|| format!("failed to parse {path}"))
    } else {
        tracing::debug!("Fetching remote file from {}", url);
        let response = reqwest::get(url).await?;
        let text = response.text().await?;
        serde_yaml::from_str(&text).with_context(|| format!("failed to parse {url}"))
    }
}

async fn load_mapping(url: &str) -> anyhow::Result<DatasetsMapping> {
    let file: NetworkDatasets = load_yaml(url).await?;
    let mut datasets = BTreeMap::new();
    for dataset in file.sqd_network_datasets {
        datasets.insert(dataset.name.clone(), DatasetId::from_url(&dataset.id));
    }
    tracing::debug!("Datasets file loaded, {} datasets found", datasets.len());
    Ok(datasets)
}

type MetadataMapping = BTreeMap<String, serde_json::Value>;

#[derive(Deserialize)]
struct MetadataFile {
    datasets: MetadataMapping,
}

async fn load_metadata(url: Option<&str>) -> anyhow::Result<MetadataMapping> {
    let Some(url) = url else {
        return Ok(BTreeMap::new());
    };
    let file: MetadataFile = load_yaml(url).await?;
    tracing::debug!("Metadata loaded, {} datasets found", file.datasets.len());
    Ok(file.datasets)
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
                && self.kind == other.kind
                && self.metadata == other.metadata
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
                        "url": "http://localhost:8080",
                        "kind": "not_used"
                    }
                },
                "local": {
                    "real_time": {
                        "url": "http://localhost:8081",
                        "kind": "solana",
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
        .collect::<BTreeMap<_, _>>();

        let metadata: MetadataMapping = serde_json::from_value(serde_json::json!({
            "arbitrum-one": { "kind": "evm", "display_name": "Arbitrum One" }
        }))
        .unwrap();

        let datasets = Datasets::parse(&config, mapping, metadata).unwrap();

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
                metadata: serde_json::Value::Null,
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
                metadata: serde_json::Value::Null,
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
                metadata: serde_json::Value::Null,
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
                metadata: serde_json::Value::Null,
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
                metadata: serde_json::Value::Null,
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
                metadata: serde_json::Value::Null,
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
                DatasetId::from_url("s3://ethereum-mainnet-1"),
            ),
            (
                "arbitrum-one".to_owned(),
                DatasetId::from_url("s3://arbitrum-one"),
            ),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();

        let datasets = Datasets::parse(&config, mapping, BTreeMap::new()).unwrap();

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
