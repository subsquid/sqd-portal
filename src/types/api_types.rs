use crate::datasets::DatasetConfig;

use super::DatasetId;

#[derive(serde::Serialize)]
pub(crate) struct AvailableDatasetApiResponse {
    pub dataset: String,
    pub aliases: Vec<String>,
    pub real_time: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum DatasetRef {
    #[serde(rename = "dataset_name")]
    Name(String),
    #[serde(rename = "dataset_id")]
    Id(DatasetId),
}

impl From<DatasetConfig> for AvailableDatasetApiResponse {
    fn from(metadata: DatasetConfig) -> Self {
        Self {
            dataset: metadata.default_name,
            aliases: metadata.aliases,
            real_time: metadata.hotblocks.is_some(),
        }
    }
}
