use crate::datasets::DatasetMetadata;

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

impl<'l> From<DatasetMetadata<'l>> for AvailableDatasetApiResponse {
    fn from(metadata: DatasetMetadata) -> Self {
        Self {
            dataset: metadata.default_name.into_owned(),
            aliases: metadata.aliases.into_owned(),
            real_time: metadata.real_time,
        }
    }
}
