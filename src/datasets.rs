use crate::cli::DatasetConfig;
use crate::types::DatasetId;

pub struct Datasets {
    available: Vec<DatasetConfig>,
}

impl Datasets {
    pub fn new(available: Vec<DatasetConfig>) -> Self {
        Self { available }
    }

    pub fn available(&self) -> Vec<DatasetConfig> {
        self.available.clone()
    }

    pub fn find_dataset(&self, slug: &str) -> Option<&DatasetConfig> {
        self.available.iter().find(|d| {
            if d.slug == slug {
                return true;
            }

            if let Some(ref aliases) = d.aliases {
                return aliases.contains(&slug.to_string());
            }

            false
        })
    }

    pub fn dataset_id(&self, slug: &str) -> Option<DatasetId> {
        self.find_dataset(slug)
            .and_then(|dataset| dataset.network_dataset_id())
    }

    pub fn dataset_ids(&self) -> impl Iterator<Item = DatasetId> + '_ {
        self.available.iter().filter_map(|d| d.network_dataset_id())
    }
}
