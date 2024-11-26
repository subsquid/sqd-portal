use std::fmt::{Display, Formatter};

use super::BlockNumber;
use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};

pub type BlockRange = std::ops::RangeInclusive<BlockNumber>;

/// Base64 encoded URL
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct DatasetId(pub String);

impl Display for DatasetId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl DatasetId {
    pub fn from_url(url: impl AsRef<[u8]>) -> Self {
        Self(BASE64_URL_SAFE_NO_PAD.encode(url))
    }

    pub fn to_url(&self) -> anyhow::Result<String> {
        let bytes = BASE64_URL_SAFE_NO_PAD.decode(&self.0)?;
        Ok(String::from_utf8(bytes)?)
    }
}
