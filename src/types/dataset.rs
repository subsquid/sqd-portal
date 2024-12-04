use core::str;
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use crate::utils::intern_string;

use super::BlockNumber;
use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};

pub type BlockRange = std::ops::RangeInclusive<BlockNumber>;

/// Base64 encoded URL
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct DatasetId(Arc<str>);

impl Display for DatasetId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl DatasetId {
    pub fn from_url(url: impl AsRef<str>) -> Self {
        Self(intern_string(url.as_ref()))
    }

    pub fn to_url(&self) -> &str {
        &self.0
    }

    pub fn to_base64(&self) -> String {
        BASE64_URL_SAFE_NO_PAD.encode(&*self.0)
    }

    pub fn from_base64(base64: impl AsRef<str>) -> anyhow::Result<Self> {
        let bytes = BASE64_URL_SAFE_NO_PAD.decode(base64.as_ref())?;
        Ok(Self(intern_string(str::from_utf8(&bytes)?)))
    }
}
