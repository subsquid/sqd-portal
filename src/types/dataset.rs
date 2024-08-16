use std::fmt::{Display, Formatter};

use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use serde::Deserialize;

/// Base64 encoded URL
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
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
}
