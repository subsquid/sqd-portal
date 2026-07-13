use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use serde::{Deserialize, Serialize};

use super::concurrency::ConcurrencyPermit;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Credential {
    Key {
        key_id: String,
        secret_sha256: String,
    },
    None {
        ip_bucket: String,
    },
    Internal {
        service_token_id: String,
    },
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Key { key_id, .. } => f
                .debug_struct("Key")
                .field("key_id", key_id)
                .field("secret_sha256", &"<redacted>")
                .finish(),
            Self::None { ip_bucket } => f
                .debug_struct("None")
                .field("ip_bucket", ip_bucket)
                .finish(),
            Self::Internal { service_token_id } => f
                .debug_struct("Internal")
                .field("service_token_id", service_token_id)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Endpoint {
    Stream,
    FinalizedStream,
    ArchivalStream,
    SqlQuery,
    TsLookup,
    LegacyQuery,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryDescriptor {
    pub requires_traces: bool,
    pub requires_statediffs: bool,
    pub first_block: Option<u64>,
    pub chain_kind: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizeRequest {
    pub credential: Credential,
    pub dataset: String,
    pub endpoint: Endpoint,
    pub query: QueryDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub account_id: String,
    pub api_key_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GrantedLimits {
    /// None means no response-size cap; Some(0) is a literal zero-byte cap.
    pub max_response_bytes: Option<u64>,
    /// None means no throughput pacing; Some(0) fails open as no bucket and emits zero-limit telemetry.
    pub throughput_bytes_per_sec: Option<u64>,
    /// None defaults to the throughput rate; Some(0) is coerced to the minimum bucket burst.
    pub burst_bytes: Option<u64>,
    /// None means no chunk-count cap; Some(0) is a literal zero-chunk cap.
    pub max_chunks: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum OnExceed {
    Reject,
    Throttle { floor_bytes_per_sec: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Granted {
    pub principal: Principal,
    #[serde(default)]
    pub tally_account_id: Option<String>,
    #[serde(skip)]
    pub entitled_chains: Option<HashSet<String>>,
    #[serde(skip)]
    pub entitled_traces: Option<HashSet<String>>,
    pub limits: GrantedLimits,
    pub on_exceed: OnExceed,
    pub quota_version: u64,
    pub quota_remaining_bytes: Option<i64>,
    #[serde(skip)]
    pub concurrency_permit: Option<ConcurrencyPermit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rejected {
    pub reason: String,
    pub http_status: u16,
    pub message: String,
    pub retry_after_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authorization {
    Granted(Granted),
    Rejected(Rejected),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSource {
    Network,
    RealTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageStatus {
    Completed,
    ClientDisconnect,
    CutQuota,
    CutMaxBytes,
    CutSuspended,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamUsageEvent {
    pub event_id: String,
    pub request_id: String,
    pub account_id: String,
    pub api_key_id: Option<String>,
    pub dataset: String,
    pub endpoint: Endpoint,
    pub data_source: DataSource,
    pub logical_bytes: u64,
    pub wire_bytes: u64,
    // Analytics-only. Current stream endpoints meter encoded response chunks,
    // not decoded block records, so this remains 0 unless an endpoint has an
    // exact count and calls MeterHandle::add_blocks.
    pub blocks: u64,
    pub chunks: u64,
    pub started_at: f64,
    pub duration_ms: u64,
    pub status: UsageStatus,
    pub pod: String,
    pub quota_version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyStatus {
    Active,
    Suspended,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SnapshotLimits {
    /// None means no throughput pacing; Some(0) fails open as no bucket and emits zero-limit telemetry.
    pub throughput_bytes_per_sec: Option<u64>,
    /// None defaults to the throughput rate; Some(0) is coerced to the minimum bucket burst.
    pub burst_bytes: Option<u64>,
    /// None means no response-size cap; Some(0) is a literal zero-byte cap.
    pub max_response_bytes: Option<u64>,
    /// None means no concurrency check; Some(0) fails open to the per-pod guardrail permit and emits zero-limit telemetry.
    pub concurrency: Option<u64>,
    #[serde(default)]
    /// None means no chunk-count cap; Some(0) is a literal zero-chunk cap.
    pub max_chunks: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Entitlements {
    #[serde(default)]
    pub chains: Vec<String>,
    #[serde(default)]
    pub traces: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quota {
    pub remaining_bytes: Option<i64>,
    pub period_end: Option<u64>,
    pub version: u64,
    pub on_exceed: OnExceed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeySnapshot {
    pub key_id: String,
    #[serde(default)]
    pub secret_sha256: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    pub status: KeyStatus,
    #[serde(default)]
    pub expires_at: Option<u64>,
    #[serde(default)]
    pub limits: Option<SnapshotLimits>,
    #[serde(default)]
    pub entitlements: Option<Entitlements>,
    #[serde(default)]
    pub quota: Option<Quota>,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicLimits {
    /// None means no throughput pacing; Some(0) fails open as no bucket and emits zero-limit telemetry.
    pub throughput_bytes_per_sec: Option<u64>,
    /// None defaults to the throughput rate; Some(0) is coerced to the minimum bucket burst.
    pub burst_bytes: Option<u64>,
    /// None means no response-size cap; Some(0) is a literal zero-byte cap.
    pub max_response_bytes: Option<u64>,
    /// None means no concurrency check; Some(0) fails open to the per-pod guardrail permit and emits zero-limit telemetry.
    pub concurrency: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicQuota {
    pub volume_bytes: Option<u64>,
    pub window_secs: u64,
    pub on_exceed: OnExceed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicDefaults {
    pub limits: PublicLimits,
    #[serde(default)]
    pub quota: Option<PublicQuota>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Defaults {
    #[serde(rename = "public_tier")]
    pub public: PublicDefaults,
    #[serde(default)]
    pub messages: HashMap<String, String>,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultsRecord {
    pub key_id: String,
    #[serde(rename = "public_tier")]
    pub public: PublicDefaults,
    #[serde(default)]
    pub messages: HashMap<String, String>,
    pub seq: u64,
}

impl From<DefaultsRecord> for Defaults {
    fn from(record: DefaultsRecord) -> Self {
        Self {
            public: record.public,
            messages: record.messages,
            seq: record.seq,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SnapshotRecord {
    Defaults(DefaultsRecord),
    Key(KeySnapshot),
}
