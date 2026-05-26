#[allow(unused_imports)]
pub use progenitor_client::{ByteStream, ClientInfo, Error, ResponseValue};
#[allow(unused_imports)]
use progenitor_client::{ClientHooks, OperationInfo, RequestBuilderExt, encode_path};
/// Types used as operation parameters and responses.
#[allow(clippy::all)]
pub mod types {
    /// Error types.
    pub mod error {
        /// Error from a `TryFrom` or `FromStr` implementation.
        pub struct ConversionError(::std::borrow::Cow<'static, str>);
        impl ::std::error::Error for ConversionError {}
        impl ::std::fmt::Display for ConversionError {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> Result<(), ::std::fmt::Error> {
                ::std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl ::std::fmt::Debug for ConversionError {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> Result<(), ::std::fmt::Error> {
                ::std::fmt::Debug::fmt(&self.0, f)
            }
        }

        impl From<&'static str> for ConversionError {
            fn from(value: &'static str) -> Self {
                Self(value.into())
            }
        }

        impl From<String> for ConversionError {
            fn from(value: String) -> Self {
                Self(value.into())
            }
        }
    }

    ///`AvailableDatasetApiResponse`
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "allOf": [
    ///    {
    ///      "description": "Additional metadata fields, present only when
    /// requested via expand[]",
    ///      "type": "object"
    ///    },
    ///    {
    ///      "type": "object",
    ///      "required": [
    ///        "aliases",
    ///        "dataset",
    ///        "real_time"
    ///      ],
    ///      "properties": {
    ///        "aliases": {
    ///          "description": "Alternative names for the dataset.",
    ///          "type": "array",
    ///          "items": {
    ///            "type": "string"
    ///          }
    ///        },
    ///        "dataset": {
    ///          "description": "The default name used to reference this dataset
    /// (e.g., ethereum-mainnet).",
    ///          "type": "string"
    ///        },
    ///        "real_time": {
    ///          "description": "Indicates if the dataset has real-time data.",
    ///          "type": "boolean"
    ///        },
    ///        "start_block": {
    ///          "description": "The block number of the first known block.",
    ///          "type": [
    ///            "integer",
    ///            "null"
    ///          ],
    ///          "format": "int64",
    ///          "minimum": 0.0
    ///        }
    ///      }
    ///    }
    ///  ]
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct AvailableDatasetApiResponse {
        ///Alternative names for the dataset.
        pub aliases: ::std::vec::Vec<::std::string::String>,
        ///The default name used to reference this dataset (e.g.,
        /// ethereum-mainnet).
        pub dataset: ::std::string::String,
        ///Indicates if the dataset has real-time data.
        pub real_time: bool,
        ///The block number of the first known block.
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub start_block: ::std::option::Option<i64>,
    }

    ///Block head information
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "description": "Block head information",
    ///  "type": "object",
    ///  "required": [
    ///    "hash",
    ///    "number"
    ///  ],
    ///  "properties": {
    ///    "hash": {
    ///      "type": "string"
    ///    },
    ///    "number": {
    ///      "type": "integer",
    ///      "format": "int64",
    ///      "minimum": 0.0
    ///    }
    ///  }
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct BlockHead {
        pub hash: ::std::string::String,
        pub number: i64,
    }

    ///Block number response for timestamp query
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "description": "Block number response for timestamp query",
    ///  "type": "object",
    ///  "required": [
    ///    "block_number"
    ///  ],
    ///  "properties": {
    ///    "block_number": {
    ///      "type": "integer",
    ///      "format": "int64",
    ///      "minimum": 0.0
    ///    }
    ///  }
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct BlockNumberResponse {
        pub block_number: i64,
    }

    ///`CurrentEpoch`
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "type": "object",
    ///  "required": [
    ///    "duration_seconds",
    ///    "ended_at",
    ///    "number",
    ///    "started_at"
    ///  ],
    ///  "properties": {
    ///    "duration_seconds": {
    ///      "type": "integer",
    ///      "format": "int64",
    ///      "minimum": 0.0
    ///    },
    ///    "ended_at": {
    ///      "type": "string"
    ///    },
    ///    "number": {
    ///      "type": "integer",
    ///      "format": "int32",
    ///      "minimum": 0.0
    ///    },
    ///    "started_at": {
    ///      "type": "string"
    ///    }
    ///  }
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct CurrentEpoch {
        pub duration_seconds: i64,
        pub ended_at: ::std::string::String,
        pub number: i32,
        pub started_at: ::std::string::String,
    }

    ///`DatasetStateResponse`
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "description": "Dataset state response",
    ///  "allOf": [
    ///    {},
    ///    {
    ///      "type": "object"
    ///    }
    ///  ]
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    #[serde(transparent)]
    pub struct DatasetStateResponse(
        pub ::serde_json::Map<::std::string::String, ::serde_json::Value>,
    );
    impl ::std::ops::Deref for DatasetStateResponse {
        type Target = ::serde_json::Map<::std::string::String, ::serde_json::Value>;
        fn deref(&self) -> &::serde_json::Map<::std::string::String, ::serde_json::Value> {
            &self.0
        }
    }

    impl ::std::convert::From<DatasetStateResponse>
        for ::serde_json::Map<::std::string::String, ::serde_json::Value>
    {
        fn from(value: DatasetStateResponse) -> Self {
            value.0
        }
    }

    impl ::std::convert::From<::serde_json::Map<::std::string::String, ::serde_json::Value>>
        for DatasetStateResponse
    {
        fn from(value: ::serde_json::Map<::std::string::String, ::serde_json::Value>) -> Self {
            Self(value)
        }
    }

    ///Generic error response
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "description": "Generic error response",
    ///  "type": "object",
    ///  "required": [
    ///    "message"
    ///  ],
    ///  "properties": {
    ///    "message": {
    ///      "type": "string"
    ///    }
    ///  }
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct ErrorResponse {
        pub message: ::std::string::String,
    }

    ///Metadata query parameters
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "description": "Metadata query parameters",
    ///  "type": "object",
    ///  "properties": {
    ///    "expand[]": {
    ///      "type": "array",
    ///      "items": {
    ///        "type": "string"
    ///      }
    ///    }
    ///  }
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct MetadataQueryParams {
        #[serde(
            rename = "expand[]",
            default,
            skip_serializing_if = "::std::vec::Vec::is_empty"
        )]
        pub expand: ::std::vec::Vec<::std::string::String>,
    }

    impl ::std::default::Default for MetadataQueryParams {
        fn default() -> Self {
            Self {
                expand: Default::default(),
            }
        }
    }

    ///`NetworkClientStatus`
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "type": "object",
    ///  "required": [
    ///    "peer_id",
    ///    "status"
    ///  ],
    ///  "properties": {
    ///    "cu_per_epoch": {
    ///      "type": [
    ///        "string",
    ///        "null"
    ///      ]
    ///    },
    ///    "current_epoch": {
    ///      "oneOf": [
    ///        {
    ///          "type": "null"
    ///        },
    ///        {
    ///          "allOf": [
    ///            {
    ///              "$ref": "#/components/schemas/CurrentEpoch"
    ///            }
    ///          ]
    ///        }
    ///      ]
    ///    },
    ///    "operator": {
    ///      "type": [
    ///        "string",
    ///        "null"
    ///      ]
    ///    },
    ///    "peer_id": {
    ///      "type": "string"
    ///    },
    ///    "sqd_locked": {
    ///      "type": [
    ///        "string",
    ///        "null"
    ///      ]
    ///    },
    ///    "status": {
    ///      "$ref": "#/components/schemas/Status"
    ///    },
    ///    "workers": {
    ///      "oneOf": [
    ///        {
    ///          "type": "null"
    ///        },
    ///        {
    ///          "allOf": [
    ///            {
    ///              "$ref": "#/components/schemas/Workers"
    ///            }
    ///          ]
    ///        }
    ///      ]
    ///    }
    ///  }
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct NetworkClientStatus {
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub cu_per_epoch: ::std::option::Option<::std::string::String>,
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub current_epoch: ::std::option::Option<CurrentEpoch>,
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub operator: ::std::option::Option<::std::string::String>,
        pub peer_id: ::std::string::String,
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub sqd_locked: ::std::option::Option<::std::string::String>,
        pub status: Status,
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub workers: ::std::option::Option<Workers>,
    }

    ///Query execution request
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "description": "Query execution request",
    ///  "type": "object",
    ///  "required": [
    ///    "query"
    ///  ],
    ///  "properties": {
    ///    "query": {
    ///      "type": "string"
    ///    }
    ///  }
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct QueryRequest {
        pub query: ::std::string::String,
    }

    ///`Status`
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "type": "string",
    ///  "enum": [
    ///    "data_loading",
    ///    "registered",
    ///    "unregistered"
    ///  ]
    ///}
    /// ```
    /// </details>
    #[derive(
        :: serde :: Deserialize,
        :: serde :: Serialize,
        Clone,
        Copy,
        Debug,
        Eq,
        Hash,
        Ord,
        PartialEq,
        PartialOrd,
    )]
    pub enum Status {
        #[serde(rename = "data_loading")]
        DataLoading,
        #[serde(rename = "registered")]
        Registered,
        #[serde(rename = "unregistered")]
        Unregistered,
    }

    impl ::std::fmt::Display for Status {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
            match *self {
                Self::DataLoading => f.write_str("data_loading"),
                Self::Registered => f.write_str("registered"),
                Self::Unregistered => f.write_str("unregistered"),
            }
        }
    }

    impl ::std::str::FromStr for Status {
        type Err = self::error::ConversionError;
        fn from_str(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
            match value {
                "data_loading" => Ok(Self::DataLoading),
                "registered" => Ok(Self::Registered),
                "unregistered" => Ok(Self::Unregistered),
                _ => Err("invalid value".into()),
            }
        }
    }

    impl ::std::convert::TryFrom<&str> for Status {
        type Error = self::error::ConversionError;
        fn try_from(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
            value.parse()
        }
    }

    impl ::std::convert::TryFrom<&::std::string::String> for Status {
        type Error = self::error::ConversionError;
        fn try_from(
            value: &::std::string::String,
        ) -> ::std::result::Result<Self, self::error::ConversionError> {
            value.parse()
        }
    }

    impl ::std::convert::TryFrom<::std::string::String> for Status {
        type Error = self::error::ConversionError;
        fn try_from(
            value: ::std::string::String,
        ) -> ::std::result::Result<Self, self::error::ConversionError> {
            value.parse()
        }
    }

    ///Status response for the portal
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "description": "Status response for the portal",
    ///  "allOf": [
    ///    {
    ///      "$ref": "#/components/schemas/NetworkClientStatus"
    ///    },
    ///    {
    ///      "type": "object",
    ///      "required": [
    ///        "portal_version"
    ///      ],
    ///      "properties": {
    ///        "portal_version": {
    ///          "description": "Portal version string (semver)",
    ///          "type": "string"
    ///        }
    ///      }
    ///    }
    ///  ]
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct StatusResponse {
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub cu_per_epoch: ::std::option::Option<::std::string::String>,
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub current_epoch: ::std::option::Option<CurrentEpoch>,
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub operator: ::std::option::Option<::std::string::String>,
        pub peer_id: ::std::string::String,
        ///Portal version string (semver)
        pub portal_version: ::std::string::String,
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub sqd_locked: ::std::option::Option<::std::string::String>,
        pub status: Status,
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub workers: ::std::option::Option<Workers>,
    }

    ///Data query request body for stream endpoints.
    ///Note: the full data query object accepts additional chain-specific
    /// filter fields beyond those documented here. See the data query
    /// specification for details.
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "description": "Data query request body for stream endpoints.\nNote:
    /// the full data query object accepts additional chain-specific filter
    /// fields\nbeyond those documented here. See the data query specification
    /// for details.",
    ///  "type": "object",
    ///  "required": [
    ///    "fromBlock"
    ///  ],
    ///  "properties": {
    ///    "fromBlock": {
    ///      "description": "The number of the first block to fetch (required)",
    ///      "type": "integer",
    ///      "format": "int64",
    ///      "minimum": 0.0
    ///    },
    ///    "parentBlockHash": {
    ///      "description": "Expected hash of the parent of the first requested
    /// block (optional)",
    ///      "type": [
    ///        "string",
    ///        "null"
    ///      ]
    ///    },
    ///    "toBlock": {
    ///      "description": "The number of the last block to fetch, inclusive
    /// (optional)",
    ///      "type": [
    ///        "integer",
    ///        "null"
    ///      ],
    ///      "format": "int64",
    ///      "minimum": 0.0
    ///    }
    ///  }
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct StreamRequestBody {
        ///The number of the first block to fetch (required)
        #[serde(rename = "fromBlock")]
        pub from_block: i64,
        ///Expected hash of the parent of the first requested block (optional)
        #[serde(
            rename = "parentBlockHash",
            default,
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub parent_block_hash: ::std::option::Option<::std::string::String>,
        ///The number of the last block to fetch, inclusive (optional)
        #[serde(
            rename = "toBlock",
            default,
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub to_block: ::std::option::Option<i64>,
    }

    ///Worker information response
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "description": "Worker information response",
    ///  "type": "object",
    ///  "required": [
    ///    "peers"
    ///  ],
    ///  "properties": {
    ///    "peers": {
    ///      "type": "array",
    ///      "items": {
    ///        "type": "string"
    ///      }
    ///    }
    ///  }
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct WorkerInfo {
        pub peers: ::std::vec::Vec<::std::string::String>,
    }

    ///`Workers`
    ///
    /// <details><summary>JSON schema</summary>
    ///
    /// ```json
    ///{
    ///  "type": "object",
    ///  "required": [
    ///    "active_count"
    ///  ],
    ///  "properties": {
    ///    "active_count": {
    ///      "type": "integer",
    ///      "format": "int64",
    ///      "minimum": 0.0
    ///    },
    ///    "rate_limit_per_worker": {
    ///      "type": [
    ///        "string",
    ///        "null"
    ///      ]
    ///    }
    ///  }
    ///}
    /// ```
    /// </details>
    #[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
    pub struct Workers {
        pub active_count: i64,
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        pub rate_limit_per_worker: ::std::option::Option<::std::string::String>,
    }
}

#[derive(Clone, Debug)]
///Client for SQD Portal API
///
///API for querying and streaming blockchain data from the SQD network
///
///Version: 0.10.0
pub struct Client {
    pub(crate) baseurl: String,
    pub(crate) client: reqwest::Client,
}

impl Client {
    /// Create a new client.
    ///
    /// `baseurl` is the base URL provided to the internal
    /// `reqwest::Client`, and should include a scheme and hostname,
    /// as well as port and a path stem if applicable.
    pub fn new(baseurl: &str) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let client = {
            let dur = ::std::time::Duration::from_secs(15u64);
            reqwest::ClientBuilder::new()
                .connect_timeout(dur)
                .timeout(dur)
        };
        #[cfg(target_arch = "wasm32")]
        let client = reqwest::ClientBuilder::new();
        Self::new_with_client(baseurl, client.build().unwrap())
    }

    /// Construct a new client with an existing `reqwest::Client`,
    /// allowing more control over its configuration.
    ///
    /// `baseurl` is the base URL provided to the internal
    /// `reqwest::Client`, and should include a scheme and hostname,
    /// as well as port and a path stem if applicable.
    pub fn new_with_client(baseurl: &str, client: reqwest::Client) -> Self {
        Self {
            baseurl: baseurl.to_string(),
            client,
        }
    }
}

impl ClientInfo<()> for Client {
    fn api_version() -> &'static str {
        "0.10.0"
    }

    fn baseurl(&self) -> &str {
        self.baseurl.as_str()
    }

    fn client(&self) -> &reqwest::Client {
        &self.client
    }

    fn inner(&self) -> &() {
        &()
    }
}

impl ClientHooks<()> for &Client {}
#[allow(clippy::all)]
impl Client {
    ///List available datasets
    ///
    ///Lists the existing datasets as a JSON array. See /metadata endpoint for
    /// the field description.
    ///
    ///Sends a `GET` request to `/datasets`
    ///
    ///Arguments:
    /// - `expand`: Fields to expand in response
    pub async fn get_datasets<'a>(
        &'a self,
        expand: Option<&'a ::std::vec::Vec<::std::string::String>>,
    ) -> Result<ResponseValue<::std::vec::Vec<types::AvailableDatasetApiResponse>>, Error<()>> {
        let url = format!("{}/datasets", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .query(&progenitor_client::QueryParam::new("expand[]", &expand))
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_datasets",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Execute a data query via a specific worker (deprecated)
    ///
    ///Sends a data query to a specific worker in the network. This endpoint is
    /// deprecated.
    ///
    ///Sends a `POST` request to `/datasets/{dataset_id}/query/{worker_id}`
    ///
    ///Arguments:
    /// - `dataset_id`: Dataset ID
    /// - `worker_id`: Worker ID
    /// - `body`
    pub async fn execute_query<'a>(
        &'a self,
        dataset_id: &'a str,
        worker_id: &'a str,
        body: &'a ::serde_json::Value,
    ) -> Result<ResponseValue<ByteStream>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/query/{}",
            self.baseurl,
            encode_path(&dataset_id.to_string()),
            encode_path(&worker_id.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .post(url)
            .json(&body)
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "execute_query",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::stream(response)),
            400u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            503u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Gets the archival head corresponding to the /archival-stream behaviour
    ///
    ///The successful response is either "null" or a JSON object with "number"
    /// and "hash" fields. Get the archival head block
    ///
    ///Returns the block number and hash of the current archival data head
    ///
    ///Sends a `GET` request to `/datasets/{dataset}/archival-head`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    pub async fn get_archival_head<'a>(
        &'a self,
        dataset: &'a str,
    ) -> Result<ResponseValue<::std::option::Option<types::BlockHead>>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/archival-head",
            self.baseurl,
            encode_path(&dataset.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_archival_head",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Stream archival data with request restrictions applied
    ///
    ///Streams blockchain data from the archival layer with rate limits and
    /// restrictions
    ///
    ///Sends a `POST` request to `/datasets/{dataset}/archival-stream`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    /// - `body`
    pub async fn run_archival_stream_restricted<'a>(
        &'a self,
        dataset: &'a str,
        body: &'a types::StreamRequestBody,
    ) -> Result<ResponseValue<()>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/archival-stream",
            self.baseurl,
            encode_path(&dataset.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .post(url)
            .json(&body)
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "run_archival_stream_restricted",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::empty(response)),
            204u16 => Ok(ResponseValue::empty(response)),
            400u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            429u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            500u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            503u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Stream archival data without request restrictions (debug)
    ///
    ///Streams blockchain data from the archival layer using request parameters
    /// as provided
    ///
    ///Sends a `POST` request to `/datasets/{dataset}/archival-stream/debug`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    /// - `body`
    pub async fn run_archival_stream<'a>(
        &'a self,
        dataset: &'a str,
        body: &'a types::StreamRequestBody,
    ) -> Result<ResponseValue<()>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/archival-stream/debug",
            self.baseurl,
            encode_path(&dataset.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .post(url)
            .json(&body)
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "run_archival_stream",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::empty(response)),
            204u16 => Ok(ResponseValue::empty(response)),
            400u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            429u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            500u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            503u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Get the height of a dataset via archival-stream (deprecated)
    ///
    ///Same as /datasets/{dataset}/height. Kept for backward compatibility.
    ///
    ///Sends a `GET` request to `/datasets/{dataset}/archival-stream/height`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    pub async fn get_archival_stream_height<'a>(
        &'a self,
        dataset: &'a str,
    ) -> Result<ResponseValue<ByteStream>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/archival-stream/height",
            self.baseurl,
            encode_path(&dataset.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self.client.get(url).headers(header_map).build()?;
        let info = OperationInfo {
            operation_id: "get_archival_stream_height",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::stream(response)),
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Gets the finalized head corresponding to the /finalized-stream behaviour
    ///
    ///The successful response is either "null" or a JSON object with "number"
    /// and "hash" fields.
    ///
    ///Sends a `GET` request to `/datasets/{dataset}/finalized-head`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    pub async fn get_finalized_head<'a>(
        &'a self,
        dataset: &'a str,
    ) -> Result<ResponseValue<::std::option::Option<types::BlockHead>>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/finalized-head",
            self.baseurl,
            encode_path(&dataset.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_finalized_head",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Stream finalized blockchain data
    ///
    ///Streams only finalized blocks from archival and hotblocks sources
    ///
    ///Sends a `POST` request to `/datasets/{dataset}/finalized-stream`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    /// - `body`
    pub async fn run_finalized_stream<'a>(
        &'a self,
        dataset: &'a str,
        body: &'a types::StreamRequestBody,
    ) -> Result<ResponseValue<()>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/finalized-stream",
            self.baseurl,
            encode_path(&dataset.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .post(url)
            .json(&body)
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "run_finalized_stream",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::empty(response)),
            204u16 => Ok(ResponseValue::empty(response)),
            400u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            429u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            500u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            503u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Get the height of a dataset via finalized-stream (deprecated)
    ///
    ///Same as /datasets/{dataset}/height. Kept for backward compatibility.
    ///
    ///Sends a `GET` request to `/datasets/{dataset}/finalized-stream/height`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    pub async fn get_finalized_stream_height<'a>(
        &'a self,
        dataset: &'a str,
    ) -> Result<ResponseValue<ByteStream>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/finalized-stream/height",
            self.baseurl,
            encode_path(&dataset.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self.client.get(url).headers(header_map).build()?;
        let info = OperationInfo {
            operation_id: "get_finalized_stream_height",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::stream(response)),
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Gets the head corresponding to the /stream behaviour
    ///
    ///If the dataset has a real-time data source, its head is returned.
    ///Otherwise, archival head is returned.
    ///
    ///The successful response is either "null" or a JSON object with "number"
    /// and "hash" fields.
    ///
    ///Sends a `GET` request to `/datasets/{dataset}/head`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    pub async fn get_head<'a>(
        &'a self,
        dataset: &'a str,
    ) -> Result<ResponseValue<::std::option::Option<types::BlockHead>>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/head",
            self.baseurl,
            encode_path(&dataset.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_head",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Get metadata for a specific dataset
    ///
    ///Returns metadata including chain info, first block, and optional
    /// expanded fields
    ///
    ///Sends a `GET` request to `/datasets/{dataset}/metadata`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    /// - `expand`: Fields to expand in response
    pub async fn get_dataset_metadata<'a>(
        &'a self,
        dataset: &'a str,
        expand: Option<&'a ::std::vec::Vec<::std::string::String>>,
    ) -> Result<ResponseValue<types::AvailableDatasetApiResponse>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/metadata",
            self.baseurl,
            encode_path(&dataset.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .query(&progenitor_client::QueryParam::new("expand[]", &expand))
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_dataset_metadata",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Get the state of a specific dataset
    ///
    ///Returns state information for the requested dataset
    ///
    ///Sends a `GET` request to `/datasets/{dataset}/state`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    pub async fn get_dataset_state<'a>(
        &'a self,
        dataset: &'a str,
    ) -> Result<ResponseValue<::serde_json::Value>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/state",
            self.baseurl,
            encode_path(&dataset.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_dataset_state",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Stream real-time blockchain data
    ///
    ///Streams blockchain data combining archival and real-time hotblocks
    /// sources
    ///
    ///Sends a `POST` request to `/datasets/{dataset}/stream`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    /// - `body`
    pub async fn run_stream<'a>(
        &'a self,
        dataset: &'a str,
        body: &'a types::StreamRequestBody,
    ) -> Result<ResponseValue<()>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/stream",
            self.baseurl,
            encode_path(&dataset.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .post(url)
            .json(&body)
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "run_stream",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::empty(response)),
            204u16 => Ok(ResponseValue::empty(response)),
            400u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            409u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            429u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            500u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            503u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Get the block number for a given timestamp
    ///
    ///Returns the first block whose timestamp is greater than or equal to the
    /// given timestamp
    ///
    ///Sends a `GET` request to
    /// `/datasets/{dataset}/timestamps/{timestamp}/block`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    /// - `timestamp`: Timestamp in seconds
    pub async fn get_blocknumber_by_timestamp<'a>(
        &'a self,
        dataset: &'a str,
        timestamp: i64,
    ) -> Result<ResponseValue<types::BlockNumberResponse>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/timestamps/{}/block",
            self.baseurl,
            encode_path(&dataset.to_string()),
            encode_path(&timestamp.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_blocknumber_by_timestamp",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            500u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            503u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Get debug information for a specific block
    ///
    ///Returns worker information for the given block
    ///
    ///Sends a `GET` request to `/datasets/{dataset}/{block}/debug`
    ///
    ///Arguments:
    /// - `dataset`: Dataset name
    /// - `block`: Block number
    pub async fn get_debug_block<'a>(
        &'a self,
        dataset: &'a str,
        block: i64,
    ) -> Result<ResponseValue<::serde_json::Value>, Error<()>> {
        let url = format!(
            "{}/datasets/{}/{}/debug",
            self.baseurl,
            encode_path(&dataset.to_string()),
            encode_path(&block.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_debug_block",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            404u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Get information about all workers
    ///
    ///Returns information about all available workers in the network
    ///
    ///Sends a `GET` request to `/debug/workers`
    pub async fn get_all_workers<'a>(
        &'a self,
    ) -> Result<ResponseValue<::serde_json::Value>, Error<()>> {
        let url = format!("{}/debug/workers", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_all_workers",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Get Prometheus metrics
    ///
    ///Returns portal metrics in OpenMetrics format
    ///
    ///Sends a `GET` request to `/metrics`
    pub async fn get_metrics<'a>(&'a self) -> Result<ResponseValue<()>, Error<()>> {
        let url = format!("{}/metrics", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self.client.get(url).headers(header_map).build()?;
        let info = OperationInfo {
            operation_id: "get_metrics",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::empty(response)),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Check if the portal is ready to serve requests
    ///
    ///Returns 200 if the portal is ready, 503 if not
    ///
    ///Sends a `GET` request to `/ready`
    pub async fn get_readiness<'a>(&'a self) -> Result<ResponseValue<()>, Error<()>> {
        let url = format!("{}/ready", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self.client.get(url).headers(header_map).build()?;
        let info = OperationInfo {
            operation_id: "get_readiness",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::empty(response)),
            503u16 => Err(Error::ErrorResponse(ResponseValue::empty(response))),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    ///Get the current status of the portal
    ///
    ///Responds with a JSON with human-readable information about the portal's
    /// state. The exact format may change.
    ///
    ///Sends a `GET` request to `/status`
    pub async fn get_status<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::StatusResponse>, Error<()>> {
        let url = format!("{}/status", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map.append(
            ::reqwest::header::HeaderName::from_static("api-version"),
            ::reqwest::header::HeaderValue::from_static(Self::api_version()),
        );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_status",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
}

/// Items consumers will typically use such as the Client.
pub mod prelude {
    #[allow(unused_imports)]
    pub use super::Client;
}
