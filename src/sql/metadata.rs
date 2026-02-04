use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::ops::Range;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::datasets::DatasetConfig;
use crate::types::DatasetId;

// We should add schemas to the NetworkClient;
// I keep them here for the moment not to pollute the code
// with SQL-specific (and experimental) stuff.
pub static SCHEMAS: Lazy<HashMap<String, Schema>> = Lazy::new(schemas_or_die);

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub datasets: Vec<Dataset>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub name: String,
    pub bucket_name: String,
    pub schema: Schema,
    pub stats: Stats,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    pub name: String,
    pub tables: Vec<Table>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Table {
    pub name: String,
    pub schema: TableSchema,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSchema {
    pub fields: Vec<Field>,
    pub primary_key: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub ltype: LogicalType,
    pub nullable: bool,
}

// Types are still simplistic. A bit more machinery is needed,
// in particular for complex types.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LogicalType {
    Bool,
    Blob,
    Integer,
    Real,
    Timestamp,
    Varchar,
}

// This is dataset-oriented
// but should be generic in the future.
#[derive(Debug, Serialize, Deserialize)]
pub struct Stats {
    pub num_blocks: u64,
    pub tx_per_block: u64,
    pub logs_per_block: u64,
    pub traces_per_block: u64,
    pub diffs_per_block: u64,
}

// stats are still faked
impl Default for Stats {
    fn default() -> Self {
        Stats {
            num_blocks: 1000000,
            tx_per_block: 50,
            logs_per_block: 150,
            traces_per_block: 100,
            diffs_per_block: 100,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SchemaErr {
    #[error("JSON parse error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Schema not found: {0}")]
    SchemaNotFound(String),
}

pub fn compute_stats(dataset: &DatasetId, table: &str, blocks: &[Range<u64>]) -> u64 {
    let stats = get_dataset_stats(dataset).unwrap_or_default();

    let total_blocks = blocks.iter().fold(0, |t, b| {
        if b.start < b.end {
            t + (b.end - b.start)
        } else {
            t
        }
    });

    let total_blocks = if total_blocks == 0 {
        stats.num_blocks
    } else {
        total_blocks
    };

    std::cmp::min(
        u32::MAX as u64,
        total_blocks
            * match table_type(table) {
                TableType::Block => 1,
                TableType::Tx => stats.tx_per_block,
                TableType::Log => stats.logs_per_block,
                TableType::Trace => stats.traces_per_block,
                TableType::Diff => stats.diffs_per_block,
                TableType::Unknown => 0,
            },
    )
}

// Again, this is still dataset-oriented,
// not generic schema.
#[derive(Debug, Clone)]
pub enum TableType {
    Block,
    Tx,
    Log,
    Trace,
    Diff,
    Unknown,
}

pub fn table_type(name: &str) -> TableType {
    if name.to_lowercase().contains("block") {
        return TableType::Block;
    }
    if name.to_lowercase().contains("transaction") {
        return TableType::Tx;
    }
    if name.to_lowercase().contains("log") {
        return TableType::Log;
    }
    if name.to_lowercase().contains("trace") {
        return TableType::Trace;
    }
    if name.to_lowercase().contains("diff") {
        return TableType::Diff;
    }
    TableType::Unknown
}

pub fn get_dataset_stats(_dataset: &DatasetId) -> Option<Stats> {
    None
}

// We should use "bucket_name":
// datasets with schemas and stats should be kept as part of the program state,
// so that we can retrieve them at any time.
pub fn schema_name_to_dataset_id(schema: &str) -> DatasetId {
    let s = format!("s3://{}", schema.replace("_", "-"));
    DatasetId::from_url(&s)
}

pub fn bucket_to_schema_name(bucket: &str) -> String {
    bucket.replace("-", "_").to_string()
}

pub fn map_datasets_on_schemas(datasets: &[DatasetConfig]) -> Result<Metadata, SchemaErr> {
    let mut ds = Vec::new();
    for d in datasets {
        let kind = if d.kind == "hyperliquidFills" {
            "hyperliquid"
        } else {
            &d.kind
        };
        let schema = SCHEMAS
            .get(kind)
            .ok_or(SchemaErr::SchemaNotFound(d.kind.to_string()))?;
        ds.push(Dataset {
            name: bucket_to_schema_name(&d.default_name),
            bucket_name: d.default_name.to_string(),
            schema: schema.clone(),
            stats: Stats::default(),
        });
    }

    Ok(Metadata { datasets: ds })
}

// This will not be one document in the future,
// but a repository from where relevant parts are retrieved
// or subscribed (in case of stats).
fn schemas_or_die() -> HashMap<String, Schema> {
    if let Ok(path) = std::env::var("SCHEMAS") {
        read_schemas(&path)
            .unwrap_or_else(|e| panic!("cannot read schemas from {}: {}", path, e))
    } else {
        // Fallback to embedded default
        read_schemas_from_str(DEFAULT_SCHEMAS_JSON)
            .expect("cannot read embedded schemas")
    }
}

fn read_schemas_from_str(path: &str) -> Result<HashMap<String, Schema>, SchemaErr> {
    let schemas: Vec<Schema> = serde_json::from_str(path)?;
    let mut m = HashMap::new();
    for schema in &schemas {
        m.insert(schema.name.to_string(), schema.clone());
    }
    Ok(m)
}

}

fn read_schemas(path: &str) -> Result<HashMap<String, Schema>, SchemaErr> {
    let f = File::open(path)?;
    let rd = BufReader::new(f);
    let schemas: Vec<Schema> = serde_json::from_reader(rd)?;
    let mut m = HashMap::new();
    for schema in &schemas {
        m.insert(schema.name.to_string(), schema.clone());
    }
    Ok(m)
}
