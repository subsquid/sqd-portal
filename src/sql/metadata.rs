use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

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
// but should be generic in the future
#[derive(Debug, Serialize, Deserialize)]
pub struct Stats {
    pub num_blocks: u64,
    pub tx_per_block: u64,
    pub logs_per_block: u64,
    pub traces_per_block: u64,
    pub diffs_per_block: u64,
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

// We should use "bucket_name" -
// The datasets and their schemas should be kept as part of the program state,
// so that we can retrieve data from them at any time.
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
            stats: Stats {
                num_blocks: 1000000,
                tx_per_block: 150,
                logs_per_block: 250,
                traces_per_block: 250,
                diffs_per_block: 100,
            },
        });
    }

    Ok(Metadata { datasets: ds })
}

fn schemas_or_die() -> HashMap<String, Schema> {
    let path = std::env::var("SCHEMAS").unwrap_or("./schemas.json".to_string());
    read_schemas(&path).expect("cannot read schemas")
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
