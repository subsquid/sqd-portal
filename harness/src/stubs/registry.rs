//! DC-3 stub: the dataset-name mapping and metadata documents.

use axum::{extract::State, routing::get, Router};

use super::Ledger;
use crate::world::ToyWorld;

#[derive(Clone)]
struct RegistryState {
    datasets_yml: String,
    metadata_yml: String,
    ledger: Ledger,
}

pub fn datasets_yml(world: &ToyWorld) -> String {
    let mut out = String::from("sqd-network-datasets:\n");
    for ds in &world.datasets {
        if let Some(id) = &ds.network_id {
            out.push_str(&format!("  - id: {}\n    name: {}\n", id, ds.name));
        }
    }
    out
}

pub fn metadata_yml(_world: &ToyWorld) -> String {
    "datasets: {}\n".to_string()
}

pub async fn start(port: u16, world: &ToyWorld) -> anyhow::Result<Ledger> {
    let ledger = Ledger::default();
    let state = RegistryState {
        datasets_yml: datasets_yml(world),
        metadata_yml: metadata_yml(world),
        ledger: ledger.clone(),
    };
    let app = Router::new()
        .route(
            "/datasets.yml",
            get(|State(s): State<RegistryState>| async move {
                s.ledger.push("datasets");
                ([("content-type", "text/yaml")], s.datasets_yml.clone())
            }),
        )
        .route(
            "/metadata.yml",
            get(|State(s): State<RegistryState>| async move {
                s.ledger.push("metadata");
                ([("content-type", "text/yaml")], s.metadata_yml.clone())
            }),
        )
        .with_state(state);
    super::serve(app, port).await?;
    Ok(ledger)
}
