//! DC-2 stub: serves the network-state document and the assignment artifact.

use axum::{extract::State, response::IntoResponse, routing::get, Router};
use serde_json::json;

use super::Ledger;
use crate::artifact::AssignmentType;

#[derive(Clone)]
struct PublisherState {
    network_state: String,
    artifact_gz: Vec<u8>,
    portal_artifact_gz: Vec<u8>,
    ledger: Ledger,
}

/// Publishes both artifacts, which is the state the scheduler holds throughout the migration —
/// so which one the portal reads is decided by its config or by `assignment_type`, never by
/// what is on offer.
pub fn network_state_json(port: u16, assignment_id: &str, effective_from: u64) -> String {
    network_state_json_publishing(
        port,
        assignment_id,
        effective_from,
        &[AssignmentType::Legacy, AssignmentType::Split],
    )
}

/// The same document carrying only `published`, and naming the first of them as its
/// `assignment_type` — what an unpinned portal follows. Every descriptor is optional upstream
/// because migration walks the state through legacy-only, both, then split-only, so a portal
/// has to cope with the artifact in force simply not being there.
pub fn network_state_json_publishing(
    port: u16,
    assignment_id: &str,
    effective_from: u64,
    published: &[AssignmentType],
) -> String {
    let url = |file| format!("http://127.0.0.1:{port}/{file}");
    let named = published.first().copied().unwrap_or(AssignmentType::Legacy);
    let mut state = json!({
        "network": "tethys",
        "assignment_type": named,
    });
    for source in published {
        match source {
            AssignmentType::Legacy => {
                state["assignment"] = json!({
                    "id": assignment_id,
                    "effective_from": effective_from,
                    "fb_url_v1": url("assignment.fb.gz"),
                });
            }
            // Both halves at once: the portal half alone does not resolve, and neither does the
            // pair without a schema bundle the portal itself never reads.
            AssignmentType::Split => {
                state["worker_assignment"] = json!({
                    "id": assignment_id,
                    "fb_url": url("worker-assignment.fb.gz"),
                    "version": "2",
                });
                state["portal_assignment"] = json!({
                    "id": assignment_id,
                    "fb_url": url("portal-assignment.fb.gz"),
                    "version": "2",
                });
                state["schema_bundle"] = json!({ "hash": "toy", "url": url("schema-bundle.json") });
            }
        }
    }
    state.to_string()
}

pub async fn start(
    port: u16,
    network_state: String,
    artifact_gz: Vec<u8>,
    portal_artifact_gz: Vec<u8>,
) -> anyhow::Result<Ledger> {
    let ledger = Ledger::default();
    let state = PublisherState {
        network_state,
        artifact_gz,
        portal_artifact_gz,
        ledger: ledger.clone(),
    };
    let app = Router::new()
        .route(
            "/network-state-tethys.json",
            get(|State(s): State<PublisherState>| async move {
                s.ledger.push("network-state");
                (
                    [("content-type", "application/json")],
                    s.network_state.clone(),
                )
            }),
        )
        .route(
            "/assignment.fb.gz",
            get(|State(s): State<PublisherState>| async move {
                s.ledger.push("artifact");
                s.artifact_gz.clone().into_response()
            }),
        )
        .route(
            // Prefixed "artifact" so a fetch of either format satisfies the same ledger check.
            "/portal-assignment.fb.gz",
            get(|State(s): State<PublisherState>| async move {
                s.ledger.push("artifact-portal");
                s.portal_artifact_gz.clone().into_response()
            }),
        )
        .with_state(state);
    super::serve(app, port).await?;
    Ok(ledger)
}
