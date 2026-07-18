//! DC-4 stub: the real-time source. Serves heads, status, and streams for
//! real-time-attached toy datasets, emitting `x-internal-*` noise so the
//! harness can assert the portal strips it (IB-4).

use std::io::Write;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use flate2::{write::GzEncoder, Compression};
use serde_json::{json, Value};

use super::Ledger;
use crate::world::ToyWorld;

#[derive(Clone)]
struct HotblocksState {
    world: ToyWorld,
    ledger: Ledger,
}

pub async fn start(port: u16, world: ToyWorld) -> anyhow::Result<Ledger> {
    let ledger = Ledger::default();
    let state = HotblocksState { world, ledger: ledger.clone() };
    let app = Router::new()
        .route("/datasets/:ds/head", get(head))
        .route("/datasets/:ds/finalized-head", get(finalized_head))
        .route("/datasets/:ds/status", get(status))
        .route("/datasets/:ds/stream", post(stream))
        .route("/datasets/:ds/finalized-stream", post(finalized_stream))
        .with_state(state);
    super::serve(app, port).await?;
    Ok(ledger)
}

fn heads_of(world: &ToyWorld, ds: &str) -> (u64, u64) {
    world
        .dataset(ds)
        .real_time
        .expect("hotblocks stub only serves real-time datasets")
}

async fn head(State(s): State<HotblocksState>, Path(ds): Path<String>) -> Response {
    s.ledger.push(format!("head {ds}"));
    let (h, _) = heads_of(&s.world, &ds);
    axum::Json(json!({ "number": h, "hash": s.world.hash(&ds, h) })).into_response()
}

async fn finalized_head(State(s): State<HotblocksState>, Path(ds): Path<String>) -> Response {
    s.ledger.push(format!("finalized-head {ds}"));
    let (_, f) = heads_of(&s.world, &ds);
    axum::Json(json!({ "number": f, "hash": s.world.hash(&ds, f) })).into_response()
}

async fn status(State(s): State<HotblocksState>, Path(ds): Path<String>) -> Response {
    s.ledger.push(format!("status {ds}"));
    let (h, f) = heads_of(&s.world, &ds);
    let d = s.world.dataset(&ds);
    axum::Json(json!({
        "kind": "evm",
        "retentionStrategy": "head",
        "data": {
            "firstBlock": d.start_block,
            "lastBlock": h,
            "lastBlockHash": s.world.hash(&ds, h),
            "lastBlockTimestamp": s.world.timestamp(h),
            "finalizedHead": { "number": f, "hash": s.world.hash(&ds, f) },
        }
    }))
    .into_response()
}

async fn stream(
    State(s): State<HotblocksState>,
    Path(ds): Path<String>,
    body: String,
) -> Response {
    serve_stream(s, ds, body, false)
}

async fn finalized_stream(
    State(s): State<HotblocksState>,
    Path(ds): Path<String>,
    body: String,
) -> Response {
    serve_stream(s, ds, body, true)
}

fn serve_stream(s: HotblocksState, ds: String, body: String, finalized: bool) -> Response {
    let query: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("bad query: {e}")).into_response();
        }
    };
    let from = query["fromBlock"].as_u64().unwrap_or(0);
    let to = query["toBlock"].as_u64();
    let (head, fin) = heads_of(&s.world, &ds);
    let frontier = if finalized { fin } else { head };
    s.ledger.push(format!(
        "{} {ds} from={from} to={to:?}",
        if finalized { "finalized-stream" } else { "stream" }
    ));

    let mut headers = HeaderMap::new();
    headers.insert("x-sqd-head-number", head.to_string().parse().unwrap());
    headers.insert("x-sqd-finalized-head-number", fin.to_string().parse().unwrap());
    headers.insert(
        "x-sqd-finalized-head-hash",
        s.world.hash(&ds, fin).parse().unwrap(),
    );
    headers.insert("x-internal-hotblocks-instance", "stub-1".parse().unwrap());

    if from > frontier {
        return (StatusCode::NO_CONTENT, headers).into_response();
    }

    let last = to.map_or(frontier, |t| t.min(frontier));
    let jsonl = s.world.jsonl(&ds, from, last);
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(&jsonl).unwrap();
    let gz = enc.finish().unwrap();

    headers.insert("content-type", "application/jsonl".parse().unwrap());
    headers.insert("content-encoding", "gzip".parse().unwrap());
    (StatusCode::OK, headers, Body::from(gz)).into_response()
}
