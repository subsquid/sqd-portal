//! DC-1 stub: a real worker peer on the portal's own p2p transport (same git
//! rev, `worker` feature), answering signed chunk queries from the toy world.
//!
//! The portal reaches it by listing the stub as a boot node (auto-whitelist +
//! direct dial); the shared dummy-chain file registers it as an active worker.
//! `PRIVATE_NETWORK=1` must be set or loopback addresses are unreachable.

use std::collections::VecDeque;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use clap::Parser;
use flate2::{write::GzEncoder, Compression};
use futures::StreamExt;
use libp2p_identity::Keypair;
use sqd_messages::query_error;
use sqd_network_transport::{
    AgentInfo, P2PTransportBuilder, TransportArgs, WorkerConfig, WorkerEvent,
};

use super::Ledger;
use crate::world::ToyWorld;

#[derive(Parser)]
struct StubCli {
    #[command(flatten)]
    transport: TransportArgs,
}

/// One row of the DC-1 worker-fault table (spec/09), injectable into a stub's
/// next answer. `Overshoot`/`Undershoot`/`BadSignature` are the integrity
/// family: the portal must discard them, reroute, and never deliver the data.
#[derive(Debug, Clone)]
pub enum WorkerFault {
    /// Report a `last_block` this many blocks past the queried range end.
    Overshoot(u64),
    /// Report a `last_block` this many blocks below the queried range start.
    Undershoot(u64),
    /// Sign with a key that is not this worker's identity.
    BadSignature,
    /// Answer with a server-error verdict.
    ServerError(String),
    /// Answer "chunk not found" — the worker is still downloading it. Same
    /// DC-1 row as ServerError, but the portal treats it as retriable.
    NotFound(String),
}

/// A fault queue shared by every stub worker in a test, so a single queued
/// fault lands on whichever worker the portal happens to pick (FV-1) — the
/// alternative would be a test that only passes for one routing choice.
#[derive(Clone, Default)]
pub struct WorkerFaults {
    queued: Arc<Mutex<VecDeque<WorkerFault>>>,
    persistent: Arc<Mutex<Option<WorkerFault>>>,
}

impl WorkerFaults {
    pub fn none() -> Self {
        Self::default()
    }

    /// Apply `fault` to the next `count` answers, then behave.
    pub fn queue(&self, fault: WorkerFault, count: usize) -> &Self {
        let mut q = self.queued.lock().unwrap();
        for _ in 0..count {
            q.push_back(fault.clone());
        }
        self
    }

    /// Apply `fault` to every answer until cleared.
    pub fn always(&self, fault: WorkerFault) -> &Self {
        *self.persistent.lock().unwrap() = Some(fault);
        self
    }

    pub fn clear(&self) {
        self.queued.lock().unwrap().clear();
        *self.persistent.lock().unwrap() = None;
    }

    fn next(&self) -> Option<WorkerFault> {
        if let Some(f) = self.queued.lock().unwrap().pop_front() {
            return Some(f);
        }
        self.persistent.lock().unwrap().clone()
    }
}

pub struct WorkerStub {
    pub ledger: Ledger,
}

/// Start the stub worker. Returns once the transport is up and the event loop
/// is answering queries in a background task.
pub async fn start(
    world: ToyWorld,
    key_path: &Path,
    signing_keypair: Keypair,
    dummy_client_path: &Path,
    udp_port: u16,
    faults: WorkerFaults,
) -> anyhow::Result<WorkerStub> {
    let listen_addr = format!("/ip4/127.0.0.1/udp/{udp_port}/quic-v1");
    let cli = StubCli::try_parse_from([
        "conformance-worker",
        "--key",
        key_path.to_str().context("key path utf8")?,
        "--p2p-listen-addrs",
        &listen_addr,
        "--p2p-public-addrs",
        &listen_addr,
        "--rpc-url",
        "http://127.0.0.1:9/",
        "--l1-rpc-url",
        "http://127.0.0.1:9/",
        "--network",
        "tethys",
        "--dummy-client-file-path",
        dummy_client_path.to_str().context("dummy path utf8")?,
    ])
    .context("stub cli parse")?;

    let agent = AgentInfo { name: "conformance-worker", version: "0.1.0" };
    let builder = P2PTransportBuilder::from_cli(cli.transport, agent)
        .await
        .context("transport builder")?;
    let (events, handle) = builder
        .build_worker(WorkerConfig::default())
        .await
        .context("build worker")?;

    let ledger = Ledger::default();
    let ledger2 = ledger.clone();
    tokio::spawn(async move {
        futures::pin_mut!(events);
        while let Some(event) = events.next().await {
            match event {
                WorkerEvent::Query { peer_id, query, resp_chan } => {
                    let fault = faults.next();
                    tracing::info!(%peer_id, chunk = %query.chunk_id, ?fault, "stub worker query");
                    let result = answer(&world, &signing_keypair, &query, &ledger2, fault);
                    if handle.send_query_result(result, resp_chan).is_err() {
                        tracing::warn!("query result queue full");
                    }
                }
                other => tracing::debug!("ignoring worker event: {other:?}"),
            }
        }
    });

    Ok(WorkerStub { ledger })
}

fn answer(
    world: &ToyWorld,
    keypair: &Keypair,
    query: &sqd_messages::Query,
    ledger: &Ledger,
    fault: Option<WorkerFault>,
) -> sqd_messages::QueryResult {
    let range = query.block_range.unwrap_or(sqd_messages::Range { begin: 0, end: 0 });
    ledger.push(format!(
        "query chunk={} range={}-{} dataset={} fault={}",
        query.chunk_id,
        range.begin,
        range.end,
        query.dataset,
        fault.as_ref().map_or("none".to_owned(), |f| format!("{f:?}")),
    ));

    match &fault {
        Some(WorkerFault::ServerError(m)) => {
            return verdict_result(query, keypair, query_error::Err::ServerError(m.clone()))
        }
        Some(WorkerFault::NotFound(m)) => {
            return verdict_result(query, keypair, query_error::Err::NotFound(m.clone()))
        }
        _ => {}
    }

    let ds = world
        .datasets
        .iter()
        .find(|d| d.network_id.as_deref() == Some(query.dataset.as_str()));
    let Some(ds) = ds else {
        return error_result(query, keypair, "unknown dataset");
    };

    // The real engine emits every block for `includeAllBlocks`, else only this
    // chunk's coverage boundary, header-only when nothing matches (INV-29).
    let include_all = serde_json::from_str::<serde_json::Value>(&query.query)
        .ok()
        .and_then(|q| q["includeAllBlocks"].as_bool())
        .unwrap_or(false);
    let jsonl = if include_all {
        world.jsonl(&ds.name, range.begin, range.end)
    } else {
        world.boundary_jsonl(&ds.name, range.begin, range.end)
    };
    let data = match sqd_messages::Compression::try_from(query.compression) {
        Ok(sqd_messages::Compression::Gzip) => {
            let mut enc = GzEncoder::new(Vec::new(), Compression::default());
            enc.write_all(&jsonl).unwrap();
            enc.finish().unwrap()
        }
        Ok(sqd_messages::Compression::None) => jsonl,
        _ => return error_result(query, keypair, "unsupported compression"),
    };

    let last_block = match &fault {
        Some(WorkerFault::Overshoot(n)) => range.end + n,
        Some(WorkerFault::Undershoot(n)) => range.begin.saturating_sub((*n).max(1)),
        _ => range.end,
    };

    let mut result = sqd_messages::QueryResult {
        query_id: query.query_id.clone(),
        result: Some(sqd_messages::query_result::Result::Ok(sqd_messages::QueryOk {
            data,
            last_block,
        })),
        ..Default::default()
    };
    // A wrong key produces a well-formed result that fails verification —
    // signing is what the portal checks, not the identity that sent it.
    let signer = match &fault {
        Some(WorkerFault::BadSignature) => &Keypair::generate_ed25519(),
        _ => keypair,
    };
    result.sign(signer).expect("result signs");
    result
}

fn error_result(
    query: &sqd_messages::Query,
    keypair: &Keypair,
    msg: &str,
) -> sqd_messages::QueryResult {
    verdict_result(query, keypair, query_error::Err::ServerError(msg.to_string()))
}

fn verdict_result(
    query: &sqd_messages::Query,
    keypair: &Keypair,
    err: query_error::Err,
) -> sqd_messages::QueryResult {
    let mut result = sqd_messages::QueryResult {
        query_id: query.query_id.clone(),
        result: Some(sqd_messages::query_result::Result::Err(sqd_messages::QueryError {
            err: Some(err),
        })),
        ..Default::default()
    };
    result.sign(keypair).expect("error signs");
    result
}
