//! DC-1 stub: a real worker peer on the portal's own p2p transport (same git
//! rev, `worker` feature), answering signed chunk queries from the toy world.
//!
//! The portal reaches it by listing the stub as a boot node (auto-whitelist +
//! direct dial); the shared dummy-chain file registers it as an active worker.
//! `PRIVATE_NETWORK=1` must be set or loopback addresses are unreachable.

use std::io::Write;
use std::path::Path;

use anyhow::Context;
use clap::Parser;
use flate2::{write::GzEncoder, Compression};
use futures::StreamExt;
use libp2p_identity::Keypair;
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
                    tracing::info!(%peer_id, chunk = %query.chunk_id, "stub worker query");
                    let result = answer(&world, &signing_keypair, &query, &ledger2);
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
) -> sqd_messages::QueryResult {
    let range = query.block_range.unwrap_or(sqd_messages::Range { begin: 0, end: 0 });
    ledger.push(format!(
        "query chunk={} range={}-{} dataset={}",
        query.chunk_id, range.begin, range.end, query.dataset
    ));

    let ds = world
        .datasets
        .iter()
        .find(|d| d.network_id.as_deref() == Some(query.dataset.as_str()));
    let Some(ds) = ds else {
        return error_result(query, keypair, "unknown dataset");
    };

    let jsonl = world.jsonl(&ds.name, range.begin, range.end);
    let data = match sqd_messages::Compression::try_from(query.compression) {
        Ok(sqd_messages::Compression::Gzip) => {
            let mut enc = GzEncoder::new(Vec::new(), Compression::default());
            enc.write_all(&jsonl).unwrap();
            enc.finish().unwrap()
        }
        Ok(sqd_messages::Compression::None) => jsonl,
        _ => return error_result(query, keypair, "unsupported compression"),
    };

    let mut result = sqd_messages::QueryResult {
        query_id: query.query_id.clone(),
        result: Some(sqd_messages::query_result::Result::Ok(sqd_messages::QueryOk {
            data,
            last_block: range.end,
        })),
        ..Default::default()
    };
    result.sign(keypair).expect("result signs");
    result
}

fn error_result(
    query: &sqd_messages::Query,
    keypair: &Keypair,
    msg: &str,
) -> sqd_messages::QueryResult {
    let mut result = sqd_messages::QueryResult {
        query_id: query.query_id.clone(),
        result: Some(sqd_messages::query_result::Result::Err(sqd_messages::QueryError {
            err: Some(sqd_messages::query_error::Err::ServerError(msg.to_string())),
        })),
        ..Default::default()
    };
    result.sign(keypair).expect("error signs");
    result
}
