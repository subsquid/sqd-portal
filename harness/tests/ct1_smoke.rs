//! CT-1 smoke on a toy world — the Phase-0 exit criterion (spec/13 §build
//! order). Boots every IB-7 stub (including a real p2p worker), runs the
//! portal as a black box, streams over both serving sources, and checks every
//! stream response against the structural validators (5 of 6; the error-envelope
//! validator is not yet exercised) and the reference model, then audits gauges at
//! quiescence (INV-30).

use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context};
use harness::driver::Decoded;
use harness::model::{model, Expect, StreamReq};
use harness::portal::{Endpoints, PortalProcess};
use harness::validators::{validate_stream, Verdict};
use harness::{artifact, driver, dummy_chain, keys, metrics_audit, portal, stubs, ToyWorld};
use serde_json::json;

fn check_clean(context: &str, v: &Verdict, d: &Decoded) -> anyhow::Result<()> {
    for w in &v.warnings {
        eprintln!("[warn] {context}: {w}");
    }
    ensure!(
        v.errors.is_empty(),
        "{context} failed validation:\n  {}\nresponse body: {}",
        v.errors.join("\n  "),
        String::from_utf8_lossy(&d.body),
    );
    Ok(())
}

fn stream_query(from: u64, to: u64) -> serde_json::Value {
    json!({
        "type": "evm",
        "fromBlock": from,
        "toBlock": to,
        "includeAllBlocks": true,
        "fields": { "block": { "number": true, "hash": true } },
    })
}

struct Ctx {
    world: ToyWorld,
    base: String,
    http: reqwest::Client,
    worker_ledgers: Vec<stubs::Ledger>,
    hotblocks_ledger: stubs::Ledger,
    publisher_ledger: stubs::Ledger,
}

#[tokio::test(flavor = "multi_thread")]
async fn ct1_smoke() -> anyhow::Result<()> {
    // Loopback p2p addresses are filtered as unreachable unless this is set.
    std::env::set_var("PRIVATE_NETWORK", "1");
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .try_init();

    let scratch_dir = tempfile::tempdir()?;
    let scratch = scratch_dir.path().to_path_buf();
    let world = ToyWorld::standard();

    let endpoints = Endpoints {
        publisher_port: harness::free_tcp_port(),
        registry_port: harness::free_tcp_port(),
        hotblocks_port: harness::free_tcp_port(),
        http_port: harness::free_tcp_port(),
    };
    // The portal pre-leases 1 + retries distinct workers per chunk, so the
    // toy network runs two stub workers. Readiness then also genuinely gates
    // on a p2p connection (⌊2·3/4⌋ = 1).
    let worker_udp_ports = [harness::free_udp_port(), harness::free_udp_port()];

    // Identities and the shared dummy chain (whitelists both directions).
    let worker_ids = [
        keys::generate(&scratch.join("worker-0.key"))?,
        keys::generate(&scratch.join("worker-1.key"))?,
    ];
    let portal_id = keys::generate(&scratch.join("portal.key"))?;
    let worker_peers: Vec<_> = worker_ids.iter().map(|w| w.peer_id).collect();
    let dummy_path = scratch.join("dummy_client.json");
    std::fs::write(
        &dummy_path,
        dummy_chain::dummy_data_json(&worker_peers, portal_id.peer_id),
    )?;

    // IB-7 stubs.
    let artifact_gz = artifact::build_gzipped(&world, &worker_peers)?;
    let publisher_ledger = stubs::publisher::start(
        endpoints.publisher_port,
        stubs::publisher::network_state_json(endpoints.publisher_port, "toy-assignment-1", 0),
        artifact_gz,
    )
    .await?;
    let _registry_ledger = stubs::registry::start(endpoints.registry_port, &world).await?;
    let hotblocks_ledger =
        stubs::hotblocks::start(endpoints.hotblocks_port, world.clone()).await?;
    let mut worker_ledgers = Vec::new();
    for (i, id) in worker_ids.iter().enumerate() {
        let stub = stubs::worker::start(
            world.clone(),
            &scratch.join(format!("worker-{i}.key")),
            id.keypair.clone(),
            &dummy_path,
            worker_udp_ports[i],
        )
        .await?;
        worker_ledgers.push(stub.ledger.clone());
        std::mem::forget(stub); // keep the transport handle alive for the test
    }
    tokio::time::sleep(Duration::from_millis(500)).await; // QUIC listeners up

    // The portal, as a black box.
    let boot_nodes = worker_ids
        .iter()
        .zip(worker_udp_ports)
        .map(|(id, port)| format!("{} /ip4/127.0.0.1/udp/{port}/quic-v1", id.peer_id))
        .collect::<Vec<_>>()
        .join(",");
    let config = portal::write_config(&scratch, &world, &endpoints)?;
    let mut portal_proc = portal::spawn(
        &scratch,
        &config,
        &scratch.join("portal.key"),
        &dummy_path,
        &boot_nodes,
        &endpoints,
    )?;

    let ctx = Ctx {
        world,
        base: portal_proc.base_url.clone(),
        http: driver::client(),
        worker_ledgers,
        hotblocks_ledger,
        publisher_ledger,
    };

    let result = run_smoke(&ctx, &mut portal_proc).await;
    if result.is_err() {
        eprintln!("=== portal log tail ===\n{}", portal_proc.log_tail(80));
        let kept = scratch_dir.keep();
        eprintln!("=== scratch kept at {} ===", kept.display());
    }
    portal_proc.terminate();
    result
}

async fn run_smoke(ctx: &Ctx, portal_proc: &mut PortalProcess) -> anyhow::Result<()> {
    portal_proc.wait_ready(Duration::from_secs(60)).await?;
    let (base, http, world) = (&ctx.base, &ctx.http, &ctx.world);

    // Catalog lists both datasets (REQ-10).
    let catalog = driver::get(http, &format!("{base}/datasets")).await?;
    ensure!(catalog.status == 200, "catalog: {}", String::from_utf8_lossy(&catalog.body));
    let catalog_text = String::from_utf8_lossy(&catalog.body);
    ensure!(catalog_text.contains("toy"), "catalog misses toy: {catalog_text}");
    ensure!(catalog_text.contains("toy-rt"), "catalog misses toy-rt: {catalog_text}");

    // Archival head from the applied artifact (REQ-11).
    let head = driver::get(http, &format!("{base}/datasets/toy/archival-head")).await?;
    ensure!(head.status == 200, "archival-head: {}", String::from_utf8_lossy(&head.body));
    let head_json: serde_json::Value = serde_json::from_slice(&head.body)?;
    ensure!(head_json["number"].as_u64() == Some(99), "archival head number: {head_json}");
    ensure!(
        head_json["hash"].as_str() == Some(world.hash("toy", 99).as_str()),
        "archival head hash: {head_json}"
    );

    // Readiness needs ⌊2·3/4⌋ = 1 live worker connection, but the worker assigned
    // this chunk may not be the one that satisfied /ready and may not be dialed
    // yet — so give the p2p dial a bounded window before asserting serving.
    wait_archival_serving(ctx, portal_proc, Duration::from_secs(30)).await?;

    // CT-1 core: archival stream within one chunk.
    let req = StreamReq { dataset: "toy".into(), from: 0, to: Some(25), finalized: true };
    let expect = model(world, &req);
    let resp =
        driver::stream(http, base, "toy", "finalized-stream", &stream_query(0, 25), "ct1-a")
            .await?;
    check_clean("archival single-chunk stream", &validate_stream(world, &req, &expect, &resp), &resp)?;

    // Archival stream crossing chunk boundaries (ordering across fan-out).
    let req = StreamReq { dataset: "toy".into(), from: 30, to: Some(85), finalized: true };
    let expect = model(world, &req);
    let resp =
        driver::stream(http, base, "toy", "finalized-stream", &stream_query(30, 85), "ct1-b")
            .await?;
    check_clean("archival cross-chunk stream", &validate_stream(world, &req, &expect, &resp), &resp)?;

    // Alias resolution serves the same dataset (DEF-1).
    let req = StreamReq { dataset: "toy-alias".into(), from: 90, to: Some(99), finalized: true };
    let expect = model(world, &req);
    let resp =
        driver::stream(http, base, "toy-alias", "finalized-stream", &stream_query(90, 99), "ct1-c")
            .await?;
    check_clean("archival stream via alias", &validate_stream(world, &req, &expect, &resp), &resp)?;

    // Real-time proxied stream (ADR-003 pass-through, internal headers stripped).
    let req = StreamReq { dataset: "toy-rt".into(), from: 10, to: Some(20), finalized: false };
    let expect = model(world, &req);
    let resp = driver::stream(http, base, "toy-rt", "stream", &stream_query(10, 20), "ct1-d").await?;
    check_clean("real-time stream", &validate_stream(world, &req, &expect, &resp), &resp)?;

    // Beyond-frontier poll → EMPTY (REQ-5; proxied 204 passes through).
    let req = StreamReq { dataset: "toy-rt".into(), from: 1000, to: None, finalized: false };
    let expect = model(world, &req);
    let resp = driver::stream(
        http,
        base,
        "toy-rt",
        "stream",
        &json!({"type": "evm", "fromBlock": 1000, "includeAllBlocks": true,
                "fields": {"block": {"number": true, "hash": true}}}),
        "ct1-e",
    )
    .await?;
    ensure!(matches!(expect, Expect::Empty { .. }), "model should expect EMPTY");
    check_clean("beyond-frontier EMPTY", &validate_stream(world, &req, &expect, &resp), &resp)?;

    // Ledger sanity: the happy-path stubs never fail, so the portal makes one
    // attempt per chunk (no retries induced). The ≤ 2 bound below is a smoke
    // check that also confirms the upstream ledgers saw the implied traffic; the
    // real FV-2 retry bound needs injected worker failures and belongs to CT-3.
    let queries: Vec<String> =
        ctx.worker_ledgers.iter().flat_map(|l| l.entries()).collect();
    eprintln!("worker stub ledger:\n  {}", queries.join("\n  "));
    // Key by (chunk, range) aggregated across requests; with no failures each
    // key should appear about once.
    let mut per_attempt: std::collections::HashMap<&str, usize> = Default::default();
    for q in &queries {
        let key = q.split(" dataset=").next().unwrap_or(q);
        *per_attempt.entry(key).or_default() += 1;
    }
    for (attempt, n) in &per_attempt {
        ensure!(*n <= 2, "{attempt} queried {n} times (> 1 + retries)");
    }
    ensure!(!queries.is_empty(), "archival streams produced no worker queries");
    ensure!(
        ctx.hotblocks_ledger.count_with_prefix("stream toy-rt") >= 2,
        "hotblocks ledger misses stream calls: {:?}",
        ctx.hotblocks_ledger.entries()
    );
    ensure!(
        ctx.publisher_ledger.count_with_prefix("artifact") >= 1,
        "artifact never fetched"
    );

    // Quiescence (one heartbeat interval, no in-flight work), then the gauge
    // audit (INV-30).
    tokio::time::sleep(Duration::from_secs(5)).await;
    let metrics = driver::get(http, &format!("{base}/metrics")).await?;
    ensure!(metrics.status == 200, "metrics endpoint: {}", metrics.status);
    let text = String::from_utf8_lossy(&metrics.body);
    let failures = metrics_audit::audit_quiescent(&text, 2.0);
    ensure!(failures.is_empty(), "gauge audit failed:\n  {}", failures.join("\n  "));

    Ok(())
}

/// Retry a one-block archival stream until the worker connection serves it.
async fn wait_archival_serving(
    ctx: &Ctx,
    portal_proc: &mut PortalProcess,
    timeout: Duration,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let mut last = None;
    while start.elapsed() < timeout {
        let d = driver::stream(
            &ctx.http,
            &ctx.base,
            "toy",
            "finalized-stream",
            &stream_query(0, 0),
            "ct1-warmup",
        )
        .await
        .context("warmup stream")?;
        if d.status == 200 {
            return Ok(());
        }
        last = Some((d.status, String::from_utf8_lossy(&d.body).to_string()));
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let tail = portal_proc.log_tail(40);
    bail!("archival serving never became available within {timeout:?}; last response: {last:?}\nportal log tail:\n{tail}");
}
