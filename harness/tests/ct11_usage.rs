//! CT-11 — shadow usage measurement, end to end against the portal as a black
//! box (REQ-60, DC-9, INV-32).
//!
//! Three claims, and they are the three the design rests on:
//!
//! 1. **Records arrive attributed and batched.** A gated request produces a
//!    record naming the key, its owner and the route, delivered to the ingest
//!    beside the exchange and signed the same way — and several requests leave
//!    in fewer deliveries than there were records, or the sink's call rate is
//!    the data plane's request rate.
//! 2. **A sink outage never reaches a client.** With the ingest refusing every
//!    delivery, the same request returns the same status, the same headers and
//!    the same bytes as it did with the ingest healthy.
//! 3. **Interim deltas plus the residual are the total.** A stream paced to
//!    outlive `P-USAGE-INTERIM` reports more than once, and those records sum
//!    to exactly the encoded bytes the client received.
//!
//! Two portals, because the property differs by configuration: one measuring,
//! and one with no `usage:` block at all — which must report nothing while
//! serving identically (the phase-2 kill switch).

use std::time::Duration;

use anyhow::{ensure, Context};
use harness::driver::Decoded;
use harness::fixture::Fixture;
use harness::portal::Auth;
use harness::stubs::control_plane::Answer;
use harness::{driver, ToyWorld};
use serde_json::{json, Value};

const PORTAL_ID: &str = "portal-harness-eu";
const ORGANIZATION: &str = "org-harness-7";

/// Long enough that the paced stream below crosses it twice, short enough that
/// the run stays seconds rather than minutes.
const INTERIM_SECS: u64 = 1;

fn token(key_id: &str) -> String {
    format!("sqd_portal_{key_id}_s3cr3tvalue-{key_id}")
}

fn bearer(key_id: &str) -> String {
    format!("Bearer {}", token(key_id))
}

/// One chunk of the toy world's archival dataset: cheap, and it exercises the
/// network path rather than the proxy.
fn query() -> Value {
    json!({
        "type": "evm",
        "fromBlock": 0,
        "toBlock": 39,
        "includeAllBlocks": true,
        "fields": { "block": { "number": true, "hash": true } },
    })
}

async fn stream_as(
    fx: &Fixture,
    dataset: &str,
    endpoint: &str,
    key_id: &str,
    request_id: &str,
) -> anyhow::Result<Decoded> {
    driver::stream_as(
        &fx.http,
        &fx.base,
        dataset,
        endpoint,
        &query(),
        request_id,
        Some(&bearer(key_id)),
    )
    .await
}

/// What a client can see of a response — the whole of it, since a record the
/// portal made about it must show up in none of these.
fn observable(d: &Decoded) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let mut headers: Vec<(String, String)> = d
        .headers
        .iter()
        // Per-request by construction, and neither says anything about
        // measurement: one is the correlation id the client sent, the other the
        // clock.
        .filter(|(name, _)| name.as_str() != "x-request-id" && name.as_str() != "date")
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    headers.sort();
    (d.status, headers, d.body.clone())
}

fn wire_bytes(events: &[Value]) -> u64 {
    events
        .iter()
        .map(|event| event["wire_bytes"].as_u64().unwrap_or_default())
        .sum()
}

fn statuses(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .map(|event| event["status"].as_str().unwrap_or_default().to_owned())
        .collect()
}

/// Waits for the reporter's own cadence rather than guessing at it: the flush
/// interval bounds publication, so a test that asserts before it would be
/// asserting on the clock.
async fn wait_for_events(fx: &Fixture, key_id: &str, at_least: usize) -> anyhow::Result<Vec<Value>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let events = fx.cp().usage_events_for(key_id);
        if events.len() >= at_least {
            return Ok(events);
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "waited 30s for {at_least} usage records for {key_id}, saw {}: {:?}",
            events.len(),
            fx.cp().usage_events(),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------------------
// The measuring portal.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn ct11_usage_measurement() -> anyhow::Result<()> {
    let auth = Auth::new(PORTAL_ID)
        .limit("exchange_rate_per_sec", 1000)
        // Short enough that the assertions below are not mostly waiting.
        .usage("flush_interval_ms", 500)
        .usage("interim_interval_secs", INTERIM_SECS);
    let mut fx = Fixture::start_with_auth(ToyWorld::standard(), 2, auth).await?;
    let result = measuring(&mut fx).await;
    fx.finish(result)
}

async fn measuring(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    fx.cp()
        .default_answer(Answer::grant_owned_by(ORGANIZATION));

    // ---- 1. Attributed, and carrying what the ingest cannot infer ----------
    let served = stream_as(fx, "toy", "finalized-stream", "attributed", "ct11-attr").await?;
    ensure!(
        served.status == 200,
        "a measured request must be served: {} — {}",
        served.status,
        String::from_utf8_lossy(&served.body),
    );
    ensure!(
        served.block_numbers() == (0..=39).collect::<Vec<u64>>(),
        "measurement must not change what is delivered",
    );

    let events = wait_for_events(fx, "attributed", 1).await?;
    let record = events.last().context("a record for the served request")?;
    ensure!(
        record["key_id"] == "attributed",
        "records are attributed to the key that caused them: {record}",
    );
    ensure!(
        record["organization_id"] == ORGANIZATION,
        "the grant's owner must reach the record (REQ-60): {record}",
    );
    ensure!(
        record["dataset"] == "toy",
        "the canonical dataset name, not the alias: {record}",
    );
    ensure!(
        record["endpoint"] == "/finalized-stream",
        "the route label, not the request path (HZ-15): {record}",
    );
    ensure!(
        record["encoding"] == "gzip",
        "encoded bytes are keyed on the encoding that produced them: {record}",
    );
    ensure!(
        record["wire_bytes"].as_u64().unwrap_or_default() > 0
            && record["event_id"].as_str().is_some_and(|id| !id.is_empty()),
        "a record needs its bytes and its idempotency key: {record}",
    );
    // The portal identity is the control plane's to stamp from the verified
    // signature; a field for it would be one a portal could forge.
    ensure!(
        record.get("pod").is_none() && record.get("portal_id").is_none(),
        "a record must not carry a portal identity of its own: {record}",
    );
    ensure!(
        !fx.cp().usage_batches().is_empty(),
        "the ingest saw no delivery at all",
    );

    // ---- 2. Batched: fewer deliveries than records ------------------------
    // Sequential on purpose. The toy pool is two workers at one query each, so
    // six concurrent streams would measure the pool rather than the reporter;
    // six requests of a few milliseconds each land well inside one flush
    // interval either way.
    let batches_before = fx.cp().usage_batches().len();
    for index in 0..6 {
        let served = stream_as(
            fx,
            "toy",
            "finalized-stream",
            "batched",
            &format!("ct11-batch-{index}"),
        )
        .await?;
        ensure!(
            served.status == 200,
            "every batched request is served: {}",
            served.status
        );
    }
    let batched = wait_for_events(fx, "batched", 6).await?;
    let deliveries = fx.cp().usage_batches().len() - batches_before;
    ensure!(
        deliveries < batched.len(),
        "six records arrived in {deliveries} deliveries: the sink's call rate is the request rate",
    );

    // ---- 3. Interim deltas and the residual sum to the total ---------------
    // The real-time stub hands its body over in pieces, so the response outlives
    // the interval the portal cuts records on. Nothing else in the harness can
    // produce a stream that lasts.
    fx.hotblocks_trickle
        .set(6, Duration::from_millis(400) * INTERIM_SECS as u32);
    let paced = stream_as(fx, "toy-rt", "finalized-stream", "paced", "ct11-paced").await?;
    fx.hotblocks_trickle.clear();
    ensure!(
        paced.status == 200 && paced.decode_errors.is_empty(),
        "the paced stream must still be a well-formed response: {} {:?}",
        paced.status,
        paced.decode_errors,
    );

    let deltas = wait_for_events(fx, "paced", 2).await?;
    ensure!(
        statuses(&deltas).last().map(String::as_str) == Some("completed"),
        "the last record ends the response: {:?}",
        statuses(&deltas),
    );
    ensure!(
        statuses(&deltas)[..deltas.len() - 1]
            .iter()
            .all(|status| status == "open"),
        "every record before the last is an interim delta: {:?}",
        statuses(&deltas),
    );
    ensure!(
        wire_bytes(&deltas) == paced.encoded_len as u64,
        "deltas summed to {} of the {} encoded bytes the client received",
        wire_bytes(&deltas),
        paced.encoded_len,
    );

    // ---- 4. A sink outage is invisible to the client ------------------------
    let healthy = stream_as(fx, "toy", "finalized-stream", "outage", "ct11-healthy").await?;
    fx.cp().refuse_usage(503);
    let attempts_before = fx.cp().usage_attempts();
    let during = stream_as(fx, "toy", "finalized-stream", "outage", "ct11-outage").await?;

    ensure!(
        observable(&during) == observable(&healthy),
        "INV-32: a sink outage changed the response — {:?} against {:?}",
        observable(&during).0,
        observable(&healthy).0,
    );
    ensure!(
        during.block_numbers() == (0..=39).collect::<Vec<u64>>(),
        "a stream must not be shortened because the sink is down",
    );
    // …and the portal is genuinely failing to report, rather than passing this
    // case by having stopped reporting at all.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while fx.cp().usage_attempts() == attempts_before {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the reporter stopped trying, so the outage case proves nothing",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The refusal is retried rather than abandoned: once the ingest recovers,
    // records land again without a restart.
    fx.cp().accept_usage();
    stream_as(fx, "toy", "finalized-stream", "recovered", "ct11-recovered").await?;
    wait_for_events(fx, "recovered", 1).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// The portal with no `usage:` block: the kill switch.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn ct11_without_a_usage_block_nothing_is_reported() -> anyhow::Result<()> {
    let auth = Auth::new(PORTAL_ID).limit("exchange_rate_per_sec", 1000);
    let mut fx = Fixture::start_with_auth(ToyWorld::standard(), 2, auth).await?;
    let result = unmeasured(&mut fx).await;
    fx.finish(result)
}

async fn unmeasured(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    fx.cp()
        .default_answer(Answer::grant_owned_by(ORGANIZATION));

    let served = stream_as(fx, "toy", "finalized-stream", "unmeasured", "ct11-off").await?;

    ensure!(
        served.status == 200 && served.block_numbers() == (0..=39).collect::<Vec<u64>>(),
        "an authorizing portal that measures nothing serves exactly as it did",
    );
    // Longer than any flush interval a default block would use, so this is a
    // portal that reports nothing rather than one that has not reported yet.
    tokio::time::sleep(Duration::from_secs(2)).await;
    ensure!(
        fx.cp().usage_attempts() == 0,
        "REQ-60: with no `usage:` block the portal must not call the ingest at all: {:?}",
        fx.cp().ledger.entries(),
    );

    // The keyless scrape carries the families — they are registered for the
    // process, like every other — and they sit at zero.
    let scrape = fx.scrape().await?;
    for family in [
        "portal_usage_events_enqueued",
        "portal_usage_events_delivered",
        "portal_usage_queue_depth",
    ] {
        let reported: Vec<&str> = scrape
            .lines()
            .filter(|line| line.starts_with(family) && !line.ends_with(" 0"))
            .collect();
        ensure!(
            reported.is_empty(),
            "{family} moved on a portal that measures nothing: {reported:?}",
        );
    }

    Ok(())
}
