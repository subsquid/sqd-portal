//! Stale-signed-timestamp rejection — the class for the 2026 production
//! incident where a portal returned hard, non-retriable `400`s for specific
//! chunk ranges.
//!
//! ## The defect
//!
//! `prepare_query` (src/network/client.rs) stamps `timestamp_ms = now` and
//! signs the worker query; a worker validates that signed timestamp at
//! admission against `MAX_TIME_LAG = 60s` (worker-rs `validate_query`) and
//! answers `BadRequest("timestamp out of allowed range")` when it is exceeded.
//! Two things compounded:
//!
//! 1. The query was signed *before* `send_to_transport` awaited its
//!    congestion-scheduler permit, so under load the signature aged in the
//!    portal's own queue.
//! 2. The verdict table (`Verdict::of`) mapped *any* worker `BadRequest` — the
//!    stale-envelope rejection included — to a non-retriable client `400`
//!    (`invalid_request_error` / `malformed_request`), no reroute, no failover.
//!
//! ## What these tests pin
//!
//! - The verdict side (landed on master as the `clock_skew` row): the
//!   stale-envelope rejection reroutes on the error cooldown, so the client
//!   sees a `200` served by another worker, and the rejecting worker's cooldown
//!   decays instead of latching ([`ct_stale_timestamp_is_retried`] drives the
//!   genuine wire rejection end to end, plus the stub now applies the real
//!   admission-time freshness check unconditionally).
//! - The signing side (this branch): the send permit is acquired *before* the
//!   query is stamped and signed (`query_worker`), so queue time can no longer
//!   age a signature ([`ct_stale_timestamp_congestion_queue_boundary`]
//!   documents the permit lifetimes that make the >60s queue itself
//!   unreachable in this harness; the ordering is pinned at unit level on the
//!   `acquire_permit_then_timestamp` seam).

use std::time::{Duration, Instant};

use anyhow::{ensure, Context};
use harness::driver::{self, Decoded};
use harness::fixture::Fixture;
use harness::portal::Tuning;
use harness::stubs::worker::WorkerFault;
use harness::ToyWorld;
use serde_json::json;

/// Chunk 2 of the toy world is blocks 40..=79 — a single-chunk range, so the
/// per-attempt worker queries in the ledgers are unambiguous.
const FROM: u64 = 40;
const TO: u64 = 79;

fn query(from: u64, to: u64) -> serde_json::Value {
    json!({
        "type": "evm",
        "fromBlock": from,
        "toBlock": to,
        "includeAllBlocks": true,
        "fields": { "block": { "number": true, "hash": true } },
    })
}

/// The pre-fix production envelope: a terminal 400 carrying the worker's
/// anti-replay reason. After the fix this must never reach a client.
fn is_stale_timestamp_400(d: &Decoded) -> bool {
    if d.status != 400 {
        return false;
    }
    let Ok(body) = serde_json::from_slice::<serde_json::Value>(&d.body) else {
        return false;
    };
    body["error"]["code"] == "malformed_request"
        && body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("timestamp out of allowed range")
}

/// A worker's stale-envelope rejection is transient: the portal must reroute
/// with a freshly signed attempt and deliver a `200`, never the pre-fix
/// terminal `400` — and the rejecting worker's error cooldown must decay
/// rather than latch.
#[tokio::test(flavor = "multi_thread")]
async fn ct_stale_timestamp_is_retried() -> anyhow::Result<()> {
    // Two workers, so the reroute has somewhere to go.
    let mut fx = Fixture::start(ToyWorld::standard(), 2).await?;
    let result = run_retry(&mut fx).await;
    fx.finish(result)
}

async fn run_retry(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    let (base, http) = (fx.base.clone(), fx.http.clone());

    // Baseline: the range is servable, so any later deviation is attributable
    // to the injected rejection and not to the world.
    let baseline = driver::stream(
        &http,
        &base,
        "toy",
        "finalized-stream",
        &query(FROM, TO),
        "stale-baseline",
    )
    .await?;
    ensure!(
        baseline.status == 200,
        "baseline must serve, got {} — {}",
        baseline.status,
        String::from_utf8_lossy(&baseline.body),
    );

    // The genuine wire rejection: whichever worker the portal picks first
    // answers `BadRequest("timestamp out of allowed range")`, exactly as a real
    // worker does when a signed timestamp is stale at admission.
    fx.worker_faults.queue(WorkerFault::StaleEnvelope, 1);
    let before = fx.queries_answered();
    let d = driver::stream(
        &http,
        &base,
        "toy",
        "finalized-stream",
        &query(FROM, TO),
        "stale-retried",
    )
    .await?;

    eprintln!(
        "[ct_stale_timestamp] response after one stale rejection: status={} body-bytes={}",
        d.status,
        d.body.len(),
    );

    // The fixed behavior: the rejection is masked by a reroute, the client
    // gets the data.
    ensure!(
        !is_stale_timestamp_400(&d),
        "the worker's anti-replay rejection reached the client as the pre-fix terminal 400: {}",
        String::from_utf8_lossy(&d.body),
    );
    ensure!(
        d.status == 200,
        "one stale rejection must be masked by a retry, got {} — {}",
        d.status,
        String::from_utf8_lossy(&d.body),
    );
    ensure!(
        d.body == baseline.body,
        "retried response differs from the unfaulted baseline"
    );

    // The retry is real: the rejected attempt plus at least one fresh attempt
    // appear in the worker ledgers.
    let sent = fx.queries_answered() - before;
    ensure!(
        sent >= 2,
        "a rejected attempt must be rerouted; only {sent} worker query was answered"
    );

    // The rejection draws the DC-1 error cooldown (spec/05: "reroute; cooldown
    // P-WORKER-ERROR-COOLDOWN") — but the cooldown must decay, never latch. The
    // pool keeps serving through it via the other worker, and once the harness's
    // 1s penalty window passes, the rejecting worker itself serves fresh queries
    // again. Its selection after decay is deterministic: Best-group workers rank
    // by measured throughput with "no data yet" ahead of any measurement, and
    // the rejector — whose only interaction was the rejection — is the one
    // worker without a throughput sample.
    fx.worker_faults.clear();
    let rejecting = rejecting_worker(fx)?;
    let served_before = fresh_serves(&fx.worker_ledgers[rejecting]);
    let after = driver::stream(
        &http,
        &base,
        "toy",
        "finalized-stream",
        &query(FROM, TO),
        "stale-after",
    )
    .await?;
    ensure!(
        after.status == 200,
        "the pool must serve right through the rejector's cooldown, got {} — {}",
        after.status,
        String::from_utf8_lossy(&after.body),
    );
    ensure!(
        after.body == baseline.body,
        "follow-up response differs from the unfaulted baseline"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut probes = 0usize;
    while fresh_serves(&fx.worker_ledgers[rejecting]) == served_before {
        ensure!(
            std::time::Instant::now() < deadline,
            "the rejecting worker never served again after {probes} probes — its \
             error cooldown latched instead of decaying; ledger: {:?}",
            fx.worker_ledgers[rejecting].entries(),
        );
        probes += 1;
        let probe = driver::stream(
            &http,
            &base,
            "toy",
            "finalized-stream",
            &query(FROM, TO),
            &format!("stale-recovery-{probes}"),
        )
        .await?;
        ensure!(
            probe.status == 200,
            "recovery probe {probes} failed with {} — {}",
            probe.status,
            String::from_utf8_lossy(&probe.body),
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Ok(())
}

/// Index of the (single) worker whose ledger recorded the injected
/// `StaleEnvelope` rejection.
fn rejecting_worker(fx: &Fixture) -> anyhow::Result<usize> {
    let hits: Vec<usize> = fx
        .worker_ledgers
        .iter()
        .enumerate()
        .filter(|(_, l)| {
            l.entries()
                .iter()
                .any(|e| e.starts_with("query ") && e.contains("fault=StaleEnvelope"))
        })
        .map(|(i, _)| i)
        .collect();
    anyhow::ensure!(
        hits.len() == 1,
        "exactly one worker must have recorded the StaleEnvelope rejection, found {}",
        hits.len()
    );
    Ok(hits[0])
}

/// Queries this worker answered normally: no scripted fault, and fresh enough
/// to pass the admission check (the stub logs `lag_ms` per query).
fn fresh_serves(ledger: &harness::stubs::Ledger) -> usize {
    ledger
        .entries()
        .iter()
        .filter(|e| e.starts_with("query ") && e.contains("fault=none"))
        .count()
}

/// The timing side of the incident, documented at its honest boundary.
///
/// In production the >60s delay arose between signing a query and the worker
/// admitting it, while the query waited for a congestion-scheduler slot. The
/// fix moves the permit acquisition *ahead* of `prepare_query` (`query_worker`,
/// client.rs), so however long `acquire` queues, the timestamp is stamped and
/// signed only once a slot is granted — queue time can no longer age the
/// signature. The permit still covers the transport send, exactly as before.
///
/// ## Why the >60s queue itself is not inducible in this harness
///
/// Reproducing the wait needs the single congestion slot held continuously for
/// >60s while another query waits on it. The permit lifetimes make that
/// unreachable with an atomic-response stub:
///
/// 1. **A stalled worker holds no slot.** The send permit is released once the
///    query bytes are written; `receive_first_byte` is *intentionally not*
///    permit-gated (an explicit comment in client.rs), so a worker that
///    withholds bytes parks there while the window stays free.
/// 2. **The download permit needs bytes in flight.** The only long-lived permit
///    is held across an *in-progress* body read (`read_response_with_permits`),
///    and the worker actor answers atomically via a `ResponseChannel` — there
///    is no "first byte then stall", so body reads are one fast pass on
///    loopback.
/// 3. **The tiny pool fails closed first.** With `max_queries_per_worker: 1`, a
///    stalled worker's lease exhausts the two-worker pool, so a concurrent
///    request fails at worker selection with a retriable `no_workers` (503)
///    long before any congestion permit is involved.
/// 4. **The stall is bypassed anyway.** The default `retries: 1` plus the 1s
///    request-timeout floor fire a speculative retry that gets a fresh answer.
///
/// So this test pins the boundary: under a pinned single-slot window and a
/// >60s stall, a second request resolves promptly and the stale-timestamp 400
/// never appears. If a change to the permit lifetimes ever lets a stall hold
/// the slot, the latency guard flips — the cue to re-examine the sign-after-
/// acquire ordering end to end.
#[tokio::test(flavor = "multi_thread")]
async fn ct_stale_timestamp_congestion_queue_boundary() -> anyhow::Result<()> {
    // Single-slot window; transport + read timeouts long enough that, if a stall
    // *did* hold the slot, it would hold it well past 60s rather than being torn
    // down early.
    let tuning = Tuning::single_slot_congestion(120, 120);
    let mut fx = Fixture::start_tuned(ToyWorld::standard(), 2, tuning).await?;
    let result = run_congestion(&mut fx).await;
    fx.finish(result)
}

async fn run_congestion(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    let (base, http) = (fx.base.clone(), fx.http.clone());
    // Longer than the 60s freshness bound: were the slot actually held for the
    // stall, B would be queued past 60s before being stamped — which, after the
    // fix, still yields a fresh signature at send.
    let stall = Duration::from_secs(66);

    // Stream A holds a worker's response for `stall`, occupying whatever
    // scheduler slot it lands on. It queries chunk 1 (blocks 0..=39), off B's
    // range.
    fx.worker_faults.queue(WorkerFault::Stall(stall), 1);
    let base_a = base.clone();
    let http_a = http.clone();
    let a = tokio::spawn(async move {
        driver::stream(
            &http_a,
            &base_a,
            "toy",
            "finalized-stream",
            &query(0, 39),
            "congestion-A",
        )
        .await
    });

    // Issue B only once a worker ledger shows A consumed the Stall fault — a
    // fixed sleep would race on a loaded runner, letting B dequeue the stall
    // itself and quietly void the scenario.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let consumed = fx
                .worker_ledgers
                .iter()
                .any(|l| l.entries().iter().any(|e| e.contains("fault=Stall(")));
            if consumed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("stream A never consumed the Stall fault within 15s"))?;
    // A brief settle so A's attempt is parked in its first-byte wait.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let b_started_at = Instant::now();
    let b = driver::stream(
        &http,
        &base,
        "toy",
        "finalized-stream",
        &query(FROM, TO),
        "congestion-B",
    )
    .await?;
    let b_latency = b_started_at.elapsed();

    eprintln!(
        "[ct_stale_timestamp/congestion] B: status={} latency={:?} retry-after={:?} body={}",
        b.status,
        b_latency,
        b.header("retry-after"),
        String::from_utf8_lossy(&b.body),
    );
    let a_result = a.await.context("stream A join")??;
    eprintln!(
        "[ct_stale_timestamp/congestion] A: status={} body-bytes={} (stall was {}s)",
        a_result.status,
        a_result.body.len(),
        stall.as_secs(),
    );

    // The boundary: a stall does not hold the single slot (see the doc
    // comment), so B resolves promptly — as a fast availability failure or a
    // served response.
    ensure!(
        b_latency < Duration::from_secs(30),
        "B resolved in {b_latency:?}; a stall is not expected to hold the congestion slot. \
         If this now exceeds 60s the permit lifetimes changed — re-examine the \
         sign-after-acquire ordering (status was {}, body {})",
        b.status,
        String::from_utf8_lossy(&b.body),
    );
    // And in no case may the anti-replay rejection surface as the pre-fix
    // terminal 400 — retriable classification plus sign-after-acquire both
    // stand in the way.
    ensure!(
        !is_stale_timestamp_400(&b),
        "B surfaced the stale-timestamp 400 the fix removes: {}",
        String::from_utf8_lossy(&b.body),
    );
    Ok(())
}
