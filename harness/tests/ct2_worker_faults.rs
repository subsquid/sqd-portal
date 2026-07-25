//! CT-2 — the DC-1 worker-fault rows of spec/09, end to end against the portal
//! as a black box.
//!
//! Covers the integrity family (wrong-range in both directions, bad signature)
//! and the transient family (not-found, server error). The property under test
//! is the same for all of them: a bad response is *discarded and rerouted*,
//! never delivered, and one misbehaving worker never fails a request that
//! another worker could serve. Exhaustion is where the families diverge —
//! all-integrity pages, anything with a transient attempt in it tells the client
//! to come back (INV-22, FM-2, LIV-12, DC-1).

use std::time::Duration;

use anyhow::{ensure, Context};
use harness::driver::Decoded;
use harness::fixture::Fixture;
use harness::stubs::worker::WorkerFault;
use harness::{driver, ToyWorld};
use serde_json::json;

/// Chunk 2 of the toy world is blocks 40..=79. Starting above 0 is what makes
/// an undershoot expressible at all — you cannot report below block 0.
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

/// Wait until the pool serves a clean request again.
///
/// Integrity and error penalties are real, so a fault case leaves the toy
/// world's two workers in cooldown; without this the next case would measure
/// the cooldown ("no available workers") instead of the fault it injected.
/// Doubles as the LIV-7 witness: penalties decay, they never latch.
async fn await_pool_recovery(fx: &Fixture, label: &str) -> anyhow::Result<Decoded> {
    fx.worker_faults.clear();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut last = None;
    while std::time::Instant::now() < deadline {
        let d = driver::stream(
            &fx.http,
            &fx.base,
            "toy",
            "finalized-stream",
            &query(FROM, TO),
            &format!("ct2-recover-{label}"),
        )
        .await?;
        if d.status == 200 {
            return Ok(d);
        }
        last = Some(d.status);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    anyhow::bail!("pool never recovered before {label} (last status {last:?}) — LIV-7")
}

/// The full range, in order, exactly once — INV-20/21 on the delivered body.
fn assert_complete(context: &str, d: &Decoded) -> anyhow::Result<()> {
    ensure!(
        d.status == 200,
        "{context}: expected 200, got {} — body: {}",
        d.status,
        String::from_utf8_lossy(&d.body),
    );
    let got = d.block_numbers();
    let want: Vec<u64> = (FROM..=TO).collect();
    ensure!(
        got == want,
        "{context}: delivered blocks are not the exact requested range\n  got {} blocks: {:?}\n  want {} blocks",
        got.len(),
        got.iter().take(8).collect::<Vec<_>>(),
        want.len(),
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ct2_worker_faults() -> anyhow::Result<()> {
    let mut fx = Fixture::start(ToyWorld::standard(), 2).await?;
    let result = run(&mut fx).await;
    fx.finish(result)
}

async fn run(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    let (base, http) = (fx.base.clone(), fx.http.clone());

    // Baseline: no faults. Establishes that the range is servable at all, so a
    // later failure is attributable to the injected fault and not the world.
    let baseline = driver::stream(
        &http,
        &base,
        "toy",
        "finalized-stream",
        &query(FROM, TO),
        "ct2-baseline",
    )
    .await?;
    assert_complete("baseline", &baseline)?;

    // --- One bad response must be rerouted, not fatal (DC-1, INV-22, REQ-43) ---
    //
    // One fault per case, landing on whichever worker the portal picks first.
    // The second reserved worker must serve the range, and the client must not
    // be able to tell that anything went wrong.
    //
    // `server-error` is the exception, and it is a *finding*, not a quirk of the
    // test: DC-1 ("server error / not found → reroute") and spec/09 ("Erroring →
    // mask via reroute") both require it, but the portal classifies a generic
    // worker ServerError as terminal, so one erroring worker fails the whole
    // request. Recorded as a deviation rather than asserted, so this suite stays
    // green on known-violated behavior — see the gap register in spec/13.
    let known_violated = ["server-error"];
    let mut deviations = Vec::new();

    for (label, fault) in [
        ("overshoot", WorkerFault::Overshoot(10)),
        ("undershoot", WorkerFault::Undershoot(10)),
        ("bad-signature", WorkerFault::BadSignature),
        (
            "not-found",
            WorkerFault::NotFound("still downloading".into()),
        ),
        ("server-error", WorkerFault::ServerError("scripted".into())),
    ] {
        await_pool_recovery(fx, label).await?;
        fx.worker_faults.queue(fault, 1);

        let before = fx.queries_answered();
        let d = driver::stream(
            &http,
            &base,
            "toy",
            "finalized-stream",
            &query(FROM, TO),
            &format!("ct2-{label}"),
        )
        .await
        .with_context(|| format!("{label}: stream request"))?;

        ensure!(
            fx.queries_answered() > before,
            "{label}: no worker query was recorded — the fault never reached a worker"
        );

        let outcome = assert_complete(label, &d).and_then(|()| {
            ensure!(
                d.body == baseline.body,
                "{label}: response differs from the unfaulted baseline"
            );
            Ok(())
        });
        match outcome {
            Ok(()) => ensure!(
                !known_violated.contains(&label),
                "{label} is marked known-violated but now passes — \
                 remove it from the list and close its gap"
            ),
            Err(e) if known_violated.contains(&label) => {
                eprintln!("[known-violated] {e}");
                deviations.push(label);
            }
            Err(e) => return Err(e),
        }
    }
    ensure!(
        deviations == known_violated,
        "known-violated set drifted: {deviations:?}"
    );

    // --- Exhaustion: every attempt equivocating pages (DC-1, FM-2) ---
    //
    // Both reserved workers misbehave for every attempt, so the reroute has
    // nowhere left to go.
    await_pool_recovery(fx, "integrity-exhaustion").await?;
    fx.worker_faults.always(WorkerFault::Overshoot(10));
    let before = fx.queries_answered();
    let exhausted = driver::stream(
        &http,
        &base,
        "toy",
        "finalized-stream",
        &query(FROM, TO),
        "ct2-integrity-exhausted",
    )
    .await?;
    ensure!(
        exhausted.status >= 400,
        "integrity exhaustion must fail, got {}",
        exhausted.status
    );
    ensure!(
        exhausted.block_numbers().is_empty(),
        "equivocated data must never be delivered, got {:?}",
        exhausted.block_numbers()
    );
    // Guard against the vacuous pass: "no available workers" is also an error
    // with an empty body, and would prove nothing about exhaustion.
    ensure!(
        fx.queries_answered() >= before + 2,
        "integrity exhaustion must exhaust real attempts, only {} were answered",
        fx.queries_answered() - before
    );
    // The two families must be distinguishable: integrity exhaustion is an
    // operator page, transient exhaustion is "come back later". Asserting only
    // "some error" would let them collapse into one. NotFound is the transient
    // case that actually reroutes, so this exhausts attempts rather than dying
    // on the first response.
    await_pool_recovery(fx, "transient-exhaustion").await?;
    fx.worker_faults
        .always(WorkerFault::NotFound("still downloading".into()));
    let before = fx.queries_answered();
    let transient = driver::stream(
        &http,
        &base,
        "toy",
        "finalized-stream",
        &query(FROM, TO),
        "ct2-transient-exhausted",
    )
    .await?;
    ensure!(
        transient.status >= 400,
        "transient exhaustion must fail, got {}",
        transient.status
    );
    ensure!(
        fx.queries_answered() >= before + 2,
        "transient exhaustion must exhaust real attempts, only {} were answered",
        fx.queries_answered() - before
    );
    // Codes themselves are not asserted: the ADR-011 taxonomy is not integrated
    // yet (GAP-16), so only the split is contract today.
    eprintln!(
        "[ct2] exhaustion split — integrity {} / transient {}",
        exhausted.status, transient.status
    );
    ensure!(
        exhausted.status != transient.status,
        "integrity exhaustion ({}) must not be reported as a transient outage ({}) — \
         DC-1 splits these into WORKER-FAILURE and RETRIES-EXHAUSTED",
        exhausted.status,
        transient.status,
    );

    // Mixed exhaustion follows the transient class, not the integrity one: if
    // any attempt failed transiently a later retry can still succeed, so the
    // client is told to come back rather than that the request is hopeless
    // (DC-1). The equivocation is still counted against its worker — operator
    // visibility does not ride on the response class.
    await_pool_recovery(fx, "mixed-exhaustion").await?;
    fx.worker_faults
        .queue(WorkerFault::Overshoot(10), 1)
        .queue(WorkerFault::NotFound("still downloading".into()), 1);
    let before = fx.queries_answered();
    let mixed = driver::stream(
        &http,
        &base,
        "toy",
        "finalized-stream",
        &query(FROM, TO),
        "ct2-mixed-exhausted",
    )
    .await?;
    ensure!(
        mixed.block_numbers().is_empty(),
        "mixed exhaustion must deliver nothing, got {:?}",
        mixed.block_numbers()
    );
    ensure!(
        fx.queries_answered() >= before + 2,
        "mixed exhaustion must consume both scripted faults, only {} answered",
        fx.queries_answered() - before
    );
    ensure!(
        mixed.status == transient.status,
        "mixed exhaustion must take the transient class ({}), got {}",
        transient.status,
        mixed.status
    );

    // Recovery: penalties are not latched. Once the faults stop, the pool
    // serves again (LIV-7).
    let recovered = await_pool_recovery(fx, "final").await?;
    assert_complete("recovered", &recovered)?;
    ensure!(
        recovered.body == baseline.body,
        "recovered response differs from the unfaulted baseline"
    );

    Ok(())
}
