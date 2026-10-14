//! CT-12 — usage pacing, against the portal as a black box (REQ-70..REQ-75,
//! ADR-018).
//!
//! The usage claim decides speed, not access: a version-2 grant admits whatever
//! its usage claim looks like, and a bad claim is read as absent and counted —
//! except where authorization runs in shadow, where the count would publish the
//! verdict shadow mode withholds (REQ-55).

use std::time::Duration;

use anyhow::ensure;
use harness::driver::Decoded;
use harness::fixture::Fixture;
use harness::metrics_audit::sum_where;
use harness::portal::Auth;
use harness::stubs::control_plane::Answer;
use harness::{driver, ToyWorld};
use serde_json::{json, Value};

const PORTAL_ID: &str = "portal-harness-eu";
const BAD_CLAIMS: &str = "portal_limit_bad_usage_claims_total";

fn bearer(key_id: &str) -> String {
    format!("Bearer sqd_portal_{key_id}_s3cr3tvalue-{key_id}")
}

/// The claim portal-api issues to an organization within its allowance.
fn usage() -> Value {
    json!({
        "state": "within",
        "stream_bytes_per_sec": 20_000_000,
        "floor_bytes_per_sec": 500_000,
        "allowance_bytes": 400_000_000_000u64,
        "used_bytes": 1_000_000u64,
        "period_end": 4_102_444_800u64,
        "as_of": 4_102_444_000u64,
    })
}

fn usage_with(field: &str, value: Value) -> Value {
    let mut claim = usage();
    claim[field] = value;
    claim
}

/// One chunk of the toy world's archival dataset.
async fn stream(fx: &Fixture, key_id: &str) -> anyhow::Result<Decoded> {
    driver::stream_as(
        &fx.http,
        &fx.base,
        "toy",
        "finalized-stream",
        &json!({
            "type": "evm",
            "fromBlock": 0,
            "toBlock": 39,
            "includeAllBlocks": true,
            "fields": { "block": { "number": true, "hash": true } },
        }),
        key_id,
        Some(&bearer(key_id)),
    )
    .await
}

/// Status, headers and body, less the two that differ per request.
fn observable(d: &Decoded) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let mut headers: Vec<_> = d
        .headers
        .iter()
        .filter(|(name, _)| name.as_str() != "x-request-id" && name.as_str() != "date")
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    headers.sort();
    (d.status, headers, d.body.clone())
}

/// Every claim a v2 grant may carry that is read as absent (REQ-70).
fn bad_claims() -> Vec<(&'static str, Answer)> {
    vec![
        (
            "bad-missing",
            Answer::Raw(json!({"result": "granted", "grant": {
                "claims_version": 2,
                "key_id": "bad-missing",
                "refresh_after": 4_102_444_800u64,
                "expires_at": 4_102_444_800u64,
            }})),
        ),
        (
            "bad-state",
            Answer::GrantV2 {
                usage: usage_with("state", json!("slowed")),
            },
        ),
        (
            "bad-rate",
            Answer::GrantV2 {
                usage: usage_with("stream_bytes_per_sec", json!(65_535)),
            },
        ),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn ct12_a_usage_claim_decides_speed_not_access() -> anyhow::Result<()> {
    let auth = Auth::new(PORTAL_ID).limit("exchange_rate_per_sec", 1000);
    let mut fx = Fixture::start_with_auth(ToyWorld::standard(), 2, auth).await?;
    let result = claims(&mut fx).await;
    fx.finish(result)
}

async fn claims(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    fx.cp().answer("v1", Answer::grant());
    fx.cp().answer("v2", Answer::GrantV2 { usage: usage() });

    // REQ-70 — with pacing off, a v2 grant is served exactly as a v1 one.
    let v1 = stream(fx, "v1").await?;
    let v2 = stream(fx, "v2").await?;
    ensure!(v1.status == 200, "v1 grant: got {}", v1.status);
    ensure!(
        observable(&v1) == observable(&v2),
        "REQ-70: with pacing off a v2 grant must be served as a v1 grant"
    );
    ensure!(
        sum_where(&fx.scrape().await?, BAD_CLAIMS, &[]) == 0.0,
        "OB-16: a good claim is not a bad one"
    );

    // A bad claim admits, unpaced, and is counted once per grant.
    let bad = bad_claims();
    for (key, answer) in &bad {
        fx.cp().answer(key, answer.clone());
        let d = stream(fx, key).await?;
        ensure!(
            observable(&d) == observable(&v1),
            "REQ-70: {key} must be served as a v1 grant, got {}",
            d.status
        );
    }
    let counted = sum_where(&fx.scrape().await?, BAD_CLAIMS, &[]);
    ensure!(
        counted == bad.len() as f64,
        "OB-16: expected {} bad usage claims, counted {counted}",
        bad.len()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ct12_shadow_authorization_counts_no_bad_claim() -> anyhow::Result<()> {
    let auth = Auth::new(PORTAL_ID)
        .enforcement("log_only")
        .pacing("enforce", true);
    let mut fx = Fixture::start_with_auth(ToyWorld::standard(), 2, auth).await?;
    let result = shadow(&mut fx).await;
    fx.finish(result)
}

async fn shadow(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    for (key, answer) in bad_claims() {
        fx.cp().answer(key, answer);
        let d = stream(fx, key).await?;
        ensure!(d.status == 200, "shadow {key}: got {}", d.status);
    }
    ensure!(
        fx.cp().exchanges() == bad_claims().len(),
        "the claims must have been exchanged for the silence to mean anything"
    );
    ensure!(
        sum_where(&fx.scrape().await?, BAD_CLAIMS, &[]) == 0.0,
        "OB-16/REQ-55: a bad claim must not be counted where authorization runs in shadow"
    );
    Ok(())
}
