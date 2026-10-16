//! CT-12 — usage pacing, against the portal as a black box (REQ-70..REQ-75,
//! ADR-018).
//!
//! The usage claim decides speed, not access: a version-2 grant admits whatever
//! its usage claim looks like, and a bad claim is read as absent and counted —
//! except where authorization runs in shadow, where the count would publish the
//! verdict shadow mode withholds (REQ-55).
//!
//! In enforce a grant's usage reaches the client as IB-10's headers, readable
//! cross-origin; in log_only nothing reaches it. Both count the bytes of every
//! response admitted with a rate, by state and source (OB-16).
//!
//! What a black box cannot show is unit-tested beside the code: the toy world's
//! responses are far smaller than one second of the slowest rate a claim may
//! carry, so none of them waits, and the age limit is five minutes. The byte
//! curve, the wakes, log_only's waits against enforce's sleeps and a body
//! arriving whole under its length are in `auth::pacing`; the age limit in the
//! stream controller; the terms the gate fixes at admission in `auth::extractor`.

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
const BYTES: &str = "portal_limit_bytes_total";

/// IB-10, in the order the binding lists them.
const USAGE_HEADERS: [&str; 6] = [
    "x-sqd-usage-state",
    "x-sqd-usage-limit-bytes",
    "x-sqd-usage-used-bytes",
    "x-sqd-usage-reset",
    "x-sqd-usage-floor-bytes-per-sec",
    "x-sqd-usage-as-of",
];

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

fn query() -> Value {
    json!({
        "type": "evm",
        "fromBlock": 0,
        "toBlock": 39,
        "includeAllBlocks": true,
        "fields": { "block": { "number": true, "hash": true } },
    })
}

/// One chunk of the toy world's archival dataset.
async fn stream(fx: &Fixture, key_id: &str) -> anyhow::Result<Decoded> {
    driver::stream_as(
        &fx.http,
        &fx.base,
        "toy",
        "finalized-stream",
        &query(),
        key_id,
        Some(&bearer(key_id)),
    )
    .await
}

/// The real-time-only dataset, served by the proxy.
async fn real_time(fx: &Fixture, key_id: &str) -> anyhow::Result<Decoded> {
    driver::stream_as(
        &fx.http,
        &fx.base,
        "toy-rt",
        "stream",
        &query(),
        key_id,
        Some(&bearer(key_id)),
    )
    .await
}

fn usage_headers(d: &Decoded) -> Vec<(&'static str, Option<String>)> {
    USAGE_HEADERS
        .iter()
        .map(|name| (*name, d.header(name).map(str::to_owned)))
        .collect()
}

fn bytes_counted(scrape: &str, state: &str, source: &str) -> f64 {
    sum_where(scrape, BYTES, &[("state", state), ("source", source)])
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
    fx.cp().answer("v2", Answer::GrantV2 { usage: usage() });
    let d = stream(fx, "v2").await?;
    ensure!(
        usage_headers(&d).iter().all(|(_, value)| value.is_none()),
        "REQ-73: no usage header where authorization runs in shadow"
    );
    ensure!(
        fx.cp().exchanges() == bad_claims().len() + 1,
        "the claims must have been exchanged for the silence to mean anything"
    );
    let scrape = fx.scrape().await?;
    ensure!(
        sum_where(&scrape, BAD_CLAIMS, &[]) == 0.0 && sum_where(&scrape, BYTES, &[]) == 0.0,
        "OB-16/REQ-55: pacing must not count where authorization runs in shadow"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ct12_enforce_tells_the_client_its_usage() -> anyhow::Result<()> {
    let auth = Auth::new(PORTAL_ID)
        .limit("exchange_rate_per_sec", 1000)
        .pacing("enforce", false);
    let mut fx = Fixture::start_with_auth(ToyWorld::standard(), 2, auth).await?;
    let result = enforce(&mut fx).await;
    fx.finish(result)
}

async fn enforce(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    fx.cp().answer("within", Answer::GrantV2 { usage: usage() });
    let mut unmetered = usage();
    for (field, value) in [
        ("state", json!("unmetered")),
        ("stream_bytes_per_sec", Value::Null),
        ("floor_bytes_per_sec", Value::Null),
        ("allowance_bytes", Value::Null),
        ("as_of", Value::Null),
    ] {
        unmetered[field] = value;
    }
    fx.cp()
        .answer("unmetered", Answer::GrantV2 { usage: unmetered });
    fx.cp().answer("v1", Answer::grant());

    // REQ-73 / IB-10 — every header with a value, from the grant.
    let d = stream(fx, "within").await?;
    ensure!(
        d.status == 200 && d.block_numbers().len() == 40,
        "within: got {}",
        d.status
    );
    let want = |values: [Option<&str>; 6]| -> Vec<(&'static str, Option<String>)> {
        USAGE_HEADERS
            .iter()
            .zip(values)
            .map(|(name, value)| (*name, value.map(str::to_owned)))
            .collect()
    };
    ensure!(
        usage_headers(&d)
            == want([
                Some("within"),
                Some("400000000000"),
                Some("1000000"),
                Some("2100-01-01T00:00:00Z"),
                Some("500000"),
                Some("2099-12-31T23:46:40Z"),
            ]),
        "IB-10: got {:?}",
        usage_headers(&d)
    );
    let unmetered = stream(fx, "unmetered").await?;
    ensure!(
        usage_headers(&unmetered)
            == want([
                Some("unmetered"),
                None,
                Some("1000000"),
                Some("2100-01-01T00:00:00Z"),
                None,
                None,
            ]),
        "IB-10: a header with no value is omitted, got {:?}",
        usage_headers(&unmetered)
    );
    let v1 = stream(fx, "v1").await?;
    ensure!(
        usage_headers(&v1).iter().all(|(_, value)| value.is_none()),
        "REQ-73: a v1 grant carries no usage header"
    );
    fx.cp().answer("bad-rate", bad_claims().remove(2).1);
    let bad = stream(fx, "bad-rate").await?;
    ensure!(
        observable(&bad) == observable(&v1),
        "REQ-70: a bad claim is served as a v1 grant"
    );

    // IB-1 — readable by a browser.
    let response = fx
        .http
        .post(format!("{}/datasets/toy/finalized-stream", fx.base))
        .header("origin", "https://app.example.com")
        .header("authorization", bearer("within"))
        .body(query().to_string())
        .send()
        .await?;
    let exposed = response
        .headers()
        .get("access-control-expose-headers")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    for name in USAGE_HEADERS {
        ensure!(
            exposed.contains(name),
            "IB-1: {name} is not exposed: {exposed}"
        );
    }
    response.bytes().await?;

    // OB-16 — the bytes of every response admitted with a rate, by state and
    // source; an error is not wrapped but still tells the client its usage.
    let rt = real_time(fx, "within").await?;
    ensure!(
        rt.status == 200 && rt.header("x-sqd-data-source") == Some("real_time"),
        "real-time: got {} from {:?}",
        rt.status,
        rt.header("x-sqd-data-source")
    );
    ensure!(
        rt.header("x-sqd-usage-state") == Some("within"),
        "IB-10: real-time"
    );
    let before = fx.scrape().await?;
    let refused = driver::stream_as(
        &fx.http,
        &fx.base,
        "toy",
        "finalized-stream",
        &json!({"type": "not-a-chain"}),
        "bad-query",
        Some(&bearer("within")),
    )
    .await?;
    ensure!(refused.status == 400, "a bad query: got {}", refused.status);
    ensure!(
        refused.header("x-sqd-usage-state") == Some("within"),
        "IB-10: the headers ride every admitted response, whatever its status"
    );
    let scrape = fx.scrape().await?;
    // The CORS request above streamed the same body as `d`.
    let network = 2.0 * d.encoded_len() as f64;
    ensure!(
        bytes_counted(&scrape, "within", "network") == network
            && bytes_counted(&before, "within", "network") == network,
        "OB-16: expected {network} network bytes, counted {} (an error must add none)",
        bytes_counted(&scrape, "within", "network")
    );
    ensure!(
        bytes_counted(&scrape, "within", "real_time") == rt.encoded_len() as f64,
        "OB-16: expected {} real-time bytes, counted {}",
        rt.encoded_len(),
        bytes_counted(&scrape, "within", "real_time")
    );
    ensure!(
        sum_where(&scrape, BYTES, &[("state", "unmetered")]) == 0.0,
        "OB-16: a response admitted without a rate is not wrapped"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ct12_log_only_serves_what_off_serves() -> anyhow::Result<()> {
    let auth = Auth::new(PORTAL_ID).pacing("log_only", true);
    let mut fx = Fixture::start_with_auth(ToyWorld::standard(), 2, auth).await?;
    let result = log_only(&mut fx).await;
    fx.finish(result)
}

async fn log_only(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    fx.cp().answer("v1", Answer::grant());
    fx.cp().answer("v2", Answer::GrantV2 { usage: usage() });

    // REQ-74 — the same status, headers and bytes as a grant nothing paces,
    // and the byte signal moves.
    for (source, v1, v2) in [
        ("network", stream(fx, "v1").await?, stream(fx, "v2").await?),
        (
            "real_time",
            real_time(fx, "v1").await?,
            real_time(fx, "v2").await?,
        ),
    ] {
        ensure!(v1.status == 200, "{source}: got {}", v1.status);
        ensure!(
            observable(&v1) == observable(&v2),
            "REQ-74: log_only must serve what off serves on {source}"
        );
        let counted = bytes_counted(&fx.scrape().await?, "within", source);
        ensure!(
            counted == v2.encoded_len() as f64,
            "OB-16: expected {} {source} bytes in log_only, counted {counted}",
            v2.encoded_len()
        );
    }
    Ok(())
}
