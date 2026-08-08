//! CT-10 — authorization, end to end against the portal as a black box.
//!
//! The credential corpus and the DC-8 fault rows, driven through the real
//! middleware stack against a control-plane stub that verifies the exchange
//! signature for real. Five portals, because the properties differ by
//! configuration: enforcing, shadow, no `auth:` block at all, one whose
//! exchange budget is small enough to saturate, and one signing with a
//! dedicated key rather than its network identity.
//!
//! The claims that need a ledger rather than a response are the reason this
//! class exists at all (GAP-33): that a refusal costs no dependency call
//! (INV-14), that the presented secret reaches the exchange and nothing else
//! (INV-38), and that the keyless scrape separates no two credentials
//! (INV-39).

use std::time::Duration;

use anyhow::{ensure, Context};
use harness::driver::Decoded;
use harness::fixture::Fixture;
use harness::metrics_audit::{samples, sum_where};
use harness::portal::Auth;
use harness::stubs::control_plane::Answer;
use harness::{driver, ToyWorld};
use serde_json::json;

const PORTAL_ID: &str = "portal-harness-eu";

// Registered under the `portal` sub-registry, so the exposed family carries
// that prefix (IB-6).
const AUTH_DECISIONS: &str = "portal_auth_decisions";
const EXCHANGES: &str = "portal_auth_exchanges";
const LIFETIMES_CAPPED: &str = "portal_auth_grant_lifetimes_capped";

/// Every OB-13 signal shadow mode must hold still. They are registered for the
/// process rather than per mode, so they exist at zero on a shadow and on a
/// portal without authorization alike — the rule is that they must not *move*.
const OB13_SIGNALS: &[&str] = &[
    "portal_auth_grant_cache_entries",
    "portal_auth_grant_cache_evictions",
    "portal_auth_grant_lifetimes_capped",
    "portal_auth_grace_admissions",
    "portal_auth_grants_in_grace",
    "portal_auth_grace_min_remaining_seconds",
    "portal_auth_exchange_duration_seconds_count",
    "portal_auth_exchange_success_age_seconds",
];

/// Present in every secret this suite mints, so one substring search covers the
/// whole corpus.
const SECRET_MARKER: &str = "s3cr3tvalue";

/// Distinctive enough to grep a whole log and a whole scrape for (INV-38), and
/// inside the IB-9 grammar so it is a token the control plane could have minted.
fn token(key_id: &str) -> String {
    format!("sqd_portal_{key_id}_{SECRET_MARKER}-{key_id}-nevertobelogged")
}

/// REQ-52 puts the error body on the list of places a secret may not reach, so
/// every response this suite reads is checked as it is read.
fn assert_body_carries_no_secret(context: &str, d: &Decoded) -> anyhow::Result<()> {
    ensure!(
        !String::from_utf8_lossy(&d.body).contains(SECRET_MARKER),
        "REQ-52/INV-38: {context}: the response body echoed the presented secret",
    );
    Ok(())
}

fn bearer(key_id: &str) -> String {
    format!("Bearer {}", token(key_id))
}

/// One chunk of the toy world, so an admitted request is cheap.
fn query() -> serde_json::Value {
    json!({
        "type": "evm",
        "fromBlock": 0,
        "toBlock": 39,
        "includeAllBlocks": true,
        "fields": { "block": { "number": true, "hash": true } },
    })
}

async fn gated(
    fx: &Fixture,
    dataset: &str,
    auth: Option<&str>,
    request_id: &str,
) -> anyhow::Result<Decoded> {
    driver::stream_as(
        &fx.http,
        &fx.base,
        dataset,
        "finalized-stream",
        &query(),
        request_id,
        auth,
    )
    .await
}

/// The ADR-011 code and type, after validator 6 has passed on the envelope
/// carrying them — which is where the IB-5 status binding and the no-hint rule
/// are checked.
fn envelope(context: &str, d: &Decoded) -> anyhow::Result<(String, String)> {
    let verdict = harness::validators::validate_error(d);
    ensure!(verdict.errors.is_empty(), "{context}: {:?}", verdict.errors);
    let body: serde_json::Value = serde_json::from_slice(&d.body)
        .with_context(|| format!("{context}: error body is not JSON"))?;
    Ok((
        body["error"]["code"]
            .as_str()
            .with_context(|| format!("{context}: validated envelope has a code"))?
            .to_owned(),
        body["error"]["type"]
            .as_str()
            .with_context(|| format!("{context}: validated envelope has a type"))?
            .to_owned(),
    ))
}

/// One refusal, checked whole: status, envelope, code and type.
async fn assert_refused(
    fx: &Fixture,
    context: &str,
    auth: Option<&str>,
    want_status: u16,
    want_code: &str,
    want_type: &str,
) -> anyhow::Result<Decoded> {
    let d = gated(fx, "toy", auth, context).await?;
    ensure!(
        d.status == want_status,
        "{context}: expected {want_status}, got {} — body: {}",
        d.status,
        String::from_utf8_lossy(&d.body),
    );
    let (code, error_type) = envelope(context, &d)?;
    ensure!(
        code == want_code && error_type == want_type,
        "{context}: expected {want_type}/{want_code}, got {error_type}/{code}",
    );
    assert_body_carries_no_secret(context, &d)?;
    Ok(d)
}

async fn assert_served(
    fx: &Fixture,
    context: &str,
    dataset: &str,
    auth: Option<&str>,
) -> anyhow::Result<()> {
    let d = gated(fx, dataset, auth, context).await?;
    ensure!(
        d.status == 200,
        "{context}: expected 200, got {} — body: {}",
        d.status,
        String::from_utf8_lossy(&d.body),
    );
    assert_body_carries_no_secret(context, &d)?;
    let got = d.block_numbers();
    let want: Vec<u64> = (0..=39).collect();
    ensure!(
        got == want,
        "{context}: delivered {} blocks, wanted 40",
        got.len()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The enforcing portal: the credential corpus, the ladder, and the fault rows.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn ct10_enforcing_gate() -> anyhow::Result<()> {
    // The rate bound is CT-10's own subject in `ct10_exchange_budget_saturates`;
    // here it is raised out of the way so a case never measures the budget
    // instead of the rung it injected.
    let auth = Auth::new(PORTAL_ID).limit("exchange_rate_per_sec", 1000);
    let mut fx = Fixture::start_with_auth(ToyWorld::standard(), 2, auth).await?;
    let result = enforcing(&mut fx).await;
    fx.finish(result)
}

async fn enforcing(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;

    // INV-14 — a refusal costs no dependency call. The ledgers are the whole
    // point: "the portal answered 403" says nothing about what it spent doing
    // it, and this rung must spend nothing.
    let workers_before = fx.queries_answered();
    let hotblocks_before = fx.hotblocks_ledger.entries().len();
    assert_refused(
        fx,
        "no credential",
        None,
        403,
        "missing_credential",
        "authentication_error",
    )
    .await?;
    // The same refusal against the real-time dataset, so the hotblocks half of
    // the ledger claim below is about a source this request would otherwise
    // have reached — `toy` has no real-time attachment, and asserting a source
    // it never uses would pass for the wrong reason.
    let d = driver::stream_as(
        &fx.http,
        &fx.base,
        "toy-rt",
        "finalized-stream",
        &query(),
        "no credential on the real-time dataset",
        None,
    )
    .await?;
    ensure!(
        d.status == 403,
        "INV-14: expected 403 on the real-time dataset, got {}",
        d.status,
    );
    ensure!(
        fx.cp().exchanges() == 0,
        "INV-14: an absent credential must not reach the control plane, saw {:?}",
        fx.cp().ledger.entries(),
    );
    ensure!(
        fx.queries_answered() == workers_before
            && fx.hotblocks_ledger.entries().len() == hotblocks_before,
        "INV-14: a refused request must not reach a serving dependency",
    );

    // IB-9 grammar — the cheapest rung runs first, so arbitrary client bytes
    // never buy a control-plane call (REQ-52, PF-7).
    let oversized_key = "k".repeat(65);
    let oversized_secret = "s".repeat(129);
    let ungrammatical = [
        ("wrong prefix", "Bearer nope_k1_secret".to_owned()),
        ("no separator", "Bearer sqd_portal_k1secret".to_owned()),
        ("bad charset", "Bearer sqd_portal_k1_bad!secret".to_owned()),
        ("empty secret", "Bearer sqd_portal_k1_".to_owned()),
        (
            "oversized key id",
            format!("Bearer sqd_portal_{oversized_key}_secret"),
        ),
        (
            "oversized secret",
            format!("Bearer sqd_portal_k1_{oversized_secret}"),
        ),
        ("wrong scheme", format!("Basic {}", token("k1"))),
        ("no scheme", token("k1")),
        ("embedded space", format!("Bearer {} extra", token("k1"))),
    ];
    for (why, header) in &ungrammatical {
        assert_refused(
            fx,
            &format!("ungrammatical: {why}"),
            Some(header),
            403,
            "invalid_credential",
            "authentication_error",
        )
        .await?;
    }
    ensure!(
        fx.cp().exchanges() == 0,
        "REQ-52: an ungrammatical token must not reach the control plane, saw {:?}",
        fx.cp().ledger.entries(),
    );

    // IB-9 offers the header channel and no other: a token in the query string
    // is not a credential, and the request is refused as having none.
    let d = driver::stream_as(
        &fx.http,
        &fx.base,
        "toy",
        &format!("finalized-stream?api_key={}", token("k1")),
        &query(),
        "query-string channel",
        None,
    )
    .await?;
    ensure!(
        d.status == 403,
        "IB-9: a credential in the query string must not admit a request, got {}",
        d.status,
    );
    let (code, _) = envelope("query-string channel", &d)?;
    ensure!(
        code == "missing_credential",
        "IB-9: a query-string token is not a credential at all, got {code}",
    );

    // LIV-14 — a freshly minted key is served on the request that presents it,
    // with no bootstrap to wait for, and costs exactly one exchange.
    fx.cp().answer("k1", Answer::grant());
    assert_served(
        fx,
        "first request on a fresh key",
        "toy",
        Some(&bearer("k1")),
    )
    .await?;
    ensure!(
        fx.cp().exchanges() == 1,
        "LIV-14: the first request should have cost exactly one exchange, saw {:?}",
        fx.cp().ledger.entries(),
    );

    // ...and the second is answered locally: the exchange is on the miss, not
    // on the request (DC-8).
    assert_served(
        fx,
        "second request on the same key",
        "toy",
        Some(&bearer("k1")),
    )
    .await?;
    ensure!(
        fx.cp().exchanges() == 1,
        "DC-8: a cached grant must answer without a second exchange",
    );

    // INV-14 — a burst on one credential costs one exchange, not one per
    // request. The delay is what makes the burst genuinely overlap; without it
    // the first would finish and the rest would be plain cache hits.
    fx.cp().answer("burst", Answer::grant());
    fx.cp().set_delay(Duration::from_millis(300));
    let before = fx.cp().exchanges();
    // The timestamp lookup is gated too, and needs no worker — so a burst
    // measures the gate rather than the toy world's two-worker pool.
    let timestamp_url = format!("{}/datasets/toy/timestamps/1700000000/block", fx.base);
    let burst = futures::future::join_all((0..8).map(|_| {
        let (http, url, auth) = (fx.http.clone(), timestamp_url.clone(), bearer("burst"));
        async move { driver::get_as(&http, &url, Some(&auth)).await }
    }))
    .await;
    fx.cp().set_delay(Duration::ZERO);
    for (i, d) in burst.iter().enumerate() {
        let d = d.as_ref().map_err(|e| anyhow::anyhow!("burst {i}: {e}"))?;
        ensure!(
            d.status != 403,
            "burst {i}: a single-flighted exchange must not refuse anyone, got 403",
        );
    }
    ensure!(
        fx.cp().exchanges() == before + 1,
        "INV-14: a burst on one fingerprint must cost one exchange, cost {}",
        fx.cp().exchanges() - before,
    );

    // REQ-53 — each denial reason reaches the client as the code it maps to,
    // and an unrecognised one is still a denial, reported as the coarsest code.
    // Every rung answers 403, so the status never says which (INV-39).
    let denials = [
        ("unknown_key", "invalid_credential", "authentication_error"),
        (
            "invalid_secret",
            "invalid_credential",
            "authentication_error",
        ),
        ("revoked", "revoked_credential", "authentication_error"),
        ("expired", "expired_credential", "authentication_error"),
        (
            "portal_not_allowed",
            "portal_not_allowed",
            "permission_error",
        ),
        (
            "a_reason_from_a_newer_control_plane",
            "invalid_credential",
            "authentication_error",
        ),
    ];
    let mut bodies = Vec::new();
    for (i, (reason, code, error_type)) in denials.iter().enumerate() {
        let key = format!("deny{i}");
        fx.cp().answer(&key, Answer::Deny((*reason).to_owned()));
        let d = assert_refused(
            fx,
            &format!("denial: {reason}"),
            Some(&bearer(&key)),
            403,
            code,
            error_type,
        )
        .await?;
        bodies.push((*reason, d.body));
    }

    // INV-39 — the unknown key and the wrong secret answer identically. If they
    // did not, the endpoint would tell a guesser which of the two they got
    // right, which is the whole reason the six rungs share a status.
    let unknown = &bodies[0].1;
    let wrong_secret = &bodies[1].1;
    ensure!(
        unknown == wrong_secret,
        "INV-39: an unknown key and a wrong secret must be indistinguishable\n  {}\n  {}",
        String::from_utf8_lossy(unknown),
        String::from_utf8_lossy(wrong_secret),
    );

    // REQ-54 / DC-8 error table — an exchange that could not be *made* is not a
    // claim about anyone's key. Every one of these is retryable and attributed
    // to the dependency; none may answer 403, which would tell a customer whose
    // key is perfectly good to stop retrying.
    let unreadable = json!({"result": "granted", "grant": {}});
    let newer_claims = |key: &str| {
        json!({"result": "granted", "grant": {
            "claims_version": 2,
            "key_id": key,
            "refresh_after": 4_102_444_800u64,
            "expires_at": 4_102_444_800u64,
        }})
    };
    let wrong_subject = json!({"result": "granted", "grant": {
        "claims_version": 1,
        "key_id": "somebody-else",
        "refresh_after": 4_102_444_800u64,
        "expires_at": 4_102_444_800u64,
    }});
    let faults: Vec<(&str, Answer)> = vec![
        ("upstream 500", Answer::Status(500)),
        ("upstream 503", Answer::Status(503)),
        // Not an authoritative "no such key": a 404 is a routing accident, and
        // reading it as a verdict is how a half-deployed replica revokes a
        // customer (GAP-34).
        ("upstream 404", Answer::Status(404)),
        ("unreadable answer", Answer::Raw(unreadable)),
        (
            "unknown claims version",
            Answer::Raw(newer_claims("fault4")),
        ),
        ("answer about another key", Answer::Raw(wrong_subject)),
    ];
    for (i, (why, answer)) in faults.into_iter().enumerate() {
        let key = format!("fault{i}");
        fx.cp().answer(&key, answer);
        assert_refused(
            fx,
            &format!("exchange fault: {why}"),
            Some(&bearer(&key)),
            502,
            "upstream_unavailable",
            "availability_error",
        )
        .await?;
    }

    // The same row, reached by the deadline rather than by an answer: the
    // portal stops waiting before its caller does (ADR-010).
    fx.cp().set_delay(Duration::from_secs(10));
    assert_refused(
        fx,
        "exchange fault: past the deadline",
        Some(&bearer("slow")),
        502,
        "upstream_unavailable",
        "availability_error",
    )
    .await?;
    fx.cp().set_delay(Duration::ZERO);

    // An over-cap lifetime is honoured and shortened, not refused — and counted,
    // because a control plane drifting past the cap is a misconfiguration an
    // operator should see before it becomes an incident (REQ-54).
    let capped_before = sum_where(&fx.scrape().await?, LIFETIMES_CAPPED, &[]);
    fx.cp().answer(
        "overlong",
        Answer::Grant {
            datasets: None,
            refresh_in: 300,
            expires_in: 30 * 24 * 3600,
        },
    );
    assert_served(fx, "over-cap lifetime", "toy", Some(&bearer("overlong"))).await?;
    let capped_after = sum_where(&fx.scrape().await?, LIFETIMES_CAPPED, &[]);
    ensure!(
        capped_after > capped_before,
        "REQ-54: an over-cap lifetime must be counted, {capped_before} → {capped_after}",
    );

    // REQ-53 — dataset scope, the one rung the grant does not settle. The grant
    // names canonical datasets, so an alias has to be resolved before matching:
    // `toy-alias` is the same dataset and must be admitted.
    fx.cp().answer("scoped", Answer::grant_for(&["toy"]));
    assert_served(fx, "scoped: canonical name", "toy", Some(&bearer("scoped"))).await?;
    assert_served(
        fx,
        "scoped: alias of the same dataset",
        "toy-alias",
        Some(&bearer("scoped")),
    )
    .await?;
    let d = driver::stream_as(
        &fx.http,
        &fx.base,
        "toy-rt",
        "finalized-stream",
        &query(),
        "scoped: another dataset",
        Some(&bearer("scoped")),
    )
    .await?;
    ensure!(
        d.status == 403,
        "REQ-53: a dataset-scoped key must be refused elsewhere, got {}",
        d.status,
    );
    let (code, error_type) = envelope("scoped: another dataset", &d)?;
    ensure!(
        code == "dataset_not_allowed" && error_type == "permission_error",
        "REQ-53: expected permission_error/dataset_not_allowed, got {error_type}/{code}",
    );

    // An empty list is not an absent one: it names no dataset, and covers none.
    fx.cp().answer("empty-scope", Answer::grant_for(&[]));
    assert_refused(
        fx,
        "empty scope covers nothing",
        Some(&bearer("empty-scope")),
        403,
        "dataset_not_allowed",
        "permission_error",
    )
    .await?;

    // NG8 / INV-31 — the open surface stays open, and readiness never turns on
    // the control plane: every replica shares one authority, so withholding
    // readiness fleet-wide would answer an outage with an outage.
    for path in [
        "/ready",
        "/metrics",
        "/status",
        "/datasets",
        "/datasets/toy/head",
        "/datasets/toy/state",
        "/datasets/toy/finalized-head",
    ] {
        let d = driver::get(&fx.http, &format!("{}{path}", fx.base)).await?;
        ensure!(
            d.status == 200,
            "NG8: {path} must answer without a credential, got {}",
            d.status,
        );
    }

    // DC-8's signing contract, from the verifying side: every exchange so far
    // carried exactly one of each header, an attributable portal id, a timestamp
    // inside the skew window, and a signature over the canonical binding.
    ensure!(
        fx.cp().rejections().is_empty(),
        "DC-8: the control plane refused an exchange the portal sent: {:?}",
        fx.cp().rejections(),
    );

    // INV-38 — the secret's single egress. The stub holds every token it was
    // handed, which is what makes the negative meaningful: the credentials did
    // travel, and they travelled exactly once, to the exchange.
    let scrape = fx.scrape().await?;
    let log = fx.portal.log_all();
    let presented = fx.cp().tokens();
    // All three guards are the audit auditing itself: grepping an empty log, an
    // empty scrape, or for a secret that never travelled would pass for the
    // wrong reason.
    ensure!(
        !presented.is_empty(),
        "INV-38: the audit proves nothing if no credential ever reached the exchange",
    );
    ensure!(
        log.contains("authorization") && scrape.contains(AUTH_DECISIONS),
        "INV-38: the audit must run against a log and a scrape that carry auth activity",
    );
    // The key id is what a protected log is *allowed* to carry, so finding one
    // there confirms the grep reads the right bytes. Enforcing mode logs only
    // the requests it turned away, hence a refused key rather than an admitted
    // one — and quoted rather than as `key_id=…`, which the log's own escape
    // codes interrupt.
    ensure!(
        log.contains("\"deny0\""),
        "INV-38: the log audit is not reading the authorization records",
    );
    for secret in &presented {
        ensure!(
            !log.contains(secret.as_str()),
            "INV-38: a presented secret reached the log",
        );
        ensure!(
            !scrape.contains(secret.as_str()),
            "INV-38: a presented secret reached the scrape",
        );
    }

    // INV-39 / IB-9 — the keyless scrape names no key. Key ids are in the
    // protected log by design; `/metrics` is served to anyone.
    for key in ["k1", "burst", "scoped", "overlong"] {
        ensure!(
            !scrape.contains(&format!("\"{key}\"")),
            "INV-39: key id {key} appears in the keyless scrape",
        );
    }

    // ...and every refusal projects onto the wire code it actually sent, and
    // nothing finer: the six internal rungs collapse onto the codes above.
    let decision_labels: Vec<String> = samples(&scrape, AUTH_DECISIONS)
        .into_iter()
        .map(|(labels, _)| labels)
        .collect();
    ensure!(
        !decision_labels.is_empty(),
        "OB-12: an enforcing portal must publish its authorization decisions",
    );
    for labels in &decision_labels {
        ensure!(
            !labels.contains("unknown_key")
                && !labels.contains("invalid_secret")
                && !labels.contains("malformed")
                && !labels.contains("denied_unrecognized"),
            "INV-39: an internal rung reached the scrape: {labels}",
        );
    }

    // The exchange counter is coarse by construction: issued and refused are one
    // class, or two bracketed scrapes would read the verdict off the counter.
    let outcomes: Vec<String> = samples(&scrape, EXCHANGES)
        .into_iter()
        .map(|(labels, _)| labels)
        .collect();
    for labels in &outcomes {
        ensure!(
            labels.contains("answered")
                || labels.contains("saturated")
                || labels.contains("failed"),
            "OB-13: unexpected exchange outcome label: {labels}",
        );
    }

    // The unattributable row of DC-8's error table, driven from the far side:
    // a control plane that no longer recognises this portal fails every exchange
    // at once — and still never as a claim about the client's key.
    fx.cp().expect_portal_id("some-other-portal");
    assert_refused(
        fx,
        "unattributable portal",
        Some(&bearer("after-identity-change")),
        502,
        "upstream_unavailable",
        "availability_error",
    )
    .await?;
    ensure!(
        fx.cp()
            .rejections()
            .iter()
            .any(|r| r.contains("unattributable-portal")),
        "DC-8: the stub should have refused the exchange as unattributable, saw {:?}",
        fx.cp().rejections(),
    );

    // INV-31 — and readiness still does not turn on any of it.
    let d = driver::get(&fx.http, &format!("{}/ready", fx.base)).await?;
    ensure!(
        d.status == 200,
        "INV-31: readiness must not depend on the control plane, got {}",
        d.status,
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Shadow mode: every verdict evaluated, none enforced, one neutral projection.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn ct10_shadow_mode_admits_and_projects_neutrally() -> anyhow::Result<()> {
    let auth = Auth::new(PORTAL_ID).enforcement("log_only");
    let mut fx = Fixture::start_with_auth(ToyWorld::standard(), 2, auth).await?;
    let result = shadow(&mut fx).await;
    fx.finish(result)
}

async fn shadow(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    fx.cp()
        .answer("shadow-denied", Answer::Deny("revoked".to_owned()));

    // Nothing the control plane says can reject a request in shadow mode —
    // including having said nothing at all, because no credential was presented.
    assert_served(fx, "shadow: no credential", "toy", None).await?;
    assert_served(
        fx,
        "shadow: ungrammatical token",
        "toy",
        Some("Bearer nope_k1_secret"),
    )
    .await?;
    assert_served(
        fx,
        "shadow: a credential the control plane denies",
        "toy",
        Some(&bearer("shadow-denied")),
    )
    .await?;

    // OB-12/13 — one neutral series covers every shadow verdict. Publishing the
    // verdict it deliberately withheld from the response would be the same
    // disclosure by another route (INV-39).
    let scrape = fx.scrape().await?;
    let decisions = samples(&scrape, AUTH_DECISIONS);
    ensure!(
        !decisions.is_empty(),
        "OB-12: shadow mode must still publish that it evaluated",
    );
    for (labels, _) in &decisions {
        ensure!(
            labels.contains("shadow_evaluated") && labels.contains("log_only"),
            "INV-39: shadow mode must project one neutral series, got {labels}",
        );
        ensure!(
            !labels.contains("error_code") && !labels.contains("error_type"),
            "INV-39: a shadow verdict must carry no code, got {labels}",
        );
    }
    ensure!(
        sum_where(&scrape, AUTH_DECISIONS, &[("decision", "reject")]) == 0.0
            && sum_where(&scrape, AUTH_DECISIONS, &[("decision", "admit")]) == 0.0,
        "INV-39: shadow mode must publish neither admissions nor refusals",
    );
    // Every OB-13 signal is silent too: any of them moving would separate the
    // credential that was denied from the one that was not. OB-13 constrains
    // movement rather than presence, so the scalar families — which are
    // registered for the process and exist at zero — are checked by value, not
    // by absence. Checking only the labelled families would miss them entirely.
    ensure!(
        samples(&scrape, EXCHANGES).is_empty(),
        "OB-13: shadow mode must not publish exchange outcomes, got {:?}",
        samples(&scrape, EXCHANGES),
    );
    for family in OB13_SIGNALS {
        ensure!(
            sum_where(&scrape, family, &[]) == 0.0,
            "OB-13: {family} moved in shadow mode, which separates a denied credential \
             from an admitted one — got {}",
            sum_where(&scrape, family, &[]),
        );
    }

    // REQ-55 — shadow mode is not free: it exchanges on the same cache-miss rule
    // enforcement uses, which is the point. The load a cutover will produce is
    // what it exists to measure. Both requests that presented nothing to
    // exchange cost nothing; the one that presented a credential cost one.
    ensure!(
        fx.cp().exchanges() == 1,
        "REQ-55: shadow mode must exchange on the same cache-miss rule, saw {:?}",
        fx.cp().ledger.entries(),
    );
    ensure!(
        fx.cp().seen().iter().all(|s| s.key_id == "shadow-denied"),
        "REQ-55: shadow mode exchanged for a credential it had nothing to ask about",
    );

    // It did evaluate, though — the protected log is where the verdict went.
    let log = fx.portal.log_all();
    ensure!(
        !log.contains(SECRET_MARKER),
        "INV-38: shadow mode leaked a presented secret into the log",
    );
    ensure!(
        log.contains("would_reject"),
        "shadow mode must record the verdict it did not enforce",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// No `auth:` block: DC-8 is vacuous and nothing in IB-9 is observable.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn ct10_without_an_auth_block_the_portal_is_inert() -> anyhow::Result<()> {
    let mut fx = Fixture::start(ToyWorld::standard(), 2).await?;
    let result = inert(&mut fx).await;
    fx.finish(result)
}

async fn inert(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;

    assert_served(fx, "inert: no credential", "toy", None).await?;
    // A key presented to a portal that has no authority to check it against is
    // not a reason to refuse: REQ-56 makes the whole binding unobservable here.
    assert_served(fx, "inert: a credential anyway", "toy", Some(&bearer("k1"))).await?;

    let scrape = fx.scrape().await?;
    ensure!(
        samples(&scrape, AUTH_DECISIONS).is_empty() && samples(&scrape, EXCHANGES).is_empty(),
        "REQ-56: a portal without authorization must publish no authorization activity",
    );
    for family in OB13_SIGNALS {
        ensure!(
            sum_where(&scrape, family, &[]) == 0.0,
            "REQ-56: {family} moved on a portal with no `auth:` block",
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Budget saturation: retryable overload, never a verdict.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn ct10_exchange_budget_saturates_into_overload() -> anyhow::Result<()> {
    let auth = Auth::new(PORTAL_ID)
        .limit("exchange_rate_per_sec", 1)
        .limit("max_inflight_exchanges", 1);
    let mut fx = Fixture::start_with_auth(ToyWorld::standard(), 2, auth).await?;
    let result = saturated(&mut fx).await;
    fx.finish(result)
}

async fn saturated(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    // Distinct credentials, so each is a genuine miss and the single-flight
    // path cannot collapse them into one call.
    fx.cp().set_delay(Duration::from_millis(400));

    let timestamp_url = format!("{}/datasets/toy/timestamps/1700000000/block", fx.base);
    let responses = futures::future::join_all((0..12).map(|i| {
        let (http, url) = (fx.http.clone(), timestamp_url.clone());
        let auth = bearer(&format!("saturate{i}"));
        async move { driver::get_as(&http, &url, Some(&auth)).await }
    }))
    .await;
    fx.cp().set_delay(Duration::ZERO);

    let mut overloaded = 0;
    for (i, d) in responses.iter().enumerate() {
        let d = d
            .as_ref()
            .map_err(|e| anyhow::anyhow!("request {i}: {e}"))?;
        ensure!(
            d.status != 403,
            "LIV-14: a saturated budget must never answer a verdict about a key, got 403 on {i}",
        );
        if d.status == 529 {
            let (code, error_type) = envelope(&format!("saturated {i}"), d)?;
            ensure!(
                code == "overloaded" && error_type == "rate_limit_error",
                "expected rate_limit_error/overloaded, got {error_type}/{code}",
            );
            // Validator 6 already required a usable hint here; IB-5 puts the
            // floor at P-RETRY-AFTER-MIN.
            overloaded += 1;
        }
    }
    ensure!(
        overloaded > 0,
        "LIV-14: a bounded budget under a burst of distinct credentials should have refused \
         at least one request as overloaded; statuses were {:?}",
        responses
            .iter()
            .map(|d| d.as_ref().map(|d| d.status).unwrap_or(0))
            .collect::<Vec<_>>(),
    );

    // The refusals are the budget's, not the control plane's: it was never asked
    // more often than the bound allows.
    ensure!(
        fx.cp().exchanges() <= 12 - overloaded,
        "DC-8: {} exchanges for {} admitted requests — the bound did not hold",
        fx.cp().exchanges(),
        12 - overloaded,
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The dedicated signing key: which key reaches the wire, not which key loads.
// ---------------------------------------------------------------------------

/// The control plane registers `auth.key_path` and nothing else, so signing
/// with the network identity is refused. A unit test on the loader cannot stand
/// in: it still passes when the key it returns is dropped on the way to the
/// signer.
#[tokio::test(flavor = "multi_thread")]
async fn ct10_a_dedicated_key_is_what_signs_the_exchange() -> anyhow::Result<()> {
    let auth = Auth::new(PORTAL_ID)
        .dedicated_key()
        .limit("exchange_rate_per_sec", 1000);
    let mut fx = Fixture::start_with_auth(ToyWorld::standard(), 2, auth).await?;
    let result = dedicated_key(&mut fx).await;
    fx.finish(result)
}

async fn dedicated_key(fx: &mut Fixture) -> anyhow::Result<()> {
    fx.wait_ready(Duration::from_secs(60)).await?;
    fx.cp().answer("dedicated", Answer::grant());

    assert_served(
        fx,
        "a credential exchanged under the dedicated key",
        "toy",
        Some(&bearer("dedicated")),
    )
    .await?;

    ensure!(
        fx.cp().rejections().is_empty(),
        "DC-8: the exchange was signed with a key the control plane does not hold: {:?}",
        fx.cp().rejections(),
    );
    Ok(())
}
