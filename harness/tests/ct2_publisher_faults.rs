//! CT-2, DC-2: the publisher does not offer the artifact the portal is pointed at.
//!
//! Splitting the assignment gave the publisher a shape a portal can be wrong about — it may
//! carry only one of the two artifacts while a portal is configured for the other, either
//! before the split blobs appear or after the legacy one stops being published. Selection is
//! absolute, so the portal has to refuse rather than quietly route from whichever artifact it
//! can see. Falling back would make a `portal` canary that never ran indistinguishable from one
//! that passed, and would leave the `legacy` kill switch switching nothing off.

use std::time::Duration;

use anyhow::ensure;
use harness::artifact::AssignmentSource;
use harness::fixture::{Assignments, Fixture};
use harness::{metrics_audit, ToyWorld};

/// Long enough for several rounds of the 1 s assignment poll, so "never applied" means the
/// portal had the chance and declined rather than not having got there yet.
const POLLS: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread")]
async fn ct2_portal_source_absent_does_not_fall_back_to_legacy() -> anyhow::Result<()> {
    absent_source_is_refused(AssignmentSource::Portal, AssignmentSource::Legacy).await
}

#[tokio::test(flavor = "multi_thread")]
async fn ct2_legacy_source_absent_does_not_fall_back_to_portal() -> anyhow::Result<()> {
    absent_source_is_refused(AssignmentSource::Legacy, AssignmentSource::Portal).await
}

/// The portal is configured for `configured`; the publisher offers only `published`.
async fn absent_source_is_refused(
    configured: AssignmentSource,
    published: AssignmentSource,
) -> anyhow::Result<()> {
    let mut fx = Fixture::start_with_assignments(
        ToyWorld::standard(),
        2,
        Assignments {
            source: configured,
            published: vec![published],
        },
    )
    .await?;

    let result = async {
        tokio::time::sleep(POLLS).await;

        // No assignment applied means no workers, so readiness never arrives. This is the
        // deliberate cost of refusing: a misconfigured portal fails visibly instead of serving
        // the wrong wire format.
        ensure!(
            fx.wait_ready(Duration::from_secs(5)).await.is_err(),
            "portal became ready with no {configured} assignment published",
        );

        // The other artifact is on offer and reachable. Not fetching it is the whole claim.
        let fetched = fx.publisher_ledger.entries();
        ensure!(
            fetched.iter().any(|e| e == "network-state"),
            "publisher was never polled at all: {fetched:?}",
        );
        ensure!(
            !fetched.iter().any(|e| e.starts_with("artifact")),
            "fetched an artifact though {configured} was not published: {fetched:?}",
        );

        // Distinguishes a deliberate refusal from any other reason nothing was applied.
        // `sum_where`, not `family_sum`: the counter is exposed as `..._total`, which only the
        // former matches — the latter would read 0 here and pass vacuously.
        let text = fx.scrape().await?;
        let missing = metrics_audit::sum_where(&text, "portal_missing_assignment_source", &[]);
        ensure!(
            missing >= 1.0,
            "portal_missing_assignment_source never incremented (got {missing}); assignment \
             series in the scrape:\n{}",
            text.lines()
                .filter(|l| l.contains("assignment"))
                .collect::<Vec<_>>()
                .join("\n"),
        );

        Ok(())
    }
    .await;

    fx.finish(result)
}
