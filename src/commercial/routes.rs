//! The authorization surface, keyed by axum's matched path.
//!
//! One table rather than a wrapper per route, so the gate is a single
//! `route_layer` and the router reads like a router. The default is what makes
//! that safe: a matched path absent from both lists below is gated as a data
//! route naming no dataset — the most closed classification there is. Adding a
//! route and forgetting it refuses traffic, which is noticed; the wrapper it
//! replaced could be forgotten into serving traffic, which is not.

use super::{DatasetSource, RouteClass};

/// Every route a key can be required for, and how it names its dataset.
const CLASSIFIED: &[(&str, DatasetSource, RouteClass)] = &[
    // Data: blocks, query results, block lookups.
    (
        "/datasets/:dataset/archival-stream",
        DatasetSource::Alias,
        RouteClass::Data,
    ),
    (
        "/datasets/:dataset/archival-stream/debug",
        DatasetSource::Alias,
        RouteClass::Data,
    ),
    (
        "/datasets/:dataset/finalized-stream",
        DatasetSource::Alias,
        RouteClass::Data,
    ),
    (
        "/datasets/:dataset/stream",
        DatasetSource::Alias,
        RouteClass::Data,
    ),
    (
        "/datasets/:dataset/timestamps/:timestamp/block",
        DatasetSource::Alias,
        RouteClass::Data,
    ),
    (
        "/datasets/:dataset_id/query/:worker_id",
        DatasetSource::EncodedId,
        RouteClass::Data,
    ),
    ("/sql/query", DatasetSource::Absent, RouteClass::Data),
    // Metadata: what the portal and its datasets are. Public on a shared
    // portal, confidential on a single-tenant one — `gated_routes` decides.
    ("/status", DatasetSource::Absent, RouteClass::Metadata),
    ("/datasets", DatasetSource::Absent, RouteClass::Metadata),
    (
        "/datasets/:dataset",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    (
        "/datasets/:dataset/metadata",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    (
        "/datasets/:dataset/state",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    (
        "/datasets/:dataset/head",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    (
        "/datasets/:dataset/archival-head",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    (
        "/datasets/:dataset/finalized-head",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    (
        "/datasets/:dataset/height",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    (
        "/datasets/:dataset/archival-stream/height",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    (
        "/datasets/:dataset/finalized-stream/height",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    (
        "/datasets/:dataset/:start_block/worker",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    (
        "/datasets/:dataset/:block/debug",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    (
        "/debug/workers",
        DatasetSource::Absent,
        RouteClass::Metadata,
    ),
    ("/sql/metadata", DatasetSource::Absent, RouteClass::Metadata),
];

/// Never gated, under either scope: ops probes and the served schema. A pod
/// that cannot answer its own readiness check leaves rotation, and existing
/// scrapers hold no customer key (REQ-51).
const ALWAYS_OPEN: &[&str] = &["/ready", "/metrics", "/api-docs/openapi.json", "/docs"];

/// What the gate does with a matched path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gating {
    Open,
    Gated(DatasetSource, RouteClass),
}

pub fn classify(matched_path: &str) -> Gating {
    if ALWAYS_OPEN.contains(&matched_path) {
        return Gating::Open;
    }
    CLASSIFIED
        .iter()
        .find(|(path, ..)| *path == matched_path)
        .map_or(
            // Unlisted: the most closed classification, so a forgotten route
            // fails loudly rather than serving.
            Gating::Gated(DatasetSource::Absent, RouteClass::Data),
            |(_, source, class)| Gating::Gated(*source, *class),
        )
}

/// Whether the path is listed at all, as opposed to falling through to the
/// closed default. Only `http_server`'s route-table test needs the distinction.
#[cfg(test)]
pub fn is_classified(matched_path: &str) -> bool {
    CLASSIFIED.iter().any(|(path, ..)| *path == matched_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unlisted_route_is_gated_as_data() {
        assert_eq!(
            classify("/datasets/:dataset/something-new"),
            Gating::Gated(DatasetSource::Absent, RouteClass::Data)
        );
    }

    #[test]
    fn the_ops_surface_is_open() {
        for path in ALWAYS_OPEN {
            assert_eq!(classify(path), Gating::Open, "{path}");
        }
    }

    /// A duplicate would make the earlier entry win silently, and the two could
    /// disagree on the class.
    #[test]
    fn every_path_is_listed_once() {
        let mut paths: Vec<_> = CLASSIFIED
            .iter()
            .map(|(path, ..)| *path)
            .chain(ALWAYS_OPEN.iter().copied())
            .collect();
        let total = paths.len();
        paths.sort_unstable();
        paths.dedup();
        assert_eq!(paths.len(), total, "a route is classified twice");
    }
}
