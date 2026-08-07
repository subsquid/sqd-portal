//! How a route says whether it needs a key.
//!
//! Both [`AuthExt`] methods return a [`Classified`], and [`Gated::route`] takes
//! nothing else — so a route that says neither does not compile. A wrapper you
//! can forget to write is open by omission.

use std::sync::Arc;

use axum::{routing::MethodRouter, Router};

use super::{extractor, Gate};

/// A method router that has declared whether it needs a key.
pub struct Classified {
    router: MethodRouter,
    gated: bool,
}

/// Declared at the route, like [`endpoint`].
///
/// [`endpoint`]: crate::utils::logging::MethodRouterExt::endpoint
pub trait AuthExt {
    /// Needs a key on a commercial deployment.
    fn auth(self) -> Classified;

    /// Served to anyone. Spelled out, not defaulted: leaving a route open
    /// should be a sentence somebody wrote.
    fn no_auth(self) -> Classified;
}

impl AuthExt for MethodRouter {
    fn auth(self) -> Classified {
        Classified {
            router: self,
            gated: true,
        }
    }

    fn no_auth(self) -> Classified {
        Classified {
            router: self,
            gated: false,
        }
    }
}

/// A router whose only way to add a route demands that the route say so.
pub struct Gated {
    router: Router,
    gate: Option<Arc<Gate>>,
}

impl Gated {
    /// Without a `commercial:` block there is no gate and nothing is wrapped.
    pub fn new(gate: Option<Arc<Gate>>) -> Self {
        Self {
            router: Router::new(),
            gate,
        }
    }

    pub fn route(mut self, path: &str, classified: Classified) -> Self {
        let Classified { router, gated } = classified;
        let router = match (self.gate.clone(), gated) {
            (Some(gate), true) => {
                let dataset = dataset_param(path);
                router.route_layer(axum::middleware::from_fn(move |req, next| {
                    extractor::middleware(gate.clone(), dataset, req, next)
                }))
            }
            _ => router,
        };
        self.router = self.router.route(path, router);
        self
    }

    /// A router mounted whole. Nothing inside passes through [`Self::route`],
    /// so its routes answer without a key — hence the reason argument.
    pub fn merge_ungated(mut self, _why: &'static str, other: Router) -> Self {
        self.router = self.router.merge(other);
        self
    }

    pub fn into_router(self) -> Router {
        self.router
    }
}

/// Whether the path names the requested dataset. Where it does not, a
/// dataset-scoped key cannot be checked and is refused (REQ-53, OQ-13).
fn dataset_param(path: &str) -> bool {
    let mut segments = path.trim_start_matches('/').split('/');
    matches!(
        (segments.next(), segments.next()),
        (Some("datasets"), Some(":dataset"))
    )
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use tower::ServiceExt;

    use super::*;
    use crate::commercial::{test_support::gate_with, Enforcement};

    async fn status(gate: Option<Arc<Gate>>, classified: Classified) -> StatusCode {
        Gated::new(gate)
            .route("/probe", classified)
            .into_router()
            .oneshot(
                Request::builder()
                    .uri("/probe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn a_gated_route_needs_a_key_and_an_open_one_does_not() {
        let gate = Some(gate_with(Enforcement::Enforce));

        let gated = status(gate.clone(), get(|| async { "served" }).auth()).await;
        assert_eq!(gated, StatusCode::FORBIDDEN);

        let open = status(gate, get(|| async { "served" }).no_auth()).await;
        assert_eq!(open, StatusCode::OK);
    }

    /// The kill switch: with no `commercial:` block the gate is never
    /// installed, so an OSS build runs no authorization middleware at all.
    #[tokio::test]
    async fn nothing_is_gated_without_a_commercial_config() {
        assert_eq!(
            status(None, get(|| async { "served" }).auth()).await,
            StatusCode::OK
        );
    }

    #[test]
    fn only_a_dataset_segment_counts_as_naming_one() {
        assert!(dataset_param("/datasets/:dataset/stream"));
        assert!(dataset_param(
            "/datasets/:dataset/timestamps/:timestamp/block"
        ));

        assert!(!dataset_param("/sql/query"));
        assert!(!dataset_param("/datasets"));
        assert!(!dataset_param("/metrics"));
        assert!(!dataset_param("/datasets/:other/stream"));
    }
}
