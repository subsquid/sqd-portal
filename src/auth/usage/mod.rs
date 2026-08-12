//! Shadow usage measurement: what was served, to whose key, in encoded bytes
//! (REQ-60, DC-9, ADR-016).
//!
//! Measurement, not metering. Nothing here can refuse, delay, shape or alter a
//! response — the only thing it does to the serving path is count bytes that
//! are already going out, and hand the count to a queue that drops rather than
//! waits (INV-32). A metering path that can reject or stall a request is
//! enforcement with the switch off, and this is deliberately not that.
//!
//! Three pieces, in the order a byte meets them:
//!
//! - [`tap`] wraps the response body on gated routes and counts encoded bytes
//!   as frames are yielded, cutting a delta record every `P-USAGE-INTERIM` and
//!   a residual at the end — including the end nobody asked for, a client that
//!   went away.
//! - [`UsageSink`] is the hand-off: one `try_send` into a bounded queue, with a
//!   full queue counted and dropped. It is the whole of what the serving path
//!   pays beyond the counting itself.
//! - [`reporter`] owns everything slow — batching, signing, POSTing, retrying,
//!   the shutdown flush — on its own task, where a failure has nowhere to
//!   propagate to.
//!
//! Inert unless `auth.usage:` is present: with no block there is no sink, so
//! the gate deposits no attribution and no body is ever wrapped.

use std::sync::Arc;

use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use super::{cache::CachedGrant, config::ResolvedAuth, signing::RequestSigner};
use crate::metrics::{UsageDrop, UsageSignals};

mod config;
mod event;
mod reporter;
mod tap;

pub use config::UsageConfig;
pub(super) use tap::tap_layer;

use event::UsageEvent;

/// Who a request is being served to, deposited into the request extensions by
/// the gate and read back by the egress tap (REQ-60).
///
/// Holds the grant rather than copying out of it: the claims are already behind
/// an `Arc` in the cache, they outlive the request, and a record is cut at most
/// twice a minute per response — so the per-request cost is three pointer
/// clones and the string copies happen only where a record is actually made.
#[derive(Debug, Clone)]
pub(crate) struct Attribution(Arc<Attributed>);

#[derive(Debug)]
struct Attributed {
    grant: Arc<CachedGrant>,
    /// The canonical name, resolved by the gate, where the route names a
    /// dataset at all.
    dataset: Option<String>,
    /// The route's declared label. Shared rather than copied: it is fixed at
    /// mount time and identical for every request to that route.
    endpoint: Arc<str>,
}

impl Attribution {
    pub(super) fn new(
        grant: Arc<CachedGrant>,
        dataset: Option<String>,
        endpoint: Arc<str>,
    ) -> Self {
        Self(Arc::new(Attributed {
            grant,
            dataset,
            endpoint,
        }))
    }

    pub(super) fn key_id(&self) -> &str {
        &self.0.grant.key_id
    }

    pub(super) fn organization_id(&self) -> Option<&str> {
        self.0.grant.organization_id.as_deref()
    }

    pub(super) fn dataset(&self) -> Option<&str> {
        self.0.dataset.as_deref()
    }

    pub(super) fn endpoint(&self) -> &str {
        &self.0.endpoint
    }
}

/// An event with the instant it was handed over, which is what the reporter's
/// age bound is measured from.
pub(super) struct Queued {
    pub(super) event: UsageEvent,
    queued_at: Instant,
}

/// The hot path's entire view of usage reporting: a bounded queue and the
/// counters that say what happened to it.
///
/// `record` is infallible from the caller's view by construction — there is no
/// error to return, because every outcome is a counter. That is the property
/// INV-32 rests on: a caller that cannot fail cannot be made to wait for a
/// sink, and no `?` in a body wrapper can turn a reporting fault into a
/// truncated response.
pub struct UsageSink {
    events: mpsc::Sender<Queued>,
    signals: UsageSignals,
    interim: std::time::Duration,
}

impl UsageSink {
    /// Never blocks, never allocates beyond the event, never looks up a metric
    /// family: the counters were bound at construction, and a full queue costs
    /// one increment and the drop (HZ-14). Deliberately silent — a per-drop log
    /// line would turn a sink outage into a log flood on the serving path,
    /// which is the same failure by a different route.
    fn record(&self, event: UsageEvent, at: Instant) {
        let queued = Queued {
            event,
            queued_at: at,
        };
        match self.events.try_send(queued) {
            Ok(()) => {
                self.signals.enqueued.inc();
            }
            Err(mpsc::error::TrySendError::Full(_)) => self.signals.dropped(UsageDrop::QueueFull),
            // The reporter has finished its shutdown flush and gone; a response
            // still draining after it is measured and dropped, by design.
            Err(mpsc::error::TrySendError::Closed(_)) => self.signals.dropped(UsageDrop::Stopped),
        }
    }

    /// How much measured time one record may cover (P-USAGE-INTERIM).
    fn interim(&self) -> std::time::Duration {
        self.interim
    }

    /// A sink whose queue a test drains itself, so the tap can be exercised
    /// without a control plane on the other end.
    #[cfg(test)]
    pub(super) fn for_test(
        capacity: usize,
        interim: std::time::Duration,
    ) -> (Arc<Self>, mpsc::Receiver<Queued>) {
        let (events, rx) = mpsc::channel(capacity);
        (
            Arc::new(Self {
                events,
                signals: UsageSignals::bind(),
                interim,
            }),
            rx,
        )
    }
}

/// Builds the sink and starts the reporter behind it. Nothing is measured
/// before this runs, and nothing outside `auth::build` calls it: the sink's
/// presence *is* the switch.
pub(super) fn start(
    config: &ResolvedAuth,
    usage: &UsageConfig,
    signer: RequestSigner,
    cancel: CancellationToken,
) -> anyhow::Result<(Arc<UsageSink>, JoinHandle<()>)> {
    let signals = UsageSignals::bind();
    let (events, receiver) = mpsc::channel(usage.queue_capacity);
    let sink = Arc::new(UsageSink {
        events,
        signals: signals.clone(),
        interim: usage.interim_interval(),
    });

    let reporter = reporter::Reporter::new(config, usage, signer, signals)?;
    tracing::info!(
        endpoint = %reporter.endpoint(),
        queue_capacity = usage.queue_capacity,
        batch_max_events = usage.batch_max_events,
        flush_interval_ms = usage.flush_interval_ms,
        interim_interval_secs = usage.interim_interval_secs,
        "usage measurement enabled"
    );
    let task = tokio::spawn(reporter.run(receiver, cancel));
    Ok((sink, task))
}
