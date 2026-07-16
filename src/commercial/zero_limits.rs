use std::sync::OnceLock;

use dashmap::DashSet;

use crate::metrics;

fn warned_limits() -> &'static DashSet<String> {
    static WARNED: OnceLock<DashSet<String>> = OnceLock::new();
    WARNED.get_or_init(DashSet::new)
}

pub(crate) fn report(identity: &str, limit: &'static str) {
    metrics::report_commercial_zero_valued_limit(limit);

    if warned_limits().insert(identity.to_string()) {
        tracing::warn!(
            key_id = identity,
            limit,
            "commercial grant carries a zero-valued limit; preserving fail-open semantics"
        );
    }
}
