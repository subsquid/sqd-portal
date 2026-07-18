//! Quiescence-gated gauge audit (INV-30): at quiescence, gauges equal modeled
//! truth. Lying metrics are failures, not cosmetics.

use std::collections::HashMap;

/// Sum all samples of a metric family from OpenMetrics text.
pub fn family_sum(text: &str, family: &str) -> Option<f64> {
    let mut sum = None;
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let name_end = line.find(|c| c == '{' || c == ' ').unwrap_or(line.len());
        if &line[..name_end] != family {
            continue;
        }
        if let Some(value) = line.rsplit(' ').next() {
            if let Ok(x) = value.parse::<f64>() {
                *sum.get_or_insert(0.0) += x;
            }
        }
    }
    sum
}

/// Gauge expectations at quiescence. Returns human-readable failures.
pub fn audit_quiescent(text: &str, known_workers: f64) -> Vec<String> {
    let expectations: HashMap<&str, f64> = HashMap::from([
        ("portal_streams_active", 0.0),
        ("portal_congestion_in_flight", 0.0),
        ("portal_queries_running", 0.0),
        ("portal_known_workers", known_workers),
    ]);
    let mut failures = Vec::new();
    for (family, expected) in expectations {
        match family_sum(text, family) {
            Some(actual) if (actual - expected).abs() < f64::EPSILON => {}
            Some(actual) => {
                failures.push(format!("{family} = {actual}, expected {expected}"))
            }
            None => failures.push(format!("{family} missing from /metrics")),
        }
    }
    failures
}
