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
        let name_end = line.find(['{', ' ']).unwrap_or(line.len());
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

/// Every sample of one family, as (label text, value). Counters are exposed
/// with a `_total` suffix that is not part of the family name, so both spellings
/// match. The label text is kept verbatim — comparing two scrapes of it is how
/// CT-10 proves a series did not move.
pub fn samples(text: &str, family: &str) -> Vec<(String, f64)> {
    let mut out = Vec::new();
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some((name_and_labels, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let (name, labels) = match name_and_labels.split_once('{') {
            Some((name, labels)) => (name, labels.trim_end_matches('}')),
            None => (name_and_labels, ""),
        };
        if name != family && name != format!("{family}_total") {
            continue;
        }
        if let Ok(x) = value.parse::<f64>() {
            out.push((labels.to_owned(), x));
        }
    }
    out
}

/// The family's total across every sample whose labels contain all of `labels`.
/// Matched on whole pairs, not by substring: `("code", v)` must not count an
/// `error_code="v"` series, or an assertion that a family did NOT move reads
/// the wrong series and passes. Absent series read as zero: a counter that
/// never incremented is not exposed.
pub fn sum_where(text: &str, family: &str, labels: &[(&str, &str)]) -> f64 {
    samples(text, family)
        .into_iter()
        .filter(|(got, _)| labels.iter().all(|(k, v)| has_label_pair(got, k, v)))
        .map(|(_, value)| value)
        .sum()
}

/// Whether `key="value"` appears as a whole pair: bounded by start or end of
/// the label text or by the `,` separator on both sides.
fn has_label_pair(labels: &str, key: &str, value: &str) -> bool {
    let needle = format!("{key}=\"{value}\"");
    labels.match_indices(&needle).any(|(at, _)| {
        let boundary_before = matches!(labels[..at].chars().next_back(), None | Some(','));
        let boundary_after = matches!(labels[at + needle.len()..].chars().next(), None | Some(','));
        boundary_before && boundary_after
    })
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
            Some(actual) => failures.push(format!("{family} = {actual}, expected {expected}")),
            None => failures.push(format!("{family} missing from /metrics")),
        }
    }
    failures
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_label_key_does_not_match_a_longer_key_it_suffixes() {
        let text = "family{error_code=\"invalid\"} 3\n\
                    family{code=\"invalid\"} 5\n\
                    family{mode=\"x\",code=\"invalid\"} 7\n";

        assert_eq!(sum_where(text, "family", &[("code", "invalid")]), 12.0);
        assert_eq!(sum_where(text, "family", &[("error_code", "invalid")]), 3.0);
        assert_eq!(
            sum_where(text, "family", &[("mode", "x"), ("code", "invalid")]),
            7.0
        );
    }
}
