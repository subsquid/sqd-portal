# 12 — Observability

Required signals, numbered. The harness treats a signal that contradicts ledger truth
as a failure (INV-30): **lying metrics are failures**. Cardinality of every labeled
family is bounded (intent — GAP-6): labels come from closed sets (endpoint, class,
outcome, dataset) — per-worker labels must be bounded or evicted.

**OB-1 — State gauges.** Active streams (census), in-flight congestion permits and
window size, open leases (or an equivalent worker-busy gauge), known workers, known
chunks and highest block per dataset. At quiescence each equals modeled truth.

**OB-2 — Progress heartbeat.** Per active stream: periodic progress (coverage cursor,
bytes) at P-HEARTBEAT-INTERVAL, plus time-to-first-byte per response. Distinguishes
idle-input (EMPTY polling) from a stalled service: a stream with no heartbeat
progress past P-STALL-BUDGET ⚠ is the LIV-2 witness.

**OB-3 — Operation metrics.** Responses counted by operation × ADR-011 error type/code
(or success) × serving source, with latency histograms. The source label is `network`,
`real_time`, or `none` for pre-routing failures; it does not imply a response header.
**Truncations count separately** from completions (SLI-6 is computed from this);
refusals by code distinguish `overloaded` from `no_workers` — the 2026-07 storm was
misdiagnosed for lack of this split.

**OB-4 — Dependency health.** Per dependency (DC-1..DC-6): call outcomes by class,
latency, and for workers: selections by priority group, penalties applied, backoff
hints received. A wedged dependency must be visible from the Portal's own metrics
alone (REQ-22).

**OB-5 — Readiness reason.** The readiness state as a gauge with a reason code
(loading / insufficient-connectivity / shutting-down / ⚠ stale-artifact), and logged
transitions. A probe flip is attributable without log archaeology.

**OB-6 — Artifact provenance.** Applied artifact identifier and ⚠ age
(ADR-013/GAP-2), application timestamps, skipped/unchanged fetch counts. A wedged
publisher is visible as monotone age growth.

**OB-7 — Data-pressure signals.** Congestion shrink events with cause, download
utilization, and the headroom-refusal counter — a global download halt must be
directly observable (window at floor + utilization pinned).

**OB-8 — Lifecycle timestamps.** Process start, listener up, first artifact applied,
ready, SIGTERM, drain start, exit — the LIV-5/LIV-11 witnesses.

**OB-9 — Alarm states.** Edge events + level reads, reason-coded, for: artifact
fetch/validation failures (⚠ GAP-1/2), background-loop deaths, usage-log drops,
signature-verification failures. Alarms are the LIV-12 witness: persistent failure is
never log-only. (Sampled error reporting to DC-7 complements, never replaces, these.)

**OB-10 — Congestion window trace.** Window size, grow/shrink counters — the LIV-8
witness.

## Property → observable mapping

| Property | Decided by |
|---|---|
| LIV-1, LIV-3, LIV-4 | OB-2 TTFB, OB-3 latency |
| LIV-2 | OB-2 heartbeat vs coverage |
| LIV-5, LIV-11 | OB-8 timestamps, OB-5 |
| LIV-6 | OB-6 identifier/age |
| LIV-7 | OB-4 selection counters |
| LIV-8 | OB-10 |
| LIV-9 | OB-3 refusal counters |
| LIV-10 | OB-1 gauges at quiescence |
| LIV-12 | OB-9 |
| INV-30/31 | OB-1, OB-5 (they are the invariant's subject) |
| SLI-1..6 | OB-2, OB-3, OB-1 + process RSS |

## Logging

Per-request correlated spans keyed by the request identifier (REQ-9, REQ-31); stream
completion summaries carrying coverage, bytes, outcome class; readiness and artifact
transitions logged at state-change only. Log content is diagnostic, not contract —
tests assert on metrics and responses, not log text.
