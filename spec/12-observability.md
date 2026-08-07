# 12 — Observability

Required signals, numbered. The harness treats a signal that contradicts ledger truth
as a failure (INV-30): **lying metrics are failures**. Cardinality of every labeled
family is bounded (intent — GAP-6): labels come from closed sets (endpoint, class,
outcome, dataset) — per-worker labels must be bounded or evicted. `/metrics` is a
keyless, client-readable surface (IB-9), so commercial deployments apply an additional
confidentiality rule: no public series exposes an internal authorization rung or lookup
detail beyond the client-visible wire outcome (INV-39).

**OB-1 — State gauges.** Active streams (census), in-flight congestion permits and
window size, open leases (or an equivalent worker-busy gauge), known workers, known
chunks and highest block per dataset. At quiescence each equals modeled truth. Dataset
identities are public on every deployment (NG6), so the scrape carries them as it always
has.

**OB-2 — Progress heartbeat.** Per active stream: periodic progress (coverage cursor,
bytes) at P-HEARTBEAT-INTERVAL, plus time-to-first-byte per response. Distinguishes
idle-input (EMPTY polling) from a stalled service: a stream with no heartbeat
progress past P-STALL-BUDGET ⚠ is the LIV-2 witness.

**OB-3 — Operation metrics.** Responses counted by operation × ADR-011 error type/code
(or success) × serving source, with latency histograms. The taxonomy rides as the
`error_code`/`error_type` labels (prefixed: a bare `type` label says nothing on a
metric), attached to **4xx/5xx only** — a 2xx carries neither, so the routine 204 that
is the steady state of every polling client cannot inflate an availability alert
(INV-30). A failure reaching the middleware unclassified is still counted, as
`unclassified`. The source label is `network`,
`real_time`, or `none` for pre-routing failures; it does not imply a response header.
**Truncations count separately** from completions (SLI-6 is computed from this);
refusals by code distinguish `overloaded` from `no_workers` — the 2026-07 storm was
misdiagnosed for lack of this split.

**OB-4 — Dependency health.** Per dependency (DC-1..DC-6): call outcomes by class,
latency, and for workers: selections by priority group, penalties applied, backoff
hints received. Every logical real-time-source request is counted exactly once in
`hotblocks_requests`, with a closed `outcome`: `response`, `replay_response`,
`replay_failed`, `canceled`, `replay_canceled`, `timeout`, or `transport_failed`
(ADR-015). The first two mean that a response head was obtained, on the first attempt and
on the replay respectively; every value states what the transport observed, never what the
client ended up seeing — that axis lives in `http_status`. The replay outcomes keep an
absorbed connection fault visible, and the two cancellation outcomes distinguish which
attempt the caller abandoned without claiming an upstream result that was never observed.
`timeout` is the expected non-replay decision; growth in `transport_failed` can expose a
connection fault whose shape the replay classifier stopped recognising. A wedged
dependency must be visible from the Portal's own metrics alone (REQ-22).

**OB-5 — Readiness reason.** The readiness state as a gauge with a reason code
(loading / insufficient-connectivity / shutting-down / no-key-snapshot on an enforcing
commercial deployment / ⚠ stale-artifact — ADR-013, GAP-2), and logged transitions. A
probe flip is attributable without log archaeology.

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
signature-verification failures, and — on a commercial deployment — a key snapshot older
than P-KEY-SNAPSHOT-MAX-AGE. That last one is the only response to key staleness there
is: the Portal keeps serving whatever the age (REQ-54), so the alarm is not a warning
about a degradation to come, it is the degradation being handled. Alarms are the LIV-12 witness: persistent failure is
never log-only. (Sampled error reporting to DC-7 complements, never replaces, these.)

**OB-10 — Congestion window trace.** Window size, grow/shrink counters — the LIV-8
witness.

**OB-11 — Admission capacity.** Refusals counted by *which* capacity ran out (stream-slot
cap, download headroom, worker backoff, worker rate limit), the admission cap itself as a
gauge, and occupancy as an accumulated time integral rather than a sampled level. Three
reasons the OB-1 census cannot stand in for these. A slot-cap refusal is decided before
the stream exists, so it is structurally absent from every stream family; a refusal raised
mid-stream never reaches a status, since the response is already committed. And a gauge
read at scrape time cannot witness saturation shorter than the scrape interval — the level
moves many times between two reads, so a fleet pinned at the cap for seconds leaves no
trace, which is precisely when clients are being refused. Integrating each admitted-stream
transition under one serialized clock preserves exact elapsed intervals independently of
the scrape cadence; a periodic flush bounds publication lag without becoming the source of
truth. Publishing the cap keeps its literal out of the alert expression. All four
reasons map to one wire code (IB-5 `overloaded`), and must: the client's move is identical,
the operator's is not.

**OB-12 — Authorization decisions.** Commercial deployments only. In enforcing mode,
every completed verdict (DEF-20) is counted on the public scrape by decision × actual
**wire code** (or success) × enforcement mode; a lookup that produced no
verdict is counted only by the OVERLOADED or UPSTREAM-FAILURE code actually returned. The
four internal reasons sharing `invalid_credential` are deliberately one public label
value. In `log_only`, every request increments the same neutral `shadow_evaluated` public
outcome: the would-be verdict and an indeterminate lookup are distinguishable only in
protected structured logs. No metric label or request-synchronous counter may let a
client bracket two keyless scrapes and learn more than its response revealed (INV-39,
ADR-011). Auth refusals in enforcing mode must still be distinguishable from every other
refusal on the OB-3 error-code axis — an auth refusal counted as `malformed_request` is a lying
metric (INV-30). Implemented as `commercial_authorization_decisions`.

**OB-13 — Key snapshot freshness and provenance.** Commercial deployments only.
Snapshot age since the last successful feed read (a gauge — the LIV-13 witness), the held
cursor, last reported head and epoch, applied-delta and rebuild counters, and sync
failures by cause. Implemented as the `commercial_key_snapshot_*` families; age is
republished at the end of every tick, failed ones included, so it climbs through an
outage rather than freezing, and its resolution is the sync interval. These public values change on background feed
activity, not synchronously with one presented key. Record count and authorize-on-miss
outcomes are omitted from the keyless scrape: either can reveal whether an
attacker-chosen id caused a lookup or inserted a record. Lookup outcomes instead emit
protected structured events (resolved, unknown, rate-limited, over the in-flight cap,
failed), and CT-10 uses those events plus the control-plane stub ledger as the
LIV-14/HZ-10 witness. A control plane that has stopped answering remains visible publicly
as monotone age growth without exposing a key id or request path.

## Property → observable mapping

| Property | Decided by |
|---|---|
| LIV-1, LIV-3, LIV-4 | OB-2 TTFB, OB-3 latency |
| LIV-2 | OB-2 heartbeat vs coverage |
| LIV-5, LIV-11 | OB-8 timestamps, OB-5 |
| LIV-6 | OB-6 identifier/age |
| LIV-7 | OB-4 selection counters |
| LIV-8 | OB-10 |
| LIV-9 | OB-3 refusal counters, OB-11 saturation integral |
| LIV-10 | OB-1 gauges at quiescence |
| LIV-12 | OB-9 |
| LIV-13, LIV-14 | OB-13 age/cursor; protected lookup events + stub ledger |
| INV-6, INV-15, INV-39 | protected OB-12 reason logs + public non-disclosure; OB-13 epoch/rebuild counters |
| INV-30/31 | OB-1, OB-5 (they are the invariant's subject) |
| SLI-1..6 | OB-2, OB-3, OB-1 + process RSS |

## Logging

Per-request correlated spans keyed by the request identifier (REQ-9, REQ-31); stream
completion summaries carrying coverage, bytes, outcome class; readiness and artifact
transitions logged at state-change only. Log content is diagnostic, not contract —
except for the credential-confidentiality and no-oracle checks that explicitly inspect it
(INV-38/39). Logs are an operator-protected sink and are never served by the client HTTP
surface.
