# 12 — Observability

Required signals, numbered. The harness treats a signal that contradicts ledger truth
as a failure (INV-30): **lying metrics are failures**. Cardinality of every labeled
family is bounded (intent — GAP-6): labels come from closed sets (endpoint, class,
outcome, dataset) — per-worker labels must be bounded or evicted. `/metrics` is a
keyless, client-readable surface (IB-9), so authorizing deployments apply an additional
confidentiality rule: no public series exposes an internal authorization rung or exchange
detail beyond the client-visible wire outcome (INV-39).

**OB-1 — State gauges.** Active streams (census), in-flight congestion permits and
window size, open leases (or an equivalent worker-busy gauge), known workers, known
chunks and highest block per dataset. At quiescence each equals modeled truth. Dataset
identities are public on every deployment (NG8), so the scrape carries them as it always
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
(loading / insufficient-connectivity / shutting-down / ⚠ stale-artifact — ADR-013,
GAP-2), and logged transitions. A probe flip is attributable without log archaeology.
Authorization contributes no reason code, in either enforcement mode: it cannot flip this
probe (INV-31), and a code for a state that cannot occur reads as a promise the gauge
does not keep.

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
signature-verification failures, and — on an authorizing deployment — requests being served
on renewal grace (a grant past `refresh_after` whose renewal is failing or locally
suppressed), sustained exchange failure, and refused signing headers. The first is the one
that has to page before the others matter: it is the leading edge of the `expires_at` cliff,
and the minimum remaining lifetime among affected grants is the whole window an operator
has to act in (REQ-54, DC-8). The last is a Portal-local misconfiguration — clock, identity,
or request construction — that fails every exchange at once, and it must not be diagnosed
as a client-key problem. Alarms are the LIV-12
witness: persistent failure is never log-only. None of the three authorization ones is emitted
or configured today (GAP-30). (Sampled error reporting to DC-7 complements, never replaces,
these.)

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

**OB-12 — Authorization decisions.** Authorizing deployments only. In enforcing mode,
every completed verdict (DEF-20) is counted on the public scrape by decision × actual
**wire code** (or success) × enforcement mode; an exchange that produced no
verdict is counted only by the OVERLOADED or UPSTREAM-FAILURE code actually returned. The
internal reasons sharing `invalid_credential` are deliberately one public label
value. In `log_only`, every request increments the same neutral `shadow_evaluated` public
outcome: the would-be verdict and an indeterminate exchange are distinguishable only in
protected structured logs. No metric label or request-synchronous counter may let a
client bracket two keyless scrapes and learn more than its response revealed (INV-39,
ADR-011). Auth refusals in enforcing mode must still be distinguishable from every other
refusal on the OB-3 error-code axis — an auth refusal counted as `malformed_request` is a lying
metric (INV-30).

**OB-13 — Grant cache and exchange health.** Authorizing deployments only. In enforcing
mode, the public scrape carries cache occupancy against P-GRANT-CACHE-CAPACITY and the
eviction rate (the HZ-13 witness); exchange attempts and outcomes by operational class —
answered, refused by budget, failed — with latency (the LIV-13/LIV-14 and DC-8 capacity
witnesses); and a count of grants whose offered lifetime was capped. For the cliff it also
carries a counter of admissions served on renewal grace, the number of grants currently in
that state, and the minimum time remaining to `expires_at` among them (zero when none are in
grace), the latter two recomputed on scrape by one walk of the cache under its lock — a walk
P-GRANT-CACHE-CAPACITY bounds. The three answer different questions: the rate says the
condition exists, the count says how wide it is, and the minimum names the first hard
refusal — the operator's lead time on the cliff, which no rate can supply. None of these
carries a key id, a fingerprint, a dataset, or a refusal reason finer than the enforcing
caller's wire response.

Shadow mode is deliberately different. None of the cache, exchange-outcome, latency,
capping, or grace signals above may *move* on its keyless scrape: `issued` versus `denied`,
or a grant-cache occupancy change, would reveal the verdict of a request whose response
admits either way. The constraint is on movement, not on presence — these families are
registered for the process, not per deployment mode, so they exist at zero on a shadow and
on a non-authorizing portal alike. A series pinned at zero is the same series for every
caller and every credential, which is what the rule protects; a series that moved would not
be. OB-12's single `shadow_evaluated` outcome is shadow mode's entire public authorization
projection. The load and cutover evidence it exists to gather remains in protected
per-exchange events and in the control plane's own telemetry.

The enforcing-mode `answered` class deliberately combines grants and denials. The cache is
keyed on the whole credential (DEF-18), so an unknown key id and a known one presented with
a wrong secret miss identically and increment the same operational counter. Bracketing two
scrapes around one's own enforcing request can therefore reveal the accepted
cache-membership residual that timing already exposes, but never the control plane's
verdict or either invalid case. Everything finer stays protected: per-exchange events
(resolved, denied with its rung, rate-limited, over the in-flight cap, failed) go to
structured logs, and CT-10 uses those plus the control-plane stub ledger as the
LIV-14/HZ-10 witness.

**OB-14 — Usage measurement health.** Measuring deployments only. The scrape carries what
was measured and what became of it: records handed to the reporter, records the sink
accepted, records dropped **by reason** — queue full, aged out past
P-USAGE-MAX-RETRY-AGE, refused on content, or produced after the reporter stopped —
deliveries that failed and will be retried, the queue's current depth, and delivery
latency. Every one of them is bound at construction rather than looked up per record: the
enqueue happens inside a response, and a metric-family lookup there is work the serving
path pays for measurement.

Drops are the point of the family. Loss is designed in (DC-9) and therefore has to be a
number rather than an inference: a total that is a lower bound is usable if the size of
the gap is known, and unusable otherwise. The reason axis is what separates a sink outage
(queue full, then aged out) from a contract break (refused on content) — the first is
operations, the second is a bug in what the Portal is sending, and they page different
people. Queue depth against the bound is the leading indicator for both.

These families carry no key id, no organization, no dataset and no request path. The
records do, and they go to the control plane over an authenticated channel; the scrape is
keyless (IB-9), and a per-customer series there would publish the customer list to anyone
who can reach `/metrics` — as well as growing without bound at the client's choosing
(HZ-15, INV-39's argument applied to measurement). Like every other family these are
registered for the process **unconditionally**, so on a Portal that measures nothing they
exist and read zero rather than being absent. That is deliberate and is not a hole in
REQ-61: non-interference is a claim about **data-surface responses** — status, headers,
body bytes, ending — not about the `/metrics` document, whose shape must not depend on
configuration. A family that appeared only where measurement was on would make a missing
counter ambiguous between "not configured" and "nothing reported", which is the harder
question to answer during an incident.

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
| LIV-13, LIV-14 | enforcing-mode OB-13 grace deadline and exchange outcomes; protected exchange events + stub ledger in both modes |
| INV-6, INV-15, INV-39 | protected OB-12 reason logs + public non-disclosure; neutral shadow projection and enforcing-only OB-13 classes |
| INV-30/31 | OB-1, OB-5 (they are the invariant's subject) |
| INV-32, REQ-60/61 | OB-14 drop reasons and queue depth against the sink stub's ledger |
| SLI-1..6 | OB-2, OB-3, OB-1 + process RSS |

## Logging

Per-request correlated spans keyed by the request identifier (REQ-9, REQ-31); stream
completion summaries carrying coverage, bytes, outcome class; readiness and artifact
transitions logged at state-change only. Log content is diagnostic, not contract —
except for the credential-confidentiality and no-oracle checks that explicitly inspect it
(INV-38/39). Logs are an operator-protected sink and are never served by the client HTTP
surface.
