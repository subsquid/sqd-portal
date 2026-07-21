# 04 — Operation contracts

One subsection per operation from the DEF summary table. *Pre* = admission and
validation; *Effect* = which dependencies are consulted (by DC-n) and in what order;
*Post* = the response contract and error mapping (classes per DEF-10; codes are the
binding's, 14). Timeouts are symbolic; concrete values in
[15-parameters.md](15-parameters.md).

## The response-purity rule (frame condition of this shape)

The record content of any response is a pure function of **(request, configuration,
dependency-provided data)**. Shared adaptive state (DEF-12) may influence only:
admission decisions, worker choice, coverage extent, and timing. It must never
influence record content, record order, or per-record values. There is no hidden
ordering between concurrent requests; the only declared couplings are the global
capacity limits (stream census, congestion window, worker health). Restated as INV-28.

## Operations table

| Op | Idempotency class | Cancellation on client disconnect |
|---|---|---|
| OP-1 Stream | naturally idempotent (pure read over immutable/refreshable data) | aborts all in-flight upstream work for the request |
| OP-2 Head read | naturally idempotent | trivial |
| OP-3 Metadata read | naturally idempotent | trivial |
| OP-4 Catalog list | naturally idempotent | trivial |
| OP-5 Timestamp resolve | naturally idempotent | aborts in-flight upstream work |
| OP-6 Dataset state read | naturally idempotent | trivial |
| OP-7 Status read | naturally idempotent | trivial |
| OP-8 Readiness probe | naturally idempotent | trivial |
| OP-9 Metrics read | naturally idempotent | trivial |
| OP-10 SQL route plan | naturally idempotent | trivial |

Every operation is safe to retry; the Portal maintains no dedup keys (client retries
are new requests).

## OP-1 — Stream

*Pre.* In order: (1) resolve alias → dataset, else NOT-FOUND; (2) parse tuning
parameters, reject zero/unparsable as BAD-REQUEST; (3) decode and validate the query
per DEF-7, else BAD-REQUEST — **no dependency call happens before validation completes
(INV-10)**; (4) clamp tuning to operator caps (INV-11); (5) admission: if the stream
census is at P-MAX-STREAMS or congestion utilization exceeds P-HEADROOM-THRESHOLD,
refuse OVERLOADED with a retry hint (INV-12) — admission is checked before any
upstream work.

*Effect.* In real-time mode with validation enabled, a parent hash whose height is at
or below the frontier is validated before any emptiness decision — conflict detection
precedes EMPTY (INV-23, ADR-014). Then route by range (INV-13): first block ≤ archival
head → **archival path**; else if the dataset has a real-time attachment → **real-time
path**; else → EMPTY after P-NO-DATA-DELAY (head metadata per DEF-8; no source marker,
DEF-6).
— Archival path: read the applied artifact (DEF-4) for the chunk sequence; for each
chunk in order, query a worker per DC-1 (choice and speculative retries are free
variables, FV-1/FV-2, bounded by 1 + retries); concurrently fetch head markers from
DC-4 (falling back to the archival head) for response metadata. Records from chunk
results are emitted strictly in chunk order as the client drains.
— Real-time path: a proxied request to DC-4, replayed at most once on a connection-class
fault before the response head (ADR-015); records, conflict payloads, and
head metadata are streamed with internal upstream headers stripped. Non-success
upstream responses are classified and rewritten into the Portal error envelope
(ADR-003 as amended by ADR-011; IB-4/IB-5).

*Post.* Success: a response per DEF-8 — coverage starting at the requested first block,
INV-20/21/22 over the record sequence, head markers and source marker per INV-24.
Failures **before the first record** map to DEF-10 classes: no worker leasable →
DATA-UNAVAILABLE; bounded transient attempts exhausted → RETRIES-EXHAUSTED; attempts
exhausted on integrity failures → WORKER-FAILURE (DC-1); real-time
source failure → UPSTREAM-FAILURE; parent-hash mismatch → CONFLICT (real-time mode
only; takes precedence over EMPTY). Failures
**after the first record** truncate per INV-25 (ADR-001); the client recovers via
continuation (DEF-9). The stream ends when the requested range or the frontier is
reached, a chunk-count limit fires, or truncation occurs — coverage extent is a free
variable (FV-4).

*Timeouts.* Each worker attempt is bounded by P-TRANSPORT-TIMEOUT; body reads by
congestion permits (DC-1). Client disconnect aborts every in-flight attempt and
releases leases and census promptly (LIV-10).

## OP-2 — Head read

*Pre.* Alias resolution; variant selection (head / finalized / archival-only).
*Effect.* Real-time-attached datasets: one DC-4 head call; archival-only: the applied
artifact's head, which also serves as the finalized head (DEF-5); a dataset lacking the
requested surface (e.g. the archival head of a dataset with no archival identity) →
NOT-FOUND. If the real-time source reports no head, fall back to the archival head.
*Post.* A block reference (DEF-2), honest per INV-24; staleness bounded by
one successful P-ASSIGNMENT-REFRESH cycle while the publisher is healthy (archival), or
live (real-time). Publisher outage has no age bound (GAP-2). DC-4 failure →
UPSTREAM-FAILURE.

## OP-3 — Metadata read

*Pre.* Alias resolution. *Effect.* Catalog snapshot (DEF-14); start block from the
applied artifact, else from DC-4 status. *Post.* Metadata object; it converges within
one successful P-DATASETS-REFRESH cycle while the registry is healthy; outage staleness
is unbounded.

## OP-4 — Catalog list

*Effect.* Catalog snapshot only; no per-request upstream calls. *Post.* Every served
dataset with aliases and capabilities (REQ-10).

## OP-5 — Timestamp resolve

*Pre.* Alias resolution; timestamp parse. *Effect.* Archival lookup first (chunk
search + one bounded single-chunk query via DC-1); on archival miss, fall back to DC-4
(status, then a bounded query). *Post.* The first available block whose timestamp is
greater than or equal to the request; a pre-history request therefore returns the first
available block, while a timestamp after the data frontier is the EMPTY outcome
(INV-27; ADR-014) — the head advances and the same request later succeeds. Overload and
error outcomes match OP-1 (REQ-13), including the ADR-011 code and hint on OVERLOADED.

## OP-6 — Dataset state read · OP-7 — Status read

Operator introspection (REQ-14). *Effect.* OP-6 reads the applied artifact; OP-7 reads
the chain-status snapshot (≤ P-CHAIN-REFRESH stale; before the first fetch it reports a
loading state, never fabricated values). *Post.* Explicitly unstable bodies — not part
of the conformance surface beyond existence and freshness.

## OP-8 — Readiness probe

*Effect.* Pure read of local state — never calls a dependency. *Post.* Ready iff:
an artifact is applied ∧ worker connectivity ≥ P-READY-CONNECTION-RATIO ∧ not shutting
down (INV-31). Intent (ADR-013 ⚠): also degrade when artifact age > P-ASSIGNMENT-MAX-AGE.

## OP-9 — Metrics read

*Effect.* Pure read. *Post.* The signal set of [12-observability.md](12-observability.md);
gauge honesty per INV-30.

## OP-10 — SQL route plan

*Pre.* Plan decode, else BAD-REQUEST. *Effect.* Applied artifact + embedded schemas;
no data queries. *Post.* A routing plan naming only workers present in the applied
artifact; no rows (NG6). Experimental: shape unspecified beyond this.

## Concurrency

Global limits only (NG2): the stream census caps concurrent OP-1/OP-5 work at
P-MAX-STREAMS (transient overshoot bounded by concurrent admissions, INV-5); the
congestion window (DEF-13) bounds concurrent chunk downloads across all streams;
P-MAX-QUERIES-PER-WORKER bounds per-worker concurrency (INV-3). At saturation, new
work is refused per INV-12 while admitted work proceeds unaffected.
