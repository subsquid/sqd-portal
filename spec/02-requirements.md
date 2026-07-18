# 02 — Requirements

Bands: 1–9 core data delivery · 10–16 discovery & metadata · 20–29 robustness &
overload · 30–34 operability · 40–44 network integration. Gaps in numbering are
reserved; additions never renumber. Acceptance status lives in
[13-conformance.md](13-conformance.md), not here.

## Core data delivery (1–9)

**REQ-1 — Ordered, exactly-once block streaming.** [MUST]
A stream request names a dataset, a query (filters and field selection), and a starting
block. A successful response covers a contiguous block-number range beginning at the
requested first block; every block in the covered range that matches the query appears
exactly once, as one record per line, in strictly ascending block order. No block
outside the covered range appears. A response may cover less than the requested range
(early stop is normal — REQ-6); a response that can cover nothing is the empty-success
case (REQ-5).
*Acceptance:* for any valid request over available data, delivered block numbers are
strictly increasing, start at the requested first block's range, contain no duplicates,
and never exceed the requested upper bound. Duplicate delivery of a range is a defect
(regression: the double-scheduling incident of 2026-06).
*Trace:* ADR-001.

**REQ-2 — Resumable progress.** [MUST]
The client must always be able to continue from wherever a stream ended. Every
successful stream response carries the dataset's current head and finalized head
(number and, for the finalized head, hash) as response metadata, so the client knows the
frontier and how far behind it is. If the response evaluated any block, it also exposes
a coverage cursor `(number, hash)` for the last one. A follow-up from cursor number + 1,
carrying the cursor hash as its parent, continues gap-free and overlap-free even when a
selective query emitted no record at the end of coverage.
*Acceptance:* stream headers expose head and finalized-head markers plus the coverage
cursor; issuing a follow-up from that cursor yields the immediately following matching
blocks; head metadata reflects the real head (never an artificially lowered value
— ADR-009; under head-lag policy the *served* frontier may sit below it). The current
HTTP binding lacks the coverage cursor (GAP-15/OQ-8), so this requirement is not fully
conformant for selective queries.
*Trace:* ADR-009, ADR-001.

**REQ-3 — Fork detection and recovery.** [MUST]
In real-time mode, if the client-supplied parent hash does not match the canonical
chain, the Portal responds with a CONFLICT carrying the canonical block(s) needed to
re-anchor: at minimum the canonical block at the parent's height, and SHOULD include
enough earlier ancestors for the client to find the last shared block without probing.
Conflict detection takes precedence over the beyond-frontier empty outcome (REQ-5): a
continuation whose parent height is at or below the frontier is validated even when its
first block lies beyond the frontier — a client polling at the head across a reorg gets
the conflict, not an endless empty poll (ADR-014). Finalized streams never conflict.
An operator MAY disable parent-hash validation portal-wide; the parent hash is then
neither validated nor propagated to any source, and conflicts are never raised.
*Acceptance:* a real-time-mode request with a stale parent hash returns HTTP 409 with a
`previousBlocks` list containing at least the canonical block at the parent height;
a continuation from the current head with a stale parent hash returns 409, not 204;
a finalized-stream request never returns 409. The current archival response meets the
MUST minimum with one entry but not the richer-ancestor SHOULD (GAP-7).

**REQ-4 — One surface, two sources.** [MUST]
The stream endpoint serves each request entirely from one source: the archival network
when the requested first block is at or below the archival head, otherwise the real-time
source when the dataset has one. Once a source is selected it is disclosed on that
response. Within one response the Portal never mixes sources (NG4); the client crosses
the boundary by resuming (REQ-2).
*Acceptance:* requests below the archival head return archival-sourced data; requests
above it, on a real-time-enabled dataset, return real-time data; every routed stream
or timestamp response — successful, empty after source selection, or failed after
source selection — carries a source marker header with value `network` or `real_time`.
Alias, validation, and admission failures, and an empty response for which routing
selected no source, occur outside source selection and carry no source marker.

**REQ-5 — Beyond-frontier requests are empty, throttled, and non-erroneous.** [MUST]
A request whose first block lies beyond the data frontier — past the head, or in the
gap between archival history and the real-time retention window — succeeds with an
empty response, delayed by P-NO-DATA-DELAY to pace client polling. It is not an error
and carries no data.
*Acceptance:* such a request returns HTTP 204 after at least P-NO-DATA-DELAY, with no
body but with the head metadata of REQ-2 (the poller sees the frontier it is waiting
on); clients polling at the head observe a duty cycle no faster than one request per
P-NO-DATA-DELAY per connection.
*Trace:* NG3.

**REQ-6 — Every delivered prefix is valid and resumable.** [MUST]
A stream may end before the requested range is exhausted, for any reason (worker
failure, caps, shutdown). Whatever was delivered must be a well-formed prefix: complete
records, valid encoding, ordering per REQ-1. Failures before the first record map to an
error status; failures after it truncate the body without corrupting it. Truncation is
recovered via REQ-2, not signaled in-band.
*Acceptance:* forcibly failing the data source mid-stream yields a decodable response
prefix whose records satisfy REQ-1 and from which a follow-up request continues
correctly.
*Trace:* ADR-001 (deliberate trade-off), OQ-2.

**REQ-7 — Request validation.** [MUST]
Malformed requests are rejected with a BAD REQUEST and a human-readable reason, before
any upstream work: unparsable or unknown-field query bodies; query bodies larger than
P-QUERY-SIZE-LIMIT; more than P-QUERY-MAX-ITEMS item selections; an upper bound below
the first block; zero-valued tuning parameters; a first block before the dataset's
start. Unknown dataset aliases return NOT FOUND.
*Acceptance:* each listed violation returns HTTP 400 (404 for unknown dataset) with the
ADR-011 error envelope and a stable code plus human-readable message, and no worker or
real-time call is made.

**REQ-8 — Client tuning within operator bounds.** [SHOULD]
Clients MAY tune per-request behavior (read-ahead buffer size, chunk count limit).
Client values are clamped to operator maxima (P-BUFFER-MAX, P-MAX-CHUNKS-PER-STREAM),
never trusted beyond them. Tuning parameters the server does not honor must be either
rejected or documented as server-controlled — silently accepting and ignoring them is a
defect.
*Acceptance:* a request with buffer size above P-BUFFER-MAX behaves identically to one
at P-BUFFER-MAX; absent parameters default to P-BUFFER-DEFAULT. (Two advertised
parameters are currently accepted and ignored: GAP-8, OQ-1.)

**REQ-9 — Request correlation.** [MUST]
Every response carries a request identifier: the client's, if supplied, else a generated
one. The identifier is echoed in the response, attached to logs, and propagated to
upstream calls, so one identifier traces a request end to end. Any byte sequence a
client supplies as an identifier is safe (REQ-21).
*Acceptance:* a supplied `x-request-id` is echoed verbatim and appears in Portal logs
for that request; absent one, a unique identifier is generated and echoed.

## Discovery & metadata (10–16)

**REQ-10 — Dataset catalog.** [MUST]
The Portal lists every dataset it serves, with aliases and capability indicators
(archival availability, real-time availability). Datasets it does not serve are
indistinguishable from nonexistent ones (NOT FOUND).
*Acceptance:* the catalog endpoint returns every configured dataset with its aliases;
querying an unlisted alias returns 404.

**REQ-11 — Truthful heads.** [MUST]
Per dataset, the Portal reports the head and finalized head. Real-time-enabled datasets
report live values from the real-time source; archival-only datasets report the highest
assigned block. While the assignment publisher is healthy, an archival head converges
within one successful P-ASSIGNMENT-REFRESH cycle; during publisher failure the Portal
serves the last applied value and no outage-age bound exists today (GAP-2). Heads are
never artificially lowered or raised (ADR-009).
*Acceptance:* reported archival head equals the assignment's highest block for the
dataset; real-time head matches the real-time source's value at request time.

**REQ-12 — Dataset metadata.** [MUST]
Per dataset: aliases, start block, real-time capability, and kind. Metadata freshness is
bounded by one successful P-DATASETS-REFRESH cycle while the registry is healthy.
During a registry outage the last snapshot remains available and may age without bound.
*Acceptance:* metadata for a dataset whose history starts at block N reports start
block N; with successful refreshes, registry changes become visible by the next cycle.

**REQ-13 — Timestamp resolution.** [SHOULD]
The Portal resolves a timestamp to a block number, consulting archival history first and
falling back to the real-time source. Its error and overload semantics match the stream
surface (REQ-20), including the beyond-frontier case: a timestamp not yet reached by the
chain is the throttled empty outcome of REQ-5, never a non-retryable error — the head
advances and the same request later succeeds (ADR-014).
*Acceptance:* a timestamp within available history returns the number of the first block
whose timestamp is greater than or equal to it; a timestamp before available history
returns the first available block, while one after the data frontier returns HTTP 204
after at least P-NO-DATA-DELAY; overload returns the same status, code, and retry hint
as streams.

**REQ-14 — Operator introspection.** [MAY]
The Portal MAY expose operator-facing state (network status, per-dataset worker ranges,
worker pool debug). These surfaces are explicitly unstable: format may change without
notice and clients must not depend on them.
*Acceptance:* introspection endpoints are excluded from the public API description by
default.

**REQ-15 — SQL routing (experimental).** [MAY]
When built with the SQL capability, the Portal accepts a relational query plan and
returns a routing plan — which workers hold which chunks, with per-chunk query text —
without executing anything (NG6). The surface is experimental and carries no stability
promise.
*Acceptance:* a valid plan over a served dataset returns a routing plan naming only
workers present in the current assignment; no data rows are returned.

## Robustness & overload (20–29)

**REQ-20 — Honest load shedding.** [MUST]
The Portal distinguishes three refusal classes and never conflates them: (a) *portal
overload* — stream cap reached or bandwidth saturated — returns the overload status
(HTTP 529) **with** an explicit retry hint of at least P-RETRY-AFTER-MIN; (b) *data
unavailability* — no worker currently holds the requested data — returns 503 **without**
a retry hint; (c) *client error* — 4xx. Shed load must stay shed: every overload
response tells conforming clients when to come back, so refusal traffic decays rather
than amplifies.
*Acceptance:* driving the Portal past P-MAX-STREAMS concurrent streams yields 529 +
`Retry-After`; removing all workers for a dataset yields 503 without `Retry-After`;
no overload path returns a bare 503. Baseline: the 2026-07 refusal storm (314 rps of
instant retries) is the failure mode this prevents.
*Trace:* ADR-007, ADR-011, ADR-012, G3.

**REQ-21 — Hostile-input safety.** [MUST]
No byte sequence a client supplies — headers, path, query parameters, body — may crash,
panic, or wedge the process, or affect any other request. The worst outcome of hostile
input is an error response on that request.
*Acceptance:* fuzzing all client-controlled inputs never terminates the process
(regressions: non-ASCII request-id panic, zero-valued tuning-parameter panic). Latent
counterexample tracked as GAP-5.

**REQ-22 — Bounded upstream interactions.** [MUST]
Every outbound call carries a deadline. Deadlines on the request path are strictly below
the deadlines of clients waiting on them (P-HOTBLOCKS-READ-TIMEOUT <
P-CLIENT-SDK-TIMEOUT), so a stalled upstream surfaces as a Portal-attributed gateway
error and a recorded metric — never as a silent client-side timeout the Portal did not
observe.
*Acceptance:* stalling the real-time source makes the Portal answer 502 within
P-HOTBLOCKS-READ-TIMEOUT and record the failure; no outbound call in the codebase is
deadline-free. The chain-RPC status loop currently violates the latter clause (GAP-18).
*Trace:* ADR-010, ADR-011.

**REQ-23 — Truthful readiness.** [MUST]
The readiness probe answers "can this instance serve correctly right now": ready only
when routing data is loaded and worker connectivity is at or above
P-READY-CONNECTION-RATIO of the known worker set; not-ready immediately once shutdown
begins. *Intent:* readiness also degrades when routing data is older than
P-ASSIGNMENT-MAX-AGE ⚠ — currently not enforced (GAP-2, ADR-013).
*Acceptance:* a fresh instance is not-ready until the first assignment applies; killing
connectivity below the ratio flips readiness within one probe interval; readiness flips
to not-ready at SIGTERM before the listener closes.

**REQ-24 — Graceful shutdown.** [MUST]
Shutdown is two-phase: on SIGTERM the Portal immediately advertises not-ready while
continuing to serve for P-PRE-DRAIN-GRACE (letting load balancers drain it), then stops
accepting work and drains in-flight streams for at most P-DRAIN-TIMEOUT. Total shutdown
never exceeds P-PRE-DRAIN-GRACE + P-DRAIN-TIMEOUT plus a constant; the orchestrator's
kill grace must exceed that sum.
*Acceptance:* under load, SIGTERM → readiness 503 at once; new connections keep being
served during the grace window; process exits within the budget; in-flight streams
either complete or truncate per REQ-6.
*Trace:* ADR-005.

**REQ-25 — Fault isolation across upstreams.** [MUST]
An upstream failure affects only the traffic that needs it: real-time source down ⇒
only real-time requests fail (archival serving, readiness unaffected); chain RPC down ⇒
no effect on any data serving (status reporting degrades only); assignment publisher
down ⇒ the Portal keeps serving from the last applied assignment (subject to the
staleness intent of REQ-23).
*Acceptance:* with the real-time source stopped, archival streams and readiness are
unaffected; with RPC stopped, streams are unaffected and only status output degrades.

**REQ-26 — Routing-artifact integrity.** [MUST — intent, currently violated]
A corrupt, truncated, or semantically invalid assignment artifact must not crash the
Portal, must not be applied, and must not let the Portal report ready on garbage
routing: the artifact is validated (structurally, at minimum cheaply) before use, a bad
artifact is rejected with an alarm, and the previous good artifact stays in service.
*Acceptance:* feeding a truncated or bit-flipped artifact leaves the process alive and
serving from the prior artifact, with an error signal raised. Currently the artifact is
adopted unverified — GAP-1; the trade-off that created this is ADR-002.

**REQ-27 — Bounded memory.** [MUST]
Steady-state memory is bounded in bytes, not merely by object counts. Per-stream slot
and result caps provide a finite theoretical ceiling, but admitted buffers also share a
global P-BUFFERED-BYTES-BUDGET ⚠ chosen so the artifact, runtime overhead, and buffers
fit P-MEMORY-BUDGET ⚠. Admission refuses new work before that byte budget is exceeded,
and refreshing the routing artifact must not require two full copies resident ⚠.
*Acceptance:* memory under saturation load stays within P-MEMORY-BUDGET; an assignment
refresh does not spike resident memory by ~2× the artifact size. Count-only limits do
not satisfy this requirement: at current defaults their multiplicative ceiling is
orders of magnitude above the provisioned budget (GAP-3/GAP-17, OQ-9). Baseline: the
2026-07-17 production OOM-kill restarts.

## Operability (30–34)

**REQ-30 — Metrics.** [MUST]
The Portal exposes an OpenMetrics endpoint covering: stream lifecycle (active,
completed, duration, bytes, blocks), worker query outcomes by class, worker selection,
congestion window state, per-dataset chunk counts and highest block, HTTP responses by
endpoint/status/source, and lock hold times. *Intent:* label cardinality is bounded —
per-worker labels must not grow monotonically with the network (GAP-6).
*Acceptance:* every listed signal is present on the metrics endpoint; a saturated
instance's stall is diagnosable from metrics alone (stream gauge pinned at
P-MAX-STREAMS, congestion window at floor).

**REQ-31 — Structured logging and error reporting.** [SHOULD]
Each request produces a correlated log span (request identifier, dataset, status,
latency); each stream logs periodic progress and a completion summary; errors are
sampled to an external error-reporting sink at P-ERROR-SAMPLE-RATE.
*Acceptance:* given a request identifier, logs reconstruct the request's path and
outcome.

**REQ-32 — Self-describing API.** [SHOULD]
The Portal serves a machine-readable API description and interactive docs. Operator/
internal endpoints are hidden from the description by default (REQ-14). The description
matches actual behavior — undocumented live routes, stale examples, or unlisted
response headers are defects (GAP-11).
*Acceptance:* every public route and response header appears in the served API
description; examples validate against the schemas.

**REQ-33 — Forgiving, safe configuration.** [MUST]
Configuration errors fail fast at startup when values are invalid, but unknown fields
only warn (ADR-008) so schema evolution never turns a config push into an outage. Every
operator knob has a working default; only identity, network endpoints, and the catalog
source are mandatory.
*Acceptance:* a config with an unknown key starts up and logs a warning naming the key;
a config with an invalid value (e.g. congestion floor > ceiling) refuses to start with
a reason.

## Network integration (40–44)

**REQ-40 — Assignment ingestion.** [MUST]
The Portal polls the assignment publisher every P-ASSIGNMENT-REFRESH, skips unchanged
artifacts (by identifier), applies new ones atomically no earlier than their declared
effective time (so the fleet cuts over together), and keeps serving the previous
artifact on any fetch or validation failure. First applied assignment gates readiness
(REQ-23).
*Acceptance:* a new artifact with a future effective time is not visible in routing
until that time; killing the publisher leaves serving unaffected for the duration of
the outage (staleness intent: ADR-013).

**REQ-41 — Worker selection and penalties.** [MUST]
Chunk queries go to the most promising worker holding the chunk: healthy and fast
preferred; workers observed erroring are avoided for P-WORKER-ERROR-COOLDOWN; workers
observed timing out are avoided for P-WORKER-TIMEOUT-COOLDOWN (longer, because a
timeout stalls a stream for up to P-TRANSPORT-TIMEOUT); a worker's requested backoff is
honored (default P-WORKER-BACKOFF); at most P-MAX-QUERIES-PER-WORKER queries run
against one worker concurrently. Failed attempts retry on a different worker, up to
1 + the retry setting (default P-RETRIES-DEFAULT) attempts per chunk.
*Acceptance:* a worker that just erred is not selected again within its cooldown while
alternatives exist; a chunk whose first worker fails is served by another worker within
the same stream.
*Trace:* ADR-004.

**REQ-42 — Bandwidth self-regulation.** [MUST]
Concurrent chunk downloads are governed by an adaptive window between
P-CONGESTION-MIN-WINDOW and P-CONGESTION-MAX-WINDOW (additive increase, multiplicative
decrease by P-CONGESTION-DECREASE, at most one shrink per P-CONGESTION-SHRINK-INTERVAL);
download slots are granted oldest-stream-first, earliest-chunk-first. Above
P-HEADROOM-THRESHOLD utilization, new streams are refused as overload (REQ-20).
*Acceptance:* inducing download timeouts shrinks the window toward the floor and
recovery re-grows it; at saturation new streams receive 529 while running streams
continue.
*Trace:* ADR-006.

**REQ-43 — Response authenticity.** [SHOULD]
Worker responses are signature-verified when verification is enabled (the default);
signed queries identify the Portal to the network.
*Acceptance:* with verification on, a response failing verification is not delivered to
the client and the attempt is retried elsewhere.

**REQ-44 — Usage accounting.** [MAY]
The Portal reports query logs to the network's accounting. Reporting is best-effort and
bounded (queue of P-LOGS-QUEUE); under pressure logs are dropped rather than ever
delaying or failing data serving.
*Acceptance:* saturating the log queue never blocks a stream; drops are observable.

## Explicitly unspecified

Deliberately left open — tests and clients must not pin these:

- Per-chain block-record schemas and within-block item ordering (owned by the query
  dialect definition, not this spec).
- The count and depth of conflict ancestor lists beyond the REQ-3 minimum (the list
  itself is ordered: ascending height, ending at the parent's height — DEF-9).
- Bodies of operator introspection endpoints (REQ-14) and the SQL surface (REQ-15).
- Behavior of deprecated endpoints (NG7), including their status-code conventions.
- Compression codecs beyond those the binding names (IB-1: gzip default, zstd on
  offer); negotiation mechanics beyond the binding's contract.
- Worker identity, ranking internals, and how throughput is measured (REQ-41 pins
  outcomes, not scores).

## Open questions

| ID | Question | Blocks | Owner |
|---|---|---|---|
| OQ-1 | Tuning params `timeout_quantile` and `retries` are advertised but overwritten on public endpoints — honor, reject, or re-document as server-controlled? | REQ-8, GAP-8 | portal team |
| OQ-2 | Should truncation (REQ-6) become client-detectable (e.g. a trailing marker), or stay resolution-by-resume per ADR-001? | REQ-6 | portal + SDK teams |
| OQ-3 | Ratify P-ASSIGNMENT-MAX-AGE and the degraded-readiness semantics (ADR-013). | REQ-23, GAP-2 | portal team |
| OQ-4 | Ratify P-MEMORY-BUDGET and the assignment-size planning figure (docs disagree: ~300 MB vs ~0.5 GB). | REQ-27, GAP-3 | portal team |
| OQ-5 | ADR-009 (head-lag) is accepted but the Portal-side header injection is not implemented — schedule or re-scope? | GAP-13 | portal team |
| OQ-7 | A stream body without a first block silently defaults to block 0, while the API description marks it required — reject instead? | REQ-7 | portal team |
| OQ-8 | Ratify the HTTP field/header carrying DEF-8's coverage cursor so selective zero-record responses can make resumable progress. | REQ-2, GAP-15 | portal + SDK teams |
| OQ-9 | Ratify a global P-BUFFERED-BYTES-BUDGET and its accounting/admission semantics. | REQ-27, GAP-17 | portal team |
| OQ-10 | Ratify the draft SLO target parameters and their benchmark gating policy. | 11 SLO table | portal team |

Closed: **OQ-6** (should the clamp-bypassing debug stream variant be exposed unconditionally,
or gated behind an operator flag?) — resolved by ADR-014: the variant is gated behind an
operator flag and disabled by default (GAP-21 until implemented). OQ numbers are never
recycled.
