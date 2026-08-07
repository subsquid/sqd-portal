# 02 — Requirements

Bands: 1–9 core data delivery · 10–16 discovery & metadata · 20–29 robustness &
overload · 30–34 operability · 40–44 network integration · 50–56 commercial access
control. Gaps in numbering are reserved; additions never renumber. Acceptance status lives in
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
a coverage cursor `(number, hash)` for the last one — carried as the last delivered
record, which the source always emits (INV-29). A follow-up from cursor number + 1,
carrying the cursor hash as its parent, continues gap-free and overlap-free even when a
selective query emitted no *matching* record at the end of coverage.
*Acceptance:* stream headers expose head and finalized-head markers plus the coverage
cursor; issuing a follow-up from that cursor yields the immediately following matching
blocks; head metadata reflects the real head (never an artificially lowered value
— ADR-009; under head-lag policy the *served* frontier may sit below it). The coverage
cursor is delivered as the last record on every response that evaluates a block (INV-29,
DEF-8), so selective resume is gap-free; there is no dedicated cursor field, by design
(DEF-8).
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
Every response carries a request identifier: the client's, if supplied as ASCII text,
else a generated one. A valid client identifier is echoed in the response, attached to
logs, and propagated to upstream calls, so one identifier traces a request end to end.
A non-ASCII `x-request-id` is malformed input and is rejected safely (REQ-21).
*Acceptance:* a supplied ASCII `x-request-id` is echoed verbatim and appears in Portal
logs for that request; a non-ASCII value receives a 400 `malformed_request` response
with a generated identifier; absent one, a unique identifier is generated and echoed.

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
P-CLIENT-TIMEOUT), so a stalled upstream surfaces as a Portal-attributed gateway
error and a recorded metric — never as a silent client-side timeout the Portal did not
observe. The bound is per upstream call: a connection that dies late in the read budget
is replayed with a fresh one (ADR-015), so a single call's worst case is
2 × P-HOTBLOCKS-READ-TIMEOUT and can exceed P-CLIENT-TIMEOUT. The ordering still holds
where it earns its keep — a stall is never replayed — and the tail costs one upstream
attempt for a caller that has already disconnected, not a silent failure. No requirement
bounds what one *client request* spends upstream: a handler that makes real-time calls in
sequence multiplies the per-call worst case, and already exceeded P-CLIENT-TIMEOUT before
ADR-015 doubled each call.
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
never exceeds P-PRE-DRAIN-GRACE + P-DRAIN-TIMEOUT plus a constant, and the deployment
must give the process that long: P-KILL-GRACE exceeds that sum. The Portal cannot enforce
its own grace — below it the process is killed mid-drain and this requirement is void,
which is an environment defect rather than a Portal one.
*Acceptance:* under load, SIGTERM → readiness 503 at once; new connections keep being
served during the grace window; process exits within the budget; in-flight streams
either complete or truncate per REQ-6; the deployment manifest sets P-KILL-GRACE above
the budget.
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
the outage (staleness intent: ADR-013). Cutting over together assumes the workers wait
too, which they do not today (OQ-11).

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

## Commercial access control (50–56)

This band applies only to a Portal the operator has configured commercially (REQ-56).
On any other deployment every requirement here is vacuous: there is no gate, no grant
cache, and no control-plane dependency. Admission decided here is binary — it never shapes
how much capacity an admitted request may consume (NG2).

**REQ-50 — Gated routes require a valid key.** [MUST]
On a commercial deployment, every request to a gated route (DEF-19) is authorized before
any other work: the presented credential (DEF-16) must be covered by a usable grant
(DEF-17) — one the control plane issued for that exact credential, that has not passed
its hard expiry, and whose claims cover this portal and the requested dataset. A request
that fails any of these is refused in the DEF-10 taxonomy and reaches no handler or serving
dependency. OP-11 may make its one declared control-plane exchange when the grant cache
holds no usable grant, and may canonicalize a dataset only after the credential has
authenticated and only when its dataset scope requires that name (INV-14).
*Acceptance:* against a gated route, a request with no credential, an unparseable token,
an unknown key id, a wrong secret, a revoked key, an expired key, a key scoped to another
portal, and a key scoped to another dataset each receive the refusal ADR-017 binds to it;
no serving-dependency stub records a call for any of them; the control-plane stub records
at most one exchange per request and none at all for a cache hit or a token outside the
grammar; only an authenticated dataset-scoped case may canonicalize the dataset. A valid
unscoped key is served exactly as the same request is served on a non-commercial
deployment.

**REQ-51 — The gated surface is fixed, and every route states whether it is in it.** [MUST]
Block delivery, queries and block lookups require a credential. Everything else — the
catalog, per-dataset metadata and state, heads and heights, the worker and debug lookups,
readiness, metrics and the served API schema — answers without one, on every deployment
(NG8). A route is in one set or the other because it says so at the point it is declared;
there is no default, so a route that says nothing does not compile. Because metrics are
client-readable, their public families never expose an internal authorization reason or
exchange detail beyond the client-visible wire outcome (OB-12/13).
*Acceptance:* a keyless metadata read succeeds and a keyless stream is refused;
`/ready`, `/metrics` and the API schema answer without a credential. A route added
without stating which set it is in fails to compile. Two keyless scrapes bracketing each
case of REQ-50's acceptance corpus differ only in ways that case's own response already
disclosed — no series distinguishes the reasons sharing `invalid_credential` from each
other, and none separates an unknown key id from a known one presented with a wrong
secret.

**REQ-52 — Credential presentation.** [MUST]
A credential is presented as an HTTP bearer token, and by no other channel. In particular
it is never read from the query string: that puts the secret in a URL, and a URL is
recorded by browser history, by `Referer` on every outbound request from a page, and by
the access log of every intermediary in front of the Portal — none of which this system
can see, reach, or clear. Only tokens in a form the control plane can actually mint are
accepted; anything else is refused as an invalid credential without being exchanged
(INV-39). The secret leaves the process in exactly one direction — the DC-8 exchange that
asks the authority about it — and nowhere else. Before that decision it may exist only in
the request-local exchange input DEF-16 bounds; it is never logged, placed in shared state,
echoed, or held past that call, and is matched against the grant cache only as a
fingerprint, in constant time (INV-38).
*Acceptance:* a valid key is accepted through the header and refused when its token is
truncated, over-long, carries an unknown prefix, or contains bytes outside the minted
alphabet; the same token in a query parameter is not a credential at all and the request
is refused as though none were presented; no log record, metric label, error body, shared
or persisted value, or outbound request other than the exchange itself, produced by any
of these, contains the secret.

**REQ-53 — The ladder has a fixed precedence.** [MUST]
Authorization evaluates in one order — credential present → credential well-formed → key
authenticates → not revoked → not expired → portal allowed → dataset allowed — and reports
the first rung that fails. The Portal owns the first two rungs and the last; the four in
between are the control plane's answer to the exchange, and a denial names the earliest of
them that failed. The split does not loosen the order: a well-formed credential is
exchanged before anything else is decided about it, and dataset scope is read from the
grant the exchange returned, so a key that is both revoked and scoped elsewhere reports
revoked. Absent scope means unrestricted (a key with no portal list is valid on any portal;
a key with no dataset list covers every dataset); an empty scope list means nothing, not
everything. A dataset-scoped key may not use a route that names no dataset. Claim semantics
are versioned: a grant whose claim vocabulary this build does not fully understand admits
nothing rather than being read for the parts it recognizes (REQ-54).
*Acceptance:* a key failing several rungs at once reports the earliest — a revoked key
scoped to another dataset reports revoked, and one that is both revoked and expired reports
revoked; a null scope list admits where an empty list refuses; a dataset-scoped key is
refused on a route with no dataset; scope matching is exact, with aliases resolved to
canonical names first, and never by prefix or wildcard. The no-dataset case makes the SQL
surface unusable for every dataset-scoped key, which is fail-closed but may not be the
intent — OQ-13.

**REQ-54 — Fail closed on the credential; bounded, explicit grace on the dependency.** [MUST]
A credential the Portal cannot positively establish as authorized is refused. Positive
establishment is a grant (DEF-17) the control plane issued for that exact credential, still
inside its hard expiry, whose claims this build understands and which covers this portal
and the requested dataset. Around that, four rules:

- **Soft staleness costs nothing.** Past a grant's `refresh_after` the Portal re-exchanges,
  and a request arriving while that runs is still served on the grant in hand.
- **A denial lands at once.** An authoritative denial replaces the cached grant the moment
  it arrives, whatever the grant's remaining lifetime.
- **A dependency failure buys time, and only to `expires_at`.** If the re-exchange cannot
  run or cannot answer, the existing grant keeps serving until its hard expiry and no
  further. That window is the entire outage grace; the control plane sizes it and the Portal
  caps what it will accept at P-GRANT-MAX-LIFETIME. Two deadlines rather than one is what
  buys the grace at all — OQ-14 records what collapsing them would cost.
- **A missing answer is never a verdict.** With no usable grant, an exchange the local budget
  refuses is OVERLOADED and one that failed, timed out, or returned something unusable is
  UPSTREAM-FAILURE. Neither is BAD-CREDENTIAL: the same credential may succeed a second later.

Readiness is conditioned on none of this. A Portal that has never reached the control plane
is ready and refuses retryably (INV-31): every replica shares one authority, so withholding
readiness on its account empties the fleet in exactly the situation nobody can recover from.
*Acceptance:* with the control plane stopped, a credential whose grant is inside its hard
expiry keeps being served and one whose grant has passed it is refused as UPSTREAM-FAILURE,
never as BAD-CREDENTIAL; a key revoked while the control plane is healthy stops being served
no later than LIV-13's bound; with no usable grant, an exchange denied by the local budget
receives OVERLOADED with `Retry-After`, while the same denial during renewal leaves a usable
grant serving and increments the grace signal; a grant offering a lifetime beyond
P-GRANT-MAX-LIFETIME is honored only to the cap; a grant carrying a claims version this
build does not recognize admits nothing; a Portal started with the control plane already
unreachable reports ready and refuses retryably.

**REQ-55 — Shadow enforcement.** [MUST]
The operator may attempt the full ladder without acting on it: every available verdict is
recorded in protected structured observability, and an exchange that cannot produce a
verdict is recorded there as indeterminate. Every request is admitted regardless —
including requests presenting no credential at all, which are not exchanged, there being
nothing to exchange. Shadow mode never refuses and never projects the would-be verdict
onto the keyless metrics surface (OB-12). It is not free: shadow traffic drives real
exchanges against the same budgets enforcement would use, which is the point — the load a
cutover will produce is measured before it decides anything.
*Acceptance:* in shadow mode every request of REQ-50's acceptance corpus is served, each
having recorded the verdict enforcement would have returned or the exchange outcome that
prevented one; valid, invalid, and indeterminate cases all increment the same neutral
public shadow counter; the control-plane stub's ledger shows shadow mode exchanging on
the same cache-miss rule as enforcement.

**REQ-56 — Open by default, never open by accident.** [MUST]
Absent commercial configuration the Portal authorizes nothing, opens no control-plane
dependency, and installs no authorization middleware. Configuration that is present but
carries no settings is a startup error: the one outcome an operator writing it cannot
have intended is the open portal. Which mode is in effect is stated once at startup.
*Acceptance:* with no commercial configuration, gated routes are served without a
credential and no control-plane call is ever made; an empty commercial block fails
startup naming the field it lacks; startup output states whether authorization is on and,
if so, in which enforcement mode.

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
- The shape of a key id or secret beyond the acceptance grammar (REQ-52), and the
  control plane's own issuance, rotation, and organization model — the Portal asks about
  one credential at a time and defines none of them.
- The wire form of a grant. The Portal reads its claims and lifetimes; whether it arrives
  as a signed capability or a typed answer is free while it never leaves the process. The
  request signature that fetches it is *not* free: ADR-018 fixes its headers, canonical
  bytes, algorithm, and encodings for the independently implemented verifier.
- Which internal rejection reason underlies a given `invalid_credential` response: they
  are deliberately indistinguishable to a client (INV-39, ADR-017).

## Open questions

| ID | Question | Blocks | Owner |
|---|---|---|---|
| OQ-1 | Tuning params `timeout_quantile` and `retries` are advertised but overwritten on public endpoints — honor, reject, or re-document as server-controlled? | REQ-8, GAP-8 | portal team |
| OQ-2 | Should truncation (REQ-6) become client-detectable (e.g. a trailing marker), or stay resolution-by-resume per ADR-001? | REQ-6 | portal + SDK teams |
| OQ-3 | Ratify P-ASSIGNMENT-MAX-AGE and the degraded-readiness semantics (ADR-013). | REQ-23, GAP-2 | portal team |
| OQ-4 | Ratify P-MEMORY-BUDGET and the assignment-size planning figure (docs disagree: ~300 MB vs ~0.5 GB). | REQ-27, GAP-3 | portal team |
| OQ-5 | ADR-009 (head-lag) is accepted but the Portal-side header injection is not implemented — schedule or re-scope? | GAP-13 | portal team |
| OQ-7 | A stream body without a first block silently defaults to block 0, while the API description marks it required — reject instead? | REQ-7 | portal team |
| OQ-9 | Ratify a global P-BUFFERED-BYTES-BUDGET and its accounting/admission semantics. | REQ-27, GAP-17 | portal team |
| OQ-10 | Ratify the draft SLO target parameters and their benchmark gating policy. | 11 SLO table | portal team |
| OQ-13 | Should a dataset-scoped key be able to use the SQL surface, which names its datasets in the body rather than the path? Today such a key is refused there outright (REQ-53), which is fail-closed but makes the surface unusable for exactly the customers most likely to be scoped. | REQ-53, OP-10 | portal team |
| OQ-11 | REQ-40's fleet-cutover premise assumes workers also honor `effective_from`; workers currently apply assignments immediately (recorded in the worker suite's open questions, `worker-rs/spec/02`), so each publication opens a window of routing to reshuffling workers (transient `no_workers`/`retries_exhausted` churn). Size the window for worker convergence, or have workers delay too? | REQ-40, REQ-41 | network team |
| OQ-14 | Should a grant carry two deadlines or one? REQ-54 takes `refresh_after` + `expires_at` because one value cannot both keep refreshes off the latency path and bound how long a stale answer is acted on. Collapsing them is simpler and makes convergence exact, at the cost of a synchronous exchange every period and no outage grace at all. | REQ-54, DC-8, LIV-13 | portal + control-plane teams |
| OQ-15 | Ratify P-GRANT-MAX-LIFETIME and the exchange budget (P-GRANT-EXCHANGE-RATE, P-GRANT-EXCHANGE-INFLIGHT, P-GRANT-CACHE-CAPACITY). The lifetime cap is the fleet's worst-case stale-authorization window and the budget decides whose new key gets turned away under a flood (HZ-10); neither has an observed value to reason from yet. | REQ-54, DC-8, HZ-10, HZ-13 | portal team |
| OQ-16 | Does anything need revocation faster than LIV-13's bound? If so it is a push invalidation channel, not a shorter refresh interval — shortening the interval multiplies exchange traffic across the whole working set to shorten one key's window. Adding the channel is a second distributed mechanism and should follow a stated freshness requirement, not precede it. | LIV-13, DC-8 | portal + control-plane teams |

Closed: **OQ-6** (should the clamp-bypassing debug stream variant be exposed unconditionally,
or gated behind an operator flag?) — resolved by ADR-014: the variant is gated behind an
operator flag and disabled by default (GAP-21 until implemented). **OQ-12** (what does an
enforcing Portal do once its key set is older than its staleness bound?) — moot since
ADR-016's 2026-08-07 revision: there is no mirrored key set to age. Its answer survives as
the reasoning REQ-54 and INV-31 still rest on — readiness never turns on the control plane's
availability, because every replica shares one authority. OQ numbers are never recycled.
