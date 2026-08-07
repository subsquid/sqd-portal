# 03 — Data model & definitions

Everything the other documents need to speak precisely, and nothing operational.
Semantics of operations live in [04-operations.md](04-operations.md); dependency
behavior in [05-dependencies.md](05-dependencies.md); invariants in
[07-invariants.md](07-invariants.md).

## Primitives

**DEF-1 — Dataset, alias.** A dataset is a named chain-data collection. It is addressed
by any of its *aliases* (opaque strings; equality is exact byte equality). A dataset has
zero or one archival identity (presence in the assignment artifact) and zero or one
real-time attachment. A dataset with neither is not served.

**DEF-2 — Block reference, block record.** A block reference is the pair (number ∈ ℕ,
hash: opaque string). A *block record* is one line of a stream response: a
self-contained encoding of one block, carrying at least its number and hash, shaped by
the query's field selection. Per-chain record schemas are explicitly unspecified here
(owned by the query dialect).

**DEF-3 — Chunk.** An immutable, contiguous, non-overlapping range of finalized blocks;
the archival network's unit of storage, assignment, and query. Chunks of a dataset are
totally ordered and gap-free from the dataset's start block to the archival head.

**DEF-4 — Assignment artifact.** The routing document the network publishes:
(identifier, effective-from time, worker set, per-dataset chunk sequences — each chunk
carrying its block range and the reference (DEF-2) of its last block — and the chunk →
worker-subset mapping). Identifiers are opaque; artifacts are ordered by their
effective-from times. The **applied artifact** is the single artifact the Portal
currently routes by. An artifact is *applied* atomically, never partially (INV-1), no
earlier than its effective-from time, and never with an effective-from earlier than the
applied one's (regression guard, INV-2).

**DEF-5 — Heads and the frontier.** Per dataset: the **archival head** (the reference
of the last assigned chunk's last block), the **real-time head** (reported by the
real-time source), and the **finalized head** — reported by the real-time source when
the dataset has a real-time attachment, else equal to the archival head (archival
chunks hold only finalized blocks, DEF-3). Every served dataset therefore has a
finalized head. The **data frontier** of a request is the highest block servable for it
right now: in real-time mode, the real-time head when the dataset has a real-time
attachment, else the archival head; in finalized mode, the finalized head. Accepted
policy may cap the data served to selected real-time requests a duration behind the
head (ADR-009); such a request's frontier is the capped one, while reported heads stay
truthful (INV-24).

**DEF-6 — Serving source.** Exactly one of `network` (archival workers) or `real_time`
(the real-time source) is selected for a routed stream or timestamp operation
(INV-13). It is disclosed on routed successful responses — including an empty response
served after source selection — and on failures produced after a source was selected.
Pre-routing failures (alias resolution, validation, admission) and an empty response
for which routing selected no source have no serving source and therefore carry no
source marker.

## Request/response objects

**DEF-7 — Stream request.** Fields: dataset alias; *query* (chain dialect tag, first
block, optional last block, optional parent hash, field selection, item filters,
include-all-blocks flag); *mode* (real-time | finalized); *tuning* (read-ahead size,
chunk count limit — clamped per INV-11). Well-formed iff: body decodes with no unknown
fields, size ≤ P-QUERY-SIZE-LIMIT, item selections ≤ P-QUERY-MAX-ITEMS, last ≥ first,
first ≥ dataset start, tuning values non-zero.

**DEF-8 — Stream response and coverage.** A successful stream response is (metadata,
record sequence). Metadata: current head, finalized head (number and hash), serving
source, and request identifier. The record sequence obeys INV-20/21/22. The response's
**coverage** is the contiguous range from the requested first block through the highest
block the response evaluated, whether or not those blocks matched a selective query. Its
**coverage cursor** is the reference `(number, hash)` of that final evaluated block.

Coverage is recoverable from delivery: the serving source always emits at least the first
and last block of coverage as records — and the boundary of every served chunk — header-only
when they match no filter (INV-29). So
with `includeAllBlocks=false` a response may carry blocks after its last *matching*
record, and the **last delivered record is always the coverage cursor**. A response that
evaluated at least one block therefore delivers at least one record; a truly empty body
is the EMPTY outcome (INV-27, 204), never a covered-but-recordless 200. A client
checkpoints the last delivered record and continues gap-free, selective queries
included. There is deliberately no dedicated cursor field or header: the final covered
block is known only once the stream ends (size-truncation is data-dependent), so it
could ride only in a fragile HTTP trailer — whereas the last record carries it in-band
and already includes the hash for fork-safe continuation (DEF-9).

**DEF-9 — Continuation and conflict.** A *continuation* from checkpoint `(N, hash(N))`
is a new request with first block N+1 and parent hash = hash(N). The checkpoint is the
coverage cursor; because the source always emits the last block of coverage (INV-29,
DEF-8), the last delivered record *is* that cursor and is always a safe checkpoint,
selective queries included. A **conflict** arises when a real-time-mode request's parent hash
disagrees with canonical data, regardless of whether routing selected `network` or
`real_time`; its payload is a non-empty list of canonical block references, ordered by
ascending height and ending at the parent's height (recovery algorithm: binding, IB-5).
Conflict detection precedes the beyond-frontier EMPTY outcome (INV-23).

**DEF-10 — Error taxonomy.** Public errors use ADR-011's closed two-axis vocabulary.
`type` determines broad retry/page policy; `code` is the stable client discriminant.
Uppercase names below are specification shorthand for exact wire pairs; they are not
additional public values.

| Type | Retryable | Meaning |
|---|---|---|
| `invalid_request_error` | no | The request cannot succeed unchanged |
| `rate_limit_error` | yes, after hint | Capacity is exhausted |
| `availability_error` | yes | Data or a dependency is temporarily unavailable |
| `api_error` | no | A Portal-owned invariant failed; page |
| `authentication_error` | no | The credential is absent, unreadable, or does not authenticate (ADR-011) |
| `permission_error` | no | The credential authenticated but does not cover this request (ADR-011) |

Both credential types answer 403, so the status never distinguishes them (ADR-011).

| Spec outcome | Wire `type` / `code` | Meaning |
|---|---|---|
| BAD-REQUEST | `invalid_request_error` / `malformed_request` or `method_not_allowed` | DEF-7 violation, bad parameter, or a verb the surface does not serve |
| NOT-FOUND | `invalid_request_error` / `unknown_dataset` or `not_found` | Alias/surface absent, or lookup has no result |
| CONFLICT | `invalid_request_error` / `base_block_mismatch` | Parent-hash mismatch (DEF-9) |
| OVERLOADED | `rate_limit_error` / `overloaded` | Capacity refusal with retry hint |
| DATA-UNAVAILABLE | `availability_error` / `no_workers` | No worker holds the data |
| RETRIES-EXHAUSTED | `availability_error` / `retries_exhausted` | All bounded worker attempts failed transiently |
| UPSTREAM-FAILURE | `availability_error` / `upstream_unavailable` | Dependency failed or exceeded its deadline |
| NOT-READY | `availability_error` / `not_ready` | Readiness probe declines traffic |
| WORKER-FAILURE | `api_error` / `worker_failure` | Worker results violated an owned integrity invariant and rerouting was exhausted (DC-1) |
| INTERNAL | `api_error` / `internal_error` or `unclassified` | Portal invariant failed or a failure escaped classification |
| NO-CREDENTIAL | `authentication_error` / `missing_credential` | A gated route was reached with no credential (REQ-50) |
| BAD-CREDENTIAL | `authentication_error` / `invalid_credential` | The token is unparseable, names no known key, or its secret does not match — one code for all three, by design (INV-39) |
| REVOKED | `authentication_error` / `revoked_credential` | The key authenticated but the control plane has withdrawn it |
| EXPIRED | `authentication_error` / `expired_credential` | The key authenticated but its expiry has passed |
| WRONG-PORTAL | `permission_error` / `portal_not_allowed` | The key is scoped to portals not including this one |
| WRONG-DATASET | `permission_error` / `dataset_not_allowed` | The key is scoped to datasets not including the requested one, or the route names no dataset and the key is dataset-scoped |

EMPTY is deliberately absent: a bodyless poll result is the correct answer to a range
that is not produced yet (INV-27), not a failure, so it carries no `type` and no `code`.
It is bound by IB-4 and observed as `status="204"`, which is exactly what a code on it
would have restated.

The last six rows exist only on an authorizing deployment (REQ-56) and are never
`api_error`: refusing an unauthenticated request is the system working, and must not
page (ADR-011). None of them is retryable and none carries a retry hint.

Exact statuses and envelope exceptions are fixed by IB-5. No dependency-specific body
or code extends this set.

## Access control (authorizing deployments only)

Every definition in this section is vacuous on a Portal with no authorization
configuration: nothing constructs these objects and no route consults them (REQ-56).

**DEF-16 — Credential.** What a client presents: the pair (**key id**, **secret**). The
key id is public and identifies a key; the secret is proof of holding it. The Portal
reduces the presented token to a **fingerprint** — a digest over the *whole* credential,
id and secret together — at the moment the request is parsed. The raw credential remains
only as request-local exchange input: a cache or negative-answer hit destroys it
immediately; on a miss, one request moves it into the single in-flight DC-8 exchange and
every coalesced waiter destroys its copy. The exchange owner destroys it on completion,
timeout, or cancellation. It is never shared or cached; only the fingerprint is. A
credential is *well-formed* iff it is presented through the channel IB-9 names and both
segments fall within the
grammar the control plane can mint; anything else is an invalid credential and is refused
as BAD-CREDENTIAL without being exchanged. NO-CREDENTIAL is reserved for a gated request
that presents no credential at all.

**DEF-17 — Grant.** The control plane's answer to one exchange: a short-lived
authorization for one credential, carrying (**claims version**; the **subject** key id;
**dataset scope**; **`refresh_after`**; **`expires_at`**). Portal scope is not a claim the
Portal evaluates — the exchange knows which portal is asking, and a key that does
not cover it is denied rather than granted. A scope is either *absent*, meaning
unrestricted, or a list matched exactly — an empty list therefore matches nothing, and the
two are never conflated (REQ-53). The two lifetimes are the control plane's to choose and
the Portal's only to cap (P-GRANT-MAX-LIFETIME): `refresh_after` is when the answer should
be renewed, `expires_at` when it may no longer be acted on. A grant whose claims version
this build does not fully understand is unusable rather than partly usable — reading a
newer vocabulary for the parts it recognizes is how an added restriction becomes an
accidental permission (REQ-54).

A **denial** is the exchange's other authoritative answer: the earliest ladder rung that
failed (REQ-53), which the Portal maps to its DEF-10 row. A denial is not a grant and is
never cached as one; it is remembered only as a negative answer, briefly and under a bound
(DC-8).

**DEF-18 — Grant cache.** The Portal's bounded in-memory map from credential fingerprint
(DEF-16) to grant, holding at most P-GRANT-CACHE-CAPACITY entries. It is keyed on the whole
credential and never on the key id: an entry reached by id alone would admit the next caller
to name that id without proving it holds the secret. Nothing populates it but exchanges the
requests themselves triggered — there is no bootstrap, no background fill, and no
authorization state a replica holds that some request did not put there. Nothing survives
restart (NG5), and a cold replica is not a degraded one: it is one whose first request per
credential costs an exchange.

**DEF-19 — Gated route.** A route that requires a credential on an authorizing deployment:
those delivering blocks, query results, or block lookups. Every other route answers
without one (NG8). Which it is, is a property of the route rather than of the request or
of the configuration, and is stated where the route is declared — a route that states
neither does not compile (REQ-51).

**DEF-20 — Authorization verdict.** The outcome of evaluating DEF-16 against a grant or
denial (DEF-17) for one request: **admit**, or **reject** carrying the first failed rung of
REQ-53's ladder. The verdict is a pure function of (credential, grant or denial, requested
dataset, current time) — it consults no capacity, no load, and no prior request (INV-15).
An exchange required because no usable grant exists may fail before a verdict: budget
exhaustion is OVERLOADED and a failed, timed-out, or unreadable exchange is
UPSTREAM-FAILURE (DC-8), both retryable system outcomes rather than claims about the
credential. A renewal suppressed while its old grant remains usable does not replace that
grant's verdict. A verdict is separately *acted on* or not, per the
enforcement mode (REQ-55): shadow mode discards a verdict when one exists, and records an
indeterminate exchange when one does not. The rung is the internal reason; what reaches the
client is the DEF-10 row it maps to, which is deliberately coarser (INV-39).

## Shared state (the frame of the stateless shape)

**DEF-11 — Request-scoped state.** Per stream: the validated request, a read-ahead
window of at most `read-ahead size` chunk slots each buffering at most
P-STORED-RESULTS-PER-CHUNK results, per-request timing estimates, and open worker
leases. All of it is discarded when the response ends. Nothing else persists per
request: no sessions, no per-client identity, no response cache.

**DEF-12 — Shared adaptive state.** The only state shared across requests, all
in-memory and reset by restart: (a) the applied artifact (DEF-4) and catalog snapshots;
(b) the **worker health map** — per worker: open-lease count, error/timeout cooldown
marks, backoff-until, throughput estimate; (c) the **congestion window** (DEF-13);
(d) the **stream census** (count of active streams, monotone stream sequence); and, on an
authorizing deployment, (e) the grant cache, the negative-answer cache, and the exchange
limiters (DEF-18, DC-8). Shared adaptive state may influence *admission, worker choice,
coverage extent, and timing* — never record content (INV-28).

**DEF-13 — Congestion window.** An adaptive bound on concurrent chunk-body downloads,
within [P-CONGESTION-MIN-WINDOW, P-CONGESTION-MAX-WINDOW]; grows additively on success,
shrinks multiplicatively (P-CONGESTION-DECREASE) on a congestion signal, at most once
per P-CONGESTION-SHRINK-INTERVAL. Utilization = in-flight / window.

**DEF-14 — Catalog objects.** Dataset catalog entry: aliases, capabilities (archival,
real-time), kind. Metadata: the above plus start block. Both are periodically refreshed
snapshots (staleness: 05 §caches).

**DEF-15 — Configuration.** The operator-supplied object binding every `P-*` parameter
([15-parameters.md](15-parameters.md)) plus identity (peer key), upstream endpoints,
and the dataset map; on an authorizing deployment it also binds the control-plane endpoint,
the enforcement mode, and the signing identity DC-8 authenticates with. Static
per process lifetime.

## Input events (background, not client-driven)

| Event | Content | Meaning | Delivery |
|---|---|---|---|
| Artifact publication | New assignment artifact (DEF-4) | Routing world changed | Polled every P-ASSIGNMENT-REFRESH; at-least-once; deduplicated by identifier |
| Catalog update | Dataset catalog/metadata | Served-dataset set changed | Polled every P-DATASETS-REFRESH; last-write-wins |
| Chain status update | Epoch, stake, compute units, worker registry | Accounting/status only | Polled every P-CHAIN-REFRESH; never affects serving (REQ-25) |

Authorization is deliberately absent from this table. Nothing pushes key state at the
Portal and no loop polls for it: a grant exists because a request asked for one (DC-8),
which is what keeps a replica's authorization state the size of its own traffic.

## Operation summary

Semantics in [04-operations.md](04-operations.md).

| Op | Name | Purpose |
|---|---|---|
| OP-1 | Stream | Deliver ordered block records for a range (modes: real-time, finalized) |
| OP-2 | Head read | Report head / finalized head / archival head |
| OP-3 | Metadata read | Dataset metadata (DEF-14) |
| OP-4 | Catalog list | List served datasets |
| OP-5 | Timestamp resolve | Timestamp → block number |
| OP-6 | Dataset state read | Operator view: worker → range map |
| OP-7 | Status read | Operator view: network/portal status |
| OP-8 | Readiness probe | Can this instance serve correctly now |
| OP-9 | Metrics read | Observability snapshot (12) |
| OP-10 | SQL route plan | Experimental: relational plan → worker/chunk routing |
| OP-11 | Request authorization | Admission step preceding OP-1..OP-10 on an authorizing deployment (REQ-50) |

## Terminology cross-reference (codebase → spec)

| Code term | Spec term |
|---|---|
| assignment / `visible_assignment` | Assignment artifact, applied artifact (DEF-4) |
| `DataChunk`, `ChunkId` | Chunk (DEF-3) |
| `WorkersPool`, priorities, cooldowns | Worker health map (DEF-12) |
| `StreamController`, `ChunkSlot`, buffer | Request-scoped read-ahead window (DEF-11) |
| `DownloadScheduler`, AIMD window | Congestion window (DEF-13) |
| `TaskManager`, `running_tasks` | Stream census (DEF-12) |
| `NoData` / delayed 204 | EMPTY (DEF-10, INV-27) |
| `BusyFor`, `TooManyStreams`, `RateLimitExceeded` | OVERLOADED (DEF-10) |
| `NoAvailableWorkers` | DATA-UNAVAILABLE (DEF-10) |
| `BaseBlockMismatch`, `previousBlocks` | CONFLICT (DEF-9, DEF-10) |
| hotblocks | Real-time source (DC-4) |
| `x-sqd-data-source` | Serving source marker (DEF-6) |
| `auth:` block / gate | Authorization configuration, and the gate it installs (REQ-56, DEF-19) |
| ladder, rung | REQ-53's ordered precedence; the rung is DEF-20's internal reason |
| `log_only` / `enforce` | Shadow and enforcing modes (REQ-55) |
| exchange | The one control-plane call: credential in, grant or denial out (DC-8) |
| fingerprint | The digest of a whole credential that keys the grant cache (DEF-16, DEF-18) |
