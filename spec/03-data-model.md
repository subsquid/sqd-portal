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
source, request identifier, and a **coverage cursor** when the response evaluated any
block. The record sequence obeys INV-20/21/22. The response's **coverage** is the
contiguous range from the requested first block through the highest block the response
evaluated, whether or not those blocks matched a selective query. Its cursor is the
reference `(number, hash)` of that final evaluated block.

Coverage and delivery are distinct: with `includeAllBlocks=false`, a response may
cover blocks after its last record, or cover a non-empty range while returning zero
records. Records alone therefore cannot reveal coverage. Until the HTTP binding exposes
the cursor (GAP-15/OQ-8), a client can safely checkpoint only the last delivered block;
with no record it makes no durable progress and MUST NOT infer that unmatched blocks
were skipped.

**DEF-9 — Continuation and conflict.** A *continuation* from checkpoint `(N, hash(N))`
is a new request with first block N+1 and parent hash = hash(N). Prefer the response's
coverage cursor; if the binding did not expose one, the last delivered record is the
only safe checkpoint. A **conflict** arises when a real-time-mode request's parent hash
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

| Spec outcome | Wire `type` / `code` | Meaning |
|---|---|---|
| BAD-REQUEST | `invalid_request_error` / `malformed_request` | DEF-7 violation or bad parameter |
| NOT-FOUND | `invalid_request_error` / `unknown_dataset` or `not_found` | Alias/surface absent, or lookup has no result |
| EMPTY | `availability_error` / `no_data` | Successful bodyless poll result (INV-27) |
| CONFLICT | `invalid_request_error` / `base_block_mismatch` | Parent-hash mismatch (DEF-9) |
| OVERLOADED | `rate_limit_error` / `overloaded` | Capacity refusal with retry hint |
| DATA-UNAVAILABLE | `availability_error` / `no_workers` | No worker holds the data |
| RETRIES-EXHAUSTED | `availability_error` / `retries_exhausted` | All bounded worker attempts failed transiently |
| UPSTREAM-FAILURE | `availability_error` / `upstream_unavailable` | Dependency failed or exceeded its deadline |
| NOT-READY | `availability_error` / `not_ready` | Readiness probe declines traffic |
| WORKER-FAILURE | `api_error` / `worker_failure` | Worker results violated an owned integrity invariant and rerouting was exhausted (DC-1) |
| INTERNAL | `api_error` / `internal_error` or `unclassified` | Portal invariant failed or a failure escaped classification |

Exact statuses and envelope exceptions are fixed by IB-5. No dependency-specific body
or code extends this set.

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
(d) the **stream census** (count of active streams, monotone stream sequence). Shared
adaptive state may influence *admission, worker choice, coverage extent, and timing* —
never record content (INV-28).

**DEF-13 — Congestion window.** An adaptive bound on concurrent chunk-body downloads,
within [P-CONGESTION-MIN-WINDOW, P-CONGESTION-MAX-WINDOW]; grows additively on success,
shrinks multiplicatively (P-CONGESTION-DECREASE) on a congestion signal, at most once
per P-CONGESTION-SHRINK-INTERVAL. Utilization = in-flight / window.

**DEF-14 — Catalog objects.** Dataset catalog entry: aliases, capabilities (archival,
real-time), kind. Metadata: the above plus start block. Both are periodically refreshed
snapshots (staleness: 05 §caches).

**DEF-15 — Configuration.** The operator-supplied object binding every `P-*` parameter
([15-parameters.md](15-parameters.md)) plus identity (peer key), upstream endpoints,
and the dataset map. Static per process lifetime.

## Input events (background, not client-driven)

| Event | Content | Meaning | Delivery |
|---|---|---|---|
| Artifact publication | New assignment artifact (DEF-4) | Routing world changed | Polled every P-ASSIGNMENT-REFRESH; at-least-once; deduplicated by identifier |
| Catalog update | Dataset catalog/metadata | Served-dataset set changed | Polled every P-DATASETS-REFRESH; last-write-wins |
| Chain status update | Epoch, stake, compute units, worker registry | Accounting/status only | Polled every P-CHAIN-REFRESH; never affects serving (REQ-25) |

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
