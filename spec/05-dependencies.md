# 05 — Dependency contracts

One subsection per downstream system. Error mapping targets the closed taxonomy DEF-10;
no dependency error body or dependency-specific code may leak to a client verbatim. The
cache/staleness table at the end folds in the lifecycle module (10) this shape doesn't
otherwise need.

**The no-undeclared-dependencies rule.** Every network or system interaction of the
Portal appears in this document. Any interaction not listed here observed in operation
is a conformance violation (INV-37).

## DC-1 — Archival workers

*Role.* Serve chunk queries; the archival path of OP-1/OP-5.
*Call contract.* Signed query per attempt; request deadline P-TRANSPORT-TIMEOUT;
first-byte wait bounded by the same; body read in bounded reads of at most
P-CONGESTION-READ-TIMEOUT each under a congestion permit (DEF-13); response size capped
at P-RESULT-MAX-SIZE; at most P-MAX-QUERIES-PER-WORKER concurrent queries per worker.
*Retry policy.* A failed attempt reroutes to a **different** worker, up to 1 + retries
attempts per chunk (default P-RETRIES-DEFAULT); a speculative extra attempt may start
when the current one exceeds the adaptive per-request estimate (quantile
P-TIMEOUT-QUANTILE). Retries never target the failed worker within its cooldown while
alternatives exist.
*Error mapping.*

| Worker fault | Own class / action |
|---|---|
| invalid-query verdict | BAD-REQUEST (terminal for the request) |
| result exceeds size cap | BAD-REQUEST advising a narrower query |
| parent-hash mismatch verdict | CONFLICT (real-time mode only — finalized-mode queries carry no parent hash, REQ-3) |
| server error / not found | reroute; cooldown P-WORKER-ERROR-COOLDOWN; exhausted ⇒ RETRIES-EXHAUSTED |
| timeout / transport failure | reroute; cooldown P-WORKER-TIMEOUT-COOLDOWN; congestion signal; exhausted ⇒ RETRIES-EXHAUSTED |
| rate-limit / overload verdict | honor backoff (worker's hint, default P-WORKER-BACKOFF); all candidates backing off longer than P-MAX-IDLE-TIME ⇒ OVERLOADED |
| integrity failure (bad signature, wrong-range or undecodable result) | discard result, reroute (REQ-43); attempts exhausted on integrity failures ⇒ WORKER-FAILURE (pages — the network serves bad data or verification is broken) |
| no worker leasable for the chunk | DATA-UNAVAILABLE |

*Degradation.* Per-worker penalties (ADR-004) — never a global circuit-break; the pool
degrades worker-by-worker. Health state is in-memory (DEF-12) and resets on restart.

## DC-2 — Assignment publisher

*Role.* Source of the assignment artifact (DEF-4); consulted by a background loop only,
never on a request path.
*Call contract.* Poll every P-ASSIGNMENT-REFRESH; fetch deadline
P-ASSIGNMENT-FETCH-TIMEOUT; unchanged identifier ⇒ no re-download; application waits
for effective-from.
*Error mapping.* Fetch/parse failure → keep serving the applied artifact; alarm
(⚠ today only a log — GAP-2). Never surfaces to clients directly.
*Degradation.* Serve-stale, currently unbounded; intent bounds it at
P-ASSIGNMENT-MAX-AGE ⚠ with degraded readiness (ADR-013). *Integrity:* intent is
validate-before-apply (REQ-26); currently trusted unverified (ADR-002, GAP-1).

## DC-3 — Dataset registry

*Role.* Dataset catalog and metadata (DEF-14); background loop.
*Call contract.* Poll every P-DATASETS-REFRESH; bounded fetch with a small number of
transient-failure retries (exponential backoff).
*Error mapping.* Refresh failure → keep the previous catalog; log. At **startup** a
permanent failure is fatal (the Portal cannot know what it serves).
*Degradation.* Serve-stale, unbounded (accepted: the catalog changes rarely).

## DC-4 — Real-time source

*Role.* Recent blocks near the head; the real-time path of OP-1, head reads (OP-2),
timestamp fallback (OP-5).
*Call contract.* Per-request proxy: connect deadline P-HOTBLOCKS-CONNECT-TIMEOUT;
per-read idle deadline P-HOTBLOCKS-READ-TIMEOUT, strictly below P-CLIENT-SDK-TIMEOUT
(ADR-010); no retries; no redirects. Success and 204 responses stream through with
internal headers stripped; error responses are status-classified and rewritten into the
Portal envelope (ADR-003, amended by ADR-011; IB-4/IB-5).
*Error mapping.*

| Fault | Own class |
|---|---|
| connect/transport failure, read stall past deadline | UPSTREAM-FAILURE (recorded — a stalled upstream must never be invisible, REQ-22) |
| upstream 429 / 503 / 529 | OVERLOADED / `overloaded`; preserve public retry/header semantics, injecting `Retry-After` = P-RETRY-AFTER-MIN when the upstream omitted it (INV-26, ADR-014) |
| other upstream 5xx | UPSTREAM-FAILURE / `upstream_unavailable`; preserve public headers, never the upstream body |
| other upstream 4xx (unmatched) | BAD-REQUEST / `malformed_request` (ADR-011 unmatched-4xx rule); status normalized to 400, upstream body never leaked |
| upstream conflict | CONFLICT / `base_block_mismatch`; preserve `previousBlocks`, add Portal envelope |
| requested range below upstream retention (gap) | EMPTY after P-NO-DATA-DELAY |
| upstream reports unknown dataset (404) | NOT-FOUND / `unknown_dataset`; log the possible configuration incoherence, never leak the upstream body |

*Degradation.* Fail-fast per request; no caching, no health state. An outage affects
only real-time traffic (REQ-25); readiness ignores this dependency by design.

## DC-5 — Chain RPC & contracts

*Role.* Epoch, stake, compute units, worker registry — status/accounting only;
background loop; **never on the serving path** (REQ-25).
*Call contract.* Poll every P-CHAIN-REFRESH; paged contract reads. ⚠ No explicit
per-call deadline exists today (HZ-8, GAP-18), contrary to ADR-010/REQ-22.
*Error mapping.* Failure → keep last snapshot, log; before the first success the status
surface reports a loading state.
*Degradation.* Serve-stale indefinitely; invisible to data clients.

## DC-6 — Usage-log sink

*Role.* Query-log accounting to the network (REQ-44).
*Call contract.* Fire-and-forget batches from a bounded queue (P-LOGS-QUEUE).
*Error mapping / degradation.* Overflow drops logs (observable — HZ-7); sink failure
never delays or fails serving.

## DC-7 — Error-reporting sink

*Role.* Sampled error/trace reports (P-ERROR-SAMPLE-RATE).
*Contract.* Fire-and-forget; failure has no client-visible effect.

## Caches & refreshed snapshots (lifecycle)

| Snapshot | Refreshed by | Staleness bound | Staleness visible? |
|---|---|---|---|
| Applied artifact (DEF-4) | DC-2 poll | one successful P-ASSIGNMENT-REFRESH cycle while healthy; none during outage today; ⚠ P-ASSIGNMENT-MAX-AGE (ADR-013) | intent: age gauge + readiness (GAP-2) |
| Dataset catalog (DEF-14) | DC-3 poll | none (accepted) | no |
| Chain status | DC-5 poll | none (status only) | loading state before first fetch |
| Worker health map (DEF-12) | per-query outcomes | rolling windows (P-WORKER-ERROR-COOLDOWN / P-WORKER-TIMEOUT-COOLDOWN) | operator debug view |
| Heads | artifact (archival) / per-request (real-time) | one successful P-ASSIGNMENT-REFRESH cycle / live; archival outage unbounded | response metadata (INV-24) |

There is no response cache: no client-visible value is ever served from a cache other
than these declared snapshots.

## Client-visible error taxonomy (closed)

The complete set across all operations is DEF-10. Anything not expressible in that
taxonomy is a spec bug, not a new error: extending the taxonomy is a binding change
(IB-5 versioning rule).
