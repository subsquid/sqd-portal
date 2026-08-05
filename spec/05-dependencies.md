# 05 — Dependency contracts

One subsection per downstream system. Error mapping targets the closed taxonomy DEF-10;
no dependency error body or dependency-specific code may leak to a client verbatim. The
cache/staleness table at the end folds in the lifecycle module (10) this shape doesn't
otherwise need.

**The no-undeclared-dependencies rule.** Every network or system interaction of the
Portal appears in this document. Any interaction not listed here observed in operation
is a conformance violation (INV-37). DC-8 is conditional on commercial configuration:
observing it on a Portal without one is the same violation (REQ-56).

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
| rate-limit / overload verdict | honor backoff (worker's hint, default P-WORKER-BACKOFF); all candidates backing off longer than P-MAX-IDLE-TIME ⇒ OVERLOADED, as does an exhausted run in which *every* attempt returned one of these two verdicts — the refusal is congestion whether it arrives before the query or as its answer |
| integrity failure (bad signature, wrong-range or undecodable result) | discard result, reroute (REQ-43); exhausted with *every* attempt an integrity failure ⇒ WORKER-FAILURE (pages — the network serves bad data or verification is broken); exhausted with any transient failure among the attempts ⇒ RETRIES-EXHAUSTED, since a later retry can still succeed and the class tells the client whether to come back. Equivocation stays operator-visible either way: it is counted per worker and alarmed regardless of the response class (OB-4/OB-9) |
| no worker leasable for the chunk | DATA-UNAVAILABLE |

*Degradation.* Per-worker penalties (ADR-004) — never a global circuit-break; the pool
degrades worker-by-worker. Health state is in-memory (DEF-12) and resets on restart.
Which penalty class a fault draws is meant to follow its cost; today it does not always,
and the divergence can take the whole pool out at once (GAP-27).

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
per-read idle deadline P-HOTBLOCKS-READ-TIMEOUT, strictly below P-CLIENT-TIMEOUT
(ADR-010); at most one replay, and only for a connection-class fault before the response
head (ADR-015) — a replayed request gets a fresh read budget, so its worst case is twice
the deadline and is not bounded by P-CLIENT-TIMEOUT; HTTP/1.1 only, since the replay's
fault classification reads HTTP/1 error shapes (ADR-015); no redirects. Success and 204
responses stream through with internal headers stripped; error responses are
status-classified and rewritten into the Portal envelope (ADR-003, amended by ADR-011;
IB-4/IB-5).
*Error mapping.*

| Fault | Own class |
|---|---|
| connect/transport failure before the response head | one replay (ADR-015); still failing ⇒ UPSTREAM-FAILURE (recorded) |
| read stall past deadline | UPSTREAM-FAILURE, never replayed (recorded — a stalled upstream must never be invisible, REQ-22) |
| upstream 429 / 529 | OVERLOADED / `overloaded`; preserve public retry/header semantics, injecting `Retry-After` = P-RETRY-AFTER-MIN when the upstream omitted it (INV-26, ADR-014) |
| upstream 503 and other 5xx | UPSTREAM-FAILURE / `upstream_unavailable`; preserve public headers — including a `Retry-After` the upstream sent — but never invent one, and never the upstream body. 503 is unavailability, not congestion: the ADR-007 line, applied to the dependency (ADR-014) |
| other upstream 4xx (unmatched) | BAD-REQUEST / `malformed_request` (ADR-011 unmatched-4xx rule); status normalized to 400, upstream body never leaked |
| upstream conflict | CONFLICT / `base_block_mismatch`; preserve `previousBlocks`, add Portal envelope |
| requested range below upstream retention (gap) | EMPTY after P-NO-DATA-DELAY |
| upstream reports unknown dataset (404) | NOT-FOUND / `unknown_dataset`; log the possible configuration incoherence, never leak the upstream body |

*Degradation.* Fail-fast per request after at most one replay; no caching, no health
state. An outage affects
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

## DC-8 — Control plane (key feed)

Exists only on a commercial deployment (REQ-56); on any other the Portal opens no
connection to it and this contract is vacuous.

*Role.* Publishes the key set the Portal mirrors (DEF-17, DEF-18) and answers direct
lookups for a single key; the source of every authorization decision (OP-11).
*Call contract.* Two interactions, both authenticated with a portal-held service
credential and neither following redirects — a redirected request carries that credential
somewhere the operator did not configure, and a redirected key set is not the control
plane's answer.

1. *Feed read* — a background loop only, never on a request path. Polls every
   P-KEY-SYNC-INTERVAL, reading pages of at most P-KEY-PAGE-LIMIT records forward from
   the held cursor with a per-page deadline P-KEY-FETCH-TIMEOUT. Ticks are serialized; a
   slow drain never overlaps the next poll. One tick drains up to
   P-KEY-MAX-PAGES-PER-TICK rather than applying exactly one page; a larger backlog
   continues on the next tick, and the resulting convergence bound is LIV-13's function
   of page count. A page that carries records without advancing the cursor, or a short
   page whose cursor remains below the head the feed reports, is a broken answer, not an
   empty one — it fails the tick.
2. *Direct lookup (authorize-on-miss)* — on the request path, and only for a key the
   snapshot does not hold, so a key minted seconds ago works before the next tick
   (LIV-14). Bounded by P-KEY-RESOLVE-RATE and at most P-KEY-RESOLVE-INFLIGHT concurrent
   calls; a negative answer is remembered for P-KEY-NEGATIVE-TTL across at most
   P-KEY-NEGATIVE-CAPACITY entries. Every bound is a fail-*closed* bound: exceeding one
   produces an immediate OVERLOADED response with a retry hint, never a delay or an
   admission (HZ-10).

*Error mapping.* No control-plane fault ever reaches a client as itself.

| Fault | Own class / action |
|---|---|
| Feed unreachable, timeout, or non-success status | keep serving the established snapshot; alarm on age (OB-13). Never fails a request by itself (REQ-54) |
| Feed envelope missing a required field | treat as a broken answer, not an empty page — fail the tick, keep the snapshot. A 200 from a wrong route or a half-deployed replica must not read as "the key set is now empty", which would quietly stop delivering revocations |
| Record malformed but identifiable | tombstone that key (DEF-17): it stops authenticating, and the older version it replaces does not survive |
| Record unidentifiable | fail the page; keep the snapshot |
| Record status this build predates | tombstone — an unknown status is fail-closed by the same path as any other unusable record |
| Epoch change, or reported head below the held cursor | the cursor's history is gone: rebuild the snapshot from the start rather than advance (DEF-18) |
| Epoch change *mid-drain* | fail the tick; pages from two epochs compose into a snapshot of neither |
| Lookup: key unknown | refuse the request (BAD-CREDENTIAL); remember the answer for P-KEY-NEGATIVE-TTL |
| Lookup: rate/concurrency bound reached | refuse immediately as OVERLOADED with `Retry-After` ≥ P-RETRY-AFTER-MIN; never queue or claim the credential is bad |
| Lookup: call fails, times out, or returns an unusable answer | refuse as UPSTREAM-FAILURE; the same credential may succeed after recovery, so this is never BAD-CREDENTIAL |
| Lookup: answer names a different key than was asked about | discard the answer and refuse as UPSTREAM-FAILURE |

*Degradation.* Fail-static and unbounded today: the established snapshot serves for as
long as the outage lasts, so a revocation issued during it does not land until the feed
returns (GAP-31). Intent bounds this at P-KEY-SNAPSHOT-MAX-AGE ⚠ (OQ-12). Before the
*first* complete bootstrap there is no snapshot to be static about, and an enforcing
Portal declines readiness instead of refusing every key (INV-31). Nothing survives
restart (NG5): every replica re-reads the feed from the start on boot, which puts the
whole key set on the startup path and in every replica's memory (HZ-11).

## Caches & refreshed snapshots (lifecycle)

| Snapshot | Refreshed by | Staleness bound | Staleness visible? |
|---|---|---|---|
| Applied artifact (DEF-4) | DC-2 poll | one successful P-ASSIGNMENT-REFRESH cycle while healthy; none during outage today; ⚠ P-ASSIGNMENT-MAX-AGE (ADR-013) | intent: age gauge + readiness (GAP-2) |
| Dataset catalog (DEF-14) | DC-3 poll | none (accepted) | no |
| Chain status | DC-5 poll | none (status only) | loading state before first fetch |
| Worker health map (DEF-12) | per-query outcomes | rolling windows (P-WORKER-ERROR-COOLDOWN / P-WORKER-TIMEOUT-COOLDOWN) | operator debug view |
| Heads | artifact (archival) / per-request (real-time) | one successful P-ASSIGNMENT-REFRESH cycle / live; archival outage unbounded | response metadata (INV-24) |
| Key snapshot (DEF-18) | DC-8 feed poll, plus authorize-on-miss for absent keys | LIV-13's page-count-dependent bound while healthy; none during outage today; ⚠ P-KEY-SNAPSHOT-MAX-AGE (OQ-12) | intent: age gauge + alarm (OB-13, GAP-31) |
| Negative key answers | DC-8 lookup | P-KEY-NEGATIVE-TTL; cleared wholesale by a snapshot rebuild | protected lookup events (OB-13) |

There is no response cache: no client-visible value is ever served from a cache other
than these declared snapshots.

## Client-visible error taxonomy (closed)

The complete set across all operations is DEF-10. Anything not expressible in that
taxonomy is a spec bug, not a new error: extending the taxonomy is a binding change
(IB-5 versioning rule).
