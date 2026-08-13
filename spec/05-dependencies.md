# 05 — Dependency contracts

One subsection per downstream system. Error mapping targets the closed taxonomy DEF-10;
no dependency error body or dependency-specific code may leak to a client verbatim. The
cache/staleness table at the end folds in the lifecycle module (10) this shape doesn't
otherwise need.

**The no-undeclared-dependencies rule.** Every network or system interaction of the
Portal appears in this document. Any interaction not listed here observed in operation
is a conformance violation (INV-37). DC-8 is conditional on authorization configuration:
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
| stale-envelope verdict (the worker's clock disagrees with ours beyond the protocol's freshness window) | reroute; cooldown P-WORKER-ERROR-COOLDOWN; exhausted ⇒ RETRIES-EXHAUSTED. Never BAD-REQUEST: the query is well-formed and the next worker may accept it unchanged |
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

## DC-8 — Control plane (credential exchange)

Exists only on an authorizing deployment (REQ-56); on any other the Portal opens no
connection to it and this contract is vacuous.

*Role.* Answers one question — is *this* credential authorized here, and under what claims
— and is the source of every authorization decision (OP-11). It publishes nothing and the
Portal mirrors nothing.

*Call contract.* One interaction, the **exchange**: the presented credential (DEF-16) in,
a grant or a denial (DEF-17) out. It runs on the request path, and only when the cache holds
no *fresh* grant for that credential's fingerprint — after a token outside the grammar has
already been refused, that means an unseen credential or one whose grant has passed
`refresh_after`. Freshness governs when an exchange happens; usability — not past
`expires_at` — governs whether the grant in hand may still answer while it does.

- *Authenticated by signature.* Every request carries exactly one `X-Portal-Id`,
  `X-Signature-Timestamp`, and `X-Signature`; the last covers the canonical binding
  under the Portal's configured Ed25519 identity. The control plane rejects absent,
  repeated, or malformed signing headers, a portal with no matching active registered key,
  and a timestamp whose absolute skew exceeds P-SIGNATURE-MAX-SKEW.
- *No redirects.* A redirected exchange carries a client's credential somewhere the
  operator did not configure, and its answer is not the control plane's.
- *Deadline.* P-GRANT-EXCHANGE-TIMEOUT per call, strictly below P-CLIENT-TIMEOUT (ADR-010)
  — a caller must never still be waiting on an exchange the Portal has stopped waiting for.
- *Bounded, without shortening a live grant.* At most P-GRANT-EXCHANGE-RATE exchanges per
  second and P-GRANT-EXCHANGE-INFLIGHT concurrent, with one *logically active* call per
  fingerprint, so a burst on the same credential costs one exchange rather than one per
  request. Each attempt owns a locally monotone generation in that fingerprint's
  coordination state. Timing out or replacing an attempt retires its generation before a
  successor starts; a late transport completion from a retired generation cannot mutate
  either cache. The generation is local coordination metadata, not a grant field. A
  denial is remembered for P-GRANT-NEGATIVE-TTL across at most P-GRANT-NEGATIVE-CAPACITY
  entries. If no usable grant exists, a rate or in-flight bound produces an immediate
  OVERLOADED response with a retry hint — never a queue or an admission (HZ-10). If a grant
  remains inside `expires_at`, the same bound merely suppresses that renewal attempt: the
  request is served on the grant and the skipped renewal is observed as grace-serving.
- *Renewal is off the latency path.* A request arriving past `refresh_after` is served on
  the grant in hand while the exchange runs; only a request with no usable grant waits for
  one. Renewals are spread by up to P-GRANT-REFRESH-JITTER, so a cohort of grants issued
  together does not come back together (HZ-12).

*Error mapping.* No control-plane fault ever reaches a client as itself.

| Fault | Own class / action |
|---|---|
| Exchange denies the credential | refuse on the rung it names (REQ-53), mapped to its DEF-10 row; evict any cached grant for that fingerprint at once; remember the denial for P-GRANT-NEGATIVE-TTL |
| Exchange unreachable, times out, or returns a non-success status | serve on a cached grant that has not passed `expires_at`, if one exists; otherwise refuse as UPSTREAM-FAILURE. Never BAD-CREDENTIAL — the credential was never judged (REQ-54) |
| Answer missing a required field, or carrying a claims version this build does not understand | unusable, not permissive: handled as a failed exchange. Reading a newer vocabulary for the parts it recognizes is how an added restriction becomes an accidental permission (DEF-17) |
| Answer about a credential other than the one asked about | discard and refuse as UPSTREAM-FAILURE |
| Answer offering a lifetime beyond P-GRANT-MAX-LIFETIME | accept the grant, capped at the bound, and count it — a control plane drifting past the cap is a misconfiguration an operator should see before it becomes an incident |
| Signing headers absent, repeated, malformed, unattributable, or outside P-SIGNATURE-MAX-SKEW in either direction | UPSTREAM-FAILURE like any other failed exchange, and alarm. The cause is the Portal's own clock, identity, or request construction, not the client's key, and it fails every exchange at once |
| Rate or in-flight bound reached | with no usable grant, refuse immediately as OVERLOADED with `Retry-After` ≥ P-RETRY-AFTER-MIN; with a grant still inside `expires_at`, skip the renewal and serve on that grant. Never queue, and never claim the credential is bad |
| Cache at P-GRANT-CACHE-CAPACITY | evict by least-recent use; the evicted credential's next request is an ordinary miss. Sustained eviction of live grants is the HZ-13 capacity signal, not a correctness event |

*Degradation.* Fail-static, and bounded by construction: a cached grant rides an outage out
to its `expires_at` and no further, so the worst-case stale-authorization window is one the
control plane chose and the Portal capped. Past it, and for every credential this replica
has not cached, an outage means refusals — retryable, attributed to the dependency, and
never converted into a claim about anyone's key. That is the deliberate direction of failure
a quiet control plane closes the gate rather than freezing it open, at the price
of being a dependency the deployment must run like a production service. Readiness never
turns on it (INV-31): every replica shares the same authority, so withholding readiness
fleet-wide would answer an outage with an outage. Nothing survives restart (NG5), and
nothing needs to — a cold replica has no bootstrap to do, only a first exchange for each
credential it serves.

## Caches & refreshed snapshots (lifecycle)

| Snapshot | Refreshed by | Staleness bound | Staleness visible? |
|---|---|---|---|
| Applied artifact (DEF-4) | DC-2 poll | one successful P-ASSIGNMENT-REFRESH cycle while healthy; none during outage today; ⚠ P-ASSIGNMENT-MAX-AGE (ADR-013) | intent: age gauge + readiness (GAP-2) |
| Dataset catalog (DEF-14) | DC-3 poll | none (accepted) | no |
| Chain status | DC-5 poll | none (status only) | loading state before first fetch |
| Worker health map (DEF-12) | per-query outcomes | rolling windows (P-WORKER-ERROR-COOLDOWN / P-WORKER-TIMEOUT-COOLDOWN) | operator debug view |
| Heads | artifact (archival) / per-request (real-time) | one successful P-ASSIGNMENT-REFRESH cycle / live; archival outage unbounded | response metadata (INV-24) |
| Grant cache (DEF-18) | DC-8 exchange, on the request that needs it | each grant's own `refresh_after` while healthy, `expires_at` absolutely — the only snapshot here with a hard bound during an outage | enforcing mode: grace count + minimum remaining expiry and exchange-outcome counters; shadow mode: protected events only (OB-13), alarmed on sustained grace (OB-9) |
| Negative answers (denials) | DC-8 exchange | P-GRANT-NEGATIVE-TTL | protected exchange events (OB-13) |

There is no response cache: no client-visible value is ever served from a cache other
than these declared snapshots.

## Client-visible error taxonomy (closed)

The complete set across all operations is DEF-10. Anything not expressible in that
taxonomy is a spec bug, not a new error: extending the taxonomy is a binding change
(IB-5 versioning rule).
