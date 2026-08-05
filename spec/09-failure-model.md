# 09 — Failure model

Response verbs: **mask** (absorb, client unaffected) · **degrade** (serve with reduced
scope/freshness, visibly) · **fail-safe** (refuse cleanly in the DEF-10 taxonomy) ·
**alarm** (raise an operator-visible signal, OB-9). A row may combine verbs.

## Global requirements

**FM-1 — No externally-triggered termination.** No input from any client or dependency
— malformed, hostile, oversized, or stalled — terminates the process. A fault inside
one stream's serving path truncates that stream only (INV-36, INV-35).

**FM-2 — Transient vs integrity classification.** Transient faults (timeouts,
connection failures, 5xx) are retried or rerouted only where the dependency contract
declares it — rerouting across workers (DC-1), one connection-class replay before the
response head (DC-4, ADR-015) — otherwise they fail-safe in the DEF-10 taxonomy. Integrity
faults (corrupt artifact, signature failure, contradictory data) are never retried
blindly: the offending input is rejected and alarmed; the last good state stays in
service.

**FM-3 — Blast-radius containment.** A fault's effect is confined to the traffic that
needs the faulty component: one worker ⇒ reroute; the real-time source ⇒ real-time
requests only; the publisher ⇒ freshness only; chain RPC ⇒ status only (REQ-25).

## Client-side faults

| Fault | Required response |
|---|---|
| Malformed body / unknown fields / oversized query | fail-safe BAD-REQUEST before any upstream work (INV-10) |
| Hostile header/param bytes (any encoding) | fail-safe; never panic (INV-36) |
| Zero/absurd tuning values | fail-safe BAD-REQUEST; over-cap values mask via clamping (INV-11) |
| Stalled reader (not draining) | backpressure within the stream's buffer bounds; no unbounded buffering; stream may truncate at shutdown (LIV-11) |
| Mid-response disconnect | mask; reclaim everything within LIV-10's bound |
| Instant-retry storm (ignoring hints) | fail-safe per request (cheap refusal path); capacity for admitted work preserved (INV-12) |

## Worker faults (DC-1)

| Fault | Required response |
|---|---|
| Slow (past adaptive estimate) | mask — speculative parallel attempt (FV-2) |
| Timeout | mask via reroute + cooldown P-WORKER-TIMEOUT-COOLDOWN; congestion signal |
| Erroring | mask via reroute + cooldown P-WORKER-ERROR-COOLDOWN |
| Rate-limiting | mask via backoff honor; all-candidates-limited ⇒ fail-safe OVERLOADED |
| Oversized response | fail-safe BAD-REQUEST (advise narrower query) |
| Equivocating (bad signature / wrong-range data) | integrity: discard, reroute, count (REQ-43); never delivered; all attempts equivocating ⇒ fail-safe WORKER-FAILURE (pages); equivocation mixed with transient failures ⇒ fail-safe RETRIES-EXHAUSTED, the equivocation still counted and alarmed (DC-1) |
| Fork verdict (parent mismatch) | fail-safe CONFLICT (INV-23) |
| All attempts exhausted transiently | fail-safe RETRIES-EXHAUSTED listing nothing sensitive; alarm-adjacent counter |

## Publisher faults (DC-2)

| Fault | Required response |
|---|---|
| Unreachable / fetch timeout | degrade serve-stale + alarm ⚠ (today: log only — GAP-2) |
| Corrupt / truncated artifact | integrity: reject, keep last good, alarm — **intent** (today: adopted unverified — GAP-1, ADR-002) |
| Stale (identifier never advances) | degrade + alarm past P-ASSIGNMENT-MAX-AGE ⚠ (ADR-013) |
| Regressive (older identifier republished) | mask — application legality ignores it (INV-2) |

## Real-time source faults (DC-4)

| Fault | Required response |
|---|---|
| Down / connect refused / connection closed before the response head | mask one replay (ADR-015); still failing ⇒ fail-safe UPSTREAM-FAILURE; archival traffic and readiness unaffected (FM-3) |
| Stalled read | fail-safe UPSTREAM-FAILURE within P-HOTBLOCKS-READ-TIMEOUT, never replayed, recorded (ADR-010) |
| Upstream 429 / 529 | fail-safe `overloaded` envelope; public headers retained, `Retry-After` injected at P-RETRY-AFTER-MIN when absent (INV-26); upstream body never leaked (ADR-011) |
| Upstream 503 and other erroring 5xx | fail-safe `upstream_unavailable` envelope; public headers retained — a `Retry-After` the upstream sent is passed on, none is invented — upstream body never leaked (ADR-011, ADR-014) |
| Erroring 4xx (unmatched) | fail-safe `malformed_request` envelope at 400; upstream body never leaked (DC-4) |
| Mid-proxy failure | truncate per INV-25; never replayed — a replay past the response head would duplicate a prefix (ADR-003, ADR-015) |
| Retention gap | fail-safe EMPTY (INV-27) |

## Control-plane faults (DC-8)

Commercial deployments only. The governing asymmetry: a fault in the *key* fails closed,
a fault in the *feed* fails static. Refusing every request because the control plane is
unreachable would convert its outage into the Portal's (REQ-54).

| Fault | Required response |
|---|---|
| Feed unreachable / timeout / error status | serve-static on the established snapshot; no request fails for this reason, at any age. Past P-KEY-SNAPSHOT-MAX-AGE the deployment alarms (OB-9/13) and the Portal's own behaviour is unchanged — that is the whole response (closed OQ-12) |
| Feed answers 200 with a malformed envelope | integrity: fail the tick, keep the snapshot, alarm. Never read as "the key set is empty" — that would silently stop delivering revocations |
| Record malformed but identifiable | integrity: tombstone the key (fail-closed), count, keep serving everything else |
| Record unidentifiable | integrity: fail the page; snapshot unchanged |
| Record with an unrecognized status | fail-closed tombstone — a status this build predates must never admit traffic |
| Feed epoch change / head below cursor | mask: rebuild the snapshot from the start (DEF-18); mid-drain, fail the tick and rebuild on the next |
| No snapshot ever established, enforcing | fail-safe: decline readiness (INV-31) — leave rotation rather than 401 every valid key |
| No snapshot ever established, shadow mode | mask: admit everything, stay ready (REQ-55) |
| Lookup rate-limited or over the in-flight cap | fail-safe: refuse immediately as OVERLOADED with a retry hint; never admit, queue, or claim the credential is invalid (HZ-10) |
| Lookup unavailable, times out, or returns an unusable answer | fail-safe: refuse as UPSTREAM-FAILURE; the unchanged credential may succeed after recovery |
| Lookup answers about a different key | integrity: discard and refuse as UPSTREAM-FAILURE |
| Service credential missing or empty at startup | fail-safe at startup: refuse to run (REQ-33) |
| Commercial block present but empty | fail-safe at startup: refuse to run — the open portal is the one outcome nobody configuring it intended (REQ-56) |

## Other dependencies

| Fault | Required response |
|---|---|
| Registry (DC-3) down at startup | fail-safe: refuse to start (can't know what it serves) |
| Registry down at refresh | degrade serve-stale, log |
| Chain RPC (DC-5) down | degrade status only; loading state before first fetch; serving unaffected |
| Log sink (DC-6) slow/full | mask: drop logs, count drops (HZ-7); never block serving |
| Error sink (DC-7) down | mask |

## Process & operator faults

| Fault | Required response |
|---|---|
| Panic inside a stream task | truncate that stream (INV-25); process lives (FM-1) |
| Panic in a background loop | alarm; loop restarts or the failure is surfaced — a dead refresh loop must not be silent (ties to GAP-2) |
| Memory pressure beyond P-MEMORY-BUDGET | fail-safe refusal preferred over process death — byte-budget admission is the pressure valve (REQ-27; today unmet — GAP-3/GAP-17) |
| Kill grace below the shutdown budget (P-KILL-GRACE) | environment defect, outside the Portal's reach: the process dies mid-drain, streams are severed without a well-formed truncation (INV-25) and LIV-11's bound is void — the deployment must raise it (REQ-24) |
| Invalid config values | fail-safe at startup: refuse to run (REQ-33) |
| Unknown config keys | mask + warn (ADR-008) |
| Dual instance behind one address | tolerated: no shared mutable state exists outside the process (NG5) |

## Fault → property → check cross-reference

| Fault family | Properties | Check |
|---|---|---|
| Client-side | INV-10, INV-11, INV-36, INV-35, LIV-10 | CT-4, CT-9, CT-3 |
| Worker | INV-20, INV-22, INV-23, INV-26, LIV-2, LIV-7, LIV-12 | CT-2 fault matrix |
| Publisher | INV-1, INV-2, INV-31, LIV-6, REQ-26 | CT-2 |
| Real-time source | INV-25, INV-26, LIV-2, REQ-22, REQ-25 | CT-2 |
| Process/operator | FM-1, INV-30, LIV-11, REQ-33 | CT-2, CT-7 |
| Control plane | INV-6, INV-31, REQ-54, REQ-55, LIV-13, LIV-14 | CT-10 |
