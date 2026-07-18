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
declares it (DC-1); otherwise they fail-safe in the DEF-10 taxonomy (DC-4). Integrity
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
| Equivocating (bad signature / wrong-range data) | integrity: discard, reroute, count (REQ-43); never delivered; all attempts equivocating ⇒ fail-safe WORKER-FAILURE (pages) |
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
| Down / connect refused | fail-safe UPSTREAM-FAILURE; archival traffic and readiness unaffected (FM-3) |
| Stalled read | fail-safe UPSTREAM-FAILURE within P-HOTBLOCKS-READ-TIMEOUT, recorded (ADR-010) |
| Upstream 429 / 503 / 529 | fail-safe `overloaded` envelope; public headers retained, `Retry-After` injected at P-RETRY-AFTER-MIN when absent (INV-26); upstream body never leaked (ADR-011) |
| Other erroring 5xx | fail-safe `upstream_unavailable` envelope; public headers retained, upstream body never leaked (ADR-011) |
| Erroring 4xx (unmatched) | fail-safe `malformed_request` envelope at 400; upstream body never leaked (DC-4) |
| Mid-proxy failure | truncate per INV-25 |
| Retention gap | fail-safe EMPTY (INV-27) |

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
