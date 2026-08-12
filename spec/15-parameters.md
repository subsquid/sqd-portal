# 15 — Parameter registry

**Mutable doc.** Every `P-*` symbol used anywhere in the suite has a row here; the
operator binds them through the configuration object (DEF-15).
"Observed" is the current default/behavior at version 0.12.1 (operator-overridable
unless marked *fixed*); "Target" is the ratified intent. ⚠ = proposed, awaiting
ratification via the linked ADR. Environmental rows describe the world the Portal
assumes, not knobs it owns.

## Serving limits

| Parameter | Role (where used) | Observed | Target |
|---|---|---|---|
| P-MAX-STREAMS | Global concurrent-stream cap; refusal above it is overload (REQ-20, REQ-27) | 1024 | 1024 |
| P-QUERY-SIZE-LIMIT | Max stream query body size (REQ-7) | 256 KiB | 256 KiB |
| P-QUERY-MAX-ITEMS | Max item selections per query (REQ-7) | 100 *(fixed)* | 100 |
| P-BUFFER-DEFAULT | Default per-stream read-ahead slots (REQ-8) | 10 | 10 |
| P-BUFFER-MAX | Operator cap on per-stream read-ahead (REQ-8, REQ-27) | 1000 | 1000 |
| P-STORED-RESULTS-PER-CHUNK | Buffered results per chunk slot (REQ-27) | 2 | 2 |
| P-MAX-CHUNKS-PER-STREAM | Operator cap on chunks per stream (REQ-8) | unlimited | unlimited |
| P-RESULT-MAX-SIZE | Max single worker-response size (REQ-27) | 250 MiB *(fixed)* | 250 MiB |
| P-BUFFERED-BYTES-BUDGET | ⚠ Global byte budget charged by every admitted stream buffer and wait (REQ-27, PF-1) | **none — GAP-17** | ⚠ Draft; ratify via OQ-9 |
| P-NO-DATA-DELAY | Server-side delay before an empty beyond-frontier response (REQ-5) | 5 s *(fixed)* | 5 s |
| P-RETRY-AFTER-MIN | Minimum retry hint on overload responses (REQ-20) | 1 s | 1 s |

## Upstream deadlines & refresh cadences

| Parameter | Role (where used) | Observed | Target |
|---|---|---|---|
| P-TRANSPORT-TIMEOUT | Worker query request deadline (REQ-41); binds first-byte wait — an aggregate per-attempt bound is intent, not implemented (GAP-26) | 60 s | 60 s |
| P-WORKER-CONNECT-TIMEOUT | Worker stream connect deadline — transport crate default, not operator-bound today (GAP-26) | 10 s | 10 s |
| P-HOTBLOCKS-CONNECT-TIMEOUT | Real-time source connect deadline (REQ-22) | 1 s | 1 s |
| P-HOTBLOCKS-READ-TIMEOUT | Real-time source per-read deadline; must stay < P-CLIENT-TIMEOUT (REQ-22, ADR-010). A replayed request spends it twice (ADR-015) | 20 s | 20 s |
| P-CLIENT-TIMEOUT | *Environmental:* the request deadline callers default to (ADR-010) | 30 s | ≥ 30 s assumed |
| P-ASSIGNMENT-REFRESH | Assignment poll interval (REQ-40, REQ-11) | 60 s | 60 s |
| P-ASSIGNMENT-SOURCE | Which published artifact routing reads: `legacy` or `portal`. Absolute — if the selected one isn't published, nothing is applied and the portal keeps serving what it has | `legacy` | `portal` |
| P-ASSIGNMENT-FETCH-TIMEOUT | Assignment fetch connect/read deadlines (REQ-22) | 5 s / 5 s | 5 s / 5 s |
| P-ASSIGNMENT-MAX-AGE | ⚠ Max tolerated assignment age before readiness degrades (REQ-23, ADR-013) | **unbounded — violated in intent** | ⚠ 15 min (draft; ratify via ADR-013) |
| P-DATASETS-REFRESH | Dataset catalog/metadata poll interval (REQ-12) | 600 s | 600 s |
| P-CHAIN-REFRESH | On-chain status poll interval (REQ-25) | 60 s | 60 s |

## Worker selection

| Parameter | Role (where used) | Observed | Target |
|---|---|---|---|
| P-MAX-QUERIES-PER-WORKER | Concurrent queries per worker (REQ-41) | 1 | 1 |
| P-WORKER-BACKOFF | Default backoff when a worker asks to slow down (REQ-41) | 1000 ms | 1000 ms |
| P-WORKER-ERROR-COOLDOWN | Avoidance window after a worker error (REQ-41, ADR-004) | 30 s | 30 s |
| P-WORKER-TIMEOUT-COOLDOWN | Avoidance window after a worker timeout (REQ-41, ADR-004) | 300 s | 300 s |
| P-RETRIES-DEFAULT | Default extra attempts per chunk (REQ-41) | 1 | 1 |
| P-TIMEOUT-QUANTILE | Quantile of recent durations that triggers a speculative retry | 0.5 | 0.5 |
| P-MAX-IDLE-TIME | Max wait on a fully backed-off worker set before refusing as overload | 1 s *(fixed)* | 1 s |

## Bandwidth regulation

| Parameter | Role (where used) | Observed | Target |
|---|---|---|---|
| P-CONGESTION-MIN-WINDOW | Concurrent-download window floor (REQ-42) | 10 | 10 |
| P-CONGESTION-MAX-WINDOW | Concurrent-download window ceiling (REQ-42) | 500 | 500 |
| P-CONGESTION-DECREASE | Multiplicative decrease factor on congestion (REQ-42) | 0.75 | 0.75 |
| P-CONGESTION-SHRINK-INTERVAL | Min interval between window shrinks (REQ-42) | 2000 ms | 2000 ms |
| P-CONGESTION-READ-TIMEOUT | Per-read stall deadline that signals congestion (REQ-42) | 1 s | 1 s |
| P-HEADROOM-THRESHOLD | Utilization above which new streams are refused (REQ-42, REQ-20) | 0.95 | 0.95 |
| P-PRIORITY-STRIDE | Stream-age weight in download-slot ordering (REQ-42) | 100 | 100 |

## Shutdown & readiness

| Parameter | Role (where used) | Observed | Target |
|---|---|---|---|
| P-PRE-DRAIN-GRACE | Serve-while-not-ready window after SIGTERM (REQ-24, ADR-005) | 25 s | 25 s |
| P-DRAIN-TIMEOUT | In-flight drain budget after intake stops (REQ-24, ADR-005) | 25 s | 25 s |
| P-KILL-GRACE | *Environmental:* the orchestrator's grace between SIGTERM and SIGKILL — Kubernetes `terminationGracePeriodSeconds` (REQ-24, LIV-11, ADR-005) | deployment-set, unverified; the Kubernetes default of 30 s is **below** the 50 s budget | ≥ P-PRE-DRAIN-GRACE + P-DRAIN-TIMEOUT + slack (≥ 60 s) |
| P-READY-CONNECTION-RATIO | Min fraction of known workers connected for readiness (REQ-23) | 3/4 *(fixed)* | 3/4 |
| P-STARTUP-BOUND | ⚠ Start → ready bound; artifact fetch and apply only, authorization configuration adding no term (LIV-5, S5) | unmeasured | ⚠ 10 min (proposed) |
| P-STALL-BUDGET | ⚠ Max zero-progress interval on a healthy stream; also the first-record bound (LIV-1, LIV-2, OB-2) | unmeasured | ⚠ 2 × P-TRANSPORT-TIMEOUT (proposed) |

## Accounting & reporting

| Parameter | Role (where used) | Observed | Target |
|---|---|---|---|
| P-LOGS-QUEUE | Usage-log queue bound; overflow drops (REQ-44) | 10000 *(fixed)* | 10000 |
| P-ERROR-SAMPLE-RATE | Error-report trace sampling (REQ-31) | 0.01 | 0.01 |
| P-HEARTBEAT-INTERVAL | Stream progress heartbeat cadence (OB-2, harness quiescence) | 5 s *(fixed)* | 5 s |
| P-MEMORY-BUDGET | ⚠ Per-replica memory budget REQ-27 must fit | 4–5 GB provisioned; **violated 2026-07-17 (OOM-kill restarts on 0.11.8)** | ⚠ ratify via OQ-4 and OQ-9 |
| P-ASSIGNMENT-SIZE | *Environmental:* mainnet assignment artifact size (REQ-27, GAP-3) | docs disagree: ~300 MB vs ~0.5 GB | resolve via OQ-4 |

## Access control

Bound only on an authorizing deployment (REQ-56); unused elsewhere. Every row is
operator-bindable in the `auth:` block and defaults to its observed value; the
targets stay proposals until OQ-15 ratifies them against a measured credential working
set, which is the only thing that can size them honestly.

| Parameter | Role (where used) | Observed | Target |
|---|---|---|---|
| P-GRANT-MAX-LIFETIME | Cap on the `expires_at` the Portal will honour, however long a one the control plane offers. The fleet's worst-case stale-authorization window, and the only lifetime term the Portal owns (REQ-54, DC-8, LIV-13) | 900 s | ⚠ 15 min (draft; ratify via OQ-15) |
| P-GRANT-EXCHANGE-TIMEOUT | Per-exchange deadline; must stay < P-CLIENT-TIMEOUT (DC-8, ADR-010, PF-7) | 2 s | ⚠ 2 s (draft; ratify via OQ-15) |
| P-GRANT-EXCHANGE-RATE | Token-bucket rate for exchanges (DC-8, LIV-14, HZ-10) | 20 /s | ⚠ one budget serves two opposed purposes — bounding attacker cost and admitting legitimate uncached keys (HZ-10); ratify via OQ-15 once CT-10 can measure the interference |
| P-GRANT-EXCHANGE-INFLIGHT | Cap on concurrent exchanges (DC-8, HZ-10) | 32 | ⚠ 32 (draft; ratify via OQ-15) |
| P-GRANT-CACHE-CAPACITY | Cap on cached grants; sized by the replica's credential working set, not by the key set (DEF-18, DC-8, HZ-13) | 65536 | ⚠ 65536 (draft; ratify via OQ-15) |
| P-GRANT-NEGATIVE-TTL | How long an authoritative denial suppresses repeat exchanges for the same fingerprint (DC-8) | 15 s | ⚠ 15 s (draft; ratify via OQ-15) |
| P-GRANT-NEGATIVE-CAPACITY | Cap on remembered denials; fingerprints are attacker-chosen, so the map is bounded rather than grown (DC-8, HZ-10) | 4096 | ⚠ 4096 (draft; ratify via OQ-15) |
| P-GRANT-REFRESH-JITTER | Spread applied to `refresh_after` so a cohort of grants issued together does not renew together (DC-8, HZ-12, LIV-13) | 10% | ⚠ 10% of the refresh interval (draft; ratify via OQ-15) |
| P-SIGNATURE-MAX-SKEW | *Environmental:* maximum absolute clock skew accepted on a signed request; a timestamp farther in the past or future fails every exchange at once (DC-8, 08 §0) | n/a — the control plane's bound, not the Portal's | control-plane-set; ⚠ 30 s assumed (ratify via OQ-15) |
| P-KEY-ID-MAX-LEN | Max accepted key-id length; mirrors what the control plane can mint (IB-9, REQ-52) | 64 *(fixed)* | 64 |
| P-KEY-SECRET-MAX-LEN | Max accepted secret length (IB-9, REQ-52) | 128 *(fixed)* | 128 |

## SLO targets

| Parameter | Role (where used) | Observed | Target |
|---|---|---|---|
| P-SLO-STREAM-TTFB-P99 | ⚠ Stream time-to-first-byte p99 (SLI-1, S1) | unmeasured | ⚠ 5 s (draft) |
| P-SLO-METADATA-TTFB-P99 | ⚠ Head/metadata time-to-first-byte p99 (SLI-1, S1) | unmeasured | ⚠ 1 s (draft) |
| P-SLO-REFUSAL-CORRECTNESS | Refusals with correct ADR-011 code and required hint (SLI-3) | ADR-011 integrated and CT-5-gated: every refusal carries a code, and OVERLOADED carries a hint at or above the floor on both emitters. Capacity exhaustion at every layer — cap refusal, worker backoff, and an exhausted run of worker capacity verdicts — answers 529 with a hint | 100% (INV-26) |
| P-SLO-AVAILABILITY | ⚠ Monthly readiness availability (SLI-4) | unmeasured | ⚠ 99.9% (draft) |
| P-SLO-MEMORY-HEADROOM | ⚠ Peak RSS / P-MEMORY-BUDGET cap (SLI-5) | unmeasured | ⚠ 0.8 (draft) |
| P-SLO-COMPLETION-INTEGRITY | ⚠ Complete stream fraction (SLI-6, S1) | unmeasured | ⚠ 0.99 (draft) |
