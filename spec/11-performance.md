# 11 — Performance & resource regime

## Workload model

Parameters (symbolic; scenarios bind them):

| Param | Meaning |
|---|---|
| W-STREAMS | concurrent streams |
| W-ARRIVAL | new-stream arrival rate |
| W-RANGE | requested range size (chunks per stream) |
| W-SELECTIVITY | query selectivity (bytes per chunk delivered) |
| W-DATASETS | distinct datasets in play |
| W-CHURN | worker-set / artifact change rate |
| W-POLL | share of beyond-frontier (head-polling) requests |

Reference scenarios:

| Scenario | Shape |
|---|---|
| S1 steady | moderate W-STREAMS, mixed ranges, healthy workers |
| S2 head-poll storm | W-POLL → 1: thousands of pollers cycling EMPTY responses |
| S3 saturation | W-ARRIVAL past capacity; refusal path dominant — the arrival load that drove the 2026-07 OOM-kill restart storm (root cause: GAP-3; coupling: PF-3) |
| S4 churn-soak | S1 plus continuous artifact churn and worker fail/recover cycles, hours long |
| S5 cold start | process start on mainnet-sized artifact through readiness to first streams |
| S6 noisy neighbor | one heavy scan stream + many light streams on other datasets |

## SLI definitions (black-box)

| SLI | Definition |
|---|---|
| SLI-1 | Time-to-first-byte: request sent → first response byte, per operation class |
| SLI-2 | Stream throughput: delivered bytes/s per stream while the client drains |
| SLI-3 | Refusal correctness: fraction of refusals carrying the correct DEF-10 class and (for OVERLOADED) a retry hint |
| SLI-4 | Availability: fraction of time ready (OP-8) excluding deploys |
| SLI-5 | Memory headroom: peak RSS / P-MEMORY-BUDGET |
| SLI-6 | Completion integrity: fraction of streams ending for a contract reason — requested bound reached, frontier reached, or a client-requested chunk/coverage limit — vs truncated by failure |

## SLO table

Targets ⚠ are provisional until ratified (OQ-4/OQ-9 for memory; OQ-10 for SLOs).
Observed values and incident baselines are status, not contract: they live in the
parameter registry ([15-parameters.md](15-parameters.md), Observed column) and the gap
register ([13-conformance.md](13-conformance.md)), and seed the regression gates there
— this doc stays immutable (README §conventions).

| SLI | Scenario | Target ⚠ |
|---|---|---|
| SLI-1 (stream) | S1 | p99 ≤ P-SLO-STREAM-TTFB-P99 ⚠ |
| SLI-1 (heads/metadata) | S1 | p99 ≤ P-SLO-METADATA-TTFB-P99 ⚠ |
| SLI-2 | S1 | ≥ SDK consumption rate (client-bound, not portal-bound) ⚠ |
| SLI-3 | S3 | ≥ P-SLO-REFUSAL-CORRECTNESS (hard invariant INV-26) |
| SLI-4 | any | ≥ P-SLO-AVAILABILITY monthly ⚠ |
| SLI-5 | S1/S4 | ≤ P-SLO-MEMORY-HEADROOM ⚠ |
| SLI-6 | S1 | ≥ P-SLO-COMPLETION-INTEGRITY ⚠ |
| readiness time | S5 | ≤ P-STARTUP-BOUND ⚠ (dominated by the artifact download, LIV-5) |
| shutdown time | any | ≤ P-PRE-DRAIN-GRACE + P-DRAIN-TIMEOUT + slack (hard, LIV-11) |

## Resource-bound requirements

**PF-1 — Byte-accounted memory ceiling.** The count-derived theoretical stream ceiling
is P-MAX-STREAMS × P-BUFFER-MAX × P-STORED-RESULTS-PER-CHUNK ×
P-RESULT-MAX-SIZE. It is an upper bound on configuration, not a safe resident-memory
budget: each admitted buffer and wait must instead be charged to the global
P-BUFFERED-BYTES-BUDGET, which together with artifact and runtime overhead fits
P-MEMORY-BUDGET. Any uncharged resident term is a spec bug. Today neither the global
byte budget nor the refresh/first-byte accounting exists (GAP-3/GAP-17).

**PF-2 — End-to-end backpressure.** A slow client slows its own stream's upstream
consumption within its buffer bound; it never grows unbounded queues anywhere
(DEF-11 bounds per stream, DEF-13 globally).

**PF-3 — Admission control as the pressure valve.** Overload is absorbed by refusing
new work (INV-12), never by degrading admitted work past its liveness bounds or by
process death (FM: memory row).

**PF-4 — Refresh budget, two-sided.** Background refreshes (artifact, catalog, chain)
must complete within their intervals (keep-up) **and** must not consume more than a
bounded share of serving resources while doing so — an artifact apply must not stall
request routing beyond a scheduling pause (HZ-5).

**PF-5 — Startup work scheduling.** Startup-critical work (artifact fetch) is
prioritized; nothing else on the startup path may push readiness past
P-STARTUP-BOUND (LIV-5).

**PF-6 — Refusal cheapness.** The refusal path allocates O(1) work and memory per
refused request — surviving S3 requires refusals to be at least an order of magnitude
cheaper than admissions.

## Hazard register

Mechanism → threatened property → probe. Hazards are timeless risk pointers; dated
defects live in the gap register (13).

| HZ | Mechanism | Threatens | Probe |
|---|---|---|---|
| HZ-1 | Artifact refresh holds old + new copies resident (~2× artifact) | PF-1, SLI-5 | RSS during refresh under S4 |
| HZ-2 | First-byte waits unmetered; per-worker transport streams uncapped | PF-1, INV-4's intent | in-flight census under stalled-worker stub |
| HZ-3 | EMPTY throttle holds an HTTP connection/task for P-NO-DATA-DELAY after releasing its census slot | connection/task capacity under S2 | connection and task occupancy at W-POLL → 1 |
| HZ-4 | Download-priority key wraps (32-bit stream × stride) | fairness (ADR-006 ordering) | unit probe at wrap boundary (GAP-12) |
| HZ-5 | Artifact swap and routing reads share one synchronous lock; per-chunk parsing under it | LIV-2, PF-4 | routing-latency percentile during forced refresh |
| HZ-6 | Per-worker metric labels never evicted; name interning grows | PF-1 (slow leak), scrape cost | series count across S4 (GAP-6) |
| HZ-7 | Usage-log queue drops silently at P-LOGS-QUEUE | accounting fidelity (REQ-44) | drop counter vs stub-sink ledger |
| HZ-8 | Chain RPC calls carry no explicit deadline | LIV-3 for the status loop | stalled-RPC stub; loop aliveness (GAP-18) |
| HZ-9 | Congestion waiter queue is unbounded | PF-1 under S3 | waiter census at saturation |

## Benchmarking requirements

Every scenario S1–S6 has a repeatable benchmark against the harness stubs. Each
benchmark records the SLO-table SLIs plus process RSS/CPU; S3 must include the
*recovery* phase (LIV-9), S5 the accept-vs-ready split (LIV-5). Baselines are
committed alongside results; regressions against a baseline fail CI like a test.
Saturation-knee characterization (max sustainable W-STREAMS / W-ARRIVAL at target
SLI-1) is measured per release and recorded in the parameter registry's Observed
column ([15-parameters.md](15-parameters.md)).
