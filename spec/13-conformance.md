# 13 — Conformance & TDD plan

**Mutable doc.** Statuses as of **2026-07-17** (0.11.8,
`master@531e713f8ccb933ffc52fd50b3056db637e4f5fa` + working tree). Statuses: **C** covered · **P** partial · **U**
unchecked; *known-violated* / *known-suspect* where reality contradicts the property.
The **Phase-0 harness exists** (`harness/` crate: IB-7 stubs with ledgers — including
a real p2p worker stub on the pinned transport rev — toy world, reference model, the six
validators, client driver, quiescence-gated gauge audit; CT-1 smoke green — GAP-14
closed 2026-07-17). CT-1 exercises only success paths, so validators 1–5 run on every
response and the 6th (error-envelope) is defined but not yet exercised. Coverage beyond
the smoke is still inline unit tests; CT-2..CT-9 remain to be built per the build order.

## Harness architecture

```
                 ┌────────────── ledger (all stub data + calls = ground truth) ────────────┐
                 │                                                                          │
 client driver ──┤  HTTP per IB-2..IB-4               ┌──────────┐   p2p queries   worker   │
 (+ fuzzers,     ├────────────────────────────────────▶          │◀───────────────▶ stub ───┤
  disconnectors, │                                    │  PORTAL  │   artifact     publisher │
  swarm load)    │            responses               │ (black   │◀───────────────▶ stub ───┤
                 ◀────────────────────────────────────│   box)   │   catalog      registry  │
 validators      │                                    │          │◀───────────────▶ stub ───┤
 (structural,    │        /metrics /ready scrape      │          │   HTTP         real-time │
  oracle diff) ──┴────────────────────────────────────▶          │◀───────────────▶ stub ───┤
                                                      └──────────┘   contract     RPC stub ─┘
                                                                     reads
```

Stubs implement the input-side binding (IB-7) and are designed to double as **fault
injectors** (delays, stalls, error verdicts, corrupt artifacts, reorgs, kill-mid-body);
the Phase-0 stubs serve only the happy path — injector variants are built per each gap's
Next column (CT-2/CT-3). The scraper reads OP-8/OP-9 continuously; the comparator diffs responses against the
reference model. **Quiescence** := no in-flight requests ∧ stub queues empty ∧ one
P-HEARTBEAT-INTERVAL with no gauge movement. Gauge audits (INV-30, LIV-10) run only at
quiescence.

## Reference model (the oracle)

A pure function over (request, configuration, stub world). The model is *derived from*
the normative docs (02–09, 14): where model and docs disagree, the docs win and the
model is a bug to fix here. Pseudocode:

```
model(req, cfg, world) -> Response | ErrorClass:
  ds = resolve(req.alias, world.catalog)             # else NOT-FOUND
  if not wellformed(req, cfg): return BAD-REQUEST    # DEF-7; OP-1 order: tuning → parse → size → items → range
  if admission_closed(world.load): return OVERLOADED(hint >= P-RETRY-AFTER-MIN)  # INV-12
  frontier = frontier_of(ds, req.mode, world)        # DEF-5
  if req.mode == real_time and req.parentHash        # conflict precedes EMPTY   # INV-23
     and req.fromBlock - 1 <= frontier
     and conflicts(req.parentHash, world.canonical(ds)):
      return CONFLICT(prev_blocks ascending, ending at parent height, >= 1 entry)
  if req.fromBlock > frontier or retention_gap(ds, req.fromBlock, world):
      return EMPTY(delay >= P-NO-DATA-DELAY,
                   headers = heads(ds, world))       # INV-27; source only if routed
  src = network if req.fromBlock <= archival_head(ds, world) else real_time      # INV-13
  scanned = blocks(ds, req.fromBlock ..= min(req.toBlock, frontier))
  coverage = FV_coverage_extent(scanned)             # contiguous evaluated prefix; FV-4
  records = [ record(b, req.query, world.data[ds])   # provenance = stub ledger  # INV-22
              for b in coverage
              if matches(b, req.query) or req.includeAllBlocks
                 or b in (first(coverage), last(coverage)) ]  # response-level boundary; source may emit more per chunk — INV-29, FV-6
  return Response(records = records,                 # INV-20/21/25/29 checked by validators
                  coverage_cursor = ref(last(coverage)), # == ref(last record); delivered as that record, no dedicated field by design (DEF-8)
                  headers = heads(ds, world),        # within staleness bounds   # INV-24
                  source  = src)                     # exact match required
```

Chunk-failure sub-model: at most 1 + retries worker attempts per chunk (ledger-checked);
transient exhaustion before the first record ⇒ RETRIES-EXHAUSTED, integrity exhaustion
⇒ WORKER-FAILURE (DC-1), after the first record ⇒ truncation (FV-3).

**Free variables** — the only legitimate divergences from the model:

| FV | Freedom | Bound |
|---|---|---|
| FV-1 | which assigned worker serves an attempt | must hold the chunk; cooldowns respected while alternatives exist |
| FV-2 | speculative attempt count/timing | ≤ 1 + retries per chunk |
| FV-3 | truncation point | any record boundary after the first record |
| FV-4 | coverage extent | contiguous evaluated prefix from `fromBlock`; *matching* records may be empty, but the coverage boundary is always emitted (INV-29), so ≥1 record whenever ≥1 block is evaluated |
| FV-5 | compression choice/framing | must decode; gzip default, zstd when offered |
| FV-6 | boundary-record granularity | the source emits a header-only coverage boundary per *served chunk* (`Plan::execute` runs per chunk), not only at the response's global first/last; these interior header-only records are licensed. Conformance checks the last record (= coverage cursor, INV-29) and the matched-record set, not exact record-set equality |

Everything else — the content and order of the records that are present, error type/code,
hint presence, source marker when routing occurred, coverage cursor, and header honesty —
is deterministic against the model; record *presence* is exact up to the extra header-only
chunk-boundary records FV-6 licenses.

## Test-class taxonomy

| CT | Class | Primary properties |
|---|---|---|
| CT-1 | Response property tests: randomized queries/worlds vs oracle + validators | INV-10/11/20/21/22/27/28/29, LIV-1, LIV-4 |
| CT-2 | Dependency-fault matrix: every DC × every fault row of 09; incl. kill/restart | INV-1/2/23/25/31/37/40, LIV-2/5/6/7/11/12, FM tables, REQ-25/26 |
| CT-3 | Concurrency swarms: admit/finish/disconnect storms, artifact swaps mid-flight | INV-1/3/4/5/12/28/30/35, LIV-9/10 |
| CT-4 | Input-fault corpus: hostile headers/params/bodies, boundary values | INV-10/36, REQ-7/21 |
| CT-5 | Interface conformance: IB-2..IB-6 codes, headers, hints, schemas, marker | INV-13/24/26, REQ-20/32 |
| CT-6 | Performance benchmarks: scenarios S1–S6, SLO gates | SLI-1..6, PF-1..6, LIV-3/8/9 |
| CT-7 | Soak/endurance: S4 churn for hours; leak & cardinality audits | HZ-1/5/6, INV-30, SLI-5 |
| CT-8 | Isolation/noisy-neighbor: S6 | INV-35 |
| CT-9 | Fuzz, both surfaces: client inputs and stub responses (payloads, artifacts) | INV-36, FM-1, GAP-1 |

## Structural validators (kind-agnostic, applied to every response)

1. Body decodes under its declared encoding, line by line (INV-25).
2. Records parse; block numbers strictly ascending; no duplicates (INV-20).
3. Every record within [fromBlock, min(toBlock, frontier)] (INV-21).
4. Records belong to the requested dataset and match the field-selection shape.
5. Successful routed responses have coherent headers: finalized ≤ head; source marker
   ∈ {network, real_time}; the coverage cursor — the last delivered record — agrees with
   the ledger (INV-24, INV-13, DEF-8, INV-29). 204 EMPTY carries head markers, and a source marker iff a source was
   selected (retention-gap case). Pre-routing failures have no source marker.
6. Errors: type/code ∈ DEF-10; hint iff OVERLOADED — present on proxied overloads too,
   preserved or injected at the floor (ADR-014); no data alongside errors (INV-26).

## Traceability matrix — properties (2026-07-17)

| Property | CT | Status | Note |
|---|---|---|---|
| INV-1, INV-2 | CT-3/2 | U | artifact-variant selection unit-tested only |
| INV-3 | CT-3 | U | lease underflow guarded in debug builds only |
| INV-4 | CT-1/3 | P | window grow/shrink/priority unit tests |
| INV-5 | CT-3 | U | transient overshoot untested |
| INV-10 | CT-4 | U | — |
| INV-11 | CT-1 | U | clamping untested (GAP-8 adjacent) |
| INV-12 | CT-3/6 | P | mapping unit-tested; cap never driven (GAP-4) |
| INV-13 | CT-5 | P | resolver units; source marker asserted on both sources by the CT-1 smoke |
| INV-20 | CT-1 | P | exactly-once regression + ordering units; CT-1 smoke oracle-diffs toy-world streams |
| INV-21 | CT-1 | P | bounds validator green on smoke responses; no randomized worlds yet |
| INV-22 | CT-1 | P | smoke diffs delivered records against the stub ledger (signed responses); rejection path untested |
| INV-23 | CT-2 | P | verdict parsing tested; flow untested; minimum 409 payload meets the invariant; richer ancestors remain a REQ-3 SHOULD shortfall (GAP-7); EMPTY-precedence at the head unverified (GAP-19) |
| INV-24 | CT-5 | P | smoke asserts head markers against stub/artifact heads on success paths |
| INV-25 | CT-2 | U | truncation never exercised |
| INV-26 | CT-5 | P | **Known-violated** — current master still emits legacy/mixed and proxied error bodies (GAP-16); ADR-011 target is not yet integrated |
| INV-27 | CT-1 | P | gap detection tested; proxied 204 smoke-tested; delay untested |
| INV-28 | CT-3 | U | — |
| INV-30 | CT-3/7 | U | gauge accounting was a past defect class |
| INV-31 | CT-2 | P | shutdown flip e2e-tested; other conjuncts not; *known-violated* on staleness intent (GAP-2) |
| INV-35 | CT-8 | U | — |
| INV-36 | CT-4/9 | P | two crash regressions covered; latent panic (GAP-5) |
| INV-37 | CT-2 | U | — |
| INV-40 | CT-2 | U | — |
| LIV-1..LIV-4 | CT-1/2/6 | U | stall budget unmeasured |
| LIV-5, LIV-6 | CT-2 | U | startup bound unmeasured (S5) |
| LIV-7 | CT-2 | U | — |
| LIV-8 | CT-6 | P | regrowth unit-tested at scheduler level |
| LIV-9 | CT-6 | U | — |
| LIV-10 | CT-3 | U | cancellation-on-drop unit-approximated only |
| LIV-11 | CT-2 | P | drain race + signal sequencing tested |
| LIV-12 | CT-2 | U | *known-violated* for the artifact loop (log-only — GAP-2) |
| FM-1 | CT-9 | P | see INV-36 |
| SLI-1..SLI-6 | CT-6 | U | no benchmarks; baselines from incidents only |

## Acceptance matrix — requirements (2026-07-17)

| REQ | Status | Note |
|---|---|---|
| REQ-1 | P | Exactly-once regression + slot-ordering units; smoke oracle diff over both sources (INV-20) |
| REQ-2 | P | Coverage cursor delivered as the last record (INV-29) → selective resume holds; no dedicated cursor field, by design (DEF-8). Resume-across-requests still untested; no CT-1 selective-tail case yet |
| REQ-3 | P | Mismatch parsing tested; flow untested; richer-ancestor SHOULD shortfall GAP-7 (INV-23 minimum holds); head-precedence GAP-19 |
| REQ-4 | P | Routing + source marker asserted by the CT-1 smoke (INV-13) |
| REQ-5 | P | Gap detection tested; delay/204 untested (INV-27) |
| REQ-6 | U | Truncation never exercised (INV-25) |
| REQ-7 | U | No tests over the rejection table (INV-10) |
| REQ-8 | U | Clamping untested; params ignored (GAP-8, INV-11) |
| REQ-9 | P | Non-ASCII id regression; response echo e2e-tested by the smoke — which found the propagate layer attached to the empty router (dead); fixed 2026-07-17. Upstream propagation untested |
| REQ-10, REQ-11 | P | Catalog listing + archival head (number and hash) asserted by the smoke |
| REQ-12 | U | — |
| REQ-13 | P | Direction/fallback units exist, but timestamp overload mapping and the ADR-011 envelope are not integrated (GAP-4, GAP-16) |
| REQ-14 | P | Internal-endpoint hiding tested |
| REQ-15 | P | Plan extraction/rewrite units; no e2e |
| REQ-20 | P | **Known-violated** — cap exhaustion is not exercised and current master misclassifies it (GAP-4); taxonomy target is GAP-16 |
| REQ-21 | P | Two crash regressions; latent panic (GAP-5, INV-36) |
| REQ-22 | P | Real-time deadline units exist; **known-violated** because chain-RPC calls lack explicit deadlines (GAP-18) |
| REQ-23 | P | Shutdown flip tested; other conjuncts not; staleness unimplemented (GAP-2, INV-31) |
| REQ-24 | P | Drain race tested; in-flight behavior during drain untested (LIV-11) |
| REQ-25 | U | No outage tests |
| REQ-26 | U | **Known-violated** (GAP-1, ADR-002) |
| REQ-27 | U | **Known-violated** — OOM incident plus no global byte budget (GAP-3, GAP-17, PF-1) |
| REQ-30 | P | Label mapping tested; gauge accounting untested (INV-30) |
| REQ-31 | P | Middleware units only |
| REQ-32 | P | Internal hiding tested; drift (GAP-11) |
| REQ-33 | C | Config warn/reject/defaults tested |
| REQ-40 | P | Variant selection tested; effective-time & outage untested (INV-2, LIV-6); regression guard unimplemented (GAP-20) |
| REQ-41 | U | Selection/penalties untested (LIV-7); FV-2 attempt bound ledger-checked by the smoke |
| REQ-42 | P | Scheduler units; headroom refusal untested (INV-4, LIV-8) |
| REQ-43 | P | Positive path exercised by the smoke (signed stub responses verified and delivered); rejection path untested |
| REQ-44 | U | — |

## Gap register — 2026-07-17

Priorities: P0 blocks the program · P1 active production risk · P2 correctness hole
with plausible trigger · P3 polish. "Next" = cheapest failing-test-first entry.

| GAP | Statement | Violates | Priority | Next |
|---|---|---|---|---|
| GAP-1 | Assignment artifact adopted with no structural validation; a corrupt blob can panic the refresh path or leave the Portal ready on garbage routing | REQ-26, INV-36, FM-2 (ADR-002) | P1 | CT-2: truncated-artifact stub → assert reject + alarm + prior artifact kept |
| GAP-2 | Artifact staleness unbounded and invisible: fetch failures log-only; readiness ignores age | REQ-23/40, INV-31 ⚠, LIV-12, OB-6/9 (ADR-013, OQ-3) | P1 | age gauge; readiness-degradation test past P-ASSIGNMENT-MAX-AGE |
| GAP-3 | Refresh holds old + new artifacts resident and first-byte waits are unmetered. Baseline: 2026-07-17 OOM-kill restart storm on 0.11.8 | REQ-27, PF-1, SLI-5 (OQ-4) | P1 | RSS-during-refresh probe under S4; heap profile to pin the dominant term |
| GAP-4 | Current master does not implement the stream-cap refusal contract: cap exhaustion yields a 503/no mandatory hint, timestamp handling does not preserve the overload outcome, and a beyond-frontier timestamp still returns 404 where ADR-014 fixes it as the 204 EMPTY outcome | REQ-20, REQ-13, INV-12, PF-6 | P1 | CT-3: occupy P-MAX-STREAMS; assert 529 + hint and admitted-stream integrity |
| GAP-5 | Latent panic on an empty stream ("first chunk missing") — known trigger fenced only | REQ-21, INV-36 | P2 | replace panic with error; empty-yield test |
| GAP-6 | Worker-labeled metric cardinality and name interning grow without eviction | REQ-30, OB cardinality rule, HZ-6 | P2 | CT-7 series-count audit across churn |
| GAP-7 | Archival-path CONFLICT payload has only one entry. It meets the REQ-3/INV-23 MUST minimum but not REQ-3's richer-ancestor SHOULD | REQ-3 SHOULD | P2 | CT-5 contract test on the 409 body |
| GAP-8 | Advertised tuning params accepted then silently ignored on public routes | REQ-8, IB-3 (OQ-1) | P2 | decide; then honor-and-clamp or 400 test |
| GAP-10 | Container healthcheck probes a nonexistent route | REQ-23 operability | P2 | point at /ready; compose test |
| GAP-11 | Served API description drift: undocumented route/header, stale examples, size-doc conflict | REQ-32, IB-2/4 | P3 | CT-5 description-vs-router sweep |
| GAP-12 | Download-priority key wraps at ~43 M streams (HZ-4) | REQ-42 fairness | P3 | widen key; wrap-boundary unit test |
| GAP-13 | ADR-009 accepted but portal-side injection unimplemented — decision drift | OQ-5 | P3 | schedule or supersede |
| GAP-16 | Current master exposes legacy/mixed error bodies and passes real-time error bodies through. ADR-011's closed type/code envelope is not integrated; its 409 and readiness exceptions also need CT-5 proof against IB-5 when the taxonomy change lands, together with ADR-014's amendments (proxied-hint injection, unmatched-4xx normalization, EMPTY head metadata, integrity-exhaustion WORKER-FAILURE) | DEF-10, INV-26, IB-5, REQ-7/13/20 | P1 | CT-5 table-driven local + proxied error-shape/status tests |
| GAP-17 | Count caps imply a multi-terabyte theoretical buffer ceiling and no global byte budget or accounting exists | REQ-27, PF-1, OQ-9 | P1 | add byte meter/admission test; set P-BUFFERED-BYTES-BUDGET |
| GAP-18 | Chain-RPC calls have no explicit deadline despite accepted ADR-010 | REQ-22, DC-5, HZ-8 | P2 | stalled-RPC stub → assert bounded call and loop recovery |
| GAP-19 | Conflict detection is not known to precede the beyond-frontier EMPTY: a real-time continuation at the head with a stale parent may poll empty instead of getting 409 (the pre-ADR-014 oracle ordered EMPTY first; master unverified) | REQ-3, INV-23, INV-27 (ADR-014) | P2 | CT-2: reorg-at-head stub world → assert 409 precedence |
| GAP-20 | No regression guard on artifact application: a republished older artifact (different identifier) would be re-applied | REQ-40, INV-2, DEF-4 (ADR-014) | P2 | CT-2: regressive publisher stub → assert not applied |
| GAP-21 | Debug stream variant bypasses clamps without an operator gate | INV-11, REQ-8 (ADR-014; closed OQ-6) | P2 | config test: route absent unless the operator flag enables it |

### Closed findings

- **GAP-9** (closed 2026-07-17): EMPTY delay occurs after the stream census permit is
  released. It can occupy an HTTP connection/task (HZ-3), but does not consume the
  stream cap.
- **GAP-14** (closed 2026-07-17): the Phase-0 harness skeleton landed as the
  `harness/` crate — stubs per IB-7 with ledgers, toy world, oracle, validators,
  driver, gauge audit; CT-1 smoke green. Its first run found a real REQ-9 defect:
  the response `x-request-id` echo layer was attached to the empty router (wrapping
  zero routes) and never ran; fixed the same day.

## Build order

- **Phase 1 — P1 gaps, failing tests first:** GAP-1/2/4/16/17 tests red → fixes;
  GAP-3 probe + heap profile → refresh-copy fix.
- **Phase 2 — correctness core:** full CT-1 oracle diffing; CT-2 fault matrix; CT-4
  corpus; burn down P2 gaps.
- **Phase 3 — robustness:** CT-3 swarms; CT-8 isolation; CT-9 fuzz; INV-30 audits.
- **Phase 4 — performance regime:** CT-6 benchmarks S1–S6; ratify ⚠ SLO targets
  (OQ-3/OQ-4/OQ-9/OQ-10); commit baselines; CT-7 soak on a CI cadence.

Each phase ends by updating this file's matrices and register in the same change.
