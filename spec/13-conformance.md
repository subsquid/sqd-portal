# 13 — Conformance & TDD plan

**Mutable doc.** Statuses as of **2026-08-07** (0.12.1,
`master@e9a1878f863c4d87615bcf21ba1ef79fdf649385`). Statuses: **C** covered · **P** partial ·
**U** unchecked; *known-violated* / *known-suspect* where reality contradicts the property.
**The commercial band (REQ-50..56, DC-8, OP-11 and the invariants scoped to them) has
landed and is CT-10-covered on its request path.** A deployment with no `commercial:` block
is still unaffected by any of it, and CT-10 pins that too. What is *not* covered is every
row whose claim is about time — grant renewal, hard expiry, denial TTL, renewal spread —
which needs a clock the harness does not yet have (GAP-33).
The **Phase-0 harness exists** (`harness/` crate: IB-7 stubs with ledgers — including
a real p2p worker stub on the pinned transport rev — toy world, reference model, the six
validators, client driver, quiescence-gated gauge audit; CT-1 smoke green — GAP-14
closed 2026-07-17). CT-1 exercises only success paths, so validators 1–5 run on every
response there; the 6th (error-envelope) is exercised by CT-2, the sole path by which it
reads an error code.
**CT-2 has started**: the worker stub is now a DC-1 fault injector (wrong-range both
directions, bad signature, server-error and not-found verdicts) shared across workers so
a fault lands wherever the portal routes, and `Fixture` boots the whole stub world for
any class that needs it. Caveat: production workers pin a newer transport rev whose
server was rewritten (stream-based accept with silent drop at buffer capacity); the stub
speaks the portal's older pinned rev, so the production server's drop paths are not
exercised — re-verify on the next dependency bump. `ct2_worker_faults` covers the worker-fault reroute rows and the
exhaustion split; the rest of CT-2 and CT-3..CT-9 remain to be built per the build order.
**CT-10 exists**: a DC-8 control-plane stub that verifies the exchange signature for real
rather than trusting the headers, with a ledger of every credential it was asked about and
the fault rows of DC-8's error table as injectors. `ct10_authorization` drives four
portals — enforcing, shadow, no commercial block, and one whose exchange budget is small
enough to saturate — because the properties differ by configuration rather than by request.
Coverage outside those three classes is still inline unit tests.
All three suites run on every pull request: the harness is a separate crate, so it needs a
build of the portal and a job of its own — a status this document cites has to be one
something re-checks.

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
injectors** (delays, stalls, error verdicts, corrupt artifacts, reorgs, kill-mid-body).
The worker stub is the first one built out: faults are queued on a handle shared by every
stub worker, so one queued fault lands on whichever worker the portal routes to and a
test never depends on FV-1's choice. The remaining injectors are built per each gap's
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
  ds = resolve(req.alias, world.catalog)             # DEF-1 alias equality; else NOT-FOUND
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
before the first record, exhaustion is RETRIES-EXHAUSTED unless *every* attempt was an
integrity failure, which is WORKER-FAILURE (DC-1); after the first record ⇒ truncation
(FV-3).

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
| CT-10 | Authorization: credential corpus × enforcement mode against a control-plane stub; exchange-fault, lifetime and convergence cases (denial mid-grant, a retired timed-out generation completing after its successor, over-cap lifetime, unreadable claims version, outage across `refresh_after` and `expires_at`); bracketed metrics scrapes proving no key-id side channel, including neutral shadow-mode projection | INV-6/10/14/15/38/39, INV-31, LIV-13/14, REQ-50..REQ-56, DC-8, IB-9, HZ-10/12/13 |

## Structural validators (kind-agnostic, applied to every response)

1. Body decodes under its declared encoding, line by line (INV-25); the encoding and
   framing themselves are free (FV-5).
2. Records parse; block numbers strictly ascending; no duplicates (INV-20).
3. Every record within [fromBlock, min(toBlock, frontier)] (INV-21).
4. Records belong to the requested dataset and match the field-selection shape.
5. Successful routed responses have coherent headers: finalized ≤ head; source marker
   ∈ {network, real_time}; the coverage cursor — the last delivered record — agrees with
   the ledger (INV-24, INV-13, DEF-8, INV-29). 204 EMPTY carries head markers, and a source marker iff a source was
   selected (retention-gap case). Pre-routing failures have no source marker.
6. Errors: type/code ∈ DEF-10; a hint on every OVERLOADED — on proxied ones too,
   preserved or injected at the floor — never on DATA-UNAVAILABLE, and on no other class
   unless the upstream sent one (ADR-014); one status across every credential refusal; no
   data alongside errors (INV-26, IB-5).

## Traceability matrix — properties (2026-08-07)

| Property | CT | Status | Note |
|---|---|---|---|
| INV-1, INV-2 | CT-3/2 | U | artifact-variant selection unit-tested only |
| INV-3 | CT-3 | P | quiescent lease census asserted zero after every randomized scheduling case and after client disconnect (controller-level, mock network); CT-3 swarm still absent |
| INV-4 | CT-1/3 | P | window grow/shrink/priority unit tests |
| INV-5 | CT-3 | U | transient overshoot untested |
| INV-10 | CT-4/10 | U | invalid-request corpus has no dependency-ledger assertion, including the bounded authorization exception |
| INV-11 | CT-1 | U | clamping untested (GAP-8 adjacent) |
| INV-12 | CT-3/6 | P | mapping unit-tested; cap never driven (GAP-4) |
| INV-13 | CT-5 | P | resolver units; source marker asserted on both sources by the CT-1 smoke |
| INV-20 | CT-1 | P | exactly-once regression + ordering units; CT-1 smoke oracle-diffs toy-world streams; controller property test asserts gapless/monotonic/no-duplicate emission under randomized scheduling adversity |
| INV-21 | CT-1/2 | P | bounds validator green on smoke responses; wrong-range worker responses are now rejected at the source seam and CT-2 proves they are never delivered; randomized worlds are controller-level only |
| INV-22 | CT-1/2 | P | smoke diffs delivered records against the stub ledger (signed responses); CT-2 now drives the rejection path — wrong-range (both directions) and bad-signature responses are discarded, rerouted, and byte-identical output is delivered from another worker |
| INV-23 | CT-2 | P | verdict parsing tested; flow untested; minimum 409 payload meets the invariant; richer ancestors remain a REQ-3 SHOULD shortfall (GAP-7); EMPTY-precedence at the head unverified (GAP-19); verdict detection is exact-string parsing of worker messages (GAP-25) |
| INV-24 | CT-5 | P | smoke asserts head markers against stub/artifact heads on success paths |
| INV-25 | CT-2 | U | truncation never exercised |
| INV-26 | CT-5 | C | CT-5 asserts the envelope, status, type/code and hint presence across the local and proxied emitters, including the 409 sibling, the OVERLOADED hint floor, replacement of an unusable upstream hint (0, non-numeric, HTTP-date), the classes that get no invented hint (upstream 503/500), a wrong verb keeping 405 with its `Allow`, and normalization of the router's other rejections |
| INV-27 | CT-1 | P | gap detection tested; proxied 204 smoke-tested; delay untested |
| INV-28 | CT-3 | U | — |
| INV-29 | CT-1 | P | boundary emission asserted by the CT-1 selective-tail resume on both sources; the network multi-chunk case exercises the per-chunk granularity FV-6 licenses. Interior boundary records are not audited, and the EMPTY case (no block evaluated) is untested. Boundary pinning is a worker-engine behavior — re-prove before adopting new engine/format fields on a dependency bump |
| INV-30 | CT-3/7 | U | gauge accounting was a past defect class |
| INV-31 | CT-2 | P | shutdown flip e2e-tested; other conjuncts not; *known-violated* on staleness intent (GAP-2). The spec states that commercial configuration adds no conjunct, which at this baseline is vacuously true |
| INV-35 | CT-8 | U | — |
| INV-36 | CT-4/9 | P | three crash regressions covered — the third, a worker reporting a last block past the queried range, panicked the stream task via an inverted continuation range; latent panic (GAP-5) |
| INV-37 | CT-2 | U | — |
| INV-40 | CT-2 | U | — |
| LIV-1..LIV-4 | CT-1/2/6 | U | stall budget unmeasured |
| LIV-5, LIV-6 | CT-2 | U | startup bound unmeasured (S5); no commercial term to measure at this baseline |
| LIV-7 | CT-2 | P | CT-2 waits for the pool to serve again between fault cases and after exhaustion; a latched penalty fails the run. Cooldown *durations* are unmeasured |
| LIV-8 | CT-6 | P | regrowth unit-tested at scheduler level |
| LIV-9 | CT-6 | U | — |
| LIV-10 | CT-3 | P | dropping a stream asserted to abort in-flight worker queries and return their leases; census slot and congestion permits are outside the controller and still unchecked |
| LIV-11 | CT-2 | P | drain race + signal sequencing tested |
| LIV-12 | CT-2 | P | worker attempts bounded and exhaustion surfaced: CT-2 asserts integrity and transient exhaustion are distinguishable outcomes, not retry loops; *known-violated* for the artifact loop (log-only — GAP-2) |
| FM-1 | CT-9 | P | see INV-36 |
| FM-2 | CT-2 | P | the DC-4 replay classifier is unit-tested on both sides of the transient/integrity line (clean close, reset, the two non-replay exclusions — ADR-015). Worker-side classification now exists and is CT-2-tested: wrong-range and bad-signature responses are integrity — discarded, rerouted, never delivered — and all-integrity exhaustion is a distinguishable outcome from transient exhaustion. Corrupt artifact (GAP-1) is still untested; the *taxonomy* of both exhaustion outcomes is inside GAP-16 |
| FM-3 | CT-2/3 | U | per-dependency confinement never exercised: no outage tests (REQ-25), no isolation swarm (INV-35) |
| SLI-1..SLI-6 | CT-6 | U | no benchmarks; baselines from incidents only |
| INV-6 | CT-10 | P | fingerprint-only reach is CT-10-covered: a burst on one credential costs one exchange and a second request on a cached grant costs none. The hard-expiry stop, no stale answer overwriting a fresh one, and a denial surviving an in-flight exchange all turn on time and remain unwritten (GAP-33) |
| INV-14 | CT-10 | P | CT-10 asserts all four call-ledger claims against the stub ledgers: a refused request reaches no worker and no real-time source, a burst of eight on one fingerprint costs one exchange, an ungrammatical token and an absent one cost none, and a cached grant costs none. Canonicalization confined to the dataset rung is not separately observable black-box |
| INV-15 | CT-10 | P | ladder precedence is CT-10-covered end to end — absent before malformed, malformed before exchange, denial before scope — and each denial reason reaches its own wire code. Determinism under saturation and the per-replica divergence bound are untested |
| INV-38 | CT-10 | P | CT-10 greps the whole portal log and the whole scrape for every secret the stub recorded receiving, after driving admissions, denials and failed exchanges — the egress is the exchange and nothing else. The audit guards itself: it fails if no credential travelled, or if the log and scrape it reads carry no authorization activity. Constant-time fingerprint comparison is not observable black-box |
| INV-39 | CT-10 | P | CT-10 asserts an unknown key and a wrong secret return byte-identical bodies at one status, that no internal rung name reaches the scrape, that no key id does, and that shadow mode publishes only `shadow_evaluated` with no code and no exchange counters. A strict bracketed-scrape delta between the two credential cases is not yet driven |
| LIV-13 | CT-10 | U | needs convergence at `refresh_after` + one exchange with the control plane healthy, and at `expires_at` with it stopped — the second is the one carrying the security claim. Both need a clock the harness does not have (GAP-33) |
| LIV-14 | CT-10 | C | CT-10 serves a freshly minted key on the request that presents it, and drives a burst of distinct credentials against a bounded budget: the refusals are `overloaded` with a usable hint, never a verdict about a key |
| DC-8 | CT-10 | P | the stub verifies the signing contract for real — one of each header, an attributable portal, bounded skew, Ed25519 over the canonical binding — and CT-10 asserts the portal never trips it, then that an unattributable portal fails every exchange as `upstream_unavailable`. Deadline, single-flight and the lifetime cap are covered, as is every unusable answer failing rather than denying: 500, 503, 404, a truncated grant, an unknown claims version and an answer about another key all reach the client as retryable. Denial-evicts and outage grace need a clock (GAP-33) |

## Acceptance matrix — requirements (2026-08-07)

| REQ | Status | Note |
|---|---|---|
| REQ-1 | P | Exactly-once regression + slot-ordering units; smoke oracle diff over both sources (INV-20) |
| REQ-2 | P | Coverage cursor delivered as the last record (INV-29) → selective resume holds; no dedicated cursor field, by design (DEF-8). CT-1 covers selective-tail resume across requests on both sources; the network multi-chunk case exercises FV-6 |
| REQ-3 | P | Mismatch parsing tested; flow untested; richer-ancestor SHOULD shortfall GAP-7 (INV-23 minimum holds); head-precedence GAP-19 |
| REQ-4 | P | Routing + source marker asserted by the CT-1 smoke (INV-13) |
| REQ-5 | P | Gap detection tested; delay/204 untested (INV-27) |
| REQ-6 | U | Truncation never exercised (INV-25) |
| REQ-7 | U | No tests over the rejection table (INV-10) |
| REQ-8 | U | Clamping untested; params ignored (GAP-8, INV-11) |
| REQ-9 | P | Non-ASCII id rejection regression; response echo e2e-tested by the smoke — which found the propagate layer attached to the empty router (dead); fixed 2026-07-17. Upstream propagation untested |
| REQ-10, REQ-11 | P | Catalog listing + archival head (number and hash) asserted by the smoke |
| REQ-12 | U | — |
| REQ-13 | P | The ADR-011 envelope is integrated on the timestamp surface; a classified refusal reaches the client whole (529 with its hint, or 503 `no_workers`) rather than flattened, and a real-time refusal runs through the same upstream classifier the stream proxy uses, so a 429 stays an overload with its hint instead of surfacing as a 500. A local upstream failure answers 502 per IB-5; the beyond-frontier 404 remains (GAP-4) |
| REQ-14 | P | Internal-endpoint hiding tested |
| REQ-15 | P | Plan extraction/rewrite units; no e2e |
| REQ-20 | P | **Known-violated** — cap exhaustion is not exercised and current master misclassifies it (GAP-4); taxonomy target is GAP-16 |
| REQ-21 | P | Three crash regressions, the newest an inverted continuation range from an out-of-range worker response; latent panic (GAP-5, INV-36) |
| REQ-22 | P | Real-time deadline units exist, including ADR-015's exclusion of read stalls from replay; **known-violated** because chain-RPC calls lack explicit deadlines (GAP-18), and because a replayed request can spend the read budget twice (ADR-015). The per-call bound is tested; a client request's total upstream spend is neither bounded nor tested |
| REQ-23 | P | Shutdown flip tested; other conjuncts not; staleness unimplemented (GAP-2, INV-31); the container healthcheck probes a route that does not exist (GAP-10) |
| REQ-24 | P | Drain race tested; in-flight behavior during drain untested (LIV-11); the P-KILL-GRACE conjunct is environmental — no in-process test can assert it |
| REQ-25 | U | No outage tests |
| REQ-26 | U | **Known-violated** (GAP-1, ADR-002) |
| REQ-27 | U | **Known-violated** — OOM incident plus no global byte budget (GAP-3, GAP-17, PF-1) |
| REQ-30 | P | Label mapping tested; gauge accounting untested (INV-30) |
| REQ-31 | P | Middleware units only |
| REQ-32 | P | Internal hiding tested; drift (GAP-11) |
| REQ-33 | C | Config warn/reject/defaults tested |
| REQ-40 | P | Variant selection tested; effective-time & outage untested (INV-2, LIV-6); regression guard unimplemented (GAP-20) |
| REQ-41 | P | FV-2 attempt bound ledger-checked by the smoke; CT-2 exercises reroute-on-failure and penalty decay (LIV-7) across five verdicts, the two capacity ones included — an exhausted run of those answers OVERLOADED with its hint rather than a bare transient outage. **Known-violated**: a generic worker server-error is classified terminal, so one erroring worker fails the request instead of rerouting (GAP-23). Cooldown durations and priority-group selection remain untested |
| REQ-42 | P | Scheduler units; headroom refusal untested (INV-4, LIV-8), and its observable — shrink cause, download utilization, headroom-refusal counter (OB-7) — is unasserted |
| REQ-43 | P | Positive path exercised by the smoke (signed stub responses verified and delivered); CT-2 now drives the rejection path — a wrongly-signed response is not delivered and the attempt is retried elsewhere, meeting the acceptance criterion. Integrity failures are counted per worker but raise no OB-9 alarm state (GAP-24) |
| REQ-44 | U | — |
| REQ-50 | P | CT-10 drives the acceptance corpus whole: no credential, an unparseable token, an unknown key, a wrong secret, a revoked key, an expired key, a key scoped to another portal and one scoped to another dataset each get the refusal ADR-011 binds to it, no serving-dependency stub records a call, and the control-plane ledger shows at most one exchange per request and none for a cache hit or a token outside the grammar. A valid unscoped key is served the same 40 blocks the non-commercial fixture serves. Canonicalization confined to the authenticated dataset rung is not observable black-box |
| REQ-51 | P | CT-10 asserts a keyless stream is refused while the catalog, per-dataset metadata and state, both heads, `/status`, `/ready`, `/metrics` and the schema all answer without one. A route declaring neither does not compile, which `Gated::route`'s signature enforces and the router unit tests pin. The bracketing rule is covered by its consequences — byte-identical bodies for the reasons sharing `invalid_credential`, and no key id or internal rung anywhere in the scrape — rather than by a strict two-scrape delta |
| REQ-52 | P | CT-10 accepts a valid key through the header and refuses the truncated, over-long, unknown-prefix and outside-alphabet forms, none of which reaches an exchange; the same token in a query parameter is refused as though none were presented. No log record, metric label or error body carries the secret — the response check runs on every response the suite reads, and the log and scrape are grepped whole. Constant-time fingerprint matching is not observable black-box |
| REQ-53 | P | The ladder's order is driven end to end — absent before malformed, malformed before any exchange, the control plane's four rungs each reporting their own code, and dataset scope read from the returned grant and evaluated last. An empty dataset list is distinguished from an absent one, and an alias resolves to its canonical name before matching. The combined case, a key both revoked and scoped elsewhere, is untested |
| REQ-54 | P | Every way an exchange can fail to produce a verdict — 500, 503, 404, a truncated grant, an unknown claims version, an answer about another key, and the deadline — reaches the client as retryable `upstream_unavailable`, never as a claim about the credential. An over-cap lifetime is honoured, shortened and counted. The grace rules are the untested half: serving through an outage and stopping at `expires_at` both need a clock (GAP-33) |
| REQ-55 | P | CT-10 runs a shadow portal: a request with no credential, one with an ungrammatical token and one the control plane denies are all served, the verdict enforcement would have returned is in the protected log, and the keyless scrape carries only the neutral `shadow_evaluated` series with no code and no exchange counters. The control-plane ledger shows it exchanging on the same cache-miss rule enforcement uses — once, for the only request that presented something to exchange. Indeterminate exchange outcomes are not separately driven |
| REQ-56 | P | CT-10 runs a portal with no `commercial:` block: gated routes are served without a credential, a credential presented anyway is not a reason to refuse, and no authorization series appears in the scrape at all. The empty-block startup error is unit-tested in `commercial::config` rather than here, and the startup mode line is logged but not asserted |

## Gap register — 2026-08-07

Priorities: P0 blocks the program · P1 active production risk · P2 correctness hole
with plausible trigger · P3 polish. "Next" = cheapest failing-test-first entry.

| GAP | Statement | Violates | Priority | Next |
|---|---|---|---|---|
| GAP-1 | Assignment artifact adopted with no structural validation; a corrupt blob can panic the refresh path or leave the Portal ready on garbage routing | REQ-26, INV-36, FM-2 (ADR-002) | P1 | CT-2: truncated-artifact stub → assert reject + alarm + prior artifact kept |
| GAP-2 | Artifact staleness unbounded and invisible: fetch failures log-only; readiness ignores age | REQ-23/40, INV-31 ⚠, LIV-12, OB-6/9 (ADR-013, OQ-3) | P1 | age gauge; readiness-degradation test past P-ASSIGNMENT-MAX-AGE |
| GAP-3 | Refresh holds old + new artifacts resident (HZ-1, ~2× P-ASSIGNMENT-SIZE) and first-byte waits are unmetered (HZ-2). Baseline: 2026-07-17 OOM-kill restart storm on 0.11.8 | REQ-27, PF-1, SLI-5 (OQ-4) | P1 | RSS-during-refresh probe under S4; heap profile to pin the dominant term |
| GAP-4 | Current master does not implement the stream-cap refusal contract: cap exhaustion yields a 503/no mandatory hint, and a beyond-frontier timestamp still returns 404 where ADR-014 fixes it as the 204 EMPTY outcome. The timestamp surface no longer flattens the overload outcome — it carries the classified refusal whole, so a congested resolve answers 529 with its hint rather than 503 `upstream_unavailable` | REQ-20, REQ-13, INV-12, PF-6 | P1 | CT-3: occupy P-MAX-STREAMS; assert 529 + hint and admitted-stream integrity |
| GAP-5 | Latent panic on an empty stream ("first chunk missing") — known trigger fenced only | REQ-21, INV-36 | P2 | replace panic with error; empty-yield test |
| GAP-6 | Worker-labeled metric cardinality and name interning grow without eviction | REQ-30, OB cardinality rule, HZ-6 | P2 | CT-7 series-count audit across churn |
| GAP-7 | Archival-path CONFLICT payload has only one entry. It meets the REQ-3/INV-23 MUST minimum but not REQ-3's richer-ancestor SHOULD | REQ-3 SHOULD | P2 | CT-5 contract test on the 409 body |
| GAP-8 | Advertised tuning params accepted then silently ignored on public routes | REQ-8, IB-3 (OQ-1) | P2 | decide; then honor-and-clamp or 400 test |
| GAP-10 | Container healthcheck probes a nonexistent route | REQ-23 operability | P2 | point at /ready; compose test |
| GAP-11 | Served API description drift: undocumented route/header, stale examples, size-doc conflict | REQ-32, IB-2/4 | P3 | CT-5 description-vs-router sweep |
| GAP-12 | Download-priority key wraps at ~43 M streams (HZ-4) | REQ-42 fairness | P3 | widen key; wrap-boundary unit test |
| GAP-13 | ADR-009 accepted but portal-side injection unimplemented — decision drift | OQ-5 | P3 | schedule or supersede |
| GAP-16 | ADR-011's envelope is integrated and CT-5-covered on both emitters, with proxied-hint injection and clamping, unmatched-4xx normalization to 400, upstream bodies neither published nor read, framework rejections normalized at the middleware onto their bound status and with their endpoint label preserved, one upstream classifier shared by the stream proxy and the timestamp route, and `/ready` declining with the IB-6 envelope. Responses that never reach the routed middleware are normalized, logged and counted by an outer layer — an unmatched *route*, which a `route_layer` cannot see, and the decompression layer's 415, which every encoding but `gzip` earns — under a constant `endpoint` label, the path being client-supplied there. Remaining: EMPTY head metadata from ADR-014; and an unmatched proxied 4xx is indistinguishable from a genuine 400 on the metric, so there is no signal for an upstream returning a status the Portal does not model | INV-26, OB-3, REQ-13 | P2 | ADR-014 remainder |
| GAP-28 | A proxied 409 whose body carries no usable `previousBlocks` — absent, empty, wrong-shaped, or past the read cap — still answers 409. The list is now typed and non-empty, so nothing unusable is published, and the refusal is logged at `error`; but the client receives a status IB-5's normative recovery procedure cannot be run against. Refusing it as `unclassified` instead is the open contract call | IB-5, INV-23, REQ-3 | P2 | decide the status; CT-5 already pins the current shape |
| GAP-17 | Count caps imply a multi-terabyte theoretical buffer ceiling and no global byte budget or accounting exists; the congestion waiter queue is unbounded on the same path (HZ-9) | REQ-27, PF-1, OQ-9 | P1 | add byte meter/admission test; set P-BUFFERED-BYTES-BUDGET |
| GAP-18 | Chain-RPC calls have no explicit deadline despite accepted ADR-010 | REQ-22, DC-5, HZ-8 | P2 | stalled-RPC stub → assert bounded call and loop recovery |
| GAP-19 | Conflict detection is not known to precede the beyond-frontier EMPTY: a real-time continuation at the head with a stale parent may poll empty instead of getting 409 (the pre-ADR-014 oracle ordered EMPTY first; master unverified) | REQ-3, INV-23, INV-27 (ADR-014) | P2 | CT-2: reorg-at-head stub world → assert 409 precedence |
| GAP-20 | No regression guard on artifact application: a republished older artifact (different identifier) would be re-applied | REQ-40, INV-2, DEF-4 (ADR-014) | P2 | CT-2: regressive publisher stub → assert not applied |
| GAP-21 | Debug stream variant bypasses clamps without an operator gate | INV-11, REQ-8 (ADR-014; closed OQ-6) | P2 | config test: route absent unless the operator flag enables it |
| GAP-22 | Pre-first-byte outcomes of the real-time source now have one denominator in `hotblocks_requests`, but no objective. `response` and `replay_response` obtained a response head; `replay_failed`, `timeout`, and `transport_failed` did not; cancellation remains visible without inventing an upstream result. SLI-4 is readiness availability and excludes deploys, SLI-6 counts truncated streams, and neither turns this counter into a request-success SLI | SLI set, OB-4 (ADR-015) | P2 | define a DC-4 response-head SLI over `hotblocks_requests`, with cancellations explicitly included or excluded by policy |
| GAP-23 | A generic worker `ServerError` verdict is classified terminal, so the first erroring worker fails the whole request with no reroute — while `NotFound` and `ServerOverloaded` from the same worker *are* rerouted. DC-1 and the 09 worker table both put server errors in the reroute column. Found by the CT-2 injector on 2026-07-25; recorded as known-violated in `ct2_worker_faults` | REQ-41, DC-1, FM-3, LIV-12 | P1 | reclassify as retriable and drop the case from CT-2's known-violated list; the risk to weigh is added load on a fleet that is erroring |
| GAP-24 | Integrity failures are counted per worker (`query_results{status="integrity"}`) but raise no alarm *state*: OB-9 lists signature-verification failures as an alarm, and FM-2 requires integrity faults to be "rejected and alarmed". A fleet quietly serving wrong-range data is visible only to someone already looking at the counter | OB-9, FM-2, REQ-43 | P2 | add the OB-9 alarm state over the integrity counter; assert the edge event in CT-2 |
| GAP-25 | DC-1's parent-hash-mismatch and oversized-result verdicts are detected by exact-string parsing of worker `server_error` messages (`unexpected base block: …`, `Response too large`); the worker contract declares message strings unstable, and the worker's second oversize string (`query result too large`) already misses the parse and lands in the terminal generic-failure path (GAP-23). Blocked on a stable worker-side verdict surface (tracked in the worker suite: `worker-rs/spec/13`, anchor-verdict gap) | DC-1, REQ-3, INV-23 | P2 | CT-2: worker stub emits a reworded mismatch string — today the client gets a terminal error instead of 409; flips to a hard gate when the stable surface lands |
| GAP-26 | No aggregate deadline bounds a worker attempt: connect is a 10 s crate default (P-WORKER-CONNECT-TIMEOUT, not operator-bound), first byte 60 s, and the body is read in 1 s-stall-bounded reads with no total bound — DC-1's single "request deadline P-TRANSPORT-TIMEOUT" is not what runs, and the transport's `request_timeout` is set but unused on this path | DC-1, REQ-22, HZ-2 | P2 | stub worker trickles a body at just under the per-read stall bound indefinitely; assert the attempt is bounded |
| GAP-27 | Penalty classification diverges from ADR-004's cost rationale: an instant stream reset (the worker's documented flood-shed posture) draws the 300 s timeout-class cooldown plus a congestion signal, and integrity penalties are split (bad signature 300 s vs undecodable/wrong-range 30 s) though DC-1 treats them as one class — a brief worker-side flood can push the whole pool to AllUnavailable | REQ-41, DC-1, LIV-7 | P2 | CT-2: instant-reset injector; assert the cooldown class matches observed cost and the pool recovers within the error-cooldown window |

| GAP-30 | Nothing consumes the authorization signals OB-12/13 require, and none of OB-9's three commercial alarms exists: no dashboard shows refusal rate by code against admitted traffic, and grace admissions — the only warning before the `expires_at` cliff, and the whole window an operator has to act in — has no alarm rule. Lives in the monitoring stack, not the binary, so it did not block the authorization path landing, but must not trail it into production either | OB-9, OB-12, OB-13 (presentation only) | P2 | alarm on the grace counter first, then build the cutover dashboard |
| GAP-32 | **Accepted residual.** A credential with no cached grant costs an exchange and so answers more slowly than a cached one. In enforcing mode, under exchange pressure it receives retryable OVERLOADED/UPSTREAM-FAILURE where a cached credential receives BAD-CREDENTIAL, so the accurate retry contract also reveals cache membership in the response; shadow mode admits both and keeps the same neutral public projection. Accepted rather than closed: collapsing the enforcing outcomes means answering a dependency failure with a claim about the key, which REQ-54 forbids. It is not a key-id oracle — the cache is keyed on the whole credential, so an unknown id and a wrong secret miss identically — and public metrics do not amplify it | INV-39 | P3 | revisit only if the channel is shown to be exploitable at scale |
| GAP-33 | CT-10's harness exists and its request-path rows are driven black-box, but every row whose claim is about *time* is still unwritten: LIV-13's convergence at `refresh_after` and at `expires_at` with the control plane stopped, a retired generation completing after its successor (INV-6), the denial TTL and the grace window, and HZ-12's renewal spread. All of them need either a controllable clock or a run long enough to cross a real one, and the stub has neither | CT-10, DC-8, INV-6, LIV-13, HZ-12 | P2 | give the control-plane stub a settable clock offset, or drive the cases with second-scale lifetimes; `expires_at` convergence with the control plane stopped is the one carrying the security claim |

### Closed findings

- **GAP-31** (closed 2026-08-06; superseded 2026-08-07): key-snapshot staleness was
  unbounded and unexported, and was closed by exporting an age gauge and alarming on it.
  The on-demand exchange removed the snapshot, and with it both the defect and its fix: the
  stale-authorization window is now a deadline the control plane sets on each grant rather
  than an age a dashboard watches. What the closure argued still holds and is why INV-31
  reads as it does — a staleness rule in the binary would pull the whole fleet from
  rotation at once, since every replica shares one authority.

- **GAP-29** (closed 2026-08-05): authorization refusals were built outside the ADR-011
  envelope, so they carried no `ErrorCode` and the middleware every routed response passes
  through rewrote them to 400 `malformed_request`. No 403 reached the wire and every refusal
  counted as a client query error. ADR-011's authentication types and codes are implemented and
  CT-5 pins an unauthenticated request behind
  the *real* middleware stack — the only place the defect was visible, since the gate and
  the normalizer were each correct alone.
- **GAP-34** (closed 2026-08-05): rate and in-flight exhaustion and control-plane errors
  all collapsed onto one "no such key" answer, so a key minted seconds earlier was told
  non-retryably that it was invalid. `Lookup` now separates the authoritative unknown from
  saturation and from failure, and the latter two answer OVERLOADED and UPSTREAM-FAILURE
  per REQ-54. The digestless tombstone half is closed too: it answers `invalid_credential`
  and keeps `revoked_tombstone` on the protected axis, where naming it discloses nothing.
  Writing the test store's control plane is what exposed the first half — until then the
  suite's "unknown key" case was really its unreachable-control-plane case. (The tombstone
  half was made moot by the on-demand exchange, which leaves the Portal holding no records to
  tombstone; the separation of saturation from failure from denial carries over intact and
  is the part worth keeping.)

- **Wrong-range worker responses** (closed 2026-07-25): a worker reporting a last block
  past the queried range end produced an inverted continuation range and panicked the
  stream task (a 500 for the client); one below the range start failed the whole stream
  with no reroute. Both are now DC-1 integrity failures — discarded, counted, rerouted —
  and CT-2 asserts the client cannot tell a single equivocating worker apart from a
  healthy one. Building the CT-2 injector that proved it is what surfaced GAP-23.
- **GAP-22, original claim** (closed 2026-07-21): the ADR-015 replay landed in the DC-4
  transport, with unit coverage for the clean close, the reset, the at-most-once bound,
  and the two non-replay exclusions (mid-body, read stall). Writing the reset case found
  a real defect in the first implementation: the classifier returned on the first
  `hyper::Error` in the source chain, so a peer that resets rather than closing cleanly —
  the common shape when a replica dies with the request still unread — was never
  replayed, and the fix would have missed most of the incident it was written for.
  GAP-22 now tracks only the indicator residual.
- **GAP-9** (closed 2026-07-17): EMPTY delay occurs after the stream census permit is
  released. It can occupy an HTTP connection/task (HZ-3), but does not consume the
  stream cap.
- **GAP-14** (closed 2026-07-17): the Phase-0 harness skeleton landed as the
  `harness/` crate — stubs per IB-7 with ledgers, toy world, oracle, validators,
  driver, gauge audit; CT-1 smoke green. Its first run found a real REQ-9 defect:
  the response `x-request-id` echo layer was attached to the empty router (wrapping
  zero routes) and never ran; fixed the same day.

## Build order

- **Phase 1 — P1 gaps, failing tests first:** GAP-1/2/4/16/17/23 tests
  red → fixes; GAP-3 probe + heap profile → refresh-copy fix. GAP-23 already has its
  failing case parked in `ct2_worker_faults`'s known-violated list. The authorization
  band is done: the path landed and CT-10 checks it, which is the order that mattered —
  an implementation landing without a harness is one whose properties nothing can check.
- **Phase 2 — correctness core:** full CT-1 oracle diffing; the rest of the CT-2 fault
  matrix (DC-2/DC-3/DC-4 injectors, kill/restart) on the `Fixture` + worker-injector
  scaffolding; CT-4 corpus; burn down P2 gaps.
- **Phase 3 — robustness:** CT-3 swarms; CT-8 isolation; CT-9 fuzz; INV-30 audits.
- **Phase 4 — performance regime:** CT-6 benchmarks S1–S6; ratify ⚠ SLO targets
  (OQ-3/OQ-4/OQ-9/OQ-10); commit baselines; CT-7 soak on a CI cadence.

Each phase ends by updating this file's matrices and register in the same change.
