# 07 — Safety invariants

Scope tags: `[state]` holds in every observable state · `[transition]` across
consecutive states · `[response]` for every response · `[recovery]` across restart.
Bands: structural 1–9 · operation legality 10–19 · response semantics 20–29 ·
reporting 30–34 · isolation 35–39 · recovery 40–44. Access-control invariants sit in
the band their scope puts them in, not in one of their own. *Check* names the test class
(CT-n, [13-conformance.md](13-conformance.md)).

## Structural (1–9)

**INV-1 — Artifact atomicity.** [state]
All routing reads at any instant derive from exactly one applied artifact (DEF-4);
a partially applied artifact is never observable.
*Why:* mixed-artifact routing sends chunks to workers that don't hold them.
*Check:* CT-3 — route reads racing an artifact swap; CT-1 oracle comparison.

**INV-2 — Artifact application legality.** [transition]
An artifact is applied only if its identifier differs from the applied one, its
effective-from time has passed, and its effective-from is not earlier than the applied
artifact's (a republished older artifact is never re-applied). Application replaces the
whole artifact.
*Why:* re-applying identical artifacts churns; early application splits the fleet;
regression re-serves routing the network already retired.
*Check:* CT-2 — publisher stub serves future-effective and regressive artifacts; assert
neither is applied.

**INV-3 — Lease balance.** [state]
Per worker, open leases ≤ P-MAX-QUERIES-PER-WORKER; every lease acquired is released
exactly once (on completion, failure, or client disconnect); at quiescence all lease
counts are zero.
*Why:* leaked leases permanently mark workers busy — silent capacity loss.
*Check:* CT-3 — swarm with random disconnects; assert quiescent lease census is zero.

**INV-4 — Congestion window bounds.** [state]
The configured window stays within [P-CONGESTION-MIN-WINDOW,
P-CONGESTION-MAX-WINDOW]. New permits are granted only when active permits are below
the current window; after a multiplicative shrink, active permits may temporarily exceed
the new window and no additional permit is granted until they drain below it. Every
acquired or reserved permit is released exactly once.
*Why:* window escape = either unbounded concurrency or a frozen portal.
*Check:* CT-1 property tests on the scheduler; CT-3 swarm.

**INV-5 — Stream census accuracy.** [state]
Active streams ≤ P-MAX-STREAMS plus a transient overshoot bounded by concurrently
arriving admissions; the census returns to the true count when arrivals settle; every
admitted stream decrements exactly once at its end.
*Why:* census drift silently shrinks (or unbounds) global capacity.
*Check:* CT-3 — concurrent admit/finish/disconnect storm; compare census to truth.

**INV-6 — Grant cache coherence.** [transition]
Every entry of the grant cache (DEF-18) is reachable only by the fingerprint of the whole
credential that earned it, and only until it passes `expires_at`; past that it admits
nothing, whatever the state of the control plane. "Newer" means the result of a
later-started local exchange generation for that fingerprint, never a comparison of grant
deadlines. Starting a successor retires the earlier generation, so a late completion from
the retired attempt cannot write. An authoritative denial evicts on arrival and is never
overwritten by an exchange generation that started before it. An answer the Portal cannot
fully read — a
missing field, an unrecognized claims version, a subject other than the one asked about —
is never stored (DEF-17).
*Why:* the ways a cache silently un-revokes a key are writing a stale answer over a fresh
one, letting an entry outlive the deadline it was issued under, and admitting on the part
of an answer that parsed.
*Check:* CT-10 — time out one exchange without suppressing the stub's late completion,
start its successor, and deliver the two answers in reverse generation order; also return a
denial from the successor while the retired call can still complete. Assert the retired
completion writes nothing, the denial survives, and an entry stops admitting at
`expires_at` with the stub unreachable.

## Operation legality (10–19)

**INV-10 — Validation precedes serving work.** [response]
A request failing DEF-7 validation triggers no serving-dependency call and no shared-state
mutation beyond authorization caches and observability. Because OP-11 precedes operation
validation, a well-formed credential with no usable grant may already have made the one
bounded DC-8 exchange INV-14 permits; no other dependency exception exists.
*Why:* invalid input must be cheap to reject and must never buy the work that serves it;
the earlier authorization exception has its own explicit bounds.
*Check:* CT-4/CT-10 — invalid-request corpus against dependency stubs; assert zero
serving-dependency calls and at most the one DC-8 exchange.

**INV-11 — Clamping.** [response]
Effective tuning = min(requested, operator cap) with defaults for absent values; no
request on an operator-enabled surface can obtain more than P-BUFFER-MAX read-ahead,
P-MAX-CHUNKS-PER-STREAM chunks, or P-STORED-RESULTS-PER-CHUNK buffered results per
chunk. The clamp-bypassing debug variant exists only behind an explicit operator flag,
disabled by default (ADR-014; GAP-21 until gated).
*Why:* client-controlled resource amplification.
*Check:* CT-1 — boundary values; observe via coverage/behavior and metrics.

**INV-12 — Admission discipline.** [transition]
When the census or congestion utilization is at its bound, new streams are refused as
OVERLOADED without touching workers; refusals never abort or degrade already-admitted
streams.
*Why:* the alternative is the 2026-07 storm: refusal work competing with real work.
*Check:* CT-3/CT-6 — saturate, assert refusal class + running-stream integrity.

**INV-13 — Single source per response.** [response]
Every routed stream/timestamp response is served entirely by one serving source
(DEF-6), and successful responses plus post-routing failures name that source truthfully.
Pre-routing validation, alias, and admission failures have no source marker.
*Why:* silent source mixing breaks continuation and fork semantics at the seam.
*Check:* CT-5 — marker vs stub ledger (which stub actually served).

**INV-14 — Authorization precedes work.** [response]
On a gated route (DEF-19), a request that does not pass OP-11 causes no serving-dependency
call, no handler execution, no stream-census admission, and no shared-state mutation
beyond the grant cache, its negative answers, and authorization observability. A credential
with no usable grant may make the one bounded DC-8 exchange OP-11 declares — one, whatever
the request rate, because concurrent requests on a fingerprint share a single call. A token
outside the grammar makes none. Dataset canonicalization is itself work: it happens only
for a credential that has already authenticated and only where the key's scope requires it
(REQ-53). On an ungated route, or a Portal with no commercial configuration, no part of
this runs.
*Why:* an unauthenticated request must not be able to buy unbounded or serving-path work.
The one attacker-reachable exception is now the ordinary path rather than a rare miss, so
its rate, concurrency, single-flight, negative-cache and deadline bounds are what stop the
gate from being an open cost amplifier pointed at the control plane (HZ-10).
*Check:* CT-10 — keyless and bad-key corpus against every gated route with dependency
stubs and a counting catalog; assert zero serving-dependency calls, at most one DC-8
exchange per fingerprint under a concurrent burst, none at all for a malformed token or a
cached grant, and canonicalization only after authentication on the dataset rung.

**INV-15 — Verdict determinism.** [response]
The authorization verdict (DEF-20) is a pure function of (credential, grant or denial,
requested dataset, current time). It never depends on load, capacity, prior requests, or
which replica served, and the ladder's precedence (REQ-53) is total: a request failing
several rungs always reports the earliest. Two replicas holding the same grant and
evaluating at the same time return the same verdict for the same request. They need not
hold the same grant — each exchanges on its own traffic, so one replica may be a refresh
ahead of another, and that divergence is bounded by `expires_at` rather than eliminated.
*Why:* a verdict that varies with load is an availability bug wearing an authorization
costume, and a precedence that varies makes the refusal reason — the operator's only
diagnostic — untrustworthy. Per-replica grant divergence is the honest cost of asking on
demand, and stating its bound is what keeps it from being mistaken for this defect.
*Check:* CT-10 — the multi-failure corpus of REQ-53 against a fixed set of grants, replayed
under saturation and on a second replica; assert identical verdicts, and that two replicas
given the same denial converge within their grants' `expires_at`.

## Response semantics (20–29)

**INV-20 — Coverage integrity.** [response]
Within a response's coverage (DEF-8): records appear in strictly ascending block order;
every block matching the query appears exactly once; no gaps. Equal to the reference
model's output for the same coverage (modulo FV free variables).
*Why:* the product's core promise (REQ-1); the 2026-06 duplicate incident class.
*Check:* CT-1 — oracle comparison over randomized queries and stub data.

**INV-21 — Bounds respect.** [response]
No record lies below the requested first block, above the requested last block, above
the frontier, or (finalized mode) above the finalized head.
*Why:* overshoot delivers unfinalized/duplicate data the client didn't ask for.
*Check:* CT-1 structural validator on every response.

**INV-22 — Provenance fidelity.** [response]
Every delivered record equals the record provided by the serving source for that block
(modulo declared re-encoding). The Portal never fabricates, edits, or reorders content
within a block record.
*Why:* a proxy that silently alters data is worse than a broken one.
*Check:* CT-1 — ledger comparison (stub-signed payloads) byte-equal after decoding.

**INV-23 — Conflict correctness.** [response]
CONFLICT is returned iff a real-time-mode request's parent hash disagrees with canonical
data at that height (validation enabled); its payload is non-empty canonical block
references, ascending, ending at the parent's height. Conflict detection precedes the
EMPTY outcome: the parent hash is validated whenever its height is at or below the
frontier, even when the first block lies beyond it — a continuation at the head across
a reorg conflicts, it does not poll empty (ADR-014). Finalized-mode requests never
yield CONFLICT.
*Why:* wrong conflict handling strands clients after reorgs (REQ-3).
*Check:* CT-2 — stub serves a reorged chain; drive the recovery algorithm to re-anchor.

**INV-24 — Metadata honesty.** [response]
Head markers equal the currently known heads (within the staleness bounds of 05);
they are never lowered, invented, or omitted on success (ADR-009: data may lag policy;
reported heads never lie).
*Why:* clients schedule polling and detect truncation from these markers.
*Check:* CT-5 — markers vs stub-controlled heads across staleness windows.

**INV-25 — Truncation well-formedness.** [response]
A stream body always ends on a record boundary with valid encoding — whether complete
or truncated; a truncated body is indistinguishable from a short complete one at the
encoding level.
*Why:* torn records corrupt client decoders (ADR-001's price must stay this low).
*Check:* CT-2 — kill the serving stub mid-stream at every phase; decode-validate.

**INV-26 — Error soundness.** [response]
Every failure maps to exactly one DEF-10 `type`/`code` pair; body-bearing errors use the
ADR-011 envelope and carry no partial data. OVERLOADED always carries a retry hint ≥
P-RETRY-AFTER-MIN — Portal-set locally, preserved from the upstream when proxied, and
injected at the floor when the upstream omitted it (IB-5, ADR-014); DATA-UNAVAILABLE
never carries one; no code outside DEF-10 is emitted.
*Why:* the closed taxonomy is what clients and SDK backoff logic key on (ADR-011/012).
*Check:* CT-5 — exhaustive fault matrix → assert type, code, hint presence, body shape.

**INV-27 — Empty-success semantics.** [response]
For an admitted request, EMPTY is returned iff no conflict takes precedence (INV-23)
and the first block exceeds the frontier (or falls in the archival/real-time retention
gap); it is delayed ≥ P-NO-DATA-DELAY, carries current head metadata (DEF-8), and
implies no skipped data: the client's progress is unchanged.
*Why:* EMPTY doubles as the head-polling throttle (REQ-5); it must not consume a stream
census slot while delaying the HTTP response.
*Check:* CT-1 — frontier boundary sweep; timing assertion.

**INV-28 — Response-content purity.** [response]
The rule of 04: record content is a function of (request, configuration, dependency
data) only. Identical requests against identical dependency state yield
content-identical record sequences up to coverage extent (FV-4), regardless of
concurrent load, prior traffic, or process age.
*Why:* the whole conformance method rests on it — it is what makes an oracle possible.
*Check:* CT-3 — same request replayed cold/hot/under-load; diff record content.

**INV-29 — Boundary-block emission.** [response]
A successful response that evaluates at least one block delivers a record for at least the
first and the last block of its coverage — header-only when the block matches no item
filter — regardless of `includeAllBlocks`. The source pins this boundary per *served
chunk* — the query engine evaluates one plan per chunk — so a multi-chunk response also
carries header-only records at interior chunk boundaries; coverage's global first and last
are the guaranteed minimum. Hence the last delivered record's reference equals the coverage
cursor (DEF-8), and a client resumes from it gap-free even when a selective query matched
nothing at the tail. A range that evaluates no block at all is the EMPTY outcome (INV-27),
not a recordless success.
*Why:* this is what makes REQ-2 resumable progress hold on the current wire with no
dedicated cursor field; the query engine pins the coverage boundary at weight 0 (a
non-matching block otherwise carries a null weight and falls out of the size budget), so
it must not silently regress.
*Check:* CT-1 — selective, multi-chunk world whose tail block matches nothing; assert the
last record is the coverage boundary, that interior chunk boundaries may add header-only
records (FV-6), and that a continuation from the last record is gap-free and overlap-free.

## Reporting (30–34)

**INV-30 — Metrics honesty.** [state]
At quiescence, gauges equal modeled truth: active streams = 0, in-flight permits = 0,
open leases = 0, known workers = artifact worker count. Lying metrics are failures, not
cosmetics.
*Why:* operators act on these during incidents (the 2026-07 storm was diagnosed
through them).
*Check:* CT-3/CT-7 — scrape after swarm quiescence; compare to harness ledger.

**INV-31 — Readiness honesty.** [state]
Ready ⇒ (an artifact is applied ∧ connectivity ≥ P-READY-CONNECTION-RATIO ∧ not
shutting down). Shutdown flips readiness before intake stops (ADR-005). Intent ⚠: ready
also ⇒ artifact age ≤ P-ASSIGNMENT-MAX-AGE (ADR-013, GAP-2). Commercial configuration adds
no conjunct in either enforcement mode and will not get one: there is nothing to load
before serving, and every replica shares one authority, so a readiness rule keyed on the
control plane would empty the fleet during exactly the outage that triggered it (REQ-54).
An unreachable control plane is answered with retryable refusals and an alarm (OB-9), which
keeps the failure attributable to the thing that failed.
*Why:* orchestrators route by this; a lying probe turns deploys into outages.
*Check:* CT-2 — drive each conjunct false via stubs; probe.

## Isolation (35–39)

**INV-35 — Request isolation.** [response]
No request's input, failure, or disconnect alters the record content of any other
response. (Capacity and timing coupling is declared and exempt — NG2.)
*Why:* multi-tenant correctness floor.
*Check:* CT-8 — adversarial neighbor swarm; diff victim responses against solo run.

**INV-36 — Hostile-input containment.** [state]
No client-supplied byte sequence terminates or wedges the process (REQ-21). The
process outlives any single request's failure, including panics inside a stream's
serving task (which truncate only that stream, FM-1).
*Why:* one curl must never be a denial of service.
*Check:* CT-4/CT-9 — fuzz all client surfaces; process liveness probe.

**INV-37 — Declared side effects only.** [state]
The Portal's only external interactions are those declared in 05 (DC-1..DC-8; DC-8 only
where the operator configured it — REQ-56).
*Why:* undeclared calls are unbudgeted failure modes and security surface.
*Check:* CT-2 — harness observes all egress; anything not stub-addressed fails.

**INV-38 — Credential confidentiality.** [state]
Past the request parser a presented secret exists only in the bounded request-local
exchange input DEF-16 describes and as the fingerprint the parser reduced it to. The raw
input is destroyed immediately on a cache or negative-answer hit; on a miss, one owner
moves it into the single DC-8 exchange and destroys it on completion, timeout, or
cancellation. It leaves the process in that direction and by no other: no log record,
metric label, span field, error body, shared or persisted value, or any other outbound
request carries it, and the grant cache holds fingerprints rather than credentials.
*Why:* an API key that reaches a log line has been disclosed to everyone with log access.
The one egress is not a loophole but the point of naming it: an exception that is written
down is one a test can bound, and INV-37 already forbids every other destination.
Fingerprint comparison is deliberately *not* required to be constant-time. The attack that
would motivate it — refining a wrong secret byte by byte from response timing — needs the
attacker to steer the bytes being compared, and what is compared is SHA-256 of a token they
supply, so steering it means inverting the hash. Buying immunity to an unavailable attack
would cost the lookup: a constant-time match against a keyed cache has to touch every entry,
turning an O(1) hit into a scan of P-GRANT-CACHE-CAPACITY on every request. What the timing
does leak is cache membership, which is the residual INV-39 already accepts.
*Check:* CT-10 — drive the full corpus with a marked secret; grep every emitted log,
metric, span, body, and shared-state dump for it; assert it appears in the control-plane
stub's ledger only on a miss and in no other stub's; force hit, timeout, cancellation, and
coalesced-waiter paths and assert no raw input outlives them.

**INV-39 — No enumeration oracle.** [response]
On every completed credential verdict, an unparseable token, an authoritatively unknown
key id, and a known key id presented with a wrong secret are indistinguishable to the
client: same status, same code, same body (ADR-011). The reasons that *are* distinguishable
— revoked, expired, wrong portal, wrong dataset — are reachable only by a client already
holding the correct secret. The distinction survives on the internal axis only, in
protected structured logs — never as a label or request-synchronous counter on the keyless
metrics surface (OB-12/13).
*Why:* telling a caller that a key id exists turns the endpoint into a key-id oracle, and
the whole point of a public key id plus a secret is that the id alone is worthless.
*Check:* CT-10 — assert the three responses are byte-identical apart from the request id;
assert the specific reasons appear only for correct-secret requests; bracket every case
under both enforcement modes with metrics scrapes and assert no public series reveals the
internal reason or the exchange path beyond the response that case received. In shadow
mode, where every case is admitted, their authorization projections are identical.
*Accepted deviation:* a credential with no usable grant costs an exchange while a cached
one answers locally, so timing differs in either enforcement mode. In enforcing mode, if
that exchange cannot run or answer, its retryable OVERLOADED/UPSTREAM-FAILURE response also
differs from a cached BAD-CREDENTIAL. Shadow mode still admits both cases and exposes the
same neutral public authorization projection; only protected telemetry records the
exchange result. The residual reveals *cache membership* under exchange pressure, and is
accepted because the alternative — collapsing both enforcing responses onto one outcome —
means answering a dependency failure with a claim about the key, which REQ-54 forbids. It
is not a key-id oracle: the cache is keyed on the whole credential, so an unknown id and a
known id with a wrong secret miss identically, and neither timing nor public counters
separate them.

## Recovery (40–44)

**INV-40 — Restart equivalence.** [recovery]
After restart and readiness, response content for any request equals the pre-restart
content given identical dependency state; adaptive-state resets (worker health,
window) may change only routing, timing, and coverage extent per INV-28.
*Why:* restart amnesia is a design decision (NG5) — it must be behavior-neutral.
*Check:* CT-2 — kill/restart between replayed identical requests; diff content.

## Reading the catalog in tests

Structural validators (13 §validators) enforce INV-20/21/25 on every response for
free. The dependency-fault matrix (CT-2) owns INV-2/23/25/31/37/40. Concurrency swarms
(CT-3) own INV-1/3/4/5/12/28/30/35. The fuzz corpus (CT-4/9) owns INV-10/36. Interface
conformance (CT-5) owns INV-13/24/26. Authorization (CT-10) shares INV-10 and owns
INV-6/14/15/38/39. Every response in every class re-checks the `[response]` band via the
validators.
