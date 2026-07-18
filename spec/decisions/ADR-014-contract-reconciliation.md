# ADR-014 — Contract reconciliation: overload hints, conflict precedence, artifact regression, archival finalized head, EMPTY metadata, timestamp frontier, gated debug surface

Status: Accepted (2026-07-17)

## Context

A full-suite consistency review found places where normative documents contradicted
each other — each contradiction a fork an implementer or test author would have to
resolve ad hoc. Most were resolved editorially (the suite already contained the
decided intent on one side). Seven resolutions change or pin *intended behavior* and
are recorded here so the immutable docs can change under the suite's own rules.

## Decisions

1. **Proxied overload responses always carry a retry hint.** ADR-012's rule is
   universal: when the real-time upstream returns 429/503/529 without `Retry-After`,
   the Portal injects `Retry-After: P-RETRY-AFTER-MIN` while otherwise preserving
   public upstream headers. A hint-less proxied overload would re-create the 2026-07
   refusal storm on the real-time path. (INV-26, DC-4, IB-5.)

2. **Conflict detection precedes the beyond-frontier EMPTY outcome.** In real-time
   mode, a parent hash whose height is at or below the frontier is validated before
   emptiness is decided: a continuation at the head across a reorg gets 409, not an
   endless empty poll. Corollaries: finalized-mode worker queries carry no parent
   hash (finalized streams never conflict), and disabling validation portal-wide
   stops both validating and propagating the parent hash. (REQ-3, INV-23, INV-27,
   OP-1, DC-1.)

3. **Artifact application is monotone.** An artifact whose effective-from is earlier
   than the applied one's is never applied, making the failure model's "regressive
   republish is masked" claim true. Identifiers stay opaque; effective-from is the
   order. (DEF-4, INV-2, REQ-40.)

4. **Every served dataset has a finalized head.** For archival-only datasets it
   equals the archival head (archival chunks hold only finalized blocks); the
   artifact's chunk entries carry their last block's reference so the head has a
   hash. The frontier definition is restated per mode, and head-lag policy (ADR-009)
   caps a request's *frontier*, never its reported heads. (DEF-4, DEF-5, OP-2.)

5. **EMPTY (204) responses carry head metadata; the source marker appears iff a
   source was selected.** The poller sees the frontier it waits on (INV-24 has no
   success exception); an EMPTY produced by routing's no-source branch carries no
   marker, a retention-gap EMPTY (routed to the real-time source) does. (REQ-2/4/5,
   DEF-6, INV-27, IB-4/IB-5.)

6. **A beyond-frontier timestamp is the EMPTY outcome, not NOT-FOUND.** The condition
   resolves itself as the head advances; typing it `invalid_request_error` ("cannot
   succeed unchanged") mislabels it non-retryable and contradicts REQ-13's promise to
   match the stream surface. OP-5 now returns 204 after ≥ P-NO-DATA-DELAY. (REQ-13,
   OP-5, IB-5.)

7. **The clamp-bypassing debug stream variant is operator-gated, disabled by
   default.** Closes OQ-6; INV-11 is again universal over every operator-enabled
   surface. (INV-11, IB-2.)

Alongside these, integrity-exhausted worker attempts surface as WORKER-FAILURE
(`api_error`, pages) — distinguishing "the network serves bad data" from transient
RETRIES-EXHAUSTED — and unmatched proxied 4xx normalize to `malformed_request`/400,
giving DEF-10's `worker_failure` a producer and DC-4 a total mapping.

## Consequences

The wire contract gains three client-visible changes (injected hints on proxied
overloads, 204-with-heads for beyond-frontier timestamps, conflict-over-EMPTY at the
head); all are strict improvements for conforming SDK retry loops. Current master
deviates where it already deviated on the error contract: tracked as GAP-16 (scope
extended), GAP-4 (timestamp 204), and new GAP-19/20/21 (conflict precedence, artifact
regression guard, debug gating). OQ-6 is closed. The reference model and validators
in 13 are updated to match; where model and normative docs disagree in the future,
the docs win.
