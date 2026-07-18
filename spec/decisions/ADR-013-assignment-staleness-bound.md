# ADR-013 — Assignment staleness must be bounded and observable

Status: Proposed (2026-07-17)

## Context

The Portal refreshes its routing artifact every P-ASSIGNMENT-REFRESH, but a fetch
failure only logs and waits for the next tick. There is no bound on how old the applied
artifact may grow, no age signal in metrics, and readiness ignores it: a wedged
publisher leaves the Portal reporting ready and serving progressively wronger routing
(missing new chunks, departed workers) indefinitely and invisibly. This contradicts the
truthful-readiness goal (REQ-23) and is registered as GAP-2 (P1). Forced by this spec:
current behavior cannot be written as a MUST without a staleness bound.

## Decision (proposed)

Expose the applied artifact's age as a metric and alarm on refresh failures. When age
exceeds P-ASSIGNMENT-MAX-AGE (proposed 15 min ⚠), degrade readiness — at minimum a
distinct degraded signal an operator can alert on; optionally full not-ready so
orchestrators rotate the instance. Continue serving from the stale artifact throughout
(availability over freshness); the bound changes *signaling*, not serving.

## Consequences

A dead publisher becomes a paged, visible failure instead of silent drift. Risks to
weigh at ratification: full not-ready across a fleet during publisher maintenance would
take every replica out simultaneously — the bound must be generous and the degraded
(non-rotating) variant may be the right first step. Ratifying this ADR fixes
P-ASSIGNMENT-MAX-AGE in the registry and closes OQ-3. Shapes REQ-23, REQ-40.
