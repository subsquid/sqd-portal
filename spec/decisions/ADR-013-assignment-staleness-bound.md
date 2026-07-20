# ADR-013 — Assignment staleness must be bounded and observable

Status: Accepted (2026-07-20), implemented in #138.

## Context

The Portal refreshes its routing artifact every P-ASSIGNMENT-REFRESH, but a fetch
failure only logs and waits for the next tick. There is no bound on how old the applied
artifact may grow, no age signal in metrics, and readiness ignores it: a wedged
publisher leaves the Portal reporting ready and serving progressively wronger routing
(missing new chunks, departed workers) indefinitely and invisibly. This contradicts the
truthful-readiness goal (REQ-23) and is registered as GAP-2 (P1). Forced by this spec:
current behavior cannot be written as a MUST without a staleness bound.

## Decision

Expose the applied artifact's age as a metric and alarm on refresh failures. Past
P-ASSIGNMENT-MAX-AGE (15 min) the artifact is reported stale through a distinct signal
an operator can alert on — `assignment_stale`, alongside `assignment_age_seconds` and
reason-coded `assignment_refreshes`.

**Readiness does not degrade.** `/ready` stays green on a stale artifact. The bound
changes *signaling*, never serving: the portal keeps routing from the artifact it has,
because stale routing beats no routing.

## Consequences

A dead publisher becomes a paged, visible failure instead of silent drift.

Readiness was left alone deliberately, and the alternative is what makes the case: a
`/ready` that fails on age takes every replica out simultaneously during publisher
maintenance, turning a routing-freshness problem into a total outage — while each of
those replicas could still serve every request correctly from the artifact it holds.
The operator gets the signal; the orchestrator does not get a reason to rotate. This
also means age alone can never page as an availability incident, so the alert on
`assignment_stale` has to be routed as a real alarm rather than inferred from
readiness.

Revisiting this — a portal that refuses traffic on stale routing — needs a new ADR, not
a config flag. Fixes P-ASSIGNMENT-MAX-AGE at 15 min in the registry and closes OQ-3.
Shapes REQ-23, REQ-40.
