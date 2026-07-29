# ADR-016 — The publisher owns assignment selection; the Portal signals staleness instead of overriding it

Status: Accepted (2026-07-28); supersedes ADR-014 decision 3; absorbs ADR-013, which was
never ratified standalone.

## Context

Two questions about the assignment refresh path were open, and they turned out to be the
same question.

**Which artifact wins.** ADR-014 decision 3 treated an assignment's effective-from
timestamp as a revision and required artifact application to be monotone in that value.
That interpretation was not part of the scheduler/publisher contract: effective-from says
when an artifact may be activated, not whether it supersedes another artifact. The
monotonic guard also prevented recovery — if the publisher selected a previously valid
artifact to roll back a bad assignment, its earlier effective-from caused the Portal to
reject the publisher's current state. This moved scheduler policy into the Portal and made
an intentional rollback indistinguishable from stale publication.

**What happens when refresh stops.** A fetch failure only logged and waited for the next
tick. Nothing bounded how old the applied artifact could grow, no age signal existed in
metrics, and readiness ignored it: a wedged publisher left the Portal reporting ready and
serving progressively wronger routing — missing new chunks, departed workers —
indefinitely and invisibly. This contradicted the truthful-readiness goal (REQ-23) and was
registered as GAP-2 (P1). Forced by this spec: the behavior could not be written as a MUST
without a staleness bound.

Both are the same trade. The Portal had been compensating for uncertainty about the
publisher with local policy, and the policy was wrong in both directions — too strict
about which artifact to accept, too silent about having no fresh one at all.

## Decision

**The publisher's currently selected assignment identifier is authoritative.** Identifiers
are opaque and compared only for equality, to avoid re-downloading the already applied
artifact. A different identifier is fetched, validated, and atomically applied even when
its effective-from predates the applied artifact's. Fetch or validation failure leaves the
applied artifact untouched.

**Effective-from is deprecated.** It is not an ordering key and never was a revision. The
Portal still honours it as an activation delay, so a scheduled cutover lands together
across the fleet, but the field is on its way out of the publisher contract and this spec
deliberately does not pin its semantics down further. New behavior must not depend on it.

**Staleness is bounded and observable; readiness is not.** The applied artifact's age is
exposed as a metric and refresh failures alarm. Past P-ASSIGNMENT-MAX-AGE (15 min) the
artifact is reported stale through a distinct signal an operator can alert on —
`assignment_stale`, alongside `assignment_age_seconds` and reason-coded
`assignment_refreshes`. `/ready` stays green on a stale artifact. The bound changes
*signaling*, never serving: the Portal keeps routing from the artifact it has, because
stale routing beats no routing.

## Consequences

The publisher can roll routing back to a previously valid assignment, and a dead publisher
becomes a paged, visible failure instead of silent drift. The Portal no longer reports or
rejects a `regressive` refresh, because no such ordering exists in its contract.

This removes Portal-side protection against an accidentally republished old identifier. If
the scheduler needs replay protection, its contract must provide an explicit monotone
revision or another signal that distinguishes an accident from an intentional rollback;
effective-from cannot encode both meanings, which is part of why it is being retired.

Readiness was left alone deliberately, and the alternative is what makes the case: a
`/ready` that fails on age takes every replica out simultaneously during publisher
maintenance, turning a routing-freshness problem into a total outage — while each of those
replicas could still serve every request correctly from the artifact it holds. The
operator gets the signal; the orchestrator does not get a reason to rotate. This also means
age alone can never page as an availability incident, so the alert on `assignment_stale`
has to be routed as a real alarm rather than inferred from readiness.

Revisiting the readiness half — a Portal that refuses traffic on stale routing — needs a
new ADR, not a config flag. Fixes P-ASSIGNMENT-MAX-AGE at 15 min in the registry and closes
OQ-3.

DEF-4, INV-2, INV-31, DC-2, REQ-23, REQ-40, and the publisher failure model are updated
accordingly.
