# ADR-017 — A control-plane outage extends a grant past its hard expiry, for a stated time

Status: Proposed (2026-09-28)

## Context

Authorization is an exchange: the first request presenting a credential trades it at the
control plane for a grant with two deadlines, `refresh_after` (renew in the background from
here) and `expires_at` (stop here). The control plane issues them five and fifteen minutes
out. REQ-54 as it landed with the authorization band made `expires_at` absolute: a grant
past it admitted nothing, whatever the control plane's state.

That rule was the closing position of a design argument, not its resolution. The alternative
on the table in August 2026 was a key snapshot the control plane publishes and every
replica mirrors, which is fail-open by construction — a replica keeps admitting on the last
snapshot it saw for as long as the control plane is gone. The exchange model was chosen
for bounded memory, no bootstrap, and one less distributed state to repair at 2 a.m., on the
stated assumption that control-plane interruptions would be short: the Cloud's availability
had been about 99.9% over two years, and a soft deadline with a hard expiry would cover a
blip. The comparison that closed the discussion conceded the one scenario the snapshot
wins outright — a warm Portal through a prolonged outage — and deferred it to "informed
optimizations later if the actual numbers show they're needed".

The number that matters is not traffic; it is the contract. Single-tenant Portals carry an
availability commitment to one customer, and under the absolute rule a control-plane
outage longer than fifteen minutes takes every one of them down at once, for a reason that
has nothing to do with the data plane they pay for. 99.9% is about nine hours a year; the
part of it that arrives in incidents longer than a grant lifetime is a breach for every
enterprise client simultaneously. The exposure is worst exactly where the dependency is
least justified: a single-tenant Portal holds a handful of keys that change perhaps monthly,
and what it needs from the control plane is a yes about each of them.

The security cost of the alternative is small and was conceded by the same argument that
chose the hard expiry. Portal keys are read-only; the control plane stores them in plaintext
and shows them in a console because they are "not particularly sensitive". Serving a
revoked read-only key for the length of an outage, against public chain data, costs almost
nothing; refusing every paying key for the length of an outage costs the commitment. And
the cost cannot be paid later: the knob that would lengthen the grace lives in the replica,
and rolling it out mid-incident restarts the pods and discards the very grants the rollout
was meant to keep.

Two properties of the ratified design are worth keeping. A denial must still land the
moment the control plane answers — the outage must not become a way to un-revoke a key
once the authority is back. And an expired grant must not become a licence to skip the
authority while it is healthy: a key idle across its expiry on a healthy control plane
should be exchanged, not served on old news.

## Decision

Past `expires_at`, a held grant keeps serving **only while the control plane is failing to
answer**, for at most P-GRANT-OUTAGE-GRACE beyond the hard expiry, and no further.

1. **Silence is the condition, and it has to be witnessed.** A grant past `expires_at`
   answers a request without waiting only if this replica has seen an exchange fail to
   answer — unreachable, timed out, an error status, an unreadable answer — at or after
   the grant's expiry, with no answer from the control plane since that failure. A denial
   is not silence; a budget refusal is not silence. Without that evidence the request
   exchanges first, and is served on the held grant only if that exchange cannot run or
   answer either. The first request past an expiry therefore pays one exchange's worth of
   latency once per outage per credential; every request after it is served at once while
   the renewal runs beside it.
2. **The authority's next word outranks the grant.** The refresh keeps running through the
   outage grace under the same cooldown as renewal grace. Its first answer wins: a grant
   replaces the stale one, a denial evicts it. A key revoked during an outage converges on
   the control plane's first answer after it (LIV-13).
3. **The grace is bounded and the operator owns the bound.** P-GRANT-OUTAGE-GRACE is a
   deployment parameter with a default of 24 hours; zero restores the hard expiry as the
   end. It is long by default because it cannot be raised during the incident it exists
   for. Whether a shared Portal should run a shorter one than a single-tenant one is
   OQ-17.
4. **Stale is its own signal.** Admissions past the hard expiry are counted apart from
   renewal grace, as is the number of grants in that state; the minimum-remaining gauge
   now names the outage-grace cliff. Renewal grace happens on every healthy refresh; stale
   admission never happens while the control plane is healthy, so any rate at all is an
   outage in progress and pages (OB-9, OB-13).

Nothing here changes the direction of failure on the credential itself, readiness (INV-31),
what survives a restart (NG5), or the lifetime cap on what the control plane may offer.

## Consequences

The worst-case stale-authorization window during an outage is P-GRANT-MAX-LIFETIME +
P-GRANT-OUTAGE-GRACE rather than P-GRANT-MAX-LIFETIME. While the control plane answers,
nothing changes: renewal converges at `refresh_after`, a denial lands at once, and a grant
past `expires_at` is exchanged before it is served.

A replica restarted during an outage still holds nothing and refuses everything retryably;
the grace protects warm replicas only. Persisting grants across restarts, or pinning a
single-tenant Portal's credentials in its configuration, are the two follow-ups that would
close that, and neither is decided here.

The evidence rule is per replica. Two replicas can disagree about whether the authority is
silent for one exchange's worth of time; INV-15's divergence bound absorbs that.

A Portal-local fault that fails every exchange — a rotated signing key, a clock skewed past
P-SIGNATURE-MAX-SKEW — reads as silence and is served through on the same grace. That is
the intended direction: the alternative is a lockout caused by the Portal's own
configuration, and the alarm on sustained exchange failure is what distinguishes the two.
