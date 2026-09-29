# ADR-017 — The outage runway is the grant lifetime; renewals get their own budget

Status: Accepted (2026-09-29)

## Context

A grant carries two deadlines: `refresh_after`, from which the Portal renews it in the
background while still serving on it, and `expires_at`, past which it admits nothing
(REQ-54). The control plane issued them five and fifteen minutes out, so a control-plane
outage longer than fifteen minutes refused every key on every enforcing Portal at once. For
single-tenant deployments with an availability commitment that is a breach unrelated to the
data plane they pay for.

The first attempt kept a grant serving past `expires_at` while the control plane was shown
to be down. It went through five review rounds and each found a new hole in deciding "shown
to be down" from local, noisy, partly attacker-influenced signals: overlapping completions,
a spent local budget, answers the Portal cannot read, one key's failure speaking for others,
and budget refusals erasing real evidence. Tightening the classifier turned real outages on
busy replicas back into lockouts; loosening it let a flood keep a revoked key working. It
did not converge.

The two deadlines already express what that attempt was reaching for. `refresh_after`
decides how quickly a change at the control plane reaches a healthy Portal; `expires_at`
decides how long a Portal keeps serving when it cannot ask. Only the second was too short.

## Decision

1. **The runway is the lifetime, and the control plane sets it per deployment.**
   `refresh_after` stays short. `expires_at` is issued per Portal, long enough to ride the
   outages the deployment must survive. Most Portals are single-tenant, so 24 h is the
   default on both sides: the control plane issues it to any Portal it does not name, and
   the Portal's cap (P-GRANT-MAX-LIFETIME) defaults to it. A shared Portal is named with a
   shorter lifetime — 1 h as a starting point — in the one place that issues it. Nothing
   in the Portal decides whether the control plane is down.
2. **Renewals spend their own budget.** Renewing a held grant uses P-GRANT-RENEWAL-RATE and
   P-GRANT-RENEWAL-INFLIGHT, never the admission budget unknown tokens drain (HZ-10). Only a
   credential the control plane once granted can reach it. Without this, a flood of junk
   tokens starves renewals and a revoked key keeps serving for the rest of its grant — which
   was ten minutes with a fifteen-minute lifetime and becomes hours with a long one.

## Consequences

While the control plane answers nothing changes: a key in use converges on revocation
within `refresh_after` + one exchange (LIV-13), and a denial evicts at once (INV-6).

The worst case is explicit: a revoked key whose renewals keep failing serves until its
`expires_at`, at most P-GRANT-MAX-LIFETIME for that deployment. A shared Portal the control
plane is not told about gets the single-tenant day; naming it is part of turning its
enforcement on. An idle key that returns
gets one request served on its held grant before the renewal's denial lands, as before.

An answer the Portal cannot read — notably a claims version it does not understand —
fails the renewal and the old grant keeps serving to its expiry, now hours rather than
minutes. A restriction carried only by a new claims version therefore reaches a Portal no
sooner than that Portal is upgraded: Portals upgrade before the control plane issues a new
claims version.

A replica restarted during an outage still holds nothing (NG5). Persisting grants, or
pinning a single-tenant Portal's credentials in its configuration, are the follow-ups that
would close that; neither is decided here.
