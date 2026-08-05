# ADR-016 — The Portal authenticates requests itself

Status: Proposed (2026-08-05)

## Context

NG1 declared per-request authentication a non-goal on the reasoning that the Portal
"runs behind a trusted perimeter". Perimeter authentication cannot carry a commercial
access decision, for two reasons the suite never wrote down:

- It is not part of this system. Nothing in the Portal's own tests, metrics, or spec
  constrains it, so whatever guarantee it provides is unspecified and untested here.
- It cannot express what commercial access actually needs. A perimeter sees a request;
  it does not know that *this* key is revoked, expired, bought only `ethereum-mainnet`,
  or belongs on a different portal.

Meanwhile the control plane already mints keys and knows their state. The missing half
is a data plane that can decide, per request, whether to serve.

## Decision

The Portal authenticates and coarsely authorizes each request itself, when — and only
when — the operator configures it to.

1. **Opt-in, and absent by default.** Without commercial configuration the Portal has no
   gate, no control-plane dependency, and no authorization middleware: byte-for-byte the
   behavior of a self-hosted build (REQ-56). Configuration present but empty is a
   startup error, never an open portal.
2. **Mirror, don't ask.** The Portal tails the control plane's key feed into an
   in-memory snapshot (DC-8) and verifies presented secrets locally against the published
   digest. The request path makes no control-plane call except the bounded
   authorize-on-miss lookup that lets a key minted seconds ago work before the next sync.
3. **Fail-closed on the key, fail-static on the feed.** An unknown key is refused. A
   control-plane outage does not invalidate a snapshot hit: the last good snapshot keeps
   answering (REQ-54). A miss that cannot be resolved is refused retryably as overload
   when its local lookup budget is exhausted, or as upstream unavailability when the call
   fails — never mislabeled as an invalid credential. The two paths differ because
   refusing every snapshot hit during a control-plane blip is a worse outage than briefly
   honoring a key that was just revoked.
4. **Enforcement is a mode, not a deploy.** `log_only` attempts the whole ladder, records
   the verdict it would have returned or an indeterminate lookup that prevented one, and
   admits regardless (REQ-55) — the cutover is a config change with a measured blast
   radius, not a leap.
5. **One gated surface.** Streams, queries and block lookups need a key; the catalog,
   heads, heights, probes and the API schema do not, on every deployment (REQ-51).
   Gating the catalog as a second scope was considered and rejected: it makes every
   route's classification a decision (NG6). Each route states which set it is in
   where it is declared, so there is no default to forget. The keyless metrics
   representation stays constrained regardless: no internal auth rung or lookup detail
   beyond the public wire outcome (OB-12/13).

**NG1 is retired** and replaced by a narrower non-goal: the Portal still performs no
per-client quota, metering, or rate limiting (NG2 unchanged) — an admitted key streams
unrestricted. Those are later phases and are deliberately out of this decision.

## Consequences

The Portal gains a dependency it can be down without (DC-8) and a readiness precondition
it cannot serve without: an *enforcing* portal that has never mirrored the key set knows
no keys, so it would answer 401 to every valid one, and must stay out of rotation until
the first sync lands (INV-31). A shadow-mode portal has no such precondition, since it
admits regardless.

Rejection reasons become a public contract surface, which is what forces ADR-017: the
ADR-011 vocabulary has no way to say "your credential is the problem" and no status
outside the 400/404/409/5xx set.

Two properties now need bounds the suite did not previously owe anyone: how long a
revocation may take to reach a replica (LIV-13) and how stale a snapshot may get before
the deployment should stop trusting it (P-KEY-SNAPSHOT-MAX-AGE ⚠, OQ-12). Age is
exported and alarmable; what the Portal should *do* once it is exceeded is the open half.
Both are the key-set analogue of the assignment-staleness question ADR-013 leaves open,
and should be ratified together.

Shapes REQ-50..REQ-56; adds DC-8; retires NG1.
