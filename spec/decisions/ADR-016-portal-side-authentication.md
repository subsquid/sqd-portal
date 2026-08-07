# ADR-016 — The Portal authenticates requests itself

Status: Proposed (2026-08-05, revised 2026-08-07)

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

**Revised 2026-08-07.** The first draft had the Portal mirror the control plane's entire
key set into memory — bootstrap from a cursor-paged feed, tail ordered deltas, rebuild on
epoch change — and answer every request from that mirror. The mechanism below replaces it
with an on-demand exchange. The position is unchanged: the Portal still decides, still
only where an operator asked for it, still fail-closed on the credential. Only the way it
learns the answer changed. §Why not the mirror records what that trade bought and cost.

## Decision

The Portal authenticates and coarsely authorizes each request itself, when — and only
when — the operator configures it to.

1. **Opt-in, and absent by default.** Without commercial configuration the Portal has no
   gate, no control-plane dependency, and no authorization middleware: byte-for-byte the
   behavior of a self-hosted build (REQ-56). Configuration present but empty is a
   startup error, never an open portal.
2. **Ask, then cache.** The Portal holds no key set. The first request presenting a given
   credential exchanges it at the control plane (DC-8) for a short-lived **grant**: an
   authorization answer carrying explicit claims and the lifetimes the control plane chose
   for them. The grant is cached under a fingerprint of the *whole* credential — never
   under the key id, or the next caller to name that id would be admitted without proving
   it holds the secret — and answers every later request locally until it needs refreshing.
3. **Two deadlines, both the control plane's.** A grant carries a soft `refresh_after` and
   a hard `expires_at`. Past the soft one the Portal re-exchanges while still serving on
   the grant it has; past the hard one it must not serve on that grant at all. An
   authoritative denial replaces a cached grant the moment it arrives, at any point in the
   grant's life. The split is what keeps a refresh off the request's latency path while
   still bounding how long a stale answer can be acted on — one deadline can do one or the
   other, not both.
4. **Fail closed on the credential, retryable on the dependency.** A credential the Portal
   cannot positively establish as authorized is refused. But an exchange that could not run
   or could not answer is not a claim about the credential: without a usable grant, budget
   exhaustion is refused as overload and a failed call as upstream unavailability, both
   retryable, and neither is mislabeled as a bad credential. A renewal denied by the local
   budget does not shorten a grant that is still usable; that request remains inside the
   explicit grace the control plane issued. The Portal never invents a lifetime; it only
   caps one (P-GRANT-MAX-LIFETIME), so a control-plane misconfiguration cannot hand the
   fleet a month-long authorization.
5. **Enforcement is a mode, not a deploy.** `log_only` attempts the whole ladder, records
   the verdict it would have returned or the exchange outcome that prevented one, and
   admits regardless (REQ-55) — the cutover is a config change with a measured blast
   radius, not a leap.
6. **One gated surface.** Streams, queries and block lookups need a key; the catalog,
   heads, heights, probes and the API schema do not, on every deployment (REQ-51).
   Gating the catalog as a second scope was considered and rejected: it makes every
   route's classification a decision (NG8). Each route states which set it is in
   where it is declared, so there is no default to forget. The keyless metrics
   representation stays constrained regardless: no internal auth rung or exchange detail
   beyond the public wire outcome (OB-12/13).

**NG1 is retired** and replaced by a narrower non-goal: the Portal still performs no
per-client quota, metering, or rate limiting (NG2 unchanged) — an admitted key streams
unrestricted. Those are later phases and are deliberately out of this decision.

## Why not the mirror

Both designs put the decision in the Portal. They differ in what each replica has to hold
and what it does when the control plane is gone.

The mirror's one real advantage is warm-outage availability: a replica that finished
bootstrapping needs nobody to serve a key it already has. That is not free, and the price
is not the one it looks like. Holding the last key set through an outage also holds every
revocation and entitlement reduction issued during it, for as long as the outage lasts.
"Keep the last snapshot" is an availability policy and an *unbounded stale-authorization*
policy in the same sentence, and exporting the age (which the first draft did) reveals the
condition without bounding it. The exchange makes that window a number the control plane
sets and the Portal caps.

The rest of the comparison follows the working set. A mirror is sized by the key set;
an exchange cache is sized by the keys actually presenting themselves at that replica. The
mirror multiplies the whole authorization corpus — verifier material included — by the
replica count, puts a full scan on every cold start and every epoch rebuild, and grows
without a bound the Portal owns. It also carries a distributed-state protocol the Portal
would have to keep correct forever: cursors, page ordering, forward-only application,
tombstones, epoch changes, and the bootstrap-capacity question underneath all of it.

What the exchange gives up is precisely the warm-outage guarantee. It is a harder
availability dependency: a control plane that is down cannot mint a grant, so a credential
this replica has not seen recently is refused — retryably, and visibly as a dependency
failure rather than as a bad key, but refused. Cached traffic rides the outage out to
`expires_at` and then stops. That is the trade, taken deliberately: a bounded, explicit
stale-authorization window and a working-set-sized replica, against a dependency the
deployment must run like a production service. There is no third option — no offline
authorization design gives immediate revocation and unlimited operation without its
authority at the same time.

The mechanism this rejects would remain a reasonable fallback if the total key set were
predictably small and capped, nearly all of it active, and serving every known key through
a prolonged control-plane outage were a hard requirement. None of those hold today.

## Consequences

The Portal gains a dependency it can be down without only for as long as the grants it
already holds live (DC-8). Two things the first draft owed the reader disappear with the
mirror: there is no bootstrap, so an enforcing Portal is ready without ever having reached
the control plane (INV-31 loses its key conjunct and LIV-5 its bootstrap term), and there
is no fleet-wide snapshot age to alarm on. Startup gets simpler and strictly faster.

Revocation convergence becomes a bound rather than a hope: `refresh_after` plus one
exchange while the control plane is healthy, and `expires_at` regardless of its health
(LIV-13). Nothing needs a kill-list.

The cost lands on the request path, and it is the thing to watch. Every credential this
replica has not cached costs a control-plane call, and the credentials an attacker
controls are exactly the uncached ones — so the exchange budget is both the Portal's
protection and, when a flood drains it, the reason a legitimate new key is turned away
(HZ-10). The first draft could fall back on a mirror; this one cannot. Bounded caches,
single-flight per fingerprint, negative caching and fail-closed miss budgets are therefore
not hardening to add later — they are what makes the gate safe to expose at all.

Rejection reasons become a public contract surface, which is what forces ADR-017: the
ADR-011 vocabulary has no way to say "your credential is the problem" and no status
outside the 400/404/409/5xx set.

How the Portal proves its own identity to the control plane is ADR-018.

Shapes REQ-50..REQ-56; adds DC-8; retires NG1.
