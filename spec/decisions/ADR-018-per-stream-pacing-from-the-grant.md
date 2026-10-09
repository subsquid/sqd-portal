# ADR-018 — Per-stream pacing from the grant

Status: Proposed (2026-10-09)

## Context

Portal plans carry an allowance in wire bytes per Portal month (D153) and a per-stream speed
(D129). An organization over its allowance is slowed to a floor speed, never refused (D51).
The control plane already measures what each key was served (REQ-60, ADR-016) and already
answers one exchange per credential (DC-8). What was missing is the half that acts on it, and
NG2 ruled it out: admission decides whether a request is served, never how much it may take.

Three facts shape the answer.

**The Portal must not hold a budget.** A replica sees only its own share of a key's traffic,
and nothing survives a restart (NG5). Counting bytes against an allowance in the pod means
dividing the allowance between replicas and reconciling the shares, which is what #131 did
with a per-pod tally rebased on every snapshot. The control plane has the totals: it
aggregates the usage records into one state per organization, so it can say how fast a key
may go, and say it again on the next exchange.

**A rate rarely changes while a stream is open.** It changes when an organization crosses its
allowance, buys more, or has its terms changed by staff, and a stream sees the change only if
it is still open then. Most streams are not: over seven days on the keyed stacks, 520 of
about 60,000 network streams that would be paced ran past 5.5 minutes. When it does happen,
the damage is small: a stream that crosses finishes at the old speed, and a stream admitted
at the floor stays slow until the client restarts it on a renewed grant.

**zstd network frames are whole worker results,** often many megabytes in one frame. A pacer
that waits per frame either stalls for a minute at the floor or lets a large frame through
unpaced.

## Decision

The control plane decides one effective rate per stream and carries it in the grant. Each
replica paces each response to the rate of the grant it was admitted on, for the response's
whole life, with no state shared between responses or replicas.

1. **Grant v2.** The Portal accepts `claims_version` 1 and 2. Version 2 requires a `usage`
   claim: `state` (`within`, `over`, `unmetered`), `stream_bytes_per_sec`,
   `floor_bytes_per_sec`, `allowance_bytes`, `used_bytes`, `period_end` and `as_of`. The rate
   is null when unpaced, the floor when there is none, the allowance when uncapped, and
   `as_of` when the organization has no period yet; times are unix seconds. The Portal paces
   with `stream_bytes_per_sec` only and copies the rest into headers. It tallies, derives and
   compares nothing. A v2 grant whose `usage` is missing or malformed is unusable like any
   answer this build cannot fully read (DEF-17, REQ-54); a rate of zero counts as malformed,
   since it would stall the stream (D51). A v1 grant is unpaced. Any other version stays an
   exchange failure. The control plane issues v2 per portal, and a portal is switched only
   once it runs a release that reads v2 (ADR-017: Portals upgrade first).

2. **Configuration under `auth.limits`.**

   ```yaml
   auth:
     limits:
       pacing:
         mode: off                 # off | log_only | enforce
         pace_real_time: false
   ```

   Unknown keys under `auth.limits` only warn, so a rollback to a release that predates the
   block still boots (ADR-008; directly under `auth:` an unknown key is fatal). `off` ignores
   `usage` entirely. Pacing acts only where authorization enforces. On a portal whose
   authorization runs in shadow, pacing is inert in every mode: its counters would move only
   for requests that were granted, and so publish the verdict shadow mode withholds from the
   keyless scrape (REQ-55). `pace_real_time` exists because whether real-time data is slowed
   is an open product question; its default leaves real-time data unpaced while it still
   counts toward the allowance.

3. **One wrapper, inside the tap.** After the handler returns, the auth middleware wraps the
   body of a successful response admitted on a v2 grant that has a rate. That is inside the
   usage tap, which still counts what was sent and when; the tap stays measurement-only
   (INV-32, REQ-61). A response naming `real_time` as its source is not wrapped unless
   `pace_real_time` is set. The stream routes stamp `x-sqd-data-source` inside the gate, and
   the routes with no stamp are the direct worker query and the SQL plan, both served by the
   network, so no route has to declare its source to the gate.

   Error responses are not wrapped. They carry no chain data, and two outer layers re-render
   them after the gate: framework rejections, and 5xx envelopes stamped with the request id,
   whose size the client's own `x-request-id` sets. Both keep the headers and extensions the
   gate set; no successful body is rewritten after the gate.

   The wrapper reports its own framing, not the inner body's: end of stream only once the
   inner body has ended and no slice is held, and a size hint that counts the bytes it holds.
   A body of known size, such as the direct worker query's single buffer, keeps an exact hint,
   so the transport and the tap read the same framing as without pacing. Forwarding the inner
   body's end of stream would let the transport drop the slices still held.

4. **Token bucket on encoded bytes, sliced.** The bucket fills at `stream_bytes_per_sec` and
   holds one second of the rate, but never less than 64 KiB, so one slice always fits. Frames
   are split with `Bytes::split_to` (zero-copy) into slices of at most 64 KiB, and a slice is
   released only once the bucket covers it. The bound (burst plus rate times elapsed time)
   therefore holds through the end of the stream, including a single large final frame, and
   the longest pause pacing adds is one slice at the rate. `poll_frame` stays synchronous: a
   response that must wait polls a stored `tokio::time::Sleep`, and a response that does not
   wait has no timer.

5. **Admission fixes everything.** The rate, the read-ahead cap and the headers come from the
   grant the response was admitted on and do not change while it streams. The pacer reads
   nothing after admission: no grant cache lookup, no exchange, no secret (DEF-16 and INV-38
   are unchanged). A changed rate reaches the requests admitted once the replica holds the
   renewed grant. The Portal never ends a response because of usage.

6. **Read-ahead capped at the floor.** The middleware inserts the usage snapshot as a request
   extension before the handler runs. In `enforce`, a stream admitted on a grant whose
   `state` is `over` and whose rate is set gets its `buffer_size` capped at 1: the scheduler
   downloads one chunk ahead. What the encoder and the pacer hold past that is not counted
   against it. The cap is read in `run_stream_internal` and `run_archival_stream`, not in
   `restrict_request`, which the `/debug` variant skips.

7. **`log_only`.** Waits are computed and not taken. Nothing is capped and no header is added,
   so the client sees exactly what `off` serves. The replica counts
   `portal_limit_would_wait_seconds_total` and `portal_limit_would_pace_responses_total`, both
   labelled by `state`. The first would-be wait of a response is logged with `key_id`,
   `organization_id`, the rate and the state. Metrics carry no key or organization.

8. **`enforce`.** Waits are taken and counted in `portal_limit_paced_seconds_total` and
   `portal_limit_paced_responses_total`, labelled by `state`. A response counts as paced once
   it has waited. No new status code exists: a paced response is a 200, and nothing is
   refused for usage.

9. **Headers (D106), in `enforce` only,** on every gated response admitted on a v2 grant:
   `x-sqd-usage-state`, `x-sqd-usage-limit-bytes` (omitted when uncapped),
   `x-sqd-usage-used-bytes`, `x-sqd-usage-reset` (`period_end`, RFC 3339),
   `x-sqd-usage-floor-bytes-per-sec` (omitted when null) and `x-sqd-usage-as-of` (RFC 3339,
   omitted when null). All six join the CORS expose list, or browsers cannot read them.

## Failures

| Failure | Behaviour |
|---|---|
| Control plane down | Held grants keep admitting requests at their rate until `expires_at`, and a response already open keeps its admission rate until it ends, past `expires_at` if it runs that long. An organization that crosses its allowance stays at full speed, and one whose period resets stays at the floor. This fails open on the rate, which is accepted and documented. |
| A v2 grant on a portal with pacing `off`, or with authorization in shadow | Not paced, like v1. |
| A portal on an older release switched to v2 by mistake | The release reads v2 as an unknown claims version. New keys get 502 `upstream_unavailable`; cached keys serve until `expires_at`, up to the grant lifetime (ADR-017), and then every key on that portal is refused. Upgrade first, then switch. |

## Consequences

**Per stream, not per key.** D129 sets speeds per stream, and the Portal holds nothing per
key. A key with N open streams receives N times the rate. Capping concurrent streams (D130)
is not part of v2.

**A new rate reaches later requests, not the open stream.** A stream that crosses its
organization's allowance keeps its admission rate until it ends, and so does a stream
admitted at the floor after its organization buys more. Only requests renew grants, and the
request that finds its grant due is still served on that grant while the renewal runs
(REQ-54). So a client whose long stream is its key's only traffic on a replica gets the new
rate on the request after the one that triggered the renewal. A restarted stream resumes
from the last block + 1, as squid-sdk and pipes-sdk do after any short response.

**How quickly a crossing reaches the client is a timeline, not a bound.** Reporting (interim
records every 30 s), the control plane's aggregation (up to 2 minutes), grant renewal (60 s
once the grant was issued at 90 % of the allowance or more, 5 minutes below that), then the
client's next request admitted on the renewed grant. That is about 3 minutes typically, and about 8 minutes from below
90 % straight to over. D51 tolerates it. Full speed after a purchase or an upgrade comes back
on the same path.

**Pacing makes streams longer.** At the floor more streams are in flight at a given moment,
each holding a connection and its read-ahead longer. The read-ahead cap holds the window at
the floor to one chunk; at plan speed a paced stream holds what a slow client already holds.
A paced stream still open when shutdown's drain budget runs out is truncated like any slow
reader's (REQ-24).

**Keys added by an edge rewrite get headers too.** Where a deployment attaches the key with a
header rewrite at the edge, its clients receive `x-sqd-usage-*` headers for a key they never
sent.

**The review of #131** (kalabukdima, 2026-07-20) raised three objections. This design answers
each by how it is built, not by an added mitigation.
- *CPU.* #131 inflated every response to count logical bytes, on a portal already seen
  burning 30 cores recompressing streams. Nothing here decompresses. The allowance and the
  rate are in encoded bytes, and the bucket counts frame lengths the body already has. The
  added work per frame is a clock read, a subtraction and, above 64 KiB, a zero-copy split,
  and only on responses that have a rate. A timer exists only while a response waits. What
  remains is more frames on zstd responses, whose frames are whole worker results, and the
  per-frame work of the wrapper itself. HZ-16 makes these a budget CT-6 measures, not a cost
  claimed to be zero.
- *Prefetch pulling data a client never receives.* #131 cut streams mid-flight when a key's
  quota ran out, so everything read ahead past the cut had been downloaded for nothing, at a
  depth the client chose through `buffer_size`. Here the Portal ends no stream because of
  usage (D51), and a stream admitted at the floor downloads one chunk ahead.
- *Why not a proxy in front.* A proxy can slow the bytes a client receives, which is the easy
  half. It cannot cap the read-ahead, which lives in the stream scheduler. And it holds no
  grant, so it would need its own exchange and the secret on a second hop. Enforcement lives
  in the pod for these reasons.

## Spec changes

- NG2 now states that per-key limits exist, are decided by the control plane and carried in
  the grant, and that each response is paced alone with no state shared between replicas.
- DEF-17 gains the v2 usage claim. DEF-16 and INV-38 are unchanged: the pacer reads nothing
  after admission.
- REQ-70..REQ-74 (usage pacing), INV-16 (pacing bound), INV-17 (pacing changes timing only),
  OB-16, IB-10 (usage headers), HZ-16 and CT-12 are new.
- LIV-13 states that a lowered rate reaches new admissions only. INV-11, LIV-2, IB-1, OP-11,
  SLI-2 and the DC-8 fault table in 09 gain one clause each.
- The tap (INV-32, REQ-61) stays measurement-only; pacing is a separate band so that
  "measurement is not metering" stays true.

## Alternatives rejected

**A byte budget in the pod.** #131 divided a key's remaining bytes by the replica count and
debited a local tally. That turns every replica into a party to a distributed counter that has
to be rebased after restarts, rollbacks and replica-count changes, and a request that started
within budget could still overspend by one response. The control plane already computes the
state centrally; the Portal only needs the rate it implies.

**Moving open streams to a new rate.** Three ways were worked out: reading a renewed grant
from the cache once a second; ending a paced stream once its grant went stale, at a chunk
boundary through a stop flag so gzip still writes its trailer; and ending every stream after
a fixed time. Each brought its own edge cases: rate changes inside the bucket, a loop while
the control plane is down, streams whose renewal was denied, replicas that disagree. All of
that served about 1 % of streams, for a damage measured in minutes of the old speed.
Renewing from the pacer was never an option: it needs the credential for the life of the
stream, which INV-38 forbids.

**Pacing per frame, without slicing.** At the floor a multi-megabyte zstd frame is a silence
of a minute or more, followed by a burst; a response that is one frame escapes the bound.

**A per-key rate shared by a replica's streams.** Closer to a per-key limit, but only per
replica: a key spread over replicas gets the rate once on each, so the limit it promises does
not hold. D129 chose per-stream speeds, which hold exactly.

**Refusing at the limit.** D51 rules it out: an over-limit organization is slowed, never
refused, so no status code is added.
