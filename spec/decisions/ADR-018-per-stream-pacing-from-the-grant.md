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

**Most streams are short; a few are not.** A rate changes when an organization crosses its
allowance, buys more, or has its terms changed by staff. A short stream picks up the change
on its own: the client's next request is admitted on the current grant. Over seven days on
the keyed stacks, 520 of about 60,000 network streams that would be paced ran past 5.5
minutes. Those few would keep their admission rate for hours, at full speed past the
allowance or at the floor after a purchase. Only requests renew grants, and the pacer cannot
start one: renewal needs the credential, which the stream no longer holds (DEF-16, INV-38).

**A response cannot be cut short at an arbitrary byte.** A gzip response is one member for the
whole body, from the recompressor and from gzjoin alike, so a body cut before the trailer
fails to decode, and squid-sdk throws on it instead of resuming.

**zstd network frames are whole worker results,** often many megabytes in one frame. A pacer
that waits per frame either stalls for a minute at the floor or lets a large frame through
unpaced.

## Decision

The control plane decides one effective rate per stream and carries it in the grant. Each
replica paces each response to the rate of the grant it was admitted on, for the response's
whole life, with no state shared between responses or replicas. A paced network stream ends
after a fixed age, so its client comes back on a current grant.

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
   block still boots (ADR-008; directly under `auth:` an unknown key is fatal). `off` validates
   `usage` and does not act on it. Pacing acts only where authorization enforces. On a portal whose
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
   renewed grant.

6. **Paced streams start no chunk after P-PACED-STREAM-MAX-AGE, 5 minutes.** In `enforce`, the
   middleware inserts a deadline, admission time plus 5 minutes, as a request extension on a
   request admitted on a grant with a rate. The network chunk stream reads it the way it reads
   the operator's chunk cap (REQ-8), on every poll: past the deadline it starts no chunk after
   its first, sends the ones already started, and ends. gzip writes its trailer and zstd stops
   after a whole frame, and nothing downloaded is discarded. The first chunk is exempt, so a
   stream whose request body arrived late still serves something; it never ends empty, which
   both clients would read as "no data for this range", and the deadline never turns a
   stream into a 204. The deadline bounds when the last chunk starts, not when the response
   ends: the chunks in flight still drain at the paced rate, which at the floor can take
   minutes for one large worker result. Both
   squid-sdk (`master` @ `4c86209`) and pipes-sdk (`main` @ `b1d46a6`) request again from the
   last block + 1 after a non-empty response ends, as after any short response. Routes with no
   chunk stream are never ended: the real-time proxy, the direct worker query, the SQL plan
   and the timestamp lookup. Neither are unpaced responses, Enterprise among them.

   Nothing re-reads the grant. The resumed request is admitted on whatever grant its replica
   holds by then, and if that grant is due its admission starts the renewal. A changed rate
   reaches the stream at the first resumption admitted by a replica that has renewed since
   the change. With the 5-minute refresh interval that is typically the second resumption on
   the same replica; replicas renew independently and only when a request finds the grant
   due, so no count is promised. The limit
   is a plain age, not a check for a stale grant. Without `peek`, a resumption is admitted on
   the held due grant while the renewal runs, so a staleness rule would either never end it
   again, leaving it at the old rate for life, or end it in a loop. An age has neither
   problem. While the control plane is down it costs at most one reconnect per long paced
   stream every 5 minutes, the cost agreed with EF on 2026-10-09.

7. **Read-ahead capped at the floor.** The middleware inserts the usage snapshot as a request
   extension before the handler runs. In `enforce`, a stream admitted on a grant whose
   `state` is `over` and whose rate is set gets its `buffer_size` capped at 1: the scheduler
   downloads one chunk ahead. What the encoder and the pacer hold past that is not counted
   against it. The cap is read in `run_stream_internal` and `run_archival_stream`, not in
   `restrict_request`, which the `/debug` variant skips.

8. **`log_only`.** Waits are computed and not taken. Nothing is ended, nothing is capped and
   no header is added, so the client sees exactly what `off` serves. The replica counts
   `portal_limit_would_wait_seconds_total` and `portal_limit_would_pace_responses_total`, both
   labelled by `state`. The first would-be wait of a response is logged with `key_id`,
   `organization_id`, the rate and the state. Metrics carry no key or organization.

9. **`enforce`.** Waits are taken and counted in `portal_limit_paced_seconds_total` and
   `portal_limit_paced_responses_total`, and streams ended by the age limit in
   `portal_limit_age_ends_total`, all labelled by `state`. A response counts as paced once
   it has waited. No new status code exists: a paced response is a 200, and nothing is
   refused for usage.

10. **Headers (D106), in `enforce` only,** on every gated response admitted on a v2 grant:
    `x-sqd-usage-state`, `x-sqd-usage-limit-bytes` (omitted when uncapped),
    `x-sqd-usage-used-bytes`, `x-sqd-usage-reset` (`period_end`, RFC 3339),
    `x-sqd-usage-floor-bytes-per-sec` (omitted when null) and `x-sqd-usage-as-of` (RFC 3339,
    omitted when null). All six join the CORS expose list, or browsers cannot read them.

## Failures

| Failure | Behaviour |
|---|---|
| Control plane down | Held grants keep admitting requests at their rate until `expires_at`. An organization that crosses its allowance stays at full speed, and one whose period resets stays at the floor. This fails open on the rate, which is accepted and documented. Long paced streams still reach their age limit and resume, at most once per 5 minutes, with no loop. A resumption is admitted only where its replica holds a usable grant; one that lands on a replica that does not is refused as `upstream_unavailable` and retried by the client. |
| A v2 grant on a portal with pacing `off`, or with authorization in shadow | Not paced, like v1. |
| A portal on an older release switched to v2 by mistake | The release reads v2 as an unknown claims version. New keys get 502 `upstream_unavailable`; cached keys serve until `expires_at`, up to the grant lifetime (ADR-017), and then every key on that portal is refused. Upgrade first, then switch. |

## Consequences

**Per stream, not per key.** D129 sets speeds per stream, and the Portal holds nothing per
key. A key with N open streams receives N times the rate. Capping concurrent streams (D130)
is not part of v2.

**A new rate reaches an open stream only through its end.** A stream keeps its admission
rate until it ends. A paced network stream starts no chunk after 5 minutes and ends once the
chunks in flight are sent; its resumption is admitted on the grant its replica holds and, if
that grant is due, starts its renewal while still served on it (REQ-54). So a changed rate
reaches a long stream at the first resumption admitted by a replica that has renewed since,
typically the second on one replica. An unpaced stream
is never ended, so a stream admitted without a rate keeps running unpaced, even after its
organization crosses into a paced state, until the client's next request.

**The age limit also lands revocations on paced streams.** A revoked key's paced stream
starts no chunk after 5 minutes, and its resumption is refused once the replica has learned
the denial, which the resumption's own renewal can bring. Unpaced streams are never ended, as before.

**How quickly a crossing reaches the client is a timeline, not a bound.** Reporting (interim
records every 30 s), the control plane's aggregation (up to 2 minutes), grant renewal (60 s
once the grant was issued at 90 % of the allowance or more, 5 minutes below that), then the
client's next request admitted on the renewed grant. That is about 3 minutes typically, and
about 8 minutes from below 90 % straight to over, plus one or two age limits and their drain
for a long stream. D51 tolerates it. Full speed after a purchase or an upgrade comes back
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
  depth the client chose through `buffer_size`. Nothing here is cut for usage (D51). The one
  end the Portal initiates, the age limit, starts no new chunk and sends the ones already
  started, so nothing downloaded is discarded. A stream admitted at the floor downloads one
  chunk ahead.
- *Why not a proxy in front.* A proxy can slow the bytes a client receives, which is the easy
  half. It cannot cap the read-ahead, which lives in the stream scheduler. It cannot end a
  stream where the gzip member can still be closed: the encoder runs in the pod, and a proxy
  sees only the encoded body. And it holds no grant, so it would need its own exchange and
  the secret on a second hop. Enforcement lives
  in the pod for these reasons.

## Spec changes

- NG2 now states that per-key limits exist, are decided by the control plane and carried in
  the grant, and that each response is paced alone with no state shared between replicas.
- DEF-17 gains the v2 usage claim. DEF-16 and INV-38 are unchanged: the pacer reads nothing
  after admission.
- REQ-70..REQ-75 (usage pacing), INV-16 (pacing bound), INV-17 (pacing changes timing and,
  through the age limit, extent only), OB-16, IB-10 (usage headers), HZ-16 and CT-12 are new.
- INV-25: an end the Portal decides — the operator's chunk cap, the age limit — stops the
  record sequence before encoding, so the body is a complete encoding. LIV-13: a lowered rate
  reaches a paced stream through its age limit. INV-11, LIV-2, IB-1, OP-1, OP-11, SLI-2,
  SLI-6 and the DC-8 fault table in 09 gain one clause each.
- The tap (INV-32, REQ-61) stays measurement-only; pacing is a separate band so that
  "measurement is not metering" stays true.

## Alternatives rejected

**A byte budget in the pod.** #131 divided a key's remaining bytes by the replica count and
debited a local tally. That turns every replica into a party to a distributed counter that has
to be rebased after restarts, rollbacks and replica-count changes, and a request that started
within budget could still overspend by one response. The control plane already computes the
state centrally; the Portal only needs the rate it implies.

**Moving an open stream to a new rate.** Reading a renewed grant from the cache once a second
and applying its rate from the next slice. It brought rate changes inside the bucket, a
lookup per response, and wrapping unpaced responses so they could pick a rate up, all for
about 1 % of streams. Ending the stream lets the client's next admission do the same work.

**Ending a paced stream only once its grant has gone stale.** That rule worked alongside the
cache lookup above, which moved a stream whose grant had been renewed onto the new rate.
Without it, a renewed grant leaves the stream at the old rate, so "not renewed" stops being
the reason to end it. It also needed an exemption for streams admitted on a due grant, or
the resumption would be ended in a loop while the control plane is down.

**Ending at an arbitrary byte.** A gzip response is one member, so the client receives an
undecodable body and squid-sdk throws instead of resuming.

**Renewing from the pacer.** It needs the credential for the life of the stream, which INV-38
forbids.

**Pacing per frame, without slicing.** At the floor a multi-megabyte zstd frame is a silence
of a minute or more, followed by a burst; a response that is one frame escapes the bound.

**A per-key rate shared by a replica's streams.** Closer to a per-key limit, but only per
replica: a key spread over replicas gets the rate once on each, so the limit it promises does
not hold. D129 chose per-stream speeds, which hold exactly.

**Refusing at the limit.** D51 rules it out: an over-limit organization is slowed, never
refused, so no status code is added.
