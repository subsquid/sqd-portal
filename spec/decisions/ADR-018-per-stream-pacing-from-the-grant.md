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
whole body, so a body cut before the trailer fails a strict decoder. squid-sdk's decoder
accepts it, but the body then ends partway through a record, and squid-sdk throws on the half
line instead of resuming. A dropped connection is different: squid-sdk and pipes-sdk both
resume from it.

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
   `floor_bytes_per_sec`, `allowance_bytes`, `used_bytes`, `period_end` and `as_of`.
   `stream_bytes_per_sec` is null when unpaced, `floor_bytes_per_sec` when there is no floor,
   `allowance_bytes` when uncapped, and `as_of` until the control plane has computed the
   usage of the current period; times are unix seconds. The Portal paces with `stream_bytes_per_sec`, reads `state` only to cap
   read-ahead (7), and copies the rest into headers. It tallies and derives nothing. A v1
   grant is unpaced.

   **A bad usage claim leaves the response unpaced, never refused.** A `usage` claim that is
   missing or malformed, or whose rate is below P-PACING-MIN-RATE (64 KiB/s), is read as
   absent: the grant stays usable, and its responses are unpaced and carry no usage header, as
   on v1. The minimum applies to a non-null `stream_bytes_per_sec`; `floor_bytes_per_sec` is
   only copied. Malformed means any field absent, of the wrong type, negative, or null where null is
   not allowed, or a `state` outside the three; any one of these discards the whole claim. A
   rate that low is a units error, not a floor: the smallest published floor is about
   0.1 MB/s, any plan rate up to 64 MB/s sent in KB/s instead of bytes reads as less than
   64 KiB/s, and at 500 B/s one worker result takes hours, which is a refusal by another name. The
   control plane never issues a rate below it, so no floor, staff overrides included, is set
   below it (DC-8). The grant's access claims are
   read as before, and a bad one still makes it unusable (DEF-17, REQ-54). The usage claim
   decides speed, not access, and nothing is refused for usage (D51), so a control-plane bug
   in the usage figures costs pacing, not traffic. Each bad claim is counted on arrival, in
   every mode, so it is seen before pacing is switched on (9). Where authorization runs in
   shadow nothing is counted (2).

   **The version is negotiated per exchange.** The pod lists the versions it reads in the
   signed exchange body, `{"credential": "…", "claims_versions": [1, 2]}`, and the control
   plane issues v2 only to a pod that lists 2, v1 otherwise. A release that predates the list
   sends none and keeps getting v1, and so does a rollback to one. v2 reaches a stack with its
   release, so ADR-017's "Portals upgrade first" holds by construction; a per-portal switch at
   the control plane raced the rollout of the pods it switched. Whether pacing acts stays per
   stack, under `auth.limits.pacing.mode`, default `off` (2). A version the pod did not list
   stays an exchange failure, as a guard. The control plane answers in an older version the
   pod listed only if that version expresses every restriction the key carries, and fails the
   exchange otherwise, so a restriction a later version adds is never dropped by answering an
   older pod. v2 adds only the usage claim, which decides speed, so v1 always qualifies. The
   control plane ignores body fields it does not read, so either side can deploy first.

   The control plane keeps one switch that answers v1 to every pod. It only narrows the
   answer to a version every release reads, so it cannot race a rollout, and it stops pacing
   on every stack as grants renew. It is the brake for usage figures that are well-formed but
   wrong, such as every organization marked `over`, which the Portal cannot tell from right
   ones; `pacing.mode: off` does the same per stack, at the cost of a redeploy each.

2. **Configuration under `auth.limits`.**

   ```yaml
   auth:
     limits:
       pacing:
         mode: off                 # off | log_only | enforce
         pace_real_time: false
   ```

   Unknown keys under `auth.limits` only warn, so a rollback to a release that predates the
   block still boots (ADR-008; directly under `auth:` an unknown key is fatal). `off` reads
   `usage`, counts a bad claim, and does not act on it. Pacing acts only where authorization
   enforces. On a portal
   whose authorization runs in shadow, pacing is inert in every mode: its counters would move
   only for requests that were granted, and so publish the verdict shadow mode withholds from
   the keyless scrape (REQ-55). `pace_real_time` exists because whether real-time data is
   slowed is an open product question; its default leaves real-time data unpaced while it
   still counts toward the allowance, and counts the waits pacing would take, so the share it
   exempts is seen (3, 9).

3. **One wrapper, inside the tap.** After the handler returns, admission wraps the body of a
   successful response admitted on a v2 grant that has a rate. That is inside the usage tap,
   which still counts what was sent and when; the tap stays measurement-only (INV-32,
   REQ-61). A response naming `real_time` as its source is wrapped in shadow unless
   `pace_real_time` is set: its waits are computed and counted as in `log_only` (8), and never
   taken. The stream routes stamp `x-sqd-data-source` inside the gate, and
   the routes with no stamp are the direct worker query and the SQL plan, both served by the
   network, so no route has to declare its source to the gate.

   Error responses are not wrapped. They carry no chain data, and two outer layers re-render
   them after the gate: framework rejections, and 5xx envelopes stamped with the request id,
   whose size the client's own `x-request-id` sets. No successful body is rewritten after the
   gate. The wrapper ends its body only once it holds no piece, and its size hint counts the
   bytes it holds, so the transport cannot drop held pieces and reads the same framing as
   without pacing.

4. **Token bucket on encoded bytes, one second deep.** The bucket fills at
   `stream_bytes_per_sec` and holds one second of it. A frame larger than that is split,
   without copying, into pieces of one second of the rate. A piece is released once the
   bucket covers it; when it does not, the response sleeps until the bucket is full. The
   bound (one second of the rate plus the rate times elapsed time) therefore holds through the
   end of the stream, including a single large final frame, and the longest pause pacing adds
   is one second plus the timer's lateness. A response held back by its rate wakes at most
   twice in any second whatever its frame size: each wake sends what the previous sleep held
   back, so two consecutive intervals between wakes together last more than a second. One
   that does not wait has no timer. Within each second its bytes go out together; nothing in
   the contract relies on smoothing them. A timer that fires late costs the stream its
   lateness, about a millisecond in a second, so a held-back stream runs that much below its
   rate.

5. **Admission fixes everything.** The rate, the read-ahead cap and the headers come from the
   grant the response was admitted on and do not change while it streams. The pacer reads
   nothing after admission: no grant cache lookup, no exchange, no secret (DEF-16 and INV-38
   are unchanged).

6. **Paced streams start no chunk after P-PACED-STREAM-MAX-AGE, 5 minutes.** In `enforce`, a
   request admitted on a grant with a rate gets a deadline: its admission time plus 5
   minutes. The network chunk stream checks it the way it checks the operator's chunk cap
   (REQ-8): past the deadline it starts no chunk after its first, sends the ones already
   started, and ends. gzip writes its trailer, zstd stops after a whole frame, and nothing
   downloaded is discarded. The first chunk is exempt, so a stream whose request body arrived
   late still serves something; it never ends empty, which both clients would read as "no
   data for this range", and the deadline never turns a stream into a 204. The deadline
   bounds when the last chunk starts, not when the response ends: the chunks in flight still
   drain at the paced rate, which at the floor can take minutes for one large worker result.
   Both squid-sdk (`master` @ `4c86209`) and pipes-sdk (`main` @ `b1d46a6`) request again from
   the last block + 1 after a non-empty response ends, as after any short response. Routes
   with no chunk stream are never ended: the real-time proxy, the direct worker query, the SQL
   plan and the timestamp lookup. Neither are unpaced responses, Enterprise among them, so a
   stream admitted without a rate keeps running unpaced, even after its organization crosses
   into a paced state, until the client's next request.

   This is the only way a changed rate reaches an open stream. Nothing re-reads the grant:
   the resumption is admitted on whatever grant its replica holds, and if that grant is due
   its admission starts the renewal while still served on it (REQ-54). The change lands at the
   first resumption admitted by a replica that has renewed since. With the 5-minute refresh
   interval that is typically the second resumption on one replica; replicas renew
   independently and only when a request finds the grant due, so no count is promised. The
   limit is a plain age, not a check for a stale grant (see the alternatives). While the
   control plane is down it costs at most one reconnect per long paced stream every 5
   minutes, the cost agreed with EF on 2026-10-09.

7. **Read-ahead capped at the floor.** In `enforce`, a stream admitted on a grant whose
   `state` is `over` and whose rate is set gets its `buffer_size` capped at 1 on every network
   stream route, the debug variant included: the scheduler downloads one chunk ahead. What
   the encoder and the pacer hold past that is not counted against it. Without the cap a
   stream at the floor could download up to P-BUFFER-MAX chunks ahead at full speed, and its
   age limit would then drain them at the floor for hours.

8. **`log_only`.** Waits are computed and not taken. Nothing is ended, nothing is capped and
   no header is added, so the client sees exactly what `off` serves. The waits go into the
   same counters `enforce` uses (9); the portal's mode says which they are. A response's
   computed waits are the sleeps `enforce` would take on the same frames, replayed on the
   clock the response would have kept had it slept, so each wait counts once; measured
   against real time instead, the shortfall at every frame would count every earlier wait
   again. Wait seconds per second on a replica then approximate how many more streams the
   pacer's sleeps would hold open there, a guide to set against P-MAX-STREAMS before `enforce`
   and not a bound: a source that pauses absorbs some waits, a client that waits for one stream
   before opening the next opens fewer, and the read-ahead clamp, which `log_only` does not
   apply, can lengthen a stream admitted over its allowance with no sleep at all. The Portal
   logs nothing per response for pacing. Who would be slowed the control plane estimates from the
   usage records it already holds: each is a key's encoded bytes over a window (REQ-60), and a
   window that carried more than the burst plus the rate times its length would have waited
   at least the difference at that rate. That is a lower bound, to the records' millisecond
   precision, not the wait: the records carry no wait, no admission rate and no response
   identity, and the rate the control plane holds now may not be the one a response was
   admitted at. Exact totals are in the counters, by state and source only.

9. **Counters.** `portal_limit_paced_seconds_total` and `portal_limit_paced_responses_total`
   count waits, taken in `enforce` and computed in `log_only` and for real-time responses in
   shadow (3); `portal_limit_bytes_total` counts the encoded bytes of every wrapped response,
   so the share that went out unpaced is seen; `portal_limit_aged_ends_total` counts streams
   ended by the age limit. All are labelled by `state`, the byte and wait counters also by
   `source` (`network` or `real_time`), and nothing finer: no key or organization.
   `portal_limit_bad_usage_claims_total` counts v2 grants whose usage claim was read as absent,
   unlabelled, in every mode including `off`, and none moves where authorization runs in
   shadow. A response counts as paced once it has waited, or would have. No new status code
   exists: a paced response is a 200, and nothing is refused for usage.

10. **Headers (D106), in `enforce` only,** on every gated response admitted on a v2 grant
    whose usage claim was read:
    `x-sqd-usage-state`, `x-sqd-usage-limit-bytes` (omitted when uncapped),
    `x-sqd-usage-used-bytes`, `x-sqd-usage-reset` (`period_end`, RFC 3339),
    `x-sqd-usage-floor-bytes-per-sec` (omitted when null) and `x-sqd-usage-as-of` (RFC 3339,
    omitted when null). All six join the CORS expose list, or browsers cannot read them.

## Failures

| Failure | Behaviour |
|---|---|
| A v2 grant whose usage claim is missing, malformed or has a rate below P-PACING-MIN-RATE | The grant is used; its responses are unpaced and carry no usage header. The claim is counted, in every mode. |
| The control plane's usage figures are well-formed but wrong | The Portal cannot tell. The control plane's v1 switch stops pacing on every stack as grants renew; `pacing.mode: off` stops it per stack, with a redeploy. |
| Control plane down | Held grants keep admitting requests at their rate until `expires_at`. An organization that crosses its allowance stays at full speed, and one whose period resets stays at the floor. This fails open on the rate, which is accepted and documented. Long paced streams still reach their age limit and resume, at most once per 5 minutes, with no loop. A resumption is admitted only where its replica holds a usable grant; one that lands on a replica that does not is refused as `upstream_unavailable` and retried by the client. |

## Consequences

**Per stream, not per key.** D129 sets speeds per stream, and the Portal holds nothing per
key. A key with N open streams receives N times the rate. Capping concurrent streams (D130)
is not part of v2.

**The age limit also lands revocations on paced streams.** A revoked key's paced stream
starts no chunk after 5 minutes, and its resumption is refused once the replica has learned
the denial, which the resumption's own renewal can bring. Unpaced streams are never ended, as
before.

**How quickly a crossing reaches the client is a timeline, not a bound.** Reporting (interim
records every 30 s), the control plane's aggregation (up to 2 minutes), grant renewal (60 s
once the grant was issued at 90 % of the allowance or more, 5 minutes below that), then the
client's next request admitted on the renewed grant. That is about 3 minutes typically, and
about 8 minutes from below 90 % straight to over, plus one or two age limits and their drain
for a long stream. D51 tolerates it. Full speed after a purchase or an upgrade comes back
on the same path.

**Pacing makes streams longer.** At the floor more streams are in flight at a given moment,
each holding a connection, its read-ahead and its parallel-stream slot longer. The read-ahead
cap holds the window at the floor to one chunk; at plan speed a paced stream holds what a slow
client already holds. The slots are the part that reaches other keys: a replica at
P-MAX-STREAMS refuses new streams as overload (REQ-20) sooner, whoever they belong to.
`log_only`'s waits estimate the pacer's share before `enforce` is switched on (8). A paced
stream still open when shutdown's drain budget runs out ends as any other stream does at
shutdown (REQ-24).

**Keys added by an edge rewrite get headers too.** Where a deployment attaches the key with a
header rewrite at the edge, its clients receive `x-sqd-usage-*` headers for a key they never
sent.

**The review of #131** (kalabukdima, 2026-07-20) raised three objections. This design answers
each by how it is built, not by an added mitigation.
- *CPU.* #131 inflated every response to count logical bytes, on a portal already seen
  burning 30 cores recompressing streams. Nothing here decompresses. The allowance and the
  rate are in encoded bytes, and the bucket counts frame lengths the body already has. The
  added work per frame is a clock read, a subtraction and, above one second of the rate, a
  zero-copy split, and only on responses that have a rate. A response held back by its rate
  wakes at most twice a second, whatever its frame size, and one that does not wait has no
  timer. What remains is more frames on zstd responses at the floor, whose frames are whole
  worker results, and the per-frame work of the wrapper itself. HZ-16 makes these a budget CT-6 measures, not a cost
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
  the secret on a second hop. Enforcement lives in the pod for these reasons.

## Known gaps

Accepted for v1. None of them stops a client that works around the limit on purpose. The
control plane finds each from the usage records it already holds, per key and hour, and the
per-key levers the deployment already has at its ingress act on it; the Portal adds nothing for
them.

- **N streams get N times the rate** (Consequences). Signal: a key's bytes per second well
  above its per-stream rate.
- **Every response starts with a full burst.** A client that splits its range into requests
  smaller than P-PACING-BURST is never slowed. Signal: many requests with a small mean
  response.
- **The drain after the age limit is bounded only by the read-ahead,** which is clamped only
  when the grant is over. A stream admitted within that crosses mid-stream still sends, at the
  plan rate after its age limit, its read-ahead window, up to P-BUFFER-MAX chunks, plus what
  the encoder and pacer already hold. Signal: plan-rate bytes in the records after the
  crossing.
- **Real-time is unpaced by default,** and a dataset with no archive is served entirely as
  `real_time`, so all of it is unpaced. Signal: real-time bytes after the allowance is crossed,
  and the shadow waits by source (9).
- **Keyless traffic is paced at the edge,** at 2.5 MB/s per stream, a stopgap. The public
  portal moving onto this pacing, at the floor and keyed by client address, is outside this
  ADR; until then a key over its allowance gets more per stream by dropping its key.

## Spec changes

- NG2 now states that per-key limits exist, are decided by the control plane and carried in
  the grant, and that each response is paced alone with no state shared between replicas.
- DEF-17 gains the v2 usage claim. DEF-16 and INV-38 are unchanged: the pacer reads nothing
  after admission.
- REQ-57 (the exchange offers the claims versions the build reads) is new, in the access
  control band since it outlives pacing; DC-8's call contract and DEF-17 gain the negotiation.
- REQ-70..REQ-75 (usage pacing), INV-16 (pacing bound), INV-17 (pacing changes timing and,
  through the age limit, extent only), OB-16, IB-10 (usage headers), HZ-16 and CT-12 are new.
- INV-25: an end the Portal decides — the operator's chunk cap, the age limit — stops the
  record sequence before encoding, so the body is a complete encoding. LIV-13: its bound covers
  requests admitted after the renewal; an open response keeps its terms, and a long paced
  network stream catches up through its age limit. INV-11, LIV-2, IB-1, OP-1, OP-11, SLI-2,
  SLI-6 and the DC-8 fault table in 09 gain one clause each.
- The tap (INV-32, REQ-61) stays measurement-only; pacing is a separate band so that
  "measurement is not metering" stays true.

## Alternatives rejected

**A byte budget in the pod.** #131 divided a key's remaining bytes by the replica count and
debited a local tally. That turns every replica into a party to a distributed counter that has
to be rebased after restarts, rollbacks and replica-count changes, and a request that started
within budget could still overspend by one response. The control plane already computes the
state centrally; the Portal only needs the rate it implies.

**Moving an open stream to a new rate, and ending it once its grant went stale.** The pacer
read the renewed grant from the cache once a second and applied its rate from the next piece,
and ended a stream whose grant had not been renewed. That brought rate changes inside the
bucket, a lookup per response, and wrapping unpaced responses so they could pick a rate up,
all for about 1 % of streams. Without the lookup, staleness is the wrong trigger: a
resumption is admitted on the held due grant while the renewal runs, so the rule would either
never end it again, leaving it at the old rate for life, or end it in a loop while the
control plane is down. A plain age has neither problem, and the client's next admission does
the lookup's work.

**Fixed 64 KiB slices.** Cutting every frame into 64 KiB slices smooths delivery within a
second, but a stream held back at a 20 MB/s plan rate wakes about 300 times a second to
release them. The bound does not need it, and the smallest published floor, about 0.1 MB/s,
already exceeds 64 KiB a second, so a 64 KiB minimum burst never applied either.

**A per-key rate shared by a replica's streams.** Closer to a per-key limit, but only per
replica: a key spread over replicas gets the rate once on each, so the limit it promises does
not hold. D129 chose per-stream speeds, which hold exactly.

**Refusing a grant whose usage claim is bad.** Strict reading makes any answer the Portal
cannot fully read unusable, which is right for access claims. For the usage claim it turns a
control-plane bug in speed figures into refused keys on every stack that enforces
authorization, for usage, which D51 rules out. Read as absent, the same bug costs pacing
until it is fixed, and the count shows it.

**Refusing at the limit.** D51 rules it out: an over-limit organization is slowed, never
refused, so no status code is added.
