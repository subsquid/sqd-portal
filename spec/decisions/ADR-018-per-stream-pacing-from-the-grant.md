# ADR-018 — Per-stream pacing from the grant

Status: Proposed (2026-10-09)

## Context

Portal plans carry an allowance in wire bytes per Portal month (D153) and a per-stream speed
(D129). An organization over its allowance is slowed to a floor speed, never refused (D51).
The control plane already measures what each key was served (REQ-60, ADR-016) and already
answers one exchange per credential (DC-8). What was missing is the half that acts on it, and
NG2 ruled it out: admission decides whether a request is served, never how much it may take.

Three facts about this codebase shape the answer.

**The Portal must not hold a budget.** A replica sees only its own share of a key's traffic,
and nothing survives a restart (NG5). Counting bytes against an allowance in the pod means
dividing the allowance between replicas and reconciling the shares, which is what #131 did
with a per-pod tally rebased on every snapshot. The control plane has the totals: it
aggregates the usage records into one state per organization, so it can say how fast a key
may go, and say it again when that changes.

**The pacer cannot renew a grant.** Renewal needs the credential, and the credential is
destroyed once admission is done (DEF-16, INV-38). A long stream holds only the fingerprint.
It can read what other requests put in the grant cache, and nothing more: a stream that is
its key's only traffic on a replica never sees a newer grant.

**A response cannot be cut short at an arbitrary byte.** A gzip response is one member for the
whole body, from the recompressor and from gzjoin alike, so a body cut before the trailer
fails to decode, and squid-sdk throws on it instead of resuming. zstd network frames are whole
worker results, often many megabytes in one frame, so a pacer that waits per frame either
stalls for a minute at the floor or lets a large frame through unpaced.

## Decision

The control plane decides one effective rate per stream and carries it in the grant. Each
replica paces each response to the rate of the grant it holds, alone, with no state shared
between responses or replicas.

1. **Grant v2.** The Portal accepts `claims_version` 1 and 2. Version 2 requires a `usage`
   claim: `state` (`within`, `over`, `unmetered`), `stream_bytes_per_sec`,
   `floor_bytes_per_sec`, `allowance_bytes`, `used_bytes`, `period_end` and `as_of`. The rate
   is null when unpaced, the floor when there is none, the allowance when uncapped, and
   `as_of` when the organization has no period yet; times are unix seconds. The Portal paces with
   `stream_bytes_per_sec` only and copies the rest into headers. It tallies, derives and
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
         end_stale_streams: false
   ```

   Unknown keys under `auth.limits` only warn, so a rollback to a release that predates the
   block still boots (ADR-008; directly under `auth:` an unknown key is fatal). `off` ignores
   `usage` entirely. Pacing has its own mode, apart from the authorization enforcement mode,
   so pacing can run in shadow on a portal that already enforces keys.

3. **One wrapper, inside the tap.** The auth middleware wraps the response body after the
   handler returns. That is inside the usage tap, which still counts what was sent and when;
   the tap stays measurement-only (INV-32, REQ-61). Every response admitted on a v2 grant is
   wrapped, paced or not, so a stream admitted unpaced can still pick up a rate. A response
   naming `real_time` as its source is left unpaced unless `pace_real_time` is set: by
   default real-time data counts toward the allowance but is not slowed. Every other response
   is paced whenever its grant has a rate. The stream routes stamp `x-sqd-data-source` inside
   the gate, and the routes with no stamp are the direct worker query and the SQL plan, both
   served by the network, so no route has to declare its source to the gate. A response
   naming no source is an error or an empty body, and pacing it costs nothing.

4. **Token bucket on encoded bytes, sliced.** The bucket fills at `stream_bytes_per_sec` and
   holds one second of the rate, but never less than 64 KiB, so one slice always fits. Frames
   are split with `Bytes::split_to` (zero-copy) into slices of at most 64 KiB, and a slice is
   released only once the bucket covers it. The bound (burst plus rate times elapsed time)
   therefore holds through the end of the stream, including a single large final frame, and
   the longest pause pacing adds is one slice at the current rate. `poll_frame` stays
   synchronous: a response that must wait polls a stored `tokio::time::Sleep`, and a response
   that does not wait has no timer.

5. **Rate pickup through `peek`.** At most once a second per response, the wrapper calls a
   new `GrantCache::peek(fingerprint, now)`: synchronous, no exchange, no LRU promotion, no
   secret. If the cache holds a newer grant for that fingerprint, which any request with the
   same key on this replica may have renewed, the new rate applies from the next slice. Not
   per frame: `recompress_gzip` yields 4 KiB frames, and the grant map's single mutex is the
   one admission takes. DEF-16 and INV-38 are unchanged: the fingerprint is the SHA-256 of
   the token, which the cache is keyed by already.

6. **The stale end, behind `end_stale_streams`** (agreed with EF on 2026-10-09: ending
   streams is acceptable). Default off; it is turned on per stack in the same change as
   `enforce`. In `enforce` with the switch on, the wrapper ends a response when all of
   these hold:
   - the grant it is paced by has a rate;
   - it took that grant up, at admission or through `peek`, before the grant's
     `refresh_after`;
   - it is now 30 s past that `refresh_after`;
   - `peek` finds nothing newer.

   A response that took up a grant already due is never ended for staleness. Admission
   serves a due grant while it renews in the background, so in a control-plane outage that is
   exactly the resumed request, and ending it would loop. A stale paced stream is therefore
   ended at most once per outage.

   **How it ends.** Never by cutting the encoded body. The middleware inserts a stop flag as
   a request extension before the handler runs; the wrapper sets it, and the network chunk
   stream reads it between chunks, the same place the operator's chunk cap ends a stream
   (REQ-8). The chunk stream ends, gzip writes its trailer, and zstd stops after a whole
   frame. The flag is read only after a chunk has been sent, so a stale end never produces an
   empty 200: squid-sdk (`if (!res.data) break`) and pipes-sdk (`if (res.stream == null)
   break`) stop the whole stream on one. After a non-empty body ends, both clients request
   again from the last block + 1, as they do after any short response (both `main` branches,
   2026-10-09). A response with no chunk boundaries never reads the flag: the real-time proxy,
   the direct worker query, the SQL plan and the timestamp lookup are never ended this way.

7. **Read-ahead capped at the floor.** The middleware also inserts the usage snapshot as a
   request extension. In `enforce`, a stream admitted on a grant whose `state` is `over` and
   whose rate is set gets its `buffer_size` capped at 1. The cap is read in
   `run_stream_internal` and `run_archival_stream`, not in `restrict_request`, which the
   `/debug` variant skips. It applies at admission only: a stream that crosses mid-way keeps
   its window until it ends.

8. **`log_only`.** Waits are computed and not taken. Nothing is ended, nothing is capped and
   no header is added, so the client sees exactly what `off` serves. The replica counts
   `portal_limit_would_wait_seconds_total` and `portal_limit_would_pace_responses_total`, both
   labelled by `state`. The first would-be wait of a response is logged with `key_id`,
   `organization_id`, the rate and the state. Metrics carry no key or organization.

9. **`enforce`.** Waits are taken and counted in `portal_limit_paced_seconds_total`,
   `portal_limit_paced_responses_total` and `portal_limit_stale_ends_total`, labelled by
   `state`. A response counts as paced once it has waited. No new status code exists: a
   paced response is a 200, and nothing is refused for usage.

10. **Headers (D106), in `enforce` only,** on every gated response admitted on a v2 grant:
    `x-sqd-usage-state`, `x-sqd-usage-limit-bytes` (omitted when uncapped),
    `x-sqd-usage-used-bytes`, `x-sqd-usage-reset` (`period_end`, RFC 3339),
    `x-sqd-usage-floor-bytes-per-sec` (omitted when null) and `x-sqd-usage-as-of` (RFC 3339,
    omitted when null). All six join the CORS expose list, or browsers cannot read them. They
    describe the grant the response was admitted on and do not change while it streams.

## Failures

| Failure | Behaviour |
|---|---|
| Control plane down | Held grants keep their rate until `expires_at`. An organization that crosses its allowance stays at full speed, and one whose period resets stays at the floor. This fails open on the rate, which is accepted and documented. A stale paced stream is ended once, and its resumption, admitted on a grant already due, is not ended again. |
| A v2 grant on a portal with pacing `off` | Not paced, like v1. |
| A portal on an older release switched to v2 by mistake | The release reads v2 as an unknown claims version. New keys get 502 `upstream_unavailable`; cached keys serve until `expires_at`, up to the grant lifetime (ADR-017), and then every key on that portal is refused. Upgrade first, then switch. |

## Consequences

**Per stream, not per key.** D129 sets speeds per stream, and the Portal holds nothing per
key. A key with N open streams receives N times the rate. Capping concurrent streams (D130)
is not part of v2.

**A single long stream keeps its admission rate while `end_stale_streams` is off.** Only
requests renew grants, the chunk count is unlimited by default (P-MAX-CHUNKS-PER-STREAM), and
the pacer has no secret. A key's other requests on the same replica move it through `peek`;
with nothing else on that replica it runs at its admission rate until it ends. Measured over
seven days on the keyed stacks, 520 of about 60,000 network streams that would be paced ran
past 5.5 minutes, one renewal interval plus the grace. The switch exists for them.

**The stale end also lands revocations on open paced streams.** With the switch on, a denial
evicts the grant (INV-6), so `peek` finds nothing newer and the stream ends 30 s after its
`refresh_after`; the resumed request is refused. Unpaced streams are never ended, as before.

**How quickly a crossing reaches the stream is a timeline, not a bound.** Reporting (interim
records every 30 s), the control plane's aggregation (up to 2 minutes), grant renewal (60 s
once the grant was issued at 90 % of the allowance or more, 5 minutes below that), then pickup
through `peek` or the stale end. That
is about 3 minutes typically, and about 8 minutes from below 90 % straight to over. D51
tolerates it. Full speed after a purchase or an upgrade comes back on the same path.

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
  added work per frame is a clock read, a subtraction and, above 64 KiB, a zero-copy split.
  A timer exists only while a response waits, and `peek` takes a lock at most once a second
  per response. What remains is more frames on zstd responses, whose frames are whole worker
  results, and the per-frame work of the wrapper itself. HZ-16 makes these a budget CT-6
  measures, not a cost claimed to be zero.
- *Prefetch pulling data a client never receives.* #131 cut streams mid-flight when a key's
  quota ran out, so everything read ahead past the cut had been downloaded for nothing, at a
  depth the client chose through `buffer_size`. Nothing here is cut for usage (D51). A stream
  admitted at the floor reads ahead one chunk. The one end the Portal initiates sits behind a
  switch, happens at most once per renewal interval per stream, and discards at most that
  stream's read-ahead, which is what a client disconnecting at the same moment discards.
- *Why not a proxy in front.* A proxy can slow the bytes a client receives, which is the easy
  half. It cannot cap the read-ahead, which lives in the stream scheduler. It cannot end a
  stream where the gzip member can still be closed: the encoder runs in the pod, and a proxy
  sees only the encoded body. And it holds no grant, so it would need its own exchange and the
  secret on a second hop. Enforcement lives in the pod for these reasons.

## Spec changes

- NG2 now states that per-key limits exist, are decided by the control plane and carried in
  the grant, and that each response is paced alone with no state shared between replicas.
- DEF-17 gains the v2 usage claim. DEF-16 and INV-38 are unchanged: the pacer reads the cache
  by fingerprint and never holds the secret.
- REQ-70..REQ-75 (usage pacing), INV-16 (pacing bound), INV-17 (pacing changes timing and
  extent only), OB-16, IB-10 (usage headers), HZ-16 and CT-12 are new.
- INV-25: an early end the Portal decides stops the record sequence before encoding, so the
  body is a complete encoding; REQ-8's chunk cap is the precedent. LIV-13: a lowered rate is a
  narrowing, and reaches open responses through `peek` or the stale end. INV-11, LIV-2, IB-1,
  OP-1, OP-11 and the DC-8 fault table in 09 gain one clause each.
- The usage state the Portal reads from the grant is the control plane's alone. The tap
  (INV-32, REQ-61) stays measurement-only; pacing is a separate band so that "measurement is
  not metering" stays true.

## Alternatives rejected

**A byte budget in the pod.** #131 divided a key's remaining bytes by the replica count and
debited a local tally. That turns every replica into a party to a distributed counter that has
to be rebased after restarts, rollbacks and replica-count changes, and a request that started
within budget could still overspend by one response. The control plane already computes the
state centrally; the Portal only needs the rate it implies.

**Renewing from the pacer.** That would give a long stream a fresh rate on its own schedule.
It needs the credential for the life of the stream, which INV-38 forbids for good reason: a
long-held secret is one more place for it to leak.

**Ending a stale stream by closing the body.** Simple, and wrong: a gzip response is one
member, so the client receives an undecodable body and squid-sdk throws instead of resuming.

**Ending every stale stream, whenever admitted.** It would also end the resumption of a stream
already ended for staleness while the control plane is down, which gets the same due grant
again, so the client would loop for the whole outage.

**Pacing per frame, without slicing.** At the floor a multi-megabyte zstd frame is a silence
of a minute or more, followed by a burst; a response that is one frame escapes the bound.

**A per-key rate shared by a replica's streams.** Closer to a per-key limit, but only per
replica: a key spread over replicas gets the rate once on each, so the limit it promises does
not hold. D129 chose per-stream speeds, which hold exactly.

**Refusing at the limit.** D51 rules it out: an over-limit organization is slowed, never
refused, so no status code is added.
