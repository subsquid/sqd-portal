# ADR-015 — One connection-class replay on the real-time path

Status: Accepted (2026-07-21)

## Context

ADR-003 made the real-time path a single un-replayed proxied request: retrying after
response bytes have arrived would duplicate a prefix, and buffering to make retries safe
would defeat streaming. That reasoning covers faults *after* the response head. It swept
in the faults that happen before it too, and those turn out to be the common ones.

Every replica replacement of the real-time source is client-visible today. The Portal
reaches the source through one virtual address and holds pooled keepalive connections, so
a terminating replica takes live connections down with it while its ready siblings sit
idle. Two fault shapes escape the transport's own recovery: a request whose bytes already
reached the wire when the peer closed — the connection dies before any response head, and
the transport cannot know the request was not acted upon, so it refuses to replay it — and
a connect-class failure against an endpoint being retired. A request the transport never
wrote is already replayed for us, on any method; that class is not what fires in
production. Observed 2026-07-21 09:14:10: a 502 burst one second after the upstream's
SIGTERM.

The failure is also invisible to the indicators: SLI-4 measures readiness and excludes
deploys, SLI-6 counts truncated streams, and these are clean pre-first-byte refusals.

## Decision

A DC-4 request that fails **before its response head is received** is replayed exactly
once, on a connection other than the failed one, when the fault is connection-class: the
connect attempt failed, or the connection closed with no response head. The client sees
the outcome of the replay.

"Closed" includes a reset, not only a clean close. A replica that dies with our request
still unread makes the kernel send RST rather than FIN, and that arrives as a bare I/O
kind with none of the transport's own connection-loss predicates set. It is the more
common shape of the incident, so classifying only the clean close would leave most of the
fault this decision exists for unreplayed. For the same reason the classifier never stops at
the first error it recognises: a layer that wraps the reset in an I/O error of its own
would otherwise answer the question before the real cause is reached.

Because the classification reads HTTP/1 error shapes, the real-time client is pinned to
HTTP/1.1. The source is a plaintext in-cluster service today, so nothing negotiates
otherwise — but over TLS, ALPN would select h2, and a GOAWAY before the head would arrive
as a protocol error none of these predicates match. The replay would stop firing with
every test still passing. Pinning makes the deployed protocol the tested one, and turns a
silent narrowing into a visible constraint on how the source may be exposed.

Two exclusions carry the weight of the decision.

Any fault **after** the response head keeps ADR-003's rule: the response truncates on a
record boundary (INV-25, ADR-001). Nothing already delivered is ever re-fetched, so no
prefix can be duplicated.

A read-idle deadline expiry is **not** connection-class. A stall consumes the whole read
budget; replaying it would push the answer past the caller's deadline and reintroduce the
blindness ADR-010 removed.

No third rule models the caller's own deadline, and deliberately so. The Portal cannot
know it — P-CLIENT-TIMEOUT is a sizing constraint on configuration, not a value to branch
on at run time, and a guess about it is wrong in both directions with no way to notice. It
also isn't needed: a caller that has given up disconnects, and disconnect reclamation
(LIV-10) cancels the replay with everything else the stream held. The caller's deadline
enforces itself. The residual case — the upstream dies late in a request, and the replay
answers after the caller has already timed out — costs one upstream attempt for an absent
client and is bounded by the same reclamation.

That residual does widen the per-call bound REQ-22 states. A replay gets a fresh read
budget, so the worst case is 2 × P-HOTBLOCKS-READ-TIMEOUT, and in that tail the Portal's
deadline no longer sits below P-CLIENT-TIMEOUT. Accepted, and recorded in REQ-22 and DC-4
rather than left implicit: the shape that fires in production is a pooled connection to a
terminating replica, which dies in milliseconds, and the fault the ordering exists to
catch — a stall — is never replayed. What a whole *client request* may spend upstream is a
separate question this decision does not touch: handlers already made real-time calls in
sequence, so that total was a multiple of the per-call bound before the replay doubled
each call.

Every logical DC-4 request is counted once in `hotblocks_requests` (OB-4), rather than
splitting replay and non-replay paths into metric families that must be joined. Its closed
`outcome` is `response` when the first attempt obtains a response head,
`replay_response` when the replay obtains one, and `replay_failed` when the replay
completes with another transport fault. `canceled` and `replay_canceled` preserve which
attempt the caller abandoned without claiming an upstream result the Portal never observed.
Every value names what the transport observed, and none asserts what the client received:
a response head of any status counts as one. Mixing the two axes in one closed set would
make the family read as a success rate it cannot support — what the client got is
`http_status`.

The same family records `timeout` for the expected non-replay decision and
`transport_failed` for any other first-attempt failure that was not replayable. This makes
healthy traffic the denominator, keeps an absorbed fault visible, and prevents zero replays
from reading as health when the classifier has instead stopped recognising the fault.
Growth in `transport_failed` is the classifier-miss signal. The HTTP/1 pin prevents one
known cause of that rot; the counter is what finds the unknown ones.

## Consequences

One client request may now reach the real-time service twice. That is safe because every
operation on that path is a read and the Portal keeps no dedup keys (04), and because the
replay is gated on zero bytes delivered — ADR-003's prefix-duplication argument is not
weakened, only narrowed to the range where it applies.

Replica replacement stops being client-visible in the common case, which is the point.
Masking is not a guarantee: behind one virtual address a replay can land on the same
retiring replica, and a sustained outage still fails safe as UPSTREAM-FAILURE one attempt
later. Bounded at one attempt and counted, it stays inside LIV-12 — this is not a retry
loop and must not grow into one.

The replay is immediate, with no backoff or jitter. A terminating replica fails every
connection it holds within the same few milliseconds, so the survivors take a correlated
burst of reconnects at exactly the moment they absorb the failover. Accepted: at one
attempt the burst is bounded at twice the in-flight count and arrives once, which is what
a connection pool refilling after any replica loss does anyway, while a delay would tax
the common case — a pooled connection to a dead peer, replayable in microseconds — to
smooth a spike the survivors already handle. Jitter becomes worth revisiting only if the
replay ever gains a second attempt, which LIV-12 says it must not.

The Portal now absorbs a class of upstream deploy fault instead of reporting it.
`hotblocks_requests{outcome="replay_response"}` growing outside a deploy means the upstream
is losing connections at a rate the client-visible error rate no longer reflects. How much
of that was actually masked is derived rather than read off this counter: a replay that
answers 503 is counted here and still reaches the client as an error, in `http_status`.
`outcome="replay_failed"` is the residual the replay did not cover — no response head at
all — and `outcome="replay_canceled"` makes an inconclusive replay visible
without blaming the upstream. The unified counter supplies an SLI denominator, but no
objective is defined for it yet (GAP-22).
