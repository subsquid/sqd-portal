# ADR-018 — The Portal signs its control-plane requests

Status: Proposed (2026-08-07)

## Context

ADR-016 puts one call on the request path: the Portal hands a client's credential to the
control plane and is handed back a grant that decides whether to serve. That call needs
two things the first draft settled in a clause. The control plane must know *which* portal
is asking, because portal scope is one of the rungs and because attribution, quota and
abuse control at the control plane have nowhere else to key. And the call must not be
forgeable, with replay bounded explicitly, because anyone who can make it can ask the
authority to bless a credential of their choosing.

The first draft used a shared bearer service token, one secret per deployment, sent on
every request. That is the weakest of the available options in a way worth naming: the
token authenticates the *sender of a header*, not the request. Whoever reads it once —
from a log line, a proxy, a misrouted request, a snapshot of a replica's environment — can
make any exchange the Portal could, indefinitely, from anywhere. It is a secret at rest on
both sides, rotating it is a coordinated change across every replica and the control plane
at once, and it binds nothing to the bytes it travels with.

## Decision

Every request the Portal makes to the control plane carries exactly one of each:

- `X-Portal-Id: <portal_id>` — the identity whose registered keys may verify the request;
- `X-Signature-Timestamp: <unix_seconds>` — the timestamp covered by the signature;
- `X-Signature: <signature>` — the signature under the Portal's configured signing key.

Missing, repeated, or malformed signing headers are a failed exchange. The control plane
uses `X-Portal-Id` to select that portal's active registered public keys and accepts when
one verifies; registration forbids the same public key under two portal identities, so a
valid signature has exactly one identity. The timestamp is accepted only when its absolute
skew from the control plane's current Unix time is no greater than
P-SIGNATURE-MAX-SKEW: a date too far in the past *or the future* is refused. The Portal
follows no redirects on these requests: a redirected exchange carries a client's
credential somewhere the operator did not configure, and its answer is not the control
plane's.

The signature authenticates the Portal *to* the control plane and nothing in the other
direction. The control plane's own identity, and therefore the integrity of the grant that
comes back, rests on the transport. This is worth stating rather than assuming: the grant's
claims are what admit traffic, so a deployment that lets the exchange run over a channel it
does not authenticate has not weakened a detail, it has moved the gate.

## Mechanism

**Ed25519 over a newline-joined canonical string, raw 64-byte signature, unpadded
base64url.** A deployment may reuse an Ed25519 network identity or configure a dedicated
Ed25519 signing identity; the wire contract is identical, and in neither case does the
control plane hold secret key material. The verifier is in every standard library worth
the name; in Node it is `crypto.verify(null, message, key, signature)` with no dependency
at all, at roughly 80 µs per call, which is spent only on a cache miss.

The registered value is the **raw 32-byte public key**, also unpadded base64url — not the
peer id — and one raw key may belong to only one `portal_id`. A peer id wraps that key in
protobuf inside a multihash, so verifying one means depending on a libp2p implementation;
the raw bytes drop straight into a JWK `x` member, which is base64url already.

Both encodings avoid `+`, `/` and `=`. Decoders reject padding and every byte outside the
base64url alphabet, so two textual encodings cannot name the same key or signature. A test
asserts the alphabet and compares the signature byte-for-byte across a real HTTP hop.

```
sqd-portal-v1
<portal_id>
<unix_seconds>
<METHOD>
<request_target>
<sha256(body) as lowercase hex>
```

Those six ASCII fields are joined by one LF byte, with no trailing LF. `portal_id` uses
`[A-Za-z0-9._:-]+`; `unix_seconds` is unsigned base-10 with no leading zero except the value
zero; `METHOD` is the uppercase method sent on the wire. `request_target` is the exact HTTP
origin-form path plus optional query as transmitted — percent escapes are neither decoded
nor normalized, and a fragment is impossible on the wire. The digest is lowercase hex over
the exact request-body octets passed to the transport; exchange requests use no content
encoding. No field can contain LF, so no two distinct requests share a canonical form. The
scheme tag leads, so a Portal and a control plane that disagree about the shape fail
verification loudly rather than subtly. A test pins the exact bytes of a worked example —
headers, key, message and signature — against which the verifying side can be checked
without running the Portal.

**Not HMAC**, though it would verify in a microsecond rather than eighty: it is a shared
secret, which is the thing the paragraph above rejects. The control plane would be back to
storing something that forges portal identities if it leaks, and rotation would be back to
a synchronized change. Eighty microseconds on a cache miss is not a price worth that.

**Not RFC 9421** HTTP Message Signatures, which is the standard and would be the right
answer if either side already spoke it. Neither does, its Node implementations are thin,
and its generality — signature suites, component lists, negotiation — is all cost here:
there is exactly one signer, one algorithm, and one request shape to cover.

The source of the configured private key — the Portal's network identity (REQ-43) or a
purpose-issued identity — and how the control plane stores registrations are deployment
choices. What the contract pins is the request-carried portal identity and timestamp, the
canonical form, the algorithm, and the active-key lookup used to verify it.

## Consequences

Nothing secret is stored at the control plane for this: a compromised replica is contained
by withdrawing one public key, and adding a portal is publishing one. Rotation stops being
a synchronized secret change and becomes an overlap between two published keys.

Binding the request target and body digest makes the signature request-specific: a captured
`X-Signature` cannot be replayed against another route, query, or credential, and the
timestamp bounds how long it can be replayed against the same one. The price is a clock.
The Portal must be within P-SIGNATURE-MAX-SKEW of the control plane in either direction or
every exchange fails — which is a new environmental assumption, recorded in 08 §0 rather
than left implicit. The failure is at least loud and correctly classified:
it is a dependency failure, refused retryably, never a claim that the client's credential
is bad.

Amends DC-8; required by ADR-016.
