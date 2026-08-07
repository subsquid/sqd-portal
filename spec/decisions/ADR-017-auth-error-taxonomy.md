# ADR-017 — Authentication and permission errors extend the ADR-011 taxonomy

Status: Proposed (2026-08-05, wording aligned 2026-08-07 with ADR-016's revision; the
decision itself is unchanged)

## Context

ADR-011 fixed a closed two-axis vocabulary: four `type` values, a generated `code` list,
and a code→status binding that IB-5 calls exact. It was written for a Portal that could
not refuse a request on grounds of *who was asking*, so it has no way to say so — and,
because the binding is closed, no status outside the set it enumerates.

ADR-016 makes the Portal refuse on exactly those grounds, with a status that appears
nowhere in IB-5, carrying reasons — missing credential, unknown key, revoked, expired,
wrong portal, wrong dataset — that map onto no existing code.

This is not a theoretical gap. A refusal that names no code is normalized by the
middleware every routed response passes through: an unmatched client error is rewritten
onto `malformed_request` and its bound status, 400. An authentication failure emitted
outside this vocabulary therefore does not merely carry a vague code — it *stops being a
refusal about the credential at all*, and a client cannot distinguish "your key is invalid" from "your query is
malformed". The observability cost matches: every auth refusal lands on the metric as
`error_code="malformed_request"`, indistinguishable from client query errors, so the one
signal an operator needs during a cutover — how much traffic is being turned away, and
why — does not exist.

Extending a closed vocabulary is a binding change under IB-8, so it is a decision, not
an edit.

## Decision

Add two `type` values and six `code` values to the ADR-011 vocabulary.

| Type | Retryable | Meaning |
|---|---|---|
| `authentication_error` | no | The credential is absent, unreadable, or does not authenticate |
| `permission_error` | no | The credential authenticated but does not cover this request |

All six answer **403**, and none carries `WWW-Authenticate`.

| Code | Type |
|---|---|
| `missing_credential` | `authentication_error` |
| `invalid_credential` | `authentication_error` |
| `revoked_credential` | `authentication_error` |
| `expired_credential` | `authentication_error` |
| `portal_not_allowed` | `permission_error` |
| `dataset_not_allowed` | `permission_error` |

**One status for all six, rather than RFC 9110's 401/403 split.** The split is drawn
where valid credentials were presented, which is exactly where a guesser learns something:
a wrong secret answering 401 while a wrong dataset answers 403 puts "your guess was
correct" on the status line, and nothing bounds how often a guess may be made. The status
therefore says only that a credential problem occurred; the code says which, to whoever
already holds the key. AWS takes the same line for the same reason.

The type axis survives the collapse because it answers a different question: whether the
credential itself is the problem or its scope is. A client branching on `type` still
knows whether to present a different key or to ask for a wider one.

Both types are non-retryable: retrying with the same credential cannot succeed, and a
client treating an auth refusal as transient produces exactly the refusal storm ADR-012
exists to prevent. Neither carries `Retry-After`. With no usable grant, an exchange that
could not be made is not an auth verdict: budget saturation remains retryable overload, and
a failed exchange remains retryable upstream unavailability. A renewal suppressed while a
grant remains usable keeps serving that grant (DC-8).

**`invalid_credential` deliberately covers three distinct internal reasons** — a token the
Portal could not parse, a key id the authority does not know, and a key id whose secret
does not match. They are one wire code because distinguishing them turns the endpoint into
an enumeration oracle: a client that learns "this key id exists, wrong secret" has been
told which of its guesses to keep. The operator's need for the distinction is real and is
met on the *internal* axis — protected structured logs (OB-12) — which no client can read.
The keyless metrics surface deliberately keeps the same coarsening as the wire. The
remaining codes are only reachable by someone already holding the right secret, so they can
be specific without leaking anything (INV-39).

`api_error` keeps its meaning: an auth refusal is never an `api_error` and must never
page. The Portal turning away an unauthenticated request is the system working.

## Consequences

IB-5's table gains six rows and the status set gains 403; CT-5 covers them like any other
binding row. `ErrorType` grows the first values that are not about the
Portal's own health, which is the point — the existing four all answer "is this
retryable and does it page?", and both new ones answer no to both.

The `code` vocabulary is public API twice over (wire field and metric label), so these
names are frozen on first release, per ADR-011.

Amends ADR-011; required by ADR-016.
