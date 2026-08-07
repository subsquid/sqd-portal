# ADR-011 — Portal API errors use a stable two-axis taxonomy

Status: Accepted (2026-07-16); extended 2026-08-05 to cover authentication and
permission errors

## Context

The Portal historically exposed prose bodies in several incompatible shapes and used
the same 503 response for transient exhaustion and Portal-owned invariant failures.
Clients could branch only on an overloaded status code, while metrics could not
distinguish causes. The real-time proxy made this worse by exposing the upstream
service's error body on the same route that emitted Portal errors.

## Decision

Model public errors on two stable axes: a coarse `type` for retry/page policy and a
specific `code` for client handling.

| `type` | Retryable | Meaning | Pages |
|---|---|---|---|
| `invalid_request_error` | no | The request cannot succeed unchanged | no |
| `rate_limit_error` | yes | Portal or upstream capacity is exhausted | no |
| `availability_error` | yes | Data or a dependency is temporarily unavailable | no |
| `api_error` | no | A Portal-owned invariant failed | yes |
| `authentication_error` | no | The credential is absent, unreadable, or does not authenticate | no |
| `permission_error` | no | The credential authenticated but does not cover this request | no |

The closed code vocabulary is `malformed_request`, `method_not_allowed`,
`unknown_dataset`, `not_found`, `base_block_mismatch`, `overloaded`, `no_workers`,
`retries_exhausted`, `upstream_unavailable`, `not_ready`, `worker_failure`,
`internal_error`, `unclassified`, `missing_credential`, `invalid_credential`,
`revoked_credential`, `expired_credential`, `portal_not_allowed`, and
`dataset_not_allowed`. A code has exactly one type; its status mapping is fixed by IB-5.

The last six belong to the two credential types — the first four to
`authentication_error`, the last two to `permission_error` — and are reachable only where
an operator configured commercial access control.

The taxonomy covers failures only. A 204 is the correct answer to a range that is not
produced yet, so it has no `type` and no `code`: it carries no body, so a code could
never reach a client, and on the metric it would only have restated `status="204"` while
making the steady state of every polling client read as an `availability_error`.

Every body-bearing error uses this envelope:

```json
{
  "error": {
    "type": "rate_limit_error",
    "code": "overloaded",
    "message": "Service is overloaded, please try again later",
    "param": "buffer_size",
    "request_id": "65dae20d-32bb-4728-afae-33d9fa892e99"
  }
}
```

`message` is unstable prose. `param` and `request_id` are optional. A 204 response is
bodyless and untyped. A 409 response keeps `previousBlocks` at the top level and adds
the `error` object beside it, preserving the recovery contract.

`request_id` is emitted on 5xx only. Every response carries it as `x-request-id`
regardless (REQ-9); the body copy exists for the support flow, where a user pastes JSON
and loses the headers. A 4xx is the client's own fault and is handled programmatically —
a 409 is a routine reorg resolved from `previousBlocks` — so nobody opens a ticket
holding one, and the copy would cost a re-render of the whole body, siblings included.

One renderer builds this body for both emitters — locally produced errors and rewritten
upstream ones. That is a correctness requirement, not a tidiness one: the two emitters
previously drifted, and 409, the one status carrying a top-level sibling, is served by
both data sources.

For proxied real-time errors, the Portal classifies the upstream response by status and
rewrites the body into this envelope, preserving public upstream headers. One classifier
serves every surface that talks to the real-time source, not just the stream proxy: an
upstream status cannot mean one thing on `/stream` and another on the timestamp route. The upstream's
own body is never published — its prose is not public API and can name instances, paths
and internal ids — and the envelope carries the code's own message instead.

The body is not read either, on any status but 409. Nothing consumes it: an upstream
server error is already logged with the pod that served it, and the upstream logs its own
failures in full, so copying a fragment here would duplicate diagnostics that already
exist where they are complete — while buffering an unbounded body would let an upstream
fault size Portal memory. `previousBlocks` on a 409 is the one upstream field that
survives, because clients walk it to recover, and it is the only reason to read at all.

That field is parsed into a non-empty typed list, not forwarded as whatever value sits
under the key. It is the one place an upstream can put bytes in front of a client, so an
upstream must not be able to put something else there — a scalar, an empty list, internal
data — and an empty list strands the client exactly as a missing one does. A body past
the read cap is refused rather than truncated: half a chain slice would resume the client
from an ancestor that is not the deepest one. A 409 left with no usable list is not a
legal 409 under IB-5 and is indistinguishable from a healthy reorg on the wire and on the
metric, so it is logged at `error` — the only witness. Whether it should instead be
refused as `unclassified` is open (GAP-28).

Successes and 204 remain streaming pass-through under ADR-003. The source marker records
that the error originated on the real-time path; upstream implementation-specific codes
and bodies are not public Portal API.

### Credential refusals

A Portal configured for commercial access control (REQ-50..REQ-56) refuses on grounds of
*who is asking* — a ground the original four types could not express. Without their own
vocabulary such a refusal is normalized by the middleware onto `malformed_request` and
its bound 400, so a client cannot tell "your key is invalid" from "your query is
malformed", and every auth refusal lands on the metric as a client query error — erasing
the one signal an operator needs during a cutover.

All six credential codes answer **403**, and none carries `WWW-Authenticate`.

**One status for all six, rather than RFC 9110's 401/403 split.** The split is drawn
where valid credentials were presented, which is exactly where a guesser learns something:
a wrong secret answering 401 while a wrong dataset answers 403 puts "your guess was
correct" on the status line, and nothing bounds guessing on a snapshot hit. The status
therefore says only that a credential problem occurred; the code says which, to whoever
already holds the key.

The type axis survives the collapse because it answers a different question: whether the
credential itself is the problem or its scope is. A client branching on `type` still
knows whether to present a different key or to ask for a wider one.

Both credential types are non-retryable: retrying with the same credential cannot succeed,
and a client treating an auth refusal as transient produces exactly the refusal storm
ADR-012 exists to prevent. Neither carries `Retry-After`. A snapshot miss that could not
be resolved is not an auth verdict: lookup saturation remains retryable overload, and
lookup failure remains retryable upstream unavailability (DC-8).

**`invalid_credential` deliberately covers four distinct internal reasons** — a token the
Portal could not parse, a key id it does not know, a key id whose secret does not match,
and a record carrying no digest with which to prove the secret. They are one wire code
because distinguishing them turns the endpoint into an enumeration oracle: a client that
learns "this key id exists, wrong secret" has been told which of its guesses to keep. The
operator's need for the distinction is met on the *internal* axis — protected structured
logs (OB-12) — which no client can read, and the keyless metrics surface keeps the same
coarsening as the wire. The remaining codes are only reachable by someone already holding
the right secret, so they can be specific without leaking anything (INV-39). A digestless
revoked or malformed tombstone cannot establish that fact and therefore remains
`invalid_credential`.

`api_error` keeps its meaning: an auth refusal is never an `api_error` and must never
page. The Portal turning away an unauthenticated request is the system working.

### Field casing

Envelope keys are snake_case: `request_id` is the only multi-word one, and it matches the
other Portal-emitted fields (`portal_version`, `start_block`, `block_number`). `code` and
`type` values are identifiers rather than prose and double as metric label values, where
snake_case is the convention.

`param` is the exception by construction — it echoes the offending field's wire name
exactly as the client sent it, so snake_case for query parameters (`buffer_size`) and
camelCase for body fields (`fromBlock`). It points into the request rather than following
a convention of its own — so an otherwise snake_case `error` object can carry
`"param": "fromBlock"`, and a 409 sets `previousBlocks` beside it. That is compatibility,
not design.

**Unsettled.** None of this ratifies a casing convention for the API. It records what
Portal-owned JSON does today, not a decision that it should stay. Do not cite it as
precedent when adding fields elsewhere; settling the question is a breaking change owed
its own migration.

## Observability

Metrics use the same `error_type` and `error_code` values. An error reaching the
middleware without a classification is `unclassified`; an unmatched 4xx is treated as
an invalid request, while an unclassified 5xx remains an `api_error` so it cannot hide.

Errors the router raises before a handler runs — a path segment that will not parse, an
unreadable query, an over-limit body — are rebuilt into the envelope at the middleware
rather than at each extractor. A bad path parameter is the commonest client mistake there
is, so "one envelope on every endpoint" is false without it, and converting call sites
one at a time leaves the next one to be found by a client. A 4xx is named
`malformed_request` and answers **400**: the code→status mapping above is closed, so a
rejection the framework happened to answer with 413 or 415 cannot keep that status while
claiming a code bound to 400 — the specifics stay in `message`. A 5xx keeps
`unclassified` and its status, and still pages, since the router failing on its own is
not the client's fault to name.

A wrong verb on an existing route is the exception, and gets its own code bound to
**405**. Folding it into `malformed_request` would be wrong on both axes: the request is
well-formed, and the answer is `Allow`, which a 400 has nowhere to carry. It is also the
one rejection the framework raises with an empty body, so the collapsed form left the
client holding a bare "Bad request" — the shape this taxonomy replaced.

The rebuilt response carries the original's extensions, not only its headers: the
endpoint label lives there, and without it the metric falls back to the raw request path,
which on a rejection is client-supplied. A malformed dynamic path would then mint an
`endpoint` label per request.

`doc_url` is deliberately omitted from the envelope. It is the remaining field worth
adding once per-code documentation pages exist; the codes are stable, so the URLs are
derivable then without a second migration.

## Consequences

The wire body is a breaking change for clients that parsed legacy prose or flat
`{"message": ...}` bodies. Codes and types are public API and dashboard vocabulary;
renaming either requires an explicit migration. The status-only mapping of upstream
errors is intentionally lossy, but the Portal now exposes one coherent contract on a
route served by either data source.

The credential codes put 403 in IB-5's status set and six rows in its table; CT-5 covers
them like any other binding row. They are the first types that are not about the Portal's
own health — the original four all answer "is this retryable and does it page?", and both
credential types answer no to both.
