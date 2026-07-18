# ADR-011 — Portal API errors use a stable two-axis taxonomy

Status: Accepted (2026-07-16)

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

The closed code vocabulary is `malformed_request`, `unknown_dataset`, `not_found`,
`base_block_mismatch`, `no_data`, `overloaded`, `no_workers`, `retries_exhausted`,
`upstream_unavailable`, `not_ready`, `worker_failure`, `internal_error`, and
`unclassified`. A code has exactly one type; its status mapping is fixed by IB-5.

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
bodyless. A 409 response keeps `previousBlocks` at the top level and adds the `error`
object beside it, preserving the recovery contract.

For proxied real-time errors, the Portal classifies the upstream response by status,
rewrites the body into this envelope, and preserves public upstream headers. Successes
and 204 remain streaming pass-through under ADR-003. The source marker records that the
error originated on the real-time path; upstream implementation-specific codes and
bodies are not public Portal API.

Metrics use the same `error_type` and `error_code` values. An error reaching the
middleware without a classification is `unclassified`; an unmatched 4xx is treated as
an invalid request, while an unclassified 5xx remains an `api_error` so it cannot hide.

## Consequences

The wire body is a breaking change for clients that parsed legacy prose or flat
`{"message": ...}` bodies. Codes and types are public API and dashboard vocabulary;
renaming either requires an explicit migration. The status-only mapping of upstream
errors is intentionally lossy, but the Portal now exposes one coherent contract on a
route served by either data source.
