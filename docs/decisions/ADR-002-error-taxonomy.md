# ADR-002: Portal API error taxonomy

| Status | Date |
|--------|------|
| Accepted | 2026-07-16 |

<!-- Status values: Proposed · Accepted · Deferred · Superseded by ADR-XXX · Deprecated -->

Companion to the hotblocks error hardening in the `data` repo
([546c1ac](https://github.com/subsquid/data/commit/546c1acb6594bccc7399d2e67c4d23a46a9c2fd3)),
which classified that service's errors internally. This record covers the portal side,
which is the public API and therefore makes different trade-offs.

## Context

The portal had a taxonomy in name only. `RequestError::short_code()` mapped each variant
to a string — and that string reached exactly one place: an argument to a `tracing::debug!`
in `controller/stream.rs`. It was on no metric, no header, no body. Consequences:

- **Errors were invisible to alerting.** `portal_http_status` carried `endpoint`, `status`
  and `data_source`. A 503 meant "something", with no way to distinguish "no workers exist"
  from "a worker query task panicked" without reading logs.
- **Transient and integrity failures were the same variant.** `RequestError::InternalError`
  was returned both when all retries legitimately ran out *and* when a portal invariant
  broke (a chunk that must exist wasn't found; a worker task panicked; a worker returned
  blocks below the queried range). Both rendered 503, so the portal told clients to retry
  its own bugs — an infinite retry loop that also hid the bug.
- **Clients had nothing to match on.** Bodies were `text/plain` prose on stream endpoints
  and `{"message": ...}` JSON on the timestamp and SQL endpoints. The only machine-readable
  discriminant was the status code, which is many-to-one over causes.
- **The OpenAPI spec described a body nothing emitted.** `openapi::ErrorResponse` was
  registered but referenced by no path; the type actually serialized was
  `types::GenericError`, which was registered nowhere.

Two mis-mappings fell out of the audit and are fixed here: SQL returned **400** when no
worker held a chunk and when its own schema files failed to read/parse; the timestamp
resolver returned **503** when hotblocks rejected a query the *portal* had generated.

## Decision

### Two axes, after Stripe

Errors are modelled on [Stripe's error object](https://docs.stripe.com/api/errors): a
coarse `type` to branch on, and a specific `code` to match. `ErrorType` has four values,
`ErrorCode` thirteen. Both live in `src/types/errors.rs`; `ErrorCode::error_type()` is the
only mapping between them.

| `type` | Retryable | Meaning | Pages |
|---|---|---|---|
| `invalid_request_error` | no | The request is wrong; replaying it fails identically | no |
| `rate_limit_error` | yes | Capacity exhausted; back off | no |
| `availability_error` | yes | Nothing is broken, the data/worker isn't there yet | no |
| `api_error` | no | An invariant we own broke | **yes** |

`api_error` is the only type that should page. This is the FM-2 "transient vs integrity"
split from the hotblocks spec, wearing Stripe's vocabulary.

| `code` | `type` | Status |
|---|---|---|
| `malformed_request` | invalid_request_error | 400 |
| `unknown_dataset` | invalid_request_error | 404 |
| `not_found` | invalid_request_error | 404 |
| `base_block_mismatch` | invalid_request_error | 409 |
| `no_data` | availability_error | 204 |
| `overloaded` | rate_limit_error | 429 / 529 |
| `no_workers` | availability_error | 503 |
| `retries_exhausted` | availability_error | 503 |
| `upstream_unavailable` | availability_error | 502 / 503 |
| `not_ready` | availability_error | 503 |
| `worker_failure` | api_error | 500 |
| `internal_error` | api_error | 500 |
| `unclassified` | api_error | — |

`RequestError::InternalError` is split into `RetriesExhausted` (503, transient) and
`Internal` (500, integrity). The four sites that meant "bug" now return 500.

### Wire format (breaking)

Error bodies become the Stripe envelope on every endpoint:

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

`code` and `type` are stable and matchable. `message` is prose and may change — clients
must not parse it. `param` appears only on parameter validation (`buffer_size`,
`timeout_quantile`, `retries`, `max_chunks`). `request_id` echoes `x-request-id` and is
stamped in by the logging middleware, which is the only layer that knows it.

Two responses deviate, both deliberately:

- **204** carries no body, so `no_data` is conveyed by the status alone.
- **409** keeps `previousBlocks` at the **top level**, with the `error` object beside it.
  Stripe would nest it, but `previousBlocks` is the documented reorg-recovery contract that
  clients walk to find a shared ancestor. Breaking it to gain purity is a bad trade; the
  envelope is additive there.

### Observability

`portal_http_status` and `portal_http_seconds_to_first_byte` gain `error_code` and
`error_type`, carrying the wire's `code` and `type`. The labels are prefixed because a bare
`type` label says nothing on a metric; the JSON field names are unaffected. They are
attached to **error responses only**, so success-path series keep their existing label set
and cardinality. `error_type` exists as a label so alert rules can say
`error_type="api_error"` instead of maintaining a regex over codes in Grafana — the mapping
stays in code.

Any error response reaching the middleware without a code is counted as `unclassified`
rather than dropped: no failure is invisible. An unclassified **4xx** is re-typed
`invalid_request_error` (a client hitting a bad route or verb); only an unclassified 5xx
stays `api_error`. Without that, a stray 405 would page someone.

`not_ready` exists for the same reason: `/ready` returns 503 on every rolling deploy, and
unclassified it would have read as an integrity fault.

### Proxied hotblocks errors

The portal classifies forwarded hotblocks responses **by status alone**
(`ErrorCode::from_upstream_status`). Hotblocks keeps its own `ErrorCode` in response
extensions, which never cross the wire, so its five distinct 400-classes
(`MALFORMED_REQUEST`, `RANGE_UNAVAILABLE`, `ITEM_UNAVAILABLE`, `KIND_MISMATCH`,
`UNSUPPORTED_QUERY`) collapse into `malformed_request` here. The `data_source` label marks
these as hotblocks-sourced.

`forward_response` rewrites upstream error bodies into the portal envelope, because a
single `/stream` request may be served by either data source and the same endpoint must not
emit two body shapes. Upstream headers (`retry-after`, `x-sqd-*`) are preserved; 204 streams
through untouched so its `x-sqd-finalized-head-*` headers survive.

## Consequences

- **Breaking.** Any client parsing today's `text/plain` error bodies, or reading the flat
  `{"message": ...}` JSON, must move to `error.message`. Status codes are unchanged except
  the integrity cases noted above (503 → 500) and the two mis-mappings (400 → 503/500).
- Codes and types are public API in two places at once — a rename silently breaks both
  client matches and dashboards. `the_wire_vocabulary_is_frozen` in `types/errors.rs` pins
  every string; changing it should mean a deliberate migration.
- The portal diverges from hotblocks' `SCREAMING_SNAKE` metric labels. A dashboard cannot
  union the two services' label values directly. Accepted: hotblocks' label is internal,
  the portal's is public and should read like the rest of its API, and the portal's view of
  hotblocks errors is status-derived and lossy anyway.
- `doc_url` is deliberately omitted. It is the remaining piece of Stripe's model worth
  adding, once per-code docs pages exist; the codes are stable, so the URLs are derivable.
- Axum's unmatched-route 404s never reach the middleware (it is a `route_layer`), so they
  are absent from `portal_http_status` entirely rather than counted as `unclassified`. The
  catch-all covers extractor rejections inside matched routes. Pre-existing; not addressed
  here.
