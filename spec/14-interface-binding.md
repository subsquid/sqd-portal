# 14 — Interface binding (HTTP)

The only normative doc naming the concrete surface — routes, codes, headers,
encodings — as *observable contract*, still no internals. **Anything not specified
here is unspecified: clients and tests must not pin it** (IB-8).

**IB-1 — Transport generalities.** HTTP/1.1+; permissive CORS (any origin/method/
header), exposing `Retry-After`, `x-request-id` and the `x-sqd-*`
stream metadata —
allowing an origin does not make a response header readable, and the CORS-safelisted set
contains none of ours, so a browser client would otherwise see the status and nothing
else. Request bodies may be gzip-compressed. Every response carries `x-request-id`
(client's value echoed verbatim when it is ASCII; a non-ASCII value is rejected with
400 `malformed_request` and a generated UUID; an absent value also gets a generated
UUID). Stream responses are chunked `application/jsonl`, compressed:
`Content-Encoding: gzip` by default, `zstd` when the client offers it via
`Accept-Encoding` (or `X-Forwarded-Accept-Encoding`).

**IB-2 — Operation → endpoint map.**

| OP | Method & path | Notes |
|---|---|---|
| OP-1 | `POST /datasets/{alias}/stream` · `/finalized-stream` | real-time / finalized modes |
| OP-1 (restricted variants) | `POST /datasets/{alias}/archival-stream` (+`/debug`) | operator/internal; `/debug` bypasses clamps and is operator-gated, disabled by default (ADR-014; GAP-21 until gated) |
| OP-2 | `GET /datasets/{alias}/head` · `/finalized-head` · `/archival-head` | |
| OP-3 | `GET /datasets/{alias}/metadata` (alias: `GET /datasets/{alias}` — undocumented, GAP-11) | `?expand[]=` |
| OP-4 | `GET /datasets` | |
| OP-5 | `GET /datasets/{alias}/timestamps/{ts}/block` | |
| OP-6 | `GET /datasets/{alias}/state` | internal |
| OP-7 | `GET /status` | internal, unstable body |
| OP-8 | `GET /ready` | 200 / 503 + reason |
| OP-9 | `GET /metrics` | OpenMetrics text |
| OP-10 | `POST /sql/query` · `GET /sql/metadata` | build-time capability; experimental |
| OP-11 | — (admission step on every gated route, IB-9) | commercial deployments only |
| — | `/docs`, `/api-docs/openapi.json` | self-description (REQ-32); never gated |
| — | deprecated: `/height` variants, `/{start_block}/worker`, `/query/{worker_id}`, `/debug/*` | NG7 — unspecified, don't pin |

**IB-3 — Stream request.** Query string: `buffer_size` (int ≥ 1, default
P-BUFFER-DEFAULT, clamped to P-BUFFER-MAX), `max_chunks` (int ≥ 1, clamped to
P-MAX-CHUNKS-PER-STREAM), `timeout_quantile`, `retries` (currently ignored on public
routes — GAP-8/OQ-1). Body (JSONC sketch; chain dialects add entity filters):

```jsonc
{
  "type": "evm",                 // dialect tag: bitcoin|evm|solana|substrate|fuel|tron|...
  "fromBlock": 10000000,         // first block; omission currently = 0 (OQ-7)
  "toBlock": 10001000,           // optional inclusive upper bound
  "parentBlockHash": "0x…",      // optional continuation anchor (DEF-9)
  "fields": { "block": { "number": true, "hash": true } },
  "includeAllBlocks": false,
  "transactions": [ … ], "logs": [ … ]   // dialect-specific item filters, ≤ P-QUERY-MAX-ITEMS total
}
```

Unknown fields → 400. Body > P-QUERY-SIZE-LIMIT → 400.

**IB-4 — Stream response.** Headers: `x-sqd-head-number`, `x-sqd-finalized-head-number`,
`x-sqd-finalized-head-hash` (INV-24), `x-sqd-data-source: network|real_time` (DEF-6;
undocumented in the served description — GAP-11). The head-marker headers appear on
every successful response including 204 EMPTY (INV-24, INV-27). The source header
appears on routed responses — successful, 204 served after source selection, or failed
after source selection — and not on validation/admission/alias failures or a no-source
EMPTY (DEF-6). Body: one JSON block record per line, per INV-20/21/22/25/29. The last line is always
the coverage boundary (INV-29) — as is each served chunk's boundary, so a multi-chunk
selective body carries interior header-only lines (FV-6) — hence DEF-8's coverage cursor is
the last record; there is no dedicated cursor field or header, by design (DEF-8). Successful and 204 real-time responses stream through
with all `x-internal-*` headers stripped; proxied error bodies are normalized under
ADR-011. Completion vs truncation is not distinguished in-band (ADR-001, OQ-2).

**IB-5 — Error mapping.** Each body-bearing error uses the ADR-011 envelope
`{"error":{"type":…, "code":…, "message":…, "param"?:…, "request_id"?:…}}`.
`message` is not stable; clients match `type` and `code`. `request_id` appears on 5xx
only; every response carries the same id as `x-request-id` (REQ-9, ADR-011). The exact
closed mapping is:

| Wire `type` / `code` | Status | Body / headers |
|---|---|---|
| `invalid_request_error` / `malformed_request` | 400 | envelope; `param` only for parameter validation |
| `invalid_request_error` / `method_not_allowed` | 405 | envelope; `Allow` preserved. The one rejection status that is *not* normalized to 400: the request is well-formed, the verb is the fault, and `Allow` is the recovery a 400 has nowhere to carry |
| `invalid_request_error` / `unknown_dataset` | 404 | envelope |
| `invalid_request_error` / `not_found` | 404 | envelope |
| `invalid_request_error` / `base_block_mismatch` | 409 | envelope plus top-level `previousBlocks` (≥ 1 entry, ascending, ending at the parent height; archival path currently lacks the richer-ancestor SHOULD — GAP-7). A proxied list is typed and non-empty or it is not published at all (GAP-28) |
| `rate_limit_error` / `overloaded` | 529 for Portal-local refusal; a proxied 429/529 retains its public status | `Retry-After` always present and ≥ P-RETRY-AFTER-MIN (INV-26): Portal-set locally, preserved from the upstream when proxied *and* readable as seconds at or above the floor, replaced at P-RETRY-AFTER-MIN otherwise — omitted, `0`, non-numeric, or the RFC's HTTP-date form, which this header is not documented to carry (ADR-014); other public upstream headers preserved |
| `availability_error` / `no_workers` | 503 | envelope; no `Retry-After` |
| `availability_error` / `retries_exhausted` | 503 | envelope |
| `availability_error` / `upstream_unavailable` | 502 for a local upstream failure; proxied upstream failures retain their status, **503 among them** — it is unavailability, not congestion (ADR-007, ADR-014) | envelope; public upstream headers retained, a `Retry-After` the upstream sent included; none is invented, since a hint on a dependency that is down rather than busy just aims the client back at it. A refusal the Portal already classified is carried whole, not re-labelled here: capacity and congestion are the Portal's own faults, not the upstream's |
| `availability_error` / `not_ready` | 503 | envelope |
| `api_error` / `worker_failure` | 500 | envelope |
| `api_error` / `internal_error` | 500 | envelope |
| `api_error` / `unclassified` | contextual 5xx | envelope; must be counted and investigated |
| `authentication_error` / `missing_credential` | 403 | envelope; no `Retry-After`, no challenge |
| `authentication_error` / `invalid_credential` | 403 | envelope; identical byte-for-byte apart from the request id whether the token was unparseable, named an unknown key, carried a wrong secret, or named a digestless tombstone (INV-39) |
| `authentication_error` / `revoked_credential` | 403 | envelope |
| `authentication_error` / `expired_credential` | 403 | envelope |
| `permission_error` / `portal_not_allowed` | 403 | envelope |
| `permission_error` / `dataset_not_allowed` | 403 | envelope |

The last six rows appear only on a commercial deployment (REQ-56) and only on a gated
route (IB-9). None is retryable and none carries a retry hint: retrying with the same
credential cannot succeed, and treating an auth refusal as transient reproduces the
ADR-012 refusal storm. All six share one status, so the status line never reveals that a
presented secret was the right one (ADR-011); none carries a challenge. None is an
`api_error`, so none pages. A snapshot miss that could not be resolved is not one of these
six rows: lookup-budget exhaustion maps to retryable `overloaded`, and lookup failure to
retryable `upstream_unavailable` (DC-8).

EMPTY (204) is not in this table: it is a success, not a refusal, and carries no
`type`/`code`. It is bound by IB-4 — no body, head-marker headers, emitted after
≥ P-NO-DATA-DELAY, and the beyond-frontier outcome of OP-5 (ADR-014).

Client recovery from 409 (normative): scan `previousBlocks` newest-first for the last
block also on the client's chain; re-request from its number + 1 with its hash as
`parentBlockHash`; if none matches, re-request from an earlier block. Timestamp
resolve (OP-5) uses the same envelope and code vocabulary.

**IB-6 — Status & introspection surfaces.** `/ready`: 200 or 503 with the ADR-011
`not_ready` envelope (OB-5). `/metrics`: OpenMetrics; families under the `portal_` prefix
with a constant portal-identity label. On commercial deployments the keyless scrape obeys
12's confidentiality rule: no internal auth rung or lookup detail beyond the public wire
outcome. `/status`, `/state`, `/debug/*`: bodies explicitly unstable (REQ-14).

**IB-7 — Input-side binding (what harness stubs implement).** Worker stub (DC-1): the
peer-to-peer query protocol — signed query in, sized/compressed result or typed error
verdict out. Publisher stub (DC-2): the network-state document (artifact identifier,
effective-from, artifact URL) plus the artifact blob. Registry stub (DC-3): catalog
and metadata documents. Real-time stub (DC-4): `/head`, `/finalized-head`, `/status`,
`/stream`, `/finalized-stream` under `datasets/{name}`, honoring the portal-supplied
client-identity header, emitting head headers and `x-internal-*` noise for
strip-testing, plus every 4xx/5xx/409 shape needed to assert error normalization. RPC
stub (DC-5): the contract read set. Control-plane stub (DC-8): the cursor-paged key feed
with its four-field envelope, plus the single-key lookup — and, as a fault injector, the
epoch flip, head rollback, non-advancing cursor, short-of-head page, malformed record and
unrecognized status the DC-8 error table names (⚠ unbuilt — GAP-33). Log-sink stub (DC-6) and error-report stub (DC-7):
fire-and-forget receivers with ledgers, so egress audits (INV-37) and drop accounting
(HZ-7) have ground truth. Stubs double as fault
injectors for the CT-2 matrix.

**IB-9 — Authorization binding.** Commercial deployments only (REQ-56); on any other
deployment nothing in this rule is observable.

*Presentation.* A credential is accepted as `Authorization: Bearer <token>` and nowhere
else. A query-string channel is deliberately not offered: it would exist to serve
transports that cannot set headers, and this binding has none — every gated route is POST
but the timestamp lookup, and `fetch` sets headers on both. What it would have is a secret
in a URL, which browser history, `Referer` and every intermediary's access log record
outside this system's reach.

*Token grammar.* `<prefix><key_id>_<secret>`, where the prefix is one the control plane
mints, the two segments draw from `[A-Za-z0-9~-]`, and their lengths are at most
P-KEY-ID-MAX-LEN and P-KEY-SECRET-MAX-LEN. A token outside this grammar is refused as
`invalid_credential` without a lookup — it is not something the control plane could have
issued (REQ-52).

*Gated surface.* The gated set is the stream routes, the timestamp-to-block lookup, the
direct worker query, and the SQL query plan (DEF-19). Every other route in IB-2 answers
without a credential on every deployment (NG6): the catalog, per-dataset metadata and
state, all head and height variants, `/status`, the worker lookup, the debug surfaces,
`/ready`, `/metrics`, `/api-docs/openapi.json` and the docs UI. A route that states
neither does not compile (REQ-51).

Because `/metrics` is deliberately keyless, its commercial representation is part of the
authorization boundary rather than an exemption from it: it must not reveal an internal
authorization rung, a key-record count, or whether one request hit the snapshot or invoked
authorize-on-miss beyond what that request's own wire outcome already disclosed
(OB-12/13, INV-39).

*Interaction with the envelope.* An auth refusal is emitted in the IB-5 envelope with the
codes above. It must reach the client with the status IB-5 binds to its code — a refusal
carrying no code is normalized onto `malformed_request` at 400 by the same rule that
catches framework rejections, which would erase the distinction this rule exists to make.
Emitting through the envelope is what prevents that, and CT-5 pins it behind the real
middleware stack rather than at the gate alone.

**IB-8 — Versioning rule.** Any change to this binding (route, code, header, schema,
taxonomy) updates this file and the interface-conformance class CT-5 in the same
change; additive changes note the version they appeared in.
