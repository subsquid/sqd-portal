# 14 — Interface binding (HTTP)

The only normative doc naming the concrete surface — routes, codes, headers,
encodings — as *observable contract*, still no internals. **Anything not specified
here is unspecified: clients and tests must not pin it** (IB-8).

**IB-1 — Transport generalities.** HTTP/1.1+; permissive CORS (any origin/method/
header). Request bodies may be gzip-compressed. Every response carries `x-request-id`
(client's value echoed verbatim if supplied — any bytes tolerated, REQ-21 — else a
generated UUID). Stream responses are chunked `application/jsonl`, compressed:
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
| — | `/docs`, `/api-docs/openapi.json` | self-description (REQ-32) |
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
`message` is not stable; clients match `type` and `code`. The exact closed mapping is:

| Wire `type` / `code` | Status | Body / headers |
|---|---|---|
| `invalid_request_error` / `malformed_request` | 400 | envelope; `param` only for parameter validation |
| `invalid_request_error` / `unknown_dataset` | 404 | envelope |
| `invalid_request_error` / `not_found` | 404 | envelope |
| `invalid_request_error` / `base_block_mismatch` | 409 | envelope plus top-level `previousBlocks` (≥ 1 entry, ascending, ending at the parent height; archival path currently lacks the richer-ancestor SHOULD — GAP-7) |
| `availability_error` / `no_data` | 204 | no body; head-marker headers per IB-4; after ≥ P-NO-DATA-DELAY. Also the beyond-frontier outcome of OP-5 (ADR-014) |
| `rate_limit_error` / `overloaded` | 529 for Portal-local refusal; a proxied 429/503/529 retains its public status | `Retry-After` always present and ≥ P-RETRY-AFTER-MIN (INV-26): Portal-set locally, preserved from the upstream when proxied, injected at P-RETRY-AFTER-MIN when the upstream omitted it (ADR-014); other public upstream headers preserved |
| `availability_error` / `no_workers` | 503 | envelope; no `Retry-After` |
| `availability_error` / `retries_exhausted` | 503 | envelope |
| `availability_error` / `upstream_unavailable` | 502 for a local upstream failure; proxied upstream failures retain their status | envelope; public upstream headers retained |
| `availability_error` / `not_ready` | 503 | envelope |
| `api_error` / `worker_failure` | 500 | envelope |
| `api_error` / `internal_error` | 500 | envelope |
| `api_error` / `unclassified` | contextual 5xx | envelope; must be counted and investigated |

Client recovery from 409 (normative): scan `previousBlocks` newest-first for the last
block also on the client's chain; re-request from its number + 1 with its hash as
`parentBlockHash`; if none matches, re-request from an earlier block. Timestamp
resolve (OP-5) uses the same envelope and code vocabulary.

**IB-6 — Status & introspection surfaces.** `/ready`: 200 or 503 with the ADR-011
`not_ready` envelope (OB-5). `/metrics`: OpenMetrics; families under the `portal_` prefix with a constant
portal-identity label. `/status`, `/state`, `/debug/*`: bodies explicitly unstable
(REQ-14).

**IB-7 — Input-side binding (what harness stubs implement).** Worker stub (DC-1): the
peer-to-peer query protocol — signed query in, sized/compressed result or typed error
verdict out. Publisher stub (DC-2): the network-state document (artifact identifier,
effective-from, artifact URL) plus the artifact blob. Registry stub (DC-3): catalog
and metadata documents. Real-time stub (DC-4): `/head`, `/finalized-head`, `/status`,
`/stream`, `/finalized-stream` under `datasets/{name}`, honoring the portal-supplied
client-identity header, emitting head headers and `x-internal-*` noise for
strip-testing, plus every 4xx/5xx/409 shape needed to assert error normalization. RPC
stub (DC-5): the contract read set. Log-sink stub (DC-6) and error-report stub (DC-7):
fire-and-forget receivers with ledgers, so egress audits (INV-37) and drop accounting
(HZ-7) have ground truth. Stubs double as fault
injectors for the CT-2 matrix.

**IB-8 — Versioning rule.** Any change to this binding (route, code, header, schema,
taxonomy) updates this file and the interface-conformance class CT-5 in the same
change; additive changes note the version they appeared in.
