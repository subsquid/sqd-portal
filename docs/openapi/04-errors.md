## Error handling

Every error the portal returns — on any endpoint, from any data source — uses one envelope:

```json
{
  "error": {
    "type": "rate_limit_error",
    "code": "overloaded",
    "message": "Service is overloaded, please try again later",
    "param": "buffer_size",
    "request_id": "0198c3f1-..."
  }
}
```

Two fields carry the meaning, and they answer different questions:

- **`type`** — the coarse category. Closed set. Branch on this.
- **`code`** — the specific cause. Match on this when you need to handle one case.

`message` is prose for humans. It is not stable and is not part of the contract — do not parse it,
do not match on it. `param` appears when the error is about one request parameter. `request_id`
appears on 5xx; the same id is on every response as the `x-request-id` header, so quote it when
reporting a problem.

> [!NOTE]
> A **204 No Content** is not an error. It is the correct answer when the requested range has no
> blocks yet, and it carries no body, no `type` and no `code`.

### The types

| `type` | Whose fault | Retry the same request? |
|---|---|---|
| `invalid_request_error` | Yours | **No.** The same request reproduces it exactly. Fix the request. |
| `authentication_error` | Yours — the credential | **No.** The same key cannot start working. Present a different one. |
| `permission_error` | Yours — the credential's scope | **No.** The key is real but does not cover this request. |
| `rate_limit_error` | Nobody's — capacity | **Yes**, after the interval in `Retry-After`. |
| `availability_error` | Ours or an upstream's, transiently | **Yes.** Honour `Retry-After` when present; otherwise use your own backoff. |
| `api_error` | Ours — a bug | **No.** Retrying cannot succeed. Report it with `request_id`. |

The split between the last two matters: `availability_error` means a later attempt can still work,
`api_error` means an invariant broke and a retry loop only wastes your time and hides ours.

The two credential types appear only on a portal that requires an API key; a self-hosted or open
portal never returns them.

### Codes

| `code` | `type` | Status | What it means |
|---|---|---|---|
| `malformed_request` | `invalid_request_error` | 400 | The request or query does not parse or does not validate. `param` names the field when one is at fault. |
| `method_not_allowed` | `invalid_request_error` | 405 | Right path, wrong verb. `Allow` lists what the path accepts. |
| `unknown_dataset` | `invalid_request_error` | 404 | No such dataset on this portal. |
| `not_found` | `invalid_request_error` | 404 | No such route or resource. |
| `base_block_mismatch` | `invalid_request_error` | 409 | `parentBlockHash` does not match the canonical parent of the first requested block. The body carries a top-level `previousBlocks` list — see [Blockchain forks](#description/blockchain-forks) for the recovery procedure. |
| `overloaded` | `rate_limit_error` | 529 locally; proxied 429/529 | The portal is at capacity, or the data source refused for capacity. Always carries `Retry-After`. |
| `no_workers` | `availability_error` | 503 | No worker currently holds the requested data. |
| `retries_exhausted` | `availability_error` | 503 | Workers were reachable; every attempt failed transiently until retries ran out. |
| `upstream_unavailable` | `availability_error` | 502 locally; proxied 5xx | A data source the portal depends on is down. A proxied failure keeps the upstream's status. |
| `not_ready` | `availability_error` | 503 | The portal is starting up or draining. Only `/ready` returns this. |
| `worker_failure` | `api_error` | 500 | A worker returned something that cannot be right. |
| `internal_error` | `api_error` | 500 | An invariant the portal owns was violated. |
| `unclassified` | `api_error` | 5xx | An error that escaped classification. Always a bug — please report it. |
| `missing_credential` | `authentication_error` | 401 | No API key was presented. |
| `invalid_credential` | `authentication_error` | 401 | The key is unreadable, unknown, or its secret does not match. The portal does not say which — telling you that a key id exists would help someone guessing them. |
| `revoked_credential` | `authentication_error` | 401 | The key was revoked. |
| `expired_credential` | `authentication_error` | 401 | The key is past its expiry. |
| `portal_not_allowed` | `permission_error` | 403 | The key is not valid on this portal. |
| `dataset_not_allowed` | `permission_error` | 403 | The key does not cover the requested dataset — including a dataset-scoped key on a route that names no dataset. |

Codes are added over time. Treat an unknown `code` as its `type`, which is why the two axes exist:
a client that branches on `type` keeps working when a new code appears.

### Backing off

`Retry-After` is mandatory on every `overloaded` response and is always at least 1 second. Honour it:

```text
HTTP/1.1 529
Retry-After: 10

{"error":{"type":"rate_limit_error","code":"overloaded","message":"..."}}
```

For `overloaded`, the value is in seconds, never an HTTP date. When the portal proxies a refusal
from a data source, it forwards that source's interval if it is usable and substitutes its own floor
otherwise.

An `upstream_unavailable` response can also preserve a `Retry-After` supplied by the data source.
Honour it when present; unlike an overload hint, the portal does not invent or normalize it.

No `authentication_error` or `permission_error` carries `Retry-After`, because no interval would
make the same credential work.

### Presenting a key

On a portal that requires one, send the key as `Authorization: Bearer <key>`. That is the only
channel — a key in the query string is ignored, because a URL ends up in browser history, in
`Referer`, and in the logs of every proxy along the way. Every 401 carries
`WWW-Authenticate: Bearer`.

### Browser clients

`Retry-After` and `x-request-id` are exposed via CORS, alongside the stream metadata headers, so all
of them are readable from JavaScript. Without that they would be invisible to a browser — the Fetch
safelist for response headers contains none of ours — and a fetch client would see a status and
nothing else.
