## Your first query

Stream a few blocks from a dataset. `fromBlock` is required; list the fields you want and the portal
returns only those:

```bash
curl "http://localhost:8000/datasets/ethereum-mainnet/stream" --compressed -d '{
  "type": "evm",
  "fromBlock": 21000000,
  "fields": { "block": { "number": true, "hash": true, "timestamp": true } }
}'
```

The response is newline-delimited JSON — one block per line. Whenever you **resume** a stream, pass
`parentBlockHash` (see [How parentBlockHash works](#description/how-parentblockhash-works)) so the portal
can tell you if the chain reorged while you were away.

For the full query language — filters, the other chain types, and every selectable field — see the
[SQD docs](https://beta.docs.sqd.dev/en/portal/evm/overview).

## Running your own portal

This reference is for consuming data from a portal. If you want to operate your own, the
[project README](https://github.com/subsquid/sqd-portal#initial-configuration) covers configuration and
deployment.
