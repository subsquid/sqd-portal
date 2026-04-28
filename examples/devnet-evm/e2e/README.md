# Devnet E2E Artefacts

These files are opt-in test artefacts for validating the hotblocks path without
depending on public RPC endpoints.

They intentionally live under `examples/devnet-evm/e2e/` and do not modify
production source code.

## Portal Hotblocks Path

This path validates the portal code against a deterministic fake HotblocksDB:

```text
fake-hotblocks-db -> sqd-portal -> HTTP client assertions
```

Run:

```bash
./examples/devnet-evm/e2e/run-fake-hotblocks-portal-e2e.sh
```

The script starts the fake HotblocksDB and a local portal process, then asserts
that the portal forwards head/stream requests and resolves block numbers by
timestamp through the hotblocks path.

The portal is started with `dummy-contract-client.json`, so this test does not
connect to SQD Network contracts or public RPC endpoints.

## Fake HotblocksDB

`fake-hotblocks-db.py` is a deterministic stand-in for HotblocksDB. It is useful
for validating a portal instance forwards `/head`, `/finalized-head`, `/status`,
`/stream`, and `/finalized-stream` correctly without needing the real ingestion
containers.

The fake dataset contains two EVM blocks:

```text
block 100, timestamp 1700000000
block 101, timestamp 1700000012
```

Run it directly:

```bash
python3 examples/devnet-evm/e2e/fake-hotblocks-db.py --host 127.0.0.1 --port 19090
```

It serves dataset-scoped endpoints such as:

```text
http://127.0.0.1:19090/datasets/base-mainnet/head
http://127.0.0.1:19090/datasets/base-mainnet/stream
```

Use `portal-hotblocks-only.config.yml` as a minimal portal config template if
you want to run the portal manually against the fake server.

Example direct fake HotblocksDB queries:

```bash
curl -sS http://127.0.0.1:19090/datasets/base-mainnet/head

curl -sS \
  http://127.0.0.1:19090/datasets/base-mainnet/stream \
  -H "Content-Type: application/json" \
  -d '{"fromBlock":100,"toBlock":101}'
```

Example portal timestamp queries when the portal is running on port `18000`
with `portal-hotblocks-only.config.yml`:

```bash
curl -sS http://127.0.0.1:18000/datasets/base-mainnet/timestamps/1700000000/block
curl -sS http://127.0.0.1:18000/datasets/base-mainnet/timestamps/1700000005/block
curl -i  http://127.0.0.1:18000/datasets/base-mainnet/timestamps/1700009999/block
```

Expected block numbers are `100`, `101`, and then `404 Not Found` for the
timestamp outside the fake hotblocks range.
