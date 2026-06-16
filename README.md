# SQD Portal

SQD Portal is a Rust service that exposes blockchain data over a streaming HTTP API, backed by [SQD Network](https://docs.sqd.dev/en/network).

## What it is

A Portal sits between data consumers and SQD Network, a decentralized data lake served by worker, gateway, and scheduler nodes. It accepts dataset queries over HTTP, fans them out to network workers, validates responses, and returns the results as a continuous stream. SQD Portal is the data API layer of [SQD](https://sqd.dev/portal), the open data platform for Web3.

## Data sources

Portal serves two kinds of data and merges them in a single stream:

- **Historical data from SQD Network.** Static, finalized blockchain data served by the network's workers. Accessing the network requires locking SQD tokens to the Portal's peer id in the onchain contract (for example, via the [Network app](https://network.subsquid.io/portal)). Locked stake determines the Portal's share of network bandwidth.
- **Real-time data from HotblocksDB.** Recent blocks near the chain head, streamed from a separate [HotblocksDB](https://github.com/subsquid/data/tree/master/crates/hotblocks) service.

To serve only real-time data for an existing chain or a local devnet, see the [devnet EVM example](./examples/devnet-evm/README.md).

## HTTP API

The server (axum) exposes per-dataset endpoints, including:

- `POST /datasets/:dataset/stream` and `POST /datasets/:dataset/finalized-stream`: query and stream blocks.
- `GET /datasets/:dataset/head`, `/finalized-head`, `/state`, `/metadata`: dataset status and metadata.
- `GET /datasets/:dataset/timestamps/:timestamp/block`: resolve a timestamp to a block number.
- `GET /status`, `GET /datasets`: Portal and dataset listing.
- `GET /metrics` (Prometheus), `GET /ready` (readiness probe).
- `/docs` (Scalar) and `/swagger-ui` with the OpenAPI spec at `/api-docs/openapi.json`.

A SQL query endpoint (`POST /sql/query`, `GET /sql/metadata`) is available when the crate is built with the `sql` feature.

Query examples are in the [Portal docs](https://docs.sqd.dev/en/portal/evm/overview).

## Configuration

Copy or symlink an example env file to `.env`:

```bash
ln -s mainnet.env .env   # mainnet
ln -s tethys.env .env    # tethys testnet
```

Each env file sets `NETWORK`, `BOOT_NODES`, RPC endpoints, `HTTP_LISTEN_ADDR`, and `CONFIG` (the path to the YAML config, `mainnet.config.yml` or `tethys.config.yml`). The YAML config declares the SQD Network datasets source and any locally configured datasets. CLI flags map to environment variables; see `--help` for the full list.

Generate a key for the Portal's peer id:

```bash
docker run -u $(id -u):$(id -g) -v .:/cwd subsquid/keygen:latest /cwd/portal.key
```

This prints the **peer id** and writes the private key to `portal.key`. The peer id is what you register onchain.

## Running with Docker

```bash
KEY_PATH=portal.key docker compose up
```

This reads `.env` and the configured `*.config.yml`.

## Running from source

[Install Rust](https://rustup.rs/) (the toolchain is pinned in `rust-toolchain.toml`), then:

```bash
cargo run --release --key-path portal.key
```

On first start the Portal downloads the network assignment file (around 300 MB) before it begins serving.

## Querying

```bash
curl "localhost:8000/datasets/ethereum-mainnet/stream" --compressed -d '{
  "type": "evm",
  "fields": {
    "block": {
      "number": true,
      "timestamp": true,
      "hash": true
    }
  },
  "includeAllBlocks": true,
  "fromBlock": 10000000
}'
```

## Documentation

- Portal: https://docs.sqd.dev/en/portal
- SQD Network: https://docs.sqd.dev/en/network

## License

AGPL-3.0. See [LICENSE.md](./LICENSE.md).
