# SQD Portal

## Overview

High-performance data gateway for the SQD Network. The portal serves as a bridge between data consumers and the decentralized network of workers, providing blockchain data access with a streaming HTTP API.

## Data Sources

### SQD Network

SQD Network consists of >2000 decentralized workers and serves static blockchain data with the delay from the head up to several hours. This allows to achieve very high streaming speed.

To access SQD Network you'll need to lock SQD tokens to your peer id in the smart contract (e.g., in the [Network App](https://network.subsquid.io/portal)). The more tokens are locked, the more bandwidth the portal will get.

### Real-time data

Latest blocks are streamed from a separate service, [HotblocksDB](https://github.com/subsquid/data/tree/master/crates/hotblocks).

Portal unites historical data from SQD Network with real-time data from HotblocksDB, allowing for both high throughput and low latency.

If you're only interested in real-time data of an existing blockchain or you want to index a small devnet, please refer to [this example](./examples/devnet-evm/README.md).

## Initial configuration

Symlink/rename the example env-file to `.env`:

```bash
# For mainnet
ln -s mainnet.env .env
```
or
```bash
# For tethys testnet
ln -s tethys.env .env
```

Generate a new key with

```bash
docker run -u $(id -u):$(id -g) -v .:/cwd subsquid/keygen:latest /cwd/portal.key
```

It will print your **peer id** to the console and save private key to the `portal.key` file.
This peer id is what you need to register on chain.

## Running with Docker

Start the portal with
```bash
KEY_PATH=portal.key docker-compose up
```
It will use `.env` and `mainnet.config.yml`/`tethys.config.yml` files for configuration.

## Running from Source

[Install Rust](https://rustup.rs/), then build and run with

```bash
cargo run --release --key-path portal.key
```

Then wait for it to download the assignment file (~300MB) and it's ready to go.

## Using

Once the portal is running, you can query it like this:
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

More query examples can be found in the [Docs](https://beta.docs.sqd.dev/en/portal/evm/overview).
