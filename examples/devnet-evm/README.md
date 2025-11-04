# DevNet EVM local environment

This is an example how to index your local devnet data to query it with [SQD API](https://docs.sqd.ai/subsquid-network/reference/evm-api/).

## Running

Start the services:

```bash
export RPC_URL=https://your-rpc-endpoint-url
docker-compose up -d
```

## Using

Once running, the query API is available at `http://localhost:8000`.

Query example:
```bash
curl "http://localhost:8000/datasets/my-devnet/stream" -H "Content-Type: application/json" --compressed -d '{
  "type": "evm",
  "fields": {
    "block": {
      "timestamp": true,
      "number": true,
      "hash": true
    }
  },
  "includeAllBlocks": true,
  "fromBlock": 0
}'
```
