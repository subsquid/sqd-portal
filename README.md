# Query gateway

This is an application that connects to the worker network and executes queries submitted via HTTP.

## Running

```
Usage: subsquid-query-gateway [OPTIONS] --network <NETWORK> --logs-collector-id <LOGS_COLLECTOR_ID> --config <CONFIG>

Options:
  -k, --key <KEY>
          Path to libp2p key file [env: KEY_PATH=]
      --config <CONFIG>
          Path to config file [env: CONFIG=]
      --p2p-listen-addrs <P2P_LISTEN_ADDRS>...
          Addresses on which the p2p node will listen [env: P2P_LISTEN_ADDRS=] [default: /ip4/0.0.0.0/udp/0/quic-v1]
      --p2p-public-addrs <P2P_PUBLIC_ADDRS>...
          Public address(es) on which the p2p node can be reached [env: P2P_PUBLIC_ADDRS=]
      --boot-nodes <BOOT_NODES>...
          Connect to boot node '<peer_id> <address>'. [env: BOOT_NODES=]
      --rpc-url <RPC_URL>
          Blockchain RPC URL [env: RPC_URL=] [default: http://127.0.0.1:8545/]
      --l1-rpc-url <L1_RPC_URL>
          Layer 1 blockchain RPC URL. If not provided, rpc_url is assumed to be L1 [env: L1_RPC_URL=]
      --gateway-registry-contract-addr <GATEWAY_REGISTRY_CONTRACT_ADDR>
          [env: GATEWAY_REGISTRY_CONTRACT_ADDR=]
      --worker-registration-contract-addr <WORKER_REGISTRATION_CONTRACT_ADDR>
          [env: WORKER_REGISTRATION_CONTRACT_ADDR=]
      --network-controller-contract-addr <NETWORK_CONTROLLER_CONTRACT_ADDR>
          [env: NETWORK_CONTROLLER_CONTRACT_ADDR=]
      --allocations-viewer-contract-addr <ALLOCATIONS_VIEWER_CONTRACT_ADDR>
          [env: ALLOCATIONS_VIEWER_CONTRACT_ADDR=]
      --multicall-contract-addr <MULTICALL_CONTRACT_ADDR>
          [env: MULTICALL_CONTRACT_ADDR=]
      --network <NETWORK>
          Network to connect to (mainnet or testnet) [env: NETWORK=] [possible values: tethys, mainnet]
      --http-listen <HTTP_LISTEN>
          HTTP server listen addr [env: HTTP_LISTEN_ADDR=] [default: 0.0.0.0:8000]
      --logs-collector-id <LOGS_COLLECTOR_ID>
          Logs collector peer id [env: LOGS_COLLECTOR_ID=]
  -h, --help
          Print help
  -V, --version
          Print version
```

When the process is running, you can submit a query and get the results streamed to your terminal:
```
$ curl "127.0.0.1:8000/stream/ethereum-mainnet" -d '{"fromBlock": 1000, "includeAllBlocks": true}' | zcat | less
```
