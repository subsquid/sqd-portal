# Query gateway

This is an application that connects to the worker network and executes queries submitted via HTTP.

## Running

```
Usage: query-gateway [OPTIONS] --network <NETWORK>

Options:
  -k, --key <KEY>
          Path to libp2p key file [env: KEY_PATH=]
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
      --config-path <CONFIG_PATH>
          Path to config file [env: CONFIG_PATH=] [default: config.yml]
      --allocations-db-path <ALLOCATIONS_DB_PATH>
          Path to allocations database file [env: ALLOCATIONS_DB_PATH=] [default: allocations.db]
  -h, --help
          Print help
  -V, --version
          Print version

```

When the process is running, first one needs to get a worker for the query:
```
$ curl 127.0.0.1:8000/network/ethereum-mainnet/16145000/worker
127.0.0.1:8000/query/czM6Ly9ldGhhLW1haW5uZXQtc2lh/12D3KooWH8MFWwU9CNKuGBxMQypELByRM8jBBgp3gKxqomMbCCXb
```

The returned URL can be further used to submit the query:
```
$ curl -X POST 127.0.0.1:8000/query/czM6Ly9ldGhhLW1haW5uZXQtc2lh/12D3KooWH8MFWwU9CNKuGBxMQypELByRM8jBBgp3gKxqomMbCCXb -d '{"fromBlock": 16145000, "toBlock": 16146000, "transactions": [{"to": ["0x9cb7712c6a91506e69e8751fcb08e72e1256477d"], "sighash": ["0x8ca887ca"]}], "logs": [{"address": ["0x0f98431c8ad98523631ae4a59f267346ea31f984"], "topic0": ["0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"]}, {"address": ["0xc36442b4a4522e871399cd717abdd847ab11fe88"], "topic0": ["0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f", "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4", "0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01", "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]}, {"topic0": ["0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c", "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde", "0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95", "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"]}], "fields": {"log": {"address": true, "topics": true, "data": true, "transaction": true}, "transaction": {"from": true, "to": true, "gasPrice": true, "gas": true}}}' -o result
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  391k  100  390k  100  1108  1248k   3540 --:--:-- --:--:-- --:--:-- 1256k

```
