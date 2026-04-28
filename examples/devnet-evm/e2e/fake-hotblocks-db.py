#!/usr/bin/env python3
import argparse
import gzip
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


BLOCKS = [
    {
        "header": {
            "number": 100,
            "hash": "0x" + "01" * 32,
            "parentHash": "0x" + "00" * 32,
            "timestamp": 1_700_000_000,
        }
    },
    {
        "header": {
            "number": 101,
            "hash": "0x" + "02" * 32,
            "parentHash": "0x" + "01" * 32,
            "timestamp": 1_700_000_012,
        }
    },
]


def block_ref(block):
    header = block["header"]
    return {"number": header["number"], "hash": header["hash"]}


class Handler(BaseHTTPRequestHandler):
    server_version = "fake-hotblocks-db/0.1"

    def log_message(self, fmt, *args):
        print("%s - %s" % (self.address_string(), fmt % args))

    def _send_json(self, obj, status=200):
        data = json.dumps(obj, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_jsonl(self, rows, gzip_body=False):
        data = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows).encode()
        if gzip_body:
            data = gzip.compress(data)

        self.send_response(200)
        self.send_header("content-type", "application/jsonl")
        if gzip_body:
            self.send_header("content-encoding", "gzip")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _route(self):
        parts = [p for p in urlparse(self.path).path.split("/") if p]
        if len(parts) != 3 or parts[0] != "datasets":
            return None, None
        return parts[1], parts[2]

    def do_GET(self):
        dataset, endpoint = self._route()
        if dataset is None:
            self._send_json({"error": "not found"}, status=404)
            return

        if endpoint in ("head", "finalized-head"):
            self._send_json(block_ref(BLOCKS[-1]))
            return

        if endpoint == "status":
            self._send_json(
                {
                    "kind": "evm",
                    "retentionStrategy": {"Head": 20},
                    "data": {
                        "firstBlock": BLOCKS[0]["header"]["number"],
                        "lastBlock": BLOCKS[-1]["header"]["number"],
                        "lastBlockHash": BLOCKS[-1]["header"]["hash"],
                        "lastBlockTimestamp": BLOCKS[-1]["header"]["timestamp"],
                        "finalizedHead": block_ref(BLOCKS[-1]),
                    },
                }
            )
            return

        self._send_json({"error": "not found"}, status=404)

    def do_POST(self):
        dataset, endpoint = self._route()
        if dataset is None or endpoint not in ("stream", "finalized-stream"):
            self._send_json({"error": "not found"}, status=404)
            return

        length = int(self.headers.get("content-length", "0"))
        raw_query = self.rfile.read(length) if length else b"{}"
        try:
            query = json.loads(raw_query.decode() or "{}")
        except json.JSONDecodeError:
            self._send_json({"error": "invalid json"}, status=400)
            return

        from_block = query.get("fromBlock", BLOCKS[0]["header"]["number"])
        to_block = query.get("toBlock", BLOCKS[-1]["header"]["number"])
        rows = [
            block
            for block in BLOCKS
            if from_block <= block["header"]["number"] <= to_block
        ]
        wants_gzip = "gzip" in self.headers.get("accept-encoding", "").lower()
        self._send_jsonl(rows, gzip_body=wants_gzip)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=19090, type=int)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"fake HotblocksDB listening on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
