#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
E2E_DIR="$ROOT_DIR/examples/devnet-evm/e2e"
FAKE_HOST="${FAKE_HOST:-127.0.0.1}"
FAKE_PORT="${FAKE_PORT:-19090}"
PORTAL_HOST="${PORTAL_HOST:-127.0.0.1}"
PORTAL_PORT="${PORTAL_PORT:-18000}"
FAKE_URL="http://${FAKE_HOST}:${FAKE_PORT}"
PORTAL_URL="http://${PORTAL_HOST}:${PORTAL_PORT}"
FAKE_LOG="/tmp/sqd-fake-hotblocks-db.log"
PORTAL_LOG="/tmp/sqd-portal-hotblocks-e2e.log"
PORTAL_KEY="/tmp/sqd-portal-hotblocks-e2e.key"

fake_pid=""
portal_pid=""

cleanup() {
  if [ -n "$portal_pid" ] && kill -0 "$portal_pid" >/dev/null 2>&1; then
    kill "$portal_pid" >/dev/null 2>&1 || true
    wait "$portal_pid" >/dev/null 2>&1 || true
  fi

  if [ -n "$fake_pid" ] && kill -0 "$fake_pid" >/dev/null 2>&1; then
    kill "$fake_pid" >/dev/null 2>&1 || true
    wait "$fake_pid" >/dev/null 2>&1 || true
  fi
}

fail() {
  echo "FAIL: $*" >&2
  echo "Fake HotblocksDB log:" >&2
  cat "$FAKE_LOG" >&2 || true
  echo >&2
  echo "Portal log:" >&2
  cat "$PORTAL_LOG" >&2 || true
  exit 1
}

assert_eq() {
  local expected="$1"
  local actual="$2"
  local label="$3"

  if [ "$actual" != "$expected" ]; then
    fail "${label}: expected '${expected}', got '${actual}'"
  fi
}

trap cleanup EXIT

cd "$ROOT_DIR"
rm -f "$FAKE_LOG" "$PORTAL_LOG"

python3 "$E2E_DIR/fake-hotblocks-db.py" \
  --host "$FAKE_HOST" \
  --port "$FAKE_PORT" \
  >"$FAKE_LOG" 2>&1 &
fake_pid="$!"

echo "Waiting for fake HotblocksDB at ${FAKE_URL}..."
for attempt in $(seq 1 30); do
  if curl -fsS "${FAKE_URL}/datasets/base-mainnet/head" >/tmp/sqd-fake-head.json 2>/dev/null; then
    break
  fi

  if [ "$attempt" -eq 30 ]; then
    fail "fake HotblocksDB did not become ready"
  fi

  sleep 1
done

cargo run --quiet -- \
  --config "$E2E_DIR/portal-hotblocks-only.config.yml" \
  --key "$PORTAL_KEY" \
  --rpc-url http://127.0.0.1:1 \
  --l1-rpc-url http://127.0.0.1:1 \
  --dummy-client-file-path "$E2E_DIR/dummy-contract-client.json" \
  --http-listen "${PORTAL_HOST}:${PORTAL_PORT}" \
  >"$PORTAL_LOG" 2>&1 &
portal_pid="$!"

echo "Waiting for portal at ${PORTAL_URL}..."
for attempt in $(seq 1 60); do
  if curl -fsS "${PORTAL_URL}/datasets/base-mainnet/head" >/tmp/sqd-portal-head.json 2>/dev/null; then
    break
  fi

  if ! kill -0 "$portal_pid" >/dev/null 2>&1; then
    fail "portal process exited before becoming ready"
  fi

  if [ "$attempt" -eq 60 ]; then
    fail "portal did not become ready"
  fi

  sleep 1
done

echo "Test: portal forwards /datasets/base-mainnet/head to HotblocksDB"
head_json="$(cat /tmp/sqd-portal-head.json)"
assert_eq '{"number":101,"hash":"0x0202020202020202020202020202020202020202020202020202020202020202"}' "$head_json" "head"

echo "Test: portal metadata uses HotblocksDB /status for start_block"
metadata_json="$(curl -fsS "${PORTAL_URL}/datasets/base-mainnet?expand[]=first_block")"
case "$metadata_json" in
  *'"start_block":100'*) ;;
  *) fail "metadata did not include start block from hotblocks status: ${metadata_json}" ;;
esac

echo "Test: portal forwards /datasets/base-mainnet/stream query to HotblocksDB"
stream_jsonl="$(curl -fsS \
  "${PORTAL_URL}/datasets/base-mainnet/stream" \
  -H "Content-Type: application/json" \
  --compressed \
  -d '{"type":"evm","fields":{"block":{"number":true,"hash":true,"timestamp":true}},"includeAllBlocks":true,"fromBlock":100,"toBlock":101}')"

if ! grep -q '"number":100' <<<"$stream_jsonl" || ! grep -q '"number":101' <<<"$stream_jsonl"; then
  fail "stream response did not include fake blocks 100 and 101: ${stream_jsonl}"
fi

echo "Test: timestamp lookup returns exact first fake block"
block_at_first_ts="$(curl -fsS "${PORTAL_URL}/datasets/base-mainnet/timestamps/1700000000/block")"
assert_eq '{"block_number":100}' "$block_at_first_ts" "timestamp at first block"

echo "Test: timestamp lookup returns next block for in-between timestamp"
block_between_ts="$(curl -fsS "${PORTAL_URL}/datasets/base-mainnet/timestamps/1700000005/block")"
assert_eq '{"block_number":101}' "$block_between_ts" "timestamp between blocks"

echo "Test: timestamp lookup returns 404 outside fake hotblocks range"
outside_status="$(curl -sS -o /tmp/sqd-outside-range.json -w '%{http_code}' \
  "${PORTAL_URL}/datasets/base-mainnet/timestamps/1700009999/block")"
assert_eq "404" "$outside_status" "timestamp outside hotblocks range status"

echo "OK: portal hotblocks path works against deterministic fake HotblocksDB."
