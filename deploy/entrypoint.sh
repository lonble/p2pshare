#!/bin/sh
# Starts p2pshare and, if BOOTSTRAP_HOST is set, waits for that node's RPC to
# come up, fetches its node ID, and bootstraps this node against it - so a
# whole swarm can come up with a single `docker compose up`, no manual
# `p2pc bootstrap` calls needed.
set -eu

DATA_DIR="${DATA_DIR:-/data}"
QUIC_ADDR="${QUIC_ADDR:-:9000}"
RPC_ADDR="${RPC_ADDR:-0.0.0.0:8000}"
RPC_PORT="${RPC_ADDR##*:}"

/app/p2pshare -addr "$QUIC_ADDR" -rpc "$RPC_ADDR" -data "$DATA_DIR" &
NODE_PID=$!

rpc_call() {
  # rpc_call <host:port> <method> [params-json]
  curl -sf -X POST -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":${3:-null}}" \
    "http://$1/"
}

echo "waiting for local node to come up..."
until rpc_call "127.0.0.1:${RPC_PORT}" status >/dev/null 2>&1; do
  sleep 0.3
done
echo "local node is up."

if [ -n "${BOOTSTRAP_HOST:-}" ]; then
  SEED="${BOOTSTRAP_HOST}:${BOOTSTRAP_RPC_PORT:-8000}"
  echo "waiting for bootstrap seed at ${SEED}..."
  until SEED_STATUS=$(rpc_call "$SEED" status 2>/dev/null); do
    sleep 0.5
  done
  SEED_ID=$(echo "$SEED_STATUS" | jq -r '.result.id')
  SEED_QUIC_ADDR="${BOOTSTRAP_HOST}:${BOOTSTRAP_QUIC_PORT:-9000}"
  echo "bootstrapping against ${SEED_ID}@${SEED_QUIC_ADDR}..."
  rpc_call "127.0.0.1:${RPC_PORT}" bootstrap "[{\"id\":\"${SEED_ID}\",\"addr\":\"${SEED_QUIC_ADDR}\"}]" >/dev/null
  echo "bootstrap done."
fi

wait "$NODE_PID"
