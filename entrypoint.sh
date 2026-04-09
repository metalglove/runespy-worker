#!/bin/sh
# Materialise worker credentials from environment variables into ~/.runespy/
# Required env vars:
#   WORKER_ID           — UUID assigned during registration
#   WORKER_KEY_PEM_B64  — base64-encoded Ed25519 private key (PEM)
#   WORKER_SECRET_B64   — base64-encoded raw HMAC shared secret
#   MASTER_URL          — WebSocket URL of the master (ws:// or wss://)

set -e

CRED_DIR="${HOME}/.runespy"
mkdir -p "$CRED_DIR"

if [ -z "$WORKER_ID" ] || [ -z "$WORKER_KEY_PEM_B64" ] || [ -z "$WORKER_SECRET_B64" ] || [ -z "$MASTER_URL" ]; then
    echo "ERROR: WORKER_ID, WORKER_KEY_PEM_B64, WORKER_SECRET_B64, and MASTER_URL must all be set." >&2
    exit 1
fi

printf '%s' "$WORKER_ID"          > "$CRED_DIR/worker_id"
printf '%s' "$WORKER_KEY_PEM_B64" | base64 -d > "$CRED_DIR/worker_key.pem"
printf '%s' "$WORKER_SECRET_B64"  | base64 -d > "$CRED_DIR/worker_secret.key"

chmod 600 "$CRED_DIR/worker_key.pem" "$CRED_DIR/worker_secret.key"

echo "Credentials written for worker $WORKER_ID"

EXTRA_ARGS=""
if [ -n "$WEBSHARE_API_KEY" ]; then
    EXTRA_ARGS="--webshare-api-key $WEBSHARE_API_KEY"
fi

exec uv run runespy-worker run --master "$MASTER_URL" --max-concurrent "${MAX_CONCURRENT:-5}" $EXTRA_ARGS
