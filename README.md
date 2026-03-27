# runespy-worker

Distributed worker client for [RuneSpy](https://runespy.com) — a RuneScape player tracking service.

Workers are volunteer-run processes that fetch player data from the RuneMetrics API on the tracker's behalf. The master server never fetches data itself; all API calls go through connected workers.

## How it works

```
                    ┌─────────────┐
                    │   Master    │
                    │  (RuneSpy)  │
                    │  Dispatcher │
                    └──┬───┬──┬──┘
          WebSocket    │   │  │    WebSocket
         ┌─────────────┘   │  └─────────────┐
         │                 │                │
    ┌────▼────┐      ┌────▼────┐      ┌────▼────┐
    │  Your   │      │ Worker  │      │ Worker  │
    │ Worker  │      │  (CLI)  │      │  (CLI)  │
    └────┬────┘      └────┬────┘      └────┬────┘
         │                │                │
         └────────── RuneMetrics API ───────┘
```

Workers connect over a persistent WebSocket, receive batches of usernames to look up, fetch their RuneMetrics profiles, and stream results back to the master for validation and storage.

## Requirements

- Python 3.12+
- [`uv`](https://github.com/astral-sh/uv) (recommended) or `pip`
- Outbound internet access to `apps.runescape.com` and `secure.runescape.com`
- Approval from the RuneSpy admin before your worker can connect

## Setup

### 1. Install

```bash
git clone https://github.com/metalglove/runespy-worker.git
cd runespy-worker
```

**With uv (recommended):**

```bash
uv pip install -e .
```

All subsequent `runespy-worker` commands must be prefixed with `uv run` (e.g. `uv run runespy-worker register ...`), or activate the virtual environment first:

```bash
source .venv/bin/activate
```

**With pip:**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install .
```

With the venv activated, `runespy-worker` is available directly.

### 2. Register

```bash
runespy-worker register --master wss://runespy.com --name "your-machine-name"
```

This generates an Ed25519 keypair at `~/.runespy/worker_key.pem` and registers with the master. Your worker will be in **pending** status until approved.

Note the **worker ID** printed — you'll need to share it with the admin.

> **Note:** Running `register` again after a keypair already exists will be blocked with a clear error. If you genuinely need to re-register (e.g. you lost your credentials), delete `~/.runespy/worker_key.pem` first.

### 3. Wait for approval

Contact the RuneSpy admin (e.g. via Discord) with your worker ID. The admin will approve your worker and send back an `encrypted_secret` blob.

The secret is only issued once at approval time and is not retrievable again from the server — keep it safe.

### 4. Save the secret and run

```bash
# Save the encrypted secret the admin sent you
runespy-worker save-secret --encrypted <base64_blob>

# Start the worker
runespy-worker run --master wss://runespy.com
```

The worker will authenticate, receive task batches, fetch player profiles, and send results back automatically.

---

## Checking your status

```bash
runespy-worker status --master wss://runespy.com
```

This shows whether your worker is `pending` or `approved`. It does **not** retrieve or display the shared secret — that is sent to you by the admin out-of-band (e.g. Discord) and saved with `save-secret`.

---

## Running with Docker

The Docker container is for the **run phase only** — it has no registration flow. You must complete the CLI setup (steps 1–4 above) on any machine with Python first to generate your credentials, then hand those credentials to Docker as environment variables.

### Step 1: Get your credentials via CLI

Follow the [Setup](#setup) section on any machine with Python. After `save-secret` completes you will have three files in `~/.runespy/`:

```
~/.runespy/worker_id
~/.runespy/worker_key.pem
~/.runespy/worker_secret.key
```

### Step 2: Build and run the container

```bash
docker build -t runespy-worker .
```

**Linux:**

```bash
docker run -d \
  -e WORKER_ID="$(cat ~/.runespy/worker_id)" \
  -e WORKER_KEY_PEM_B64="$(base64 -w0 ~/.runespy/worker_key.pem)" \
  -e WORKER_SECRET_B64="$(base64 -w0 ~/.runespy/worker_secret.key)" \
  -e MASTER_URL="wss://runespy.com" \
  -e MAX_CONCURRENT="5" \
  --name runespy-worker \
  --restart unless-stopped \
  runespy-worker
```

**macOS** (`base64` does not support `-w`):

```bash
docker run -d \
  -e WORKER_ID="$(cat ~/.runespy/worker_id)" \
  -e WORKER_KEY_PEM_B64="$(base64 ~/.runespy/worker_key.pem)" \
  -e WORKER_SECRET_B64="$(base64 ~/.runespy/worker_secret.key)" \
  -e MASTER_URL="wss://runespy.com" \
  -e MAX_CONCURRENT="5" \
  --name runespy-worker \
  --restart unless-stopped \
  runespy-worker
```

The container writes credentials to `~/.runespy/` at startup and connects to the master automatically.

---

## Credential files

| Path | Contents | Permissions |
|------|----------|-------------|
| `~/.runespy/worker_key.pem` | Ed25519 private key (PEM/PKCS8) | 0600 |
| `~/.runespy/worker_secret.key` | Raw 32-byte HMAC shared secret | 0600 |
| `~/.runespy/worker_id` | Worker UUID (not secret) | default |

Both secret files are created with `chmod 600` automatically.

---

## Security model

Each worker is authenticated with a two-factor scheme:

1. **Ed25519 challenge-response** — on every connection the server sends a random nonce; the worker signs it with its private key and the server verifies the signature against the registered public key.
2. **HMAC-SHA256 message signing** — all messages are signed with a 32-byte shared secret issued at approval time. The server stores only a bcrypt hash of this secret; the plaintext is delivered once, encrypted with AES-GCM keyed from your public key.

Workers that submit structurally invalid, temporally inconsistent, or suspiciously-timed data accumulate violation counts. Repeated hard violations result in suspension.

### What the server validates on every result

- **Structural**: all required keys present, exactly 29 skills, valid level/XP ranges, skill XP sums match totals
- **Temporal**: XP, levels, and quest counts never decrease relative to the stored history
- **Timing**: fetch timestamp within 60 seconds of server time, fetch duration within a plausible range (5ms–30s)

### What this worker fetches

- `https://apps.runescape.com/runemetrics/profile/profile` — player profile (XP, skills, activities)
- `https://secure.runescape.com/m=hiscore/index_lite.ws` — hiscores fallback for private profiles

No credentials, cookies, or account data are ever accessed. Only public RuneMetrics and hiscores endpoints are used.

---

## Protocol overview

All worker-to-server messages use a signed JSON envelope:

```json
{
  "type": "<message_type>",
  "id": "<uuid4>",
  "ts": 1741305600.123,
  "worker_id": "<worker_uuid>",
  "hmac": "<hex_digest>",
  "payload": { ... }
}
```

### Authentication flow

```
Worker                          Master
  │                               │
  │──── connect ──────────────────▶│
  │◀──── challenge {nonce} ────────│
  │──── auth {sig, hmac} ─────────▶│
  │◀──── config {delay, limits} ───│
  │──── ready {capacity} ─────────▶│
  │◀──── assign_batch {tasks} ─────│
  │──── batch_result {results} ───▶│
  │         ...                    │
```

### Task lifecycle

```
PENDING ──▶ ASSIGNED ──▶ SUCCESS
                    └──▶ FAILED ──▶ (retry, max 3)
                               └──▶ DEAD_LETTER
```

---

## CLI reference

```
runespy-worker register    --master <url> --name <name>
runespy-worker save-secret --encrypted <base64_blob>
runespy-worker run         --master <url> [--max-concurrent <n>]
runespy-worker status      --master <url>
```
