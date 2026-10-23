# runespy-worker

Distributed worker client for [RuneSpy](https://runespy.com) — a RuneScape player tracking service.

Workers are volunteer-run processes that fetch player data from the RuneMetrics API on the tracker's behalf. The master server never fetches data itself; all API calls go through connected workers.

## Quick start (Docker)

The easiest way to run a worker — no Python or build tools needed:

```bash
docker run -d \
  -p 127.0.0.1:8080:8080 \
  -v ~/.runespy:/root/.runespy \
  --name runespy-worker \
  --restart unless-stopped \
  ghcr.io/metalglove/runespy-worker
```

Open **http://localhost:8080** to register, get approved, and start tracking. See the [Docker](#docker) section for more options (proxies, headless mode).

---

## How it works

```
                    ┌─────────────┐
                    │   Master    │
                    │  (RuneSpy)  │
                    │  Dispatcher │
                    └──┬───┬──┬───┘
          WebSocket    │   │  │    WebSocket
         ┌─────────────┘   │  └─────────────┐
         │                 │                │
    ┌────▼────┐       ┌────▼────┐      ┌────▼────┐
    │  Your   │       │ Worker  │      │ Worker  │
    │ Worker  │       │  (CLI)  │      │  (CLI)  │
    └────┬────┘       └────┬────┘      └────┬────┘
         │                 │                │
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

Contact the RuneSpy admin with your worker ID. Once approved, your registered Ed25519 private key is sufficient to connect; no secret needs to be exchanged.

### 4. Run the approved worker

```bash
# Start the worker
runespy-worker run --master wss://runespy.com
```

The worker will authenticate, receive task batches, fetch player profiles, and send results back automatically.

---

## Checking your status

```bash
runespy-worker status --master wss://runespy.com
```

This shows whether your registered worker is `pending` or `approved`.

---

## Docker

A pre-built image is available on GitHub Container Registry — no need to clone or build anything.

### Quick start (web UI)

```bash
docker run -d \
  -p 127.0.0.1:8080:8080 \
  -v ~/.runespy:/root/.runespy \
  --name runespy-worker \
  --restart unless-stopped \
  ghcr.io/metalglove/runespy-worker
```

Open `http://localhost:8080` to register, configure proxies, and monitor the worker. The `~/.runespy` volume persists credentials across container restarts.

To add proxies, pass the Webshare API key as an environment variable:

```bash
docker run -d \
  -p 127.0.0.1:8080:8080 \
  -v ~/.runespy:/root/.runespy \
  -e WEBSHARE_API_KEY="your-api-key" \
  --name runespy-worker \
  --restart unless-stopped \
  ghcr.io/metalglove/runespy-worker
```

### Headless mode

For pre-configured workers that don't need the web UI (e.g. servers), use the entrypoint script:

```bash
docker run -d \
  --entrypoint /entrypoint.sh \
  -e WORKER_ID="$(cat ~/.runespy/worker_id)" \
  -e WORKER_KEY_PEM_B64="$(base64 < ~/.runespy/worker_key.pem)" \
  -e MASTER_URL="wss://runespy.com" \
  -e WEBSHARE_API_KEY="your-api-key" \
  --name runespy-worker \
  --restart unless-stopped \
  ghcr.io/metalglove/runespy-worker
```

### Building locally

If you prefer to build the image yourself:

```bash
git clone https://github.com/metalglove/runespy-worker.git
cd runespy-worker
docker build -t runespy-worker .
```

Then use `runespy-worker` instead of `ghcr.io/metalglove/runespy-worker` in the commands above.

### Web UI flow

The UI guides you through four steps:

1. **Register** — enter a worker name, generates an Ed25519 keypair and registers with the master. Shows your worker ID to share with the admin.

2. **Approval** — share the worker ID with the admin, then start the worker after approval.

3. **Proxy configuration** (optional) — enter a Webshare API key or single proxy URL. The worker restarts automatically to pick up the new config.

4. **Run / Stop / Restart** — control the worker process. If a registered ID and private key are present when the container starts, the worker starts automatically.

The dashboard shows live statistics (tasks completed/failed, batches, uptime, proxy count), the server-assigned config (rate limit, concurrency, fetch delay), and a scrolling log viewer.

To keep the container running across reboots, ensure Docker starts on login:

- **Linux**: `sudo systemctl enable docker`
- **macOS / Windows**: Docker Desktop → Settings → General → "Start Docker Desktop when you log in"

---

## Credential files

| Path | Contents | Permissions |
|------|----------|-------------|
| `~/.runespy/worker_key.pem` | Ed25519 private key (PEM/PKCS8) | 0600 |
| `~/.runespy/worker_id` | Worker UUID (not secret) | default |

The private key is created with `chmod 600` automatically.

---

## Security model

Each worker authenticates its registered identity on a fresh WebSocket challenge:

1. **Ed25519 challenge-response** — on every connection the server sends a random nonce; the worker signs it with its private key and the server verifies the signature against the registered public key.
2. **Authenticated session** — subsequent messages use that WebSocket session. The previous public-key-XOR secret delivery has been removed; it did not provide recipient confidentiality and the server did not require its HMAC.

Workers that submit structurally invalid, temporally inconsistent, or suspiciously-timed data accumulate violation counts for administrative review.

### What the server validates on every result

- **Structural**: all required keys present, exactly 29 skills, valid level/XP ranges, skill XP sums match totals
- **Temporal**: XP and skill levels must not decrease relative to stored history
- **Timing**: fetch timestamp within 60 seconds of server time, fetch duration within a plausible range (5ms–30s)

### What this worker fetches

- `https://apps.runescape.com/runemetrics/profile/profile` — player profile (XP, skills, activities)
- `https://secure.runescape.com/m=hiscore/index_lite.ws` — hiscores fallback for private profiles

No credentials, cookies, or account data are ever accessed. Only public RuneMetrics and hiscores endpoints are used.

---

## Protocol overview

Messages use a JSON envelope over the authenticated WebSocket. The runtime
uses Ed25519 challenge-response for authentication; subsequent envelopes have
no HMAC. Use `wss://` to protect the connection in transit:

```json
{
  "type": "<message_type>",
  "id": "<uuid4>",
  "ts": 1741305600.123,
  "worker_id": "<worker_uuid>",
  "payload": { ... }
}
```

### Authentication flow

```
Worker                          Master
  │                               │
  │──── connect ──────────────────▶│
  │◀──── challenge {nonce} ────────│
  │──── auth {signature} ─────────▶│
  │◀──── config {delay, limits} ───│
  │──── ready {capacity, capabilities} ─────────▶│
  │◀──── assign_batch {tasks + attempt_count} ─────│
  │──── batch_result {results + attempt_count} ───▶│
  │         ...                    │
```

The `auth.payload.signature` signs the server nonce with the registered
Ed25519 private key. The ready capabilities include `assignment_attempt_v1`.
Every success/error result echoes its assigned `task_id` and `attempt_count`;
the master rejects stale generations and results from another worker.
The optional HMAC helpers in `protocol.py` are legacy compatibility utilities,
not a second authentication requirement or a secret-distribution mechanism.

### Task lifecycle

```
PENDING ──▶ ASSIGNED ──▶ SUCCESS
                    └──▶ FAILED ──▶ (retry, max 3)
                               └──▶ DEAD_LETTER
```

---

## Using proxies

Workers can optionally route API requests through HTTP proxies. This spreads traffic across multiple IPs, allowing higher throughput and reducing the chance of rate limiting.

[Webshare](https://www.webshare.io/) is the recommended provider. Their shared datacenter proxy plan at $2.99/mo (100 proxies, 250 GB bandwidth) is sufficient for tracking up to ~10,000 players at 20-minute intervals.

### Setup

1. Create a [Webshare](https://www.webshare.io/) account (free tier includes 10 proxies)
2. Go to **Dashboard > API** and copy your API key
3. Pass it when starting your worker:

```bash
# Via environment variable
export WEBSHARE_API_KEY="your-api-key"
runespy-worker run --master wss://runespy.com

# Or via CLI flag
runespy-worker run --master wss://runespy.com --webshare-api-key "your-api-key"
```

The worker automatically fetches your proxy list from Webshare's API on startup and rotates through them round-robin. The master detects how many proxies you have and scales your rate limits and concurrency accordingly — no manual tuning needed.

### Docker with proxies

Add the environment variable to your `docker run` command (see [Docker](#docker) section for full examples).

### Single proxy

If you have your own proxy (not from Webshare), you can pass it directly:

```bash
runespy-worker run --master wss://runespy.com --proxy-url "http://user:pass@host:port"
```

### Scaling reference

| Proxies | Players (20-min poll) | Webshare plan |
|---------|----------------------|---------------|
| 10 | ~1,000 | Free |
| 50 | ~5,000 | 100 proxies ($2.99/mo) |
| 100 | ~10,000 | 100 proxies ($2.99/mo) |

---

## CLI reference

```
runespy-worker register    --master <url> --name <name>
runespy-worker run         --master <url> [--max-concurrent <n>]
                           [--webshare-api-key <key>] [--proxy-url <url>]
runespy-worker status      --master <url>
```

## Assignment generations

This worker advertises `assignment_attempt_v1` and echoes `attempt_count` from
every assignment in both success and error results. Upgrade workers before
enabling the matching backend enforcement. Older backends ignore the new field;
the updated backend leaves legacy workers without this capability unassigned.
Existing registered IDs and Ed25519 keys remain valid. No secret file is needed.

Run the protocol regressions with `python -m unittest discover -s tests -v`.
