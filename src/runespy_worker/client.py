"""WebSocket client — connects to master, handles tasks."""

import asyncio
import json
import logging
import time
from datetime import UTC, datetime

import httpx
import websockets

from runespy_worker.crypto import (
    hmac_challenge,
    load_private_key,
    load_secret,
    load_worker_id,
    sign_challenge,
)
from runespy_worker.fetcher import fetch_hiscores, fetch_profile
from runespy_worker.protocol import build_message

logger = logging.getLogger("runespy_worker")

BATCH_FLUSH_INTERVAL = 5.0  # seconds between batch sends
BATCH_MAX_SIZE = 20         # send early if this many results are queued


class RateLimiter:
    """Token bucket rate limiter — shared across all concurrent tasks."""

    def __init__(self, rate: float, period: float = 3600.0):
        """
        rate:   max requests allowed per period
        period: window size in seconds (default: 1 hour)
        """
        self._tokens = rate
        self._rate = rate
        self._refill_rate = rate / period  # tokens per second
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self._rate, self._tokens + elapsed * self._refill_rate)
            if self._tokens < 1:
                wait = (1 - self._tokens) / self._refill_rate
                await asyncio.sleep(wait)
                self._tokens = 0
            else:
                self._tokens -= 1


def setup_logging():
    """Configure worker logging with timestamps and colours."""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "\033[2m%(asctime)s\033[0m [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# Counters shared with heartbeat
_stats = {"completed": 0, "failed": 0}


async def heartbeat_loop(ws, worker_id: str, secret: bytes, interval: float = 30.0):
    """Send periodic heartbeats."""
    start = time.time()

    while True:
        await asyncio.sleep(interval)
        msg = build_message("heartbeat", {
            "uptime": int(time.time() - start),
            "tasks_completed": _stats["completed"],
            "tasks_failed": _stats["failed"],
            "current_load": 0,
        }, worker_id, secret)
        try:
            await ws.send(msg)
            logger.debug("Heartbeat sent (uptime=%ds)", int(time.time() - start))
        except Exception:
            break


async def batch_sender_loop(
    ws,
    result_queue: asyncio.Queue,
    worker_id: str,
    secret: bytes,
):
    """Drain the result queue and send batch_result messages to the server."""
    while True:
        await asyncio.sleep(BATCH_FLUSH_INTERVAL)

        items = []
        while not result_queue.empty() and len(items) < BATCH_MAX_SIZE:
            items.append(result_queue.get_nowait())

        if not items:
            continue

        msg = build_message("batch_result", {"results": items}, worker_id, secret)
        try:
            await ws.send(msg)
            logger.info(
                "\033[32mBatch sent\033[0m — %d result(s)",
                len(items),
            )
        except Exception as e:
            logger.warning("Failed to send batch: %s — re-queuing %d item(s)", e, len(items))
            for item in items:
                await result_queue.put(item)
            break


async def process_task(
    result_queue: asyncio.Queue,
    task: dict,
    worker_id: str,
    secret: bytes,
    http_client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
    fetch_delay: float = 4.0,
):
    """Fetch a player profile and put the result into the batch queue."""
    async with semaphore:
        task_id = task["task_id"]
        username = task["username"]

        logger.info("\033[36mFetching\033[0m %s", username)

        await rate_limiter.acquire()
        start_ms = time.time() * 1000
        data, error = await fetch_profile(http_client, username)
        if error == "PROFILE_PRIVATE":
            logger.info("Profile private for %s, trying hiscores fallback", username)
            data, error = await fetch_hiscores(http_client, username)
            if error:
                error = "PROFILE_PRIVATE"
        fetch_time_ms = time.time() * 1000 - start_ms

        if error:
            _stats["failed"] += 1
            logger.warning(
                "\033[31mFailed\033[0m %s — %s (%.0fms)",
                username, error, fetch_time_ms,
            )
            await result_queue.put({
                "status": "error",
                "task_id": task_id,
                "username": username,
                "error_code": error,
                "detail": "",
            })
        else:
            _stats["completed"] += 1
            await result_queue.put({
                "status": "success",
                "task_id": task_id,
                "username": username,
                "data": data,
                "fetch_time_ms": round(fetch_time_ms, 1),
                "fetched_at": datetime.now(UTC).isoformat(),
            })
            logger.info(
                "\033[32mFetched\033[0m %s — %.0fms (queued for batch)",
                username, fetch_time_ms,
            )

        await asyncio.sleep(fetch_delay)


async def run(master_url: str, max_concurrent: int = 5):
    """Main worker loop — connect, authenticate, process tasks."""
    setup_logging()

    worker_id = load_worker_id()
    private_key = load_private_key()
    secret = load_secret()

    logger.info("Worker ID: %s", worker_id)
    logger.info("Ed25519 public key loaded")
    logger.info("HMAC shared secret loaded (%d bytes)", len(secret))

    ws_url = f"{master_url}/api/workers/ws/connect?worker_id={worker_id}"

    while True:
        try:
            logger.info("Connecting to %s ...", master_url)
            async with websockets.connect(ws_url) as ws:
                logger.info("\033[32mConnected\033[0m to %s", master_url)

                # Wait for challenge
                challenge_raw = await ws.recv()
                challenge_msg = json.loads(challenge_raw)

                if challenge_msg.get("type") != "challenge":
                    logger.error("Expected challenge, got: %s", challenge_msg.get("type"))
                    continue

                nonce = challenge_msg["payload"]["nonce"]
                logger.info(
                    "Challenge received — nonce=%s...%s (%d hex chars)",
                    nonce[:8], nonce[-4:], len(nonce),
                )

                # Respond with auth
                sig = sign_challenge(private_key, nonce)
                mac = hmac_challenge(secret, nonce)
                logger.info(
                    "Signing challenge — Ed25519 sig=%s... HMAC=%s...",
                    sig[:16], mac[:16],
                )
                auth_msg = build_message("auth", {
                    "signature": sig,
                    "hmac": mac,
                }, worker_id, secret)
                await ws.send(auth_msg)
                logger.info("Auth message sent (HMAC-SHA256 envelope)")

                # Wait for config
                config_raw = await ws.recv()
                config_msg = json.loads(config_raw)
                fetch_delay = 4.0
                rate_limit_per_hour = 300
                if config_msg.get("type") == "config":
                    payload = config_msg.get("payload", {})
                    fetch_delay = payload.get("fetch_delay", 3.0)
                    max_concurrent = payload.get("max_concurrent", max_concurrent)
                    rate_limit_per_hour = payload.get("rate_limit_per_hour", 300)
                    logger.info(
                        "\033[32mAuthenticated\033[0m — fetch_delay=%.1fs, max_concurrent=%d, rate_limit=%d/hr",
                        fetch_delay, max_concurrent, rate_limit_per_hour,
                    )
                elif config_msg.get("type") == "error":
                    logger.error("Auth rejected: %s", config_msg.get("error"))
                    break
                rate_limiter = RateLimiter(rate=rate_limit_per_hour)

                # Send ready
                ready_msg = build_message("ready", {
                    "capacity": max_concurrent,
                }, worker_id, secret)
                await ws.send(ready_msg)
                logger.info("Sent ready (capacity=%d) — waiting for tasks", max_concurrent)

                semaphore = asyncio.Semaphore(max_concurrent)
                result_queue: asyncio.Queue = asyncio.Queue()

                heartbeat_task = asyncio.create_task(
                    heartbeat_loop(ws, worker_id, secret)
                )
                batch_task = asyncio.create_task(
                    batch_sender_loop(ws, result_queue, worker_id, secret)
                )

                try:
                    async with httpx.AsyncClient() as http_client:
                        async for raw in ws:
                            msg = json.loads(raw)
                            msg_type = msg.get("type")

                            if msg_type == "assign_batch":
                                tasks = msg.get("payload", {}).get("tasks", [])
                                usernames = [t["username"] for t in tasks]
                                logger.info(
                                    "\033[33mBatch received\033[0m — %d task(s): %s",
                                    len(tasks),
                                    ", ".join(usernames[:10]) + ("..." if len(usernames) > 10 else ""),
                                )
                                for task in tasks:
                                    asyncio.create_task(
                                        process_task(
                                            result_queue, task, worker_id, secret,
                                            http_client, semaphore, rate_limiter, fetch_delay,
                                        )
                                    )

                            elif msg_type == "config":
                                payload = msg.get("payload", {})
                                new_delay = payload.get("fetch_delay")
                                new_concurrent = payload.get("max_concurrent")
                                new_rate = payload.get("rate_limit_per_hour")
                                if new_delay is not None:
                                    fetch_delay = float(new_delay)
                                if new_concurrent is not None:
                                    new_concurrent = int(new_concurrent)
                                    semaphore = asyncio.Semaphore(new_concurrent)
                                    max_concurrent = new_concurrent
                                if new_rate is not None:
                                    rate_limiter = RateLimiter(rate=int(new_rate))
                                logger.info(
                                    "\033[36mConfig updated\033[0m — fetch_delay=%.1fs, max_concurrent=%d, rate_limit=%d/hr",
                                    fetch_delay, max_concurrent, rate_limiter._rate,
                                )
                                # Acknowledge with updated capacity
                                ready_msg = build_message("ready", {
                                    "capacity": max_concurrent,
                                }, worker_id, secret)
                                await ws.send(ready_msg)

                            elif msg_type == "heartbeat_ack":
                                logger.debug("Heartbeat ACK received")

                            elif msg_type == "revoke":
                                task_ids = msg.get("payload", {}).get("task_ids", [])
                                logger.warning("Tasks revoked by master: %s", task_ids)

                            elif msg_type == "shutdown":
                                logger.warning("\033[31mShutdown requested by master\033[0m")
                                break

                            else:
                                logger.debug("Unknown message type: %s", msg_type)

                finally:
                    heartbeat_task.cancel()
                    batch_task.cancel()

        except (websockets.ConnectionClosed, ConnectionRefusedError, OSError) as e:
            logger.warning("Connection lost: %s. Reconnecting in 10s...", e)
            await asyncio.sleep(10)
        except Exception as e:
            logger.error("Error: %s. Reconnecting in 30s...", e)
            await asyncio.sleep(30)
