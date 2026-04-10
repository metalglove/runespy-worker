"""WebSocket client — connects to master, authenticates, and processes task batches.

Concurrency model
-----------------
All fetch tasks run concurrently, bounded by two independent controls:

  * ``asyncio.Semaphore(max_concurrent)`` — hard cap on the number of tasks
    that can be in-flight at once (default 5, overridable by the server via
    ``config`` messages).

  * ``RateLimiter`` — token-bucket that enforces the per-hour request cap
    assigned by the server at authentication time.  Tokens refill continuously;
    a task blocks (awaits) until a token is available rather than being dropped.

Results are not sent immediately.  Instead every ``process_task`` call puts its
result dict into a shared ``asyncio.Queue``; a dedicated ``batch_sender_loop``
coroutine drains the queue and ships ``batch_result`` messages every
``BATCH_FLUSH_INTERVAL`` seconds (or immediately when the queue reaches
``BATCH_MAX_SIZE`` items).  This reduces WebSocket traffic and batching overhead
on the server.

Connection lifecycle
--------------------
``run()`` wraps everything in an outer ``while True`` retry loop so that
transient network failures cause a reconnect rather than a crash.  On each
fresh connection the full handshake is repeated:

  1. Receive ``challenge`` (random hex nonce from server).
  2. Sign nonce with Ed25519 private key (proves worker identity).
  3. Compute HMAC-SHA256 of nonce with shared secret (proves secret possession).
  4. Send ``auth`` envelope containing both proofs.
  5. Receive ``config`` (per-worker fetch parameters) or ``error`` (auth failure).
  6. Send ``ready`` (advertise concurrency capacity to the dispatcher).
  7. Enter message dispatch loop: ``assign_batch`` → spawn tasks,
     ``config`` → update limits live, ``shutdown`` → clean exit.

Background tasks (``heartbeat_loop``, ``batch_sender_loop``) are created after
authentication and cancelled on disconnect so they do not outlive the WebSocket.
"""

import asyncio
import itertools
import json
import logging
import re
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path

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
    """Token-bucket rate limiter shared across all concurrent fetch tasks.

    Tokens refill continuously at ``rate / period`` tokens per second up to a
    maximum of ``rate``.  ``acquire()`` blocks until a token is available rather
    than raising an exception, so callers never need to handle rejection.

    A single lock serialises token accounting so concurrent tasks cannot
    simultaneously read a non-zero balance and each decrement it.
    """

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
        wait = 0.0
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self._rate, self._tokens + elapsed * self._refill_rate)
            if self._tokens < 1:
                wait = (1 - self._tokens) / self._refill_rate
                self._tokens = 0
            else:
                self._tokens -= 1
        if wait:
            await asyncio.sleep(wait)


def setup_logging():
    """Configure worker logging with timestamps and colours."""
    fmt = logging.Formatter(
        "\033[2m%(asctime)s\033[0m [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    handler = logging.StreamHandler()
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    stats_handler = _StatsLogHandler()
    stats_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    ))
    logger.addHandler(stats_handler)
    logger.setLevel(logging.INFO)


# Counters shared with heartbeat and stats file
_stats = {"completed": 0, "failed": 0, "batches_received": 0, "batches_sent": 0}
_state: dict = {
    "status": "starting",
    "worker_id": None,
    "connected_since": None,
    "config": {},
    "proxy_count": 0,
}
_recent_logs: deque[str] = deque(maxlen=200)
_STATS_PATH = Path.home() / ".runespy" / "stats.json"
_LOGS_PATH = Path.home() / ".runespy" / "logs.json"


class _StatsLogHandler(logging.Handler):
    """Captures log lines into _recent_logs for the web UI."""

    _ANSI_RE = re.compile(r"\033\[[0-9;]*m")

    def emit(self, record):
        line = self.format(record)
        clean = self._ANSI_RE.sub("", line)
        _recent_logs.append(clean)


def _atomic_write(path: Path, content: str):
    """Write to a temp file then atomically rename to avoid partial reads."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(content)
    tmp.replace(path)


def _write_stats():
    """Write current stats + state to JSON files for the web UI."""
    data = {
        **_state,
        "stats": {**_stats},
        "uptime": int(time.time() - _state.get("_start_time", time.time())),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    data.pop("_start_time", None)
    try:
        _atomic_write(_STATS_PATH, json.dumps(data))
        _atomic_write(_LOGS_PATH, json.dumps(list(_recent_logs)))
    except OSError:
        pass


async def stats_writer_loop(interval: float = 5.0):
    """Periodically write stats to disk for the web UI."""
    while True:
        _write_stats()
        await asyncio.sleep(interval)


async def heartbeat_loop(ws, worker_id: str, secret: bytes, interval: float = 30.0):
    """Send a signed ``heartbeat`` message to the server every *interval* seconds.

    The heartbeat carries cumulative task counters and the current uptime so the
    server can detect stalled workers.  The loop exits silently on any send
    error (the outer connection loop handles reconnection).
    """
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
    """Drain the result queue and send ``batch_result`` messages to the server.

    Wakes every ``BATCH_FLUSH_INTERVAL`` seconds and collects up to
    ``BATCH_MAX_SIZE`` items from *result_queue*.  If the send fails (e.g.
    connection dropped), all dequeued items are re-queued so they are not lost
    and the loop exits to let the outer connection loop reconnect.
    """
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
            _stats["batches_sent"] += 1
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
    proxy_cycle: itertools.cycle | None,
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
    fetch_delay: float = 4.0,
):
    """Fetch one player profile and enqueue the result for batch delivery.

    Acquires *semaphore* before doing any work so the total number of
    in-flight fetches stays within ``max_concurrent``.  Within the semaphore,
    acquires a token from *rate_limiter* (may sleep) before calling the network.

    On ``PROFILE_PRIVATE`` from RuneMetrics the hiscores endpoint is tried as a
    fallback; if that also fails the task is reported as ``PROFILE_PRIVATE``.

    Sleeps for *fetch_delay* seconds after the fetch completes (inside the
    semaphore hold) to spread load evenly across the hour.

    The result dict placed in *result_queue* matches the schema expected by
    ``batch_sender_loop`` and ultimately the server's ``process_success_item``
    / ``process_error_item`` handlers.
    """
    async with semaphore:
        task_id = task["task_id"]
        username = task["username"]

        proxy_url = next(proxy_cycle) if proxy_cycle else None
        logger.info("\033[36mFetching\033[0m %s%s", username,
                    f" via {proxy_url.split('@')[-1]}" if proxy_url else "")

        await rate_limiter.acquire()
        start_ms = time.time() * 1000
        async with httpx.AsyncClient(proxy=proxy_url) as client:
            data, error = await fetch_profile(client, username)
            if error == "PROFILE_PRIVATE":
                logger.info("Profile private for %s, trying hiscores fallback", username)
                data, error = await fetch_hiscores(client, username)
                if error:
                    error = "PROFILE_PRIVATE"
            elif error == "NOT_A_MEMBER":
                logger.info("Player %s is banned (NOT_A_MEMBER)", username)

        # Retry direct (no proxy) on proxy failure
        if error == "PROXY_ERROR" and proxy_url:
            logger.warning("Proxy failed for %s, retrying direct", username)
            async with httpx.AsyncClient() as direct_client:
                data, error = await fetch_profile(direct_client, username)
                if error == "PROFILE_PRIVATE":
                    data, error = await fetch_hiscores(direct_client, username)
                    if error:
                        error = "PROFILE_PRIVATE"
                elif error == "NOT_A_MEMBER":
                    logger.info("Player %s is banned (NOT_A_MEMBER)", username)

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


async def _proxy_retry_loop(
    api_key: str,
    proxy_state: dict,
    ws,
    worker_id: str,
    secret: bytes,
    max_concurrent: int,
    interval: float = 1800.0,
):
    """Retry Webshare proxy fetch every *interval* seconds (default 30 min).

    When proxies are recovered, updates *proxy_state* in-place and sends a new
    ``ready`` message so the master can push scaled config.
    """
    from runespy_worker.cli import _fetch_webshare_proxies, _test_proxy

    while True:
        await asyncio.sleep(interval)
        logger.info("Retrying Webshare proxy fetch...")

        urls = await asyncio.to_thread(_fetch_webshare_proxies, api_key)
        if not urls:
            logger.warning("Proxy retry failed — will try again in %.0fm", interval / 60)
            continue

        if not await asyncio.to_thread(_test_proxy, urls[0]):
            logger.warning("Proxy connectivity check failed — will try again in %.0fm", interval / 60)
            continue

        # Proxies recovered
        proxy_state["urls"] = urls
        proxy_state["cycle"] = itertools.cycle(urls)
        _state["proxy_count"] = len(urls)
        logger.info("\033[32mProxies recovered\033[0m — %d proxies active", len(urls))

        ready_payload = {"capacity": max_concurrent, "has_proxy": True, "proxy_count": len(urls)}
        ready_msg = build_message("ready", ready_payload, worker_id, secret)
        try:
            await ws.send(ready_msg)
        except Exception:
            pass
        return  # done, proxies are live


async def run(master_url: str, max_concurrent: int = 5, proxy_urls: list[str] | None = None, webshare_api_key: str | None = None):
    """Connect to the master server and run the worker until interrupted.

    Loads credentials from ``~/.runespy/`` (worker_id, private key, shared
    secret) then enters an outer reconnection loop.  On each connection attempt:

    1. Open WebSocket to ``/api/workers/ws/connect?worker_id=<id>``.
    2. Complete the two-factor auth handshake (Ed25519 + HMAC challenge-response).
    3. Apply server-supplied config (fetch delay, concurrency, rate limit).
    4. Advertise capacity with a ``ready`` message.
    5. Dispatch incoming ``assign_batch`` tasks as concurrent coroutines.
    6. Handle live ``config`` updates (rebuild semaphore / rate limiter in-place).
    7. Respond to ``shutdown`` by breaking the inner loop cleanly.

    Connection failures reconnect after 10 s; unexpected errors after 30 s.
    """
    setup_logging()

    worker_id = load_worker_id()
    private_key = load_private_key()
    secret = load_secret()

    _state["worker_id"] = worker_id
    _state["_start_time"] = time.time()
    _state["status"] = "connecting"

    logger.info("Worker ID: %s", worker_id)
    logger.info("Ed25519 public key loaded")
    logger.info("HMAC shared secret loaded (%d bytes)", len(secret))

    proxy_cycle = None
    if proxy_urls:
        proxy_cycle = itertools.cycle(proxy_urls)
        for p in proxy_urls:
            masked = p.split("@")[-1] if "@" in p else p
            logger.info("Proxy: %s", masked)
        logger.info("Rotating across %d proxies", len(proxy_urls))
    _state["proxy_count"] = len(proxy_urls or [])

    # Mutable container so the retry loop can update proxy state mid-connection
    _proxy = {"cycle": proxy_cycle, "urls": proxy_urls or []}

    ws_url = f"{master_url}/api/workers/ws/connect?worker_id={worker_id}"

    stats_task = asyncio.create_task(stats_writer_loop())

    while True:
        try:
            _state["status"] = "connecting"
            _write_stats()
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
                    _state["status"] = "authenticated"
                    _state["connected_since"] = datetime.now(UTC).isoformat()
                    _state["config"] = {
                        "fetch_delay": fetch_delay,
                        "max_concurrent": max_concurrent,
                        "rate_limit_per_hour": rate_limit_per_hour,
                    }
                    logger.info(
                        "\033[32mAuthenticated\033[0m — fetch_delay=%.1fs, max_concurrent=%d, rate_limit=%d/hr",
                        fetch_delay, max_concurrent, rate_limit_per_hour,
                    )
                elif config_msg.get("type") == "error":
                    logger.error("Auth rejected: %s", config_msg.get("error"))
                    break
                rate_limiter = RateLimiter(rate=rate_limit_per_hour)

                # Send ready
                ready_payload = {"capacity": max_concurrent}
                if _proxy["urls"]:
                    ready_payload["has_proxy"] = True
                    ready_payload["proxy_count"] = len(_proxy["urls"])
                ready_msg = build_message("ready", ready_payload, worker_id, secret)
                await ws.send(ready_msg)
                _state["status"] = "running"
                _write_stats()
                logger.info("Sent ready (capacity=%d, proxies=%d) — waiting for tasks", max_concurrent, len(_proxy["urls"]))

                semaphore = asyncio.Semaphore(max_concurrent)
                result_queue: asyncio.Queue = asyncio.Queue()

                heartbeat_task = asyncio.create_task(
                    heartbeat_loop(ws, worker_id, secret)
                )
                batch_task = asyncio.create_task(
                    batch_sender_loop(ws, result_queue, worker_id, secret)
                )

                # Retry proxy fetch in background if we fell back to direct
                proxy_retry_task = None
                if webshare_api_key and not _proxy["urls"]:
                    proxy_retry_task = asyncio.create_task(
                        _proxy_retry_loop(webshare_api_key, _proxy, ws, worker_id, secret, max_concurrent)
                    )

                try:
                    async for raw in ws:
                        msg = json.loads(raw)
                        msg_type = msg.get("type")

                        if msg_type == "assign_batch":
                            tasks = msg.get("payload", {}).get("tasks", [])
                            _stats["batches_received"] += 1
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
                                        _proxy["cycle"], semaphore, rate_limiter, fetch_delay,
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
                            _state["config"] = {
                                "fetch_delay": fetch_delay,
                                "max_concurrent": max_concurrent,
                                "rate_limit_per_hour": rate_limiter._rate,
                            }
                            logger.info(
                                "\033[36mConfig updated\033[0m — fetch_delay=%.1fs, max_concurrent=%d, rate_limit=%d/hr",
                                fetch_delay, max_concurrent, rate_limiter._rate,
                            )
                            # Acknowledge with updated capacity
                            ready_payload = {"capacity": max_concurrent}
                            if _proxy["urls"]:
                                ready_payload["has_proxy"] = True
                                ready_payload["proxy_count"] = len(_proxy["urls"])
                            ready_msg = build_message("ready", ready_payload, worker_id, secret)
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
                    if proxy_retry_task:
                        proxy_retry_task.cancel()

        except (websockets.ConnectionClosed, ConnectionRefusedError, OSError) as e:
            _state["status"] = "reconnecting"
            _state["connected_since"] = None
            _write_stats()
            logger.warning("Connection lost: %s. Reconnecting in 10s...", e)
            await asyncio.sleep(10)
        except Exception as e:
            _state["status"] = "reconnecting"
            _state["connected_since"] = None
            _write_stats()
            logger.error("Error: %s. Reconnecting in 30s...", e)
            await asyncio.sleep(30)
