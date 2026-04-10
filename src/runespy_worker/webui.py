"""Minimal web UI for runespy-worker setup, monitoring, and proxy configuration.

Wraps the CLI commands (register, save-secret, run) and reads the stats/logs
files written by the worker process to display a live dashboard.
"""

import json
import socket
import threading
from pathlib import Path
from subprocess import CalledProcessError, Popen, run

import requests
from flask import Flask, jsonify, redirect, render_template, request, url_for

app = Flask(__name__)
RUNE_HOME = Path.home() / ".runespy"
MASTER_URL = "wss://runespy.com"

_worker_proc: Popen | None = None
_proc_lock = threading.Lock()


def _read_file(name: str) -> str | None:
    p = RUNE_HOME / name
    return p.read_text().strip() if p.exists() else None


def _has_file(name: str) -> bool:
    return (RUNE_HOME / name).exists()


def _read_proxy_config() -> tuple[str | None, str | None]:
    webshare_key = _read_file("webshare_api_key")
    proxy_url = _read_file("proxy_url")
    return webshare_key, proxy_url


def _save_proxy_config(webshare_api_key: str | None, proxy_url: str | None):
    RUNE_HOME.mkdir(parents=True, exist_ok=True)
    key_path = RUNE_HOME / "webshare_api_key"
    url_path = RUNE_HOME / "proxy_url"

    if webshare_api_key:
      key_path.write_text(webshare_api_key.strip())
      url_path.unlink(missing_ok=True)
    elif proxy_url:
      url_path.write_text(proxy_url.strip())
      key_path.unlink(missing_ok=True)
    else:
      key_path.unlink(missing_ok=True)
      url_path.unlink(missing_ok=True)


def _read_stats() -> dict | None:
    p = RUNE_HOME / "stats.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _read_logs() -> list[str]:
    p = RUNE_HOME / "logs.json"
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return []


def _format_uptime(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h}h {m}m"


def _format_bytes(num: int | float | None) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num or 0)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.1f} {unit}"
        value /= 1024
    return "0 B"


def _fetch_webshare_json(path: str) -> dict | None:
    webshare_api_key, _ = _read_proxy_config()
    if not webshare_api_key:
        return None

    try:
        res = requests.get(
            f"https://proxy.webshare.io/api/v2/{path}",
            headers={"Authorization": f"Token {webshare_api_key}"},
            timeout=10,
        )
        res.raise_for_status()
        return res.json()
    except Exception as e:
        print(f"Failed Webshare request for {path}: {e}")
        return None


def _fetch_webshare_stats() -> dict | None:
    webshare_api_key, _ = _read_proxy_config()
    if not webshare_api_key:
        return None

    aggregate = _fetch_webshare_json("stats/aggregate/")
    if not aggregate:
        return None

    bandwidth_total = aggregate.get("bandwidth_total", 0)
    bandwidth_projected = aggregate.get("bandwidth_projected", 0)

    bandwidth_limit_gb = None
    bandwidth_limit_human = "—"

    subscription = _fetch_webshare_json("subscription/")
    if subscription:
        plan_info = subscription.get("plan")
        plan_id = None

        if isinstance(plan_info, dict):
            plan_id = plan_info.get("id")
        elif isinstance(plan_info, int):
            plan_id = plan_info
        else:
            plan_id = subscription.get("plan_id")

        if plan_id:
            plan = _fetch_webshare_json(f"subscription/plan/{plan_id}/")
            if plan:
                bandwidth_limit_gb = plan.get("bandwidth_limit")
                if bandwidth_limit_gb == 0:
                    bandwidth_limit_human = "Unlimited"
                elif bandwidth_limit_gb is not None:
                    bandwidth_limit_human = f"{bandwidth_limit_gb:g} GB"

    return {
        "bandwidth_total": bandwidth_total,
        "bandwidth_projected": bandwidth_projected,
        "bandwidth_total_human": _format_bytes(bandwidth_total),
        "bandwidth_projected_human": _format_bytes(bandwidth_projected),
        "bandwidth_limit_gb": bandwidth_limit_gb,
        "bandwidth_limit_human": bandwidth_limit_human,
    }


def _is_running() -> bool:
    with _proc_lock:
        if _worker_proc is None:
            return False
        return _worker_proc.poll() is None


def _build_worker_cmd() -> list[str]:
    cmd = ["uv", "run", "runespy-worker", "run", "--master", MASTER_URL]
    webshare_key, proxy_url = _read_proxy_config()
    if webshare_key:
        cmd += ["--webshare-api-key", webshare_key]
    elif proxy_url:
        cmd += ["--proxy-url", proxy_url]
    return cmd


def _start_worker():
    global _worker_proc
    with _proc_lock:
        if _worker_proc is not None and _worker_proc.poll() is None:
            return
        try:
            _worker_proc = Popen(_build_worker_cmd())
        except FileNotFoundError:
            pass


def _stop_worker():
    global _worker_proc
    with _proc_lock:
        if _worker_proc is None or _worker_proc.poll() is not None:
            _worker_proc = None
            return
        _worker_proc.terminate()
        try:
            _worker_proc.wait(timeout=5)
        except Exception:
            _worker_proc.kill()
        _worker_proc = None


@app.route("/")
def index():
    worker_id = _read_file("worker_id")
    worker_name = _read_file("worker_name")
    has_secret = _has_file("worker_secret.key")
    is_running = _is_running()

    stats = _read_stats()
    logs = _read_logs()
    webshare_api_key, proxy_url = _read_proxy_config()
    proxy_stats = _fetch_webshare_stats()

    worker_status = None
    uptime_display = "0s"
    proxy_count = 0

    if stats:
        worker_status = stats.get("status")
        uptime_display = _format_uptime(stats.get("uptime", 0))
        proxy_count = stats.get("proxy_count", 0)
    elif is_running:
        worker_status = "starting"

    flash_error = request.args.get("error")
    flash_success = request.args.get("success")

    return render_template(
        "index.html",
        worker_id=worker_id,
        worker_name=worker_name,
        has_secret=has_secret,
        is_running=is_running,
        worker_status=worker_status,
        stats=stats,
        logs=logs,
        uptime_display=uptime_display,
        webshare_api_key=webshare_api_key,
        proxy_url=proxy_url,
        proxy_count=proxy_count,
        proxy_stats=proxy_stats,
        flash_error=flash_error,
        flash_success=flash_success,
    )


@app.route("/register", methods=["POST"])
def register():
    name = request.form.get("name", "").strip()
    if not name:
        name = socket.gethostname()

    RUNE_HOME.mkdir(parents=True, exist_ok=True)
    (RUNE_HOME / "worker_name").write_text(name)

    try:
        run(
            ["uv", "run", "runespy-worker", "register", "--master", MASTER_URL, "--name", name],
            check=True,
        )
    except CalledProcessError as e:
        return redirect(url_for("index", error=f"Registration failed: {e}"))

    return redirect(url_for("index", success="Registered successfully. Share your worker ID with the admin."))


@app.route("/save-secret", methods=["POST"])
def save_secret():
    encrypted = request.form.get("encrypted", "").strip()
    if not encrypted:
        return redirect(url_for("index", error="No encrypted secret provided."))

    try:
        run(
            ["uv", "run", "runespy-worker", "save-secret", "--encrypted", encrypted],
            check=True,
        )
    except CalledProcessError as e:
        return redirect(url_for("index", error=f"Failed to save secret: {e}"))

    _start_worker()
    return redirect(url_for("index", success="Secret saved. Worker started."))


@app.route("/save-proxy-config", methods=["POST"])
def save_proxy_config():
    webshare_key = request.form.get("webshare_api_key", "").strip() or None
    proxy_url = request.form.get("proxy_url", "").strip() or None

    if webshare_key and proxy_url:
        return redirect(
            url_for(
                "index",
                error="Choose either Webshare API key or a single proxy URL, not both.",
                tab="settings",
            )
        )

    _save_proxy_config(webshare_key, proxy_url)

    if _is_running():
        _stop_worker()
        _start_worker()
        return redirect(url_for("index", success="Proxy config saved. Worker restarted.", tab="settings"))

    return redirect(url_for("index", success="Proxy config saved.", tab="settings"))


@app.route("/run-worker", methods=["POST"])
def run_worker():
    if not _has_file("worker_secret.key"):
        return redirect(url_for("index", error="Save the shared secret first."))
    _start_worker()
    return redirect(url_for("index", success="Worker started."))


@app.route("/restart-worker", methods=["POST"])
def restart_worker():
    if not _has_file("worker_secret.key"):
        return redirect(url_for("index", error="Save the shared secret first."))
    _stop_worker()
    _start_worker()
    return redirect(url_for("index", success="Worker restarted."))


@app.route("/stop-worker", methods=["POST"])
def stop_worker():
    _stop_worker()
    return redirect(url_for("index", success="Worker stopped."))


@app.route("/api/stats")
def api_stats():
    stats = _read_stats() or {}
    proxy_stats = _fetch_webshare_stats()

    response = dict(stats)
    response["logs"] = _read_logs()
    response["is_running"] = _is_running()

    if proxy_stats:
        response["proxyStats"] = {
            "bandwidthTotalHuman": proxy_stats["bandwidth_total_human"],
            "bandwidthProjectedHuman": proxy_stats["bandwidth_projected_human"],
            "bandwidthLimitHuman": proxy_stats["bandwidth_limit_human"],
        }

    return jsonify(response)


def main():
    import os

    if not _read_file("webshare_api_key") and not _read_file("proxy_url"):
        env_key = os.environ.get("WEBSHARE_API_KEY")
        env_proxy = os.environ.get("PROXY_URL")
        if env_key or env_proxy:
            _save_proxy_config(env_key, env_proxy)

    if _has_file("worker_secret.key"):
        _start_worker()

    app.run(host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()