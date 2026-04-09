"""Minimal web UI for runespy-worker setup, monitoring, and proxy configuration.

Wraps the CLI commands (register, save-secret, run) and reads the stats/logs
files written by the worker process to display a live dashboard.
"""

import json
import socket
from pathlib import Path
from subprocess import CalledProcessError, Popen, run

from flask import Flask, redirect, render_template, request, url_for

app = Flask(__name__)
RUNE_HOME = Path.home() / ".runespy"
MASTER_URL = "wss://runespy.com"

# Worker subprocess handle
_worker_proc: Popen | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_file(name: str) -> str | None:
    p = RUNE_HOME / name
    return p.read_text().strip() if p.exists() else None


def _has_file(name: str) -> bool:
    return (RUNE_HOME / name).exists()


def _read_proxy_config() -> tuple[str | None, str | None]:
    """Read saved proxy configuration."""
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


def _is_running() -> bool:
    global _worker_proc
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
    if not _is_running():
        _worker_proc = Popen(_build_worker_cmd())


def _stop_worker():
    global _worker_proc
    if _is_running():
        _worker_proc.terminate()
        try:
            _worker_proc.wait(timeout=5)
        except Exception:
            _worker_proc.kill()
        _worker_proc = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    worker_id = _read_file("worker_id")
    worker_name = _read_file("worker_name")
    has_secret = _has_file("worker_secret.key")
    is_running = _is_running()

    stats = _read_stats()
    logs = _read_logs()
    webshare_api_key, proxy_url = _read_proxy_config()

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

    return render_template("index.html",
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
        flash_error=flash_error,
        flash_success=flash_success,
    )


@app.route("/register", methods=["POST"])
def register():
    name = request.form.get("name", "").strip()
    if not name:
        name = socket.gethostname()

    # Persist name
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
        return redirect(url_for("index", error="Choose either Webshare API key or a single proxy URL, not both.", tab="settings"))

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


# ---------------------------------------------------------------------------
# JSON API for programmatic access
# ---------------------------------------------------------------------------

@app.route("/api/stats")
def api_stats():
    stats = _read_stats() or {}
    stats["logs"] = _read_logs()[-50:]
    return stats


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    # Auto-start worker if credentials are present
    if _has_file("worker_secret.key"):
        _start_worker()

    app.run(host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
