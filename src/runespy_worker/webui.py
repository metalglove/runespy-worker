from pathlib import Path
from subprocess import run, CalledProcessError, Popen

from flask import Flask, request, redirect, url_for

app = Flask(__name__)
RUNE_HOME = Path.home() / ".runespy"
MASTER_URL = "wss://runespy.com"

# Track the worker process started by this app
worker_process: Popen | None = None


def read_worker_files():
    wid_path = RUNE_HOME / "worker_id"
    key_path = RUNE_HOME / "worker_key.pem"
    secret_path = RUNE_HOME / "worker_secret.key"
    name_path = RUNE_HOME / "worker_name"

    wid = wid_path.read_text().strip() if wid_path.exists() else None
    key_pem = key_path.read_bytes() if key_path.exists() else None
    secret = secret_path.read_bytes() if secret_path.exists() else None
    name = name_path.read_text().strip() if name_path.exists() else None
    return wid, key_pem, secret, name


def write_worker_name(name: str) -> None:
    RUNE_HOME.mkdir(parents=True, exist_ok=True)
    (RUNE_HOME / "worker_name").write_text(name.strip())


def is_worker_running() -> bool:
    """Check if the worker process we started is still running."""
    global worker_process
    if worker_process is None:
        return False
    return worker_process.poll() is None  # None means still running


def start_worker():
    """Start worker if not already running."""
    global worker_process
    if not is_worker_running():
        worker_process = Popen(
            ["runespy-worker", "run", "--master", MASTER_URL]
        )


@app.route("/", methods=["GET"])
def index():
    worker_id, key_pem, secret, worker_name = read_worker_files()

    creds_status = "yes" if (worker_id and key_pem and secret) else "no"
    secret_saved = "yes" if secret else "no"
    worker_running = "yes" if is_worker_running() else "no"

    # Name input: only show textbox if we don't have a saved name yet.
    if worker_name:
        name_html = f"<p>Worker name: <code>{worker_name}</code></p>"
        name_input = ""
    else:
        name_html = "<p>Worker name: <em>not set</em></p>"
        name_input = """
      <p>Set worker name: <input name="name" value="your-machine-name"/></p>
"""

    # Secret textarea only if not yet saved.
    secret_form_html = ""
    if not secret:
        secret_form_html = """
    <form method="post" action="/save-secret">
      <textarea name="encrypted" rows="4" cols="80"
        placeholder="Paste encrypted_secret base64 here"></textarea><br/>
      <button type="submit">Save secret</button>
    </form>
"""

    return f"""
<html>
  <body>
    <h1>RuneSpy Worker Web UI</h1>

    <h2>1. Register</h2>
    <p>Master URL: <code>{MASTER_URL}</code></p>
    {name_html}
    <form method="post" action="/register">
      {name_input}
      <button type="submit">Register</button>
    </form>
    <p>Current worker ID: <code>{worker_id or "not registered"}</code></p>

    <h2>2. Secret</h2>
    <p>Credentials present: <code>{creds_status}</code></p>
    <p>Secret saved: <code>{secret_saved}</code></p>
    {secret_form_html}

    <h2>3. Worker</h2>
    <p>Worker running (this container): <code>{worker_running}</code></p>
    <form method="post" action="/run-worker">
      <button type="submit" {"disabled" if not secret else ""}>Start worker</button>
    </form>
    <form method="post" action="/restart-worker">
      <button type="submit" {"disabled" if not secret else ""}>Restart worker</button>
    </form>
  </body>
</html>
"""


@app.route("/register", methods=["POST"])
def register():
    _, _, _, existing_name = read_worker_files()
    name = existing_name or request.form.get("name", "").strip() or "your-machine-name"

    # Persist name so it's constant after first set.
    write_worker_name(name)

    try:
        run(
            [
                "runespy-worker",
                "register",
                "--master",
                MASTER_URL,
                "--name",
                name,
            ],
            check=True,
        )
    except CalledProcessError as e:
        return f"Error running register: {e}", 500
    return redirect(url_for("index"))


@app.route("/save-secret", methods=["POST"])
def save_secret():
    encrypted = request.form.get("encrypted", "").strip()
    if not encrypted:
        return "Missing encrypted secret", 400

    try:
        run(
            ["runespy-worker", "save-secret", "--encrypted", encrypted],
            check=True,
        )
    except CalledProcessError as e:
        return f"Error running save-secret: {e}", 500

    # Secret is on disk now; auto-start worker.
    start_worker()
    return redirect(url_for("index"))


@app.route("/run-worker", methods=["POST"])
def run_worker():
    start_worker()
    return redirect(url_for("index"))


@app.route("/restart-worker", methods=["POST"])
def restart_worker():
    global worker_process
    if is_worker_running():
        worker_process.terminate()
    start_worker()
    return redirect(url_for("index"))


def main():
    # On container start, if secret exists, auto-start worker.
    _, _, secret, _ = read_worker_files()
    if secret:
        start_worker()

    app.run(host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()