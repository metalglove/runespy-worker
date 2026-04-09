"""CLI for the RuneSpy distributed worker client.

Commands
--------
register    Generate an Ed25519 keypair, register with the master server,
            and print the worker ID to share with the admin for approval.
            Blocked if a keypair already exists to prevent accidental
            overwriting of credentials.

save-secret Decrypt the encrypted secret blob issued by the admin and write
            the raw 32-byte secret to ``~/.runespy/worker_secret.key``.
            Must be run before ``run``.

run         Connect to the master, authenticate, and start processing tasks.
            Requires all three credential files to be present.

status      Query the REST API for the worker's current status (pending /
            approved).  Does not retrieve or display the shared secret.

All credential files live in ``~/.runespy/``.  See ``crypto.py`` for details
on the key format and secret delivery scheme.
"""

import asyncio
import json

import click


@click.group()
def main():
    """RuneSpy distributed worker client."""


@main.command()
@click.option("--master", required=True, help="Master server WebSocket URL (e.g., wss://tracker.example.com)")
@click.option("--name", required=True, help="Human-readable name for this worker")
def register(master, name):
    """Register this worker with the master server."""
    asyncio.run(_register(master, name))


async def _register(master_url: str, name: str):
    import websockets
    from runespy_worker.crypto import (
        CONFIG_DIR,
        ensure_config_dir,
        generate_keypair,
        get_public_key_b64,
        save_private_key,
        save_worker_id,
    )

    ensure_config_dir()

    key_path = CONFIG_DIR / "worker_key.pem"
    if key_path.exists():
        click.echo(
            f"A keypair already exists at {key_path}.\n"
            "If you meant to re-register, delete it first:\n"
            f"  rm {key_path}",
            err=True,
        )
        return

    # Generate keypair
    private_key, public_key_b64 = generate_keypair()
    save_private_key(private_key)
    click.echo(f"Generated Ed25519 keypair: {key_path}")

    # Connect and register
    ws_url = f"{master_url}/api/workers/ws/register"
    try:
        async with websockets.connect(ws_url) as ws:
            await ws.send(json.dumps({
                "type": "register",
                "payload": {
                    "name": name,
                    "public_key": public_key_b64,
                },
            }))

            response_raw = await ws.recv()
            response = json.loads(response_raw)

            if "error" in response:
                click.echo(f"Registration failed: {response['error']}", err=True)
                return

            worker_id = response.get("worker_id")
            if worker_id:
                save_worker_id(worker_id)
                click.echo(f"Registered as worker: {worker_id}")
                click.echo("Status: pending -- waiting for admin approval.")
                click.echo(f"Public key: {public_key_b64}")
                click.echo("\nOnce approved, run: runespy-worker run --master " + master_url)
            else:
                click.echo("Unexpected response from server", err=True)

    except Exception as e:
        click.echo(f"Failed to connect: {e}", err=True)


@main.command()
@click.option("--master", required=True, help="Master server WebSocket URL")
@click.option("--max-concurrent", default=5, help="Max concurrent fetch tasks")
@click.option("--proxy-url", envvar="PROXY_URL", default=None,
              help="Single HTTP proxy URL (e.g. http://user:pass@host:port)")
@click.option("--webshare-api-key", envvar="WEBSHARE_API_KEY", default=None,
              help="Webshare API key — auto-fetches proxy list and rotates")
def run(master, max_concurrent, proxy_url, webshare_api_key):
    """Connect to master and start processing tasks."""
    from runespy_worker.crypto import CONFIG_DIR

    # Verify we have credentials
    if not (CONFIG_DIR / "worker_key.pem").exists():
        click.echo("No worker key found. Run 'runespy-worker register' first.", err=True)
        return
    if not (CONFIG_DIR / "worker_id").exists():
        click.echo("No worker ID found. Run 'runespy-worker register' first.", err=True)
        return
    if not (CONFIG_DIR / "worker_secret.key").exists():
        click.echo("No shared secret found. Has the admin approved this worker?", err=True)
        click.echo("Check with: runespy-worker status --master " + master)
        return

    if proxy_url and webshare_api_key:
        click.echo("Specify either --proxy-url or --webshare-api-key, not both.", err=True)
        return

    proxy_urls: list[str] = []
    if webshare_api_key:
        proxy_urls = _fetch_webshare_proxies(webshare_api_key)
        if not proxy_urls:
            click.echo("Failed to fetch proxies from Webshare.", err=True)
            return
        click.echo(f"Loaded {len(proxy_urls)} proxies from Webshare")
    elif proxy_url:
        proxy_urls = [proxy_url]

    from runespy_worker.client import run as client_run
    click.echo("Starting worker...")
    asyncio.run(client_run(master, max_concurrent=max_concurrent, proxy_urls=proxy_urls))


def _fetch_webshare_proxies(api_key: str) -> list[str]:
    """Fetch proxy list from Webshare API (synchronous, runs before event loop)."""
    import httpx as _httpx

    proxies: list[str] = []
    page = 1
    while True:
        try:
            resp = _httpx.get(
                "https://proxy.webshare.io/api/v2/proxy/list/",
                params={"mode": "direct", "page": page, "page_size": 100},
                headers={"Authorization": f"Token {api_key}"},
                timeout=15,
            )
            resp.raise_for_status()
        except _httpx.HTTPError as e:
            click.echo(f"Webshare API error: {e}", err=True)
            return []

        data = resp.json()
        for p in data.get("results", []):
            url = f"http://{p['username']}:{p['password']}@{p['proxy_address']}:{p['port']}"
            proxies.append(url)

        if not data.get("next"):
            break
        page += 1

    return proxies


@main.command()
@click.option("--master", required=True, help="Master server WebSocket URL")
def status(master):
    """Check worker approval status and retrieve secret if approved."""
    asyncio.run(_check_status(master))


async def _check_status(master_url: str):
    import httpx
    from runespy_worker.crypto import (
        CONFIG_DIR,
        decrypt_secret,
        get_public_key_b64,
        load_private_key,
        load_worker_id,
        save_secret,
    )

    try:
        worker_id = load_worker_id()
    except FileNotFoundError:
        click.echo("No worker ID found. Run 'runespy-worker register' first.", err=True)
        return

    # Convert ws/wss URL to http/https for REST call
    http_url = master_url.replace("wss://", "https://").replace("ws://", "http://")

    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{http_url}/api/workers/activate", params={"worker_id": worker_id})
        data = resp.json()

    status = data.get("status", "unknown")
    click.echo(f"Worker {worker_id}: {status}")

    if status == "approved" and not (CONFIG_DIR / "worker_secret.key").exists():
        click.echo("Worker is approved but secret not yet saved locally.")
        click.echo("The encrypted secret was provided during admin approval.")
        click.echo("Contact the admin to get your encrypted_secret value, then run:")
        click.echo("  runespy-worker save-secret --encrypted <base64_blob>")


@main.command("save-secret")
@click.option("--encrypted", required=True, help="Base64 encrypted secret from admin")
def save_secret_cmd(encrypted):
    """Save the encrypted shared secret after admin approval."""
    from runespy_worker.crypto import (
        decrypt_secret,
        get_public_key_b64,
        load_private_key,
        save_secret,
    )

    private_key = load_private_key()
    public_key_b64 = get_public_key_b64(private_key)

    try:
        secret = decrypt_secret(encrypted, public_key_b64)
        path = save_secret(secret)
        click.echo(f"Shared secret saved to {path}")
        click.echo("You can now run: runespy-worker run --master <url>")
    except Exception as e:
        click.echo(f"Failed to decrypt secret: {e}", err=True)


if __name__ == "__main__":
    main()
