"""CLI for the RuneSpy distributed worker client."""

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
        ensure_config_dir,
        generate_keypair,
        get_public_key_b64,
        save_private_key,
        save_worker_id,
    )

    ensure_config_dir()

    # Generate keypair
    private_key, public_key_b64 = generate_keypair()
    key_path = save_private_key(private_key)
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
def run(master, max_concurrent):
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

    from runespy_worker.client import run as client_run
    click.echo("Starting worker...")
    asyncio.run(client_run(master, max_concurrent=max_concurrent))


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
