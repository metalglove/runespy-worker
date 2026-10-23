"""Ed25519 worker identity and local key storage.

The server authenticates possession of the private key by verifying a fresh
challenge signature. No shared secret or recipient-encryption scheme is used.
"""

import base64
import os
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
    load_pem_private_key,
)

CONFIG_DIR = Path.home() / ".runespy"


def ensure_config_dir() -> Path:
    """Create ~/.runespy/ if it does not exist. Returns the path."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    return CONFIG_DIR


def generate_keypair() -> tuple[Ed25519PrivateKey, str]:
    """Generate a new Ed25519 keypair.

    Returns a tuple of (private_key, public_key_b64) where public_key_b64 is
    the base64-encoded 32-byte raw public key sent to the server at registration.
    """
    private_key = Ed25519PrivateKey.generate()
    pub_raw = private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return private_key, base64.b64encode(pub_raw).decode()


def save_private_key(key: Ed25519PrivateKey, path: Path | None = None) -> Path:
    """Serialise *key* to a PEM/PKCS8 file and set permissions to 0600."""
    if path is None:
        path = ensure_config_dir() / "worker_key.pem"
    pem = key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    path.write_bytes(pem)
    os.chmod(path, 0o600)
    return path


def load_private_key(path: Path | None = None) -> Ed25519PrivateKey:
    """Load an Ed25519 private key from a PEM file."""
    if path is None:
        path = CONFIG_DIR / "worker_key.pem"
    pem = path.read_bytes()
    key = load_pem_private_key(pem, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("Expected Ed25519 private key")
    return key


def get_public_key_b64(private_key: Ed25519PrivateKey) -> str:
    """Return the base64-encoded 32-byte raw public key for *private_key*."""
    pub_raw = private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return base64.b64encode(pub_raw).decode()


def save_worker_id(worker_id: str, path: Path | None = None) -> Path:
    """Write the worker UUID string to ~/.runespy/worker_id."""
    if path is None:
        path = ensure_config_dir() / "worker_id"
    path.write_text(worker_id)
    return path


def load_worker_id(path: Path | None = None) -> str:
    """Read and return the worker UUID string from ~/.runespy/worker_id."""
    if path is None:
        path = CONFIG_DIR / "worker_id"
    return path.read_text().strip()


def sign_challenge(private_key: Ed25519PrivateKey, nonce_hex: str) -> str:
    """Sign the hex-encoded challenge nonce with Ed25519.

    Returns the 64-byte signature as a hex string. The server verifies this
    against the public key stored at registration to confirm the worker's
    identity.
    """
    signature = private_key.sign(bytes.fromhex(nonce_hex))
    return signature.hex()
