"""Ed25519 key management and challenge-response for the worker client."""

import base64
import hashlib
import hmac as _hmac
import os
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
    load_pem_private_key,
)

CONFIG_DIR = Path.home() / ".runespy"


def ensure_config_dir() -> Path:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    return CONFIG_DIR


def generate_keypair() -> tuple[Ed25519PrivateKey, str]:
    """Generate Ed25519 keypair. Returns (private_key, public_key_b64)."""
    private_key = Ed25519PrivateKey.generate()
    pub_raw = private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return private_key, base64.b64encode(pub_raw).decode()


def save_private_key(key: Ed25519PrivateKey, path: Path | None = None) -> Path:
    """Save private key to PEM file."""
    if path is None:
        path = ensure_config_dir() / "worker_key.pem"
    pem = key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    path.write_bytes(pem)
    os.chmod(path, 0o600)
    return path


def load_private_key(path: Path | None = None) -> Ed25519PrivateKey:
    """Load private key from PEM file."""
    if path is None:
        path = CONFIG_DIR / "worker_key.pem"
    pem = path.read_bytes()
    key = load_pem_private_key(pem, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("Expected Ed25519 private key")
    return key


def get_public_key_b64(private_key: Ed25519PrivateKey) -> str:
    """Get base64-encoded public key from private key."""
    pub_raw = private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return base64.b64encode(pub_raw).decode()


def save_secret(secret: bytes, path: Path | None = None) -> Path:
    """Save shared secret to file."""
    if path is None:
        path = ensure_config_dir() / "worker_secret.key"
    path.write_bytes(secret)
    os.chmod(path, 0o600)
    return path


def load_secret(path: Path | None = None) -> bytes:
    """Load shared secret from file."""
    if path is None:
        path = CONFIG_DIR / "worker_secret.key"
    return path.read_bytes()


def save_worker_id(worker_id: str, path: Path | None = None) -> Path:
    """Save worker ID to file."""
    if path is None:
        path = ensure_config_dir() / "worker_id"
    path.write_text(worker_id)
    return path


def load_worker_id(path: Path | None = None) -> str:
    """Load worker ID from file."""
    if path is None:
        path = CONFIG_DIR / "worker_id"
    return path.read_text().strip()


def decrypt_secret(encrypted_b64: str, public_key_b64: str) -> bytes:
    """Decrypt shared secret from the encrypted blob returned during approval."""
    payload = base64.b64decode(encrypted_b64)
    pub_raw = base64.b64decode(public_key_b64)

    nonce = payload[:12]
    xored_key = payload[12:44]
    ciphertext = payload[44:]

    aes_key = bytes(a ^ b for a, b in zip(xored_key, pub_raw))
    aesgcm = AESGCM(aes_key)
    return aesgcm.decrypt(nonce, ciphertext, None)


def sign_challenge(private_key: Ed25519PrivateKey, nonce_hex: str) -> str:
    """Sign a challenge nonce with Ed25519. Returns hex signature."""
    signature = private_key.sign(bytes.fromhex(nonce_hex))
    return signature.hex()


def hmac_challenge(secret: bytes, nonce_hex: str) -> str:
    """Compute HMAC-SHA256 of the nonce. Returns hex digest."""
    return _hmac.new(secret, bytes.fromhex(nonce_hex), hashlib.sha256).hexdigest()
