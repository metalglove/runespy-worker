"""Message envelope construction and canonical JSON for the worker protocol."""

import hashlib
import hmac as _hmac
import json
import time
import uuid


def canonical_json(msg: dict) -> bytes:
    """Build canonical JSON for HMAC signing (excludes 'hmac' key)."""
    signable = {k: v for k, v in msg.items() if k != "hmac"}
    return json.dumps(signable, sort_keys=True, separators=(",", ":")).encode()


def build_message(msg_type: str, payload: dict, worker_id: str, secret: bytes) -> str:
    """Build a signed message envelope."""
    msg = {
        "type": msg_type,
        "id": str(uuid.uuid4()),
        "ts": time.time(),
        "worker_id": worker_id,
        "payload": payload,
    }
    msg["hmac"] = _hmac.new(secret, canonical_json(msg), hashlib.sha256).hexdigest()
    return json.dumps(msg)


def verify_hmac(raw: str, secret: bytes) -> dict | None:
    """Parse and verify an incoming message. Returns parsed dict or None."""
    msg = json.loads(raw)
    provided = msg.get("hmac")
    if not provided:
        # Server messages during initial handshake may not have HMAC
        return msg

    expected = _hmac.new(secret, canonical_json(msg), hashlib.sha256).hexdigest()
    if not _hmac.compare_digest(provided, expected):
        return None
    return msg
