"""Message envelope construction and HMAC verification for the worker protocol.

Messages use a JSON envelope on an Ed25519-authenticated WebSocket. The optional
legacy HMAC field is retained for callers but is not required by the server:

    {
        "type":      "<message_type>",
        "id":        "<uuid4>",          # unique per message; used for dedup
        "ts":        1741305600.123,      # Unix timestamp
        "worker_id": "<worker_uuid>",
        "payload":   { ... },            # message-type-specific data
        "hmac":      "<hex_digest>"      # HMAC-SHA256 over all other fields
    }

The HMAC is computed over a canonical JSON representation of the message
with the "hmac" key excluded (since its value isn't known yet at signing
time). Keys are sorted and whitespace is stripped to ensure a deterministic
byte sequence regardless of insertion order.

Server-to-worker messages (challenge, config, assign_batch, heartbeat_ack,
shutdown, revoke) are not HMAC-signed by the server; the worker accepts them
based on the authenticated WebSocket session alone.
"""

import hashlib
import hmac as _hmac
import json
import time
import uuid


def canonical_json(msg: dict) -> bytes:
    """Produce a deterministic byte representation of *msg* for HMAC signing.

    The "hmac" key is excluded so the digest can be computed before it is
    embedded.  Keys are sorted and separators stripped so the output is
    identical regardless of how the dict was constructed.
    """
    signable = {k: v for k, v in msg.items() if k != "hmac"}
    return json.dumps(signable, sort_keys=True, separators=(",", ":")).encode()


def build_message(msg_type: str, payload: dict, worker_id: str, secret: bytes | None = None) -> str:
    """Build a complete signed message envelope and return it as a JSON string.

    Assigns a fresh UUID and current timestamp, then appends the HMAC digest
    computed over the canonical form of the message.
    """
    msg = {
        "type": msg_type,
        "id": str(uuid.uuid4()),
        "ts": time.time(),
        "worker_id": worker_id,
        "payload": payload,
    }
    if secret is not None:
        msg["hmac"] = _hmac.new(secret, canonical_json(msg), hashlib.sha256).hexdigest()
    return json.dumps(msg)


def verify_hmac(raw: str, secret: bytes) -> dict | None:
    """Parse *raw* JSON and verify its HMAC signature against *secret*.

    Returns the parsed message dict if the signature is valid, or None if it
    has been tampered with.

    Messages that carry no "hmac" field are returned as-is — the server does
    not sign its own outbound messages, so challenge / assign_batch / config
    messages pass through unsigned during normal operation.
    """
    msg = json.loads(raw)
    provided = msg.get("hmac")
    if not provided:
        return msg

    expected = _hmac.new(secret, canonical_json(msg), hashlib.sha256).hexdigest()
    if not _hmac.compare_digest(provided, expected):
        return None
    return msg
