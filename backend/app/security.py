from __future__ import annotations

import base64
import hashlib
from typing import Optional, Tuple
import hmac
import time
from cryptography.fernet import Fernet, InvalidToken

from .config import settings


def _derive_key(secret: str) -> bytes:
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def _fernet() -> Fernet:
    return Fernet(_derive_key(settings.secret_key))


def encrypt_text(plain: str) -> str:
    f = _fernet()
    token = f.encrypt(plain.encode("utf-8"))
    return token.decode("utf-8")


def decrypt_text(token: str) -> Optional[str]:
    f = _fernet()
    try:
        return f.decrypt(token.encode("utf-8")).decode("utf-8")
    except (InvalidToken, Exception):
        return None


# --- Simple password hashing helpers (for demo/local use) ---
def hash_password(password: str) -> str:
    """Deterministically hash a password using the app secret.

    NOTE: For production systems, use a dedicated password hashing algorithm
    like argon2 or bcrypt with per-user salts. This helper is intentionally
    simple for local/demo environments.
    """
    digest = hashlib.sha256((settings.secret_key + ":" + (password or "")).encode("utf-8")).hexdigest()
    return digest


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return hash_password(password) == (password_hash or "")
    except Exception:
        return False


# --- Server-signed short-lived embed tokens ---
def _b64url_encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("utf-8").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    pad = '=' * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode('utf-8'))


def sign_embed_token(public_id: str, ttl_seconds: int) -> Tuple[str, int]:
    now = int(time.time())
    # Clamp TTL: 60s .. 30 days
    ttl = max(60, min(ttl_seconds or 0, 60 * 60 * 24 * 30))
    exp = now + ttl
    payload = f"{public_id}.{exp}".encode("utf-8")
    sig = hmac.new(settings.secret_key.encode("utf-8"), payload, hashlib.sha256).digest()
    token = f"{_b64url_encode(payload)}.{_b64url_encode(sig)}"
    return token, exp


def verify_embed_token(token: str, public_id: str) -> bool:
    try:
        if not token:
            return False
        parts = token.split('.')
        if len(parts) != 2:
            return False
        payload_b = _b64url_decode(parts[0])
        sig_b = _b64url_decode(parts[1])
        expected_sig = hmac.new(settings.secret_key.encode("utf-8"), payload_b, hashlib.sha256).digest()
        if not hmac.compare_digest(sig_b, expected_sig):
            return False
        payload = payload_b.decode('utf-8')
        pub, exp_s = payload.split('.', 1)
        if pub != public_id:
            return False
        exp = int(exp_s)
        now = int(time.time())
        return now <= exp
    except Exception:
        return False
