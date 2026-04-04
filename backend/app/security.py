from __future__ import annotations

import base64
import hashlib
from typing import Optional, Tuple
import hmac
import time
from cryptography.fernet import Fernet, InvalidToken
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, VerificationError, InvalidHashError

from .config import settings

_ph = PasswordHasher()


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


# --- Password hashing (argon2id with automatic per-user salt) ---
def hash_password(password: str) -> str:
    """Hash a password using argon2id with a random per-user salt."""
    return _ph.hash(password or "")


def _is_legacy_hash(password_hash: str) -> bool:
    """Detect old SHA256 hex-digest hashes (64 hex chars, no $ prefix)."""
    return bool(password_hash and len(password_hash) == 64 and not password_hash.startswith("$"))


def _verify_legacy(password: str, password_hash: str) -> bool:
    """Verify against old SHA256+secret scheme for migration."""
    digest = hashlib.sha256((settings.secret_key + ":" + (password or "")).encode("utf-8")).hexdigest()
    return hmac.compare_digest(digest, password_hash or "")


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against its hash.

    Supports both new argon2id hashes and legacy SHA256 hashes for
    transparent migration. When a legacy hash matches, the caller should
    re-hash with hash_password() and update the stored hash.
    """
    if not password_hash:
        return False
    # Legacy SHA256 hashes: 64-char hex without $ prefix
    if _is_legacy_hash(password_hash):
        return _verify_legacy(password, password_hash)
    # Argon2id hash
    try:
        return _ph.verify(password_hash, password or "")
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False
    except Exception:
        return False


def needs_rehash(password_hash: str) -> bool:
    """Check if a password hash should be upgraded (legacy or weak params)."""
    if _is_legacy_hash(password_hash):
        return True
    try:
        return _ph.check_needs_rehash(password_hash)
    except Exception:
        return False


# --- Password reset tokens (HMAC-signed, time-limited) ---
def sign_reset_token(user_id: str, ttl_seconds: int = 3600) -> str:
    """Create a signed password-reset token valid for ttl_seconds (default 1h)."""
    now = int(time.time())
    exp = now + ttl_seconds
    payload = f"reset:{user_id}:{exp}".encode("utf-8")
    sig = hmac.new(settings.secret_key.encode("utf-8"), payload, hashlib.sha256).digest()
    return f"{_b64url_encode(payload)}.{_b64url_encode(sig)}"


def verify_reset_token(token: str) -> Optional[str]:
    """Verify a reset token. Returns user_id if valid, None otherwise."""
    try:
        if not token:
            return None
        parts = token.split(".")
        if len(parts) != 2:
            return None
        payload_b = _b64url_decode(parts[0])
        sig_b = _b64url_decode(parts[1])
        expected = hmac.new(settings.secret_key.encode("utf-8"), payload_b, hashlib.sha256).digest()
        if not hmac.compare_digest(sig_b, expected):
            return None
        payload = payload_b.decode("utf-8")
        prefix, user_id, exp_s = payload.split(":", 2)
        if prefix != "reset":
            return None
        if int(time.time()) > int(exp_s):
            return None
        return user_id
    except Exception:
        return None


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
