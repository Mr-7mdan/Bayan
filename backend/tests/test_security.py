"""Unit tests for auth primitives in app.security (spec 25).

Pure functions, no fixtures. Complements test_auth_tokens.py (session tokens).
Run from backend/:  python -m pytest tests/test_security.py
"""
import hashlib

from app.security import (
    hash_password,
    verify_password,
    needs_rehash,
    sign_reset_token,
    verify_reset_token,
    sign_embed_token,
    verify_embed_token,
)


# --- Password hashing ---
def test_hash_verify_roundtrip():
    h = hash_password("s3cret-pw")
    assert h != "s3cret-pw"  # never stored plaintext
    assert verify_password("s3cret-pw", h) is True


def test_verify_wrong_password_false():
    h = hash_password("s3cret-pw")
    assert verify_password("wrong", h) is False


def test_verify_empty_hash_false():
    assert verify_password("anything", "") is False


def test_needs_rehash_on_legacy_hash():
    # Legacy hashes are 64-char hex (sha256) with no argon2 "$" prefix.
    legacy = hashlib.sha256(b"x").hexdigest()
    assert len(legacy) == 64 and "$" not in legacy
    assert needs_rehash(legacy) is True
    assert needs_rehash(hash_password("fresh")) is False


# --- Reset tokens ---
def test_reset_token_roundtrip():
    t = sign_reset_token("user-123", ttl_seconds=3600)
    assert verify_reset_token(t) == "user-123"


def test_reset_token_expired():
    t = sign_reset_token("user-123", ttl_seconds=-1)
    assert verify_reset_token(t) is None


def test_reset_token_tampered():
    t = sign_reset_token("user-123")
    assert verify_reset_token(t[:-2] + "xx") is None
    assert verify_reset_token("not-a-token") is None


# --- Embed tokens ---
def test_embed_token_valid():
    token, exp = sign_embed_token("pub-abc", ttl_seconds=3600)
    assert exp > 0
    assert verify_embed_token(token, "pub-abc") is True


def test_embed_token_wrong_public_id():
    token, _ = sign_embed_token("pub-abc", ttl_seconds=3600)
    assert verify_embed_token(token, "pub-xyz") is False


def test_embed_token_tampered():
    token, _ = sign_embed_token("pub-abc", ttl_seconds=3600)
    assert verify_embed_token(token[:-2] + "xx", "pub-abc") is False
    assert verify_embed_token("", "pub-abc") is False
