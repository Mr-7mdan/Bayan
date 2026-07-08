"""Self-contained checks for the HMAC session-token layer (spec 02).

Run from backend/:  python -m pytest tests/test_auth_tokens.py
or directly:        python tests/test_auth_tokens.py
"""
from app.security import sign_session_token, verify_session_token, _pw_fingerprint


def test_roundtrip():
    t = sign_session_token("u1", "$argon2id$fakehash", 60)
    res = verify_session_token(t)
    assert res is not None
    uid, fp = res
    assert uid == "u1" and fp == _pw_fingerprint("$argon2id$fakehash")


def test_tampered_signature_rejected():
    t = sign_session_token("u1", "$argon2id$fakehash", 60)
    assert verify_session_token(t[:-2] + "xx") is None


def test_expired_rejected():
    assert verify_session_token(sign_session_token("u1", "h", -10)) is None


def test_password_change_invalidates():
    # A different stored hash yields a different fingerprint => old token no longer matches.
    t = sign_session_token("u1", "hash-A", 60)
    _, fp = verify_session_token(t)
    assert fp != _pw_fingerprint("hash-B")


if __name__ == "__main__":
    test_roundtrip()
    test_tampered_signature_rejected()
    test_expired_rejected()
    test_password_change_invalidates()
    print("OK: all auth token self-tests passed")
