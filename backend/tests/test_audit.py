"""Spec 05 audit logging: sanitization, no-raise isolation, and read API.

Run:  cd backend && PYTHONPATH=. ./venv/bin/python -m pytest tests/test_audit.py -q
"""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

import app.audit as audit_mod
from app.audit import audit, _sanitize
from app.models import SessionLocal, init_db, AuditLog, User


@pytest.fixture(scope="module", autouse=True)
def _db_ready():
    init_db()  # ensures audit_log table exists (created by create_all)
    yield


def _cleanup(action_prefix: str) -> None:
    db = SessionLocal()
    try:
        db.query(AuditLog).filter(AuditLog.action.like(action_prefix + "%")).delete(synchronize_session=False)
        db.commit()
    finally:
        db.close()


def test_sanitize_strips_secrets_by_name():
    out = _sanitize({"password": "s3cret", "connectionUri": "pg://u:p@h/db",
                     "token": "abc", "name": "ok", "nested": {"newPassword": "x", "keep": 1}})
    assert out["password"] == "[REDACTED]"
    assert out["connectionUri"] == "[REDACTED]"
    assert out["token"] == "[REDACTED]"
    assert out["name"] == "ok"
    assert out["nested"]["newPassword"] == "[REDACTED]"
    assert out["nested"]["keep"] == 1


def test_audit_writes_row_and_redacts():
    action = f"test.event.{uuid4().hex[:8]}"
    try:
        audit(action, actor_id="x", details={"password": "s3cret", "name": "ok",
                                              "connectionUri": "pg://secret"})
        db = SessionLocal()
        try:
            row = db.query(AuditLog).filter(AuditLog.action == action).first()
            assert row is not None
            assert "[REDACTED]" in (row.details_json or "")
            assert "s3cret" not in (row.details_json or "")
            assert "pg://secret" not in (row.details_json or "")
            parsed = json.loads(row.details_json)
            assert parsed["name"] == "ok"
        finally:
            db.close()
    finally:
        _cleanup("test.event.")


def test_audit_snapshots_actor_email():
    action = f"test.email.{uuid4().hex[:8]}"
    db = SessionLocal()
    uid = f"u_{uuid4().hex[:8]}"
    email = f"{uuid4().hex[:8]}@t.io"
    try:
        u = User(id=uid, name="t", email=email, password_hash="$x", role="user", active=True)
        db.add(u); db.commit()
    finally:
        db.close()
    try:
        audit(action, actor_id=uid)
        db = SessionLocal()
        try:
            row = db.query(AuditLog).filter(AuditLog.action == action).first()
            assert row is not None and row.actor_email == email
        finally:
            db.close()
    finally:
        _cleanup("test.email.")
        db = SessionLocal()
        try:
            db.query(User).filter(User.id == uid).delete()
            db.commit()
        finally:
            db.close()


def test_audit_failure_does_not_raise(monkeypatch):
    """A broken audit write (e.g. locked DB) must never propagate into the request path."""
    def _boom(*a, **k):
        raise RuntimeError("db is locked")

    monkeypatch.setattr(audit_mod, "SessionLocal", _boom)
    # Must return None without raising.
    assert audit("test.boom", actor_id="x", details={"password": "p"}) is None
