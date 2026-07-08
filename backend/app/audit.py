"""Append-only audit logging for security-relevant actions (spec 05).

The single ``audit()`` entry point writes one row per event using its own DB
session so an audit failure can never roll back the business transaction (and
vice versa). It NEVER raises into the request path — every error is logged and
swallowed. Call it AFTER the endpoint's own ``db.commit()``.

Secrets are stripped by field name before serialization: passwords, hashes,
connection URIs (plain or encrypted), and tokens must never land in the log.
"""
from __future__ import annotations

import json
import logging
from typing import Optional, Any
from uuid import uuid4

from fastapi import Request

from .models import SessionLocal, AuditLog, User

log = logging.getLogger("bayan.audit")  # spec 07 may swap this for a structured logger

# ponytail: fields stripped by name; add nested-path redaction only if details ever nest secrets
_REDACT = {
    "password", "newPassword", "oldPassword", "connectionUri", "connectionURI",
    "token", "password_hash", "connection_encrypted", "api_key", "apiKey", "secret",
}


def _sanitize(d: Any) -> Any:
    if isinstance(d, dict):
        return {k: ("[REDACTED]" if k in _REDACT else _sanitize(v)) for k, v in d.items()}
    if isinstance(d, list):
        return [_sanitize(v) for v in d]
    return d


def audit(
    action: str,
    actor_id: Optional[str] = None,
    target_type: Optional[str] = None,
    target_id: Optional[str] = None,
    request: Optional[Request] = None,
    status: str = "success",
    details: Optional[dict] = None,
) -> None:
    """Append one audit row. Own session; never raises."""
    try:
        db = SessionLocal()
        try:
            email = None
            if actor_id:
                try:
                    u = db.query(User).filter(User.id == actor_id).first()
                    email = u.email if u else None
                except Exception:
                    email = None
            row = AuditLog(
                id=str(uuid4()),
                actor_id=actor_id,
                actor_email=email,
                action=action,
                target_type=target_type,
                target_id=target_id,
                ip=(request.client.host if request and request.client else None),
                user_agent=(request.headers.get("user-agent") if request else None),
                status=status,
                details_json=(json.dumps(_sanitize(details)) if details else None),
            )
            db.add(row)
            db.commit()
        finally:
            db.close()
        log.info(
            "audit %s actor=%s target=%s/%s status=%s",
            action, actor_id, target_type, target_id, status,
        )
    except Exception:
        log.exception("audit write failed for action=%s", action)
