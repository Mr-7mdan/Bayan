"""Central authorization: role constants, permission model, and per-resource
guards. Spec 04 (RBAC). Builds on spec 02's identity resolution in ``auth.py``.

Migration-window rule (mirrors ``users._require_self_or_admin``): every guard
here is a **no-op while ``settings.auth_enforce`` is off**, so legacy clients
that still rely on ``?actorId=`` / path ids keep working unchanged. When the
flag flips on, identity comes only from the spec 02 session token and the full
matrix is enforced.
"""
from __future__ import annotations

from enum import IntEnum

from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session

from .auth import get_current_user_optional, get_db
from .config import settings
from .models import (
    Dashboard,
    Datasource,
    DatasourceShare,
    User,
    get_share_permission,
)

ROLE_ADMIN = "admin"
ROLE_USER = "user"


class Permission(IntEnum):
    NONE = 0
    VIEW = 1
    EDIT = 2
    OWNER = 3
    ADMIN = 4


def is_admin(user: User | None) -> bool:
    return bool(user and (user.role or ROLE_USER).lower() == ROLE_ADMIN)


# --- Dependencies -----------------------------------------------------------

def require_user(user: User | None = Depends(get_current_user_optional)) -> User | None:
    """Authenticated-user gate. Enforced only when ``auth_enforce`` is on.

    Returns the resolved ``User`` (or ``None`` during the migration window, in
    which case callers must skip ownership filtering — treat ``None`` as "legacy
    unscoped access", never as a real principal).
    """
    if not settings.auth_enforce:
        return user
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def require_admin(user: User | None = Depends(get_current_user_optional)) -> User | None:
    """Admin gate. No-op while ``auth_enforce`` is off (legacy window)."""
    if not settings.auth_enforce:
        return user
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="Admin required")
    return user


# --- Per-resource permission resolution -------------------------------------

def dashboard_permission(db: Session, user: User | None, dash: Dashboard) -> Permission:
    if is_admin(user):
        return Permission.ADMIN
    if not user:
        return Permission.NONE
    # Legacy NULL-owner rows are treated as owned by the requester (matches the
    # long-standing dashboards.py behavior).
    if not dash.user_id or dash.user_id == user.id:
        return Permission.OWNER
    perm = get_share_permission(db, dash.id, user.id)
    return {"rw": Permission.EDIT, "ro": Permission.VIEW}.get(perm or "", Permission.NONE)


def datasource_permission(db: Session, user: User | None, ds: Datasource) -> Permission:
    if is_admin(user):
        return Permission.ADMIN
    if not user:
        return Permission.NONE
    if not ds.user_id or ds.user_id == user.id:
        return Permission.OWNER
    share = (
        db.query(DatasourceShare)
        .filter(DatasourceShare.datasource_id == ds.id, DatasourceShare.user_id == user.id)
        .first()
    )
    perm = share.permission if share else None
    return {"rw": Permission.EDIT, "ro": Permission.VIEW}.get(perm or "", Permission.NONE)


def require_dashboard(db: Session, user: User | None, dash_id: str, need: Permission) -> Dashboard:
    dash = db.get(Dashboard, dash_id)
    if not dash:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    if not settings.auth_enforce:
        return dash
    if dashboard_permission(db, user, dash) < need:
        raise HTTPException(status_code=403, detail="Forbidden")
    return dash


def require_datasource(db: Session, user: User | None, ds_id: str, need: Permission) -> Datasource:
    ds = db.get(Datasource, ds_id)
    if not ds:
        raise HTTPException(status_code=404, detail="Datasource not found")
    if not settings.auth_enforce:
        return ds
    if datasource_permission(db, user, ds) < need:
        raise HTTPException(status_code=403, detail="Forbidden")
    return ds


def require_owned(db: Session, user: User | None, model, obj_id: str):
    """Generic owner-or-admin fetch for ``user_id``-scoped rows (AlertRule,
    Contact). 404 if missing; 403 if not owner/admin (enforced only when the
    flag is on)."""
    obj = db.get(model, obj_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")
    if not settings.auth_enforce:
        return obj
    if is_admin(user):
        return obj
    owner = (getattr(obj, "user_id", None) or "")
    if not user or owner != user.id:
        raise HTTPException(status_code=403, detail="Forbidden")
    return obj
