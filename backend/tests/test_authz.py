"""Regression tests for spec 04 RBAC / per-resource authorization.

Exercised with ``auth_enforce`` forced ON (the enforced state). Under the
default OFF state every guard is a no-op, so these assertions only make sense
with the flag on. Uses real HMAC session tokens (spec 02) and the app's real
metadata DB; all rows created here use unique ids and are removed in teardown.

Run:  cd backend && PYTHONPATH=. ./venv/bin/python -m pytest tests/test_authz.py -q
"""
from __future__ import annotations

import json
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.models import (
    SessionLocal,
    init_db,
    User,
    Dashboard,
    Datasource,
    DatasourceShare,
    AlertRule,
    Contact,
    ShareLink,
    grant_share_permission,
)
from app.security import sign_session_token, hash_password


PW = "$argon2id$test"  # any fixed hash; tokens are signed against it


def _mk_user(db, role: str) -> User:
    u = User(id=f"u_{uuid4().hex[:8]}", name="t", email=f"{uuid4().hex[:8]}@t.io",
             password_hash=PW, role=role, active=True)
    db.add(u); db.commit(); db.refresh(u)
    return u


def _tok(u: User) -> str:
    return sign_session_token(u.id, u.password_hash, 3600)


def _auth(u: User) -> dict:
    return {"Authorization": f"Bearer {_tok(u)}"}


@pytest.fixture(scope="module")
def ctx():
    init_db()
    prev = settings.auth_enforce
    settings.auth_enforce = True
    db = SessionLocal()
    created = []
    try:
        admin = _mk_user(db, "admin")
        user = _mk_user(db, "user")
        owner = _mk_user(db, "user")  # foreign owner
        created += [admin, user, owner]

        def dash(name):
            d = Dashboard(id=f"d_{uuid4().hex[:8]}", user_id=owner.id, name=name,
                          definition_json=json.dumps({"layout": [], "widgets": {}}))
            db.add(d); db.commit(); db.refresh(d); created.append(d); return d

        dash_foreign = dash("foreign")            # no share -> user forbidden
        dash_ro = dash("ro"); grant_share_permission(db, dash_ro.id, user.id, "ro")
        dash_rw = dash("rw"); grant_share_permission(db, dash_rw.id, user.id, "rw")
        dash_pub = dash("pub")
        sl = ShareLink(id=f"s_{uuid4().hex[:8]}", dashboard_id=dash_pub.id,
                       public_id=uuid4().hex[:10])  # unprotected (token_hash NULL)
        db.add(sl); db.commit(); db.refresh(sl); created.append(sl)

        ds = Datasource(id=f"ds_{uuid4().hex[:8]}", user_id=owner.id, name="ds",
                        type="duckdb", connection_encrypted=None, options_json="{}")
        db.add(ds); db.commit(); db.refresh(ds); created.append(ds)
        share = DatasourceShare(id=f"dss_{uuid4().hex[:8]}", datasource_id=ds.id,
                                user_id=user.id, permission="ro")
        db.add(share); db.commit(); created.append(share)

        alert = AlertRule(id=f"a_{uuid4().hex[:8]}", name="al", kind="alert",
                          user_id=owner.id, config_json="{}")
        db.add(alert); db.commit(); db.refresh(alert); created.append(alert)

        contact = Contact(id=f"c_{uuid4().hex[:8]}", user_id=owner.id, name="ct")
        db.add(contact); db.commit(); db.refresh(contact); created.append(contact)

        client = TestClient(app_instance())
        yield {
            "client": client, "admin": admin, "user": user, "owner": owner,
            "dash_foreign": dash_foreign, "dash_ro": dash_ro, "dash_rw": dash_rw,
            "public_id": sl.public_id, "ds": ds, "alert": alert, "contact": contact,
        }
    finally:
        for obj in reversed(created):
            try:
                db.delete(db.merge(obj)); db.commit()
            except Exception:
                db.rollback()
        db.close()
        settings.auth_enforce = prev


def app_instance():
    from app.main import app
    return app


# --- 401: no token on non-public route groups ------------------------------

@pytest.mark.parametrize("method,path", [
    ("get", "/api/contacts"),
    ("get", "/api/alerts"),
    ("post", "/api/holidays"),
    ("get", "/api/admin/metrics-live"),
])
def test_401_without_token(ctx, method, path):
    kwargs = {"json": {}} if method == "post" else {}
    r = getattr(ctx["client"], method)(path, **kwargs)
    assert r.status_code == 401, (path, r.status_code)


def test_401_users_subresource(ctx):
    r = ctx["client"].get(f"/api/users/{ctx['owner'].id}/favorites")
    assert r.status_code == 401


# --- 403: authed non-admin user on foreign / privileged resources ----------

def test_403_foreign_dashboard(ctx):
    r = ctx["client"].get(f"/api/dashboards/{ctx['dash_foreign'].id}", headers=_auth(ctx["user"]))
    assert r.status_code == 403


def test_403_datasource_patch_ro_share(ctx):
    r = ctx["client"].patch(f"/api/datasources/{ctx['ds'].id}",
                            json={"name": "x"}, headers=_auth(ctx["user"]))
    assert r.status_code == 403


def test_403_alert_delete(ctx):
    r = ctx["client"].delete(f"/api/alerts/{ctx['alert'].id}", headers=_auth(ctx["user"]))
    assert r.status_code == 403


def test_403_contact_put(ctx):
    r = ctx["client"].put(f"/api/contacts/{ctx['contact'].id}",
                          json={"name": "z"}, headers=_auth(ctx["user"]))
    assert r.status_code == 403


def test_403_admin_only_endpoints(ctx):
    c, hdr = ctx["client"], _auth(ctx["user"])
    assert c.get("/api/admin/metrics-live", headers=hdr).status_code == 403
    assert c.post("/api/holidays", headers=hdr,
                  json={"name": "h", "rule_type": "specific", "specific_date": "2026-01-01"}).status_code == 403
    assert c.put("/api/alerts/config/email", headers=hdr, json={}).status_code == 403


def test_403_other_users_favorites(ctx):
    r = ctx["client"].get(f"/api/users/{ctx['owner'].id}/favorites", headers=_auth(ctx["user"]))
    assert r.status_code == 403


# --- 200: shares, admin, public --------------------------------------------

def test_200_ro_share_dashboard_get(ctx):
    r = ctx["client"].get(f"/api/dashboards/{ctx['dash_ro'].id}", headers=_auth(ctx["user"]))
    assert r.status_code == 200


def test_200_admin_metrics_live(ctx):
    r = ctx["client"].get("/api/admin/metrics-live", headers=_auth(ctx["admin"]))
    assert r.status_code == 200


def test_200_admin_can_delete_alert(ctx):
    # admin passes require_owned even for a foreign-owned alert (read path proves access)
    r = ctx["client"].get(f"/api/alerts/{ctx['alert'].id}", headers=_auth(ctx["admin"]))
    assert r.status_code == 200


def test_200_own_favorites(ctx):
    r = ctx["client"].get(f"/api/users/{ctx['user'].id}/favorites", headers=_auth(ctx["user"]))
    assert r.status_code == 200


def test_200_public_dashboard_no_auth(ctx):
    r = ctx["client"].get(f"/api/dashboards/public/{ctx['public_id']}")
    assert r.status_code == 200
