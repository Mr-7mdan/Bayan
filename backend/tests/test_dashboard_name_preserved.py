"""Regression tests for the dashboard title being replaced by "New Dashboard".

Observed in production: dashboard af924b87 was saved as "United Daily Executive
Summary v3" at 18:57 and 20:33 on 2026-07-10, then became "New Dashboard" at
21:03:30 — visible in dashboard_versions.

Cause: the builder autosaves on layout normalisation, which can run before the
client has loaded the real title (tab discarded and restored, route re-entered,
a failed GET). `dashboardName` was still its initial 'New Dashboard' placeholder
and every save path sent `name: dashboardName || 'New Dashboard'`, stamping the
placeholder over the stored title. No user edit was required.

Fix: `name` is optional on update and an omitted/blank one keeps the stored
title. The client omits it until it holds a server- or user-supplied name.

Run:  cd backend && PYTHONPATH=. ./venv/bin/python -m pytest tests/test_dashboard_name_preserved.py -q
"""
from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.models import SessionLocal, init_db, User, Dashboard
from app.routers import dashboards as dash_router

DEFN = {"layout": [], "widgets": {}, "options": {}}


@pytest.fixture()
def ctx():
    # test_backup_and_versions repoints the engine and leaves tables half-created;
    # tolerate that rather than let suite ordering decide if these can run.
    try:
        init_db()
    except Exception:
        pass
    db = SessionLocal()
    uid = f"t_dashadm_{uuid4().hex[:8]}"
    db.add(User(id=uid, name="Dash Admin", email=f"{uid}@test.local",
                password_hash="x", role="admin", active=True))
    db.commit()

    app = FastAPI()
    app.include_router(dash_router.router)
    app.dependency_overrides[dash_router.actor_id_optional] = lambda: uid
    made: list[str] = []
    try:
        yield TestClient(app), db, uid, made
    finally:
        for did in made:
            db.query(Dashboard).filter(Dashboard.id == did).delete()
        db.query(User).filter(User.id == uid).delete()
        db.commit()
        db.close()


def _create(client, made, name):
    r = client.post("/dashboards", json={"name": name, "definition": DEFN})
    assert r.status_code == 200, r.text
    made.append(r.json()["id"])
    return r.json()


def test_update_without_name_keeps_stored_title(ctx):
    """The exact production failure: an autosave that carries no title."""
    client, _db, _uid, made = ctx
    original = f"United Daily Executive Summary {uuid4().hex[:6]}"
    created = _create(client, made, original)

    r = client.post("/dashboards", json={"id": created["id"], "definition": DEFN})
    assert r.status_code == 200, r.text
    assert r.json()["name"] == original


def test_update_with_blank_name_keeps_stored_title(ctx):
    client, _db, _uid, made = ctx
    original = f"Quarterly Review {uuid4().hex[:6]}"
    created = _create(client, made, original)

    for blank in ("", "   "):
        r = client.post("/dashboards", json={"id": created["id"], "name": blank, "definition": DEFN})
        assert r.status_code == 200, r.text
        assert r.json()["name"] == original, f"blank {blank!r} clobbered the title"


def test_explicit_rename_still_works(ctx):
    """Omitting the name must not break renaming — only the placeholder case."""
    client, _db, _uid, made = ctx
    created = _create(client, made, f"Before {uuid4().hex[:6]}")
    after = f"After {uuid4().hex[:6]}"

    r = client.post("/dashboards", json={"id": created["id"], "name": after, "definition": DEFN})
    assert r.status_code == 200, r.text
    assert r.json()["name"] == after

    assert client.get(f"/dashboards/{created['id']}").json()["name"] == after


def test_create_without_name_is_rejected(ctx):
    """A brand-new dashboard still needs a title; only updates may omit it."""
    client, _db, _uid, _made = ctx
    r = client.post("/dashboards", json={"definition": DEFN})
    assert r.status_code == 422, r.text


def test_repeated_nameless_autosaves_do_not_drift(ctx):
    """The builder saves continuously; the title must survive every one."""
    client, _db, _uid, made = ctx
    original = f"Executive Summary {uuid4().hex[:6]}"
    created = _create(client, made, original)

    for i in range(5):
        defn = {"layout": [{"i": f"w{i}", "x": 0, "y": i, "w": 4, "h": 3}], "widgets": {}, "options": {}}
        r = client.post("/dashboards", json={"id": created["id"], "definition": defn})
        assert r.status_code == 200, r.text

    assert client.get(f"/dashboards/{created['id']}").json()["name"] == original
