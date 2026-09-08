"""Regression tests for POST /datasources/import.

Three defects motivated these:

1. Export returned a bare array while import only accepted ``{"items": [...]}``,
   and the two item schemas had drifted, so an export file round-tripped only
   because the frontend reshaped it and Pydantic silently dropped extra keys.
2. When an admin imported a file from ANOTHER instance, ``userId`` was honoured
   verbatim. The row was created owned by a user that does not exist locally, and
   ``GET /datasources`` (which filters by owner) never returned it — the import
   reported success while several datasources appeared to vanish.
3. A failing item aborted the rest of the batch, and sync-task errors were
   swallowed by a bare ``except: pass``, so nothing surfaced to the user.

Uses the app's real metadata DB; all rows created here use unique ids and are
removed in teardown.

Run:  cd backend && PYTHONPATH=. ./venv/bin/python -m pytest tests/test_datasource_import.py -q
"""
from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.models import SessionLocal, init_db, User, Datasource, SyncTask
from app.routers import datasources as ds_router

# A user id that belongs to a different Bayan instance.
GHOST_OWNER = "ghost-owner-does-not-exist-here"


@pytest.fixture()
def ctx():
    # test_backup_and_versions repoints the engine at a throwaway DB and leaves
    # the tables half-created, so a later init_db() raises "table already exists".
    # That is a pre-existing isolation leak in that suite; tolerate it here rather
    # than let ordering decide whether these tests can run.
    try:
        init_db()
    except Exception:
        pass
    db = SessionLocal()
    admin_id = f"t_admin_{uuid4().hex[:8]}"
    db.add(User(id=admin_id, name="Import Admin", email=f"{admin_id}@test.local",
                password_hash="x", role="admin", active=True))
    db.commit()

    app = FastAPI()
    app.include_router(ds_router.router)
    # The real dependency reads a session token; impersonate the admin directly.
    app.dependency_overrides[ds_router.actor_id_optional] = lambda: admin_id

    prefix = f"t_ds_{uuid4().hex[:8]}"
    try:
        yield TestClient(app), db, admin_id, prefix
    finally:
        rows = db.query(Datasource).filter(Datasource.name.like(f"{prefix}%")).all()
        for r in rows:
            db.query(SyncTask).filter(SyncTask.datasource_id == r.id).delete()
            db.delete(r)
        db.query(User).filter(User.id == admin_id).delete()
        db.commit()
        db.close()


def export_file(prefix: str):
    """Shaped exactly like GET /datasources/export output, foreign owner included."""
    return [
        {
            "id": "src-1", "name": f"{prefix}_alpha", "type": "mysql",
            "connectionUri": "mysql://u:p@h/db", "options": {"k": 1},
            "userId": GHOST_OWNER, "active": True, "createdAt": "2026-01-01T00:00:00",
            "syncTasks": [{
                "id": "t1", "datasourceId": "src-1", "sourceSchema": "mt5",
                "sourceTable": "mt5_deals", "destTableName": f"{prefix}_deals",
                "mode": "snapshot", "pkColumns": [], "selectColumns": [],
                "groupKey": "g", "createdAt": "2026-01-01T00:00:00",
            }],
        },
        {
            "id": "src-2", "name": f"{prefix}_beta", "type": "mysql",
            "connectionUri": None, "options": None, "userId": GHOST_OWNER,
            "active": False, "createdAt": "2026-01-01T00:00:00", "syncTasks": [],
        },
    ]


def test_bare_export_array_is_accepted(ctx):
    """The file /export produces must import without any reshaping."""
    client, _db, _admin, prefix = ctx
    r = client.post("/datasources/import", json=export_file(prefix))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["created"] == 2
    assert body["failed"] == 0
    # One result per submitted item, always.
    assert len(body["results"]) == 2


def test_envelope_shape_still_accepted(ctx):
    client, _db, _admin, prefix = ctx
    r = client.post("/datasources/import", json={"items": export_file(prefix)})
    assert r.status_code == 200, r.text
    assert r.json()["created"] == 2


def test_foreign_owner_is_remapped_and_reported(ctx):
    """The bug that made imported datasources invisible."""
    client, db, admin_id, prefix = ctx
    body = client.post("/datasources/import", json=export_file(prefix)).json()

    rows = db.query(Datasource).filter(Datasource.name.like(f"{prefix}%")).all()
    assert len(rows) == 2
    # Owned by the importer, so GET /datasources actually returns them.
    assert all(r.user_id == admin_id for r in rows), [(r.name, r.user_id) for r in rows]
    # And the remap is surfaced rather than done behind the user's back.
    assert any("does not exist on this instance" in w
               for res in body["results"] for w in res["warnings"])


def test_missing_connection_string_is_reported(ctx):
    client, _db, _admin, prefix = ctx
    body = client.post("/datasources/import", json=export_file(prefix)).json()
    beta = next(r for r in body["results"] if r["sourceName"] == f"{prefix}_beta")
    assert any("No connection string" in w for w in beta["warnings"])


def test_sync_tasks_round_trip(ctx):
    client, _db, _admin, prefix = ctx
    body = client.post("/datasources/import", json=export_file(prefix)).json()
    alpha = next(r for r in body["results"] if r["sourceName"] == f"{prefix}_alpha")
    assert alpha["syncTasksImported"] == 1
    assert alpha["syncTasksFailed"] == 0


def test_reimport_updates_instead_of_duplicating(ctx):
    client, db, _admin, prefix = ctx
    client.post("/datasources/import", json=export_file(prefix))
    second = client.post("/datasources/import", json=export_file(prefix)).json()
    assert second["updated"] == 2
    assert second["created"] == 0
    assert db.query(Datasource).filter(Datasource.name == f"{prefix}_alpha").count() == 1


def test_rename_creates_a_separate_datasource(ctx):
    """What the import wizard does when the user resolves a name collision."""
    client, db, _admin, prefix = ctx
    client.post("/datasources/import", json=export_file(prefix))
    renamed = [{**export_file(prefix)[0], "name": f"{prefix}_alpha (2)"}]
    r = client.post("/datasources/import", json=renamed).json()
    assert r["created"] == 1
    assert db.query(Datasource).filter(Datasource.name.like(f"{prefix}_alpha%")).count() == 2


def test_bad_item_does_not_abort_the_batch(ctx):
    """A sync task with no destination table must be reported, not swallowed."""
    client, _db, _admin, prefix = ctx
    batch = [
        {"name": f"{prefix}_good", "type": "mysql", "connectionUri": "mysql://x"},
        {"name": f"{prefix}_partial", "type": "mysql", "connectionUri": "mysql://x",
         "syncTasks": [{"sourceTable": "s", "destTableName": "", "mode": "snapshot"}]},
    ]
    body = client.post("/datasources/import", json=batch).json()
    assert body["created"] == 2, body
    partial = next(r for r in body["results"] if r["sourceName"] == f"{prefix}_partial")
    assert any("no destination table" in w for w in partial["warnings"]), partial
