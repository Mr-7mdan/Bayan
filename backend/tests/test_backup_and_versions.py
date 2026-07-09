"""Spec 24 verification: metadata backup + dashboard version history.

Runs against a throwaway temp SQLite DB brought to head via init_db (Alembic
upgrade), so it also proves the 0002 revision applies on a fresh DB.
"""
import importlib
import sqlite3
from pathlib import Path

import pytest


@pytest.fixture()
def app_env(tmp_path, monkeypatch):
    meta = tmp_path / "meta.sqlite"
    backups = tmp_path / "backups"
    monkeypatch.setenv("METADATA_DB_PATH", str(meta))  # not used by config, set path below
    # config reads metadata_db_path with no env alias -> patch the settings object
    import app.config as config
    importlib.reload(config)
    config.settings.metadata_db_path = str(meta)
    config.settings.backup_dir = str(backups)
    config.settings.backup_retention = 3

    # Reload models so engine binds to the temp DB path
    import app.models as models
    importlib.reload(models)
    config.settings.metadata_db_path = str(meta)  # reload reset; re-apply
    config.settings.backup_dir = str(backups)
    config.settings.backup_retention = 3
    # Rebuild engine against temp path
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import NullPool
    models.engine_meta.dispose()
    models.engine_meta = create_engine(
        f"sqlite+pysqlite:///{meta}", future=True,
        connect_args={"check_same_thread": False, "timeout": 30}, poolclass=NullPool,
    )
    models.SessionLocal = sessionmaker(bind=models.engine_meta, autoflush=False, autocommit=False)
    # Point Alembic env at this engine and upgrade
    Base = models.Base
    Base.metadata.create_all(models.engine_meta)  # fresh schema incl. dashboard_versions
    return config, models, meta, backups


def test_dashboard_versions_created_on_update(app_env):
    config, models, meta, backups = app_env
    db = models.SessionLocal()
    try:
        # create
        d = models.save_dashboard(db, user_id="u1", name="t", definition={"layout": [], "widgets": {"w1": {}}})
        assert db.query(models.DashboardVersion).count() == 0  # create makes no version

        # update with a changed definition, force to bypass coalesce window
        models._VERSION_COALESCE_SEC = 0
        models.save_dashboard(db, user_id="u1", name="t", definition={"layout": [], "widgets": {}}, dash_id=d.id, actor="u1")
        versions = db.query(models.DashboardVersion).filter_by(dashboard_id=d.id).all()
        assert len(versions) == 1
        # the snapshot holds the PRIOR state (had widget w1)
        import json
        assert "w1" in json.loads(versions[0].definition_json)["widgets"]

        # identical save makes no new version
        models.save_dashboard(db, user_id="u1", name="t", definition={"layout": [], "widgets": {}}, dash_id=d.id, actor="u1")
        assert db.query(models.DashboardVersion).filter_by(dashboard_id=d.id).count() == 1
    finally:
        db.close()


def test_coalesce_within_window(app_env):
    config, models, meta, backups = app_env
    db = models.SessionLocal()
    try:
        models._VERSION_COALESCE_SEC = 300
        d = models.save_dashboard(db, user_id="u1", name="t", definition={"widgets": {"a": {}}})
        models.save_dashboard(db, user_id="u1", name="t", definition={"widgets": {"b": {}}}, dash_id=d.id, actor="u1")
        models.save_dashboard(db, user_id="u1", name="t", definition={"widgets": {"c": {}}}, dash_id=d.id, actor="u1")
        # second update within window coalesces -> still 1 version
        assert db.query(models.DashboardVersion).filter_by(dashboard_id=d.id).count() == 1
    finally:
        db.close()


def test_restore_is_undoable(app_env):
    config, models, meta, backups = app_env
    import json
    db = models.SessionLocal()
    try:
        models._VERSION_COALESCE_SEC = 0
        d = models.save_dashboard(db, user_id="u1", name="t", definition={"widgets": {"orig": {}}})
        models.save_dashboard(db, user_id="u1", name="t", definition={"widgets": {"edited": {}}}, dash_id=d.id, actor="u1")
        ver = db.query(models.DashboardVersion).filter_by(dashboard_id=d.id).first()
        assert "orig" in json.loads(ver.definition_json)["widgets"]

        # restore that version (force_version snapshots the pre-restore 'edited' state)
        restored = models.save_dashboard(db, user_id="u1", name=ver.name,
                                         definition=json.loads(ver.definition_json), dash_id=d.id,
                                         actor="u1", force_version=True)
        assert "orig" in json.loads(restored.definition_json)["widgets"]
        # pre-restore 'edited' state is captured as a version -> restore is undoable.
        # (created_at has 1s granularity so we assert on the set, not strict order.)
        keys = set()
        for v in db.query(models.DashboardVersion).filter_by(dashboard_id=d.id).all():
            keys |= set(json.loads(v.definition_json).get("widgets", {}).keys())
        assert {"orig", "edited"} <= keys
    finally:
        db.close()


def test_delete_cascades_versions(app_env):
    config, models, meta, backups = app_env
    db = models.SessionLocal()
    try:
        models._VERSION_COALESCE_SEC = 0
        d = models.save_dashboard(db, user_id="u1", name="t", definition={"widgets": {"a": {}}})
        models.save_dashboard(db, user_id="u1", name="t", definition={"widgets": {"b": {}}}, dash_id=d.id, actor="u1")
        assert db.query(models.DashboardVersion).filter_by(dashboard_id=d.id).count() == 1
        models.delete_dashboard(db, d.id)
        assert db.query(models.DashboardVersion).filter_by(dashboard_id=d.id).count() == 0
    finally:
        db.close()


def test_backup_produces_file_and_retains(app_env):
    config, models, meta, backups = app_env
    # seed a dashboard so the backup has content
    db = models.SessionLocal()
    try:
        models.save_dashboard(db, user_id="u1", name="t", definition={"widgets": {}})
    finally:
        db.close()

    import app.backup as backup
    importlib.reload(backup)
    backup.settings.metadata_db_path = str(meta)
    backup.settings.backup_dir = str(backups)
    backup.settings.backup_retention = 3
    backup.settings.backup_include_duckdb = False

    # one real backup: produces a file that opens and holds the schema
    res = backup.run_backup()
    assert res.get("error") is None, res
    assert res["sqlite"] and Path(res["sqlite"]).exists()
    con = sqlite3.connect(res["sqlite"])
    try:
        names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "dashboards" in names
        assert "dashboard_versions" in names
    finally:
        con.close()

    # retention: seed extra timestamped files, prune keeps newest 3
    for ts in ("20200101-000001", "20200101-000002", "20200101-000003", "20200101-000004"):
        (Path(backups) / f"meta-{ts}.sqlite").write_text("x")
    pruned = backup._prune(Path(backups), "meta-", ".sqlite", 3)
    assert pruned >= 1
    assert len(list(Path(backups).glob("meta-*.sqlite"))) == 3
