"""/datasources/sync-freshness: last successful sync of the local tables a report reads.

The executive summary reads Daily_Positions from a DuckDB snapshot; if its sync
fails the figures go silently stale. Reports use this endpoint to flag that.

Run:  cd backend && PYTHONPATH=. ./venv/bin/python -m pytest tests/test_sync_freshness.py -q
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base, Datasource, SyncTask, SyncRun
from app.routers.datasources import sync_freshness, SyncFreshnessItem


def _db():
    eng = create_engine("sqlite://")
    Base.metadata.create_all(eng)
    db = sessionmaker(bind=eng)()
    db.add_all([
        Datasource(id="duck", user_id="u", name="Local DuckDB", type="duckdb"),
        Datasource(id="mysql", user_id="u", name="pcma", type="mysql"),
        SyncTask(id="t1", datasource_id="crm", source_schema="s", source_table="", dest_table_name="Daily_Positions",
                 mode="snapshot", schedule_cron="0 2 * * *", enabled=True, group_key="g"),
    ])
    db.add_all([
        SyncRun(id="ok", task_id="t1", datasource_id="crm", mode="snapshot",
                started_at=datetime(2026, 10, 2, 2, 0), finished_at=datetime(2026, 10, 2, 2, 1), row_count=779),
        SyncRun(id="bad", task_id="t1", datasource_id="crm", mode="snapshot",
                started_at=datetime(2026, 10, 3, 2, 0), finished_at=None, error="lock held"),
    ])
    db.commit()
    return db


def test_reports_last_success_not_last_failure():
    out = sync_freshness([SyncFreshnessItem(datasourceId="duck", source="Daily_Positions")], db=_db())
    assert out == [{"source": "Daily_Positions", "hasSyncTask": True, "lastSuccessAt": "2026-10-02T02:01:00Z",
                    "lastRunAt": "2026-10-03T02:00:00Z"}]


def test_live_sources_and_duplicates_skipped():
    out = sync_freshness([SyncFreshnessItem(datasourceId="mysql", source="mt5.mt5_deals"),
                          SyncFreshnessItem(datasourceId="duck", source="main.Daily_Positions"),
                          SyncFreshnessItem(datasourceId="duck", source="Daily_Positions")], db=_db())
    assert [o["source"] for o in out] == ["Daily_Positions"]


def test_table_without_any_successful_sync():
    out = sync_freshness([SyncFreshnessItem(datasourceId="duck", source="Unknown_Table")], db=_db())
    assert out[0]["hasSyncTask"] is False and out[0]["lastSuccessAt"] is None
