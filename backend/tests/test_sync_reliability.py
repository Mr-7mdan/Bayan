from datetime import datetime, timezone, timedelta

from app.timeutil import as_utc


def test_as_utc():
    naive = datetime(2026, 1, 1, 12, 0)
    assert as_utc(naive) == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    aware = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert as_utc(aware) is not None and as_utc(aware).utcoffset().total_seconds() == 0
    assert as_utc(None) is None


def test_stuck_elapsed_math_is_aware_utc():
    # Regression for the naive/aware juggling bug: elapsed math must compare
    # aware-UTC vs aware-UTC. A naive DB timestamp 45 min old reads as >30 min stale.
    now = datetime.now(timezone.utc)
    naive_last_activity = (now - timedelta(minutes=45)).replace(tzinfo=None)  # DB stores naive UTC
    elapsed = now - as_utc(naive_last_activity)
    assert elapsed > timedelta(minutes=30)
    # A fresh (5 min) state must NOT be considered stuck.
    fresh = (now - timedelta(minutes=5)).replace(tzinfo=None)
    assert (now - as_utc(fresh)) < timedelta(minutes=30)


def test_cache_gen_changes_key():
    from app.routers import query as q
    k1 = q._cache_key("sql", "ds1", "select 1", {})
    q.bump_result_cache_generation()
    k2 = q._cache_key("sql", "ds1", "select 1", {})
    assert k1 != k2
