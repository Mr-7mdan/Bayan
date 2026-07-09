"""Spec 09 — DuckDB pool hardening: per-conn ATTACH tracking, pool cap, health-check."""
import duckdb
import pytest

from app import db


def test_no_global_conn_attached_dict():
    # id(conn)-keyed global dict is gone.
    assert not hasattr(db, "_DUCK_CONN_ATTACHED")
    assert not hasattr(db, "_forget_conn_attached")


def test_replay_uses_per_conn_set_and_onearg_compat():
    db.register_duck_attach("t1", "ATTACH ':memory:' AS t1")
    c = duckdb.connect(":memory:")
    try:
        s: set = set()
        db._replay_attaches_on_conn(c, s)
        assert "t1" in s
        # one-arg form (query.py compat) must not raise
        db._replay_attaches_on_conn(c)
    finally:
        c.close()


def test_tracked_conn_has_slots():
    t = db._TrackedConn.__new__(db._TrackedConn)
    assert db._TrackedConn.__slots__ == ("conn", "attached")
    # slotted class rejects arbitrary attributes
    with pytest.raises(AttributeError):
        t.bogus = 1


def test_pool_default_size_is_capped():
    db._drain_duck_read_pool()
    db._init_duck_read_pool(":memory:")
    n = 0
    while True:
        try:
            db._DUCK_READ_POOL.get_nowait()
            n += 1
        except Exception:
            break
    db._drain_duck_read_pool()
    assert 4 <= n <= 16, n


def test_env_override_uncapped(monkeypatch):
    monkeypatch.setattr(db, "_DUCK_READ_POOL_SIZE", 32)
    db._drain_duck_read_pool()
    db._init_duck_read_pool(":memory:")
    n = 0
    while True:
        try:
            db._DUCK_READ_POOL.get_nowait()
            n += 1
        except Exception:
            break
    db._drain_duck_read_pool()
    assert n == 32, n


def test_pooled_wrap_replaces_broken_conn_on_error():
    db._drain_duck_read_pool()
    db._init_duck_read_pool(":memory:")
    pool = db._DUCK_READ_POOL
    tracked = pool.get_nowait()
    wrap = db._PooledCursorWrap(tracked, pool)
    with wrap:
        pass  # borrow; close the conn to simulate a wedged connection
    # Re-borrow, close underlying conn, then exit with an exception -> replaced.
    tracked2 = pool.get_nowait()
    wrap2 = db._PooledCursorWrap(tracked2, pool)
    conn = wrap2.__enter__()
    conn.close()  # invalidate
    wrap2.__exit__(RuntimeError, RuntimeError("boom"), None)
    # A healthy tracked conn is back in the pool and answers SELECT 1.
    back = pool.get_nowait()
    assert back.conn.execute("SELECT 1").fetchall() == [(1,)]
    db._drain_duck_read_pool()
