---
id: 09-duckdb-pool-hardening
title: Harden DuckDB read pool and ATTACH registry
priority: P1
effort: M
depends_on: []
area: backend
---

## Problem

The DuckDB read pool in `backend/app/db.py` tracks which ATTACH statements have been replayed onto each pooled connection using a module-global dict keyed by `id(conn)`. Python object ids are reused after GC, so a *new* connection can inherit the "already attached" set of a dead one and silently skip ATTACH replay — remote catalogs (e.g. MySQL `pcma`) go missing and queries fail with "catalog not found". Ephemeral connections leak entries into that dict (never removed on close), which both grows memory and increases the id-reuse collision surface. The dict is also mutated from multiple threads without a lock. Separately, pool connections are opened read-write, and the SQLite metadata engine uses `NullPool` with no WAL/busy_timeout pragmas, so concurrent metadata writes can hit `database is locked`.

## Current State

All verified 2026-07-07 on branch `feature/alpha-themes-foundation`.

**`backend/app/db.py`**

- Lines 223-226 — pool globals; `SimpleQueue` (unbounded type, but only pre-filled once), size from `DUCKDB_READ_POOL_SIZE` env or `max(4, cpu*2)` (line 306) — no upper cap on big machines.
- Line 236 — the buggy registry keying:
  ```python
  _DUCK_CONN_ATTACHED: dict[int, set[str]] = {}
  ```
- Lines 249-288 `_replay_attaches_on_conn(conn)` — keys by `id(conn)` (line 255), mutates `_DUCK_CONN_ATTACHED` with no lock (only the registry snapshot at 251-254 is locked). Has graceful handling for "already attached" errors (lines 266-268) — this makes replay effectively idempotent, which the fix relies on.
- Lines 291-293 `_forget_conn_attached(conn)` — pops by `id(conn)`. Called from `_drain_duck_read_pool` (line 334) and `_PooledCursorWrap.__exit__` failure path (line 404), but **NOT** from `_TmpCursorWrap.__exit__` (lines 584-594) — ephemeral connections opened at line 574 leak their dict entry on close. This is the id-reuse feeder.
- Lines 296-319 `_init_duck_read_pool` — opens pool conns read-write with a comment (lines 310-314) claiming ATTACH needs write access. The stronger reason (undocumented): duckdb's in-process instance cache rejects `read_only=True` for a path already open read-write by `_DUCK_SHARED_CONN` — raises "different configuration" (the codebase already handles this error string at lines 30, 496).
- Lines 342-408 `_PooledCursorWrap` — borrows raw conn, replays ATTACHes in `__init__` (line 369), returns conn to pool unconditionally on `__exit__` (line 400) even if the query raised — a wedged/invalidated conn goes back into rotation.
- Lines 525-594 `open_duck_native` — 3 strategies: pool (548-553), shared-conn cursor (556-571, replays at 559), ephemeral (573-594, replays at 580, leaks tracking on close).
- Lines 597-615 `close_duck_shared` — clears registry and `_DUCK_CONN_ATTACHED` (line 603).
- Line 710 `_apply_duck_pragmas(conn)` — existing helper, reuse for replacement conns.

**External callers that must keep working (do not change their code):**

- `backend/app/routers/query.py:24` imports `_replay_attaches_on_conn` and calls it with the object yielded by `open_duck_native` at lines 2021, 2074, 6231, 9628 — signature must stay callable as `_replay_attaches_on_conn(conn)`.
- `backend/app/routers/datasources.py:2000,2010,2325,2335` call `register_duck_attach(alias, attach_sql)` — unchanged.
- ~30 call sites of `open_duck_native(...)` across `query.py`, `datasources.py`, `api_ingest.py`, `main.py` — all use it as a context manager yielding a conn/cursor with the duckdb API surface; that contract must not change.

**`backend/app/models.py`**

- Lines 173-179:
  ```python
  engine_meta = create_engine(
      f"sqlite+pysqlite:///{settings.metadata_db_path}",
      future=True,
      connect_args={"check_same_thread": False},
      pool_pre_ping=True,
      poolclass=NullPool,
  )
  ```
  No WAL, no busy_timeout. `event` is not imported in models.py (line 11 imports only Boolean/DateTime/etc.).

## Desired State

- ATTACH replay tracking is keyed by a stable per-connection token (a small wrapper object owned by the pool), not `id()` of a GC-recyclable object. No global tracking dict; no leak for ephemeral conns.
- Pool connections that raised during use are health-checked (`SELECT 1`) before re-entering the pool; broken ones are closed and replaced so pool size stays constant.
- Pool size default is capped (`min(16, max(4, cpu*2))`); `DUCKDB_READ_POOL_SIZE` env override still wins.
- The read-only decision is documented in code: pooled conns stay read-write because duckdb's in-process instance cache forbids mixed `read_only` configs against the RW shared connection (plus ATTACH replay needs catalog writes).
- SQLite metadata engine runs `PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`, `PRAGMA synchronous=NORMAL` on every new connection.

## Implementation Plan

All edits in `backend/app/db.py` unless noted. Order matters only within step 1.

**1. Replace `id(conn)` tracking with a per-connection holder.**

1a. Delete global `_DUCK_CONN_ATTACHED` (line 236) and `_forget_conn_attached` (lines 291-293). Remove its call sites: `_drain_duck_read_pool` line 334, `_PooledCursorWrap.__exit__` line 404, `close_duck_shared` line 603.

1b. Add a tiny holder the pool queue stores instead of raw conns:

```python
class _TrackedConn:
    """Pool entry: a duckdb connection + the set of ATTACH aliases replayed onto it."""
    __slots__ = ("conn", "attached")
    def __init__(self, conn):
        self.conn = conn
        self.attached: set[str] = set()
```

1c. Change `_replay_attaches_on_conn` signature to `def _replay_attaches_on_conn(conn, attached: set | None = None) -> None`. Body: if `attached is None`, use a fresh local `set()` (the "already attached" handling at current lines 266-268 makes duplicate replay harmless — this preserves backward compat for the four `query.py` call sites that pass only `conn`). Replace `already = _DUCK_CONN_ATTACHED.get(...)` block (lines 255-259) with `already = attached if attached is not None else set()`. Delete the `conn_id = id(conn)` line. No lock needed on `already`: a pooled conn has exactly one borrower at a time, the shared conn's set is guarded by the GIL for set.add and replay is idempotent anyway — add a one-line comment saying so.

1d. Shared connection tracking: add module global `_DUCK_SHARED_ATTACHED: set[str] = set()` next to `_DUCK_SHARED_CONN` (line 213). In `open_duck_native` strategy 2 (line 559) call `_replay_attaches_on_conn(con, _DUCK_SHARED_ATTACHED)`. Reset it (`.clear()`) in `init_duck_shared` when reopening after a path change (after line 487) and in `close_duck_shared` (replacing the line-603 dict clear).

1e. Ephemeral path (strategy 3, line 580): call `_replay_attaches_on_conn(con)` with no set — throwaway tracking, nothing to leak. `_TmpCursorWrap` needs no change.

**2. Pool stores `_TrackedConn`; wrapper health-checks on error.**

2a. `_init_duck_read_pool` line 317: `_DUCK_READ_POOL.put(_TrackedConn(c))`.

2b. `_drain_duck_read_pool` loop (lines 330-339): `t = pool.get_nowait()`, close `t.conn` (drop the `_forget_conn_attached` call).

2c. `_PooledCursorWrap`: `__slots__ = ("_tracked", "_pool")`; `__init__(self, tracked, pool)` calls `_replay_attaches_on_conn(tracked.conn, tracked.attached)`; `__getattr__`/`__enter__`/cancellation registration use `self._tracked.conn`. In `__exit__`:
   - unregister cancel token (unchanged);
   - if `exc_type is not None`: run `self._tracked.conn.execute("SELECT 1")` inside try/except; on failure, close the conn and open a replacement (`_duckdb.connect(_DUCK_READ_POOL_PATH)` + `_apply_duck_pragmas`, wrapped in a fresh `_TrackedConn`) so pool size stays constant; if replacement open fails, just drop it (pool shrinks by one — acceptable, `open_duck_native` falls back to the shared conn when the pool is empty);
   - `put` the (original or replacement) `_TrackedConn` back; keep the existing except-branch that closes on a drained/replaced pool (lines 401-407).

2d. `open_duck_native` strategy 1 (lines 550-551): `tracked = _DUCK_READ_POOL.get_nowait(); return _PooledCursorWrap(tracked, _DUCK_READ_POOL)`.

**3. Cap the pool size.** Line 306: `pool_size = size or _DUCK_READ_POOL_SIZE or min(16, max(4, (os.cpu_count() or 4) * 2))`. Env override (`DUCKDB_READ_POOL_SIZE`, line 226) is intentionally uncapped.

**4. Document the read-only decision.** Replace the NOTE comment at lines 310-314 with one stating both reasons: (a) duckdb's in-process instance cache raises "different configuration" if the same file is opened `read_only=True` while `_DUCK_SHARED_CONN` holds it read-write, and (b) ATTACH replay writes session catalog metadata. Do NOT attempt `read_only=True` — it will break `init_duck_shared` ordering (pool is initialized from inside it, line 514, while the RW conn is already open).

**5. SQLite WAL + busy_timeout** — `backend/app/models.py`:

5a. Add `event` to the sqlalchemy import on line 11.

5b. Immediately after `engine_meta = create_engine(...)` (line 179), add:

```python
@event.listens_for(engine_meta, "connect")
def _set_sqlite_pragmas(dbapi_conn, _record):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA busy_timeout=5000")
    cur.execute("PRAGMA synchronous=NORMAL")
    cur.close()
```

NullPool means this fires per-connection; all three pragmas are cheap and `journal_mode=WAL` persists in the db file anyway. Do NOT enable `foreign_keys` — existing rows may have dangling refs and the schema was never enforced.

## Files to Modify

- `backend/app/db.py` — delete `_DUCK_CONN_ATTACHED`/`_forget_conn_attached`; add `_TrackedConn` + `_DUCK_SHARED_ATTACHED`; rework `_replay_attaches_on_conn` signature; pool stores tracked conns; `_PooledCursorWrap` health-check + replace on error; cap default pool size; fix read-only comment.
- `backend/app/models.py` — import `event`; add SQLite pragma connect listener after `engine_meta`.

No changes to `query.py`, `datasources.py`, `api_ingest.py`, `main.py` — their contracts (`open_duck_native` context manager, `_replay_attaches_on_conn(conn)`, `register_duck_attach`) are preserved.

## Acceptance Criteria

- [ ] `grep -n "id(conn)" backend/app/db.py` returns nothing; `_DUCK_CONN_ATTACHED` and `_forget_conn_attached` no longer exist.
- [ ] `_replay_attaches_on_conn(conn)` (one arg) still works — the four `query.py` call sites (2021, 2074, 6231, 9628) run unmodified.
- [ ] A pooled connection that raises during use is `SELECT 1`-validated before re-entering the pool; a broken one is replaced, keeping pool size constant.
- [ ] Default pool size ≤ 16; `DUCKDB_READ_POOL_SIZE=32` still yields 32 connections.
- [ ] Read-only rationale comment present at pool-init connect call.
- [ ] Fresh backend start: `sqlite3 <metadata_db_path> "PRAGMA journal_mode"` reports `wal`.
- [ ] MySQL/Postgres-attached dashboards still render (ATTACH replay works on pool, shared, and ephemeral paths).
- [ ] Existing tests pass: `cd backend && python -m pytest tests/ -x -q`.

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/backend
python -m pytest tests/ -x -q

# Unit-level smoke (run from backend/):
python - <<'EOF'
from app import db
# 1. registry replay uses per-conn sets, ephemeral conns don't leak globals
assert not hasattr(db, "_DUCK_CONN_ATTACHED")
db.register_duck_attach("t1", "ATTACH ':memory:' AS t1")
t = db._TrackedConn.__new__(db._TrackedConn)  # slots exist
import duckdb
c = duckdb.connect(":memory:")
s = set()
db._replay_attaches_on_conn(c, s)
assert "t1" in s
db._replay_attaches_on_conn(c)  # one-arg form (query.py compat) — must not raise
c.close()
# 2. pool init caps default size
db._init_duck_read_pool(":memory:")
n = 0
while True:
    try: db._DUCK_READ_POOL.get_nowait(); n += 1
    except Exception: break
assert 4 <= n <= 16, n
print("OK")
EOF

# WAL check after starting the app once:
sqlite3 "$(python -c 'from app.config import settings; print(settings.metadata_db_path)')" "PRAGMA journal_mode;"
# expect: wal

# Manual: open a dashboard backed by a MySQL-attached datasource; fire >8
# concurrent widget queries (browser refresh) — all succeed, no
# "catalog ... does not exist" errors in backend logs.
```

## Out of Scope

- Opening pool connections `read_only=True` (blocked by duckdb in-process config cache; documented instead).
- Pooling cursors off the shared connection, changing `open_duck_native`'s public contract, or touching its ~30 call sites.
- Connection max-age/recycling (conns are process-lifetime by design; health-check-on-error covers the failure mode).
- Enabling SQLite `foreign_keys` or migrating metadata schema.
- The duckdb-engine SQLAlchemy path (`_DUCK_ENGINE`, lines 761+) — separate concern.
