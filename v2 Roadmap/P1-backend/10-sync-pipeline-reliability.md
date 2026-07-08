---
id: 10-sync-pipeline-reliability
title: "Sync pipeline reliability: timezones, blocking, cache busting"
priority: P1
effort: M
depends_on: []
area: backend
---

## Problem

Three reliability defects in the sync pipeline:

1. **Timezone juggling.** Stuck-sync detection and abort logic mix naive and aware datetimes via ad-hoc `.replace(tzinfo=None)`. SQLite `DateTime` columns are naive; writes use aware `datetime.now(timezone.utc)`. Any code path that stores a non-UTC or aware-with-offset value silently corrupts elapsed-time math (DST shifts, server TZ changes) → syncs falsely reset or never reset.
2. **Scheduler blocking.** `run_task_job` executes the whole sync synchronously inside APScheduler's worker thread with `max_instances=1`. A hung DB driver call (MySQL network stall, DuckDB lock) wedges that job forever — no timeout exists anywhere. After enough hangs the scheduler's default 10-thread pool is exhausted and ALL jobs (syncs + alerts) stop.
3. **No cache invalidation on sync.** Query result cache is TTL-only. A completed sync writes new rows to DuckDB but cached results (Redis TTL configurable via `RESULT_CACHE_TTL`, can be minutes) keep serving stale data with no way to bust them.

## Current State

All refs verified 2026-07-07.

**Naive/aware juggling — `backend/app/scheduler.py:178-235` (`_auto_reset_stuck`):**
```python
# scheduler.py:196
now = datetime.now(timezone.utc).replace(tzinfo=None)
...
# scheduler.py:209
elapsed = now - last_activity.replace(tzinfo=None)
...
# scheduler.py:223
elapsed_str = f"{(now - last_activity.replace(tzinfo=None)).total_seconds()/60:.0f}min" ...
```
Sibling with the same pattern — `backend/app/routers/datasources.py:1224` (abort endpoint's stuck check):
```python
time_since_update = datetime.now(timezone.utc).replace(tzinfo=None) - last_update.replace(tzinfo=None)
```
Sibling inline normalizations (already correct but duplicated) — `scheduler.py:147-150` and `scheduler.py:407-410`:
```python
if _last.tzinfo is None:
    _last = _last.replace(tzinfo=_tz_mod.utc)
```
Naive local `now` handed to APScheduler — `backend/app/routers/datasources.py:731-738` (enqueue path of `run_sync_now`):
```python
# Use local now (not utcnow) for DateTrigger to avoid timezone misfire by ~3h
sched.add_job(func=run_task_job, trigger=DateTrigger(run_date=_dt.now()), ...)
```
APScheduler localizes naive datetimes to the *scheduler* timezone (`_SCHEDULER_TZ`, `scheduler.py:12`), not the OS-local zone — this "fix" misfires again whenever `SCHEDULER_TIMEZONE` differs from the host TZ.

**Model columns are naive** — `backend/app/models.py:752-775` (`SyncState`): `last_run_at`, `started_at`, `progress_updated_at` are plain `DateTime` (no `timezone=True`). SQLite has no TZ storage; convention must be "naive = UTC".

**Blocking scheduler job — `backend/app/scheduler.py:238-260` (`run_task_job`):**
```python
def run_task_job(ds_id: str, task_id: str) -> None:
    db = SessionLocal()
    try:
        _auto_reset_stuck(db, ds_id, stale_threshold_minutes=30)
        ds: Optional[Datasource] = db.get(Datasource, ds_id)
        actor = (ds.user_id if ds and ds.user_id else None)
        ds_router.run_sync_now(ds_id, response=Response(), taskId=task_id, execute=True, actorId=actor, db=db)
```
Jobs registered with `max_instances=1` at `scheduler.py:105-114`. `run_sync_now` (`datasources.py:654-1108`) runs the full batch loop inline; the sync engines (`run_sequence_sync` `backend/app/db.py:1436`, `run_snapshot_sync` `db.py:1650`) already accept a cooperative `should_abort` callback checked per batch (`db.py:1549`, `db.py:1633`, `db.py:1734`) — wired to `SyncState.cancel_requested` via `_check_abort` (`datasources.py:820-833`). There is no timeout that sets it.

**TTL-only result cache — `backend/app/routers/query.py:757-843`:**
```python
# query.py:758
_CACHE_TTL_SECONDS = 5
# query.py:781-784
def _cache_key(prefix: str, datasource_id: Optional[str], sql_inner: str, params: Dict[str, Any]) -> str:
    ds = datasource_id or "__local__"
    items = ",".join(f"{k}={repr(v)}" for k, v in sorted(params.items()))
    return f"{prefix}|{ds}|{sql_inner}|{items}"
```
Redis TTL configurable via `RESULT_CACHE_TTL` env (`query.py:774-778`); Redis client helper `_get_redis()` at `query.py:635-649`. Cache key call sites: `query.py:1978` + `2053` (duck path, `cache_ds = f"{payload.datasourceId or '__local__'}@{db_path}"` at `query.py:1897`), `2212` + `2242` (remote SQL path), `6166` (distinct values), `9548` (pivot). No invalidation function exists.

**Sync completion points** (where new data becomes visible, all in `run_sync_now`): API `datasources.py:925-937`, sequence `:964-971`, snapshot `:994-1000`; per-task `finally` commits at `:1069-1096`; lock release + return at `:1098-1108`.

**Config style:** `backend/app/config.py:7` `class Settings(BaseSettings)` with `model_config = SettingsConfigDict(env_file=".env", ...)` at line 15; existing example `scheduler_timezone` at line 90.

## Desired State

1. One shared helper converts any DB datetime to aware UTC; all elapsed-time math is aware-UTC vs aware-UTC. No `.replace(tzinfo=None)` left in sync/scheduler code.
2. `run_task_job` runs the sync in a dedicated bounded thread pool with a configurable wall-clock timeout. On timeout it sets `cancel_requested=True` (cooperative abort the sync engines already honor) and returns — the APScheduler worker thread is never wedged.
3. Completed syncs bump a result-cache generation counter that is part of every cache key, so stale entries are unreachable immediately (works for both process-local dict and Redis).

## Implementation Plan

### Step 1 — `as_utc` helper (new file `backend/app/timeutil.py`)

```python
from datetime import datetime, timezone

def as_utc(dt: datetime | None) -> datetime | None:
    """DB convention: naive datetimes are UTC. Returns aware UTC."""
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
```
New module (not models.py) to avoid import cycles: scheduler.py, routers, and models can all import it.

### Step 2 — normalize comparisons

1. `backend/app/scheduler.py` `_auto_reset_stuck` (lines 196, 209, 223): replace with
   ```python
   from .timeutil import as_utc
   now = datetime.now(timezone.utc)                 # line 196
   elapsed = now - as_utc(last_activity)            # line 209
   # line 223: (now - as_utc(last_activity)).total_seconds()
   ```
2. `backend/app/routers/datasources.py:1224`:
   ```python
   time_since_update = datetime.now(timezone.utc) - as_utc(last_update)
   ```
3. `scheduler.py:147-150` and `:407-410` catch-up blocks: replace the 3-line inline naive check with `_last = as_utc(_st.last_run_at)` / `_last = as_utc(a.last_run_at)`.
4. `datasources.py:731-738`: replace `DateTrigger(run_date=_dt.now())` with `DateTrigger(run_date=datetime.now(timezone.utc))` (aware — APScheduler converts correctly regardless of `SCHEDULER_TIMEZONE`). Delete the misleading comment at line 731. `datetime`/`timezone` are already imported at datasources.py top; drop the local `from datetime import datetime as _dt` at line 721 if now unused.

Do NOT change model columns or write paths — naive-UTC storage stays the convention (SQLite; migration not worth it). Aware values written by `datetime.now(timezone.utc)` round-trip fine through the helper.

### Step 3 — sync job timeout (scheduler isolation)

1. `backend/app/config.py` — add to `Settings`:
   ```python
   sync_job_timeout_seconds: int = Field(default=3600, alias="SYNC_JOB_TIMEOUT_SECONDS")
   ```
   (match alias/Field style used by `scheduler_timezone` at config.py:90).
2. `backend/app/scheduler.py` — module-level dedicated pool and rewritten `run_task_job`:
   ```python
   from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
   # ponytail: bounded shared pool; a truly hung driver call leaks one thread until restart —
   # acceptable, capped at 4. Per-datasource pools if that ever bites.
   _SYNC_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="sync-job")

   def _execute_sync(ds_id: str, task_id: str) -> None:
       db = SessionLocal()
       try:
           ds = db.get(Datasource, ds_id)
           actor = (ds.user_id if ds and ds.user_id else None)
           ds_router.run_sync_now(ds_id, response=Response(), taskId=task_id, execute=True, actorId=actor, db=db)
       finally:
           db.close()

   def run_task_job(ds_id: str, task_id: str) -> None:
       db = SessionLocal()
       try:
           _auto_reset_stuck(db, ds_id, stale_threshold_minutes=30)
       finally:
           db.close()
       fut = _SYNC_POOL.submit(_execute_sync, ds_id, task_id)
       try:
           fut.result(timeout=max(60, int(settings.sync_job_timeout_seconds)))
       except FutureTimeout:
           print(f"[SYNC_JOB] TIMEOUT task={task_id} ds={ds_id} after {settings.sync_job_timeout_seconds}s — requesting abort", flush=True)
           _request_cancel(task_id)
       except Exception as e:
           print(f"[SYNC_JOB] FAILED task={task_id} ds={ds_id}: {e}", flush=True)

   def _request_cancel(task_id: str) -> None:
       db = SessionLocal()
       try:
           from .models import SyncState
           st = db.query(SyncState).filter(SyncState.task_id == task_id).first()
           if st and st.in_progress:
               st.cancel_requested = True
               db.add(st); db.commit()
       except Exception:
           db.rollback()
       finally:
           db.close()
   ```
   Keep the existing broad exception handling philosophy (`scheduler.py:252-255` comment: unhandled exceptions kill the scheduler). Note `run_task_job` still blocks its APScheduler slot up to the timeout — that is fine (`max_instances=1` still prevents overlap); the point is it can no longer block *forever*, and `cancel_requested` makes the runaway worker exit at its next batch boundary (`_check_abort`, `datasources.py:820`). `_auto_reset_stuck` remains the backstop for workers that never reach a batch boundary.
3. `shutdown_scheduler` (`scheduler.py:46-60`): add `_SYNC_POOL.shutdown(wait=False, cancel_futures=True)` (best-effort, inside the existing try).

### Step 4 — cache generation busting

1. `backend/app/routers/query.py` — in the cache section (after `_RC_TTL_SECONDS`, ~line 779):
   ```python
   # ponytail: single global generation — any sync busts all cached results.
   # Fine at hourly sync cadence + seconds-level TTLs; per-datasource keys if churn matters.
   _RESULT_CACHE_GEN = 0

   def _cache_generation() -> int:
       r = _get_redis()
       if r is not None:
           try:
               return int(r.get("q:gen") or 0)
           except Exception:
               pass
       return _RESULT_CACHE_GEN

   def bump_result_cache_generation() -> None:
       global _RESULT_CACHE_GEN
       _RESULT_CACHE_GEN += 1
       r = _get_redis()
       if r is not None:
           try:
               r.incr("q:gen")
           except Exception:
               pass
   ```
2. Extend `_cache_key` (`query.py:781-784`) — one-line change so all 6+ call sites (1978, 2053, 2212, 2242, 6166, 9548) are covered without touching them:
   ```python
   return f"{prefix}|g{_cache_generation()}|{ds}|{sql_inner}|{items}"
   ```
3. `backend/app/routers/datasources.py` — in `run_sync_now`, after the task loop and before releasing locks (~line 1098), bump once if anything ran:
   ```python
   if results:
       try:
           from .query import bump_result_cache_generation
           bump_result_cache_generation()
       except Exception:
           pass
   ```
   Import locally (codebase convention for cross-router imports; avoids any cycle — verified query.py does not import datasources.py).

### Step 5 — smoke check

Add `backend/tests/test_sync_reliability.py` (or `backend/test_sync_reliability.py` if no tests dir exists — check first) with two asserts, no fixtures:
```python
from datetime import datetime, timezone
from app.timeutil import as_utc

def test_as_utc():
    naive = datetime(2026, 1, 1, 12, 0)
    assert as_utc(naive) == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    aware = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert as_utc(aware) is not None and as_utc(aware).utcoffset().total_seconds() == 0
    assert as_utc(None) is None

def test_cache_gen_changes_key():
    from app.routers import query as q
    k1 = q._cache_key("sql", "ds1", "select 1", {})
    q.bump_result_cache_generation()
    k2 = q._cache_key("sql", "ds1", "select 1", {})
    assert k1 != k2
```

## Files to Modify

- `backend/app/timeutil.py` — NEW: `as_utc()` helper (~10 lines).
- `backend/app/scheduler.py` — use `as_utc` in `_auto_reset_stuck` (196/209/223) and catch-up blocks (147-150, 407-410); rewrite `run_task_job` with `_SYNC_POOL` + timeout + `_request_cancel`; pool shutdown in `shutdown_scheduler`.
- `backend/app/routers/datasources.py` — `as_utc` at line 1224; aware `DateTrigger` run_date at 731-738; `bump_result_cache_generation()` call after task loop (~1098).
- `backend/app/routers/query.py` — `_cache_generation` / `bump_result_cache_generation` + generation segment in `_cache_key` (781-784).
- `backend/app/config.py` — `sync_job_timeout_seconds` setting.
- `backend/tests/test_sync_reliability.py` — NEW: two smoke tests.

## Acceptance Criteria

- [ ] `grep -rn "replace(tzinfo=None)" backend/app/scheduler.py backend/app/routers/datasources.py` returns nothing.
- [ ] `_auto_reset_stuck` still resets a state whose `progress_updated_at` is >30 min old and leaves fresh ones alone (manual check per Verification).
- [ ] A scheduler-triggered sync exceeding `SYNC_JOB_TIMEOUT_SECONDS` gets `cancel_requested=True` and the APScheduler worker is released (log shows `[SYNC_JOB] TIMEOUT`), and subsequent scheduled jobs still fire.
- [ ] `SYNC_JOB_TIMEOUT_SECONDS` env var overrides the 3600s default.
- [ ] After a sync completes, an identical query issued within the cache TTL returns the new row count (generation changed → cache miss), both with and without `REDIS_URL` set.
- [ ] Manual "Run now" (enqueue path, `execute=false`) fires immediately regardless of `SCHEDULER_TIMEZONE` value.
- [ ] Both smoke tests pass.

## Verification

```bash
# 1. Unit smoke
cd backend && python -m pytest tests/test_sync_reliability.py -q

# 2. No naive-datetime juggling left in sync code
grep -rn "replace(tzinfo=None)" backend/app/scheduler.py backend/app/routers/datasources.py  # expect empty

# 3. Timeout path: start backend with a tiny timeout, trigger a scheduled-style run
SYNC_JOB_TIMEOUT_SECONDS=60 uvicorn app.main:app --port 8000  # from backend/
# trigger a long sync via API, watch logs for "[SYNC_JOB] TIMEOUT" then confirm:
sqlite3 backend/app.db "SELECT task_id, in_progress, cancel_requested FROM sync_states WHERE cancel_requested=1;"

# 4. Cache busting: run a widget query twice around a sync
curl -s -X POST localhost:8000/api/datasources/<DS_ID>/sync/run?execute=true&actorId=<ADMIN_ID>
# immediately re-issue the same /api/query request the dashboard sends; response reflects new rows
# (before this change, with RESULT_CACHE_TTL=60 the old rows were served for up to 60s)

# 5. Stuck reset still works: mark a state stale, run scheduler job path
sqlite3 backend/app.db "UPDATE sync_states SET in_progress=1, progress_updated_at=datetime('now','-45 minutes') WHERE task_id='<TASK_ID>';"
# trigger the task's scheduled run (or call run_task_job in a shell); log shows [AUTO_RESET_STUCK] Resetting
```

## Out of Scope

- Migrating SQLite `DateTime` columns to `DateTime(timezone=True)` or aware storage (convention "naive = UTC" retained).
- The many `datetime.utcnow()` sites in `alerts_service.py` / `alerts.py` / `contacts.py` (alerts pipeline, separate concern).
- Per-datasource cache generations (global counter is deliberate — see ponytail note).
- Hard-killing hung sync threads (cooperative abort only; impossible to kill threads safely in CPython).
- Moving sync execution to a real task queue (Celery/RQ) — APScheduler + bounded pool suffices at current scale.
- Frontend changes (progress UI already polls `SyncState`).
