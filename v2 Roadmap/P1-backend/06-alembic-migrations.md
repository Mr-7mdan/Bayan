---
id: 06-alembic-migrations
title: Adopt Alembic for schema migrations
priority: P1
effort: M
depends_on: []
area: backend
---

## Problem

The SQLite metadata schema is managed by hand: `init_db()` runs `Base.metadata.create_all()` and then a ~100-line block of `PRAGMA table_info` + `ALTER TABLE ADD COLUMN` + `CREATE TABLE IF NOT EXISTS` guards, all wrapped in `except Exception: pass`. There is no version tracking, no ordering, no rollback, and a silent failure mode: if one ALTER throws, the rest of the block is skipped without any log. One-off data-migration scripts live loose in `backend/scripts/` and `backend/`. Every new column means another hand-written PRAGMA guard.

## Current State

- `backend/app/models.py:183-284` — `init_db()`:
  - `models.py:184` — `Base.metadata.create_all(bind=engine_meta)`
  - `models.py:186-263` — one giant `try/except Exception: pass` containing ad-hoc migrations:
    - `share_links.token_hash` (191), `CREATE TABLE IF NOT EXISTS user_notifications` (193), `share_permissions` (195) + index `uq_perm_dash_user` (197), `datasource_shares` (199) + `permission` column (204) + index `uq_ds_share` (205), `users.active` (210), `datasources.active` (215), `sync_states` × 7 columns (219-232), `CREATE TABLE IF NOT EXISTS sync_runs` (234-247), `sync_tasks.select_columns_json` / `custom_query` (252-254), `email_config.base_template_html` / `logo_url` (259-261)
  - `models.py:264-271` — second try block: `ai_config.base_url` (269)
  - `models.py:272-284` — third try block: `CREATE TABLE IF NOT EXISTS holiday_rules`
  - Every table created by raw SQL here also has a proper SQLAlchemy model (`UserNotification`, `SharePermission`, `DatasourceShare`, `SyncRun`, `HolidayRule`), so `Base.metadata` is the complete source of truth.
- `backend/app/models.py:173-179` — engine: `create_engine(f"sqlite+pysqlite:///{settings.metadata_db_path}", ..., poolclass=NullPool)`
- `backend/app/config.py:30` — `metadata_db_path: str = Field(default=".data/meta.sqlite")`
- `backend/app/main.py:107-129` — `@app.on_event("startup")` calls `init_db()` at line 129. This runs **per Gunicorn worker** (see `backend/run_prod_gunicorn.sh`).
- `backend/app/routers/datasources.py:17` — imports `init_db` but never calls it (dead import).
- `backend/requirements.txt:7` — `sqlalchemy>=2.0.25`; **no alembic**.
- One-off data-migration scripts (data, not schema — they stay as scripts):
  - `backend/migrate_secret_key.py` — re-encrypts credentials after SECRET_KEY rotation (reads OLD_SECRET_KEY env; SECRET_KEY present in backend/.env — never quote it)
  - `backend/scripts/migrate_widget_table_names.py` — rewrites widget `querySpec.source` JSON
  - `backend/scripts/migrate_custom_columns_syntax.py` — normalizes custom-column SQL syntax
- Packaging: `scripts/build_backend_artifact.sh:32-33` copies only `app/` and `scripts/` into the release tarball — so migration files must live **inside `backend/app/`** to ship.

## Desired State

- Alembic owns the schema. One frozen baseline revision matches current `Base.metadata`. Startup runs `upgrade head` programmatically; existing installs are auto-stamped at baseline (after a one-time legacy backfill so pre-baseline DBs converge); fresh installs get the full schema from the baseline revision. `create_all` and the PRAGMA/ALTER block are gone. New schema changes = `alembic revision --autogenerate`.

## Implementation Plan

1. **Add dependency.** `backend/requirements.txt`: add `alembic>=1.13`. Install: `cd backend && ./venv/bin/pip install "alembic>=1.13"`.

2. **Create migration package inside the app package** (so the release tarball ships it):
   ```
   backend/app/alembic/
     env.py
     script.py.mako      # copy from alembic's default template
     versions/
   backend/alembic.ini    # dev-CLI convenience only, not packaged
   ```
   `backend/alembic.ini`: `script_location = app/alembic` and leave `sqlalchemy.url` empty (env.py sets it).

3. **`backend/app/alembic/env.py`** — minimal, offline+online:
   ```python
   from alembic import context
   from app.models import Base, engine_meta

   target_metadata = Base.metadata

   def run_migrations_online():
       with engine_meta.connect() as connection:
           context.configure(
               connection=connection,
               target_metadata=target_metadata,
               render_as_batch=True,  # SQLite ALTER support
           )
           with context.begin_transaction():
               context.run_migrations()

   run_migrations_online()
   ```
   (Skip offline mode; nobody generates SQL scripts for this SQLite DB. `render_as_batch=True` is mandatory — SQLite can't ALTER most things natively.)
   Note: `alembic` CLI must run from `backend/` with the venv active so `app.*` imports resolve (same pattern as `backend/scripts/migrate_widget_table_names.py:17`).

4. **Generate the frozen baseline** against a scratch empty DB so autogenerate emits the full schema:
   ```bash
   cd backend
   METADATA_DB_PATH=/tmp/bayan_baseline.sqlite ./venv/bin/alembic revision --autogenerate -m "baseline"
   rm /tmp/bayan_baseline.sqlite
   ```
   (Confirm the env var name matches pydantic-settings mapping for `metadata_db_path`; `Settings` uses default env mapping, so `METADATA_DB_PATH` works.)
   Review the generated file in `backend/app/alembic/versions/`: it must contain create_table for all 22 tables in `models.py` (users, contacts, datasources, datasource_shares, share_permissions, dashboards, share_links, embed_tokens, user_notifications, collections, collection_items, sync_tasks, sync_states, sync_locks, sync_runs, alert_rules, alert_runs, email_config, sms_config_hadara, ai_config, holiday_rules) plus unique constraints `uq_users_email`, `uq_ds_share`, `uq_perm_dash_user`, `uq_collections_user_name`, `uq_collection_item`. Rename the file's revision id to something stable and define a module constant so code can reference it, e.g. `revision = "0001_baseline"`.

5. **Replace `init_db()` body** in `backend/app/models.py` (keep the name — `main.py:129` and the `datasources.py:17` import keep working):
   ```python
   BASELINE_REV = "0001_baseline"

   def _alembic_cfg():
       from alembic.config import Config
       cfg = Config()
       cfg.set_main_option("script_location", str(Path(__file__).parent / "alembic"))
       return cfg

   def init_db() -> None:
       from alembic import command
       from sqlalchemy import inspect
       cfg = _alembic_cfg()
       insp = inspect(engine_meta)
       tables = set(insp.get_table_names())
       if "alembic_version" not in tables and "users" in tables:
           # Existing pre-Alembic install: converge old DBs to baseline shape, then stamp.
           _legacy_backfill()
           command.stamp(cfg, BASELINE_REV)
       command.upgrade(cfg, "head")
   ```
   `_legacy_backfill()` = the current lines 186-284 moved verbatim into a private function (it is already idempotent). It runs exactly once per install (only when `alembic_version` is absent), then never again. Add `# ponytail: delete _legacy_backfill once all installs are stamped (check alembic_version exists everywhere), target v5.x`.
   Delete the old `Base.metadata.create_all` call — the baseline revision replaces it.

6. **Multi-worker safety** (Gunicorn: startup hook fires in every worker). `upgrade head` with no pending revisions is a cheap no-op, but two workers racing a real upgrade can collide on SQLite locks. Guard with a `BEGIN IMMEDIATE` advisory pattern is overkill; instead wrap `init_db()`'s body in `try/except OperationalError` with one retry after `time.sleep(2)` — losers of the race find the work already done. Also pass `connect_args={"check_same_thread": False, "timeout": 30}` on `engine_meta` (`models.py:176`) so concurrent workers wait instead of failing instantly.

7. **Remove dead import**: drop `init_db` from `backend/app/routers/datasources.py:17`.

8. **Developer workflow doc** — add a short section to `backend/deploy/DEPLOY.md`: "Schema changes: edit `app/models.py`, then `cd backend && ./venv/bin/alembic revision --autogenerate -m 'desc'`, review the generated file (SQLite: keep batch ops), commit it. Migrations apply automatically on app startup." No separate deploy step needed — startup applies migrations, which matches how `init_db` already deployed schema changes.

9. **Leave data-migration scripts alone.** `migrate_secret_key.py`, `migrate_widget_table_names.py`, `migrate_custom_columns_syntax.py` mutate row data/JSON conditioned on env secrets and live DuckDB state — they are operational one-offs, not schema versions. Converting them into Alembic revisions would make every fresh install run them pointlessly. Out of scope (below).

## Files to Modify

- `backend/requirements.txt` — add `alembic>=1.13`
- `backend/alembic.ini` — new; CLI config pointing at `app/alembic`
- `backend/app/alembic/env.py` — new; wires `Base.metadata` + `engine_meta`, `render_as_batch=True`
- `backend/app/alembic/script.py.mako` — new; default template
- `backend/app/alembic/versions/0001_baseline.py` — new; frozen autogenerated baseline
- `backend/app/models.py` — replace `init_db()` body (lines 183-284) with stamp-or-upgrade logic; move old block to `_legacy_backfill()`; add `timeout: 30` to `connect_args` at line 176
- `backend/app/routers/datasources.py` — remove unused `init_db` from import at line 17
- `backend/deploy/DEPLOY.md` — add "Schema changes" workflow section

## Acceptance Criteria

- [ ] `alembic>=1.13` in requirements; `backend/app/alembic/` ships inside the `app/` dir (covered by `build_backend_artifact.sh` which copies `app/` wholesale)
- [ ] Fresh install: deleting the DB file and starting the app creates all 22 tables + `alembic_version` containing `0001_baseline`; no `create_all` call remains in `models.py`
- [ ] Existing install: a DB copied from production (no `alembic_version`) starts cleanly, gets stamped `0001_baseline`, and all previously ad-hoc columns exist
- [ ] Old-version install: a DB missing e.g. `sync_tasks.custom_query` gets it via `_legacy_backfill()` before stamping
- [ ] Second startup on the same DB is a no-op (no errors, no schema changes)
- [ ] Two Gunicorn workers starting simultaneously do not crash on migration lock contention
- [ ] `cd backend && ./venv/bin/alembic revision --autogenerate -m test` on an up-to-date DB produces an empty (no-op) migration — proves baseline matches `Base.metadata`; delete the test revision
- [ ] `init_db` import in `datasources.py` removed; app imports clean (`./venv/bin/python -c "from app.main import app"`)

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/backend

# 1. Fresh DB
METADATA_DB_PATH=/tmp/fresh.sqlite ./venv/bin/python -c "from app.models import init_db; init_db()"
sqlite3 /tmp/fresh.sqlite "SELECT version_num FROM alembic_version; SELECT count(*) FROM sqlite_master WHERE type='table';"
# expect: 0001_baseline, and 23 tables (22 + alembic_version)

# 2. Legacy DB adoption (use a copy of the real DB — never the live file)
cp bayan.db /tmp/legacy.sqlite   # or the configured .data/meta.sqlite
METADATA_DB_PATH=/tmp/legacy.sqlite ./venv/bin/python -c "from app.models import init_db; init_db()"
sqlite3 /tmp/legacy.sqlite "SELECT version_num FROM alembic_version;"           # 0001_baseline
sqlite3 /tmp/legacy.sqlite "PRAGMA table_info(sync_tasks);" | grep custom_query # present

# 3. Idempotency
METADATA_DB_PATH=/tmp/legacy.sqlite ./venv/bin/python -c "from app.models import init_db; init_db()"  # no error

# 4. Baseline parity — must generate an empty migration, then delete it
METADATA_DB_PATH=/tmp/fresh.sqlite ./venv/bin/alembic revision --autogenerate -m parity_check
grep -c "op\." app/alembic/versions/*parity_check* ; rm app/alembic/versions/*parity_check*  # expect 0 ops (grep exits 1)

# 5. App boots end to end
./run_dev.sh &  # then:
curl -s http://127.0.0.1:8000/docs -o /dev/null -w "%{http_code}\n"  # 200

# 6. Existing tests still pass
./venv/bin/python -m pytest tests/ -q
```

## Out of Scope

- Converting the data-migration scripts (`migrate_secret_key.py`, `migrate_widget_table_names.py`, `migrate_custom_columns_syntax.py`) into Alembic revisions — they are conditional, env-dependent operational one-offs
- DuckDB analytics-file schema management (Alembic covers the SQLite metadata DB only)
- Downgrade paths in the baseline (leave autogenerated `downgrade()` as-is; never run in practice)
- Moving migration execution out of app startup into a separate deploy step (per-worker startup + retry guard is sufficient for SQLite)
- Deleting `_legacy_backfill()` — tracked via the `ponytail:` comment for a future release
