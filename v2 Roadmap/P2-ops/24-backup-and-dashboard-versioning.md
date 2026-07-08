---
id: 24-backup-and-dashboard-versioning
title: Backup strategy and dashboard/widget version history
priority: P2
effort: M
depends_on: ['06-alembic-migrations']
area: fullstack
---

## Problem

There is no backup story: the SQLite metadata DB (`.data/meta.sqlite` — users, dashboards, datasources, encrypted credentials, sync tasks) and the DuckDB analytics store (`.data/local.duckdb`) have zero scheduled backups, no retention policy, and no documented restore procedure. Separately, dashboards have no version history: every autosave (600 ms debounce on any layout/widget edit) overwrites `dashboards.definition_json` in place. One accidental widget delete + autosave = permanent loss.

## Current State

- **Dashboard storage** — `backend/app/models.py:104-112`: `Dashboard` has `id`, `user_id`, `name`, `definition_json` (Text, whole dashboard incl. all widgets), `created_at`, `updated_at`. There is **no separate Widget model** — widgets live inside `definition_json.widgets`. Versioning the dashboard row versions the widgets too.
- **Single write chokepoint** — `backend/app/models.py:312-324`: `save_dashboard(db, user_id, name, definition, dash_id)` — updates in place (`d.definition_json = json.dumps(definition)`) with no history.
- **Callers of the write path**:
  - `backend/app/routers/dashboards.py:236-266` — `POST /dashboards` (`save_dash`) → `save_dashboard` (line 251).
  - `backend/app/routers/dashboards.py:740-748` — import upsert: the update branch (740-745) writes `d.definition_json` **directly**, bypassing `save_dashboard`; the create branch (747) uses it.
- **Frontend save paths** — `frontend/src/app/page.tsx:672-704` (`scheduleServerSave`, 600 ms debounce → `Api.saveDashboard`) and `page.tsx:1328` (title-change immediate save). API client: `frontend/src/lib/api.ts:736` (`saveDashboard`).
- **Databases** — `backend/app/config.py:30` `metadata_db_path` default `.data/meta.sqlite`; `config.py:27` `duckdb_path` default `.data/local.duckdb`. Engine: `models.py:173-180` (`sqlite+pysqlite`, NullPool). Active DuckDB file is switchable at runtime via a `.data/duckdb.active` pointer file — resolve with `get_active_duck_path()` (`backend/app/db.py:618`). `open_duck_native()` (`db.py:525`) opens an extra native connection to the active store.
- **Scheduler exists** — `backend/app/scheduler.py:22` `ensure_scheduler_started()` (APScheduler `BackgroundScheduler`), `scheduler.py:67` `schedule_all_jobs()` (CronTrigger jobs with `replace_existing`/`max_instances=1` pattern). Started in `backend/app/main.py:156-161` gated by `RUN_SCHEDULER` env; `backend/run_prod_gunicorn.sh:33` defaults `RUN_SCHEDULER=0` under multi-worker gunicorn (scheduler runs in a single designated process). Backup job inherits this gating for free.
- **Admin router pattern** — `backend/app/routers/admin.py:31` `_is_admin`; admin-gated ops endpoints like `/admin/scheduler/refresh` (line 45) and `/admin/duckdb/active` (153-161) show the pattern to copy.
- **Schema migrations** — after spec 06, Alembic owns the schema (`backend/app/alembic/versions/`); new tables are autogenerate revisions, not `init_db()` PRAGMA hacks.
- **UI entry point** — `frontend/src/components/builder/TitleBar.tsx:236-247`: actions kebab menu (Save / Pack rows / Publish…) — natural home for "Version history".

## Desired State

1. Nightly `VACUUM INTO` backup of the SQLite metadata DB into `.data/backups/`, count-based retention, admin API to trigger/list, documented restore runbook. DuckDB: optional file copy (it is a rebuildable cache — sync tasks can repopulate it), documented as such.
2. `dashboard_versions` table capturing the prior definition on every meaningful save (time-coalesced to avoid autosave churn), list + restore API, "Version history" dialog in the builder, per-dashboard pruning.

## Implementation Plan

### Part A — Metadata backups

1. **Config** — `backend/app/config.py`, add to `Settings`:
   ```python
   backup_dir: str = Field(default=".data/backups", validation_alias=AliasChoices("BACKUP_DIR"))
   backup_cron: str = Field(default="30 3 * * *", validation_alias=AliasChoices("BACKUP_CRON"))
   backup_retention: int = Field(default=14, validation_alias=AliasChoices("BACKUP_RETENTION"))
   backup_include_duckdb: bool = Field(default=False, validation_alias=AliasChoices("BACKUP_INCLUDE_DUCKDB"))
   ```

2. **New module `backend/app/backup.py`** — `run_backup() -> dict`:
   - `mkdir -p` `settings.backup_dir`.
   - SQLite: use stdlib `sqlite3` directly (NOT `engine_meta` — SQLAlchemy autobegins a transaction and `VACUUM` cannot run inside one):
     ```python
     import sqlite3
     dest = Path(settings.backup_dir) / f"meta-{datetime.now():%Y%m%d-%H%M%S}.sqlite"
     con = sqlite3.connect(settings.metadata_db_path)
     try:
         con.execute("VACUUM INTO ?", (str(dest),))
     finally:
         con.close()
     ```
     `VACUUM INTO` is online-safe (consistent snapshot, no writer lock held for the duration) and compacts the copy.
   - DuckDB (only when `settings.backup_include_duckdb`): resolve the live file via `get_active_duck_path()` (`app/db.py:618`), open a second native connection with `open_duck_native()` (`db.py:525`), run `CHECKPOINT`, close, then `shutil.copy2` to `duck-{ts}.duckdb`. Add comment: `# ponytail: copy after CHECKPOINT is best-effort — DuckDB store is rebuildable from sync sources; upgrade to duckdb EXPORT DATABASE if strict consistency ever matters`.
   - Retention: glob `meta-*.sqlite` (and `duck-*.duckdb`) in `backup_dir`, sort by name desc (timestamped names sort chronologically), unlink everything past `settings.backup_retention` per prefix.
   - Return `{"sqlite": str(dest), "duckdb": ..., "pruned": n, "duration_ms": ...}`. Log one line on success/failure — never raise out of the scheduled job.

3. **Schedule it** — `backend/app/scheduler.py`, add:
   ```python
   def schedule_backup_job() -> None:
       from .backup import run_backup
       from .config import settings
       sched = ensure_scheduler_started()
       trig = CronTrigger.from_crontab(settings.backup_cron, timezone=_SCHEDULER_TZ)
       sched.add_job(func=run_backup, trigger=trig, id="backup:meta",
                     replace_existing=True, max_instances=1, coalesce=True,
                     misfire_grace_time=3600)
   ```
   Call `schedule_backup_job()` in `backend/app/main.py` startup right after `schedule_all_alert_jobs()` (line 161), inside the existing `RUN_SCHEDULER` gate and try/except.

4. **Admin endpoints** — `backend/app/routers/admin.py` (copy the `_is_admin` + 403 pattern from `scheduler_refresh`, line 45):
   - `POST /admin/backup/run` → calls `run_backup()`, returns its dict.
   - `GET /admin/backup/list` → `[{name, sizeBytes, mtime}]` from `settings.backup_dir` (sorted newest first).

5. **Restore runbook** — create `docs/ops/backup-restore.md`:
   - SQLite restore: stop backend → `cp .data/backups/meta-<ts>.sqlite .data/meta.sqlite` → start backend (Alembic `upgrade head` on startup reconciles if the backup predates newer migrations).
   - DuckDB restore: either copy a `duck-*.duckdb` backup over the file named in `.data/duckdb.active` (backend stopped), or simply re-run sync tasks ("Sync now") to rebuild — the DuckDB store is derived data.
   - Note: `.data/backups/` lives on the same disk; for real DR, ship it off-host (cron + rsync/rclone — out of scope here, but say so in the doc).

### Part B — Dashboard version history

6. **Model** — `backend/app/models.py`, next to `Dashboard` (line 104):
   ```python
   class DashboardVersion(Base):
       __tablename__ = "dashboard_versions"
       id: Mapped[str] = mapped_column(String, primary_key=True)
       dashboard_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
       name: Mapped[str] = mapped_column(String, nullable=False)
       definition_json: Mapped[str] = mapped_column(Text, nullable=False)
       created_by: Mapped[Optional[str]] = mapped_column(String, nullable=True)
       created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
   ```

7. **Alembic revision** (this is why `depends_on: 06`):
   ```bash
   cd backend && ./venv/bin/alembic revision --autogenerate -m "dashboard_versions"
   ```
   Review the generated file (one `create_table` + index), then rely on startup `upgrade head`.

8. **Snapshot in the chokepoint** — `backend/app/models.py`, modify `save_dashboard` (line 312). Before overwriting on the update path, snapshot the **old** state, time-coalesced:
   ```python
   _VERSION_COALESCE_SEC = 300  # ponytail: 1 version per 5 min per dashboard max; tune via config if users ask

   def _snapshot_dashboard_version(db, d: Dashboard, actor: Optional[str], force: bool = False) -> None:
       latest = (db.query(DashboardVersion)
                   .filter(DashboardVersion.dashboard_id == d.id)
                   .order_by(DashboardVersion.created_at.desc()).first())
       if latest and latest.definition_json == d.definition_json:
           return  # identical — nothing to keep
       if not force and latest and latest.created_at and \
          (datetime.utcnow() - latest.created_at).total_seconds() < _VERSION_COALESCE_SEC:
           return  # autosave churn — coalesce
       db.add(DashboardVersion(id=str(uuid4()), dashboard_id=d.id, name=d.name,
                               definition_json=d.definition_json, created_by=actor))
       # prune: keep newest 20
       ids = [r.id for r in db.query(DashboardVersion.id)
                .filter(DashboardVersion.dashboard_id == d.id)
                .order_by(DashboardVersion.created_at.desc()).offset(20).all()]
       if ids:
           db.query(DashboardVersion).filter(DashboardVersion.id.in_(ids)).delete(synchronize_session=False)
   ```
   In `save_dashboard`, on the `dash_id` branch: call `_snapshot_dashboard_version(db, d, actor=user_id)` **before** mutating `d.name`/`d.definition_json`, only when the new `json.dumps(definition)` differs from `d.definition_json`. Add optional `actor: Optional[str] = None, force_version: bool = False` params (default keeps all existing call signatures valid).
   (`DashboardVersion.created_at` is stored in UTC by SQLite `CURRENT_TIMESTAMP`, matching `datetime.utcnow()` — same convention the rest of models.py uses.)

9. **Fix the bypassing sibling** — `backend/app/routers/dashboards.py:740-745` (import update branch writes `d.definition_json` directly). Replace the manual `d.name = ...; d.definition_json = ...; db.add/commit/refresh` block with:
   ```python
   d = save_dashboard(db, user_id=owner, name=it.name, definition=defn, dash_id=d.id)
   ```
   so imports also produce a version snapshot.

10. **Version API** — `backend/app/routers/dashboards.py`. IMPORTANT: register these routes **above** `GET /{dash_id}` (line 269) or reuse the fact that `/{dash_id}/versions` has more path segments (FastAPI matches fixed-structure routes fine here — `/{dash_id}` only captures single-segment paths, so order is actually safe; place them after `save_dash` for readability):
    - `GET /dashboards/{dash_id}/versions` — permission: owner, admin, or shared ro/rw (copy the read-permission block from `get_dash`, lines 274-280). Returns `[{id, createdAt, createdBy, name, widgetsCount}]` (parse `definition_json` only to count `widgets` keys; do NOT return full definitions in the list).
    - `POST /dashboards/{dash_id}/versions/{version_id}/restore` — permission: owner, admin, or `rw` (copy the write-permission block from `save_dash`, lines 240-250). Steps: load version (404 if missing or `dashboard_id` mismatch); call `save_dashboard(db, user_id=d.user_id, name=version.name, definition=json.loads(version.definition_json), dash_id=dash_id, actor=actorId, force_version=True)` — `force_version=True` makes the pre-restore state a version immediately (so restore is always undoable). Return `DashboardOut` (reuse the sanitize-and-validate block from `get_dash`, lines 282-312, extracted into a small helper `_definition_out(d)` to avoid a third copy — `get_public` lines 538-567 already duplicates it; refactor all three to the helper).
    - Delete cascade: in `delete_dashboard` (`models.py:578`), also `db.query(DashboardVersion).filter(dashboard_id == dash_id).delete()`.

11. **Frontend API client** — `frontend/src/lib/api.ts` (next to `saveDashboard`, line 736):
    ```ts
    listDashboardVersions: (id: string, actorId?: string) =>
      http<DashboardVersionItem[]>(`/dashboards/${encodeURIComponent(id)}/versions${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`),
    restoreDashboardVersion: (id: string, versionId: string, actorId?: string) =>
      http<DashboardOut>(`/dashboards/${encodeURIComponent(id)}/versions/${encodeURIComponent(versionId)}/restore${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'POST' }),
    ```
    with `type DashboardVersionItem = { id: string; createdAt: string; createdBy?: string | null; name: string; widgetsCount: number }`.

12. **UI entry point** — minimal:
    - `frontend/src/components/builder/TitleBar.tsx`: add optional prop `onVersionsAction?: () => void`; add a menu item `Version history…` in the actions menu after `Save` (line 239 pattern), `disabled={!dashboardId}`.
    - `frontend/src/app/page.tsx`: state `versionsOpen`; pass `onVersionsAction={() => setVersionsOpen(true)}` to `TitleBar` (props block at 1308-1321). Render a `Dialog.Root` (copy the Publish dialog pattern, line 1774): on open, fetch `Api.listDashboardVersions(dashboardId, user?.id)`; render rows `createdAt (localized) — name — N widgets — [Restore]`; Restore → `confirm()` → `Api.restoreDashboardVersion(...)` → apply returned `definition` to state exactly like `onLoad` does (call the existing `onLoad()` handler — it re-fetches the dashboard), close dialog, toast "Restored".

## Files to Modify

- `backend/app/config.py` — add 4 backup settings fields.
- `backend/app/backup.py` — **new**: `run_backup()` (VACUUM INTO, optional DuckDB copy, retention prune).
- `backend/app/scheduler.py` — add `schedule_backup_job()`.
- `backend/app/main.py` — call `schedule_backup_job()` in startup (after line 161).
- `backend/app/routers/admin.py` — `POST /admin/backup/run`, `GET /admin/backup/list`.
- `backend/app/models.py` — `DashboardVersion` model, `_snapshot_dashboard_version()`, hook into `save_dashboard` (+`actor`/`force_version` params), cascade delete in `delete_dashboard`.
- `backend/app/alembic/versions/xxxx_dashboard_versions.py` — **new** autogenerated revision.
- `backend/app/routers/dashboards.py` — versions list/restore endpoints; import branch (740-745) routed through `save_dashboard`; extract `_definition_out()` helper.
- `frontend/src/lib/api.ts` — `listDashboardVersions`, `restoreDashboardVersion`, `DashboardVersionItem`.
- `frontend/src/components/builder/TitleBar.tsx` — `Version history…` menu item + prop.
- `frontend/src/app/page.tsx` — versions dialog + restore wiring.
- `docs/ops/backup-restore.md` — **new** restore runbook.

## Acceptance Criteria

- [ ] `POST /api/admin/backup/run` (admin) creates `.data/backups/meta-<ts>.sqlite`; the copy opens with `sqlite3` and contains the `dashboards` table.
- [ ] With >retention backups present, the next run deletes the oldest so exactly `BACKUP_RETENTION` remain.
- [ ] `backup:meta` job appears in `GET /api/admin/scheduler/jobs` when `RUN_SCHEDULER=1`.
- [ ] Saving a dashboard with a changed definition creates a `dashboard_versions` row of the **prior** state; two saves within 5 min coalesce to one version; identical saves create none.
- [ ] Import of an existing dashboard id also snapshots (bypass at dashboards.py:740-745 removed).
- [ ] Per-dashboard versions capped at 20 (oldest pruned).
- [ ] `GET /dashboards/{id}/versions` respects read permissions; restore respects write permissions (403 for ro-share actor).
- [ ] Restore returns the restored definition, and the pre-restore state exists as the newest version (restore is undoable).
- [ ] Deleting a dashboard deletes its versions.
- [ ] Builder actions menu shows "Version history…"; restoring from the dialog reloads the canvas with the old layout/widgets.
- [ ] Alembic revision applies cleanly on both a fresh DB and an existing `.data/meta.sqlite`.
- [ ] No secret values in backups docs or spec (backups themselves contain encrypted credentials — runbook notes to protect `backup_dir` with same file perms as `.data`).

## Verification

```bash
cd backend
# migration
./venv/bin/alembic upgrade head
sqlite3 .data/meta.sqlite ".schema dashboard_versions"

# backup
curl -s -X POST "http://localhost:8000/api/admin/backup/run?actorId=<ADMIN_ID>" | jq
ls -la .data/backups/
sqlite3 .data/backups/meta-*.sqlite "select count(*) from dashboards;"

# versioning: save twice with different definitions >5min apart (or temporarily set _VERSION_COALESCE_SEC=0)
curl -s -X POST http://localhost:8000/api/dashboards -H 'Content-Type: application/json' \
  -d '{"id":"<DASH_ID>","name":"t","userId":"dev_user","definition":{"layout":[],"widgets":{}}}'
curl -s "http://localhost:8000/api/dashboards/<DASH_ID>/versions?actorId=dev_user" | jq
curl -s -X POST "http://localhost:8000/api/dashboards/<DASH_ID>/versions/<VER_ID>/restore?actorId=dev_user" | jq '.definition.widgets | keys'

# permissions: ro-share actor gets 403 on restore
curl -s -o /dev/null -w '%{http_code}\n' -X POST "http://localhost:8000/api/dashboards/<DASH_ID>/versions/<VER_ID>/restore?actorId=<RO_USER>"
```
Manual: open a dashboard at :3000, delete a widget (autosave fires), actions menu → Version history… → Restore the previous entry → widget reappears; restore again → returns to post-delete state (undo of undo).

## Out of Scope

- Off-host/remote backup shipping (rsync/rclone/S3) — runbook mentions it, not implemented.
- Point-in-time WAL archiving (litestream) — VACUUM INTO nightly is enough at this scale.
- DuckDB `EXPORT DATABASE`/parquet backups — DuckDB store is rebuildable from sync sources.
- Version diff viewer / named versions / per-widget granular history — list + restore only.
- Versioning datasources, alert rules, or sync tasks.
- Backup encryption (backups inherit the already-encrypted credential columns).
