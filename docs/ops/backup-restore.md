# Backup & Restore Runbook

Bayan backs up the **SQLite metadata DB** (`.data/meta.sqlite`) nightly via
`VACUUM INTO`. The DuckDB analytics store (`.data/local.duckdb`) is rebuildable
derived data and is only backed up when explicitly enabled.

## What is backed up

| Store | Contents | Backed up |
|-------|----------|-----------|
| `meta.sqlite` | users, dashboards, datasources, **encrypted** credentials, sync tasks, dashboard version history | Always (nightly) |
| `local.duckdb` | materialized analytics tables | Only if `BACKUP_INCLUDE_DUCKDB=1` |

Backups land in `BACKUP_DIR` (default `.data/backups/`) as
`meta-YYYYMMDD-HHMMSS.sqlite` (and `duck-*.duckdb` when enabled).

## Configuration (env)

| Var | Default | Meaning |
|-----|---------|---------|
| `BACKUP_DIR` | `.data/backups` | Where backup files are written |
| `BACKUP_CRON` | `30 3 * * *` | Nightly schedule (scheduler timezone) |
| `BACKUP_RETENTION` | `14` | Keep this many per prefix; older pruned |
| `BACKUP_INCLUDE_DUCKDB` | `false` | Also copy the DuckDB store |

The backup job runs only in the process where `RUN_SCHEDULER=1` (single
designated worker under multi-worker gunicorn).

## Admin API

- `POST /api/admin/backup/run` — trigger a backup now; returns
  `{sqlite, duckdb, pruned, duration_ms}`.
- `GET /api/admin/backup/list` — `[{name, sizeBytes, mtime}]`, newest first.

Both require an admin session.

## Restore

### SQLite (metadata)
1. Stop the backend.
2. `cp .data/backups/meta-<ts>.sqlite .data/meta.sqlite`
3. Start the backend. Alembic `upgrade head` runs on startup and reconciles the
   schema if the backup predates newer migrations.

### DuckDB (analytics)
Either:
- Stop the backend, copy a `duck-*.duckdb` backup over the file named in
  `.data/duckdb.active`, restart; **or**
- Simply re-run sync tasks ("Sync now") to rebuild — the DuckDB store is
  derived from the sync sources.

## Security

Backups contain the same **already-encrypted** credential columns as the live
DB — no plaintext secrets. Protect `BACKUP_DIR` with the same filesystem
permissions as `.data/`.

## Off-host / DR (out of scope here)

`.data/backups/` lives on the same disk as the DB. For real disaster recovery,
ship backups off-host (e.g. a cron `rsync`/`rclone` to remote storage or S3).
Not implemented by Bayan — wire it up at the ops layer.
