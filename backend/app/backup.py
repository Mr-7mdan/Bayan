"""Metadata (SQLite) backup — nightly VACUUM INTO with count-based retention.

The SQLite metadata DB holds users, dashboards, datasources and *already-
encrypted* credential columns; VACUUM INTO copies it verbatim, so no plaintext
secret ever leaves the DB. We log paths and counts only — never row contents.

DuckDB is a rebuildable analytics cache (sync tasks repopulate it); it is only
copied when BACKUP_INCLUDE_DUCKDB is set. See docs/ops/backup-restore.md.
"""
from __future__ import annotations

import glob
import logging
import shutil
import sqlite3
import time
from datetime import datetime
from pathlib import Path

from .config import settings

logger = logging.getLogger("bayan.backup")


def _prune(backup_dir: Path, prefix: str, suffix: str, keep: int) -> int:
    """Delete all but the newest `keep` files matching prefix*suffix.
    Timestamped names sort chronologically, so name-desc == newest-first."""
    files = sorted(glob.glob(str(backup_dir / f"{prefix}*{suffix}")), reverse=True)
    pruned = 0
    for stale in files[keep:]:
        try:
            Path(stale).unlink()
            pruned += 1
        except Exception as e:
            logger.warning("backup prune failed for %s: %s", stale, e)
    return pruned


def run_backup() -> dict:
    """Snapshot the metadata DB (and optionally DuckDB) into settings.backup_dir.

    Never raises — the scheduled job must not kill the APScheduler thread.
    Returns a summary dict (also used as the admin endpoint response).
    """
    started = time.time()
    result: dict = {"sqlite": None, "duckdb": None, "pruned": 0, "duration_ms": 0}
    try:
        backup_dir = Path(settings.backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")

        # SQLite: VACUUM INTO is online-safe and compacts the copy. Use stdlib
        # sqlite3 directly — SQLAlchemy autobegins a txn and VACUUM can't run in one.
        dest = backup_dir / f"meta-{ts}.sqlite"
        con = sqlite3.connect(settings.metadata_db_path)
        try:
            con.execute("VACUUM INTO ?", (str(dest),))
        finally:
            con.close()
        result["sqlite"] = str(dest)
        pruned = _prune(backup_dir, "meta-", ".sqlite", int(settings.backup_retention))

        # DuckDB (opt-in): checkpoint the live store, then copy the file.
        if settings.backup_include_duckdb:
            try:
                from .db import get_active_duck_path, open_duck_native
                live = get_active_duck_path()
                with open_duck_native(live) as cur:
                    cur.execute("CHECKPOINT")
                # ponytail: copy after CHECKPOINT is best-effort — DuckDB store is
                # rebuildable from sync sources; upgrade to duckdb EXPORT DATABASE
                # if strict consistency ever matters.
                duck_dest = backup_dir / f"duck-{ts}.duckdb"
                shutil.copy2(live, duck_dest)
                result["duckdb"] = str(duck_dest)
                pruned += _prune(backup_dir, "duck-", ".duckdb", int(settings.backup_retention))
            except Exception as e:
                logger.warning("duckdb backup skipped: %s", e)

        result["pruned"] = pruned
        result["duration_ms"] = int((time.time() - started) * 1000)
        logger.info("backup ok: sqlite=%s duckdb=%s pruned=%s in %sms",
                    result["sqlite"], result["duckdb"], result["pruned"], result["duration_ms"])
    except Exception as e:
        result["error"] = str(e)
        result["duration_ms"] = int((time.time() - started) * 1000)
        logger.warning("backup FAILED: %s", e)
    return result


def list_backups() -> list[dict]:
    """[{name, sizeBytes, mtime}] for every backup file, newest first."""
    backup_dir = Path(settings.backup_dir)
    if not backup_dir.exists():
        return []
    out: list[dict] = []
    for p in backup_dir.iterdir():
        if not p.is_file():
            continue
        try:
            st = p.stat()
            out.append({
                "name": p.name,
                "sizeBytes": st.st_size,
                "mtime": datetime.utcfromtimestamp(st.st_mtime).isoformat() + "Z",
            })
        except Exception:
            continue
    out.sort(key=lambda x: x["mtime"], reverse=True)
    return out
