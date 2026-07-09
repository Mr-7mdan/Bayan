from __future__ import annotations

import logging
from typing import Optional
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from .config import settings
from .timeutil import as_utc

logger = logging.getLogger(__name__)

# Timezone used for all cron triggers and scheduler clock.
# Configured via SCHEDULER_TIMEZONE env var (see config.py). Defaults to UTC.
_SCHEDULER_TZ: str = (settings.scheduler_timezone or 'UTC').strip() or 'UTC'

from .models import SessionLocal, SyncTask, Datasource, AlertRule, AlertRun
from fastapi import Response
from .alerts_service import run_rule
from .routers import datasources as ds_router

_scheduler: Optional[BackgroundScheduler] = None


def scheduler_is_running() -> bool:
    """Side-effect-free accessor for health checks (does not start/restart)."""
    return bool(_scheduler is not None and _scheduler.running)

# Dedicated bounded pool so a hung sync never wedges an APScheduler worker thread.
# ponytail: bounded shared pool; a truly hung driver call leaks one thread until restart —
# acceptable, capped at 4. Per-datasource pools if that ever bites.
_SYNC_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="sync-job")


def ensure_scheduler_started() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler(timezone=_SCHEDULER_TZ)
        _scheduler.start()
    elif not _scheduler.running:
        # Scheduler daemon thread died — restart it and reload all jobs
        logger.debug("[SCHEDULER] WARNING: Scheduler was not running — restarting!")
        try:
            _scheduler.shutdown(wait=False)
        except Exception:
            pass
        _scheduler = BackgroundScheduler(timezone=_SCHEDULER_TZ)
        _scheduler.start()
        # Reload jobs into the fresh scheduler
        try:
            schedule_all_jobs()
            schedule_all_alert_jobs()
            logger.debug("[SCHEDULER] Reloaded all jobs after restart")
        except Exception as e:
            logger.warning(f"[SCHEDULER] Failed to reload jobs: {e}")
    return _scheduler


def shutdown_scheduler(wait: bool = True) -> None:
    """Shut down the singleton scheduler if running.
    This prevents background threads from lingering during app shutdown,
    which can trigger GIL-related crashes on Windows when the interpreter exits.
    """
    global _scheduler
    try:
        if _scheduler is not None:
            try:
                _scheduler.shutdown(wait=wait)
            finally:
                _scheduler = None
        try:
            _SYNC_POOL.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
    except Exception:
        # Best-effort; ignore errors during shutdown
        _scheduler = None


def _job_id(task_id: str) -> str:
    return f"sync:{task_id}"


def _purge_audit_logs() -> None:
    """Daily retention purge for the append-only audit_log table (spec 05).
    Deletes rows older than AUDIT_RETENTION_DAYS. Not itself audited (noise)."""
    import logging
    from .models import SessionLocal, AuditLog
    from datetime import datetime, timedelta
    db = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(days=int(settings.audit_retention_days))
        deleted = db.query(AuditLog).filter(AuditLog.ts < cutoff).delete(synchronize_session=False)
        db.commit()
        logging.getLogger("bayan.audit").info("audit purge removed %s rows older than %s", deleted, cutoff.isoformat())
    except Exception as e:
        logging.getLogger("bayan.audit").warning("audit purge failed: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()


def schedule_backup_job() -> None:
    """Register the nightly metadata-backup job (spec 24), same static pattern
    as audit:purge. Inherits RUN_SCHEDULER gating from main.py startup."""
    from .backup import run_backup
    sched = ensure_scheduler_started()
    try:
        trig = CronTrigger.from_crontab(settings.backup_cron, timezone=_SCHEDULER_TZ)
        sched.add_job(
            func=run_backup,
            trigger=trig,
            id="backup:meta",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
    except Exception as _be:
        logger.warning(f"[SCHEDULER] Failed to register backup:meta: {_be}")


def schedule_all_jobs() -> dict:
    """Sync scheduler state with DB SyncTasks that have schedule_cron and enabled.
    Returns a summary dict with counts.
    """
    sched = ensure_scheduler_started()
    added = 0
    removed = 0
    updated = 0

    # Desired jobs from DB
    db = SessionLocal()
    try:
        tasks = db.query(SyncTask).filter(SyncTask.schedule_cron.is_not(None), SyncTask.enabled == True).all()
        desired = { _job_id(t.id): t for t in tasks }
    finally:
        db.close()

    # Remove obsolete jobs
    existing = { j.id: j for j in sched.get_jobs() }
    for jid in list(existing.keys()):
        if not jid.startswith("sync:"):
            continue
        if jid not in desired:
            try:
                sched.remove_job(jid)
                removed += 1
            except Exception:
                pass

    # Upsert desired jobs
    for jid, t in desired.items():
        try:
            trig = CronTrigger.from_crontab(t.schedule_cron, timezone=_SCHEDULER_TZ)
        except Exception:
            # Skip malformed cron
            continue
        # Add or replace
        try:
            sched.add_job(
                func=run_task_job,
                trigger=trig,
                id=jid,
                replace_existing=True,
                kwargs={"ds_id": t.datasource_id, "task_id": t.id},
                max_instances=1,
                coalesce=True,
                misfire_grace_time=3600,  # 1 hour — sync jobs run hourly, 5min was too tight
            )
            if jid in existing:
                updated += 1
            else:
                added += 1
        except Exception:
            # Ignore and continue
            continue

    # Static daily audit-log retention purge (spec 05)
    try:
        sched.add_job(
            func=_purge_audit_logs,
            trigger=CronTrigger.from_crontab("30 3 * * *", timezone=_SCHEDULER_TZ),
            id="audit:purge",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=3600,
        )
    except Exception as _ae:
        logger.warning(f"[SCHEDULER] Failed to register audit:purge: {_ae}")

    # --- Catch-up: fire any sync job missed since the last run ---
    # Handles server restarts where APScheduler recalculates next_run_time into the
    # future, skipping today's already-due slot.
    try:
        try:
            from zoneinfo import ZoneInfo as _ZI
        except ImportError:
            from backports.zoneinfo import ZoneInfo as _ZI  # type: ignore
        _tz = _ZI(_SCHEDULER_TZ)
        _now_local = datetime.now(_tz)
        _window_start = _now_local - timedelta(hours=24)
        _catchup_db = SessionLocal()
        try:
            for jid, t in desired.items():
                try:
                    _trig = CronTrigger.from_crontab(t.schedule_cron, timezone=_SCHEDULER_TZ)
                    _candidate = _trig.get_next_fire_time(None, _window_start)
                    if _candidate is None or _candidate >= _now_local:
                        continue
                    # Check last successful run from SyncState
                    from .models import SyncState
                    _st = _catchup_db.query(SyncState).filter(SyncState.task_id == t.id).first()
                    if _st and _st.last_run_at is not None:
                        _last = as_utc(_st.last_run_at)
                        if _last.astimezone(_tz) >= _candidate:
                            continue  # Already ran for this slot
                    # Fire catch-up
                    logger.debug(f"[SYNC_CATCHUP] Firing catch-up for task={t.id} (missed {_candidate})")
                    sched.add_job(
                        func=run_task_job,
                        trigger='date',
                        run_date=datetime.now(_tz) + timedelta(seconds=10),
                        id=f"sync_catchup:{t.id}",
                        replace_existing=True,
                        kwargs={"ds_id": t.datasource_id, "task_id": t.id},
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=600,
                    )
                except Exception as _ce:
                    logger.warning(f"[SYNC_CATCHUP] Error for task {t.id}: {_ce}")
        finally:
            try:
                _catchup_db.close()
            except Exception:
                pass
    except Exception as _catchup_err:
        logger.warning(f"[SYNC_CATCHUP] Catch-up failed: {_catchup_err}")

    return {"added": added, "updated": updated, "removed": removed, "total": len(desired)}


def _auto_reset_stuck(db, ds_id: str, stale_threshold_minutes: int = 30) -> int:
    """Auto-reset sync states that are in_progress but have stopped making progress.

    A sync is considered stuck when progress_updated_at (or started_at as fallback)
    is older than stale_threshold_minutes. This allows long-running syncs that are
    actively fetching rows to continue, while resetting ones that crashed mid-sync.
    Returns the number of states reset."""
    from .models import SyncState, SyncTask
    try:
        in_progress = (
            db.query(SyncState)
            .join(SyncTask, SyncTask.id == SyncState.task_id)
            .filter(SyncTask.datasource_id == ds_id, SyncState.in_progress == True)
            .all()
        )
        if not in_progress:
            return 0
        reset_count = 0
        now = datetime.now(timezone.utc)
        for st in in_progress:
            # Use the most recent activity timestamp: progress_updated_at (set on each batch),
            # falling back to started_at, then last_run_at
            last_activity = (
                getattr(st, 'progress_updated_at', None)
                or getattr(st, 'started_at', None)
                or getattr(st, 'last_run_at', None)
            )
            if last_activity is None:
                is_stuck = True
                reason = "no activity timestamp"
            else:
                elapsed = now - as_utc(last_activity)
                is_stuck = elapsed > timedelta(minutes=stale_threshold_minutes)
                reason = f"no progress for {elapsed.total_seconds()/60:.0f}min (threshold={stale_threshold_minutes}min)"
            if is_stuck:
                logger.debug(f"[AUTO_RESET_STUCK] Resetting stuck sync: task_id={st.task_id}, progress={st.progress_current}/{st.progress_total}, {reason}")
                st.in_progress = False
                st.cancel_requested = False
                st.progress_current = None
                st.progress_total = None
                st.progress_phase = None
                st.progress_updated_at = None
                db.add(st)
                reset_count += 1
            else:
                elapsed_str = f"{(now - as_utc(last_activity)).total_seconds()/60:.0f}min" if last_activity else "?"
                logger.debug(f"[AUTO_RESET_STUCK] Sync still active: task_id={st.task_id}, progress={st.progress_current}/{st.progress_total}, last_activity={elapsed_str} ago")
        if reset_count:
            db.commit()
            logger.debug(f"[AUTO_RESET_STUCK] Reset {reset_count} stuck syncs for datasource {ds_id}")
        return reset_count
    except Exception as e:
        logger.warning(f"[AUTO_RESET_STUCK] Error: {e}")
        try:
            db.rollback()
        except Exception:
            pass
        return 0


def _execute_sync(ds_id: str, task_id: str) -> None:
    """Run the actual sync. Executes inside the dedicated _SYNC_POOL thread, NOT the
    APScheduler worker, so a hung DB driver call can never wedge the scheduler."""
    db = SessionLocal()
    try:
        ds: Optional[Datasource] = db.get(Datasource, ds_id)
        actor = (ds.user_id if ds and ds.user_id else None)
        ds_router.run_sync_now(ds_id, response=Response(), taskId=task_id, execute=True, actorId=actor, db=db)
    finally:
        try:
            db.close()
        except Exception:
            pass


def _request_cancel(task_id: str) -> None:
    """Cooperatively abort a runaway sync by flipping cancel_requested; the sync
    engines honor it at their next batch boundary (_check_abort, datasources.py)."""
    db = SessionLocal()
    try:
        from .models import SyncState
        st = db.query(SyncState).filter(SyncState.task_id == task_id).first()
        if st and st.in_progress:
            st.cancel_requested = True
            db.add(st); db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        try:
            db.close()
        except Exception:
            pass


def run_task_job(ds_id: str, task_id: str) -> None:
    """Scheduler job wrapper to execute a single SyncTask.

    IMPORTANT: This runs inside APScheduler's daemon thread. Unhandled exceptions
    here can kill the scheduler, stopping ALL future jobs permanently.

    The sync itself runs in a bounded dedicated pool with a wall-clock timeout. On
    timeout we request a cooperative abort and return — the APScheduler slot is
    released (still max_instances=1, so no overlap) instead of blocking forever.
    """
    db = SessionLocal()
    try:
        # Auto-reset any syncs with no progress for >30 minutes (backstop for
        # workers that never reach a batch boundary to honor cancel_requested).
        _auto_reset_stuck(db, ds_id, stale_threshold_minutes=30)
    except Exception as e:
        logger.warning(f"[SYNC_JOB] auto-reset failed task={task_id} ds={ds_id}: {e}")
    finally:
        try:
            db.close()
        except Exception:
            pass

    # config.py setting (deferred): sync_job_timeout_seconds: int = 3600. Read
    # defensively until it lands so this module imports/runs without it.
    timeout = max(60, int(getattr(settings, "sync_job_timeout_seconds", 3600)))
    fut = _SYNC_POOL.submit(_execute_sync, ds_id, task_id)
    try:
        fut.result(timeout=timeout)
    except FutureTimeout:
        logger.warning(f"[SYNC_JOB] TIMEOUT task={task_id} ds={ds_id} after {timeout}s — requesting abort")
        _request_cancel(task_id)
    except Exception as e:
        # Catch ALL exceptions to prevent killing the scheduler daemon thread.
        # APScheduler silently dies if the job function raises.
        logger.warning(f"[SYNC_JOB] FAILED task={task_id} ds={ds_id}: {e}")


def list_jobs() -> list[dict]:
    sched = ensure_scheduler_started()
    out: list[dict] = []
    for j in sched.get_jobs():
        info = {"id": j.id, "nextRunAt": j.next_run_time.isoformat() if isinstance(j.next_run_time, datetime) else None}
        if j.id.startswith("sync:"):
            info.update({
                "dsId": (j.kwargs or {}).get("ds_id"),
                "taskId": (j.kwargs or {}).get("task_id"),
            })
            out.append(info)
        elif j.id.startswith("alert:"):
            info.update({"alertId": (j.kwargs or {}).get("alert_id")})
            out.append(info)
        elif j.id.startswith("audit:") or j.id.startswith("backup:"):
            out.append(info)
    # Sort by next run
    out.sort(key=lambda x: x.get("nextRunAt") or "")
    return out


# --- Alerts scheduling ---
def _alert_job_id(alert_id: str) -> str:
    return f"alert:{alert_id}"


def run_alert_rule_job(alert_id: str) -> None:
    db = SessionLocal()
    try:
        a = db.get(AlertRule, alert_id)
        if not a or (not a.enabled):
            return
        # Log start
        ar = AlertRun(id=__import__('uuid').uuid4().hex, alert_id=a.id)
        db.add(ar); db.commit()
        try:
            ok, msg = run_rule(db, a, skip_time_window=True)
            a.last_run_at = datetime.now(timezone.utc)
            a.last_status = msg or ("ok" if ok else "failed")
            ar.finished_at = datetime.now(timezone.utc)
            ar.status = ("ok" if ok else "failed")
            ar.message = msg or ("ok" if ok else "failed")
            db.add(a); db.add(ar); db.commit()
        except Exception as e:
            ar.finished_at = datetime.now(timezone.utc); ar.status = "failed"; ar.message = str(e)
            db.add(ar); db.commit()
    finally:
        try:
            db.close()
        except Exception:
            pass


def schedule_all_alert_jobs(default_cron: str = "*/15 * * * *") -> dict:
    """Sync scheduler with enabled alert rules.
    - Time triggers with explicit cron: schedule as-is
    - Threshold triggers: schedule by their cron if present; otherwise use default cadence
    """
    sched = ensure_scheduler_started()
    added = updated = removed = 0
    db = SessionLocal()
    try:
        rules = db.query(AlertRule).filter(AlertRule.enabled == True).all()
        desired: dict[str, str] = {}
        for a in rules:
            try:
                cfg = __import__('json').loads(a.config_json or '{}')
            except Exception:
                cfg = {}
            triggers = cfg.get('triggers') or []
            # If any trigger has cron, prefer that; else default cadence
            cron_list: list[str] = []
            for t in triggers:
                cr = str((t or {}).get('cron') or '').strip()
                if cr:
                    cron_list.append(cr)
            if not cron_list:
                cron_list = [default_cron]
            for cr in cron_list:
                jid = _alert_job_id(a.id)
                desired[jid] = cr
        # Remove obsolete
        existing = { j.id: j for j in sched.get_jobs() if j.id.startswith('alert:') }
        for jid in list(existing.keys()):
            if jid not in desired:
                try:
                    sched.remove_job(jid)
                    removed += 1
                except Exception:
                    pass
        # Upsert
        for jid, cr in desired.items():
            try:
                trig = CronTrigger.from_crontab(cr, timezone=_SCHEDULER_TZ)
            except Exception:
                # Skip bad cron
                continue
            try:
                sched.add_job(
                    func=run_alert_rule_job,
                    trigger=trig,
                    id=jid,
                    replace_existing=True,
                    kwargs={"alert_id": jid.split(':',1)[1]},
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300,
                )
                if jid in existing:
                    updated += 1
                else:
                    added += 1
            except Exception:
                continue
    finally:
        try:
            db.close()
        except Exception:
            pass
    # --- Catch-up: fire any job that was missed since the last run ---
    # Handles server restarts and post-scheduled-time edits where APScheduler
    # recalculates next_run_time into the future, skipping today's already-due slot.
    try:
        try:
            from zoneinfo import ZoneInfo as _ZI
        except ImportError:
            from backports.zoneinfo import ZoneInfo as _ZI  # type: ignore
        _tz = _ZI(_SCHEDULER_TZ)
        _now_local = datetime.now(_tz)
        _window_start = _now_local - timedelta(hours=24)
        for a in rules:
            try:
                _cfg = __import__('json').loads(a.config_json or '{}')
                _trigs = _cfg.get('triggers') or []
                _crons = [
                    str((t or {}).get('cron') or '').strip()
                    for t in _trigs
                    if str((t or {}).get('cron') or '').strip()
                ]
                if not _crons or _crons[0] == default_cron:
                    continue
                _trig = CronTrigger.from_crontab(_crons[0], timezone=_SCHEDULER_TZ)
                _candidate = _trig.get_next_fire_time(None, _window_start)
                if _candidate is None or _candidate >= _now_local:
                    continue
                _last = as_utc(a.last_run_at)
                if _last is not None:
                    if _last.astimezone(_tz) >= _candidate:
                        continue
                sched.add_job(
                    func=run_alert_rule_job,
                    trigger='date',
                    run_date=datetime.now(_tz) + timedelta(seconds=10),
                    id=f"alert_catchup:{a.id}",
                    replace_existing=True,
                    kwargs={"alert_id": a.id},
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=3600,
                )
            except Exception:
                continue
    except Exception:
        pass
    return {"added": added, "updated": updated, "removed": removed, "total": len(desired)}
