from __future__ import annotations

from typing import Optional
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from .config import settings

# Timezone used for all cron triggers and scheduler clock.
# Configured via SCHEDULER_TIMEZONE env var (see config.py). Defaults to UTC.
_SCHEDULER_TZ: str = (settings.scheduler_timezone or 'UTC').strip() or 'UTC'

from .models import SessionLocal, SyncTask, Datasource, AlertRule, AlertRun
from fastapi import Response
from .alerts_service import run_rule
from .routers import datasources as ds_router

_scheduler: Optional[BackgroundScheduler] = None


def ensure_scheduler_started() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler(timezone=_SCHEDULER_TZ)
        _scheduler.start()
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
    except Exception:
        # Best-effort; ignore errors during shutdown
        _scheduler = None


def _job_id(task_id: str) -> str:
    return f"sync:{task_id}"


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
                misfire_grace_time=300,
            )
            if jid in existing:
                updated += 1
            else:
                added += 1
        except Exception:
            # Ignore and continue
            continue

    return {"added": added, "updated": updated, "removed": removed, "total": len(desired)}


def run_task_job(ds_id: str, task_id: str) -> None:
    """Scheduler job wrapper to execute a single SyncTask."""
    db = SessionLocal()
    try:
        ds: Optional[Datasource] = db.get(Datasource, ds_id)
        actor = (ds.user_id if ds and ds.user_id else None)
        # Use the same business logic as the API endpoint
        ds_router.run_sync_now(ds_id, response=Response(), taskId=task_id, execute=True, actorId=actor, db=db)
    finally:
        try:
            db.close()
        except Exception:
            pass


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
    return {"added": added, "updated": updated, "removed": removed, "total": len(desired)}
