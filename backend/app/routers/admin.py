from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from datetime import datetime
from sqlalchemy.orm import Session
from pathlib import Path
import json

from ..auth import require_admin
from ..audit import audit
from ..models import SessionLocal, User, Datasource, AuditLog
from ..schemas import BrandingUpdateIn, BrandingOut
from ..config import settings
from ..scheduler import list_jobs, schedule_all_jobs
from ..metrics import snapshot as metrics_snapshot
from ..metrics_state import get_recent_actors, get_open_dashboards
from ..db import get_active_duck_path, set_active_duck_path
from ..security import decrypt_text
from pydantic import BaseModel
import re

router = APIRouter(prefix="/admin", tags=["admin"]) 


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/scheduler/jobs")
async def scheduler_jobs(db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    return list_jobs()


@router.post("/scheduler/refresh")
async def scheduler_refresh(request: Request, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    result = schedule_all_jobs()
    audit("admin.scheduler.refresh", actor_id=(admin.id if admin else None), target_type="system", request=request)
    return result


@router.get("/metrics-live")
async def metrics_live(db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    snap = metrics_snapshot()
    # Helpers to sum by metric name
    def sum_gauge(name: str, **label_eq):
        total = 0.0
        for g in (snap.get("gauges") or []):
            if g.get("name") != name:
                continue
            labels = g.get("labels") or {}
            ok = True
            for k, v in label_eq.items():
                if labels.get(k) != v:
                    ok = False; break
            if ok:
                try:
                    total += float(g.get("value") or 0)
                except Exception:
                    continue
        return total
    def sum_counter(name: str):
        total = 0.0
        for c in (snap.get("counters") or []):
            if c.get("name") != name:
                continue
            try:
                total += float(c.get("value") or 0)
            except Exception:
                continue
        return total
    def sum_summary(name: str):
        ssum = 0.0; cnt = 0
        for s in (snap.get("summaries") or []):
            if s.get("name") != name:
                continue
            try:
                ssum += float(s.get("sum") or 0)
                cnt += int(s.get("count") or 0)
            except Exception:
                continue
        return ssum, cnt

    inflight_duck = sum_gauge("query_inflight", endpoint="query", engine="duckdb")
    inflight_sqlal = sum_gauge("query_inflight", endpoint="query", engine="sqlalchemy")
    inflight_distinct = sum_gauge("query_inflight", endpoint="distinct")
    inflight_pt = sum_gauge("query_inflight", endpoint="period_totals")
    inflight_ptb = sum_gauge("query_inflight", endpoint="period_totals_batch")
    inflight_ptc = sum_gauge("query_inflight", endpoint="period_totals_compare")
    inflight_total = inflight_duck + inflight_sqlal + inflight_distinct + inflight_pt + inflight_ptb + inflight_ptc

    cache_hits = sum_counter("query_cache_hit_total")
    cache_miss = sum_counter("query_cache_miss_total")
    rate_limited = sum_counter("query_rate_limited_total")
    dur_sum, dur_count = sum_summary("query_duration_ms")
    dur_avg = (dur_sum / dur_count) if dur_count > 0 else None

    jobs = list_jobs()
    rec_actors = [{"id": aid, "lastAt": datetime.utcfromtimestamp(ts).isoformat()+'Z'} for aid, ts in get_recent_actors(900)]
    open_dash = get_open_dashboards(900)
    # Notifications (email/sms) totals
    email_sent = sum_counter("notifications_email_sent_total")
    email_failed = sum_counter("notifications_email_failed_total")
    sms_sent = sum_counter("notifications_sms_sent_total")
    sms_failed = sum_counter("notifications_sms_failed_total")

    return {
        "now": datetime.utcnow().isoformat()+"Z",
        "query": {
            "inflight": {
                "duckdb": inflight_duck,
                "sqlalchemy": inflight_sqlal,
                "distinct": inflight_distinct,
                "period_totals": inflight_pt,
                "period_totals_batch": inflight_ptb,
                "period_totals_compare": inflight_ptc,
                "total": inflight_total,
            },
            "cache": { "hits": cache_hits, "misses": cache_miss, "hitRatio": (cache_hits/(cache_hits+cache_miss)) if (cache_hits+cache_miss)>0 else None },
            "rateLimited": rate_limited,
            "durationsMs": { "sum": dur_sum, "count": dur_count, "avg": dur_avg },
        },
        "scheduler": { "jobs": jobs },
        "actors": { "recent": rec_actors },
        "embeddings": { "jobs": [] },
        "alerts": { "scheduled": [j for j in jobs if isinstance(j.get("id"), str) and j["id"].startswith("alert:")] },
        "dashboards": { "open": open_dash },
        "notifications": {
            "email": { "sent": email_sent, "failed": email_failed },
            "sms": { "sent": sms_sent, "failed": sms_failed },
        },
        "raw": snap,
    }


class _SetDuckActivePayload(BaseModel):
    datasourceId: str | None = None
    path: str | None = None


@router.get("/duckdb/active")
async def duckdb_active(db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    return { "path": get_active_duck_path() }


@router.post("/duckdb/active")
async def duckdb_set_active(payload: _SetDuckActivePayload, request: Request, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    # Prefer datasourceId if provided
    if payload.datasourceId:
        ds = db.get(Datasource, payload.datasourceId)
        if not ds:
            raise HTTPException(status_code=404, detail="Datasource not found")
        t = (ds.type or '').lower()
        if 'duckdb' not in t:
            raise HTTPException(status_code=400, detail="Datasource is not of type duckdb")
        # Resolve DSN from encrypted secret and set path
        dsn = decrypt_text(ds.connection_encrypted or "") if ds.connection_encrypted else None
        if dsn:
            path_set = set_active_duck_path(dsn)
            audit("admin.duckdb.set_active", actor_id=(admin.id if admin else None), target_type="system",
                  target_id=payload.datasourceId, request=request, details={"path": path_set})
            return { "path": path_set }
        # No connection URI means this datasource IS the default local store.
        # Just confirm (and re-persist) the current active path — do NOT derive a
        # new name-based path which would switch to an empty file.
        current_path = get_active_duck_path()
        path_set = set_active_duck_path(current_path)
        audit("admin.duckdb.set_active", actor_id=(admin.id if admin else None), target_type="system",
              target_id=payload.datasourceId, request=request, details={"path": path_set})
        return { "path": path_set }
    # Or accept a direct path string
    if payload.path and str(payload.path).strip():
        path_set = set_active_duck_path(str(payload.path).strip())
        audit("admin.duckdb.set_active", actor_id=(admin.id if admin else None), target_type="system",
              request=request, details={"path": path_set})
        return { "path": path_set }
    raise HTTPException(status_code=400, detail="datasourceId or path is required")


@router.put("/branding", response_model=BrandingOut)
async def update_branding(payload: BrandingUpdateIn, request: Request, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    try:
        data_dir = Path(settings.metadata_db_path).resolve().parent
        data_dir.mkdir(parents=True, exist_ok=True)
        f = data_dir / "branding.json"
        current = {}
        try:
            if f.exists():
                current = json.loads(f.read_text(encoding="utf-8")) or {}
        except Exception:
            current = {}
        # Merge allowed fields. An explicit empty string is interpreted as
        # "clear this override" so the UI can reset back to the Bayan default.
        for k in ("orgName", "logoLight", "logoDark", "favicon"):
            v = getattr(payload, k, None)
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                current.pop(k, None)
            else:
                current[k] = v.strip() if isinstance(v, str) else v
        f.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
        # Echo effective branding (with Bayan defaults filled in) so the UI
        # immediately shows what users will see.
        from ..main import _coalesce_branding  # late import to avoid cycle at module load
        eff = _coalesce_branding(current)
        audit("admin.branding.update", actor_id=(admin.id if admin else None), target_type="system", request=request)
        return BrandingOut(
            fonts={"primary": "Inter", "code": "ui-monospace"},
            palette={},
            orgName=eff["orgName"],
            logoLight=eff["logoLight"],
            logoDark=eff["logoDark"],
            favicon=eff["favicon"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/branding/reset", response_model=BrandingOut)
async def reset_branding(request: Request, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    """Clear all branding overrides — restores the Bayan default look."""
    try:
        data_dir = Path(settings.metadata_db_path).resolve().parent
        data_dir.mkdir(parents=True, exist_ok=True)
        f = data_dir / "branding.json"
        # Remove every override key but keep any non-branding extras intact.
        current = {}
        try:
            if f.exists():
                current = json.loads(f.read_text(encoding="utf-8")) or {}
        except Exception:
            current = {}
        for k in ("orgName", "logoLight", "logoDark", "favicon"):
            current.pop(k, None)
        f.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
        from ..main import _coalesce_branding
        eff = _coalesce_branding(current)
        audit("admin.branding.reset", actor_id=(admin.id if admin else None), target_type="system", request=request)
        return BrandingOut(
            fonts={"primary": "Inter", "code": "ui-monospace"},
            palette={},
            orgName=eff["orgName"],
            logoLight=eff["logoLight"],
            logoDark=eff["logoDark"],
            favicon=eff["favicon"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Audit log read API (spec 05): append-only, admin-only, no update/delete ---
@router.get("/audit-logs")
async def audit_logs(
    actorId: str | None = Query(default=None),
    action: str | None = Query(default=None),        # prefix match, e.g. "auth."
    actor: str | None = Query(default=None),          # filter by actor_id or actor_email
    targetId: str | None = Query(default=None),
    since: str | None = Query(default=None),          # ISO datetime
    until: str | None = Query(default=None),
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0),
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin),
):
    q = db.query(AuditLog)
    if action:
        q = q.filter(AuditLog.action.like(action + "%"))
    if actor:
        q = q.filter((AuditLog.actor_id == actor) | (AuditLog.actor_email == actor))
    if targetId:
        q = q.filter(AuditLog.target_id == targetId)
    if since:
        q = q.filter(AuditLog.ts >= datetime.fromisoformat(since))
    if until:
        q = q.filter(AuditLog.ts <= datetime.fromisoformat(until))
    total = q.count()
    rows = q.order_by(AuditLog.ts.desc()).offset(offset).limit(limit).all()
    return {"total": total, "items": [
        {"id": r.id, "ts": (r.ts.isoformat() + "Z" if r.ts else None), "actorId": r.actor_id,
         "actorEmail": r.actor_email, "action": r.action, "targetType": r.target_type,
         "targetId": r.target_id, "ip": r.ip, "userAgent": r.user_agent,
         "status": r.status, "details": json.loads(r.details_json or "null")}
        for r in rows]}
