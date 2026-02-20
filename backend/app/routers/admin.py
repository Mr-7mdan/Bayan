from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from datetime import datetime
from sqlalchemy.orm import Session
from pathlib import Path
import json

from ..models import SessionLocal, User, Datasource
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


def _is_admin(db: Session, actor_id: str | None) -> bool:
    if not actor_id:
        return False
    u = db.query(User).filter(User.id == str(actor_id).strip()).first()
    return bool(u and (u.role or "user").lower() == "admin")


@router.get("/scheduler/jobs")
async def scheduler_jobs(actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    if not _is_admin(db, actorId):
        raise HTTPException(status_code=403, detail="Forbidden")
    return list_jobs()


@router.post("/scheduler/refresh")
async def scheduler_refresh(actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    if not _is_admin(db, actorId):
        raise HTTPException(status_code=403, detail="Forbidden")
    return schedule_all_jobs()


@router.get("/metrics-live")
async def metrics_live(actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    if not _is_admin(db, actorId):
        raise HTTPException(status_code=403, detail="Forbidden")
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
async def duckdb_active(actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    if not _is_admin(db, actorId):
        raise HTTPException(status_code=403, detail="Forbidden")
    return { "path": get_active_duck_path() }


@router.post("/duckdb/active")
async def duckdb_set_active(payload: _SetDuckActivePayload, actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    if not _is_admin(db, actorId):
        raise HTTPException(status_code=403, detail="Forbidden")
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
            return { "path": path_set }
        # No connection URI means this datasource IS the default local store.
        # Just confirm (and re-persist) the current active path — do NOT derive a
        # new name-based path which would switch to an empty file.
        current_path = get_active_duck_path()
        path_set = set_active_duck_path(current_path)
        return { "path": path_set }
    # Or accept a direct path string
    if payload.path and str(payload.path).strip():
        path_set = set_active_duck_path(str(payload.path).strip())
        return { "path": path_set }
    raise HTTPException(status_code=400, detail="datasourceId or path is required")


@router.put("/branding", response_model=BrandingOut)
async def update_branding(payload: BrandingUpdateIn, actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    if not _is_admin(db, actorId):
        raise HTTPException(status_code=403, detail="Forbidden")
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
        # Merge allowed fields
        for k in ("orgName", "logoLight", "logoDark", "favicon"):
            v = getattr(payload, k, None)
            if v is not None:
                current[k] = v
        f.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
        # Echo combined object back in BrandingOut shape (palette/fonts not changed here)
        return BrandingOut(fonts={"primary": "Inter", "code": "ui-monospace"}, palette={}, orgName=current.get("orgName"), logoLight=current.get("logoLight"), logoDark=current.get("logoDark"), favicon=current.get("favicon"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
