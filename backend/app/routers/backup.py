"""Admin backup endpoints (spec 24). Kept in a dedicated router that shares the
`/admin` prefix so the metadata-backup surface lives beside admin.py without
touching it. Paths: POST /api/admin/backup/run, GET /api/admin/backup/list."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from ..auth import require_admin
from ..audit import audit
from ..models import SessionLocal, User
from ..backup import run_backup, list_backups

router = APIRouter(prefix="/admin", tags=["admin"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/backup/run")
async def backup_run(request: Request, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    result = run_backup()
    audit("admin.backup.run", actor_id=(admin.id if admin else None), target_type="system",
          request=request, details={"pruned": result.get("pruned"), "durationMs": result.get("duration_ms")})
    return result


@router.get("/backup/list")
async def backup_list(db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    return list_backups()
