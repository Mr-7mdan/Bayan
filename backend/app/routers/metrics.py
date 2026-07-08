from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from typing import Optional

from ..metrics_state import open_dashboard as _open_dash, close_dashboard as _close_dash
from ..authz import require_user

router = APIRouter(prefix="/metrics", tags=["metrics"])

class DashEvent(BaseModel):
    kind: str = Field(pattern=r"^(builder|public)$")
    dashboardId: str
    sessionId: str

@router.post("/dashboards/open")
async def dashboards_open(payload: DashEvent, _user = Depends(require_user)) -> dict:
    try:
        _open_dash(payload.kind, payload.dashboardId, payload.sessionId)
    except Exception:
        pass
    return { "ok": True }

@router.post("/dashboards/close")
async def dashboards_close(payload: DashEvent, _user = Depends(require_user)) -> dict:
    try:
        _close_dash(payload.kind, payload.dashboardId, payload.sessionId)
    except Exception:
        pass
    return { "ok": True }
