from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional, Literal

from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter(prefix="/periods", tags=["periods"]) 

DeltaMode = Literal[
    "TD_YSTD",
    "TW_LW",
    "MONTH_LMONTH",
    "MTD_LMTD",
    "TY_LY",
    "YTD_LYTD",
    "TQ_LQ",
    "Q_TY_VS_Q_LY",
    "QTD_TY_VS_QTD_LY",
    "M_TY_VS_M_LY",
    "MTD_TY_VS_MTD_LY",
]

WeekStart = Literal["sat", "sun", "mon"]


class ResolvePeriodsRequest(BaseModel):
    mode: DeltaMode
    now: Optional[str] = Field(default=None, description="ISO timestamp; if omitted, server now() UTC is used")
    tzOffsetMinutes: Optional[int] = Field(default=None, description="Client timezone offset minutes from UTC; if provided, we shift now by this offset for boundary calculations")
    weekStart: Optional[WeekStart] = Field(default="mon")


class ResolvePeriodsResponse(BaseModel):
    curStart: str
    curEnd: str
    prevStart: str
    prevEnd: str


# Helpers

def _floor_to_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _prev_month_start(dt: datetime) -> datetime:
    ms = _month_start(dt)
    prev_last_day = ms - timedelta(days=1)
    return _month_start(prev_last_day)


def _quarter_start(dt: datetime) -> datetime:
    q = (dt.month - 1) // 3
    first_month = q * 3 + 1
    return dt.replace(month=first_month, day=1, hour=0, minute=0, second=0, microsecond=0)


def _prev_quarter_start(dt: datetime) -> datetime:
    qs = _quarter_start(dt)
    prev_day = qs - timedelta(days=1)
    return _quarter_start(prev_day)


def _week_start(dt: datetime, start: WeekStart) -> datetime:
    # Python weekday(): Monday=0..Sunday=6
    wd = dt.weekday()  # 0..6
    start_idx = {"mon": 0, "sun": 6, "sat": 5}[start]
    # Convert desired start to Python weekday index delta
    # For sun as start, target idx = 6; for sat start, target idx = 5
    delta = (wd - start_idx) % 7
    return _floor_to_day(dt - timedelta(days=delta))


@router.post("/resolve", response_model=ResolvePeriodsResponse)
def resolve_periods(payload: ResolvePeriodsRequest) -> ResolvePeriodsResponse:
    # Use UTC as base
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    if payload.now:
        try:
            now = datetime.fromisoformat(payload.now)
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone.utc)
            else:
                now = now.astimezone(timezone.utc)
        except Exception:
            pass
    if payload.tzOffsetMinutes is not None:
        # Shift now by client offset to align boundaries to client local day/week/month
        now = now + timedelta(minutes=payload.tzOffsetMinutes)

    mode = payload.mode
    week_start = payload.weekStart or "mon"

    if mode == "TD_YSTD":
        cur_start = _floor_to_day(now)
        cur_end = now
        prev_start = cur_start - timedelta(days=1)
        prev_end = cur_start - timedelta(microseconds=1)
    elif mode == "TW_LW":
        ws = _week_start(now, week_start)  # start of this week
        cur_start = ws
        cur_end = now
        prev_start = ws - timedelta(days=7)
        prev_end = ws - timedelta(microseconds=1)
    elif mode == "MONTH_LMONTH":
        ms = _month_start(now)
        cur_start = ms
        cur_end = now
        prev_start = _prev_month_start(now)
        prev_end = ms - timedelta(microseconds=1)
    elif mode == "MTD_LMTD":
        ms = _month_start(now)
        cur_start = ms
        cur_end = now
        # Align last month to same day-of-month (cap at month end)
        prev_ms = _prev_month_start(now)
        # compute day index
        day = now.day
        # get last day of previous month
        next_of_prev_ms = _month_start(ms)
        last_day_prev_month = (next_of_prev_ms - timedelta(days=1)).day
        align_day = min(day, last_day_prev_month)
        prev_end = prev_ms.replace(day=align_day, hour=23, minute=59, second=59, microsecond=999999)
        prev_start = prev_ms
    elif mode == "TY_LY":
        cur_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        cur_end = now
        prev_start = cur_start.replace(year=cur_start.year - 1)
        prev_end = cur_start - timedelta(microseconds=1)
    elif mode == "YTD_LYTD":
        cur_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        cur_end = now
        prev_start = cur_start.replace(year=cur_start.year - 1)
        # Align last year to same day-of-year
        prev_end = prev_start + (cur_end - cur_start)
    elif mode == "TQ_LQ":
        qs = _quarter_start(now)
        cur_start = qs
        cur_end = now
        prev_start = _prev_quarter_start(now)
        prev_end = qs - timedelta(microseconds=1)
    elif mode == "Q_TY_VS_Q_LY":
        qs = _quarter_start(now)
        cur_start = qs
        cur_end = now
        prev_start = qs.replace(year=qs.year - 1)
        prev_end = qs - timedelta(microseconds=1)
    elif mode == "QTD_TY_VS_QTD_LY":
        qs = _quarter_start(now)
        cur_start = qs
        cur_end = now
        prev_start = qs.replace(year=qs.year - 1)
        # Align last year to same day-of-quarter
        prev_end = prev_start + (cur_end - cur_start)
    elif mode == "M_TY_VS_M_LY":
        ms = _month_start(now)
        cur_start = ms
        cur_end = now
        prev_start = ms.replace(year=ms.year - 1)
        prev_end = ms - timedelta(microseconds=1)
    elif mode == "MTD_TY_VS_MTD_LY":
        ms = _month_start(now)
        cur_start = ms
        cur_end = now
        prev_start = ms.replace(year=ms.year - 1)
        # Align last year to same day-of-month (cap at month end)
        day = now.day
        # get last day of previous month same time last year
        next_of_prev_ms = ms.replace(year=ms.year - 1).replace(month=ms.month + 1 if ms.month < 12 else 1, year=ms.year - 1 if ms.month < 12 else ms.year)
        last_day_prev_month = (next_of_prev_ms - timedelta(days=1)).day
        align_day = min(day, last_day_prev_month)
        prev_end = prev_start.replace(day=align_day, hour=now.hour, minute=now.minute, second=now.second, microsecond=now.microsecond)
    else:
        cur_start = _floor_to_day(now)
        cur_end = now
        prev_start = cur_start - timedelta(days=1)
        prev_end = cur_start - timedelta(microseconds=1)

    # Convert back to UTC ISO strings
    def iso(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).isoformat()

    return ResolvePeriodsResponse(
        curStart=iso(cur_start),
        curEnd=iso(cur_end),
        prevStart=iso(prev_start),
        prevEnd=iso(prev_end),
    )
