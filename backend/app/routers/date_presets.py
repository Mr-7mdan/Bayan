"""Date preset preview API endpoint."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

from ..date_presets import PresetConfig, materialize_holidays, resolve_preset

router = APIRouter(prefix="/date-presets", tags=["date-presets"])


# ── Pydantic schemas ─────────────────────────────────────────────────────

class PreviewRequest(BaseModel):
    period: str = "week"
    offset: str = "this"
    as_of: str = "today"
    range_mode: str = "full"
    include_weekends: bool = True
    apply_holidays: bool = False
    weekends: Optional[str] = None

    @field_validator("period")
    @classmethod
    def period_must_be_valid(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("period must not be empty")
        allowed = {"day", "week", "month", "quarter", "year"}
        if v not in allowed:
            raise ValueError(f"period must be one of {sorted(allowed)}, got '{v}'")
        return v


class PreviewResponse(BaseModel):
    gte: str
    lt: str
    label: str


# ── Helpers ───────────────────────────────────────────────────────────────

_MONTH_ABBR = [
    "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


def _format_date(d: datetime) -> str:
    """Format as 'Mar 8, 2026'."""
    return f"{_MONTH_ABBR[d.month]} {d.day}, {d.year}"


def _format_label(gte: datetime, lt: datetime) -> str:
    """Human-readable inclusive range label."""
    from datetime import timedelta
    inclusive_end = lt - timedelta(days=1)
    if gte == inclusive_end:
        return _format_date(gte)
    if gte.year == inclusive_end.year:
        if gte.month == inclusive_end.month:
            return f"{_MONTH_ABBR[gte.month]} {gte.day} \u2013 {inclusive_end.day}, {gte.year}"
        return f"{_MONTH_ABBR[gte.month]} {gte.day} \u2013 {_MONTH_ABBR[inclusive_end.month]} {inclusive_end.day}, {gte.year}"
    return f"{_format_date(gte)} \u2013 {_format_date(inclusive_end)}"


def _load_holidays() -> frozenset[str]:
    """Load materialized holidays for preview (reuses query.py pattern)."""
    try:
        from ..models import HolidayRule, SessionLocal
        db = SessionLocal()
        try:
            rules = db.query(HolidayRule).all()
            rule_dicts = [
                {
                    "rule_type": r.rule_type,
                    "specific_date": r.specific_date,
                    "recurrence_expr": r.recurrence_expr,
                }
                for r in rules
            ]
            year = datetime.now().year
            all_dates: set[str] = set()
            for y in (year - 1, year, year + 1):
                all_dates.update(materialize_holidays(rule_dicts, y))
            return frozenset(all_dates)
        finally:
            db.close()
    except Exception:
        return frozenset()


# ── Endpoint ──────────────────────────────────────────────────────────────

@router.post("/preview", response_model=PreviewResponse)
async def preview_preset(req: PreviewRequest):
    """Resolve a PresetConfig to concrete date bounds for UI preview."""
    config: PresetConfig = {
        "period": req.period,  # type: ignore[typeddict-item]
        "offset": req.offset,  # type: ignore[typeddict-item]
        "as_of": req.as_of,  # type: ignore[typeddict-item]
        "range_mode": req.range_mode,  # type: ignore[typeddict-item]
        "include_weekends": req.include_weekends,
        "apply_holidays": req.apply_holidays,
    }
    holidays = _load_holidays() if req.apply_holidays else None
    gte, lt = resolve_preset(config, weekends=req.weekends, holidays=holidays)
    return PreviewResponse(
        gte=gte.strftime("%Y-%m-%d"),
        lt=lt.strftime("%Y-%m-%d"),
        label=_format_label(gte, lt),
    )
