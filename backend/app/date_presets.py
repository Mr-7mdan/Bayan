"""
Composable date preset resolver.

Replaces the hardcoded 34+ preset strings with a parametric system.
Supports both the new structured format and legacy string presets
via LEGACY_PRESET_MAP for backward compatibility.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Any, Literal, Optional, Sequence, TypedDict

# ── Type definitions ─────────────────────────────────────────────────────

Period = Literal["day", "week", "month", "quarter", "year"]
Offset = Literal["this", "previous", "last_year_this", "last_year_previous"]
AsOf = Literal["today", "last_working_day"]
RangeMode = Literal["full", "to_date", "end_of_period"]


def _shift_years(d: datetime, years: int) -> datetime:
    """Subtract *years* from *d*, handling Feb 29 by falling back to Feb 28."""
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        # Feb 29 in a leap source year → Feb 28 in the target year
        return d.replace(year=d.year - years, day=28)


class PresetConfig(TypedDict, total=False):
    period: Period
    offset: Offset
    as_of: AsOf
    range_mode: RangeMode
    include_weekends: bool
    apply_holidays: bool


# ── Weekend / week-start helpers ─────────────────────────────────────────

WEEKENDS_MAP: dict[str, tuple[int, int]] = {
    "SAT_SUN": (5, 6),   # Python weekday: Mon=0 … Sun=6
    "FRI_SAT": (4, 5),
}


def _get_weekend_days(weekends_raw: str | None = None) -> tuple[int, int]:
    """Return weekend day numbers from config or env."""
    raw = (weekends_raw or os.environ.get("WEEKENDS", "SAT_SUN")).upper().strip()
    return WEEKENDS_MAP.get(raw, (5, 6))


def _infer_week_start(weekend_days: tuple[int, int]) -> int:
    """Infer week start day from weekends: day after last weekend day.

    Returns Python weekday int (Mon=0 … Sun=6).
    """
    last_weekend = max(weekend_days)
    return (last_weekend + 1) % 7


# ── Date navigation helpers ──────────────────────────────────────────────

def _prev_workday(
    d: datetime,
    weekend_days: tuple[int, int],
    holidays: frozenset[str] | None = None,
) -> datetime:
    """Last working day strictly before *d* (skips weekends + holidays)."""
    candidate = d - timedelta(days=1)
    while (
        candidate.weekday() in weekend_days
        or (holidays and candidate.strftime("%Y-%m-%d") in holidays)
    ):
        candidate -= timedelta(days=1)
    return candidate


def _prev_workday_inclusive(
    d: datetime,
    weekend_days: tuple[int, int],
    holidays: frozenset[str] | None = None,
) -> datetime:
    """Most recent working day at or before *d*."""
    return _prev_workday(d + timedelta(days=1), weekend_days, holidays)


def _next_workday_inclusive(
    d: datetime,
    weekend_days: tuple[int, int],
    holidays: frozenset[str] | None = None,
) -> datetime:
    """Earliest working day at or after *d* (skips weekends + holidays)."""
    candidate = d
    while (
        candidate.weekday() in weekend_days
        or (holidays and candidate.strftime("%Y-%m-%d") in holidays)
    ):
        candidate += timedelta(days=1)
    return candidate


def _week_start(d: datetime, week_start_dow: int) -> datetime:
    """Most recent week-start day at or before *d*."""
    offset = (d.weekday() - week_start_dow + 7) % 7
    return d - timedelta(days=offset)


def _month_start(d: datetime) -> datetime:
    return datetime(d.year, d.month, 1)


def _next_month_start(d: datetime) -> datetime:
    if d.month == 12:
        return datetime(d.year + 1, 1, 1)
    return datetime(d.year, d.month + 1, 1)


def _prev_month_start(d: datetime) -> datetime:
    if d.month == 1:
        return datetime(d.year - 1, 12, 1)
    return datetime(d.year, d.month - 1, 1)


def _quarter_start(d: datetime) -> datetime:
    q = (d.month - 1) // 3
    return datetime(d.year, q * 3 + 1, 1)


def _next_quarter_start(d: datetime) -> datetime:
    q = (d.month - 1) // 3 + 1
    if q > 3:
        return datetime(d.year + 1, 1, 1)
    return datetime(d.year, q * 3 + 1, 1)


def _prev_quarter_start(d: datetime) -> datetime:
    q = (d.month - 1) // 3 - 1
    if q < 0:
        return datetime(d.year - 1, 10, 1)
    return datetime(d.year, q * 3 + 1, 1)


def _year_start(d: datetime) -> datetime:
    return datetime(d.year, 1, 1)


def _next_year_start(d: datetime) -> datetime:
    return datetime(d.year + 1, 1, 1)


def _prev_year_start(d: datetime) -> datetime:
    return datetime(d.year - 1, 1, 1)


def _last_day_of_period(
    period_start: datetime,
    period_end: datetime,
    include_weekends: bool,
    weekend_days: tuple[int, int],
    holidays: frozenset[str] | None = None,
) -> datetime:
    """Last day of a period. If include_weekends is False, returns last working day."""
    if include_weekends:
        return period_end - timedelta(days=1)
    return _prev_workday(period_end, weekend_days, holidays)


def _to_date_anchor_in_previous_period(
    anchor: datetime,
    period: Period,
    period_start: datetime,
    prev_period_start: datetime,
) -> datetime:
    """Mirror the anchor's position within the current period into the previous period.

    For example, if anchor is March 17 and period is month:
    - Current period start = March 1
    - Position = 17 days into the period
    - Previous period start = February 1
    - Result = February 17 (clamped to period end if needed)
    """
    if period == "day":
        return prev_period_start
    elif period == "week":
        days_into = (anchor - period_start).days
        return prev_period_start + timedelta(days=days_into)
    elif period == "month":
        try:
            return prev_period_start.replace(day=anchor.day)
        except ValueError:
            # e.g., March 31 → February doesn't have 31 days
            next_of_prev = _next_month_start(prev_period_start)
            return next_of_prev - timedelta(days=1)
    elif period == "quarter":
        days_into = (anchor - period_start).days
        return prev_period_start + timedelta(days=days_into)
    elif period == "year":
        try:
            return prev_period_start.replace(month=anchor.month, day=anchor.day)
        except ValueError:
            # Feb 29 in non-leap year
            return prev_period_start.replace(month=anchor.month, day=28)
    return anchor


# ── Core parametric resolver ─────────────────────────────────────────────

def resolve_preset(
    config: PresetConfig,
    *,
    now: datetime | None = None,
    weekends: str | None = None,
    holidays: frozenset[str] | None = None,
) -> tuple[datetime, datetime]:
    """Resolve a composable preset config to (gte, lt) date bounds.

    Parameters
    ----------
    config : PresetConfig
        The composable preset dimensions.
    now : datetime, optional
        Override "now" for testing. Defaults to datetime.now().
    weekends : str, optional
        Weekend config string ("SAT_SUN" or "FRI_SAT"). Defaults to env.
    holidays : frozenset of "YYYY-MM-DD" strings, optional
        Materialized holiday dates for the relevant year(s).

    Returns
    -------
    (gte, lt) : tuple of datetime
        Inclusive start, exclusive end.
    """
    if now is None:
        now = datetime.now()
    today = datetime(now.year, now.month, now.day)

    period: Period = config.get("period", "week")
    offset: Offset = config.get("offset", "this")
    as_of: AsOf = config.get("as_of", "today")
    range_mode: RangeMode = config.get("range_mode", "full")
    include_weekends: bool = config.get("include_weekends", True)
    apply_holidays_flag: bool = config.get("apply_holidays", False)

    weekend_days = _get_weekend_days(weekends)
    week_start_dow = _infer_week_start(weekend_days)

    # Holidays only apply when the flag is on and a calendar is provided
    active_holidays = holidays if apply_holidays_flag else None

    # ── Step 1: Determine anchor ──
    if as_of == "last_working_day":
        # Always exclude today — "last working day" means the most recent
        # working day *before* today (e.g. the last trading day).
        anchor = _prev_workday(today, weekend_days, active_holidays)
    else:
        anchor = today

    # ── Step 1b: Year offsets — shift anchor back 1 year, collapse to this/previous ──
    if offset == "last_year_this":
        anchor = _shift_years(anchor, 1)
        offset = "this"
    elif offset == "last_year_previous":
        anchor = _shift_years(anchor, 1)
        offset = "previous"

    # ── Step 2: Determine "this" period boundaries (always calendar) ──
    if period == "day":
        this_start = anchor
        this_end = anchor + timedelta(days=1)
        if not include_weekends:
            # Previous day should be the previous working day, not just -1 calendar day
            prev_start = _prev_workday(anchor, weekend_days, active_holidays)
        else:
            prev_start = anchor - timedelta(days=1)
        prev_end = prev_start + timedelta(days=1)
    elif period == "week":
        this_start = _week_start(anchor, week_start_dow)
        this_end = this_start + timedelta(days=7)
        prev_start = this_start - timedelta(days=7)
        prev_end = this_start
    elif period == "month":
        this_start = _month_start(anchor)
        this_end = _next_month_start(anchor)
        prev_start = _prev_month_start(anchor)
        prev_end = this_start
    elif period == "quarter":
        this_start = _quarter_start(anchor)
        this_end = _next_quarter_start(anchor)
        prev_start = _prev_quarter_start(anchor)
        prev_end = this_start
    elif period == "year":
        this_start = _year_start(anchor)
        this_end = _next_year_start(anchor)
        prev_start = _prev_year_start(anchor)
        prev_end = this_start
    else:
        raise ValueError(f"Unknown period: {period}")

    # ── Step 2b: Trim to working days when include_weekends=False ──
    # For "day" period, as_of="last_working_day" already handles anchor
    # selection; no boundary trimming needed.
    # Save calendar boundaries for to_date mirroring (needs calendar positions).
    cal_this_start = this_start
    cal_prev_start = prev_start
    if not include_weekends and period != "day":
        this_start = _next_workday_inclusive(this_start, weekend_days, active_holidays)
        # lt is exclusive, so: day after last working day before the calendar end
        this_end = _prev_workday(this_end, weekend_days, active_holidays) + timedelta(days=1)
        prev_start = _next_workday_inclusive(prev_start, weekend_days, active_holidays)
        prev_end = _prev_workday(prev_end, weekend_days, active_holidays) + timedelta(days=1)

    # ── Step 3: Select period based on offset ──
    if offset == "this":
        p_start = this_start
        p_end = this_end
    else:  # previous
        p_start = prev_start
        p_end = prev_end

    # ── Step 4: Apply range mode ──
    if range_mode == "full":
        gte = p_start
        lt = p_end
    elif range_mode == "to_date":
        gte = p_start
        if offset == "this":
            # Up to and including the anchor (clamped to working day if needed)
            effective_anchor = anchor
            if not include_weekends:
                effective_anchor = _prev_workday_inclusive(anchor, weekend_days, active_holidays)
            lt = effective_anchor + timedelta(days=1)
        else:
            # Mirror anchor's position into the previous period
            # Use calendar boundaries for mirroring, not trimmed ones
            to_date_end = _to_date_anchor_in_previous_period(
                anchor, period, cal_this_start, cal_prev_start,
            )
            if not include_weekends:
                to_date_end = _prev_workday_inclusive(to_date_end, weekend_days, active_holidays)
            lt = to_date_end + timedelta(days=1)
        # Clamp lt to not exceed the period end
        if lt > p_end:
            lt = p_end
    elif range_mode == "end_of_period":
        # Single day: last day of the period
        ld = _last_day_of_period(
            p_start, p_end, include_weekends, weekend_days, active_holidays,
        )
        gte = ld
        lt = ld + timedelta(days=1)
    else:
        raise ValueError(f"Unknown range_mode: {range_mode}")

    return gte, lt


# ── Legacy preset mapping ────────────────────────────────────────────────

LEGACY_PRESET_MAP: dict[str, PresetConfig] = {
    # Days
    "today":                          PresetConfig(period="day", offset="this",     as_of="today",            range_mode="full",          include_weekends=True,  apply_holidays=False),
    "yesterday":                      PresetConfig(period="day", offset="previous", as_of="today",            range_mode="full",          include_weekends=True,  apply_holidays=False),
    # "day_before_yesterday" removed — handled by _BEFORE_LAST_PRESETS + _shift_one_period_back
    "last_working_day":               PresetConfig(period="day", offset="this",     as_of="last_working_day", range_mode="full",          include_weekends=False, apply_holidays=False),
    "day_before_last_working_day":    PresetConfig(period="day", offset="previous", as_of="last_working_day", range_mode="full",          include_weekends=False, apply_holidays=False),
    # Working Weeks
    "twwtlwd":                        PresetConfig(period="week", offset="this",     as_of="last_working_day", range_mode="to_date",      include_weekends=False, apply_holidays=False),
    "last_working_week":              PresetConfig(period="week", offset="previous", as_of="last_working_day", range_mode="full",          include_weekends=False, apply_holidays=False),
    # "week_before_last_working_week" removed — handled by _BEFORE_LAST_PRESETS + _shift_one_period_back
    "lwwtlwd":                        PresetConfig(period="week", offset="previous", as_of="last_working_day", range_mode="to_date",       include_weekends=False, apply_holidays=False),
    # Calendar Weeks
    "this_week":                      PresetConfig(period="week", offset="this",     as_of="today",            range_mode="full",          include_weekends=True,  apply_holidays=False),
    "last_week":                      PresetConfig(period="week", offset="previous", as_of="today",            range_mode="full",          include_weekends=True,  apply_holidays=False),
    # "week_before_last" removed — handled by _BEFORE_LAST_PRESETS + _shift_one_period_back
    # Months
    "this_month":                     PresetConfig(period="month", offset="this",     as_of="today",            range_mode="full",         include_weekends=True,  apply_holidays=False),
    "tmtlwd":                         PresetConfig(period="month", offset="this",     as_of="last_working_day", range_mode="to_date",      include_weekends=False, apply_holidays=False),
    "last_month":                     PresetConfig(period="month", offset="previous", as_of="today",            range_mode="full",         include_weekends=True,  apply_holidays=False),
    "last_working_month":             PresetConfig(period="month", offset="previous", as_of="last_working_day", range_mode="full",         include_weekends=False, apply_holidays=False),
    # "month_before_last_working_month" removed — handled by _BEFORE_LAST_PRESETS + _shift_one_period_back
    "lwmtlwd":                        PresetConfig(period="month", offset="previous", as_of="last_working_day", range_mode="to_date",      include_weekends=False, apply_holidays=False),
    # Year
    "ytlwd":                          PresetConfig(period="year", offset="this",     as_of="last_working_day", range_mode="to_date",       include_weekends=False, apply_holidays=False),
    "ytd":                            PresetConfig(period="year", offset="this",     as_of="today",            range_mode="to_date",       include_weekends=True,  apply_holidays=False),
    "mtd":                            PresetConfig(period="month", offset="this",    as_of="today",            range_mode="to_date",       include_weekends=True,  apply_holidays=False),
    # Quarters
    "this_quarter":                   PresetConfig(period="quarter", offset="this",     as_of="today", range_mode="full", include_weekends=True,  apply_holidays=False),
    "last_quarter":                   PresetConfig(period="quarter", offset="previous", as_of="today", range_mode="full", include_weekends=True,  apply_holidays=False),
    # Years
    "this_year":                      PresetConfig(period="year", offset="this",     as_of="today", range_mode="full", include_weekends=True,  apply_holidays=False),
    "last_year":                      PresetConfig(period="year", offset="previous", as_of="today", range_mode="full", include_weekends=True,  apply_holidays=False),
    # EOF Weeks
    "eof_last_working_week":             PresetConfig(period="week", offset="previous", as_of="last_working_day", range_mode="end_of_period", include_weekends=False, apply_holidays=False),
    # "eof_week_before_last_working_week" removed — handled by _BEFORE_LAST_PRESETS + _shift_one_period_back
    "eof_lwwtlwd":                       PresetConfig(period="week", offset="previous", as_of="last_working_day", range_mode="end_of_period", include_weekends=False, apply_holidays=False),
    "eof_this_week":                     PresetConfig(period="week", offset="this",     as_of="today",            range_mode="end_of_period", include_weekends=True,  apply_holidays=False),
    "eof_last_week":                     PresetConfig(period="week", offset="previous", as_of="today",            range_mode="end_of_period", include_weekends=True,  apply_holidays=False),
    # EOF Months
    "eof_this_month":                       PresetConfig(period="month", offset="this",     as_of="today",            range_mode="end_of_period", include_weekends=True,  apply_holidays=False),
    "eof_last_month":                       PresetConfig(period="month", offset="previous", as_of="today",            range_mode="end_of_period", include_weekends=True,  apply_holidays=False),
    "eof_last_working_month":               PresetConfig(period="month", offset="previous", as_of="last_working_day", range_mode="end_of_period", include_weekends=False, apply_holidays=False),
    # "eof_month_before_last_working_month" removed — handled by _BEFORE_LAST_PRESETS + _shift_one_period_back
    "eof_lwmtlwd":                          PresetConfig(period="month", offset="previous", as_of="last_working_day", range_mode="end_of_period", include_weekends=False, apply_holidays=False),
}


# ── "Before last" special handling ───────────────────────────────────────
# These legacy presets represent a *second* previous period (2 periods back).
# In the new system, only "this" and "previous" exist.
# We handle these by resolving as "previous" then shifting one more period back.
_BEFORE_LAST_PRESETS: dict[str, str] = {
    "day_before_yesterday": "yesterday",
    "week_before_last": "last_week",
    "week_before_last_working_week": "last_working_week",
    "month_before_last_working_month": "last_working_month",
    "eof_week_before_last_working_week": "eof_last_working_week",
    "eof_month_before_last_working_month": "eof_last_working_month",
}

# Pattern for "last_N_days" relative-day presets (e.g. last_7_days, last_30_days)
_LAST_N_DAYS_RE = re.compile(r"^last_(\d+)_days$")


def _shift_one_period_back(
    gte: datetime, lt: datetime, period: Period
) -> tuple[datetime, datetime]:
    """Shift a resolved date range one additional period into the past."""
    if period == "day":
        return gte - timedelta(days=1), lt - timedelta(days=1)
    elif period == "week":
        return gte - timedelta(days=7), lt - timedelta(days=7)
    elif period == "month":
        new_gte = _prev_month_start(gte)
        new_lt = _prev_month_start(lt) if lt != _next_month_start(gte) else gte
        return new_gte, new_lt
    elif period == "quarter":
        return _prev_quarter_start(gte), gte
    elif period == "year":
        return _prev_year_start(gte), gte
    return gte, lt


# ── Holiday materialization ──────────────────────────────────────────────

_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_DOW_MAP = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> datetime:
    """Find the Nth occurrence of a weekday in a month."""
    first = datetime(year, month, 1)
    delta = (weekday - first.weekday() + 7) % 7
    first_occ = first + timedelta(days=delta)
    return first_occ + timedelta(weeks=n - 1)


def materialize_holidays(
    rules: Sequence[dict],
    year: int,
) -> frozenset[str]:
    """Convert holiday rules into a set of YYYY-MM-DD date strings for a given year."""
    dates: set[str] = set()
    for rule in rules:
        rt = rule.get("rule_type", "")
        if rt == "specific":
            sd = rule.get("specific_date", "")
            if sd and sd.startswith(str(year)):
                dates.add(sd)
        elif rt == "recurring":
            expr = rule.get("recurrence_expr", "").upper().strip()
            if not expr:
                continue
            parts = expr.split("-")
            # Fixed date: "DEC-25"
            if len(parts) == 2 and parts[0] in _MONTH_MAP:
                month = _MONTH_MAP[parts[0]]
                try:
                    day = int(parts[1])
                    dates.add(f"{year}-{month:02d}-{day:02d}")
                except ValueError:
                    pass
            # Nth weekday: "NTH-MON-3-JAN"
            elif len(parts) == 4 and parts[0] == "NTH":
                dow_str, n_str, month_str = parts[1], parts[2], parts[3]
                if dow_str in _DOW_MAP and month_str in _MONTH_MAP:
                    try:
                        n = int(n_str)
                        d = _nth_weekday_of_month(year, _MONTH_MAP[month_str], _DOW_MAP[dow_str], n)
                        dates.add(d.strftime("%Y-%m-%d"))
                    except (ValueError, OverflowError):
                        pass
    return frozenset(dates)


# ── Top-level where-dict resolver ────────────────────────────────────────

# UI meta keys to strip before SQL generation
_UI_META_KEYS = frozenset({
    "filterPreset", "filter_preset", "_preset", "_meta",
    "startDate", "endDate", "start", "end",
    "__week_start_day", "__weekends", "__eof_skip_weekends",
})


def resolve_date_presets(
    where: dict | None,
    *,
    holidays_loader: Any | None = None,
) -> dict | None:
    """Expand any `field__date_preset` entries into `field__gte` / `field__lt` pairs.

    Accepts both legacy string presets and new structured PresetConfig dicts.
    Drop-in replacement for the old _resolve_date_presets in query.py.

    Parameters
    ----------
    where : dict or None
        The query where clause from the widget config.
    holidays_loader : callable, optional
        A callable that returns a frozenset of "YYYY-MM-DD" holiday date strings.
        Called lazily only when a preset has apply_holidays=True.
    """
    if not where:
        return where

    # Read legacy meta keys for backward compat
    _weekends_raw = str(
        where.get("__weekends", os.environ.get("WEEKENDS", "SAT_SUN"))
    ).upper().strip()

    # Read operator hints BEFORE stripping
    _op_hints: dict[str, str] = {}
    for _ok, _ov in where.items():
        if isinstance(_ok, str) and _ok.endswith("__op") and isinstance(_ov, str):
            _op_hints[_ok[:-4]] = _ov.lower().strip()

    # Strip UI meta keys
    where = {k: v for k, v in where.items() if k not in _UI_META_KEYS}
    # Strip __op keys
    where = {k: v for k, v in where.items()
             if not (isinstance(k, str) and k.endswith("__op"))} or {}

    # Pre-collect preset bases to skip stale __gte/__lt
    _preset_bases: set[str] = set()
    for k, v in where.items():
        if isinstance(k, str) and k.endswith("__date_preset"):
            _preset_bases.add(k[: -len("__date_preset")])

    _holidays_cache: frozenset[str] | None = None

    def _get_holidays() -> frozenset[str] | None:
        nonlocal _holidays_cache
        if _holidays_cache is None and holidays_loader is not None:
            _holidays_cache = holidays_loader()
        return _holidays_cache

    expanded: dict = {}
    for k, v in where.items():
        if isinstance(k, str) and k.endswith("__date_preset"):
            base = k[: -len("__date_preset")]

            # Guard: skip empty / None / falsy presets entirely
            if not v:
                continue

            # Determine if legacy string or new structured dict
            if isinstance(v, str):
                preset_key = v.lower().strip()
                if not preset_key:  # empty after strip
                    continue
                is_before_last = preset_key in _BEFORE_LAST_PRESETS
                # For "before last" presets, look up the base preset config
                lookup_key = _BEFORE_LAST_PRESETS.get(preset_key, preset_key) if is_before_last else preset_key
                config = LEGACY_PRESET_MAP.get(lookup_key)
                if config is None:
                    # Check for last_N_days pattern (e.g. last_7_days, last_30_days)
                    m = _LAST_N_DAYS_RE.match(preset_key)
                    if m:
                        n = int(m.group(1))
                        today = datetime.now().replace(
                            hour=0, minute=0, second=0, microsecond=0
                        )
                        gte = today - timedelta(days=n)
                        lt = today + timedelta(days=1)
                        expanded[f"{base}__gte"] = gte.strftime("%Y-%m-%d")
                        expanded[f"{base}__lt"] = lt.strftime("%Y-%m-%d")
                        continue
                    # Unknown legacy preset — pass through
                    expanded[k] = v
                    continue
            elif isinstance(v, dict):
                # Guard: require at least 'period' key for structured presets
                if "period" not in v:
                    continue
                config = PresetConfig(**{
                    key: val for key, val in v.items()
                    if key in PresetConfig.__annotations__
                })
                is_before_last = False
            else:
                expanded[k] = v
                continue

            # Resolve holidays if needed
            holidays = _get_holidays() if config.get("apply_holidays") else None

            # Resolve
            gte, lt = resolve_preset(
                config,
                weekends=_weekends_raw,
                holidays=holidays,
            )

            # Handle "before last" legacy presets
            if is_before_last:
                gte, lt = _shift_one_period_back(gte, lt, config["period"])

            # Apply operator hints
            _bound_op = _op_hints.get(base, "")
            if gte and _bound_op not in ("lt", "lte"):
                _new_gte = gte.strftime("%Y-%m-%d")
                _exist_gte = expanded.get(f"{base}__gte")
                if _bound_op in ("gte", "gt") and _exist_gte:
                    expanded[f"{base}__gte"] = max(_exist_gte, _new_gte)
                else:
                    expanded[f"{base}__gte"] = _new_gte
            if lt and _bound_op not in ("gte", "gt"):
                _new_lt = lt.strftime("%Y-%m-%d")
                _exist_lt = expanded.get(f"{base}__lt")
                if _bound_op in ("lt", "lte") and _exist_lt:
                    expanded[f"{base}__lt"] = min(_exist_lt, _new_lt)
                else:
                    expanded[f"{base}__lt"] = _new_lt
        else:
            # Skip stale __gte/__lt for fields that have a __date_preset
            _is_stale = (
                isinstance(k, str)
                and (k.endswith("__gte") or k.endswith("__lt"))
                and any(k == f"{b}__gte" or k == f"{b}__lt" for b in _preset_bases)
            )
            if not _is_stale:
                expanded[k] = v
    return expanded
