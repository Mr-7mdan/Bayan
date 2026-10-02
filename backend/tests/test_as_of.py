"""`__as_of` in a where clause resolves date presets as if that day were today.

This is what reproduces a dashboard as it stood on a past date. The cases mirror
the United Daily Executive Summary review of 25/09/2026, which was viewed on
Monday 28/09: "Today" is the last working day (Fri 25/09), MTD runs to it, and
LMTD/LYTD run to the same day of the previous month/year.

Run:  cd backend && PYTHONPATH=. ./venv/bin/python -m pytest tests/test_as_of.py -q
"""
from __future__ import annotations

from app.date_presets import resolve_date_presets


def _preset(period, offset, range_mode="to_date", as_of="last_working_day", weekends=True):
    return {"period": period, "offset": offset, "as_of": as_of, "range_mode": range_mode,
            "include_weekends": weekends, "apply_holidays": False}


def _bounds(where):
    r = resolve_date_presets(where) or {}
    return r.get("Time__gte"), r.get("Time__lt")


def test_as_of_sets_the_reference_day():
    w = {"Time__date_preset": _preset("day", "this", "full", weekends=False), "__as_of": "2026-09-28"}
    assert _bounds(w) == ("2026-09-25", "2026-09-26")  # Monday -> last working day Friday


def test_as_of_drives_every_period_shape():
    base = {"__as_of": "2026-09-28"}
    cases = {
        ("day", "previous", "full"): ("2026-09-24", "2026-09-25"),
        ("month", "this", "to_date"): ("2026-09-01", "2026-09-26"),
        ("month", "previous", "to_date"): ("2026-08-01", "2026-08-26"),
        ("year", "this", "to_date"): ("2026-01-01", "2026-09-26"),
        ("year", "previous", "to_date"): ("2025-01-01", "2025-09-26"),
    }
    for (period, offset, mode), want in cases.items():
        assert _bounds({**base, "Time__date_preset": _preset(period, offset, mode)}) == want, (period, offset, mode)


def test_legacy_string_presets_honour_as_of():
    w = {"Time__date_preset": "last_working_day", "__weekends": "SAT_SUN", "__as_of": "2026-09-28"}
    assert _bounds(w) == ("2026-09-25", "2026-09-26")


def test_as_of_never_reaches_sql():
    r = resolve_date_presets({"Time__date_preset": "last_working_day", "__as_of": "2026-09-28", "Login": [1]})
    assert "__as_of" not in r and r["Login"] == [1]


def _cb(where):
    r = resolve_date_presets({**where, "__as_of": "2026-09-28"}) or {}
    return r.get("created_at__gte"), r.get("created_at__lt"), r


def test_shift_wd_maps_business_days_to_next_morning_snapshots():
    """daily_kpi rows are captured the next working morning (row 26/08 = close of 25/08).
    Same day last month, by business day, must reach the 26/08 row."""
    gte, lt, r = _cb({"created_at__date_preset": _preset("month", "previous"), "created_at__shift_wd": 1})
    assert (gte, lt) == ("2026-08-04", "2026-08-27")
    assert "created_at__shift_wd" not in r  # never reaches SQL as a column filter


def test_shift_wd_window_starting_on_weekend_skips_previous_friday():
    """Aug 1 2026 is a Saturday. Monday 3/8's row is Friday 31/7's close and must be
    excluded — shifting the raw bounds by a day would have let it in."""
    gte, _, _ = _cb({"created_at__date_preset": _preset("month", "previous"), "created_at__shift_wd": 1})
    assert gte == "2026-08-04"  # first business day Mon 3/8 -> captured Tue 4/8


def test_shift_wd_monday_snapshot_holds_fridays_close():
    gte, lt, _ = _cb({"created_at__date_preset": _preset("day", "this", "full"), "created_at__shift_wd": 1})
    assert (gte, lt) == ("2026-09-28", "2026-09-29")  # business Fri 25/9 -> captured Mon 28/9


def test_no_shift_leaves_window_alone():
    gte, lt, _ = _cb({"created_at__date_preset": _preset("year", "previous")})
    assert (gte, lt) == ("2025-01-01", "2025-09-26")


def test_invalid_as_of_falls_back_to_now():
    """A malformed date must not break the dashboard — it is simply ignored."""
    w = {"Time__date_preset": _preset("day", "this", "full"), "__as_of": "not-a-date"}
    gte, lt = _bounds(w)
    assert gte and lt  # resolved against the real clock
