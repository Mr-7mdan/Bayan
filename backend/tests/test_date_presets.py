"""Tests for the composable date preset resolver."""
import pytest
from datetime import datetime, timedelta
from app.date_presets import (
    PresetConfig,
    resolve_preset,
    resolve_date_presets,
    LEGACY_PRESET_MAP,
    Period,
    Offset,
    AsOf,
    RangeMode,
    _prev_workday,
    _prev_workday_inclusive,
    _next_workday_inclusive,
    _week_start,
    _infer_week_start,
    _get_weekend_days,
)


class TestPresetConfig:
    """Test the PresetConfig typed-dict validation."""

    def test_valid_config(self):
        cfg = PresetConfig(
            period="week",
            offset="this",
            as_of="today",
            range_mode="full",
            include_weekends=True,
            apply_holidays=False,
        )
        assert cfg["period"] == "week"
        assert cfg["offset"] == "this"

    def test_legacy_map_covers_all_old_presets(self):
        """Every old preset string must have a mapping."""
        expected_presets = {
            "today", "yesterday", "day_before_yesterday",
            "last_working_day", "day_before_last_working_day",
            "twwtlwd", "last_working_week", "week_before_last_working_week",
            "lwwtlwd", "this_week", "last_week", "week_before_last",
            "this_month", "tmtlwd", "ytlwd", "last_month",
            "last_working_month", "month_before_last_working_month",
            "lwmtlwd", "this_quarter", "last_quarter",
            "this_year", "last_year",
            "eof_last_working_week", "eof_week_before_last_working_week",
            "eof_this_week", "eof_last_week",
            "eof_this_month", "eof_last_month",
            "eof_last_working_month", "eof_month_before_last_working_month",
            "eof_lwwtlwd", "eof_lwmtlwd",
            "ytd", "mtd",
        }
        assert expected_presets.issubset(set(LEGACY_PRESET_MAP.keys()))


class TestHelpers:
    """Test low-level date helpers."""

    def test_infer_week_start_sat_sun(self):
        assert _infer_week_start((5, 6)) == 0  # Monday

    def test_infer_week_start_fri_sat(self):
        assert _infer_week_start((4, 5)) == 6  # Sunday

    def test_prev_workday_skips_weekend(self):
        # Monday March 16, 2026 → prev workday = Friday March 13
        mon = datetime(2026, 3, 16)
        result = _prev_workday(mon, (5, 6))
        assert result == datetime(2026, 3, 13)

    def test_prev_workday_skips_holidays(self):
        # Thursday March 12, 2026 → if Wed March 11 is holiday → Tue March 10
        thu = datetime(2026, 3, 12)
        holidays = frozenset({"2026-03-11"})
        result = _prev_workday(thu, (5, 6), holidays)
        assert result == datetime(2026, 3, 10)

    def test_prev_workday_inclusive_on_workday(self):
        # Wednesday = workday → returns Wednesday
        wed = datetime(2026, 3, 18)
        result = _prev_workday_inclusive(wed, (5, 6))
        assert result == wed

    def test_prev_workday_inclusive_on_weekend(self):
        # Saturday March 14 → returns Friday March 13
        sat = datetime(2026, 3, 14)
        result = _prev_workday_inclusive(sat, (5, 6))
        assert result == datetime(2026, 3, 13)

    def test_next_workday_inclusive_on_workday(self):
        # Wednesday = workday → returns Wednesday
        wed = datetime(2026, 3, 18)
        result = _next_workday_inclusive(wed, (5, 6))
        assert result == wed

    def test_next_workday_inclusive_on_weekend(self):
        # Saturday March 14 → returns Monday March 16
        sat = datetime(2026, 3, 14)
        result = _next_workday_inclusive(sat, (5, 6))
        assert result == datetime(2026, 3, 16)

    def test_next_workday_inclusive_skips_holidays(self):
        # Monday March 16, holiday → Tuesday March 17
        mon = datetime(2026, 3, 16)
        holidays = frozenset({"2026-03-16"})
        result = _next_workday_inclusive(mon, (5, 6), holidays)
        assert result == datetime(2026, 3, 17)


class TestResolvePresetDay:
    """Day period tests."""

    def test_today(self):
        now = datetime(2026, 3, 17)  # Tuesday
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="this", as_of="today", range_mode="full",
                         include_weekends=True),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 17)
        assert lt == datetime(2026, 3, 18)

    def test_yesterday(self):
        now = datetime(2026, 3, 17)
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="previous", as_of="today", range_mode="full",
                         include_weekends=True),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 16)
        assert lt == datetime(2026, 3, 17)

    def test_last_working_day_on_weekday(self):
        # as_of=last_working_day always excludes today: Tue → Mon
        now = datetime(2026, 3, 17)  # Tuesday
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 16)  # Monday (prev workday before Tue)
        assert lt == datetime(2026, 3, 17)

    def test_last_working_day_on_monday(self):
        # as_of=last_working_day always excludes today: Mon → Fri
        now = datetime(2026, 3, 16)  # Monday
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 13)  # Friday (prev workday before Mon)
        assert lt == datetime(2026, 3, 14)

    def test_last_working_day_on_weekend(self):
        now = datetime(2026, 3, 14)  # Saturday
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 13)
        assert lt == datetime(2026, 3, 14)

    def test_day_before_last_working_day(self):
        # anchor = _prev_workday(Tue) = Mon. offset=previous → Mon - 1 = Sun Mar 15.
        # period=day doesn't apply Step 2b trimming, so the raw Sunday is returned.
        now = datetime(2026, 3, 17)  # Tuesday
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="previous", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 15)  # Sunday (raw: anchor Mon - 1)
        assert lt == datetime(2026, 3, 16)


class TestResolvePresetWeek:
    """Week period tests."""

    def test_this_week_calendar(self):
        now = datetime(2026, 3, 17)  # Tuesday. SAT_SUN weekends → week starts Monday.
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="this", as_of="today", range_mode="full",
                         include_weekends=True),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 16)
        assert lt == datetime(2026, 3, 23)

    def test_previous_week_calendar(self):
        now = datetime(2026, 3, 17)
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="previous", as_of="today", range_mode="full",
                         include_weekends=True),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 9)
        assert lt == datetime(2026, 3, 16)

    def test_this_working_week_to_date(self):
        # anchor = _prev_workday(Tue) = Mon. Week Mon-Fri, to_date → [Mon, Tue).
        now = datetime(2026, 3, 17)  # Tuesday
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="this", as_of="last_working_day", range_mode="to_date",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 16)  # Mon (week start)
        assert lt == datetime(2026, 3, 17)   # Tue (exclusive: anchor Mon + 1)

    def test_end_of_previous_working_week(self):
        now = datetime(2026, 3, 17)  # Tuesday
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="previous", as_of="last_working_day",
                         range_mode="end_of_period", include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 13)
        assert lt == datetime(2026, 3, 14)

    def test_this_working_week_full(self):
        """Working week (no weekends) should span Mon-Fri (5 days)."""
        now = datetime(2026, 3, 18)  # Wednesday
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="this", as_of="today", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        # This working week: Mon Mar 16 - Fri Mar 20 (lt=Sat Mar 21, exclusive)
        assert gte == datetime(2026, 3, 16)
        assert lt == datetime(2026, 3, 21)

    def test_previous_working_week_full(self):
        """Previous working week should span Mon-Fri of the prior week."""
        now = datetime(2026, 3, 18)  # Wednesday
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="previous", as_of="today", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        # Previous working week: Mon Mar 9 - Fri Mar 13 (lt=Sat Mar 14, exclusive)
        assert gte == datetime(2026, 3, 9)
        assert lt == datetime(2026, 3, 14)

    def test_working_week_fri_sat_weekends(self):
        """FRI_SAT weekends: working week is Sun-Thu (5 days)."""
        now = datetime(2026, 3, 18)  # Wednesday
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="this", as_of="today", range_mode="full",
                         include_weekends=False),
            now=now, weekends="FRI_SAT",
        )
        # FRI_SAT weekends → working week starts Sunday. max(4,5)+1 % 7 = 6 (Sun)
        # Walk back from Wed Mar 18 to Sun Mar 15
        assert gte == datetime(2026, 3, 15)
        assert lt == datetime(2026, 3, 20)  # Sun + 5 = Fri (exclusive)


class TestResolvePresetMonth:
    """Month period tests."""

    def test_this_month(self):
        now = datetime(2026, 3, 17)
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="this", as_of="today", range_mode="full",
                         include_weekends=True),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 1)
        assert lt == datetime(2026, 4, 1)

    def test_previous_month(self):
        now = datetime(2026, 3, 17)
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="previous", as_of="today", range_mode="full",
                         include_weekends=True),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 2, 1)
        assert lt == datetime(2026, 3, 1)

    def test_month_to_date(self):
        now = datetime(2026, 3, 17)
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="this", as_of="today", range_mode="to_date",
                         include_weekends=True),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 1)
        assert lt == datetime(2026, 3, 18)

    def test_previous_month_to_date(self):
        now = datetime(2026, 3, 17)
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="previous", as_of="today", range_mode="to_date",
                         include_weekends=True),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 2, 1)
        assert lt == datetime(2026, 2, 18)

    def test_end_of_previous_month(self):
        now = datetime(2026, 3, 17)
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="previous", as_of="today", range_mode="end_of_period",
                         include_weekends=True),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 2, 28)
        assert lt == datetime(2026, 3, 1)

    def test_end_of_previous_working_month(self):
        now = datetime(2026, 3, 17)
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="previous", as_of="today", range_mode="end_of_period",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        # Feb 28, 2026 is Saturday → last workday = Feb 27 (Friday)
        assert gte == datetime(2026, 2, 27)
        assert lt == datetime(2026, 2, 28)


class TestResolvePresetQuarterYear:
    """Quarter and Year period tests."""

    def test_this_quarter(self):
        now = datetime(2026, 3, 17)
        gte, lt = resolve_preset(
            PresetConfig(period="quarter", offset="this", as_of="today", range_mode="full",
                         include_weekends=True),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 1, 1)
        assert lt == datetime(2026, 4, 1)

    def test_year_to_date(self):
        now = datetime(2026, 3, 17)
        gte, lt = resolve_preset(
            PresetConfig(period="year", offset="this", as_of="today", range_mode="to_date",
                         include_weekends=True),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 1, 1)
        assert lt == datetime(2026, 3, 18)


class TestExcludeWeekends:
    """Comprehensive tests for include_weekends=False across all periods.

    Ensures consistent, predictable behavior: when weekends are excluded,
    gte starts on first working day, lt ends day after last working day,
    and no gte/lt boundary falls on a weekend.
    """

    # ── Week ──────────────────────────────────────────────────────────
    def test_week_this_full(self):
        now = datetime(2026, 3, 18)  # Wednesday
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="this", as_of="today", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 16)  # Mon
        assert lt == datetime(2026, 3, 21)   # Sat (excl) → Fri inclusive

    def test_week_previous_full(self):
        now = datetime(2026, 3, 18)
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="previous", as_of="today", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 9)   # Mon
        assert lt == datetime(2026, 3, 14)   # Sat (excl) → Fri 13 inclusive

    def test_week_previous_full_fri_sat(self):
        """FRI_SAT weekends: working week Sun-Thu."""
        now = datetime(2026, 3, 18)  # Wed
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="previous", as_of="today", range_mode="full",
                         include_weekends=False),
            now=now, weekends="FRI_SAT",
        )
        assert gte == datetime(2026, 3, 8)   # Sun
        assert lt == datetime(2026, 3, 13)   # Fri (excl) → Thu 12 inclusive

    # ── Month ─────────────────────────────────────────────────────────
    def test_month_previous_full(self):
        """Feb 2026: Feb 1 = Sun, Feb 28 = Sat."""
        now = datetime(2026, 3, 18)
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="previous", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 2, 2)   # Mon (Feb 1 is Sun → next workday)
        assert lt == datetime(2026, 2, 28)   # Sat (excl) → Fri 27 inclusive

    def test_month_this_full(self):
        """Mar 2026: Mar 1 = Sun."""
        now = datetime(2026, 3, 18)
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 2)   # Mon (Mar 1 is Sun → next workday)
        assert lt == datetime(2026, 4, 1)    # Wed (excl) → Tue Mar 31 inclusive (workday)

    def test_month_previous_full_start_on_saturday(self):
        """Aug 2026: Aug 1 = Sat."""
        now = datetime(2026, 9, 3)  # Sep 3 Thu — prev month = August
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="previous", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 8, 3)   # Mon (Aug 1 = Sat → next workday)
        assert lt == datetime(2026, 9, 1)    # Tue (excl) → Mon Aug 31 inclusive (workday)

    def test_month_previous_full_end_on_sunday(self):
        """May 2026: May 31 = Sun."""
        now = datetime(2026, 6, 3)
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="previous", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 5, 1)   # Fri (workday)
        assert lt == datetime(2026, 5, 30)   # Sat (excl) → Fri May 29 inclusive

    def test_month_previous_to_date(self):
        # anchor = _prev_workday(Wed Mar 18) = Tue Mar 17.
        # Mirror: day 17 of prev month → Feb 17. to_date lt = Feb 18.
        now = datetime(2026, 3, 18)  # Wed
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="previous", as_of="last_working_day", range_mode="to_date",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 2, 2)   # Mon (Feb 1 = Sun, trimmed)
        assert lt == datetime(2026, 2, 18)   # Wed (excl) → Tue Feb 17 inclusive

    def test_month_previous_end_of_period(self):
        now = datetime(2026, 3, 18)
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="previous", as_of="last_working_day", range_mode="end_of_period",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        # Feb 28 = Sat → last working day = Fri Feb 27
        assert gte == datetime(2026, 2, 27)
        assert lt == datetime(2026, 2, 28)

    def test_month_this_to_date_anchor_on_weekend(self):
        """as_of=today on a Saturday with include_weekends=False → clamp to Friday."""
        now = datetime(2026, 3, 14)  # Saturday
        gte, lt = resolve_preset(
            PresetConfig(period="month", offset="this", as_of="today", range_mode="to_date",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 2)   # Mon (Mar 1 = Sun)
        assert lt == datetime(2026, 3, 14)   # Sat (excl) → Fri 13 inclusive

    # ── Quarter ───────────────────────────────────────────────────────
    def test_quarter_this_full(self):
        now = datetime(2026, 3, 18)  # Q1 2026: Jan 1 (Thu) - Mar 31 (Tue)
        gte, lt = resolve_preset(
            PresetConfig(period="quarter", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 1, 1)   # Thu (workday)
        assert lt == datetime(2026, 4, 1)    # Wed (excl) → Tue Mar 31 inclusive

    def test_quarter_previous_full(self):
        """Q4 2025: Oct 1 (Wed) - Dec 31 (Wed)."""
        now = datetime(2026, 3, 18)
        gte, lt = resolve_preset(
            PresetConfig(period="quarter", offset="previous", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2025, 10, 1)  # Wed (workday)
        assert lt == datetime(2026, 1, 1)    # Thu (excl) → Wed Dec 31 inclusive

    def test_quarter_start_on_weekend(self):
        """Q2 2026: Apr 1 (Wed) — no issue. Q3 2023: Jul 1 (Sat) → trim."""
        now = datetime(2023, 7, 5)  # Q3 2023, Jul 1 = Sat
        gte, lt = resolve_preset(
            PresetConfig(period="quarter", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2023, 7, 3)   # Mon (Jul 1 = Sat → next workday)

    # ── Year ──────────────────────────────────────────────────────────
    def test_year_this_full(self):
        now = datetime(2026, 3, 18)  # Jan 1 2026 = Thu, Dec 31 2026 = Thu
        gte, lt = resolve_preset(
            PresetConfig(period="year", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 1, 1)   # Thu (workday)
        assert lt == datetime(2027, 1, 1)    # Fri (excl) → Thu Dec 31 inclusive

    def test_year_start_on_weekend(self):
        """2022: Jan 1 = Saturday → trim to Mon Jan 3."""
        now = datetime(2022, 6, 15)
        gte, lt = resolve_preset(
            PresetConfig(period="year", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2022, 1, 3)   # Mon (Jan 1 = Sat, Jan 2 = Sun)

    def test_year_end_on_weekend(self):
        """2023: Dec 31 = Sunday → trim to Fri Dec 29."""
        now = datetime(2023, 6, 15)
        gte, lt = resolve_preset(
            PresetConfig(period="year", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert lt == datetime(2023, 12, 30)  # Sat (excl) → Fri Dec 29 inclusive

    # ── Invariant: no weekend boundary on any combo ───────────────────
    @pytest.mark.parametrize("period", ["week", "month", "quarter", "year"])
    @pytest.mark.parametrize("offset", ["this", "previous"])
    @pytest.mark.parametrize("range_mode", ["full", "to_date", "end_of_period"])
    def test_no_weekend_boundaries(self, period, offset, range_mode):
        """gte and inclusive-lt must never fall on a weekend when include_weekends=False."""
        now = datetime(2026, 3, 18)
        gte, lt = resolve_preset(
            PresetConfig(period=period, offset=offset, as_of="last_working_day",
                         range_mode=range_mode, include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        inclusive_end = lt - timedelta(days=1)
        assert gte.weekday() not in (5, 6), (
            f"gte={gte.date()} ({gte.strftime('%A')}) is a weekend!"
        )
        assert inclusive_end.weekday() not in (5, 6), (
            f"inclusive end={inclusive_end.date()} ({inclusive_end.strftime('%A')}) is a weekend!"
        )
        assert gte <= lt, f"gte={gte.date()} must be <= lt={lt.date()}"


class TestResolvePresetHolidays:
    """Holiday calendar integration tests."""

    def test_lwd_skips_holiday(self):
        now = datetime(2026, 3, 17)  # Tuesday
        holidays = frozenset({"2026-03-17"})  # Tuesday is a holiday
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False, apply_holidays=True),
            now=now, weekends="SAT_SUN", holidays=holidays,
        )
        assert gte == datetime(2026, 3, 16)
        assert lt == datetime(2026, 3, 17)

    def test_holidays_ignored_when_flag_off(self):
        # apply_holidays=False: Mar 17 is a holiday but flag is off, so holidays
        # don't affect anchor. But as_of=last_working_day still steps back:
        # _prev_workday(Tue) = Mon Mar 16.
        now = datetime(2026, 3, 17)  # Tuesday
        holidays = frozenset({"2026-03-17"})
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False, apply_holidays=False),
            now=now, weekends="SAT_SUN", holidays=holidays,
        )
        assert gte == datetime(2026, 3, 16)  # Mon (prev workday, holidays ignored)
        assert lt == datetime(2026, 3, 17)


class TestResolveDatePresetsWhere:
    """Test the top-level where-dict resolver (dual-format detection)."""

    def test_new_format_dict(self):
        where = {
            "Time__date_preset": {
                "period": "day",
                "offset": "this",
                "as_of": "today",
                "range_mode": "full",
                "include_weekends": True,
            },
            "Status": "Active",
        }
        result = resolve_date_presets(where)
        assert "Time__gte" in result
        assert "Time__lt" in result
        assert result["Status"] == "Active"
        assert "Time__date_preset" not in result

    def test_legacy_string_format(self):
        where = {
            "Time__date_preset": "today",
            "__weekends": "SAT_SUN",
        }
        result = resolve_date_presets(where)
        assert "Time__gte" in result
        assert "Time__lt" in result
        assert "__weekends" not in result

    def test_stale_bounds_stripped(self):
        where = {
            "Time__date_preset": "today",
            "Time__gte": "2020-01-01",
            "Time__lt": "2020-01-02",
        }
        result = resolve_date_presets(where)
        assert result["Time__gte"] != "2020-01-01"

    def test_none_where_passthrough(self):
        assert resolve_date_presets(None) is None

    def test_empty_where_passthrough(self):
        assert resolve_date_presets({}) == {}

    def test_unknown_legacy_preset_passthrough(self):
        where = {"Time__date_preset": "some_unknown_preset"}
        result = resolve_date_presets(where)
        assert result.get("Time__date_preset") == "some_unknown_preset"

    def test_non_preset_keys_preserved(self):
        where = {
            "Status": "Active",
            "Region__in": ["US", "EU"],
        }
        result = resolve_date_presets(where)
        assert result["Status"] == "Active"
        assert result["Region__in"] == ["US", "EU"]
