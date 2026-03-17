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
        now = datetime(2026, 3, 17)  # Tuesday
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 17)
        assert lt == datetime(2026, 3, 18)

    def test_last_working_day_on_monday(self):
        now = datetime(2026, 3, 16)  # Monday
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 16)
        assert lt == datetime(2026, 3, 17)

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
        now = datetime(2026, 3, 17)  # Tuesday
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="previous", as_of="last_working_day", range_mode="full",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 16)
        assert lt == datetime(2026, 3, 17)


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
        now = datetime(2026, 3, 17)  # Tuesday
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="this", as_of="last_working_day", range_mode="to_date",
                         include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 16)
        assert lt == datetime(2026, 3, 18)

    def test_end_of_previous_working_week(self):
        now = datetime(2026, 3, 17)  # Tuesday
        gte, lt = resolve_preset(
            PresetConfig(period="week", offset="previous", as_of="last_working_day",
                         range_mode="end_of_period", include_weekends=False),
            now=now, weekends="SAT_SUN",
        )
        assert gte == datetime(2026, 3, 13)
        assert lt == datetime(2026, 3, 14)


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
        now = datetime(2026, 3, 17)  # Tuesday
        holidays = frozenset({"2026-03-17"})
        gte, lt = resolve_preset(
            PresetConfig(period="day", offset="this", as_of="last_working_day", range_mode="full",
                         include_weekends=False, apply_holidays=False),
            now=now, weekends="SAT_SUN", holidays=holidays,
        )
        assert gte == datetime(2026, 3, 17)
        assert lt == datetime(2026, 3, 18)


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
