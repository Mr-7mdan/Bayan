"""Tests for holiday calendar functionality."""
import pytest
from datetime import datetime

from app.date_presets import materialize_holidays


# ── Model tests ──────────────────────────────────────────────────────────


class TestHolidayRuleModel:
    def test_create_specific_date_rule(self):
        """A specific-date holiday rule stores a single date."""
        pytest.importorskip("sqlalchemy")
        from app.models import HolidayRule
        rule = HolidayRule(
            id="h1",
            name="New Year",
            rule_type="specific",
            specific_date="2026-01-01",
        )
        assert rule.rule_type == "specific"
        assert rule.specific_date == "2026-01-01"

    def test_create_recurring_rule(self):
        """A recurring rule stores a recurrence expression."""
        pytest.importorskip("sqlalchemy")
        from app.models import HolidayRule
        rule = HolidayRule(
            id="h2",
            name="Christmas",
            rule_type="recurring",
            recurrence_expr="DEC-25",
        )
        assert rule.rule_type == "recurring"
        assert rule.recurrence_expr == "DEC-25"


# ── Materialization tests ────────────────────────────────────────────────


class TestMaterializeHolidays:
    def test_specific_date(self):
        rules = [{"rule_type": "specific", "specific_date": "2026-12-25"}]
        result = materialize_holidays(rules, year=2026)
        assert "2026-12-25" in result

    def test_recurring_fixed_date(self):
        rules = [{"rule_type": "recurring", "recurrence_expr": "DEC-25"}]
        result = materialize_holidays(rules, year=2026)
        assert "2026-12-25" in result
        result_2027 = materialize_holidays(rules, year=2027)
        assert "2027-12-25" in result_2027

    def test_recurring_nth_weekday(self):
        # 3rd Monday of January 2026: Jan 1=Thu, 1st Mon=5, 2nd Mon=12, 3rd Mon=19
        rules = [{"rule_type": "recurring", "recurrence_expr": "NTH-MON-3-JAN"}]
        result = materialize_holidays(rules, year=2026)
        assert "2026-01-19" in result

    def test_specific_date_wrong_year_excluded(self):
        rules = [{"rule_type": "specific", "specific_date": "2025-12-25"}]
        result = materialize_holidays(rules, year=2026)
        assert "2025-12-25" not in result

    def test_multiple_rules(self):
        rules = [
            {"rule_type": "specific", "specific_date": "2026-03-21"},
            {"rule_type": "recurring", "recurrence_expr": "DEC-25"},
        ]
        result = materialize_holidays(rules, year=2026)
        assert "2026-03-21" in result
        assert "2026-12-25" in result

    def test_empty_rules(self):
        result = materialize_holidays([], year=2026)
        assert len(result) == 0

    def test_invalid_recurrence_ignored(self):
        rules = [{"rule_type": "recurring", "recurrence_expr": "INVALID"}]
        result = materialize_holidays(rules, year=2026)
        assert len(result) == 0

    def test_nth_weekday_different_months(self):
        # 1st Friday of March 2026: Mar 1=Sun, 1st Fri=6
        rules = [{"rule_type": "recurring", "recurrence_expr": "NTH-FRI-1-MAR"}]
        result = materialize_holidays(rules, year=2026)
        assert "2026-03-06" in result

    def test_frozenset_returned(self):
        rules = [{"rule_type": "specific", "specific_date": "2026-01-01"}]
        result = materialize_holidays(rules, year=2026)
        assert isinstance(result, frozenset)
