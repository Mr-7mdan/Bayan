"""Unit tests for app.sql_dialect_normalizer.auto_normalize (spec 25).

Converts the 3 print-only cases from the old backend/test_normalizer.py scratch
script into real asserts. Run from backend/:
    python -m pytest tests/test_sql_dialect_normalizer.py
"""
from app.sql_dialect_normalizer import auto_normalize


def test_bracket_idents_to_duckdb_quoting():
    expr = (
        "CASE  WHEN [s].[ClientID] = '2' THEN 'BOP' "
        " WHEN [s].[ClientID] = '4' THEN 'BOJ'END"
    )
    out = auto_normalize(expr, "duckdb")
    assert '"s"."ClientID"' in out
    assert "[" not in out and "]" not in out
    # Missing space before the trailing END keyword is repaired.
    assert "'BOJ' END" in out


def test_missing_space_before_end_repaired():
    expr = "CASE WHEN status = 1 THEN 'Active'ELSE 'Inactive'END"
    out = auto_normalize(expr, "duckdb")
    assert "'Inactive' END" in out
    assert out.rstrip().endswith("END")


def test_complex_bracket_idents_duckdb_and_mysql():
    expr = "[schema].[table].[column] = [s].[ClientID]"
    duck = auto_normalize(expr, "duckdb")
    assert duck == '"schema"."table"."column" = "s"."ClientID"'
    mysql = auto_normalize(expr, "mysql")
    assert mysql == "`schema`.`table`.`column` = `s`.`ClientID`"
