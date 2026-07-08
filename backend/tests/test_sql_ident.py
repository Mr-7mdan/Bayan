"""Regression checks for SQL-injection hardening (spec 03)."""

import pytest


def test_quote_ident_escapes():
    from app.sql_ident import quote_ident
    assert quote_ident('a"b') == '"a""b"'                       # duckdb default
    assert quote_ident('x`y', 'mysql') == '`x``y`'
    assert quote_ident('a]b', 'mssql') == '[a]]b]'
    # breakout attempt cannot escape the quotes
    q = quote_ident('x" ; DROP TABLE t; --')
    assert q.count('"') % 2 == 0 and ';' in q  # ';' stays inside the quotes


def test_quote_ident_idempotent():
    from app.sql_ident import quote_ident
    # already-quoted input is unwrapped once then re-quoted, not double-wrapped
    assert quote_ident('"col"') == '"col"'
    assert quote_ident(quote_ident('col')) == '"col"'


def test_quote_source_dotted():
    from app.sql_ident import quote_source
    assert quote_source('schema.table') == '"schema"."table"'
    # expressions/subqueries are left untouched
    assert quote_source('(SELECT 1)') == '(SELECT 1)'


def test_validate_expr_blocks_stacked():
    from app.sqlgen import validate_expr
    with pytest.raises(ValueError):
        validate_expr("1); DROP TABLE t; --", "duckdb")
    with pytest.raises(ValueError):
        validate_expr("(SELECT 1) /* x */", "duckdb")
    assert validate_expr("amount * 1.2", "duckdb")
    # a legitimate CASE expression still builds
    assert validate_expr("CASE WHEN x = 1 THEN 2 ELSE 3 END", "duckdb")


def test_attach_rejects_quote():
    from app.routers.query import build_attach_string
    with pytest.raises(ValueError):
        build_attach_string({'host': 'h', 'port': 3306, 'user': 'u', "password": "p' OR '1"})
    with pytest.raises(ValueError):
        build_attach_string({'host': "h'; DROP", 'port': 3306})
    # a clean set of credentials builds a well-formed string
    s = build_attach_string({'host': 'h', 'port': 3306, 'user': 'u', 'password': 'p', 'database': 'd'})
    assert s == "host=h port=3306 user=u password=p database=d"


def test_scrub_redacts_secrets():
    from app.sql_ident import scrub
    out = scrub("ATTACH failed near password=hunter2", ["hunter2"])
    assert "hunter2" not in out and "***" in out
