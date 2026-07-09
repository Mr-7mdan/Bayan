"""
SQLGlot parity / equivalence matrix (spec 11, Phase C).

Goal: de-risk flipping the SQLGlot default by proving the SQLGlotBuilder
produces SQL that, when EXECUTED against a real DuckDB fixture, returns the
same result set as an independent reference query -- and, for the DISTINCT
shape, the same result set as the legacy ``sqlgen.build_distinct_sql``.

Design notes / known intentional differences:
- The SQLGlot aggregation builder coerces the measure column with
  ``TRY_CAST(REGEXP_REPLACE(CAST(col AS TEXT), ...) AS DOUBLE)``. For clean
  numeric data this coercion is the identity, so we compare against a plain
  reference query with a float tolerance (1e-9).
- Derived-column-in-WHERE (e.g. ``{"Date (Year)": [...]}``) is a known-broken
  path under the installed sqlglot (see the xfail in test_sqlglot_builder.py)
  and is deliberately NOT exercised here.
- Non-DuckDB dialects have no live DB in CI, so we only assert the generated
  SQL parses via ``sqlgen_glot.validate_sql`` (same pattern as
  test_sqlglot_builder.py::test_postgres_dialect).
"""
import datetime as _dt

import duckdb
import pytest

from app.sqlgen import build_distinct_sql
from app.sqlgen_glot import SQLGlotBuilder, validate_sql

TOL = 1e-9


# ---------------------------------------------------------------------------
# Fixture: a deterministic DuckDB table `t` (~52 rows) covering a date column,
# a numeric column, a text column with quotes + NULLs, and a column whose name
# needs quoting ("Order Count").
# ---------------------------------------------------------------------------
def _make_rows():
    rows = []
    cats = ["north", "south", "O'Brien", None, "east"]
    base = _dt.date(2023, 1, 5)
    for i in range(52):
        d = base + _dt.timedelta(days=i * 11)  # spread across ~1.5 years
        amount = round(10.0 + (i % 7) * 3.5, 2)
        cat = cats[i % len(cats)]
        order_count = (i % 5) + 1
        rows.append((d, amount, cat, order_count))
    return rows


@pytest.fixture()
def conn():
    c = duckdb.connect(":memory:")
    c.execute(
        'CREATE TABLE t ('
        '  d DATE, amount DOUBLE, category VARCHAR, "Order Count" INTEGER'
        ')'
    )
    c.executemany('INSERT INTO t VALUES (?, ?, ?, ?)', _make_rows())
    yield c
    c.close()


def _norm(rows):
    """Normalize a result set for order-insensitive, type-tolerant compare."""
    out = []
    for row in rows:
        cells = []
        for v in row:
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                cells.append(("num", round(float(v), 9)))
            elif v is None:
                cells.append(("none", None))
            else:
                cells.append(("str", str(v)))
        out.append(tuple(cells))
    return sorted(out, key=lambda r: [str(c) for c in r])


def _assert_same(conn, sqlglot_sql, reference_sql):
    got = _norm(conn.execute(sqlglot_sql).fetchall())
    exp = _norm(conn.execute(reference_sql).fetchall())
    assert got == exp, (
        f"result-set mismatch\nSQLGlot: {sqlglot_sql}\n"
        f"reference: {reference_sql}\ngot={got}\nexp={exp}"
    )


# ---------------------------------------------------------------------------
# Aggregation matrix: SQLGlot output vs an independent DuckDB reference query.
# ---------------------------------------------------------------------------
AGG_REF = {
    "sum": "SUM(amount)",
    "avg": "AVG(amount)",
    "min": "MIN(amount)",
    "max": "MAX(amount)",
}


@pytest.mark.parametrize("agg", ["sum", "avg", "min", "max"])
def test_agg_by_category(conn, agg):
    b = SQLGlotBuilder("duckdb")
    sql = b.build_aggregation_query(
        source="t", x_field="category", y_field="amount", agg=agg, group_by="none"
    )
    ref = f'SELECT category AS x, {AGG_REF[agg]} AS value FROM t GROUP BY 1'
    _assert_same(conn, sql, ref)


def test_agg_count_star(conn):
    b = SQLGlotBuilder("duckdb")
    sql = b.build_aggregation_query(
        source="t", x_field="category", agg="count", group_by="none"
    )
    ref = 'SELECT category AS x, COUNT(*) AS value FROM t GROUP BY 1'
    _assert_same(conn, sql, ref)


def test_agg_distinct_count(conn):
    b = SQLGlotBuilder("duckdb")
    sql = b.build_aggregation_query(
        source="t", x_field="category", y_field="Order Count",
        agg="distinct", group_by="none",
    )
    ref = 'SELECT category AS x, COUNT(DISTINCT "Order Count") AS value FROM t GROUP BY 1'
    _assert_same(conn, sql, ref)


def test_agg_with_legend(conn):
    b = SQLGlotBuilder("duckdb")
    sql = b.build_aggregation_query(
        source="t", x_field="category", y_field="amount",
        legend_field="Order Count", agg="sum", group_by="none",
    )
    ref = ('SELECT category AS x, "Order Count" AS legend, SUM(amount) AS value '
           'FROM t GROUP BY 1, 2')
    _assert_same(conn, sql, ref)


@pytest.mark.parametrize("bucket,trunc", [
    ("day", "day"),
    ("month", "month"),
    ("quarter", "quarter"),
    ("year", "year"),
])
def test_agg_time_bucketing(conn, bucket, trunc):
    b = SQLGlotBuilder("duckdb")
    sql = b.build_aggregation_query(
        source="t", x_field="d", y_field="amount", agg="sum", group_by=bucket,
    )
    ref = f"SELECT DATE_TRUNC('{trunc}', d) AS x, SUM(amount) AS value FROM t GROUP BY 1"
    _assert_same(conn, sql, ref)


def test_agg_week_sum_invariant(conn):
    """Week bucketing (mon/sun) partitions rows -- bucket boundaries differ by
    week_start, but the total across buckets must equal the grand total."""
    grand = conn.execute("SELECT SUM(amount) FROM t").fetchone()[0]
    for ws in ("mon", "sun"):
        b = SQLGlotBuilder("duckdb")
        sql = b.build_aggregation_query(
            source="t", x_field="d", y_field="amount", agg="sum",
            group_by="week", week_start=ws,
        )
        total = sum(r[1] for r in conn.execute(sql).fetchall())
        assert abs(total - grand) < 1e-6


def test_where_scalar_eq(conn):
    b = SQLGlotBuilder("duckdb")
    sql = b.build_aggregation_query(
        source="t", x_field="category", y_field="amount", agg="sum",
        group_by="none", where={"category": "north"},
    )
    ref = ("SELECT category AS x, SUM(amount) AS value FROM t "
           "WHERE category = 'north' GROUP BY 1")
    _assert_same(conn, sql, ref)


def test_where_in_list(conn):
    b = SQLGlotBuilder("duckdb")
    sql = b.build_aggregation_query(
        source="t", x_field="category", y_field="amount", agg="sum",
        group_by="none", where={"category": ["north", "south"]},
    )
    ref = ("SELECT category AS x, SUM(amount) AS value FROM t "
           "WHERE category IN ('north', 'south') GROUP BY 1")
    _assert_same(conn, sql, ref)


def test_where_range(conn):
    b = SQLGlotBuilder("duckdb")
    sql = b.build_aggregation_query(
        source="t", x_field="category", y_field="amount", agg="sum",
        group_by="none", where={"amount__gte": 15, "amount__lt": 25},
    )
    ref = ("SELECT category AS x, SUM(amount) AS value FROM t "
           "WHERE amount >= 15 AND amount < 25 GROUP BY 1")
    _assert_same(conn, sql, ref)


def test_where_quoted_column(conn):
    b = SQLGlotBuilder("duckdb")
    sql = b.build_aggregation_query(
        source="t", x_field="category", y_field="amount", agg="sum",
        group_by="none", where={"Order Count": [1, 2]},
    )
    ref = ('SELECT category AS x, SUM(amount) AS value FROM t '
           'WHERE "Order Count" IN (1, 2) GROUP BY 1')
    _assert_same(conn, sql, ref)


@pytest.mark.parametrize("order,direction", [("value", "desc"), ("value", "asc"), ("x", "asc")])
def test_order_and_limit(conn, order, direction):
    b = SQLGlotBuilder("duckdb")
    sql = b.build_aggregation_query(
        source="t", x_field="category", y_field="amount", agg="sum",
        group_by="none", order_by=order, order=direction, limit=3,
    )
    col = "value" if order == "value" else "x"
    ref = (f"SELECT category AS x, SUM(amount) AS value FROM t GROUP BY 1 "
           f"ORDER BY {col} {direction} LIMIT 3")
    got = conn.execute(sql).fetchall()
    exp = conn.execute(ref).fetchall()
    assert len(got) == len(exp) == 3
    # Compare the ordered VALUE column: deterministic even when group sums tie
    # (which row wins a tie at the LIMIT boundary is not, so don't compare x).
    got_vals = [round(float(r[1]), 9) for r in got]
    exp_vals = [round(float(r[1]), 9) for r in exp]
    assert got_vals == exp_vals


def test_expr_map_in_x(conn):
    """Custom column referenced as X: emulate a derived 'Year' bucket."""
    b = SQLGlotBuilder("duckdb")
    expr_map = {"Yr": 'EXTRACT(year FROM "d")'}
    sql = b.build_aggregation_query(
        source="t", x_field="Yr", y_field="amount", agg="sum",
        group_by="none", expr_map=expr_map,
    )
    ref = 'SELECT EXTRACT(year FROM d) AS x, SUM(amount) AS value FROM t GROUP BY 1'
    _assert_same(conn, sql, ref)


def test_expr_map_in_y(conn):
    b = SQLGlotBuilder("duckdb")
    expr_map = {"Doubled": '"amount" * 2'}
    sql = b.build_aggregation_query(
        source="t", x_field="category", y_field="Doubled", agg="sum",
        group_by="none", expr_map=expr_map,
    )
    ref = 'SELECT category AS x, SUM(amount * 2) AS value FROM t GROUP BY 1'
    _assert_same(conn, sql, ref)


# ---------------------------------------------------------------------------
# DISTINCT parity: legacy sqlgen.build_distinct_sql vs SQLGlot build_distinct_query.
# This is a true legacy-vs-SQLGlot equivalence (same shape in both builders).
# ---------------------------------------------------------------------------
def _exec_legacy(conn, sql, params):
    # Legacy emits SQLAlchemy-style ":name"; DuckDB uses "$name".
    duck_sql = sql
    duck_params = {}
    for k, v in (params or {}).items():
        duck_sql = duck_sql.replace(f":{k}", f"${k}")
        duck_params[k] = v
    if duck_params:
        return conn.execute(duck_sql, duck_params).fetchall()
    return conn.execute(duck_sql).fetchall()


@pytest.mark.parametrize("field", ["category", "Order Count"])
def test_distinct_plain(conn, field):
    legacy_sql, params = build_distinct_sql(
        dialect="duckdb", source="t", field=field, where=None
    )
    glot_sql = SQLGlotBuilder("duckdb").build_distinct_query(source="t", field=field)
    legacy = _norm(_exec_legacy(conn, legacy_sql, params))
    glot = _norm(conn.execute(glot_sql).fetchall())
    assert legacy == glot


def test_distinct_derived_year(conn):
    """Derived date-part field 'd (Year)' -- both builders extract the year."""
    field = "d (Year)"
    legacy_sql, params = build_distinct_sql(
        dialect="duckdb", source="t", field=field, where=None
    )
    glot_sql = SQLGlotBuilder("duckdb").build_distinct_query(source="t", field=field)
    legacy = _norm(_exec_legacy(conn, legacy_sql, params))
    glot = _norm(conn.execute(glot_sql).fetchall())
    assert legacy == glot


def test_distinct_with_where(conn):
    where = {"category": ["north", "south"]}
    legacy_sql, params = build_distinct_sql(
        dialect="duckdb", source="t", field="Order Count", where=where
    )
    glot_sql = SQLGlotBuilder("duckdb").build_distinct_query(
        source="t", field="Order Count", where=where
    )
    legacy = _norm(_exec_legacy(conn, legacy_sql, params))
    glot = _norm(conn.execute(glot_sql).fetchall())
    assert legacy == glot


# ---------------------------------------------------------------------------
# Cross-dialect: no live DB, so assert the generated SQL parses (validate_sql).
# ---------------------------------------------------------------------------
_CROSS = [
    pytest.param(d, b, marks=pytest.mark.xfail(
        reason="known SQLGlotBuilder gap: tsql day-bucket emits an unbound DType "
               "node (follow-up); month/quarter/year tsql are fine",
        strict=True,
    ) if (d, b) == ("mssql", "day") else [])
    for d in ("postgres", "mysql", "mssql")
    for b in ("day", "month", "quarter", "year")
]


@pytest.mark.parametrize("dialect,bucket", _CROSS)
def test_cross_dialect_parses(dialect, bucket):
    b = SQLGlotBuilder(dialect)
    sql = b.build_aggregation_query(
        source="t", x_field="d", y_field="amount", agg="sum", group_by=bucket,
        where={"category": ["north"], "amount__gte": 10},
        order_by="value", order="desc", limit=10,
    )
    ok, err = validate_sql(sql, b.dialect)
    assert ok, f"{dialect} SQL failed to parse: {err}\n{sql}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
