# SQLGlot Integration Plan for Bayan Dashboard Builder

## Executive Summary
Migrate from manual SQL string building to SQLGlot for consistent, reliable, multi-dialect SQL generation across DuckDB, PostgreSQL, MySQL, MSSQL, and SQLite.

## Goals
- ✅ **Reduce bugs** from manual SQL string building
- ✅ **Eliminate inconsistencies** across database dialects
- ✅ **Improve maintainability** - reduce 500+ lines of SQL building to <100 lines
- ✅ **Add safety** - catch SQL errors at build time, not runtime
- ✅ **Enable flexibility** - easy to add new aggregations, transforms, filters

---

## Phase 1: Foundation (Week 1)

### 1.1 Setup & Dependencies
**File**: `backend/requirements.txt`
```python
sqlglot>=25.0.0
```

**Install**:
```bash
cd backend
pip install sqlglot==25.0.0
```

### 1.2 Create SQLGlot Utility Module
**File**: `backend/app/sqlgen_v2.py`

```python
"""
SQLGlot-based SQL generation for multi-dialect support.
Replaces manual SQL string building with type-safe query construction.
"""
from typing import Any, Dict, List, Optional, Union
import sqlglot
from sqlglot import exp, parse_one, select
from sqlglot.expressions import Expression

class QueryBuilder:
    """Build SQL queries using SQLGlot for multi-dialect support"""
    
    def __init__(self, dialect: str = "duckdb"):
        """
        Initialize query builder.
        
        Args:
            dialect: Target SQL dialect (duckdb, postgres, mysql, mssql, sqlite)
        """
        self.dialect = self._normalize_dialect(dialect)
    
    def _normalize_dialect(self, dialect: str) -> str:
        """Normalize dialect names to SQLGlot format"""
        mapping = {
            "duckdb": "duckdb",
            "postgres": "postgres",
            "postgresql": "postgres",
            "postgre": "postgres",
            "mysql": "mysql",
            "mssql": "tsql",
            "sqlserver": "tsql",
            "sqlite": "sqlite",
        }
        return mapping.get(dialect.lower(), "duckdb")
    
    def build_aggregation_query(
        self,
        source: str,
        x_field: Optional[str] = None,
        y_field: Optional[str] = None,
        legend_field: Optional[str] = None,
        agg: str = "sum",
        where: Optional[Dict[str, Any]] = None,
        group_by: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> str:
        """
        Build aggregation query with multi-dialect support.
        
        Args:
            source: Table name
            x_field: X-axis field (dimension)
            y_field: Y-axis field (measure)
            legend_field: Legend/category field
            agg: Aggregation function (sum, count, avg, min, max)
            where: Filter conditions
            group_by: Time grouping (day, week, month, quarter, year)
            order_by: Ordering field
            limit: Result limit
            
        Returns:
            SQL string for target dialect
        """
        # Start with base table
        query = select("*").from_(source)
        
        # Build SELECT clause
        select_exprs = []
        
        if x_field:
            if group_by and group_by != "none":
                # Apply time bucketing
                x_expr = self._build_time_bucket(x_field, group_by)
            else:
                x_expr = exp.column(x_field)
            select_exprs.append(x_expr.as_("x"))
        
        if legend_field:
            select_exprs.append(exp.column(legend_field).as_("legend"))
        
        # Build aggregation
        if agg == "count":
            agg_expr = exp.Count(this=exp.Star())
        elif agg == "distinct" and y_field:
            agg_expr = exp.Count(this=exp.column(y_field), distinct=True)
        elif agg in ("sum", "avg", "min", "max") and y_field:
            agg_func = {
                "sum": exp.Sum,
                "avg": exp.Avg,
                "min": exp.Min,
                "max": exp.Max,
            }[agg]
            agg_expr = agg_func(this=exp.column(y_field))
        else:
            agg_expr = exp.Count(this=exp.Star())
        
        select_exprs.append(agg_expr.as_("value"))
        
        # Apply SELECT
        query = query.select(*select_exprs, append=False)
        
        # Apply WHERE clauses
        if where:
            for key, value in where.items():
                if key in ("start", "startDate", "end", "endDate"):
                    continue
                    
                if isinstance(value, list):
                    if len(value) > 0:
                        query = query.where(exp.column(key).isin(*[exp.Literal.string(str(v)) for v in value]))
                elif value is not None:
                    query = query.where(exp.column(key).eq(exp.Literal.string(str(value))))
        
        # Apply GROUP BY
        group_by_cols = []
        if x_field:
            group_by_cols.append(1)  # Position 1 (x)
        if legend_field:
            group_by_cols.append(2)  # Position 2 (legend)
        
        if group_by_cols:
            query = query.group_by(*[exp.Literal.number(i) for i in group_by_cols])
        
        # Apply ORDER BY
        if order_by:
            query = query.order_by(order_by)
        elif group_by_cols:
            query = query.order_by(*[exp.Literal.number(i) for i in group_by_cols])
        
        # Apply LIMIT
        if limit:
            query = query.limit(limit)
        
        # Generate SQL for target dialect
        return query.sql(dialect=self.dialect, pretty=False)
    
    def _build_time_bucket(self, field: str, group_by: str) -> Expression:
        """Build time bucketing expression for the target dialect"""
        col = exp.column(field)
        
        if self.dialect == "duckdb":
            return exp.func("DATE_TRUNC", exp.Literal.string(group_by), col)
        elif self.dialect == "postgres":
            return exp.func("date_trunc", exp.Literal.string(group_by), col)
        elif self.dialect == "tsql":  # MSSQL
            if group_by == "day":
                return exp.Cast(this=col, to=exp.DataType.Type.DATE)
            elif group_by == "month":
                return exp.func("DATEFROMPARTS", 
                    exp.func("YEAR", col),
                    exp.func("MONTH", col),
                    exp.Literal.number(1))
            # Add more MSSQL groupings as needed
        elif self.dialect == "mysql":
            if group_by == "day":
                return exp.func("DATE", col)
            elif group_by == "month":
                return exp.func("DATE_FORMAT", col, exp.Literal.string("%Y-%m-01"))
        
        # Fallback
        return col


def transpile_sql(sql: str, source_dialect: str = "duckdb", target_dialect: str = None) -> str:
    """
    Transpile SQL from one dialect to another.
    
    Args:
        sql: Source SQL string
        source_dialect: Source dialect (duckdb, postgres, mysql, etc.)
        target_dialect: Target dialect (if None, validates and reformats only)
    
    Returns:
        Transpiled SQL string
        
    Example:
        >>> sql = "SELECT DATE_TRUNC('month', date) FROM table"
        >>> transpile_sql(sql, "duckdb", "postgres")
        'SELECT DATE_TRUNC(\\'month\\', date) FROM table'
    """
    try:
        if target_dialect is None:
            target_dialect = source_dialect
            
        # Parse and transpile
        return sqlglot.transpile(sql, read=source_dialect, write=target_dialect)[0]
    except Exception as e:
        # Fallback to original SQL if transpilation fails
        print(f"[SQLGlot] Transpilation failed: {e}")
        return sql


def validate_sql(sql: str, dialect: str = "duckdb") -> tuple[bool, Optional[str]]:
    """
    Validate SQL syntax for a given dialect.
    
    Args:
        sql: SQL string to validate
        dialect: Target dialect
        
    Returns:
        (is_valid, error_message)
        
    Example:
        >>> validate_sql("SELECT * FROM table WHERE id = ", "duckdb")
        (False, "Syntax error at position 35")
    """
    try:
        sqlglot.parse_one(sql, dialect=dialect)
        return (True, None)
    except sqlglot.errors.ParseError as e:
        return (False, str(e))
```

---

## Phase 2: Integration (Week 2)

### 2.1 Update `query.py` to Use SQLGlot

**File**: `backend/app/routers/query.py`

Add import at top:
```python
from ..sqlgen_v2 import QueryBuilder, transpile_sql, validate_sql
```

### 2.2 Replace Manual SQL Building

**Before** (lines 3297-3306):
```python
if ("mssql" in ds_type) or ("sqlserver" in ds_type):
    sql_inner = (
        f"SELECT {x_expr} as x, {legend_expr} as legend, {value_expr} as value "
        f"{base_from_sql}{where_sql} GROUP BY {x_expr}, {legend_expr}{order_seg_mssql}"
    )
else:
    sql_inner = (
        f"SELECT {x_expr} as x, {legend_expr} as legend, {value_expr} as value "
        f"{base_from_sql}{where_sql} GROUP BY 1,2{order_seg_std}"
    )
```

**After**:
```python
builder = QueryBuilder(dialect=ds_type)
sql_inner = builder.build_aggregation_query(
    source=spec.source,
    x_field=x_col,
    y_field=spec.y,
    legend_field=spec.legend,
    agg=agg,
    where=spec.where,
    group_by=spec.groupBy,
    order_by=spec.orderBy,
    limit=lim,
)
```

### 2.3 Add Logging & Validation

```python
# Validate SQL before execution
is_valid, error = validate_sql(sql_inner, ds_type)
if not is_valid:
    print(f"[SQLGlot] Invalid SQL generated: {error}")
    print(f"[SQLGlot] SQL: {sql_inner}")
    # Fallback to manual building or raise error

print(f"[SQLGlot] Generated SQL for {ds_type}: {sql_inner[:200]}")
```

---

## Phase 3: Testing (Week 3)

### 3.1 Unit Tests
**File**: `backend/tests/test_sqlgen_v2.py`

```python
import pytest
from app.sqlgen_v2 import QueryBuilder, transpile_sql, validate_sql


class TestQueryBuilder:
    def test_simple_aggregation_duckdb(self):
        builder = QueryBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="sales",
            x_field="date",
            y_field="amount",
            agg="sum",
            group_by="month",
        )
        assert "DATE_TRUNC" in sql
        assert "SUM" in sql
        assert "GROUP BY" in sql
    
    def test_aggregation_with_legend(self):
        builder = QueryBuilder("postgres")
        sql = builder.build_aggregation_query(
            source="orders",
            x_field="order_date",
            y_field="total",
            legend_field="category",
            agg="sum",
        )
        assert "category" in sql.lower()
        assert "GROUP BY" in sql
    
    def test_where_clause_list(self):
        builder = QueryBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="products",
            y_field="price",
            agg="avg",
            where={"status": ["active", "pending"]},
        )
        assert "IN" in sql
    
    def test_count_aggregation(self):
        builder = QueryBuilder("mysql")
        sql = builder.build_aggregation_query(
            source="users",
            agg="count",
        )
        assert "COUNT(*)" in sql


class TestTranspile:
    def test_duckdb_to_postgres(self):
        sql = "SELECT DATE_TRUNC('month', date) FROM sales"
        result = transpile_sql(sql, "duckdb", "postgres")
        assert result is not None
    
    def test_validation_valid(self):
        sql = "SELECT * FROM table WHERE id = 1"
        is_valid, error = validate_sql(sql, "duckdb")
        assert is_valid
        assert error is None
    
    def test_validation_invalid(self):
        sql = "SELECT * FROM WHERE"
        is_valid, error = validate_sql(sql, "duckdb")
        assert not is_valid
        assert error is not None
```

### 3.2 Integration Tests

Create test cases for each dialect:
- ✅ DuckDB (primary)
- ✅ PostgreSQL
- ✅ MySQL
- ✅ MSSQL
- ✅ SQLite

Compare outputs:
1. Old manual SQL vs SQLGlot SQL
2. Verify row counts match
3. Verify aggregation values match

---

## Phase 4: Rollout (Week 4)

### 4.1 Feature Flag
Add environment variable for gradual rollout:

```python
# backend/app/config.py
USE_SQLGLOT = os.getenv("USE_SQLGLOT", "false").lower() == "true"
```

```python
# backend/app/routers/query.py
from ..config import settings

if settings.USE_SQLGLOT:
    # Use SQLGlot path
    builder = QueryBuilder(dialect=ds_type)
    sql = builder.build_aggregation_query(...)
else:
    # Use legacy path
    sql = f"SELECT {x_expr}..."
```

### 4.2 Rollout Stages

**Stage 1: Development** (Day 1-2)
- Enable for dev environment only
- Test all chart types
- Fix any edge cases

**Stage 2: Staging** (Day 3-5)
- Enable for staging with A/B testing
- Monitor error rates
- Compare query performance

**Stage 3: Production** (Day 6-7)
- Gradual rollout: 10% → 50% → 100% traffic
- Monitor dashboards for issues
- Keep legacy fallback active

---

## Phase 5: Cleanup (Week 5)

### 5.1 Remove Legacy Code
Once SQLGlot is stable in production:

```python
# Delete manual SQL building functions (lines 2848-3320 in query.py)
# Keep only SQLGlot path
```

### 5.2 Update Documentation
- Document new query builder API
- Add examples for common patterns
- Create migration guide for custom SQL

---

## Benefits After Migration

### Before:
- ❌ 500+ lines of manual SQL building
- ❌ Separate code paths for each dialect
- ❌ Hard to test SQL correctness
- ❌ Easy to miss edge cases
- ❌ SQL injection risks

### After:
- ✅ ~100 lines using SQLGlot
- ✅ Single code path for all dialects
- ✅ Type-safe query building
- ✅ Auto-validated SQL
- ✅ Built-in SQL injection protection

---

## Risk Mitigation

1. **Performance**: SQLGlot adds minimal overhead (~1-2ms per query)
2. **Compatibility**: Keep fallback to legacy SQL if SQLGlot fails
3. **Testing**: Comprehensive test suite before rollout
4. **Monitoring**: Track query errors and performance metrics
5. **Rollback**: Feature flag allows instant rollback

---

## Metrics to Track

- Query generation time (before/after)
- SQL error rates
- Query execution time
- Number of unique SQL patterns
- Code coverage for query builder

---

## Timeline

| Week | Phase | Deliverable |
|------|-------|-------------|
| 1 | Foundation | SQLGlot installed, `sqlgen_v2.py` created |
| 2 | Integration | `/query/spec` endpoint using SQLGlot |
| 3 | Testing | All tests passing, edge cases covered |
| 4 | Rollout | Production deployment with feature flag |
| 5 | Cleanup | Legacy code removed, docs updated |

---

## Next Steps

1. ✅ Fix TypeScript build errors (DONE)
2. ⏳ Review and approve this plan
3. ⏳ Install SQLGlot: `pip install sqlglot`
4. ⏳ Create `sqlgen_v2.py` with QueryBuilder
5. ⏳ Write unit tests
6. ⏳ Integrate with one endpoint
7. ⏳ Test and iterate
8. ⏳ Full rollout

---

## Questions to Answer

1. Do you want to support all 5 dialects initially or start with DuckDB + PostgreSQL?
2. Should we add query optimization features (e.g., predicate pushdown)?
3. Do you want SQL query caching to improve performance?
4. Should we log all generated SQL for debugging?

---

**Ready to proceed with Phase 1?** Let me know if you want me to start creating the `sqlgen_v2.py` file.
