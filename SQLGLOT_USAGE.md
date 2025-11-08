# SQLGlot Dual-Mode Implementation - Usage Guide

## Overview

SQLGlot is now integrated into the backend in **dual-mode**: it runs side-by-side with the legacy SQL builder, controlled by feature flags.

---

## Installation

### 1. Install Dependencies

```bash
cd backend
pip install -r requirements.txt
```

This will install `sqlglot>=25.0.0` along with other dependencies.

### 2. Verify Installation

```bash
python -c "import sqlglot; print(f'SQLGlot version: {sqlglot.__version__}')"
```

---

## Configuration

### Environment Variables

Add to `backend/.env`:

```bash
# Enable/disable SQLGlot globally
ENABLE_SQLGLOT=false

# Control which users use SQLGlot
# Options:
#   "" (empty) = disabled for all users
#   "*" = enabled for all users
#   "user1,user2" = enabled only for specific user IDs
SQLGLOT_USERS=
```

---

## Usage

### Enable SQLGlot for Testing

**Option 1: Enable for yourself only**

```bash
# In backend/.env
ENABLE_SQLGLOT=true
SQLGLOT_USERS=your_user_id
```

**Option 2: Enable for all users**

```bash
# In backend/.env
ENABLE_SQLGLOT=true
SQLGLOT_USERS=*
```

**Option 3: Enable for specific users**

```bash
# In backend/.env
ENABLE_SQLGLOT=true
SQLGLOT_USERS=user1@example.com,user2@example.com
```

### Restart Backend

```bash
cd backend
# If using uvicorn
uvicorn app.main:app --reload

# Or if using your start script
./start.sh
```

---

## How It Works

### Request Flow

```
Frontend sends query
       ↓
Backend receives /api/query/spec
       ↓
Check should_use_sqlglot(user_id)
       ↓
   ┌───┴───┐
   ↓       ↓
SQLGlot  Legacy
   ↓       ↓
   └───┬───┘
       ↓
   Same output format
```

### Code Path

When `ENABLE_SQLGLOT=true`:

1. **Check user**: `should_use_sqlglot(actorId)` validates if this user should use SQLGlot
2. **Generate SQL**: `SQLGlotBuilder.build_aggregation_query(...)` creates SQL using SQLGlot
3. **Execute**: Same `run_query()` path as legacy
4. **Fallback**: If SQLGlot fails, automatically falls back to legacy builder

### Metrics

The implementation tracks:

- `sqlglot_queries_total`: Successful SQLGlot queries
- `legacy_queries_total`: Legacy SQL builder queries
- `sqlglot_errors_total`: SQLGlot failures (with fallback)

---

## Testing

### Run Unit Tests

```bash
cd backend
pytest tests/test_sqlglot_builder.py -v
```

### Manual Testing

1. **Enable SQLGlot for your user**
2. **Open a dashboard** with charts
3. **Check backend logs** for:
   ```
   [SQLGlot] ENABLED for user=...
   [SQLGlot] Generated SQL (duckdb): SELECT ...
   ```

4. **Compare outputs** - should match legacy exactly

### Verify Fallback

1. **Trigger an error** (e.g., invalid configuration)
2. **Check logs** for:
   ```
   [SQLGlot] ERROR: ..., falling back to legacy SQL builder
   ```
3. **Verify** query still works (using legacy)

---

## Comparing SQLGlot vs Legacy

### View Generated SQL

Both paths log their generated SQL:

```python
# SQLGlot
[SQLGlot] Generated SQL (duckdb): SELECT DATE_TRUNC('month', date) AS x, ...

# Legacy
[BACKEND] Built SQL with agg=sum, value_expr=SUM(amount), sql_inner=SELECT ...
```

### Expected Differences

SQLGlot may generate slightly different but equivalent SQL:

**Legacy:**
```sql
SELECT DATE_TRUNC('month', date) as x, category as legend, SUM(amount) as value
FROM sales WHERE status = 'completed' GROUP BY 1,2 ORDER BY 1,2 LIMIT 1000
```

**SQLGlot:**
```sql
SELECT DATE_TRUNC('month', date) AS x, category AS legend, SUM(amount) AS value 
FROM sales WHERE status = 'completed' GROUP BY 1, 2 ORDER BY 1, 2 LIMIT 1000
```

Differences:
- Capitalization (AS vs as)
- Spacing (GROUP BY 1,2 vs GROUP BY 1, 2)

**Both produce identical results.**

---

## Supported Features

### ✅ Currently Supported

- **Aggregations**: SUM, COUNT, AVG, MIN, MAX, DISTINCT
- **Time Bucketing**: day, week, month, quarter, year
- **Legend/Categories**: Multi-series charts
- **WHERE Clauses**: Single values, lists (IN), NULL checks
- **ORDER BY**: By field or value
- **LIMIT/OFFSET**: Pagination
- **Dialects**: DuckDB, PostgreSQL, MySQL, MSSQL, SQLite

### ⏳ Not Yet Implemented

- Custom measures (using `spec.measure`)
- Derived fields (e.g., "Date (Year)")
- Complex transformations
- Joins
- Window functions
- CTEs

These will fall back to legacy builder.

---

## Troubleshooting

### SQLGlot Not Running

**Check:**
1. `ENABLE_SQLGLOT=true` in `.env`
2. Your user ID is in `SQLGLOT_USERS` (or set to `*`)
3. Backend was restarted after changing `.env`

**Verify:**
```bash
# Check logs when making a query
tail -f backend/logs/app.log | grep SQLGlot
```

### SQLGlot Errors

**Common issues:**

1. **Invalid dialect**: Check `ds_type` is supported
2. **Missing field**: `x_field`, `y_field`, etc. may be None
3. **Complex query**: Falls back to legacy automatically

**Solution:**
SQLGlot automatically falls back to legacy. Check logs for the error:
```
[SQLGlot] ERROR: ..., falling back to legacy SQL builder
```

### Different Results

If SQLGlot produces different results than legacy:

1. **Capture both SQL queries** from logs
2. **Run manually** in your database
3. **Compare row counts** and values
4. **Report the issue** with SQL samples

---

## Gradual Rollout Strategy

### Phase 1: Development (Now)

```bash
ENABLE_SQLGLOT=true
SQLGLOT_USERS=your_dev_user_id
```

- Test all chart types
- Verify outputs match legacy
- Fix any bugs

### Phase 2: Staging

```bash
ENABLE_SQLGLOT=true
SQLGLOT_USERS=*
```

- Enable for all staging users
- Monitor error rates
- A/B test performance

### Phase 3: Production (10%)

```bash
ENABLE_SQLGLOT=true
SQLGLOT_USERS=user1,user2,user3,...
```

- Enable for 10% of users
- Monitor dashboards
- Increase gradually: 25% → 50% → 100%

### Phase 4: Full Rollout

```bash
ENABLE_SQLGLOT=true
SQLGLOT_USERS=*
```

- All users use SQLGlot
- Legacy still available as fallback
- Monitor for 30 days

### Phase 5: Deprecate Legacy

After SQLGlot is stable:
- Remove legacy SQL building code
- Keep only SQLGlot path
- Update documentation

---

## API Reference

### SQLGlotBuilder

```python
from app.sqlgen_glot import SQLGlotBuilder

builder = SQLGlotBuilder(dialect="duckdb")

sql = builder.build_aggregation_query(
    source="sales",              # Table name
    x_field="date",              # X-axis field
    y_field="amount",            # Y-axis field (measure)
    legend_field="category",     # Legend field (optional)
    agg="sum",                   # Aggregation (sum, count, avg, etc.)
    where={"status": "active"},  # Filter conditions
    group_by="month",            # Time bucketing (day, week, month, etc.)
    order_by="value",            # Order by field
    order="desc",                # Sort direction
    limit=1000,                  # Result limit
    week_start="mon",            # Week start day
)
```

### should_use_sqlglot

```python
from app.sqlgen_glot import should_use_sqlglot

# Check if SQLGlot should be used for this user
use_it = should_use_sqlglot(user_id="user@example.com")
```

### validate_sql

```python
from app.sqlgen_glot import validate_sql

# Validate SQL syntax
is_valid, error = validate_sql("SELECT * FROM table", dialect="duckdb")
if not is_valid:
    print(f"SQL Error: {error}")
```

---

## Next Steps

1. ✅ **Install dependencies**: `pip install -r requirements.txt`
2. ✅ **Configure flags**: Set `ENABLE_SQLGLOT` and `SQLGLOT_USERS`
3. ✅ **Restart backend**: Apply configuration
4. ⏳ **Test manually**: Create charts, verify outputs
5. ⏳ **Run tests**: `pytest tests/test_sqlglot_builder.py`
6. ⏳ **Monitor logs**: Watch for SQLGlot activity
7. ⏳ **Gradual rollout**: Increase user percentage

---

## Support

For issues or questions:
- Check logs: `tail -f backend/logs/app.log`
- Run tests: `pytest tests/test_sqlglot_builder.py -v`
- Review plan: `SQLGLOT_DUAL_MODE_PLAN.md`

---

**Status**: ✅ Phase 1 Complete (Backend Infrastructure)
**Next**: Phase 2 (Frontend Toggle - Optional)
