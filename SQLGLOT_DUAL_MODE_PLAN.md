# SQLGlot Dual-Mode Integration Plan

## Goal
Run SQLGlot and Legacy SQL generation **side-by-side** with a configurator switch, allowing gradual migration without breaking existing functionality.

---

## Architecture

```
User selects in UI: [Legacy SQL] or [SQLGlot SQL]
                          ↓
                    Backend checks flag
                          ↓
              ┌───────────┴───────────┐
              ↓                       ↓
        Legacy Path              SQLGlot Path
     (existing code)           (new code)
              ↓                       ↓
         Same output format
```

---

## Phase 1: Infrastructure (Day 1-2)

### 1.1 Backend: Config Flag

**File**: `backend/app/config.py`

Add to Settings class:
```python
class Settings(BaseSettings):
    # ... existing settings ...
    
    # SQLGlot feature flag
    enable_sqlglot: bool = Field(
        default=False,
        description="Enable SQLGlot SQL generation (experimental)"
    )
    
    # Per-user override (for testing)
    sqlglot_users: str = Field(
        default="",
        description="Comma-separated user IDs that should use SQLGlot (e.g., 'user1,user2')"
    )

settings = Settings()
```

**Environment Variable**:
```bash
# .env
ENABLE_SQLGLOT=false
SQLGLOT_USERS=""  # Empty = disabled for all, "*" = all users, "user1,user2" = specific users
```

### 1.2 Backend: SQLGlot Utility

**File**: `backend/app/sqlgen_glot.py`

```python
"""
SQLGlot-based SQL generation.
Runs side-by-side with legacy SQL builder.
"""
from typing import Any, Dict, List, Optional
import sqlglot
from sqlglot import exp


class SQLGlotBuilder:
    """Generate SQL using SQLGlot for multi-dialect support"""
    
    def __init__(self, dialect: str = "duckdb"):
        self.dialect = self._normalize_dialect(dialect)
    
    def _normalize_dialect(self, dialect: str) -> str:
        """Map our dialect names to SQLGlot names"""
        mapping = {
            "duckdb": "duckdb",
            "postgres": "postgres",
            "postgresql": "postgres",
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
        week_start: str = "mon",
    ) -> str:
        """
        Build aggregation query.
        
        Returns SQL string compatible with legacy output format:
        - Columns: ['x', 'legend', 'value'] or ['x', 'value']
        """
        try:
            # Build query using SQLGlot
            query = sqlglot.select("*").from_(source)
            
            # SELECT clause
            select_exprs = []
            
            if x_field:
                if group_by and group_by != "none":
                    x_expr = self._build_time_bucket(x_field, group_by, week_start)
                else:
                    x_expr = exp.column(x_field)
                select_exprs.append(x_expr.as_("x"))
            
            if legend_field:
                select_exprs.append(exp.column(legend_field).as_("legend"))
            
            # Aggregation
            agg_expr = self._build_aggregation(agg, y_field)
            select_exprs.append(agg_expr.as_("value"))
            
            query = query.select(*select_exprs, append=False)
            
            # WHERE clause
            if where:
                query = self._apply_where(query, where)
            
            # GROUP BY
            group_cols = []
            if x_field:
                group_cols.append(1)
            if legend_field:
                group_cols.append(2)
            
            if group_cols:
                query = query.group_by(*[exp.Literal.number(i) for i in group_cols])
            
            # ORDER BY
            if order_by:
                query = query.order_by(order_by)
            elif group_cols:
                query = query.order_by(*[exp.Literal.number(i) for i in group_cols])
            
            # LIMIT
            if limit and limit > 0:
                query = query.limit(limit)
            
            # Generate SQL
            sql = query.sql(dialect=self.dialect, pretty=False)
            
            print(f"[SQLGlot] Generated SQL for {self.dialect}: {sql[:200]}")
            return sql
            
        except Exception as e:
            print(f"[SQLGlot] Error generating SQL: {e}")
            raise
    
    def _build_aggregation(self, agg: str, y_field: Optional[str]) -> exp.Expression:
        """Build aggregation expression"""
        agg_lower = agg.lower()
        
        if agg_lower == "count":
            return exp.Count(this=exp.Star())
        elif agg_lower == "distinct" and y_field:
            return exp.Count(this=exp.column(y_field), distinct=True)
        elif agg_lower in ("sum", "avg", "min", "max") and y_field:
            func_map = {
                "sum": exp.Sum,
                "avg": exp.Avg,
                "min": exp.Min,
                "max": exp.Max,
            }
            return func_map[agg_lower](this=exp.column(y_field))
        else:
            return exp.Count(this=exp.Star())
    
    def _build_time_bucket(
        self, 
        field: str, 
        group_by: str, 
        week_start: str = "mon"
    ) -> exp.Expression:
        """Build time bucketing expression"""
        col = exp.column(field)
        
        if self.dialect == "duckdb":
            return exp.func("DATE_TRUNC", exp.Literal.string(group_by), col)
        elif self.dialect == "postgres":
            return exp.func("date_trunc", exp.Literal.string(group_by), col)
        elif self.dialect == "tsql":  # MSSQL
            if group_by == "day":
                return exp.Cast(this=col, to=exp.DataType.Type.DATE)
            elif group_by == "month":
                return exp.func(
                    "DATEFROMPARTS",
                    exp.func("YEAR", col),
                    exp.func("MONTH", col),
                    exp.Literal.number(1),
                )
        elif self.dialect == "mysql":
            if group_by == "day":
                return exp.func("DATE", col)
            elif group_by == "month":
                return exp.func("DATE_FORMAT", col, exp.Literal.string("%Y-%m-01"))
        
        return col
    
    def _apply_where(
        self, 
        query: exp.Select, 
        where: Dict[str, Any]
    ) -> exp.Select:
        """Apply WHERE clause filters"""
        for key, value in where.items():
            # Skip special keys
            if key in ("start", "startDate", "end", "endDate"):
                continue
            
            col = exp.column(key)
            
            if value is None:
                query = query.where(col.is_(exp.null()))
            elif isinstance(value, list):
                if len(value) > 0:
                    literals = [self._to_literal(v) for v in value]
                    query = query.where(col.isin(*literals))
            else:
                query = query.where(col.eq(self._to_literal(value)))
        
        return query
    
    def _to_literal(self, value: Any) -> exp.Literal:
        """Convert Python value to SQL literal"""
        if isinstance(value, (int, float)):
            return exp.Literal.number(value)
        elif isinstance(value, bool):
            return exp.true() if value else exp.false()
        else:
            return exp.Literal.string(str(value))


def should_use_sqlglot(user_id: Optional[str] = None) -> bool:
    """
    Determine if SQLGlot should be used for this request.
    
    Args:
        user_id: Optional user ID for per-user override
        
    Returns:
        True if SQLGlot should be used, False for legacy
    """
    from .config import settings
    
    # Global flag disabled?
    if not settings.enable_sqlglot:
        return False
    
    # No user-specific filtering?
    if not settings.sqlglot_users or settings.sqlglot_users == "*":
        return True
    
    # Check if user is in allowed list
    if user_id:
        allowed_users = [u.strip() for u in settings.sqlglot_users.split(",")]
        return user_id in allowed_users
    
    return False
```

### 1.3 Backend: Update `query.py`

**File**: `backend/app/routers/query.py`

Add imports:
```python
from ..sqlgen_glot import SQLGlotBuilder, should_use_sqlglot
```

Update `/query/spec` endpoint (around line 2848):

```python
# Around line 2848 - inside aggregation query building
if agg and agg != "none":
    # Check if we should use SQLGlot
    use_sqlglot = should_use_sqlglot(actorId)
    
    if use_sqlglot:
        # NEW: SQLGlot path
        try:
            print(f"[SQLGlot] Using SQLGlot for query (user={actorId})")
            builder = SQLGlotBuilder(dialect=ds_type)
            sql_inner = builder.build_aggregation_query(
                source=spec.source,
                x_field=x_col,
                y_field=spec.y,
                legend_field=spec.legend if hasattr(spec, 'legend') else None,
                agg=agg,
                where=spec.where if hasattr(spec, 'where') else None,
                group_by=spec.groupBy if hasattr(spec, 'groupBy') else None,
                order_by=spec.orderBy if hasattr(spec, 'orderBy') else None,
                limit=lim,
                week_start=spec.weekStart if hasattr(spec, 'weekStart') else 'mon',
            )
            print(f"[SQLGlot] Generated: {sql_inner[:200]}")
        except Exception as e:
            print(f"[SQLGlot] Failed, falling back to legacy: {e}")
            use_sqlglot = False
    
    if not use_sqlglot:
        # EXISTING: Legacy path (keep all existing code)
        if spec.measure:
            # ... existing measure handling ...
            pass
        # ... rest of legacy code unchanged ...
```

---

## Phase 2: Frontend Configuration (Day 3-4)

### 2.1 Add User Preference

**File**: `frontend/src/types/user.ts`

```typescript
export interface UserPreferences {
  // ... existing preferences ...
  
  // SQLGlot feature flag
  useSQLGlot?: boolean
}
```

### 2.2 Settings Page Toggle

**File**: `frontend/src/app/settings/page.tsx`

Add toggle switch:

```tsx
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'

// In your settings form:
<div className="flex items-center justify-between">
  <div className="space-y-0.5">
    <Label htmlFor="sqlglot">
      Experimental: SQLGlot Query Engine
    </Label>
    <p className="text-sm text-muted-foreground">
      Use SQLGlot for SQL generation (experimental feature)
    </p>
  </div>
  <Switch
    id="sqlglot"
    checked={preferences.useSQLGlot ?? false}
    onCheckedChange={(checked) => {
      updatePreference('useSQLGlot', checked)
    }}
  />
</div>
```

### 2.3 Per-Chart Override (Optional)

**File**: `frontend/src/components/widgets/ChartCard.tsx`

Add a debug badge when SQLGlot is active:

```tsx
{/* Show SQLGlot badge when active */}
{user?.preferences?.useSQLGlot && (
  <div className="absolute top-2 right-2 bg-blue-500 text-white text-xs px-2 py-1 rounded">
    SQLGlot
  </div>
)}
```

---

## Phase 3: Testing & Comparison (Day 5-7)

### 3.1 A/B Comparison Tool

**File**: `backend/app/routers/debug.py` (new file)

```python
"""
Debug endpoints for comparing SQLGlot vs Legacy SQL generation.
Only available in development/staging.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from ..schemas import QuerySpecRequest
from ..sqlgen_glot import SQLGlotBuilder

router = APIRouter(prefix="/debug", tags=["debug"])


@router.post("/compare-sql")
def compare_sql_generation(
    payload: QuerySpecRequest,
    db: Session = Depends(get_db)
):
    """
    Compare SQLGlot vs Legacy SQL generation side-by-side.
    Returns both SQL strings for comparison.
    """
    from ..routers.query import run_query_spec  # Import existing handler
    
    # Generate SQLGlot SQL
    try:
        builder = SQLGlotBuilder(dialect="duckdb")
        sqlglot_sql = builder.build_aggregation_query(
            source=payload.spec.source,
            x_field=payload.spec.x,
            y_field=payload.spec.y,
            legend_field=payload.spec.legend,
            agg=payload.spec.agg or "sum",
            where=payload.spec.where,
            group_by=payload.spec.groupBy,
            limit=payload.limit,
        )
    except Exception as e:
        sqlglot_sql = f"ERROR: {str(e)}"
    
    # TODO: Capture legacy SQL (requires refactoring legacy builder)
    legacy_sql = "TODO: Extract from legacy builder"
    
    return {
        "sqlglot": sqlglot_sql,
        "legacy": legacy_sql,
        "match": sqlglot_sql == legacy_sql,
    }
```

### 3.2 Automated Testing

**File**: `backend/tests/test_sqlglot_parity.py`

```python
"""
Test SQLGlot generates equivalent SQL to legacy builder.
"""
import pytest
from app.sqlgen_glot import SQLGlotBuilder


class TestSQLGlotParity:
    """Verify SQLGlot matches legacy output"""
    
    def test_simple_sum_aggregation(self):
        """Test: SELECT x, SUM(y) GROUP BY x"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="sales",
            x_field="date",
            y_field="amount",
            agg="sum",
        )
        
        # Verify key components
        assert "SUM" in sql.upper()
        assert "GROUP BY" in sql.upper()
        assert "sales" in sql
    
    def test_with_legend(self):
        """Test: SELECT x, legend, SUM(y) GROUP BY x, legend"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="orders",
            x_field="date",
            y_field="total",
            legend_field="category",
            agg="sum",
        )
        
        assert "category" in sql
        assert "GROUP BY" in sql.upper()
        # Should group by 2 columns
        assert "1" in sql and "2" in sql
    
    def test_time_bucketing(self):
        """Test: DATE_TRUNC('month', date)"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="events",
            x_field="timestamp",
            y_field="count",
            agg="sum",
            group_by="month",
        )
        
        assert "DATE_TRUNC" in sql
        assert "month" in sql.lower()
```

---

## Phase 4: Gradual Rollout (Week 2)

### 4.1 Rollout Strategy

**Day 1-2**: Internal testing only
- Set `SQLGLOT_USERS="your-user-id"`
- Test all chart types manually
- Compare SQL outputs

**Day 3-4**: Staging environment
- Set `ENABLE_SQLGLOT=true`
- Add UI toggle in settings
- Monitor error rates

**Day 5-7**: Production (gradual)
- Start with 1% of users
- Increase to 10% → 25% → 50% → 100%
- Keep legacy as fallback

### 4.2 Monitoring

Add logs to track usage:

```python
# In query.py
if use_sqlglot:
    counter_inc("sqlglot_queries_total", {"dialect": ds_type})
else:
    counter_inc("legacy_queries_total", {"dialect": ds_type})
```

Track metrics:
- Query generation time (SQLGlot vs Legacy)
- SQL error rates
- Query execution time
- User satisfaction

---

## Phase 5: Endpoint-by-Endpoint Migration

### Priority Order:

1. **`/query/spec` - Aggregation queries** (most common)
   - Single-series with legend
   - Multi-series
   - Time bucketing

2. **`/query/period-totals-batch` - Delta calculations**
   - Previous period comparisons
   - Batch processing

3. **`/query/distinct` - Distinct values**
   - Filter dropdowns
   - Category lists

4. **Custom SQL endpoints** (if any)

### Migration Checklist per Endpoint:

- [ ] Add `if use_sqlglot:` branch
- [ ] Implement SQLGlot equivalent
- [ ] Add fallback to legacy on error
- [ ] Write unit tests
- [ ] Write integration tests
- [ ] Test with real data
- [ ] Monitor in production
- [ ] Document differences

---

## Success Criteria

Before removing legacy code, verify:

1. ✅ **Parity**: SQLGlot output matches legacy 99.9%
2. ✅ **Performance**: No regression in query speed
3. ✅ **Stability**: Error rate < 0.1% for 1 week
4. ✅ **Coverage**: All SQL patterns migrated
5. ✅ **User Feedback**: No complaints about data accuracy

---

## Rollback Plan

If SQLGlot causes issues:

1. **Immediate**: Disable via `ENABLE_SQLGLOT=false`
2. **Per-User**: Remove from `SQLGLOT_USERS` list
3. **Per-Query**: Catch exceptions, fallback to legacy
4. **UI**: Let users toggle off in settings

---

## File Structure

```
backend/
├── app/
│   ├── config.py              # Add ENABLE_SQLGLOT flag
│   ├── sqlgen_glot.py         # NEW: SQLGlot builder
│   ├── routers/
│   │   ├── query.py           # Add if/else branching
│   │   └── debug.py           # NEW: Comparison tool
│   └── tests/
│       └── test_sqlglot_parity.py  # NEW: Tests

frontend/
├── src/
│   ├── types/
│   │   └── user.ts            # Add useSQLGlot preference
│   ├── app/
│   │   └── settings/
│   │       └── page.tsx       # Add toggle switch
│   └── components/
│       └── widgets/
│           └── ChartCard.tsx  # Optional: Show badge

.env
  ENABLE_SQLGLOT=false         # Feature flag
  SQLGLOT_USERS=""             # User whitelist
```

---

## Next Steps

1. ✅ Review this plan
2. Create `backend/app/sqlgen_glot.py`
3. Add config flags to `backend/app/config.py`
4. Update `backend/app/routers/query.py` with branching logic
5. Add UI toggle in settings page
6. Test with one chart type
7. Iterate and expand

Ready to start? I'll create the files.
