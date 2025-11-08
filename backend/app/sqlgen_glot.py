"""
SQLGlot-based SQL generation for multi-dialect support.
Runs side-by-side with SQLGlot-based SQL builder for multi-dialect query generation.
"""
print("[SQLGlot] ★★★ sqlgen_glot.py MODULE LOADED ★★★")
from typing import Any, Dict, Optional, List
import logging
import re
import sqlglot
from sqlglot import exp
print(f"[SQLGlot] ★★★ sqlglot version {sqlglot.__version__} imported ★★★")

logger = logging.getLogger(__name__)


class SQLGlotBuilder:
    """
    Generate SQL using SQLGlot for multi-dialect support.
    
    This builder runs in parallel with the legacy SQL string builder,
    allowing gradual migration and A/B testing.
    """
    
    def __init__(self, dialect: str = "duckdb"):
        """
        Initialize query builder.
        
        Args:
            dialect: Target SQL dialect (duckdb, postgres, mysql, mssql, sqlite)
        """
        self.dialect = self._normalize_dialect(dialect)
    
    def _normalize_dialect(self, dialect: str) -> str:
        """
        Normalize dialect names to SQLGlot format.
        
        Maps our internal dialect names to SQLGlot's expected names.
        """
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
        agg: str = "count",
        where: Optional[Dict[str, Any]] = None,
        group_by: Optional[str] = None,
        order_by: Optional[str] = None,
        order: str = "asc",
        limit: Optional[int] = None,
        week_start: str = "mon",
        date_field: Optional[str] = None,  # For date range filtering
        expr_map: Optional[Dict[str, str]] = None,  # Custom column mapping
        ds_type: Optional[str] = None,  # Dialect for date part resolution
        series: Optional[List[Dict[str, Any]]] = None,  # Multi-series support
        legend_fields: Optional[List[str]] = None,  # Multi-legend support
    ) -> str:
        """
        Build aggregation query with multi-dialect support.
        
        Output format matches legacy builder:
        - Columns: ['x', 'legend', 'value'] or ['x', 'value']
        - Compatible with existing frontend expectations
        
        Args:
            source: Table name
            x_field: X-axis field (dimension)
            y_field: Y-axis field (measure for aggregation)
            legend_field: Legend/category field
            agg: Aggregation function (sum, count, avg, min, max, distinct)
            where: Filter conditions dict
            group_by: Time grouping (day, week, month, quarter, year, none)
            order_by: Ordering field (x, value, legend)
            order: Sort order (asc, desc)
            limit: Result limit
            week_start: Week start day (mon, sun) for week grouping
            
        Returns:
            SQL string for target dialect
            
        Example:
            >>> builder = SQLGlotBuilder("duckdb")
            >>> sql = builder.build_aggregation_query(
            ...     source="sales",
            ...     x_field="date",
            ...     y_field="amount",
            ...     legend_field="category",
            ...     agg="sum",
            ...     group_by="month"
            ... )
        """
        try:
            # MULTI-SERIES: If series array provided, generate UNION ALL query
            if series and len(series) > 0:
                print(f"[SQLGlot] Multi-series detected: {len(series)} series")
                return self._build_multi_series_query(
                    source=source,
                    x_field=x_field,
                    series=series,
                    where=where,
                    group_by=group_by,
                    order_by=order_by,
                    order=order,
                    limit=limit,
                    week_start=week_start,
                    expr_map=expr_map,
                    ds_type=ds_type,
                    legend_field=legend_field,  # Pass through for combination
                    legend_fields=legend_fields,  # Pass through for combination
                )
            
            # Helper: resolve custom columns and date parts
            def resolve_field(field: Optional[str]) -> tuple[Optional[str], bool]:
                """
                Resolve a field name to its SQL expression if it's a custom column or date part.
                Returns (resolved_expr, is_expression) tuple.
                """
                if not field:
                    return field, False
                
                # Check if it's a custom column
                if expr_map and field in expr_map:
                    expr = expr_map[field]
                    # Strip table aliases (e.g., s.ClientID -> ClientID, src.OrderDate -> OrderDate)
                    # Only strip short lowercase identifiers (typical aliases), not schema names
                    expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)
                    print(f"[SQLGlot] ✅ Resolving custom column '{field}' in SELECT → {expr[:80]}...")
                    return expr, True
                
                # Check if it's a date part pattern
                match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", field, flags=re.IGNORECASE)
                if match:
                    base_col = match.group(1).strip()
                    kind = match.group(2).lower()
                    expr = self._build_datepart_expr(base_col, kind, ds_type or self.dialect)
                    print(f"[SQLGlot] ✅ Resolving date part '{field}' in SELECT → {expr[:80]}")
                    return expr, True
                
                return field, False
            
            # Resolve fields before building query
            x_field_resolved, x_is_expr = resolve_field(x_field)
            y_field_resolved, y_is_expr = resolve_field(y_field)
            
            # MULTI-LEGEND: Handle legend_fields array (single or multiple)
            legend_field_resolved = legend_field
            legend_is_expr = False
            
            if legend_fields and len(legend_fields) > 0:
                if len(legend_fields) == 1:
                    # Single field in array - just resolve it normally
                    print(f"[SQLGlot] Single legend field in array: {legend_fields[0]}")
                    legend_field_resolved, legend_is_expr = resolve_field(legend_fields[0])
                else:
                    # Multiple fields - concatenate with dialect-aware separator
                    print(f"[SQLGlot] Multi-legend detected: {len(legend_fields)} fields: {legend_fields}")
                    
                    # Resolve each field first
                    resolved_fields = []
                    for lf in legend_fields:
                        lf_resolved, _ = resolve_field(lf)
                        resolved_fields.append(lf_resolved if lf_resolved else lf)
                    
                    # Build dialect-aware concatenation
                    dialect_lower = (ds_type or self.dialect).lower()
                    if "mssql" in dialect_lower or "sqlserver" in dialect_lower:
                        # MSSQL: CONCAT(field1, ' - ', field2)
                        legend_field_resolved = "CONCAT(" + ", ' - ', ".join([f'"{f}"' for f in resolved_fields]) + ")"
                    elif "mysql" in dialect_lower:
                        # MySQL: CONCAT(field1, ' - ', field2)
                        legend_field_resolved = "CONCAT(" + ", ' - ', ".join([f'"{f}"' for f in resolved_fields]) + ")"
                    else:
                        # DuckDB/PostgreSQL/SQLite: field1 || ' - ' || field2
                        legend_field_resolved = " || ' - ' || ".join([f'"{f}"' for f in resolved_fields])
                    
                    legend_is_expr = True
                    print(f"[SQLGlot] Multi-legend expression: {legend_field_resolved[:100]}")
            elif legend_field:
                # Single legend field (not in array) - resolve normally
                legend_field_resolved, legend_is_expr = resolve_field(legend_field)
            
            # Use resolved fields
            x_field = x_field_resolved
            y_field = y_field_resolved
            legend_field = legend_field_resolved
            
            # Start with base table
            # Handle schema.table format (e.g., "main.table_name")
            if "." in source:
                parts = source.split(".", 1)
                schema_name = parts[0]
                table_name = parts[1]
                # Use exp.Table directly to avoid parsing issues
                table_expr = exp.Table(
                    this=exp.Identifier(this=table_name, quoted=True),
                    db=exp.Identifier(this=schema_name, quoted=False)
                )
            else:
                # Simple table name
                table_expr = exp.Table(this=exp.Identifier(this=source, quoted=True))
            
            query = sqlglot.select("*").from_(table_expr)
            
            # Build SELECT clause
            select_exprs = []
            group_positions = []
            
            # X field (with optional time bucketing)
            if x_field:
                if x_is_expr:
                    # Parse as raw SQL expression
                    x_expr = sqlglot.parse_one(x_field, dialect=self.dialect)
                elif group_by and group_by != "none":
                    x_expr = self._build_time_bucket(x_field, group_by, week_start)
                else:
                    x_expr = exp.Column(this=exp.Identifier(this=x_field, quoted=True))
                select_exprs.append(x_expr.as_("x"))
                group_positions.append(1)
            
            # Legend field
            if legend_field:
                if legend_is_expr:
                    # Parse as raw SQL expression
                    legend_col = sqlglot.parse_one(legend_field, dialect=self.dialect)
                else:
                    legend_col = exp.Column(this=exp.Identifier(this=legend_field, quoted=True))
                select_exprs.append(legend_col.as_("legend"))
                group_positions.append(len(select_exprs))
            
            # Aggregation (always aliased as 'value')
            agg_expr = self._build_aggregation(agg, y_field, y_is_expr)
            select_exprs.append(agg_expr.as_("value"))
            
            # Apply SELECT
            query = query.select(*select_exprs, append=False)
            
            # Apply WHERE clauses
            if where:
                query = self._apply_where(query, where, date_field=date_field or x_field)
            
            # Apply GROUP BY (by position)
            if group_positions:
                query = query.group_by(*[exp.Literal.number(i) for i in group_positions])
            
            # Apply ORDER BY
            if order_by:
                # Order by specified field
                order_col = exp.Column(this=exp.Identifier(this=order_by, quoted=True))
                if order.lower() == "desc":
                    query = query.order_by(order_col, desc=True)
                else:
                    query = query.order_by(order_col)
            elif group_positions:
                # Default: order by grouped fields
                query = query.order_by(*[exp.Literal.number(i) for i in group_positions])
            
            # Apply LIMIT
            if limit and limit > 0:
                query = query.limit(limit)
            
            # Generate SQL for target dialect
            sql = query.sql(dialect=self.dialect, pretty=False)
            
            logger.info(f"[SQLGlot] Generated SQL ({self.dialect}): {sql[:150]}...")
            print(f"[SQLGlot] Generated SQL ({self.dialect}): {sql[:150]}...")
            return sql
            
        except Exception as e:
            logger.error(f"[SQLGlot] ERROR generating SQL: {e}")
            print(f"[SQLGlot] ERROR generating SQL: {e}")
            raise
    
    def build_distinct_query(
        self,
        source: str,
        field: str,
        where: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        order: str = "asc",
        limit: Optional[int] = None,
        expr_map: Optional[Dict[str, str]] = None,
        ds_type: Optional[str] = None,
    ) -> str:
        """
        Build DISTINCT values query with multi-dialect support.
        
        Args:
            source: Table name
            field: Field to get distinct values for
            where: Optional filter conditions
            order_by: Optional ordering field
            order: Sort order (asc, desc)
            limit: Result limit
            expr_map: Custom column mapping
            ds_type: Dialect for date part resolution
            
        Returns:
            SQL string for target dialect
        """
        try:
            # Helper: resolve custom columns and date parts
            def resolve_field(field_name: Optional[str]) -> tuple[Optional[str], bool]:
                """Resolve field to SQL expression if it's a custom column or date part"""
                if not field_name:
                    return field_name, False
                
                # Check if it's a custom column
                if expr_map and field_name in expr_map:
                    expr = expr_map[field_name]
                    # Strip table aliases
                    expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)
                    print(f"[SQLGlot] ✅ Resolving custom column '{field_name}' in DISTINCT")
                    return expr, True
                
                # Check if it's a date part pattern
                match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", field_name, flags=re.IGNORECASE)
                if match:
                    base_col = match.group(1).strip()
                    kind = match.group(2).lower()
                    expr = self._build_datepart_expr(base_col, kind, ds_type or self.dialect)
                    print(f"[SQLGlot] ✅ Resolving date part '{field_name}' in DISTINCT")
                    return expr, True
                
                return field_name, False
            
            # Resolve field
            field_resolved, is_expr = resolve_field(field)
            
            # Start with base table or subquery
            if source.strip().startswith("(") and " AS " in source.upper():
                # It's a subquery like "(SELECT ...) AS _base"
                # Parse it as raw SQL
                from_clause = sqlglot.parse_one(source, dialect=self.dialect)
                query = sqlglot.select("*").from_(from_clause)
            elif "." in source:
                parts = source.split(".", 1)
                schema_name = parts[0]
                table_name = parts[1]
                table_expr = exp.Table(
                    this=exp.Identifier(this=table_name, quoted=True),
                    db=exp.Identifier(this=schema_name, quoted=False)
                )
                query = sqlglot.select("*").from_(table_expr)
            else:
                table_expr = exp.Table(this=exp.Identifier(this=source, quoted=True))
                query = sqlglot.select("*").from_(table_expr)
            
            # Build SELECT with DISTINCT
            if is_expr:
                # Parse as raw SQL expression
                field_expr = sqlglot.parse_one(field_resolved, dialect=self.dialect)
            else:
                field_expr = exp.Column(this=exp.Identifier(this=field_resolved, quoted=True))
            
            query = query.select(field_expr, append=False).distinct()
            
            # Apply WHERE clauses
            if where:
                query = self._apply_where(query, where)
            
            # Apply ORDER BY
            if order_by:
                order_expr = exp.Column(this=exp.Identifier(this=order_by, quoted=True))
                query = query.order_by(order_expr, order="desc" if order.lower() == "desc" else "asc")
            else:
                # Default: order by the selected field
                query = query.order_by(field_expr, order="desc" if order.lower() == "desc" else "asc")
            
            # Apply LIMIT
            if limit and limit > 0:
                query = query.limit(limit)
            
            # Generate SQL
            sql = query.sql(dialect=self.dialect, pretty=False)
            
            logger.info(f"[SQLGlot] Generated DISTINCT SQL ({self.dialect}): {sql[:150]}...")
            print(f"[SQLGlot] Generated DISTINCT SQL ({self.dialect}): {sql[:150]}...")
            return sql
            
        except Exception as e:
            logger.error(f"[SQLGlot] ERROR generating DISTINCT SQL: {e}")
            print(f"[SQLGlot] ERROR generating DISTINCT SQL: {e}")
            raise
    
    def build_period_totals_query(
        self,
        source: str,
        y_field: Optional[str],
        agg: str,
        date_field: str,
        start: str,
        end: str,
        where: Optional[Dict[str, Any]] = None,
        legend_field: Optional[str] = None,
        expr_map: Optional[Dict[str, str]] = None,
        ds_type: Optional[str] = None,
    ) -> str:
        """
        Build period totals query (aggregation over a date range).
        
        Args:
            source: Table name
            y_field: Field to aggregate
            agg: Aggregation function (sum, count, avg, min, max, distinct)
            date_field: Date field for range filtering
            start: Start date (ISO format)
            end: End date (ISO format)
            where: Optional additional filter conditions
            legend_field: Optional grouping field
            expr_map: Custom column mapping
            ds_type: Dialect for date part resolution
            
        Returns:
            SQL string for target dialect
        """
        try:
            # Helper: resolve custom columns and date parts
            def resolve_field(field_name: Optional[str]) -> tuple[Optional[str], bool]:
                """Resolve field to SQL expression if it's a custom column or date part"""
                if not field_name:
                    return field_name, False
                
                # Check if it's a custom column
                if expr_map and field_name in expr_map:
                    expr = expr_map[field_name]
                    # Strip table aliases
                    expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)
                    print(f"[SQLGlot] ✅ Resolving custom column '{field_name}' in period-totals")
                    return expr, True
                
                # Check if it's a date part pattern
                match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", field_name, flags=re.IGNORECASE)
                if match:
                    base_col = match.group(1).strip()
                    kind = match.group(2).lower()
                    expr = self._build_datepart_expr(base_col, kind, ds_type or self.dialect)
                    print(f"[SQLGlot] ✅ Resolving date part '{field_name}' in period-totals")
                    return expr, True
                
                return field_name, False
            
            # Resolve fields
            y_field_resolved, y_is_expr = resolve_field(y_field)
            legend_field_resolved, legend_is_expr = resolve_field(legend_field)
            date_field_resolved, date_is_expr = resolve_field(date_field)
            
            # Start with base table or subquery
            if source.strip().startswith("(") and " AS " in source.upper():
                # It's a subquery like "(SELECT ...) AS _base"
                from_clause = sqlglot.parse_one(source, dialect=self.dialect)
                query = sqlglot.select("*").from_(from_clause)
            elif "." in source:
                parts = source.split(".", 1)
                schema_name = parts[0]
                table_name = parts[1]
                table_expr = exp.Table(
                    this=exp.Identifier(this=table_name, quoted=True),
                    db=exp.Identifier(this=schema_name, quoted=False)
                )
                query = sqlglot.select("*").from_(table_expr)
            else:
                table_expr = exp.Table(this=exp.Identifier(this=source, quoted=True))
                query = sqlglot.select("*").from_(table_expr)
            
            # Build SELECT clause
            select_exprs = []
            
            # Legend field (if provided)
            if legend_field_resolved:
                if legend_is_expr:
                    legend_col = sqlglot.parse_one(legend_field_resolved, dialect=self.dialect)
                else:
                    legend_col = exp.Column(this=exp.Identifier(this=legend_field_resolved, quoted=True))
                select_exprs.append(legend_col)
            
            # Aggregation
            agg_expr = self._build_aggregation(agg, y_field_resolved, y_is_expr)
            select_exprs.append(agg_expr)
            
            # Apply SELECT
            query = query.select(*select_exprs, append=False)
            
            # Apply WHERE clauses
            # 1. Date range filter
            if date_is_expr:
                date_col = sqlglot.parse_one(date_field_resolved, dialect=self.dialect)
            else:
                date_col = exp.Column(this=exp.Identifier(this=date_field_resolved, quoted=True))
            
            query = query.where(date_col.between(exp.Literal.string(start), exp.Literal.string(end)))
            
            # 2. Additional WHERE conditions
            if where:
                query = self._apply_where(query, where)
            
            # Apply GROUP BY (if legend field exists)
            if legend_field_resolved:
                query = query.group_by(1)  # Group by position
            
            # Generate SQL
            sql = query.sql(dialect=self.dialect, pretty=False)
            
            logger.info(f"[SQLGlot] Generated period-totals SQL ({self.dialect}): {sql[:150]}...")
            print(f"[SQLGlot] Generated period-totals SQL ({self.dialect}): {sql[:150]}...")
            return sql
            
        except Exception as e:
            logger.error(f"[SQLGlot] ERROR generating period-totals SQL: {e}")
            print(f"[SQLGlot] ERROR generating period-totals SQL: {e}")
            raise
    
    def _build_aggregation(self, agg: str, y_field: Optional[str], is_expr: bool = False) -> exp.Expression:
        """
        Build aggregation expression.
        
        Supports: count, distinct, sum, avg, min, max
        """
        agg_lower = agg.lower()
        
        if agg_lower == "count":
            return exp.Count(this=exp.Star())
        elif agg_lower == "distinct" and y_field:
            if is_expr:
                col_expr = sqlglot.parse_one(y_field, dialect=self.dialect)
            else:
                col_expr = exp.Column(this=exp.Identifier(this=y_field, quoted=True))
            return exp.Count(this=col_expr, distinct=True)
        elif agg_lower in ("sum", "avg", "min", "max") and y_field:
            # Handle numeric cleaning for DuckDB (try casting)
            if is_expr:
                col_expr = sqlglot.parse_one(y_field, dialect=self.dialect)
            else:
                col_expr = exp.Column(this=exp.Identifier(this=y_field, quoted=True))
            
            # For DuckDB, wrap in TRY_CAST to handle non-numeric values
            if self.dialect == "duckdb" and agg_lower in ("sum", "avg"):
                # Cast to VARCHAR first for REGEXP_REPLACE
                col_as_varchar = exp.Cast(this=col_expr, to=exp.DataType.build("VARCHAR"))
                col_expr = exp.func(
                    "TRY_CAST",
                    exp.func("REGEXP_REPLACE", col_as_varchar, exp.Literal.string("[^0-9.-]"), exp.Literal.string("")),
                    exp.DataType.build("DOUBLE")
                )
            
            func_map = {
                "sum": exp.Sum,
                "avg": exp.Avg,
                "min": exp.Min,
                "max": exp.Max,
            }
            return func_map[agg_lower](this=col_expr)
        else:
            # Fallback to COUNT(*)
            return exp.Count(this=exp.Star())
    
    def _build_time_bucket(
        self, 
        field: str, 
        group_by: str, 
        week_start: str = "mon"
    ) -> exp.Expression:
        """
        Build time bucketing expression based on dialect.
        
        Handles: day, week, month, quarter, year
        """
        col = exp.Column(this=exp.Identifier(this=field, quoted=True))
        group_by_lower = group_by.lower()
        
        if self.dialect == "duckdb":
            # DuckDB: DATE_TRUNC('month', field)
            return exp.func("DATE_TRUNC", exp.Literal.string(group_by_lower), col)
            
        elif self.dialect == "postgres":
            # PostgreSQL: date_trunc('month', field)
            return exp.func("date_trunc", exp.Literal.string(group_by_lower), col)
            
        elif self.dialect == "tsql":  # MSSQL
            # MSSQL: Use DATEFROMPARTS for bucketing
            if group_by_lower == "day":
                return exp.Cast(this=col, to=exp.DataType.Type.DATE)
            elif group_by_lower == "month":
                return exp.func(
                    "DATEFROMPARTS",
                    exp.func("YEAR", col),
                    exp.func("MONTH", col),
                    exp.Literal.number(1),
                )
            elif group_by_lower == "year":
                return exp.func(
                    "DATEFROMPARTS",
                    exp.func("YEAR", col),
                    exp.Literal.number(1),
                    exp.Literal.number(1),
                )
            # Fallback
            return exp.Cast(this=col, to=exp.DataType.Type.DATE)
            
        elif self.dialect == "mysql":
            # MySQL: DATE_FORMAT for bucketing
            if group_by_lower == "day":
                return exp.func("DATE", col)
            elif group_by_lower == "month":
                return exp.func("DATE_FORMAT", col, exp.Literal.string("%Y-%m-01"))
            elif group_by_lower == "year":
                return exp.func("DATE_FORMAT", col, exp.Literal.string("%Y-01-01"))
            # Fallback
            return exp.func("DATE", col)
        
        # Fallback: return column as-is
        return col
    
    def _apply_where(
        self, 
        query: exp.Select, 
        where: Dict[str, Any],
        date_field: Optional[str] = None
    ) -> exp.Select:
        """
        Apply WHERE clause filters.
        
        Handles:
        - Single values: field = 'value'
        - Lists: field IN ('val1', 'val2')
        - None: field IS NULL
        - Comparison operators: field__gte, field__gt, field__lte, field__lt
        - Date ranges: start, startDate, end, endDate (requires x_field)
        
        Note: Does NOT validate column existence - trusts database to handle it.
        This allows derived columns and transforms to work.
        """
        for key, value in where.items():
            # Check if key is an expression (starts with parenthesis)
            # This indicates a resolved derived column like "(strftime('%Y', OrderDate))"
            is_expression = key.startswith("(") and key.endswith(")")
            
            if is_expression:
                # Parse the expression
                try:
                    # Remove outer parentheses and parse
                    expr_sql = key[1:-1]
                    col = sqlglot.parse_one(expr_sql, dialect=self.dialect)
                except Exception as e:
                    logger.warning(f"[SQLGlot] Failed to parse expression '{key}': {e}")
                    continue
            else:
                # Handle comparison operators (field__gte, field__lte, etc.)
                if "__" in key and key not in ("start", "startDate", "end", "endDate"):
                    field, operator = key.rsplit("__", 1)
                    col = exp.Column(this=exp.Identifier(this=field, quoted=True))
                    
                    if operator == "gte":
                        query = query.where(col >= self._to_literal(value))
                    elif operator == "gt":
                        query = query.where(col > self._to_literal(value))
                    elif operator == "lte":
                        query = query.where(col <= self._to_literal(value))
                    elif operator == "lt":
                        query = query.where(col < self._to_literal(value))
                    else:
                        # Unknown operator, treat as regular field name
                        col = exp.Column(this=exp.Identifier(this=key, quoted=True))
                        query = query.where(col == self._to_literal(value))
                    continue
                
                # Handle date range filters
                if key in ("start", "startDate") and date_field:
                    date_col = exp.Column(this=exp.Identifier(this=date_field, quoted=True))
                    query = query.where(date_col >= self._to_literal(value))
                    continue
                elif key in ("end", "endDate") and date_field:
                    date_col = exp.Column(this=exp.Identifier(this=date_field, quoted=True))
                    query = query.where(date_col <= self._to_literal(value))
                    continue
                elif key in ("start", "startDate", "end", "endDate"):
                    # Date range key but no date_field specified, skip
                    logger.warning(f"[SQLGlot] Date range filter '{key}' ignored: no date_field specified")
                    continue
                
                # Regular column name
                col = exp.Column(this=exp.Identifier(this=key, quoted=True))
            
            if value is None:
                # NULL check
                query = query.where(col.is_(exp.null()))
            elif isinstance(value, list):
                # IN clause
                if len(value) > 0:
                    literals = [self._to_literal(v) for v in value]
                    query = query.where(col.isin(*literals))
            else:
                # Equality
                query = query.where(col.eq(self._to_literal(value)))
        
        return query
    
    def _build_datepart_expr(self, base_col: str, kind: str, dialect: str) -> str:
        """Build dialect-specific date part expression"""
        q = f'"{base_col}"'
        kind_l = kind.lower()
        dial = dialect.lower()
        
        # DuckDB
        if "duckdb" in dial:
            if kind_l == 'year': return f"strftime({q}, '%Y')"
            if kind_l == 'quarter': return f"concat(strftime({q}, '%Y'), '-Q', CAST(EXTRACT(QUARTER FROM {q}) AS INTEGER))"
            if kind_l == 'month': return f"strftime({q}, '%Y-%m')"
            if kind_l == 'month name': return f"strftime({q}, '%B')"
            if kind_l == 'month short': return f"strftime({q}, '%b')"
            if kind_l == 'week': return f"concat(strftime({q}, '%Y'), '-W', substr('00' || strftime({q}, '%W'), -2))"
            if kind_l == 'day': return f"strftime({q}, '%Y-%m-%d')"
            if kind_l == 'day name': return f"strftime({q}, '%A')"
            if kind_l == 'day short': return f"strftime({q}, '%a')"
        
        # PostgreSQL
        elif "postgres" in dial or "postgre" in dial:
            if kind_l == 'year': return f"to_char({q}, 'YYYY')"
            if kind_l == 'quarter': return f"to_char({q}, 'YYYY-\"Q\"Q')"
            if kind_l == 'month': return f"to_char({q}, 'YYYY-MM')"
            if kind_l == 'month name': return f"to_char({q}, 'FMMonth')"
            if kind_l == 'month short': return f"to_char({q}, 'Mon')"
            if kind_l == 'week': return f"to_char({q}, 'YYYY') || '-W' || lpad(to_char({q}, 'IW'), 2, '0')"
            if kind_l == 'day': return f"to_char({q}, 'YYYY-MM-DD')"
            if kind_l == 'day name': return f"to_char({q}, 'FMDay')"
            if kind_l == 'day short': return f"to_char({q}, 'Dy')"
        
        # MSSQL
        elif "mssql" in dial or "sqlserver" in dial or "tsql" in dial:
            if kind_l == 'year': return f"CAST(YEAR({q}) AS varchar(10))"
            if kind_l == 'quarter': return f"CAST(YEAR({q}) AS varchar(4)) + '-Q' + CAST(DATEPART(QUARTER, {q}) AS varchar(1))"
            if kind_l == 'month': return f"CONCAT(CAST(YEAR({q}) AS varchar(4)), '-', RIGHT('0' + CAST(MONTH({q}) AS varchar(2)), 2))"
            if kind_l == 'month name': return f"DATENAME(month, {q})"
            if kind_l == 'month short': return f"LEFT(DATENAME(month, {q}), 3)"
            if kind_l == 'week': return f"CONCAT(CAST(YEAR({q}) AS varchar(4)), '-W', RIGHT('0' + CAST(DATEPART(ISO_WEEK, {q}) AS varchar(2)), 2))"
            if kind_l == 'day': return f"CONCAT(CAST(YEAR({q}) AS varchar(4)), '-', RIGHT('0'+CAST(MONTH({q}) AS varchar(2)),2), '-', RIGHT('0'+CAST(DAY({q}) AS varchar(2)),2))"
            if kind_l == 'day name': return f"DATENAME(weekday, {q})"
            if kind_l == 'day short': return f"LEFT(DATENAME(weekday, {q}), 3)"
        
        # MySQL
        elif "mysql" in dial:
            if kind_l == 'year': return f"DATE_FORMAT({q}, '%Y')"
            if kind_l == 'quarter': return f"CONCAT(DATE_FORMAT({q}, '%Y'), '-Q', QUARTER({q}))"
            if kind_l == 'month': return f"DATE_FORMAT({q}, '%Y-%m')"
            if kind_l == 'month name': return f"DATE_FORMAT({q}, '%M')"
            if kind_l == 'month short': return f"DATE_FORMAT({q}, '%b')"
            if kind_l == 'week': return f"CONCAT(DATE_FORMAT({q}, '%Y'), '-W', LPAD(WEEK({q}, 3), 2, '0'))"
            if kind_l == 'day': return f"DATE_FORMAT({q}, '%Y-%m-%d')"
            if kind_l == 'day name': return f"DATE_FORMAT({q}, '%W')"
            if kind_l == 'day short': return f"DATE_FORMAT({q}, '%a')"
        
        # Fallback
        return q
    
    def _to_literal(self, value: Any) -> exp.Literal:
        """Convert Python value to SQL literal"""
        if isinstance(value, (int, float)):
            return exp.Literal.number(value)
        elif isinstance(value, bool):
            return exp.true() if value else exp.false()
        else:
            return exp.Literal.string(str(value))
    
    def build_pivot_query(
        self,
        source: str,
        rows: List[str],
        cols: List[str],
        value_field: Optional[str] = None,
        agg: str = "count",
        where: Optional[Dict[str, Any]] = None,
        group_by: Optional[str] = None,
        week_start: str = "mon",
        limit: Optional[int] = None,
        expr_map: Optional[Dict[str, str]] = None,
        ds_type: Optional[str] = None,
    ) -> str:
        """
        Build pivot query for server-side aggregation.
        
        Returns long-form data: [row_dims..., col_dims..., value]
        
        Args:
            source: Table name
            rows: List of row dimension fields
            cols: List of column dimension fields
            value_field: Measure field to aggregate (None for COUNT(*))
            agg: Aggregation function (count, sum, avg, min, max, distinct)
            where: Filter conditions
            group_by: Time bucketing for date fields
            week_start: Week start day for week grouping
            limit: Optional row limit
            expr_map: Custom column mapping
            ds_type: Dialect for resolution
            
        Returns:
            SQL string for pivot aggregation
            
        Example:
            >>> builder = SQLGlotBuilder("duckdb")
            >>> sql = builder.build_pivot_query(
            ...     source="sales",
            ...     rows=["Region"],
            ...     cols=["Category"],
            ...     value_field="Amount",
            ...     agg="sum"
            ... )
        """
        try:
            # Helper to resolve fields (reuse from build_aggregation_query)
            def resolve_field(field: Optional[str]) -> tuple[Optional[str], bool]:
                if not field:
                    return field, False
                
                # Check custom columns
                if expr_map and field in expr_map:
                    expr = expr_map[field]
                    expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)
                    return expr, True
                
                # Check date parts
                match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", field, flags=re.IGNORECASE)
                if match:
                    base_col = match.group(1).strip()
                    kind = match.group(2).lower()
                    expr = self._build_datepart_expr(base_col, kind, ds_type or self.dialect)
                    return expr, True
                
                return field, False
            
            # Build base query
            table_expr = exp.to_table(source)
            query = exp.select("*").from_(table_expr)
            
            # Resolve all dimension fields
            all_dims = rows + cols
            select_exprs = []
            group_exprs = []
            
            for dim in all_dims:
                dim_resolved, is_expr = resolve_field(dim)
                if is_expr:
                    # Parse as raw SQL expression
                    dim_expr = sqlglot.parse_one(dim_resolved, dialect=ds_type or self.dialect)
                    select_exprs.append(dim_expr.as_(dim))
                    group_exprs.append(dim_expr)
                else:
                    # Simple column reference
                    select_exprs.append(exp.column(dim_resolved or dim).as_(dim))
                    group_exprs.append(exp.column(dim_resolved or dim))
            
            # Build aggregation expression
            agg_lower = agg.lower()
            if agg_lower == "count":
                value_expr = exp.Count(this=exp.Star())
            elif agg_lower == "distinct":
                if value_field:
                    val_resolved, _ = resolve_field(value_field)
                    value_expr = exp.Count(this=exp.column(val_resolved or value_field), distinct=True)
                else:
                    value_expr = exp.Count(this=exp.Star())
            elif agg_lower in ("sum", "avg", "min", "max"):
                if value_field:
                    val_resolved, _ = resolve_field(value_field)
                    
                    # DuckDB numeric cleaning
                    if "duckdb" in (ds_type or self.dialect).lower():
                        # COALESCE(try_cast(regexp_replace(...), ...), 0.0)
                        clean_expr = sqlglot.parse_one(
                            f"COALESCE(try_cast(regexp_replace(CAST({val_resolved or value_field} AS VARCHAR), '[^0-9\\\\.-]', '') AS DOUBLE), try_cast({val_resolved or value_field} AS DOUBLE), 0.0)",
                            dialect="duckdb"
                        )
                        value_expr = getattr(exp, agg_lower.capitalize())(this=clean_expr)
                    else:
                        value_expr = getattr(exp, agg_lower.capitalize())(this=exp.column(val_resolved or value_field))
                else:
                    # No value field - fallback to COUNT
                    value_expr = exp.Count(this=exp.Star())
            else:
                # Unknown aggregation - fallback to COUNT
                value_expr = exp.Count(this=exp.Star())
            
            select_exprs.append(value_expr.as_("value"))
            
            # Build final query
            final_query = exp.select(*select_exprs).from_(table_expr)
            
            # Add WHERE clause
            if where:
                where_conditions = self._build_where_conditions(where, expr_map, ds_type)
                if where_conditions:
                    final_query = final_query.where(*where_conditions)
            
            # Add GROUP BY
            if group_exprs:
                final_query = final_query.group_by(*group_exprs)
            
            # Add ORDER BY (same as GROUP BY for pivot)
            if group_exprs:
                final_query = final_query.order_by(*group_exprs)
            
            # Add LIMIT
            if limit:
                final_query = final_query.limit(limit)
            
            # Generate SQL
            sql = final_query.sql(dialect=ds_type or self.dialect)
            print(f"[SQLGlot] Generated PIVOT SQL ({ds_type or self.dialect}): {sql[:200]}...")
            return sql
            
        except Exception as e:
            print(f"[SQLGlot] Pivot query error: {e}")
            raise
    
    def _build_multi_series_query(
        self,
        source: str,
        x_field: Optional[str],
        series: List[Dict[str, Any]],
        where: Optional[Dict[str, Any]],
        group_by: Optional[str],
        order_by: Optional[str],
        order: str,
        limit: Optional[int],
        week_start: str,
        expr_map: Optional[Dict[str, str]],
        ds_type: Optional[str],
        legend_field: Optional[str] = None,  # Support combining with legend
        legend_fields: Optional[List[str]] = None,  # Support multi-legend with multi-series
    ) -> str:
        """
        Build multi-series query using UNION ALL.
        
        Each series becomes a separate query with its own y_field and agg,
        with the series name as the legend value.
        
        Output format: x, legend (series name), value
        
        Example:
            series = [
                {"name": "Revenue", "y": "SalesAmount", "agg": "sum"},
                {"name": "Cost", "y": "CostAmount", "agg": "sum"}
            ]
            
            Result:
            SELECT * FROM (
                SELECT x, 'Revenue' as legend, SUM(SalesAmount) as value FROM ...
                UNION ALL
                SELECT x, 'Cost' as legend, SUM(CostAmount) as value FROM ...
            ) ORDER BY x, legend
        """
        queries = []
        has_legend = bool(legend_field or legend_fields)
        
        for idx, s in enumerate(series):
            series_name = s.get("name") or s.get("y") or f"Series {idx + 1}"
            y_field = s.get("y")
            agg = (s.get("agg") or "sum").lower()
            
            # Build individual query for this series
            try:
                single_query = self.build_aggregation_query(
                    source=source,
                    x_field=x_field,
                    y_field=y_field,
                    legend_field=legend_field if has_legend else None,  # Include original legend if present
                    agg=agg,
                    where=where,
                    group_by=group_by,
                    order_by=None,  # Don't order individual queries
                    order=order,
                    limit=None,  # Don't limit individual queries
                    week_start=week_start,
                    expr_map=expr_map,
                    ds_type=ds_type,
                    series=None,  # Prevent recursion
                    legend_fields=legend_fields if has_legend else None,  # Include multi-legend if present
                )
                
                # Wrap query to combine original legend (if any) with series name
                # Extract just the SELECT part without ORDER BY and LIMIT
                single_query = single_query.split(" ORDER BY ")[0].split(" LIMIT ")[0]
                
                if has_legend:
                    # Combine original legend with series name using dialect-aware concatenation
                    dialect_lower = (ds_type or self.dialect).lower()
                    if "mssql" in dialect_lower or "sqlserver" in dialect_lower or "mysql" in dialect_lower:
                        # MSSQL/MySQL: CONCAT(legend, ' - ', 'series_name')
                        wrapped = f"SELECT x, CONCAT(legend, ' - ', '{series_name}') as legend, value FROM ({single_query}) AS _s{idx}"
                    else:
                        # DuckDB/PostgreSQL/SQLite: legend || ' - ' || 'series_name'
                        wrapped = f"SELECT x, legend || ' - ' || '{series_name}' as legend, value FROM ({single_query}) AS _s{idx}"
                else:
                    # Just series name as legend
                    wrapped = f"SELECT x, '{series_name}' as legend, value FROM ({single_query}) AS _s{idx}"
                
                queries.append(wrapped)
                
            except Exception as e:
                print(f"[SQLGlot] Error building query for series '{series_name}': {e}")
                continue
        
        if not queries:
            raise ValueError("No valid series queries could be built")
        
        # Combine with UNION ALL
        combined = " UNION ALL ".join(queries)
        
        # Add ORDER BY and LIMIT to outer query
        final_sql = f"SELECT * FROM ({combined}) AS _multi_series"
        
        if order_by:
            if order_by.lower() == "value":
                final_sql += f" ORDER BY value {order.upper()}"
            elif order_by.lower() == "x":
                final_sql += f" ORDER BY x {order.upper()}, legend"
            else:
                final_sql += f" ORDER BY x, legend"
        else:
            final_sql += " ORDER BY x, legend"
        
        if limit:
            final_sql += f" LIMIT {limit}"
        
        print(f"[SQLGlot] Generated multi-series SQL with {len(queries)} series")
        return final_sql


def should_use_sqlglot(user_id: Optional[str] = None) -> bool:
    """
    Determine if SQLGlot should be used for this request.
    
    Checks:
    1. Global feature flag (ENABLE_SQLGLOT)
    2. User-specific whitelist (SQLGLOT_USERS)
    
    Args:
        user_id: Optional user ID for per-user override
        
    Returns:
        True if SQLGlot should be used, False for legacy
        
    Example:
        >>> # .env: ENABLE_SQLGLOT=true, SQLGLOT_USERS="user1,user2"
        >>> should_use_sqlglot("user1")  # True
        >>> should_use_sqlglot("user3")  # False
    """
    print(f"[SQLGlot] ★ should_use_sqlglot() CALLED with user_id={user_id}")
    from .config import settings
    
    # DEBUG: Always log what we see
    print(f"[SQLGlot] Config check: enable_sqlglot={settings.enable_sqlglot}, sqlglot_users='{settings.sqlglot_users}', user_id={user_id}")
    
    # Global flag disabled?
    if not settings.enable_sqlglot:
        print(f"[SQLGlot] DISABLED by feature flag")
        return False
    
    # No user filtering or wildcard?
    if not settings.sqlglot_users or settings.sqlglot_users.strip() == "*":
        print(f"[SQLGlot] ENABLED for all users (wildcard or empty)")
        return True
    
    # Check if user is in allowed list
    if user_id:
        allowed_users = [u.strip() for u in settings.sqlglot_users.split(",") if u.strip()]
        return user_id in allowed_users
    
    # No user ID provided, use global flag only
    return True


def validate_sql(sql: str, dialect: str = "duckdb") -> tuple[bool, Optional[str]]:
    """
    Validate SQL syntax for a given dialect.
    
    Args:
        sql: SQL string to validate
        dialect: Target dialect
        
    Returns:
        (is_valid, error_message)
        
    Example:
        >>> validate_sql("SELECT * FROM table WHERE id = 1", "duckdb")
        (True, None)
        >>> validate_sql("SELECT * FROM WHERE", "duckdb")
        (False, "Syntax error...")
    """
    try:
        sqlglot.parse_one(sql, dialect=dialect)
        return (True, None)
    except sqlglot.errors.ParseError as e:
        return (False, str(e))
    except Exception as e:
        return (False, str(e))
