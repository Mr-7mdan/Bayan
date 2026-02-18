"""
SQLGlot-based SQL generation for multi-dialect support.
Runs side-by-side with SQLGlot-based SQL builder for multi-dialect query generation.
"""
import sys
sys.stderr.write("[SQLGlot] sqlgen_glot.py MODULE LOADED \n")
sys.stderr.flush()
from typing import Any, Dict, Optional, List
import logging
import re
import sqlglot
from sqlglot import exp
sys.stderr.write(f"[SQLGlot] sqlglot version {sqlglot.__version__} imported \n")
sys.stderr.flush()

logger = logging.getLogger(__name__)


def _sg_norm_name(name: str) -> str:
    """Normalize identifier names for expr_map lookups.

    Strips quotes/brackets, drops schema/alias prefixes, and lowercases.
    This keeps SQLGlot custom-column resolution tolerant to casing/quoting
    differences between frontend field names and datasource transform names.
    """
    s = str(name or "").strip().strip("[]").strip('"').strip("`")
    parts = s.split(".")
    return parts[-1].lower() if parts else s.lower()


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
        x_field: Optional[any] = None,  # Can be str or List[str] for multi-level X
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
            normalized_expr_map: Dict[str, str] = {}
            if expr_map:
                for k, v in expr_map.items():
                    try:
                        nk = _sg_norm_name(k)
                        if nk and nk not in normalized_expr_map:
                            normalized_expr_map[nk] = v
                    except Exception:
                        continue

            # MULTI-SERIES: If series array provided, generate UNION ALL query
            if series and len(series) > 0:
                sys.stderr.write(f"[SQLGlot] Multi-series detected: {len(series)} series\n")
                sys.stderr.flush()
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
                if expr_map:
                    expr = None
                    if field in expr_map:
                        expr = expr_map[field]
                    else:
                        norm_key = _sg_norm_name(field)
                        expr = normalized_expr_map.get(norm_key)

                    if expr is not None:
                        def expand_aliases(expr_str: str, depth: int = 0) -> str:
                            if depth > 10:
                                return expr_str

                            expanded = expr_str
                            matches = re.findall(r'"([^"]+)"', expr_str)
                            for match in matches:
                                alias_expr = None
                                if match in expr_map:
                                    alias_expr = expr_map.get(match)
                                else:
                                    alias_expr = normalized_expr_map.get(_sg_norm_name(match))
                                if alias_expr:
                                    alias_expr = re.sub(r'\s+AS\s+"[^"]+"', '', str(alias_expr), flags=re.IGNORECASE)
                                    alias_expr = re.sub(r'\s+AS\s+\[[^\]]+\]', '', str(alias_expr), flags=re.IGNORECASE)
                                    alias_expr = re.sub(r'^\(("[^"]+"|[a-zA-Z0-9_]+)\)$', r'\1', str(alias_expr).strip())
                                    alias_expr = re.sub(r'"[a-z][a-z_]{0,4}"\.', '', str(alias_expr))
                                    alias_expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', str(alias_expr))
                                    alias_expr = re.sub(r'\[([a-z][a-z_]{0,4})\]\.', '', str(alias_expr))
                                    if 'duckdb' in (ds_type or self.dialect).lower():
                                        alias_expr = re.sub(r'\[([^\]]+)\]', r'"\1"', str(alias_expr))
                                    if 'duckdb' in (ds_type or self.dialect).lower() and re.fullmatch(r'\d+', str(match)):
                                        alias_expr_s = expand_aliases(str(alias_expr), depth + 1)

                                        def _wrap_col(m: re.Match) -> str:
                                            col_name = m.group(1)
                                            return f'(COALESCE(TRY_CAST(REGEXP_REPLACE(CAST("{col_name}" AS TEXT), \'[^0-9\\\\.-]\', \'\') AS DOUBLE), TRY_CAST("{col_name}" AS DOUBLE), 0.0))'

                                        alias_expr_s = re.sub(r'\("([^"]+)"\)', _wrap_col, alias_expr_s)
                                        expanded = expanded.replace(
                                            f'"{match}"',
                                            f'({alias_expr_s})'
                                        )
                                    else:
                                        expanded = expanded.replace(f'"{match}"', f'({alias_expr})')

                            if expanded != expr_str:
                                return expand_aliases(expanded, depth + 1)
                            return expanded

                        expr_s = str(expr)
                        # Debug: Check CASE statements for completeness
                        if "CASE" in expr_s.upper():
                            has_end = "END" in expr_s.upper()
                            sys.stderr.write(f"[SQLGlot] CASE expression for '{field}': length={len(expr_s)}, has_END={has_end}\n")
                            sys.stderr.flush()
                            if not has_end:
                                sys.stderr.write(f"[SQLGlot] ERROR: CASE expression missing END keyword!\n")
                                sys.stderr.write(f"[SQLGlot] Expression: {expr_s}\n")
                                sys.stderr.flush()
                        # Strip table aliases (e.g., s.ClientID -> ClientID, src.OrderDate -> OrderDate)
                        # Only strip short lowercase identifiers (typical aliases), not schema names
                        expr_s = re.sub(r'"[a-z][a-z_]{0,4}"\.', '', expr_s)  # Quoted aliases
                        expr_s = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr_s)  # Unquoted aliases
                        expr_s = re.sub(r'\[([a-z][a-z_]{0,4})\]\.', '', expr_s)  # SQL Server bracket aliases
                        # Convert SQL Server bracket notation to double quotes for DuckDB
                        if 'duckdb' in (ds_type or self.dialect).lower():
                            expr_s = re.sub(r'\[([^\]]+)\]', r'"\1"', expr_s)
                        expr_s = expand_aliases(expr_s)
                        sys.stderr.write(f"[SQLGlot] [OK] Resolving custom column '{field}' in SELECT -> {expr_s[:80]}...\n")
                        sys.stderr.flush()
                        return expr_s, True
                
                # Check if it's a date part pattern
                match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", field, flags=re.IGNORECASE)
                if match:
                    base_col = match.group(1).strip()
                    kind = match.group(2).lower()
                    expr = self._build_datepart_expr(base_col, kind, ds_type or self.dialect)
                    sys.stderr.write(f"[SQLGlot] [OK] Resolving date part '{field}' in SELECT -> {expr[:80]}\n")
                    sys.stderr.flush()
                    return expr, True
                
                return field, False
            
            # SEASONALITY DETECTION: Check for 12-month pattern (x=date, legend=Year, groupBy=month)
            seasonality_mode = False
            legend_base_col = None
            legend_kind = None
            
            # Parse legend field to detect date part pattern
            legend_raw = None
            if legend_fields and len(legend_fields) > 0:
                legend_raw = legend_fields[0]
            elif legend_field:
                legend_raw = legend_field
            
            if legend_raw:
                legend_match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", legend_raw, flags=re.IGNORECASE)
                if legend_match:
                    legend_base_col = legend_match.group(1).strip()
                    legend_kind = legend_match.group(2).lower()
            
            # Detect 12-month seasonality pattern
            # For seasonality check, use primary x field if array
            x_field_check = x_field[0] if isinstance(x_field, (list, tuple)) and len(x_field) > 0 else x_field
            sys.stderr.write(f"[SQLGlot] Seasonality check: group_by={group_by}, legend_base_col={legend_base_col}, legend_kind={legend_kind}, x_field={x_field_check}\n")
            sys.stderr.flush()
            if (group_by == "month" and 
                legend_base_col and legend_kind == "year" and 
                x_field_check and x_field_check == legend_base_col):
                seasonality_mode = True
                sys.stderr.write(f"[SQLGlot] [OK] 12-MONTH SEASONALITY ENABLED: x={x_field_check}, legend={legend_raw}, groupBy=month\n")
                sys.stderr.flush()
            else:
                sys.stderr.write(f"[SQLGlot] [SKIP] Seasonality NOT detected\n")
                sys.stderr.flush()
            
            # Resolve fields before building query
            # Handle x_field as array (multi-level X) - resolve the primary (first) field,
            # but keep the original x_field value so we can still detect multi-level X later.
            x_field_for_resolve = x_field[0] if isinstance(x_field, (list, tuple)) and len(x_field) > 0 else x_field
            x_field_resolved, x_is_expr = resolve_field(x_field_for_resolve)
            y_field_resolved, y_is_expr = resolve_field(y_field)
            sys.stderr.write(f"[SQLGlot] X field resolution: '{x_field}' -> '{x_field_resolved}', is_expr={x_is_expr}\n")
            sys.stderr.write(f"[SQLGlot] Y field resolution: '{y_field}' -> '{y_field_resolved}', is_expr={y_is_expr}\n")
            sys.stderr.flush()

            # x_field_primary is what we actually use for single-level X:
            # - if resolve_field returned an expression, use that
            # - otherwise, use the original field name
            x_field_primary_resolved = x_field_resolved if x_is_expr else x_field_for_resolve

            # MULTI-LEGEND: Handle legend_fields array (single or multiple)
            legend_field_resolved = legend_field
            legend_is_expr = False
            
            if legend_fields and len(legend_fields) > 0:
                if len(legend_fields) == 1:
                    # Single field in array - just resolve it normally
                    sys.stderr.write(f"[SQLGlot] Single legend field in array: {legend_fields[0]}\n")
                    sys.stderr.flush()
                    legend_field_resolved, legend_is_expr = resolve_field(legend_fields[0])
                else:
                    # Multiple fields - concatenate with dialect-aware separator
                    sys.stderr.write(f"[SQLGlot] Multi-legend detected: {len(legend_fields)} fields: {legend_fields}\n")
                    sys.stderr.flush()
                    
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
                    sys.stderr.write(f"[SQLGlot] Multi-legend expression: {legend_field_resolved[:100]}\n")
                    sys.stderr.flush()
            elif legend_field:
                # Single legend field (not in array) - resolve normally
                legend_field_resolved, legend_is_expr = resolve_field(legend_field)
            
            # Use resolved fields for Y/legend; keep original x_field so we still know
            # when the caller passed an array (multi-level X).
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
            x_order_expr = None  # For ordering by month number in seasonality mode
            
            # X field (with optional time bucketing)
            # Multi-level X: when x_field is an array like ["OrderDate (Day)", "OrderDate (Year)"]
            sys.stderr.write(f"[SQLGlot] build_aggregation_query received x_field={x_field}, type={type(x_field)}\n")
            is_multi_level_x = isinstance(x_field, (list, tuple)) and len(x_field) > 1
            x_field_primary = x_field_primary_resolved
            sys.stderr.write(f"[SQLGlot] is_multi_level_x={is_multi_level_x}, x_field_primary={x_field_primary}\n")
            sys.stderr.flush()
            
            if x_field:
                if is_multi_level_x:
                    # MULTI-LEVEL X: Concatenate all x field expressions with '|' delimiter
                    sys.stderr.write(f"[SQLGlot] Multi-level X detected: {x_field}\n")
                    sys.stderr.flush()
                    x_part_exprs = []
                    x_order_parts = []
                    for xf in x_field:
                        # Resolve each x field (check for derived date parts, custom columns)
                        xf_resolved, xf_is_expr = resolve_field(xf)
                        if xf_is_expr:
                            part_expr = xf_resolved
                        else:
                            # Check for derived date part pattern (re is imported at module level)
                            match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", xf, flags=re.IGNORECASE)
                            if match:
                                base_col = match.group(1).strip()
                                kind = match.group(2).lower()
                                part_expr = self._build_datepart_expr(base_col, kind, ds_type or self.dialect)
                                # Also build order expr for this part
                                if kind in ('year', 'quarter', 'month', 'week', 'day'):
                                    x_order_parts.append(part_expr)
                            else:
                                part_expr = f'"{xf_resolved}"'
                                x_order_parts.append(part_expr)
                        x_part_exprs.append(f"CAST({part_expr} AS VARCHAR)")
                    
                    # Build concatenation expression based on dialect
                    dialect_lower = (ds_type or self.dialect or "").lower()
                    if "duckdb" in dialect_lower or "postgres" in dialect_lower:
                        concat_expr = " || '|' || ".join(x_part_exprs)
                    elif "mssql" in dialect_lower or "sqlserver" in dialect_lower:
                        concat_expr = " + '|' + ".join(x_part_exprs)
                    else:
                        # MySQL/SQLite style CONCAT
                        parts_with_sep = []
                        for i, p in enumerate(x_part_exprs):
                            parts_with_sep.append(p)
                            if i < len(x_part_exprs) - 1:
                                parts_with_sep.append("'|'")
                        concat_expr = f"CONCAT({', '.join(parts_with_sep)})"
                    
                    x_expr = sqlglot.parse_one(concat_expr, dialect=self.dialect)
                    # Order by outer level (last) first, then inner levels
                    if x_order_parts:
                        x_order_expr = sqlglot.parse_one(", ".join(reversed(x_order_parts)), dialect=self.dialect)
                    sys.stderr.write(f"[SQLGlot] Multi-level X expr: {concat_expr[:100]}...\n")
                    sys.stderr.flush()
                elif seasonality_mode:
                    # SEASONALITY MODE: Extract month name/short for x, add numeric month for ordering
                    sys.stderr.write(f"[SQLGlot] Building 12-month seasonality x-axis for {x_field_primary}\n")
                    sys.stderr.flush()
                    # Build month short expression (Jan, Feb, Mar...)
                    month_expr = self._build_datepart_expr(x_field_primary, "month short", ds_type or self.dialect)
                    x_expr = sqlglot.parse_one(month_expr, dialect=self.dialect)
                    # Build numeric month for ordering (1-12)
                    month_num_expr = self._build_month_number_expr(x_field_primary, ds_type or self.dialect)
                    x_order_expr = sqlglot.parse_one(month_num_expr, dialect=self.dialect)
                    sys.stderr.write(f"[SQLGlot] Month label expr: {month_expr}\n")
                    sys.stderr.write(f"[SQLGlot] Month order expr: {month_num_expr}\n")
                    sys.stderr.flush()
                elif x_is_expr:
                    # Parse as raw SQL expression
                    x_expr = sqlglot.parse_one(x_field_primary, dialect=self.dialect)
                elif group_by and group_by != "none":
                    x_expr = self._build_time_bucket(x_field_primary, group_by, week_start)
                else:
                    x_expr = exp.Column(this=exp.Identifier(this=x_field_primary, quoted=True))
                select_exprs.append(x_expr.as_("x"))
                group_positions.append(1)
                
                # Add ordering column in seasonality mode
                if seasonality_mode and x_order_expr:
                    select_exprs.append(x_order_expr.as_("_xo"))
                    group_positions.append(len(select_exprs))
            
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
                sys.stderr.write(f"[SQLGlot] WHERE clause before _apply_where: {where}\n")
                sys.stderr.flush()
                query = self._apply_where(query, where, date_field=date_field or x_field, expr_map=expr_map)
                sys.stderr.write(f"[SQLGlot] WHERE clause applied\n")
                sys.stderr.flush()
            
            # Apply GROUP BY (by position)
            sys.stderr.write(f"[SQLGlot] Group positions before GROUP BY: {group_positions}\n")
            sys.stderr.flush()
            if group_positions:
                query = query.group_by(*[exp.Literal.number(i) for i in group_positions])
                sys.stderr.write(f"[SQLGlot] Applied GROUP BY with positions: {group_positions}\n")
                sys.stderr.flush()
            
            # Apply ORDER BY
            if seasonality_mode and x_order_expr:
                # SEASONALITY MODE: Order by numeric month (_xo), then legend
                sys.stderr.write(f"[SQLGlot] [OK] Applying seasonality ordering: ORDER BY _xo, legend\n")
                sys.stderr.flush()
                # Apply ORDER BY inside the query before wrapping
                query = query.order_by(exp.Column(this=exp.Identifier(this="_xo", quoted=False)))
                if legend_field:
                    query = query.order_by(exp.Column(this=exp.Identifier(this="legend", quoted=False)))
                # Wrap in subquery to select only x, legend, value (exclude _xo from output)
                subquery = query.subquery("_seasonality")
                query = sqlglot.select("x", "legend", "value").from_(subquery)
            elif order_by:
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
            
            # Post-process: Convert SQL Server bracket notation to DuckDB double quotes
            if 'duckdb' in self.dialect.lower():
                sql = re.sub(r'\[([^\]]+)\]', r'"\1"', sql)
                sys.stderr.write(f"[SQLGlot] Converted bracket notation to double quotes for DuckDB\n")
                sys.stderr.flush()
            
            # Print full SQL for debugging
            logger.info(f"[SQLGlot] Generated SQL ({self.dialect}): {sql[:500]}...")
            sys.stderr.write(f"[SQLGlot] Generated FULL SQL ({self.dialect}):\n")
            sys.stderr.write(sql + "\n")
            sys.stderr.flush()
            return sql
            
        except Exception as e:
            logger.error(f"[SQLGlot] ERROR generating SQL: {e}")
            sys.stderr.write(f"[SQLGlot] ERROR generating SQL: {e}\n")
            sys.stderr.flush()
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
            # Build a normalized view of expr_map for tolerant lookups
            normalized_expr_map: Dict[str, str] = {}
            if expr_map:
                for k, v in expr_map.items():
                    try:
                        nk = _sg_norm_name(k)
                        if nk and nk not in normalized_expr_map:
                            normalized_expr_map[nk] = v
                    except Exception:
                        continue

            # Helper: resolve custom columns and date parts
            def resolve_field(field_name: Optional[str]) -> tuple[Optional[str], bool]:
                """Resolve field to SQL expression if it's a custom column or date part"""
                if not field_name:
                    return field_name, False
                
                # Check if it's a custom column
                if expr_map:
                    expr = None
                    # Prefer exact match first
                    if field_name in expr_map:
                        expr = expr_map[field_name]
                    else:
                        # Fallback to normalized (case/quote-insensitive) match
                        norm_key = _sg_norm_name(field_name)
                        expr = normalized_expr_map.get(norm_key)
                    if expr is not None:
                        # Strip table aliases
                        expr = re.sub(r'"[a-z][a-z_]{0,4}"\.', '', expr)  # Quoted aliases
                        expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)  # Unquoted aliases
                        expr = re.sub(r'\[([a-z][a-z_]{0,4})\]\.', '', expr)  # SQL Server bracket aliases
                        # Convert SQL Server bracket notation to double quotes for DuckDB
                        if 'duckdb' in (ds_type or self.dialect).lower():
                            expr = re.sub(r'\[([^\]]+)\]', r'"\1"', expr)
                        sys.stderr.write(f"[SQLGlot] [OK] Resolving custom column '{field_name}' in DISTINCT\n")
                        sys.stderr.flush()
                        return expr, True
                
                # Check if it's a date part pattern
                match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", field_name, flags=re.IGNORECASE)
                if match:
                    base_col = match.group(1).strip()
                    kind = match.group(2).lower()
                    expr = self._build_datepart_expr(base_col, kind, ds_type or self.dialect)
                    sys.stderr.write(f"[SQLGlot] [OK] Resolving date part '{field_name}' in DISTINCT\n")
                    sys.stderr.flush()
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
                query = self._apply_where(query, where, expr_map=expr_map)
            
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
            
            # Post-process: Convert SQL Server bracket notation to DuckDB double quotes
            if 'duckdb' in self.dialect.lower():
                sql = re.sub(r'\[([^\]]+)\]', r'"\1"', sql)
                sys.stderr.write(f"[SQLGlot] Converted bracket notation to double quotes for DuckDB\n")
                sys.stderr.flush()
            
            logger.info(f"[SQLGlot] Generated DISTINCT SQL ({self.dialect}): {sql[:150]}...")
            sys.stderr.write(f"[SQLGlot] Generated DISTINCT SQL ({self.dialect}): {sql[:150]}...\n")
            sys.stderr.flush()
            return sql
            
        except Exception as e:
            logger.error(f"[SQLGlot] ERROR generating DISTINCT SQL: {e}")
            sys.stderr.write(f"[SQLGlot] ERROR generating DISTINCT SQL: {e}\n")
            sys.stderr.flush()
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
                    expr = re.sub(r'"[a-z][a-z_]{0,4}"\.', '', expr)  # Quoted aliases
                    expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)  # Unquoted aliases
                    expr = re.sub(r'\[([a-z][a-z_]{0,4})\]\.', '', expr)  # SQL Server bracket aliases
                    # Convert SQL Server bracket notation to double quotes for DuckDB
                    if 'duckdb' in (ds_type or self.dialect).lower():
                        expr = re.sub(r'\[([^\]]+)\]', r'"\1"', expr)
                    sys.stderr.write(f"[SQLGlot] [OK] Resolving custom column '{field_name}' in period-totals\n")
                    sys.stderr.flush()
                    return expr, True
                
                # Check if it's a date part pattern
                match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", field_name, flags=re.IGNORECASE)
                if match:
                    base_col = match.group(1).strip()
                    kind = match.group(2).lower()
                    expr = self._build_datepart_expr(base_col, kind, ds_type or self.dialect)
                    sys.stderr.write(f"[SQLGlot] [OK] Resolving date part '{field_name}' in period-totals\n")
                    sys.stderr.flush()
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
                query = self._apply_where(query, where, expr_map=expr_map)
            
            # Apply GROUP BY (if legend field exists)
            if legend_field_resolved:
                query = query.group_by(1)  # Group by position
            
            # Generate SQL
            sql = query.sql(dialect=self.dialect, pretty=False)
            
            # Post-process: Convert SQL Server bracket notation to DuckDB double quotes
            if 'duckdb' in self.dialect.lower():
                sql = re.sub(r'\[([^\]]+)\]', r'"\1"', sql)
                sys.stderr.write(f"[SQLGlot] Converted bracket notation to double quotes for DuckDB\n")
                sys.stderr.flush()
            
            logger.info(f"[SQLGlot] Generated period-totals SQL ({self.dialect}): {sql[:150]}...")
            sys.stderr.write(f"[SQLGlot] Generated period-totals SQL ({self.dialect}): {sql[:150]}...\n")
            sys.stderr.flush()
            return sql
            
        except Exception as e:
            logger.error(f"[SQLGlot] ERROR generating period-totals SQL: {e}")
            sys.stderr.write(f"[SQLGlot] ERROR generating period-totals SQL: {e}\n")
            sys.stderr.flush()
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
                y_expr_s = y_field
                if self.dialect == "duckdb" and agg_lower in ("sum", "avg"):
                    def _wrap_col(m: re.Match) -> str:
                        col_name = m.group(1)
                        return f'(COALESCE(TRY_CAST(REGEXP_REPLACE(CAST("{col_name}" AS TEXT), \'[^0-9\\\\.-]\', \'\') AS DOUBLE), TRY_CAST("{col_name}" AS DOUBLE), 0.0))'

                    y_expr_s = re.sub(r'\("([^"]+)"\)', _wrap_col, y_expr_s)
                col_expr = sqlglot.parse_one(y_expr_s, dialect=self.dialect)
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
        date_field: Optional[str] = None,
        expr_map: Optional[Dict[str, str]] = None
    ) -> exp.Select:
        """
        Apply WHERE clause filters.
        
        Handles:
        - Single values: field = 'value'
        - Lists: field IN ('val1', 'val2')
        - None: field IS NULL
        - Comparison operators: field__gte, field__gt, field__lte, field__lt
        - Date ranges: start, startDate, end, endDate (requires x_field)
        - Custom columns: Resolves custom column names to their expressions
        
        Note: Does NOT validate column existence - trusts database to handle it.
        This allows derived columns and transforms to work.
        """
        import sys
        sys.stderr.write(f"[SQLGLOT_WHERE_DEBUG] _apply_where received WHERE keys: {list(where.keys()) if where else 'None'}\n")
        sys.stderr.flush()
        
        # Build a normalized view of expr_map for tolerant WHERE lookups
        normalized_expr_map: Dict[str, str] = {}
        if expr_map:
            for k, v in expr_map.items():
                try:
                    nk = _sg_norm_name(k)
                    if nk and nk not in normalized_expr_map:
                        normalized_expr_map[nk] = v
                except Exception:
                    continue

        # Collect all WHERE conditions to combine with AND at the end
        conditions = []

        try:
            where_work: Dict[str, Any] = dict(where or {})
            sys.stderr.write(f"[SQLGLOT_WHERE_DEBUG] _apply_where working items: {where_work}\n")
            sys.stderr.flush()
        except Exception:
            where_work = where or {}

        try:
            dp_eq: Dict[str, Dict[str, List[int]]] = {}
            for k, v in list(where_work.items()):
                if not isinstance(k, str):
                    continue
                if "__" in k:
                    continue
                m = re.match(r"^(.*)\s*\((Year|Month)\)$", k.strip(), flags=re.IGNORECASE)
                if not m:
                    continue
                base_col = m.group(1).strip()
                kind = m.group(2).strip().lower()
                
                vals = []
                if isinstance(v, (list, tuple)):
                    vals = list(v)
                else:
                    vals = [v]
                
                int_vals = []
                for x in vals:
                    if isinstance(x, int):
                        int_vals.append(x)
                    elif isinstance(x, str) and re.fullmatch(r"\d+", x.strip()):
                        try:
                            int_vals.append(int(x.strip()))
                        except Exception:
                            pass
                
                if not int_vals:
                    sys.stderr.write(f"[SQLGLOT_WHERE_DEBUG] Skipping {k} because values {v} contain no valid ints\n")
                    sys.stderr.flush()
                    continue

                sys.stderr.write(f"[SQLGLOT_WHERE_DEBUG] Found date part filter: {k} = {int_vals}\n")
                sys.stderr.flush()

                if base_col:
                    current = dp_eq.setdefault(base_col, {})
                    existing = current.get(kind, [])
                    # distinct sorted
                    current[kind] = sorted(list(set(existing + int_vals)))
            
            if dp_eq:
                sys.stderr.write(f"[SQLGLOT_WHERE_DEBUG] Potential date rewrites: {dp_eq}\n")
                sys.stderr.flush()

            for base_col, parts in (dp_eq or {}).items():
                years = parts.get("year", [])
                months = parts.get("month", [])
                
                if not years:
                    continue

                import datetime
                ranges = []
                
                for yv in years:
                    if months:
                        # Cartesian product of Year x Month
                        for mv in months:
                            if mv < 1 or mv > 12:
                                continue
                            try:
                                start_dt = datetime.date(yv, mv, 1)
                                if mv == 12:
                                    end_dt = datetime.date(yv + 1, 1, 1)
                                else:
                                    end_dt = datetime.date(yv, mv + 1, 1)
                                ranges.append((start_dt, end_dt))
                            except Exception:
                                pass
                    else:
                        # Whole year
                        try:
                            start_dt = datetime.date(yv, 1, 1)
                            end_dt = datetime.date(yv + 1, 1, 1)
                            ranges.append((start_dt, end_dt))
                        except Exception:
                            pass

                if ranges:
                    try:
                        where_work.pop(f"{base_col} (Year)", None)
                        where_work.pop(f"{base_col} (Month)", None)
                    except Exception:
                        pass
                    
                    col = exp.Column(this=exp.Identifier(this=base_col, quoted=True))
                    range_conds = []
                    for start_dt, end_dt in ranges:
                        lit_start = exp.Cast(this=exp.Literal.string(start_dt.isoformat()), to=exp.DataType.build("DATE"))
                        lit_end = exp.Cast(this=exp.Literal.string(end_dt.isoformat()), to=exp.DataType.build("DATE"))
                        # col >= start AND col < end
                        range_conds.append(exp.And(this=(col >= lit_start), expression=(col < lit_end)))
                    
                    if range_conds:
                        if len(range_conds) == 1:
                            conditions.append(range_conds[0])
                        else:
                            # Combine with OR
                            final_or = range_conds[0]
                            for rc in range_conds[1:]:
                                final_or = exp.Or(this=final_or, expression=rc)
                            conditions.append(final_or)
                        
                        sys.stderr.write(f"[SQLGLOT_WHERE_DEBUG] Applied sargable date range rewrite for {base_col} ({len(ranges)} ranges)\n")
                        sys.stderr.flush()

        except Exception as e:
            sys.stderr.write(f"[SQLGLOT_WHERE_DEBUG] Error in date rewrite: {e}\n")
            sys.stderr.flush()
            pass
        
        where = where_work

        def resolve_where_field(field_name: str) -> tuple[Any, bool]:
            """Resolve a WHERE field to its SQL expression if it's a custom column"""
            if not field_name:
                return field_name, False
            
            # Check if it's a custom column
            if expr_map:
                expr = None
                if field_name in expr_map:
                    expr = expr_map[field_name]
                else:
                    norm_key = _sg_norm_name(field_name)
                    expr = normalized_expr_map.get(norm_key)
                if expr is not None:
                    # Strip table aliases (e.g., s.ClientID -> ClientID)
                    expr = re.sub(r'"[a-z][a-z_]{0,4}"\.', '', expr)  # Quoted aliases
                    expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)  # Unquoted aliases
                    expr = re.sub(r'\[([a-z][a-z_]{0,4})\]\.', '', expr)  # SQL Server bracket aliases
                    # Convert SQL Server bracket notation to double quotes for DuckDB
                    if 'duckdb' in self.dialect.lower():
                        expr = re.sub(r'\[([^\]]+)\]', r'"\1"', expr)
                    # Expand nested alias references (e.g., Brinks references SourceRegion/DestRegion)
                    # so the final expression doesn't rely on non-materialized aliases.
                    try:
                        def _expand_aliases_where(_expr_s: str, _depth: int = 0) -> str:
                            if _depth > 10:
                                sys.stderr.write(f"[SQLGlot] WARNING: Max recursion depth reached in WHERE alias expansion\n")
                                sys.stderr.flush()
                                return _expr_s
                            expanded = _expr_s
                            try:
                                matches = re.findall(r'"([^"]+)"', _expr_s)
                            except Exception:
                                matches = []
                            
                            sys.stderr.write(f"[SQLGlot] WHERE expansion depth={_depth}, found {len(matches)} quoted identifiers\n")
                            sys.stderr.flush()
                            
                            for match in matches:
                                m_s = str(match or "").strip()
                                if not m_s:
                                    continue
                                alias_expr = None
                                if m_s in expr_map:
                                    alias_expr = expr_map[m_s]
                                    sys.stderr.write(f"[SQLGlot] Found '{m_s}' in expr_map\n")
                                    sys.stderr.flush()
                                else:
                                    alias_expr = normalized_expr_map.get(_sg_norm_name(m_s))
                                    if alias_expr:
                                        sys.stderr.write(f"[SQLGlot] Found '{m_s}' in normalized_expr_map\n")
                                        sys.stderr.flush()
                                if alias_expr is None:
                                    continue
                                alias_expr_s = str(alias_expr)
                                alias_expr_s = re.sub(r'\s+AS\s+"[^"]+"', '', alias_expr_s, flags=re.IGNORECASE)
                                sys.stderr.write(f"[SQLGlot] Expanding '{m_s}' -> {alias_expr_s[:100]}...\n")
                                sys.stderr.flush()
                                expanded = expanded.replace(f'"{m_s}"', f'({alias_expr_s})')
                            if expanded != _expr_s:
                                sys.stderr.write(f"[SQLGlot] Expression changed, recursing to depth {_depth + 1}\n")
                                sys.stderr.flush()
                                return _expand_aliases_where(expanded, _depth + 1)
                            return expanded

                        expr = _expand_aliases_where(str(expr))
                    except Exception:
                        pass
                    sys.stderr.write(f"[SQLGlot] [OK] Resolving custom column '{field_name}' in WHERE -> {expr[:80]}...\n")
                    sys.stderr.flush()
                    # Parse the expression and return it
                    try:
                        return sqlglot.parse_one(expr, dialect=self.dialect), True
                    except Exception as e:
                        logger.warning(f"[SQLGlot] Failed to parse custom column expression '{expr}': {e}")
                        return field_name, False
            
            return field_name, False

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
                    field_s = str(field or "").strip()
                    col_override = None
                    if field_s.startswith("(") and field_s.endswith(")"):
                        try:
                            expr_sql = field_s[1:-1]
                            col_override = sqlglot.parse_one(expr_sql, dialect=self.dialect)
                        except Exception as e:
                            logger.warning(f"[SQLGlot] Failed to parse expression '{field_s}': {e}")
                    
                    # Extract scalar value for operators that expect it (if value is a list)
                    # 'ne' operator handles lists natively (as NOT IN), so exclude it
                    if isinstance(value, (list, tuple)) and operator != "ne":
                        value = value[0] if len(value) > 0 else None
                        
                    sys.stderr.write(f"[SQLGlot] _apply_where: Processing comparison {field}__{operator} = {value}\n")
                    sys.stderr.flush()
                    value_expr = None

                    if col_override is not None:
                        col = col_override
                    # Support derived date-part filters like "DueDate (Year)__gte" by expanding
                    # them to expressions on the base column instead of expecting a physical
                    # "DueDate (Year)" column in the FROM clause.
                    dp_match = re.match(
                        r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$",
                        field,
                        flags=re.IGNORECASE,
                    )
                    if col_override is None and dp_match:
                        base_col = dp_match.group(1).strip()
                        kind = dp_match.group(2)
                        try:
                            kind_l = str(kind or "").strip().lower()

                            # If the client sends ISO date bounds for a derived date-part filter
                            # (e.g., DueDate (Year)__gte = '2025-01-01' or DueDate (Month Name)__lt = '2025-12-01'),
                            # treat it as a comparison on the base date column.
                            is_iso_date = (
                                isinstance(value, str)
                                and re.match(r"^\d{4}-\d{2}-\d{2}", value.strip()) is not None
                                and operator in {"gte", "gt", "lte", "lt"}
                            )
                            if is_iso_date:
                                col = exp.Column(this=exp.Identifier(this=base_col, quoted=True))
                                iso = value.strip()[:10]
                                if "duckdb" in (self.dialect or "").lower():
                                    value_expr = exp.Cast(
                                        this=exp.Literal.string(iso),
                                        to=exp.DataType.build("DATE"),
                                    )
                                else:
                                    value_expr = exp.Literal.string(iso)
                                sys.stderr.write(
                                    f"[SQLGlot] _apply_where: Using base date column for {field}__{operator} (iso date bound {iso})\n"
                                )
                                sys.stderr.flush()
                            else:
                                expr_sql = self._build_datepart_expr(base_col, kind, self.dialect)
                                col = sqlglot.parse_one(expr_sql, dialect=self.dialect)
                                # Best-effort type alignment for comparisons on date parts.
                                # - Numeric date parts (year/quarter/month/week/day) should compare to numbers.
                                # - Name-based date parts (month name/short, day name/short) should compare to strings.
                                numeric_kinds = {"year", "quarter", "month", "week", "day"}
                                if kind_l in numeric_kinds:
                                    if isinstance(value, str):
                                        s_val = value.strip()
                                        # Only coerce if the whole string is digits (avoid turning '2025-01' into 2025)
                                        if re.fullmatch(r"\d+", s_val):
                                            try:
                                                value = int(s_val)
                                            except Exception:
                                                pass
                                else:
                                    # Ensure we don't compare VARCHAR expressions to numeric literals
                                    if isinstance(value, (int, float)):
                                        value = str(int(value)) if isinstance(value, int) else str(value)
                                sys.stderr.write(f"[SQLGlot] _apply_where: Using datepart expr for {field} -> {expr_sql}\n")
                                sys.stderr.flush()
                        except Exception as e:
                            logger.warning(f"[SQLGlot] Failed to build datepart expr for '{field}': {e}")
                            # Fallback to regular resolution path below
                            resolved, is_custom = resolve_where_field(field)
                            if is_custom:
                                col = resolved
                                sys.stderr.write(f"[SQLGlot] _apply_where: Resolved {field} to custom column expression\n")
                                sys.stderr.flush()
                            else:
                                col = exp.Column(this=exp.Identifier(this=field, quoted=True))
                                sys.stderr.write(f"[SQLGlot] _apply_where: Using column {field}\n")
                                sys.stderr.flush()
                    elif col_override is None:
                        # Try to resolve as custom column first
                        resolved, is_custom = resolve_where_field(field)
                        if is_custom:
                            col = resolved
                            sys.stderr.write(f"[SQLGlot] _apply_where: Resolved {field} to custom column expression\n")
                            sys.stderr.flush()
                        else:
                            col = exp.Column(this=exp.Identifier(this=field, quoted=True))
                            sys.stderr.write(f"[SQLGlot] _apply_where: Using column {field}\n")
                            sys.stderr.flush()
                    
                    # Build condition expression
                    condition = None
                    lit_value = value_expr if value_expr is not None else self._to_literal(value)
                    if operator == "gte":
                        condition = col >= lit_value
                        sys.stderr.write(f"[SQLGlot] _apply_where: Applied {field} >= {value}\n")
                        sys.stderr.flush()
                    elif operator == "gt":
                        condition = col > lit_value
                        sys.stderr.write(f"[SQLGlot] _apply_where: Applied {field} > {value}\n")
                        sys.stderr.flush()
                    elif operator == "lte":
                        condition = col <= lit_value
                        sys.stderr.write(f"[SQLGlot] _apply_where: Applied {field} <= {value}\n")
                        sys.stderr.flush()
                    elif operator == "lt":
                        condition = col < lit_value
                        sys.stderr.write(f"[SQLGlot] _apply_where: Applied {field} < {value}\n")
                        sys.stderr.flush()
                    elif operator == "ne":
                        # NOT EQUALS: use NOT IN for arrays, != for scalars
                        if isinstance(value, (list, tuple)) and len(value) > 0:
                            literals = [self._to_literal(v) for v in value]
                            condition = ~col.isin(*literals)
                            sys.stderr.write(f"[SQLGlot] _apply_where: Applied {field} NOT IN {value}\n")
                            sys.stderr.flush()
                        else:
                            condition = col != lit_value
                            sys.stderr.write(f"[SQLGlot] _apply_where: Applied {field} != {value}\n")
                            sys.stderr.flush()
                    elif operator in {"contains", "notcontains", "startswith", "endswith"}:
                        # String matching operators
                        if operator == "contains":
                            pattern = f"%{value}%"
                            condition = col.like(self._to_literal(pattern))
                            sys.stderr.write(f"[SQLGlot] _apply_where: Applied {field} LIKE '%{value}%'\n")
                            sys.stderr.flush()
                        elif operator == "notcontains":
                            pattern = f"%{value}%"
                            condition = ~col.like(self._to_literal(pattern))
                            sys.stderr.write(f"[SQLGlot] _apply_where: Applied {field} NOT LIKE '%{value}%'\n")
                            sys.stderr.flush()
                        elif operator == "startswith":
                            pattern = f"{value}%"
                            condition = col.like(self._to_literal(pattern))
                            sys.stderr.write(f"[SQLGlot] _apply_where: Applied {field} LIKE '{value}%'\n")
                            sys.stderr.flush()
                        elif operator == "endswith":
                            pattern = f"%{value}"
                            condition = col.like(self._to_literal(pattern))
                            sys.stderr.write(f"[SQLGlot] _apply_where: Applied {field} LIKE '%{value}'\n")
                            sys.stderr.flush()
                    else:
                        # Unknown operator, treat as regular field name
                        col = exp.Column(this=exp.Identifier(this=key, quoted=True))
                        condition = col == self._to_literal(value)
                        sys.stderr.write(f"[SQLGlot] _apply_where: Unknown operator {operator}, treating as equality\n")
                        sys.stderr.flush()
                    
                    # Collect conditions instead of applying immediately
                    if condition is not None:
                        conditions.append(condition)
                    continue
                
                # Handle date range filters
                if key in ("start", "startDate") and date_field:
                    # Resolve date_field if it's a custom column
                    resolved_date, is_custom_date = resolve_where_field(date_field)
                    if is_custom_date:
                        date_col = resolved_date
                    else:
                        date_col = exp.Column(this=exp.Identifier(this=date_field, quoted=True))
                    conditions.append(date_col >= self._to_literal(value))
                    continue
                elif key in ("end", "endDate") and date_field:
                    # Resolve date_field if it's a custom column
                    resolved_date, is_custom_date = resolve_where_field(date_field)
                    if is_custom_date:
                        date_col = resolved_date
                    else:
                        date_col = exp.Column(this=exp.Identifier(this=date_field, quoted=True))
                    conditions.append(date_col <= self._to_literal(value))
                    continue
                elif key in ("start", "startDate", "end", "endDate"):
                    # Date range key but no date_field specified, skip
                    logger.warning(f"[SQLGlot] Date range filter '{key}' ignored: no date_field specified")
                    continue
                
                # Regular column name - handle derived date-part keys like "DueDate (Year)"
                # by expanding them to expressions on the base column. This avoids relying
                # on a physical "DueDate (Year)" column in the source.
                dp_match = re.match(
                    r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$",
                    key,
                    flags=re.IGNORECASE,
                )
                if dp_match:
                    base_col = dp_match.group(1).strip()
                    kind = dp_match.group(2)
                    try:
                        expr_sql = self._build_datepart_expr(base_col, kind, self.dialect)
                        col = sqlglot.parse_one(expr_sql, dialect=self.dialect)
                        sys.stderr.write(f"[SQLGlot] _apply_where: Using datepart expr for {key} -> {expr_sql}\n")
                        sys.stderr.flush()
                    except Exception as e:
                        logger.warning(f"[SQLGlot] Failed to build datepart expr for '{key}': {e}")
                        resolved, is_custom = resolve_where_field(key)
                        if is_custom:
                            col = resolved
                        else:
                            col = exp.Column(this=exp.Identifier(this=key, quoted=True))
                else:
                    # Regular column name - try to resolve as custom column first
                    resolved, is_custom = resolve_where_field(key)
                    if is_custom:
                        col = resolved
                    else:
                        col = exp.Column(this=exp.Identifier(this=key, quoted=True))
            
            # Build condition based on value type
            if value is None:
                # NULL check
                conditions.append(col.is_(exp.null()))
            elif isinstance(value, list):
                # IN clause
                if len(value) > 0:
                    literals = [self._to_literal(v) for v in value]
                    conditions.append(col.isin(*literals))
            else:
                # Equality
                conditions.append(col.eq(self._to_literal(value)))
        
        # Combine all conditions with AND
        if conditions:
            combined_condition = conditions[0]
            for condition in conditions[1:]:
                combined_condition = combined_condition & condition
            query = query.where(combined_condition)
            sys.stderr.write(f"[SQLGlot] _apply_where: Applied {len(conditions)} conditions combined with AND\n")
            sys.stderr.flush()
        
        return query
    
    def _build_datepart_expr(self, base_col: str, kind: str, dialect: str) -> str:
        """Build dialect-specific date part expression"""
        q = f'"{base_col}"'
        kind_l = kind.lower()
        dial = dialect.lower()
        
        # DuckDB
        if "duckdb" in dial:
            if kind_l == 'year': return f"EXTRACT(YEAR FROM {q})"  # Return integer, not string
            if kind_l == 'quarter': return f"EXTRACT(QUARTER FROM {q})"  # Return integer, not string
            if kind_l == 'month': return f"EXTRACT(MONTH FROM {q})"  # Return integer, not string
            if kind_l == 'month name': return f"strftime({q}, '%B')"
            if kind_l == 'month short': return f"strftime({q}, '%b')"
            if kind_l == 'week': return f"EXTRACT(WEEK FROM {q})"  # Return integer, not string
            if kind_l == 'day': return f"EXTRACT(DAY FROM {q})"  # Return integer, not string
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
            if kind_l == 'year': return f"YEAR({q})"  # Return integer, not string
            if kind_l == 'quarter': return f"DATEPART(QUARTER, {q})"  # Return integer, not string
            if kind_l == 'month': return f"MONTH({q})"  # Return integer, not string
            if kind_l == 'month name': return f"DATENAME(month, {q})"
            if kind_l == 'month short': return f"LEFT(DATENAME(month, {q}), 3)"
            if kind_l == 'week': return f"DATEPART(ISO_WEEK, {q})"  # Return integer, not string
            if kind_l == 'day': return f"DAY({q})"  # Return integer, not string
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
    
    def _build_month_number_expr(self, base_col: str, dialect: str) -> str:
        """Build dialect-specific numeric month extraction (1-12) for ordering"""
        q = f'"{base_col}"'
        dial = dialect.lower()
        
        # DuckDB
        if "duckdb" in dial:
            return f"EXTRACT(month FROM {q})"
        
        # PostgreSQL
        elif "postgres" in dial or "postgre" in dial:
            return f"EXTRACT(month FROM {q})"
        
        # MSSQL
        elif "mssql" in dial or "sqlserver" in dial or "tsql" in dial:
            return f"MONTH({q})"
        
        # MySQL
        elif "mysql" in dial:
            return f"MONTH({q})"
        
        # Fallback
        return f"EXTRACT(month FROM {q})"
    
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
        date_format: Optional[str] = None,
        date_columns: Optional[List[str]] = None,
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
            # Normalize dialect name for SQLGlot
            normalized_dialect = self._normalize_dialect(ds_type or self.dialect)

            normalized_expr_map: Dict[str, str] = {}
            if expr_map:
                for k, v in expr_map.items():
                    try:
                        nk = _sg_norm_name(k)
                        if nk and nk not in normalized_expr_map:
                            normalized_expr_map[nk] = v
                    except Exception:
                        continue
            
            # Helper to resolve fields (reuse from build_aggregation_query)
            def resolve_field(field: Optional[str]) -> tuple[Optional[str], bool]:
                if not field:
                    return field, False
                
                # Check custom columns
                expr = None
                if expr_map:
                    if field in expr_map:
                        expr = expr_map[field]
                    else:
                        expr = normalized_expr_map.get(_sg_norm_name(field))
                if expr is not None:
                    sys.stderr.write(f"[SQLGlot] Pivot: Resolved '{field}' -> {expr[:100]}...\n")
                    sys.stderr.flush()
                    expr = re.sub(r'"[a-z][a-z_]{0,4}"\.', '', expr)  # Quoted aliases
                    expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)  # Unquoted aliases
                    return expr, True
                else:
                    sys.stderr.write(f"[SQLGlot] Pivot: '{field}' not in expr_map (has {len(expr_map) if expr_map else 0} entries, keys={list(expr_map.keys()) if expr_map else []})\n")
                    sys.stderr.flush()
                
                # Check date parts
                match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", field, flags=re.IGNORECASE)
                if match:
                    base_col = match.group(1).strip()
                    kind = match.group(2).lower()
                    expr = self._build_datepart_expr(base_col, kind, normalized_dialect)
                    sys.stderr.write(f"[SQLGlot] Pivot: Resolved date part '{field}' -> {expr[:100]}...\n")
                    sys.stderr.flush()
                    return expr, True
                
                sys.stderr.write(f"[SQLGlot] Pivot: '{field}' is a plain column (no custom expr, no date part)\n")
                sys.stderr.flush()
                return field, False
            
            # Build base query (support subqueries or aliased sources)
            s_source = str(source).strip()
            sys.stderr.write(f"[SQLGlot] build_pivot_query: source parameter (first 200 chars): {s_source[:200]}\n")
            sys.stderr.write(f"[SQLGlot] build_pivot_query: source starts with '(': {s_source.startswith('(')}\n")
            sys.stderr.write(f"[SQLGlot] build_pivot_query: source starts with 'select': {s_source.lower().startswith('select')}\n")
            sys.stderr.flush()
            try:
                # If the source looks like a subquery or raw SELECT, parse it as an expression
                if s_source.startswith("(") or s_source.lower().startswith("select"):
                    table_expr = sqlglot.parse_one(s_source, dialect=normalized_dialect)
                    sys.stderr.write(f"[SQLGlot] build_pivot_query: Parsed source as subquery/SELECT\n")
                    sys.stderr.flush()
                else:
                    table_expr = exp.to_table(s_source, dialect=normalized_dialect)
                    sys.stderr.write(f"[SQLGlot] build_pivot_query: Converted source to table\n")
                    sys.stderr.flush()
            except Exception as e:
                # Fallback to table conversion
                sys.stderr.write(f"[SQLGlot] build_pivot_query: Exception parsing source: {e}, falling back to table conversion\n")
                sys.stderr.flush()
                table_expr = exp.to_table(s_source, dialect=normalized_dialect)
            query = exp.select("*").from_(table_expr)
            sys.stderr.write(f"[SQLGlot] build_pivot_query: Initial query FROM clause: {query.sql(dialect=normalized_dialect)[:200]}\n")
            sys.stderr.flush()
            
            # Resolve all dimension fields (deduplicate to avoid "column specified multiple times" errors)
            all_dims_raw = rows + cols
            # Preserve order but remove duplicates (case-insensitive)
            seen_dims = set()
            all_dims = []
            for dim in all_dims_raw:
                dim_lower = dim.lower()
                if dim_lower not in seen_dims:
                    all_dims.append(dim)
                    seen_dims.add(dim_lower)
            
            select_exprs = []
            group_positions = []  # Positions for GROUP BY (DuckDB, Postgres)
            group_columns = []    # Column names for GROUP BY (SQL Server)
            
            # Helper to apply date formatting
            def apply_date_format(col_expr: str, col_name: str) -> str:
                """Apply date formatting to a column if it's in the date_columns list"""
                if not date_format or not date_columns or col_name not in date_columns:
                    return col_expr
                
                # Convert format string to SQL format function
                # DD-MM-YYYY -> strftime('%d-%m-%Y', col) for DuckDB
                format_map = {
                    'DD': '%d',
                    'MM': '%m',
                    'YYYY': '%Y',
                    'YY': '%y',
                    'HH': '%H',
                    'mm': '%M',
                    'ss': '%S'
                }
                sql_format = date_format
                for key, val in format_map.items():
                    sql_format = sql_format.replace(key, val)
                
                # Apply dialect-specific formatting
                if normalized_dialect == 'duckdb':
                    return f"strftime({col_expr}, '{sql_format}')"
                elif normalized_dialect == 'postgres':
                    return f"to_char({col_expr}, '{sql_format.replace('%', '')}')"
                elif normalized_dialect == 'mysql':
                    return f"DATE_FORMAT({col_expr}, '{sql_format}')"
                elif normalized_dialect == 'mssql':
                    # MSSQL uses FORMAT function with .NET format strings
                    mssql_format = date_format.replace('DD', 'dd').replace('MM', 'MM').replace('YYYY', 'yyyy')
                    return f"FORMAT({col_expr}, '{mssql_format}')"
                else:
                    return col_expr
            
            for idx, dim in enumerate(all_dims, 1):  # 1-indexed for SQL
                dim_resolved, is_expr = resolve_field(dim)
                if is_expr:
                    # Parse as raw SQL expression
                    dim_expr = sqlglot.parse_one(dim_resolved, dialect=normalized_dialect)
                    select_exprs.append(dim_expr.as_(dim))
                    group_positions.append(idx)
                    group_columns.append(exp.column(dim))  # Use the alias for GROUP BY
                else:
                    # Simple column reference - avoid self-aliasing (e.g., VaultName AS VaultName)
                    # which causes DuckDB to reject GROUP BY references to that column
                    col_name = dim_resolved or dim
                    
                    # Check if this column needs date formatting
                    if date_format and date_columns and dim in date_columns:
                        formatted_expr = apply_date_format(col_name, dim)
                        # Parse the formatted expression and add as aliased column
                        formatted_parsed = sqlglot.parse_one(formatted_expr, dialect=normalized_dialect)
                        select_exprs.append(formatted_parsed.as_(dim))
                        group_columns.append(exp.column(dim))  # Use the alias for GROUP BY
                    elif col_name.lower() == dim.lower():
                        # No alias needed - just select the column
                        select_exprs.append(exp.column(col_name))
                        group_columns.append(exp.column(col_name))
                    else:
                        # Alias needed (e.g., different source column)
                        select_exprs.append(exp.column(col_name).as_(dim))
                        group_columns.append(exp.column(dim))  # Use the alias for GROUP BY
                    group_positions.append(idx)
            
            # NOTE: Do NOT apply Sankey-specific column aliases here.
            # Both pivot tables and Sankey charts use this function, but they need different formats:
            # - Pivot tables need actual column names
            # - Sankey charts can rename columns on the frontend if needed
            # Applying Sankey aliases here breaks pivot table widgets.
            
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
                    val_resolved, is_val_expr = resolve_field(value_field)
                    
                    # If val_resolved contains references to custom column aliases (e.g., "20", "50"),
                    # we need to recursively expand them to their base expressions
                    if is_val_expr and expr_map:
                        # Recursively expand alias references in the expression
                        def expand_aliases(expr_str: str, depth: int = 0) -> str:
                            if depth > 10:  # Prevent infinite recursion
                                return expr_str
                            
                            # Find all quoted identifiers that might be aliases
                            pattern = r'"([^"]+)"'
                            matches = re.findall(pattern, expr_str)
                            
                            expanded = expr_str
                            for match in matches:
                                # Check if this is a custom column alias
                                if match in expr_map:
                                    # Get the base expression for this alias
                                    alias_expr = expr_map[match]
                                    # Remove AS alias part if present
                                    alias_expr = re.sub(r'\s+AS\s+"[^"]+"', '', alias_expr, flags=re.IGNORECASE)
                                    # Strip parentheses around simple column references
                                    alias_expr = re.sub(r'^\(("[^"]+"|[a-zA-Z0-9_]+)\)$', r'\1', alias_expr.strip())
                                    # Replace the alias reference with its base expression
                                    expanded = expanded.replace(f'"{match}"', f'({alias_expr})')
                            
                            # Recursively expand if we made changes
                            if expanded != expr_str:
                                return expand_aliases(expanded, depth + 1)
                            return expanded
                        
                        val_resolved = expand_aliases(val_resolved)
                        sys.stderr.write(f"[SQLGlot] Pivot: Expanded aliases in value expression: {val_resolved[:200]}...\n")
                        sys.stderr.flush()
                        
                        # For DuckDB, wrap each base column reference with numeric cleaning
                        # to handle VARCHAR columns that need to be cast to numbers before addition
                        if "duckdb" in (ds_type or self.dialect).lower():
                            # Find all quoted column references (e.g., "Category1", "Category2")
                            col_pattern = r'\("([^"]+)"\)'
                            def wrap_column(match):
                                col_name = match.group(1)
                                # Wrap each column with numeric cleaning
                                return f'(COALESCE(TRY_CAST(REGEXP_REPLACE(CAST("{col_name}" AS TEXT), \'[^0-9\\\\.-]\', \'\') AS DOUBLE), TRY_CAST("{col_name}" AS DOUBLE), 0.0))'
                            val_resolved = re.sub(col_pattern, wrap_column, val_resolved)
                            sys.stderr.write(f"[SQLGlot] Pivot: Wrapped columns with numeric cleaning: {val_resolved[:200]}...\n")
                            sys.stderr.flush()
                    
                    # DuckDB numeric cleaning
                    if "duckdb" in normalized_dialect.lower():
                        # For expressions, use the cleaned val_resolved directly
                        if is_val_expr and expr_map:
                            # Parse the entire cleaned expression
                            clean_expr = sqlglot.parse_one(val_resolved, dialect=normalized_dialect)
                        else:
                            # For simple columns, use TRY_CAST only (no aggressive REGEXP_REPLACE)
                            # This preserves original values and matches source database results
                            col_name = val_resolved or value_field
                            col_ref = exp.column(col_name)
                            
                            # Build: TRY_CAST(col AS DOUBLE)
                            # DuckDB's TRY_CAST returns NULL for invalid values, SUM ignores NULLs
                            clean_expr = exp.TryCast(this=col_ref, to=exp.DataType.build("DOUBLE"))
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
                # For pivot, custom columns are already materialized in the FROM subquery
                # So we should NOT expand them in WHERE - just reference the alias names
                # Pass empty expr_map to prevent expansion
                final_query = self._apply_where(final_query, where, expr_map=expr_map or {})
            
            # Add GROUP BY (use columns for SQL Server, positions for others)
            if group_positions:
                if normalized_dialect == 'tsql':
                    # SQL Server requires actual column references, not positions
                    # Use string-based GROUP BY to force column names instead of positions
                    sys.stderr.write(f"[SQLGlot] Pivot: GROUP BY columns for SQL Server: {[str(c) for c in group_columns]}\n")
                    sys.stderr.flush()
                    # Don't use .group_by() as it may simplify to positions - manually add to SQL
                    # We'll handle this after generating the base query
                    final_query = final_query.group_by(*group_columns)
                else:
                    # Other dialects support positions
                    sys.stderr.write(f"[SQLGlot] Pivot: GROUP BY positions: {group_positions}\n")
                    sys.stderr.flush()
                    final_query = final_query.group_by(*[exp.Literal.number(i) for i in group_positions])
            
            # Add ORDER BY (by position for all dialects - ORDER BY supports positions everywhere)
            if group_positions and not limit:
                final_query = final_query.order_by(*[exp.Literal.number(i) for i in group_positions])
            
            # Add LIMIT
            if limit:
                final_query = final_query.limit(limit)
            
            # Generate SQL
            sql = final_query.sql(dialect=normalized_dialect)
            
            # Fix GROUP BY for SQL Server - SQLGlot may convert column names to positions
            if normalized_dialect == 'tsql' and group_columns:
                # Replace "GROUP BY 1, 2" with "GROUP BY VaultName, CurrencyName"
                group_by_cols = ", ".join([c.sql(dialect=normalized_dialect) for c in group_columns])
                # Find and replace the GROUP BY clause (re is imported at module level)
                sql = re.sub(r'GROUP BY (\d+(?:, \d+)*)', f'GROUP BY {group_by_cols}', sql, flags=re.IGNORECASE)
                sys.stderr.write(f"[SQLGlot] Fixed GROUP BY for SQL Server: GROUP BY {group_by_cols}\n")
                sys.stderr.flush()
            
            sys.stderr.write(f"[SQLGlot] Generated PIVOT SQL ({normalized_dialect}): {sql}\n")
            sys.stderr.flush()
            
            # GUARD: Check for unexpanded custom columns
            if expr_map:
                try:
                    for alias in expr_map:
                         if f'"{alias}"' in sql or f'[{alias}]' in sql:
                             sys.stderr.write(f"[SQLGlot] WARNING: Custom column alias '{alias}' found in generated SQL! This may cause 'Column not found' errors if not materialized.\n")
                             sys.stderr.flush()
                except Exception:
                    pass

            return sql
            
        except Exception as e:
            sys.stderr.write(f"[SQLGlot] Pivot query error: {e}\n")
            sys.stderr.flush()
            raise
    
    def _build_multi_series_query(
        self,
        source: str,
        x_field: Optional[any],  # Can be str or List[str] for multi-level X
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
                # Only split if ORDER BY appears at top level (not inside subquery)
                def strip_top_level_clauses(sql: str) -> str:
                    """Remove ORDER BY and LIMIT only if they're at the top level, not inside subqueries"""
                    # Count parentheses to detect if we're inside a subquery
                    paren_depth = 0
                    order_by_pos = -1
                    limit_pos = -1
                    
                    # Find ORDER BY at depth 0
                    i = 0
                    while i < len(sql):
                        if sql[i] == '(':
                            paren_depth += 1
                        elif sql[i] == ')':
                            paren_depth -= 1
                        elif paren_depth == 0 and order_by_pos == -1:
                            if sql[i:i+9].upper() == ' ORDER BY':
                                order_by_pos = i
                        elif paren_depth == 0 and limit_pos == -1:
                            if sql[i:i+6].upper() == ' LIMIT':
                                limit_pos = i
                        i += 1
                    
                    # Cut at the earliest top-level clause
                    cut_pos = len(sql)
                    if order_by_pos != -1:
                        cut_pos = min(cut_pos, order_by_pos)
                    if limit_pos != -1:
                        cut_pos = min(cut_pos, limit_pos)
                    
                    return sql[:cut_pos]
                
                stripped_query = strip_top_level_clauses(single_query)
                sys.stderr.write(f"[SQLGlot] Original query length: {len(single_query)}, stripped length: {len(stripped_query)}\n")
                sys.stderr.write(f"[SQLGlot] Stripped query (first 200 chars): {stripped_query[:200]}\n")
                sys.stderr.flush()
                single_query = stripped_query
                
                if has_legend:
                    # For single-series queries (Sankey, simple charts), don't append series name
                    # Only combine legend + series name for actual multi-series charts
                    if len(series) == 1:
                        # Single series: just use the legend value (for Sankey, etc.)
                        wrapped = f"SELECT x, legend, value FROM ({single_query}) AS _s{idx}"
                    else:
                        # Multi-series: combine original legend with series name using dialect-aware concatenation
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
                
                sys.stderr.write(f"[SQLGlot] Wrapped query for series '{series_name}' (first 300 chars): {wrapped[:300]}\n")
                sys.stderr.flush()
                queries.append(wrapped)
                
            except Exception as e:
                sys.stderr.write(f"[SQLGlot] Error building query for series '{series_name}': {e}\n")
                sys.stderr.flush()
                continue
        
        if not queries:
            raise ValueError("No valid series queries could be built")
        
        # Combine with UNION ALL
        combined = " UNION ALL ".join(queries)
        sys.stderr.write(f"[SQLGlot] Combined query (first 500 chars): {combined[:500]}\n")
        sys.stderr.flush()
        
        # Add ORDER BY and LIMIT to outer query
        final_sql = f"SELECT * FROM ({combined}) AS _multi_series"
        
        # Check if queries already have seasonality ordering (ORDER BY _xo inside)
        # If so, don't add outer ORDER BY as it would override the correct month order
        has_seasonality_ordering = "ORDER BY _xo" in combined
        
        if has_seasonality_ordering:
            sys.stderr.write(f"[SQLGlot] Skipping outer ORDER BY - queries have seasonality ordering\n")
            sys.stderr.flush()
        elif order_by:
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
        
        sys.stderr.write(f"[SQLGlot] Generated multi-series SQL with {len(queries)} series\n")
        sys.stderr.write(f"[SQLGlot] Final multi-series SQL (first 500 chars): {final_sql[:500]}\n")
        sys.stderr.write(f"[SQLGlot] Full multi-series SQL:\n")
        sys.stderr.write(final_sql + "\n")
        sys.stderr.flush()
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
    sys.stderr.write(f"[SQLGlot] * should_use_sqlglot() CALLED with user_id={user_id}\n")
    sys.stderr.flush()
    from .config import settings
    
    # DEBUG: Always log what we see
    sys.stderr.write(f"[SQLGlot] Config check: enable_sqlglot={settings.enable_sqlglot}, sqlglot_users='{settings.sqlglot_users}', user_id={user_id}\n")
    sys.stderr.flush()
    
    # Global flag disabled?
    if not settings.enable_sqlglot:
        sys.stderr.write(f"[SQLGlot] DISABLED by feature flag\n")
        sys.stderr.flush()
        return False
    
    # No user filtering or wildcard?
    if not settings.sqlglot_users or settings.sqlglot_users.strip() == "*":
        sys.stderr.write(f"[SQLGlot] ENABLED for all users (wildcard or empty)\n")
        sys.stderr.flush()
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
