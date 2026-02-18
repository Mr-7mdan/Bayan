from __future__ import annotations

from typing import Any, Dict, List, Tuple
import re
from .sql_dialect_normalizer import normalize_sql_expression

_re_cc = re

SUPPORTED_TRANSLATE = {"postgres", "postgresql", "mssql"}


def _dialect_name(dialect: str | None) -> str:
    d = (dialect or "").lower()
    if d.startswith("postgres"): return "postgres"
    if d.startswith("mysql") or d.startswith("mariadb"): return "mysql"
    if d.startswith("mssql"): return "mssql"
    if d.startswith("duckdb"): return "duckdb"
    if d.startswith("sqlite"): return "sqlite"
    return d or "unknown"


def _lit(v: Any) -> str:
    if v is None: return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    s = s.replace("'", "''")
    return f"'{s}'"


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_segment(d: str, seg: str) -> str:
    # Return segment quoted for the dialect d. If already quoted, return as-is.
    if not seg:
        return seg
    if seg.startswith('"') and seg.endswith('"'):
        return seg
    if seg.startswith('`') and seg.endswith('`'):
        return seg
    if seg.startswith('[') and seg.endswith(']'):
        return seg
    if not _IDENT_RE.match(seg):
        # Non-simple: quote anyway
        if d == 'mysql':
            return f"`{seg}`"
        if d == 'mssql':
            return f"[{seg}]"
        return f'"{seg}"'
    # Simple identifier: still quote to be safe
    if d == 'mysql':
        return f"`{seg}`"
    if d == 'mssql':
        return f"[{seg}]"
    return f'"{seg}"'


def _qtable(dialect: str, name: str) -> str:
    d = _dialect_name(dialect)
    raw = (name or '').strip()
    if not raw:
        return raw
    # If looks like an expression/subquery, return as-is
    if '(' in raw or ')' in raw:
        return raw
    # Quote each dotted segment
    parts = raw.split('.')
    qparts = [_quote_segment(d, p) for p in parts]
    return '.'.join(qparts)


def _qcol(dialect: str, name: str) -> str:
    d = _dialect_name(dialect)
    raw = (name or '').strip()
    if not raw:
        return raw
    # Allow alias.column form; if expression-like, return as-is
    if '(' in raw or ')' in raw:
        return raw
    parts = raw.split('.')
    if len(parts) == 1:
        return _quote_segment(d, parts[0])
    if len(parts) == 2:
        a, c = parts
        # Always quote both segments for safety
        return f"{_quote_segment(d, a)}.{_quote_segment(d, c)}"
    # More complex (schema.db.table.col) – quote each segment when possible
    try:
        qparts = [_quote_segment(d, seg) for seg in parts]
        return '.'.join(qparts)
    except Exception:
        return raw


def _qleft_if_ident(dialect: str, left: str) -> str:
    # Quote left-hand identifier only if it's a simple ident or alias.ident
    raw = (left or '').strip()
    if _IDENT_RE.match(raw):
        return _qcol(dialect, raw)
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$", raw or ''):
        return _qcol(dialect, raw)
    return left


def _qal(dialect: str, alias: str) -> str:
    """Quote an alias safely for the given dialect."""
    d = _dialect_name(dialect)
    return _quote_segment(d, alias)


def _unquote_ident(name: str) -> str:
    s = str(name or '').strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith('`') and s.endswith('`')):
        return s[1:-1]
    if s.startswith('[') and s.endswith(']'):
        return s[1:-1]
    return s


def _normalize_expr_idents(dialect: str, expr: str, *, numericify: bool = False) -> str:
    """Normalize bracket-quoted identifiers inside free-form expressions
    to the correct quoting for the given dialect. This allows users to write
    MSSQL-style [Column With Spaces] in custom/computed expressions even when
    targeting DuckDB/Postgres/etc.

    Notes:
    - Leaves content inside string literals ('...') untouched
    - For non-MSSQL dialects, replaces [name] with properly quoted identifier using _quote_segment for the target dialect
    - Also normalizes double-quoted/backtick-quoted identifiers to the dialect's quoting style
    - If numericify=True, wraps identifiers with a numeric-cast wrapper suitable for the dialect
    - For MSSQL, returns the expression unchanged
    """
    d = _dialect_name(dialect)
    if d == 'mssql' or not expr:
        return expr
    
    # First pass: Apply dialect normalizer for common fixes (bracket syntax, CASE/END spacing)
    from .sql_dialect_normalizer import auto_normalize
    expr = auto_normalize(expr, d)
    s = str(expr)
    out: list[str] = []
    i = 0
    L = len(s)
    in_sq = False  # inside single-quoted string literal

    def _numwrap(ident_sql: str) -> str:
        if not numericify:
            return ident_sql
        if d == 'duckdb':
            return (
                f"COALESCE(try_cast(regexp_replace(CAST({ident_sql} AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), "
                f"try_cast({ident_sql} AS DOUBLE), 0.0)"
            )
        if d in {'postgres'}:
            return f"CAST({ident_sql} AS DOUBLE PRECISION)"
        if d in {'mysql'}:
            return f"CAST({ident_sql} AS DECIMAL(38,10))"
        if d in {'sqlite'}:
            return f"CAST({ident_sql} AS REAL)"
        # default
        return f"CAST({ident_sql} AS DOUBLE)"
    while i < L:
        ch = s[i]
        if ch == "'":
            # toggle single-quoted string (handle escaped '')
            out.append(ch)
            i += 1
            in_sq = not in_sq
            # handle doubled quotes inside strings
            if in_sq:
                while i < L and s[i] == "'" and (i+1) < L and s[i+1] == "'":
                    out.extend(["'", "'"])
                    i += 2
            continue
        if ch == '[' and not in_sq:
            j = s.find(']', i + 1)
            if j != -1:
                inner = s[i + 1 : j]
                # Quote this segment for the target dialect
                out.append(_numwrap(_quote_segment(d, inner)))
                i = j + 1
                continue
        # Normalize double-quoted identifier tokens
        if ch == '"' and not in_sq:
            j = s.find('"', i + 1)
            if j != -1:
                inner = s[i + 1 : j]
                out.append(_numwrap(_quote_segment(d, inner)))
                i = j + 1
                continue
        # Normalize backtick-quoted identifier tokens
        if ch == '`' and not in_sq:
            j = s.find('`', i + 1)
            if j != -1:
                inner = s[i + 1 : j]
                out.append(_numwrap(_quote_segment(d, inner)))
                i = j + 1
                continue
        out.append(ch)
        i += 1
    return ''.join(out)


def _order_token(dialect: str, by: str) -> str:
    """Return a safe ORDER BY token: keep numeric ordinals raw; otherwise quote column."""
    s = str(by or '').strip()
    if s.isdigit():
        return s
    return _qcol(_dialect_name(dialect), s)


def _cond_sql(cond: Dict[str, Any]) -> str:
    # cond: { op, left, right }
    op = str(cond.get("op") or "eq").lower()
    left = str(cond.get("left") or "")
    right = cond.get("right")
    if op == "in":
        arr = right if isinstance(right, (list, tuple)) else [right]
        arr_sql = ", ".join(_lit(v) for v in arr)
        return f"{left} IN ({arr_sql})"
    if op == "like":
        return f"{left} LIKE {_lit(right)}"
    if op == "regex":
        # Not portable; use LIKE as best-effort
        return f"{left} LIKE {_lit(right)}"
    map_ops = {"eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
    sym = map_ops.get(op, "=")
    return f"{left} {sym} {_lit(right)}"


def _cond_sql_q(dialect: str, cond: Dict[str, Any]) -> str:
    """Like _cond_sql but quotes left identifier when simple."""
    c = dict(cond or {})
    if "left" in c:
        try:
            c["left"] = _qleft_if_ident(dialect, str(c.get("left") or ""))
        except Exception:
            pass
    return _cond_sql(c)


def _case_expr(dialect: str, target: str, cases: List[Dict[str, Any]], else_val: Any | None) -> str:
    parts = ["CASE"]
    for it in (cases or []):
        when = it.get("when") or {}
        then = it.get("then")
        parts.append(f" WHEN {_cond_sql_q(_dialect_name(dialect), when)} THEN {_lit(then)}")
    if else_val is not None:
        parts.append(f" ELSE {_lit(else_val)}")
    parts.append(" END")
    return "".join(parts)


def _null_func(dialect: str, mode: str, target: str, value: Any) -> str:
    d = _dialect_name(dialect)
    m = (mode or "coalesce").lower()
    if m == "isnull":
        if d == "mssql":
            return f"ISNULL({target}, {_lit(value)})"
        # fallthrough to coalesce elsewhere
    if m == "ifnull":
        if d in {"mysql", "sqlite"}:
            return f"IFNULL({target}, {_lit(value)})"
    # default
    return f"COALESCE({target}, {_lit(value)})"


def _replace_chain(target: str, search: Any, replace: Any) -> str:
    # Accept scalar or arrays; chain REPLACE
    if isinstance(search, (list, tuple)):
        reps = list(zip(search, (replace if isinstance(replace, (list, tuple)) else [replace] * len(search))))
        expr = target
        for a, b in reps:
            expr = f"REPLACE({expr}, {_lit(a)}, {_lit(b)})"
        return expr
    return f"REPLACE({target}, {_lit(search)}, {_lit(replace)})"


def _translate_or_replace(dialect: str, target: str, search: str, replace: str) -> str:
    d = _dialect_name(dialect)
    if d in SUPPORTED_TRANSLATE:
        return f"TRANSLATE({target}, {_lit(search)}, {_lit(replace)})"
    # emulate via chained REPLACE char-by-char
    expr = target
    for a, b in zip(list(search), list(replace)):
        expr = f"REPLACE({expr}, {_lit(a)}, {_lit(b)})"
    return expr


def _agg_expr(dialect: str, fn: str, column: str) -> Tuple[str, str | None]:
    d = _dialect_name(dialect)
    f = (fn or "").lower()
    col = column.strip() or "*"
    # Numeric/stat funcs
    if f in {"sum", "avg", "min", "max"}:
        return f"{f.upper()}({col})", None
    if f == "count":
        return f"COUNT({col})", None
    # string_agg mapping
    if f == "string_agg":
        if d in {"postgres"}:
            return f"string_agg(CAST({col} AS text), ',')", None
        if d == "mssql":
            return f"STRING_AGG(CAST({col} AS NVARCHAR(4000)), ',')", None
        if d in {"mysql"}:
            return f"GROUP_CONCAT({col} SEPARATOR ',')", None
        if d in {"sqlite"}:
            return f"GROUP_CONCAT({col}, ',')", None
        if d in {"duckdb"}:
            return f"string_agg(CAST({col} AS VARCHAR), ',')", None
        return f"GROUP_CONCAT({col}, ',')", None
    # array_agg mapping
    if f == "array_agg":
        if d in {"postgres", "duckdb"}:
            return f"array_agg({col})", None
        if d == "mysql":
            return f"JSON_ARRAYAGG({col})", None
        if d == "sqlite":
            # Best-effort: JSON group array (requires JSON1), fall back to GROUP_CONCAT
            return f"JSON_GROUP_ARRAY({col})", None
        if d == "mssql":
            # Best-effort: return delimited string
            return f"STRING_AGG(CAST({col} AS NVARCHAR(4000)), ',')", None
        return f"GROUP_CONCAT({col}, ',')", None
    # default passthrough
    return f"{f.upper()}({col})", None


def build_sql(
    *,
    dialect: str,
    source: str,
    base_select: List[str] | None,
    custom_columns: List[Dict[str, Any]] | None,
    transforms: List[Dict[str, Any]] | None,
    joins: List[Dict[str, Any]] | None,
    defaults: Dict[str, Any] | None,
    limit: int | None,
) -> Tuple[str, List[str], List[str]]:
    warnings: List[str] = []
    d = _dialect_name(dialect)

    # Collect alias names produced by custom columns and transforms so we can
    # avoid selecting a same-named base column (e.g., when user selects a
    # custom alias like "ClientCode" that does not physically exist on source).
    alias_names: List[str] = []
    for cc in (custom_columns or []):
        nm = str(cc.get("name") or "").strip()
        if nm:
            alias_names.append(nm)
    # Also include computed transforms
    for tr in (transforms or []):
        if str(tr.get("type") or "").lower() == "computed":
            nm = str(tr.get("name") or "").strip()
            if nm:
                alias_names.append(nm)
    for tr in (transforms or []):
        t = (tr.get("type") or "").lower()
        if t in {"computed", "case", "replace", "translate", "nullhandling"}:
            tgt = str(tr.get("name") or tr.get("target") or "").strip()
            if tgt:
                alias_names.append(tgt)
    alias_set = {s.lower() for s in alias_names}
    print(f"[build_sql] alias_names collected: {alias_names[:10]}")

    # Detect unpivot transform (first occurrence only)
    unpivot_tr = None
    for tr in (transforms or []):
        if str((tr.get("type") or "")).lower() == "unpivot":
            unpivot_tr = tr
            break

    # Use configurable base alias: 's' normally; 'u' when unpivot is applied via a derived UNION subquery
    base_alias = "u" if unpivot_tr else "s"

    # Build base select, prefix simple columns with base alias and quote
    select_cols: List[str] = []
    seen_qcols: set[str] = set()
    has_star = False
    # Only default to '*' when base_select is explicitly None. An empty list
    # means the caller wants to project only custom columns/transforms.
    base_select_tokens = base_select if base_select is not None else ["*"]
    for it in base_select_tokens:
        token = str(it).strip()
        # Normalize '*' to base-only star to avoid bringing in join columns implicitly
        if token == "*" or token.lower() == f"{base_alias}.*" or token.lower() == "s.*":
            if not has_star:
                select_cols.append(f"{base_alias}.*")
                has_star = True
            continue
        # Skip non-column tokens that may leak from client WHERE composition
        # e.g., 'field__gte', 'field__lt', or global keys like 'startDate'
        if "__" in token:
            try:
                base, op = token.split("__", 1)
                if op in {"gte", "gt", "lte", "lt"}:
                    continue
            except Exception:
                pass
        if token in {"start", "startDate", "end", "endDate"}:
            continue
        # Skip tokens that are custom column names - they'll be added by the custom columns loop
        # Strip table prefix (e.g., "s.ClientType" -> "ClientType") before checking
        token_base = token.split('.')[-1] if '.' in token else token
        if token_base in alias_names:
            continue
        # Support derived date parts specified as "BaseField (Part)" in base_select
        # This mirrors build_distinct_sql's mapping but projects as a regular column with an alias
        try:
            DERIVED_RE = re.compile(r"^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$")
            m = DERIVED_RE.match(token)
        except Exception:
            m = None
        if m:
            base_name = m.group(1).strip()
            part = m.group(2)
            # Ensure base alias 's.' for bare columns
            src = base_name
            if src and ("(" not in src and ")" not in src and " " not in src) and "." not in src:
                src = f"s.{src}"
            col = _qcol(d, src)
            # Dialect-aware date-part extraction
            if d == 'mssql':
                if part == 'Year': expr = f"YEAR({col})"
                elif part == 'Quarter': expr = f"DATEPART(quarter, {col})"
                elif part == 'Month': expr = f"MONTH({col})"
                elif part == 'Month Name': expr = f"DATENAME(month, {col})"
                elif part == 'Month Short': expr = f"LEFT(DATENAME(month, {col}), 3)"
                elif part == 'Week': expr = f"DATEPART(iso_week, {col})"
                elif part == 'Day': expr = f"DAY({col})"
                elif part == 'Day Name': expr = f"DATENAME(weekday, {col})"
                elif part == 'Day Short': expr = f"LEFT(DATENAME(weekday, {col}), 3)"
                else: expr = col
            elif d in {'postgres', 'duckdb'}:
                if part == 'Year': expr = f"EXTRACT(year FROM {col})"
                elif part == 'Quarter': expr = f"EXTRACT(quarter FROM {col})"
                elif part == 'Month': expr = f"EXTRACT(month FROM {col})"
                elif part == 'Month Name': expr = f"to_char({col}, 'FMMonth')"
                elif part == 'Month Short': expr = f"to_char({col}, 'Mon')"
                elif part == 'Week': expr = f"EXTRACT(week FROM {col})"
                elif part == 'Day': expr = f"EXTRACT(day FROM {col})"
                elif part == 'Day Name': expr = f"to_char({col}, 'FMDay')"
                elif part == 'Day Short': expr = f"to_char({col}, 'Dy')"
                else: expr = col
            elif d == 'mysql':
                if part == 'Year': expr = f"YEAR({col})"
                elif part == 'Quarter': expr = f"QUARTER({col})"
                elif part == 'Month': expr = f"MONTH({col})"
                elif part == 'Month Name': expr = f"DATE_FORMAT({col}, '%M')"
                elif part == 'Month Short': expr = f"DATE_FORMAT({col}, '%b')"
                elif part == 'Week': expr = f"WEEK({col}, 3)"  # ISO week, Mon start
                elif part == 'Day': expr = f"DAY({col})"
                elif part == 'Day Name': expr = f"DATE_FORMAT({col}, '%W')"
                elif part == 'Day Short': expr = f"DATE_FORMAT({col}, '%a')"
                else: expr = col
            elif d == 'sqlite':
                if part == 'Year': expr = f"CAST(strftime('%Y', {col}) AS INTEGER)"
                elif part == 'Quarter':
                    expr = (
                        f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                        f"WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 "
                        f"WHEN 4 THEN 2 WHEN 5 THEN 2 WHEN 6 THEN 2 "
                        f"WHEN 7 THEN 3 WHEN 8 THEN 3 WHEN 9 THEN 3 "
                        f"ELSE 4 END"
                    )
                elif part == 'Month': expr = f"CAST(strftime('%m', {col}) AS INTEGER)"
                elif part == 'Month Name':
                    expr = (
                        f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                        f"WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April' "
                        f"WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' "
                        f"WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December' END"
                    )
                elif part == 'Month Short':
                    expr = (
                        f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                        f"WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr' "
                        f"WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' "
                        f"WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec' END"
                    )
                elif part == 'Week': expr = f"CAST(strftime('%W', {col}) AS INTEGER)"  # Monday start
                elif part == 'Day': expr = f"CAST(strftime('%d', {col}) AS INTEGER)"
                elif part == 'Day Name':
                    expr = (
                        f"CASE strftime('%w', {col}) "
                        f"WHEN '0' THEN 'Sunday' WHEN '1' THEN 'Monday' WHEN '2' THEN 'Tuesday' WHEN '3' THEN 'Wednesday' "
                        f"WHEN '4' THEN 'Thursday' WHEN '5' THEN 'Friday' WHEN '6' THEN 'Saturday' END"
                    )
                elif part == 'Day Short':
                    expr = (
                        f"CASE strftime('%w', {col}) "
                        f"WHEN '0' THEN 'Sun' WHEN '1' THEN 'Mon' WHEN '2' THEN 'Tue' WHEN '3' THEN 'Wed' "
                        f"WHEN '4' THEN 'Thu' WHEN '5' THEN 'Fri' WHEN '6' THEN 'Sat' END"
                    )
                else: expr = col
            else:
                # Fallback: pass-through
                expr = col
            select_cols.append(f"{expr} AS {_qal(d, token)}")
            continue
        # Keep expressions as-is (parenthesized)
        if ("(" in token) or (")" in token):
            select_cols.append(token)
            continue
        # Skip if this token (qualified or not) collides with a transform/custom alias.
        simple = token
        if "." in simple:
            try:
                simple = simple.split(".", 1)[1]
            except Exception:
                pass
        simple = _unquote_ident(simple)
        if simple.lower() in alias_set:
            continue
        # If '*' already included base columns, skip re-adding bare base columns to prevent duplicates
        if has_star and "." not in token:
            continue
        # Ensure base alias for bare columns
        if "." not in token:
            token = f"{base_alias}.{token}"
        qtok = _qcol(d, token)
        # Deduplicate identical quoted columns
        lk = qtok.strip().lower()
        if lk in seen_qcols:
            continue
        seen_qcols.add(lk)
        select_cols.append(qtok)

    # Apply computed/custom columns as projections
    # Only include custom columns if:
    # 1. base_select contains "*" (select all), OR
    # 2. The custom column name is explicitly in base_select, OR
    # 3. base_select is an explicit empty list (caller wants only custom columns)
    if has_star:
        requested_cols_lower = set()
    else:
        if base_select is not None and len(base_select) == 0:
            requested_cols_lower = set()
            # Include all custom column names
            for cc in (custom_columns or []):
                name = str((cc or {}).get("name") or "").strip()
                if name:
                    requested_cols_lower.add(name.lower())
            # Include transforms that produce aliases (computed, case, replace, etc.)
            for tr in (transforms or []):
                t = str((tr or {}).get("type") or "").lower()
                if t == "computed":
                    name = str((tr or {}).get("name") or "").strip()
                    if name:
                        requested_cols_lower.add(name.lower())
                elif t in {"case", "replace", "translate", "nullhandling"}:
                    target = str((tr or {}).get("target") or "").strip()
                    if target:
                        requested_cols_lower.add(target.lower())
        else:
            requested_cols_lower = {
                str(c).strip().split('.')[-1].lower() for c in (base_select or [])
            }
    
    # Build a map of custom column names to their normalized expressions
    # This allows us to substitute references to other custom columns
    cc_expr_map: Dict[str, str] = {}
    for cc in (custom_columns or []):
        name = str(cc.get("name") or "")
        expr = str(cc.get("expr") or "")
        ctype = str(cc.get("type") or "").lower()
        
        # Always include custom columns in the expression map so dependencies can be resolved
        # even if the column itself isn't in the final SELECT.
        
        if name and expr:
            # Detect pure arithmetic: only identifiers, numbers, and arithmetic operators (+, -, *, /)
            _num_hint = (ctype == 'number')
            if not _num_hint:
                try:
                    e_check = str(expr or '').strip()
                    # Remove all quoted identifiers, numbers, whitespace, and arithmetic operators
                    e_clean = _re_cc.sub(r'"[^"]+"|\'[^\']+\'|\d+|\s+', '', e_check)
                    e_clean = e_clean.replace('+', '').replace('-', '').replace('*', '').replace('/', '').replace('(', '').replace(')', '')
                    # If nothing left, it's pure arithmetic
                    if not e_clean and ('+' in e_check or '-' in e_check or '*' in e_check or '/' in e_check):
                        _num_hint = True
                except Exception:
                    pass
            normalized_expr = _normalize_expr_idents(d, expr, numericify=_num_hint)
            # Normalize SQL dialect (convert MSSQL syntax to target dialect)
            normalized_expr = normalize_sql_expression(normalized_expr, d)
            cc_expr_map[name] = normalized_expr
            print(f"[build_sql] Added '{name}' to cc_expr_map (expr length={len(normalized_expr)})", flush=True)

    # Also populate cc_expr_map from joins so that transforms can reference joined columns by their alias
    # This maps an alias (e.g. "SourceRegion") to its qualified column (e.g. "j1"."SourceRegion")
    if joins:
        print(f"[build_sql] Augmenting cc_expr_map from {len(joins)} joins...", flush=True)
        for i, j in enumerate(joins):
            alias = f"j{i+1}"
            cols = j.get("columns") or []
            for col in cols:
                cname = str((col or {}).get("name") or "")
                alias_col = str((col or {}).get("alias") or cname)
                if cname and alias_col:
                    # Map alias_col to qualified column
                    # We use _qcol logic manually here because we know the table alias is 'jN'
                    # The target expression should be fully qualified for DuckDB (e.g. j1.Col)
                    # We quote it so that it's treated as a safe identifier when inlined.
                    # Inlining will wrap it in parens: (j1.Col).
                    expr = f"{alias}.{_quote_segment(d, cname)}"
                    # If dialect requires quoting the alias too? DuckDB is fine with unquoted simple aliases
                    # but let's be safe if alias has weird chars (unlikely for jN).
                    # Actually _qcol handles "table.col".
                    expr = _qcol(d, f"{alias}.{cname}")
                    cc_expr_map[alias_col] = expr
                    print(f"[build_sql] Added joined column '{alias_col}' -> '{expr}' to cc_expr_map", flush=True)

    # Also populate cc_expr_map from transforms (computed/case/replace/etc.)
    # so that later transforms (like case) can inline expressions from earlier transforms
    print(f"[build_sql] Augmenting cc_expr_map from {len(transforms or [])} transforms...", flush=True)
    for tr in (transforms or []):
        t = str((tr or {}).get("type") or "").lower()
        if t == "computed":
            nm = str((tr or {}).get("name") or "").strip()
            ex = str((tr or {}).get("expr") or "").strip()
            if nm and ex:
                # Apply same normalization/numeric-hint logic as custom columns
                _num_hint = False
                try:
                    e_check = ex
                    e_clean = _re_cc.sub(r'"[^"]+"|\'[^\']+\'|\d+|\s+', '', e_check)
                    e_clean = e_clean.replace('+', '').replace('-', '').replace('*', '').replace('/', '').replace('(', '').replace(')', '')
                    if not e_clean and ('+' in e_check or '-' in e_check or '*' in e_check or '/' in e_check):
                        _num_hint = True
                except Exception:
                    pass
                normalized_expr = _normalize_expr_idents(d, ex, numericify=_num_hint)
                normalized_expr = normalize_sql_expression(normalized_expr, d)
                cc_expr_map[nm] = normalized_expr
                print(f"[build_sql] Added computed transform '{nm}' to cc_expr_map", flush=True)
        elif t == "case":
            target = str((tr or {}).get("target") or "").strip()
            cases = (tr or {}).get("cases") or []
            else_val = (tr or {}).get("else") or (tr or {}).get("else_")
            if target and cases:
                expr = _case_expr(d, target, cases, else_val)
                cc_expr_map[target] = expr
                print(f"[build_sql] Added case transform '{target}' to cc_expr_map", flush=True)
        elif t == "replace":
            target = str((tr or {}).get("target") or "").strip()
            search = tr.get("search")
            repl = tr.get("replace")
            if target and search is not None and repl is not None:
                src = _qcol(d, target)
                expr = _replace_chain(src, search, repl)
                cc_expr_map[target] = expr
        elif t == "translate":
            target = str((tr or {}).get("target") or "").strip()
            s = str(tr.get("search") or "")
            r = str(tr.get("replace") or "")
            if target and s:
                src = _qcol(d, target)
                expr = _translate_or_replace(d, src, s, r)
                cc_expr_map[target] = expr
        elif t == "nullhandling":
            target = str((tr or {}).get("target") or "").strip()
            mode = str((tr or {}).get("mode") or "coalesce")
            val = tr.get("value")
            if target:
                src = _qcol(d, target)
                expr = _null_func(d, mode, src, val)
                cc_expr_map[target] = expr
    
    # Defensive rewrite: if unpivot is active, remap any lingering 's.' references in projections to the current base alias
    cc_names_lower = {n.lower() for n in cc_expr_map.keys()}
    skipped_cc_names: set = set()
    
    for cc in (custom_columns or []):
        name = str(cc.get("name") or "")
        if name not in cc_expr_map:
            continue
        
        print(f"[build_sql] Including custom column '{name}'", flush=True)
        expr = cc_expr_map[name]
        original_expr = expr
        
        # Check if this expression contains qualified refs to other custom columns
        potential_cc_refs = _re_cc.findall(r'"[a-zA-Z]"\."([^"]+)"', expr)
        if potential_cc_refs:
            print(f"[build_sql] '{name}' has potential custom col refs: {potential_cc_refs}", flush=True)
            for ref in potential_cc_refs:
                if ref in cc_expr_map:
                    print(f"[build_sql]   -> '{ref}' IS in cc_expr_map, will substitute", flush=True)
                elif ref.lower() in cc_names_lower:
                    print(f"[build_sql]   -> '{ref}' (case-insensitive) IS in cc_expr_map", flush=True)
                else:
                    print(f"[build_sql]   -> '{ref}' NOT in cc_expr_map", flush=True)
        
        # Check if this is an arithmetic expression that needs NULL handling
        is_arithmetic = any(op in original_expr for op in ['+', '-', '*', '/'])
        
        # Substitute any references to other custom columns with their expressions
        # Match patterns like [CustomColName] or "CustomColName" or "s"."CustomColName"
        substitution_count = 0
        unresolved_refs: set = set()
        
        def replace_custom_ref(match):
            nonlocal substitution_count, unresolved_refs
            ref_name = match.group(1) or match.group(2)
            if ref_name in cc_expr_map:
                substitution_count += 1
                substituted = cc_expr_map[ref_name]
                # Wrap substituted expression in parentheses to preserve order of operations
                # Add COALESCE for NULL handling in arithmetic expressions
                if is_arithmetic and d == "mssql":
                    substituted = f"COALESCE({substituted}, 0)"
                print(f"[build_sql] Substituting [{ref_name}] -> {substituted[:50]}...", flush=True)
                return f"({substituted})"
            # Check if this looks like a custom column name that we can't resolve
            if ref_name.lower() in cc_names_lower and ref_name not in cc_expr_map:
                unresolved_refs.add(ref_name)
            return match.group(0)  # No substitution needed
        
        # First: Replace qualified identifiers like "s"."Name" or [s].[Name] where Name is a custom column
        # This handles cases like "s"."VisitType" where VisitType is another custom column
        def replace_qualified_ref(match):
            nonlocal substitution_count, unresolved_refs
            col_name = match.group(1) or match.group(2)  # The column part after the dot
            if not col_name:
                return match.group(0)
            if col_name in cc_expr_map:
                substitution_count += 1
                substituted = cc_expr_map[col_name]
                if is_arithmetic and d == "mssql":
                    substituted = f"COALESCE({substituted}, 0)"
                print(f"[build_sql] Substituting qualified \"s\".\"{col_name}\" -> {substituted[:50]}...", flush=True)
                return f"({substituted})"
            # Check if this is a custom column name (case-insensitive) that exists but with different case
            for cc_name in cc_expr_map.keys():
                if cc_name.lower() == col_name.lower():
                    substitution_count += 1
                    substituted = cc_expr_map[cc_name]
                    if is_arithmetic and d == "mssql":
                        substituted = f"COALESCE({substituted}, 0)"
                    print(f"[build_sql] Substituting qualified \"s\".\"{col_name}\" (case-insensitive match to '{cc_name}') -> {substituted[:50]}...", flush=True)
                    return f"({substituted})"
            # Mark as unresolved if it looks like a custom column name we know about
            if col_name.lower() in cc_names_lower:
                unresolved_refs.add(col_name)
            return match.group(0)  # No substitution needed
        
        # Replace "s"."Name" or [s].[Name] patterns (qualified with table alias)
        # Use case-insensitive matching for the table alias
        expr = _re_cc.sub(r'"[a-zA-Z]"\."([^"]+)"|\[[a-zA-Z]\]\.\[([^\]]+)\]', replace_qualified_ref, expr)
        
        # Debug: log if we found any qualified refs that look like custom columns
        qual_refs_found = _re_cc.findall(r'"[a-zA-Z]"\."([^"]+)"', original_expr)
        if qual_refs_found:
            print(f"[build_sql] Found qualified refs in '{name}': {qual_refs_found}, cc_expr_map keys: {list(cc_expr_map.keys())[:10]}", flush=True)
        
        # Replace [Name] and "Name" with their expressions (unqualified)
        expr = _re_cc.sub(r'\[([^\]]+)\]|"([^"]+)"', replace_custom_ref, expr)
        
        # If there are unresolved references to custom columns, skip this custom column
        # This prevents SQL errors when a custom column references another custom column
        # that can't be resolved (e.g., "s"."VisitType" where VisitType is defined later)
        if unresolved_refs:
            print(f"[build_sql] SKIP custom column '{name}': unresolved refs to other custom columns: {unresolved_refs}", flush=True)
            skipped_cc_names.add(name)
            continue
        
        # Final safety check: scan the expression for any remaining qualified refs to custom column names
        # This catches cases where the substitution didn't work for some reason
        remaining_qual_refs = _re_cc.findall(r'"[a-zA-Z]"\."([^"]+)"', expr)
        unresolved_final = set()
        for ref in remaining_qual_refs:
            if ref.lower() in cc_names_lower:
                unresolved_final.add(ref)
        if unresolved_final:
            print(f"[build_sql] SKIP custom column '{name}': still has unresolved custom column refs after substitution: {unresolved_final}", flush=True)
            skipped_cc_names.add(name)
            continue
        
        if substitution_count > 0:
            print(f"[build_sql] Made {substitution_count} substitutions in '{name}'", flush=True)
            print(f"[build_sql] Original: {original_expr[:100]}...", flush=True)
            print(f"[build_sql] Expanded: {expr[:100]}...", flush=True)

        # For DuckDB, arithmetic custom columns may reference VARCHAR sources (e.g. numeric strings).
        # To ensure the arithmetic operates on numeric types, cast double-quoted identifiers to DOUBLE
        # with a simple COALESCE(try_cast(...), 0.0). Avoid embedding regexp patterns here to keep
        # the generated SQL simpler and prevent parser issues.
        if is_arithmetic and d == "duckdb":
            def _wrap_duckdb_numeric(m):
                ident = m.group(0)  # e.g. "Category4"
                return f"COALESCE(try_cast({ident} AS DOUBLE), 0.0)"
            expr = _re_cc.sub(r'"[^"]+"', _wrap_duckdb_numeric, expr)
        
        select_cols.append(f"({expr}) AS {_qal(d, name)}")

    for tr in (transforms or []):
        t = (tr.get("type") or "").lower()
        if t == "computed":
            name = str(tr.get("name") or "")
            expr = str(tr.get("expr") or "")
            # Skip if not explicitly requested and no "*" in base_select
            if not has_star and name.lower() not in requested_cols_lower:
                print(f"[build_sql] Skipping computed transform '{name}' (not requested)")
                continue
            if name and expr:
                # Apply substitution for custom columns/joined columns
                # This mirrors the logic in the custom_columns loop
                original_expr = expr
                
                def replace_custom_ref_tr(match):
                    ref_name = match.group(1) or match.group(2)
                    if ref_name in cc_expr_map:
                        substituted = cc_expr_map[ref_name]
                        return f"({substituted})"
                    # Try case-insensitive lookup
                    for k, v in cc_expr_map.items():
                        if k.lower() == ref_name.lower():
                            return f"({v})"
                    return match.group(0)

                # Substitute "Col" or [Col] or "s"."Col" patterns
                # The regex matches: "ident" OR [ident] OR "alias"."ident"
                expr = _re_cc.sub(r'"([^"]+)"|\[([^\]]+)\]|"[a-zA-Z]"\."([^"]+)"', replace_custom_ref_tr, expr)
                
                # computed transforms have no explicit type; keep ident normalization only
                # But we DO want numeric casting if it looks arithmetic?
                # For now, rely on explicit casting or raw expression.
                expr = _normalize_expr_idents(d, expr, numericify=False)
                
                # Normalize SQL dialect (convert MSSQL syntax to target dialect)
                expr = normalize_sql_expression(expr, d)
                
                # DuckDB numeric wrapper for arithmetic expressions
                is_arithmetic = any(op in original_expr for op in ['+', '-', '*', '/'])
                if is_arithmetic and d == "duckdb":
                    def _wrap_duckdb_numeric(m):
                        ident = m.group(0)
                        return f"COALESCE(try_cast({ident} AS DOUBLE), 0.0)"
                    expr = _re_cc.sub(r'"[^"]+"', _wrap_duckdb_numeric, expr)

                select_cols.append(f"({expr}) AS {_qal(d, name)}")
        elif t == "case":
            target = str(tr.get("target") or "")
            cases = tr.get("cases") or []
            else_val = tr.get("else") or tr.get("else_")
            # Skip if not explicitly requested and no "*" in base_select
            if not has_star and target.lower() not in requested_cols_lower:
                print(f"[build_sql] Skipping case transform '{target}' (not requested)")
                continue
            if target and cases:
                # Remap WHEN.left alias 's.' to current base_alias when unpivot is applied
                try:
                    import re as _re
                    remapped: List[Dict[str, Any]] = []
                    for it in cases:
                        w = dict((it or {}).get("when") or {})
                        lhs = str(w.get("left") or "")
                        if lhs:
                            # Convert bracketed [s].[Col] -> alias.Col then let _case_expr quoting handle it
                            m = _re.match(r"^\s*\[s\]\.\[([^\]]+)\]\s*$", lhs)
                            if m:
                                lhs = f"{base_alias}.{m.group(1)}"
                            elif lhs.startswith("s."):
                                lhs = f"{base_alias}." + lhs.split(".", 1)[1]
                            # Inline custom/transform aliases using cc_expr_map so dialects like DuckDB
                            # do not need to resolve SELECT-list aliases within the same projection.
                            lhs_norm = _unquote_ident(lhs)
                            # Strip base alias if present (e.g. s.Col -> Col) to find the lookup key
                            lookup_key = lhs_norm
                            if lookup_key.startswith(f"{base_alias}."):
                                lookup_key = lookup_key[len(base_alias)+1:]
                            elif lookup_key.startswith("s."):
                                lookup_key = lookup_key[2:]
                            
                            # Ensure lookup_key is a simple identifier (no remaining dots/spaces)
                            if lookup_key and "(" not in lookup_key and ")" not in lookup_key and " " not in lookup_key and "." not in lookup_key:
                                if lookup_key in cc_expr_map:
                                    ref_expr = cc_expr_map[lookup_key]
                                else:
                                    for _nm in cc_expr_map.keys():
                                        if _nm.lower() == lookup_key.lower():
                                            ref_expr = cc_expr_map[_nm]
                                            break

                            if ref_expr:
                                lhs = f"({ref_expr})"
                            w["left"] = lhs
                        remapped.append({"when": w, "then": (it or {}).get("then")})
                    expr = _case_expr(d, target, remapped, else_val)
                except Exception:
                    expr = _case_expr(d, target, cases, else_val)
                select_cols.append(f"({expr}) AS {_qal(d, target)}")
            else:
                warnings.append("case transform missing target/cases")
        elif t == "replace":
            target = str(tr.get("target") or "")
            search = tr.get("search")
            repl = tr.get("replace")
            # Skip if not explicitly requested and no "*" in base_select
            if not has_star and target.lower() not in requested_cols_lower:
                print(f"[build_sql] Skipping replace transform '{target}' (not requested)")
                continue
            if target is not None and search is not None and repl is not None:
                src = target
                if src and ("(" not in src and ")" not in src and " " not in src) and "." not in src:
                    src = f"{base_alias}.{src}"
                src = _qcol(d, src)
                expr = _replace_chain(src, search, repl)
                select_cols.append(f"({expr}) AS {_qal(d, target)}")
            else:
                warnings.append("replace transform missing fields")
        elif t == "translate":
            target = str(tr.get("target") or "")
            s = str(tr.get("search") or "")
            r = str(tr.get("replace") or "")
            # Skip if not explicitly requested and no "*" in base_select
            if not has_star and target.lower() not in requested_cols_lower:
                print(f"[build_sql] Skipping translate transform '{target}' (not requested)")
                continue
            if target and s:
                src = target
                if src and ("(" not in src and ")" not in src and " " not in src) and "." not in src:
                    src = f"{base_alias}.{src}"
                src = _qcol(d, src)
                expr = _translate_or_replace(d, src, s, r)
                select_cols.append(f"({expr}) AS {_qal(d, target)}")
            else:
                warnings.append("translate transform missing fields")
        elif t == "nullhandling":
            target = str(tr.get("target") or "")
            mode = str(tr.get("mode") or "coalesce")
            val = tr.get("value")
            # Skip if not explicitly requested and no "*" in base_select
            if not has_star and target.lower() not in requested_cols_lower:
                print(f"[build_sql] Skipping nullhandling transform '{target}' (not requested)")
                continue
            if target:
                src = target
                if src and ("(" not in src and ")" not in src and " " not in src) and "." not in src:
                    src = f"{base_alias}.{src}"
                src = _qcol(d, src)
                expr = _null_func(d, mode, src, val)
                select_cols.append(f"({expr}) AS {_qal(d, target)}")
            else:
                warnings.append("nullHandling missing target")
        elif t == "unpivot":
            # handled in FROM clause below
            continue
        else:
            warnings.append(f"unsupported transform type: {t}")

    # Defensive rewrite: if unpivot is active, remap any lingering 's.' references in projections to the current base alias
    if unpivot_tr and select_cols:
        remapped_cols: List[str] = []
        for c in select_cols:
            try:
                c2 = c.replace("[s].[", f"[{base_alias}].[")
                # also handle non-bracketed patterns like ' s.'
                c2 = c2.replace(" s.", f" {base_alias}.")
                remapped_cols.append(c2)
            except Exception:
                remapped_cols.append(c)
        select_cols = remapped_cols

    # FROM and optional JOINs
    if unpivot_tr:
        # UNION ALL over sourceColumns; include all base columns via s.* and two new columns
        scols = [str(c).strip() for c in (unpivot_tr.get("sourceColumns") or []) if str(c).strip()]
        key_col = str(unpivot_tr.get("keyColumn") or "metric").strip() or "metric"
        val_col = str(unpivot_tr.get("valueColumn") or "value").strip() or "value"
        omit_zero = bool(unpivot_tr.get("omitZeroNull") or False)
        # Build a simple alias->expression map so Unpivot can reference custom columns as expressions
        alias_exprs: Dict[str, str] = {}
        for cc in (custom_columns or []):
            try:
                nm = str((cc or {}).get("name") or "").strip()
                ex = str((cc or {}).get("expr") or "").strip()
                ctype = str((cc or {}).get("type") or "").lower()
                if nm and ex:
                    alias_exprs[_unquote_ident(nm)] = _normalize_expr_idents(d, ex, numericify=(ctype == 'number'))
            except Exception:
                pass
        for tr in (transforms or []):
            try:
                ttype = str((tr or {}).get("type") or "").lower()
                if ttype == "computed":
                    nm = str((tr or {}).get("name") or "").strip()
                    ex = str((tr or {}).get("expr") or "").strip()
                    if nm and ex:
                        alias_exprs[_unquote_ident(nm)] = _normalize_expr_idents(d, ex)
                elif ttype == "case":
                    target = str((tr or {}).get("target") or "").strip()
                    cases = (tr or {}).get("cases") or []
                    else_val = (tr or {}).get("else") or (tr or {}).get("else_")
                    if target and cases:
                        expr = _case_expr(d, target, cases, else_val)
                        alias_exprs[_unquote_ident(target)] = expr
                elif ttype == "replace":
                    target = str((tr or {}).get("target") or "").strip()
                    search = (tr or {}).get("search")
                    repl = (tr or {}).get("replace")
                    if target is not None and search is not None and repl is not None:
                        src = target
                        if src and ("(" not in src and ")" not in src and " " not in src) and "." not in src:
                            src = f"s.{src}"
                        src = _qcol(d, src)
                        expr = _replace_chain(src, search, repl)
                        alias_exprs[_unquote_ident(target)] = expr
                elif ttype == "translate":
                    target = str((tr or {}).get("target") or "").strip()
                    s = str((tr or {}).get("search") or "")
                    r = str((tr or {}).get("replace") or "")
                    if target and s:
                        src = target
                        if src and ("(" not in src and ")" not in src and " " not in src) and "." not in src:
                            src = f"s.{src}"
                        src = _qcol(d, src)
                        expr = _translate_or_replace(d, src, s, r)
                        alias_exprs[_unquote_ident(target)] = expr
                elif ttype == "nullhandling":
                    target = str((tr or {}).get("target") or "").strip()
                    mode = str((tr or {}).get("mode") or "coalesce")
                    val = (tr or {}).get("value")
                    if target:
                        src = target
                        if src and ("(" not in src and ")" not in src and " " not in src) and "." not in src:
                            src = f"s.{src}"
                        src = _qcol(d, src)
                        expr = _null_func(d, mode, src, val)
                        alias_exprs[_unquote_ident(target)] = expr
            except Exception:
                pass
        parts: List[str] = []
        for col in scols:
            label = _lit(_unquote_ident(col))
            # If the source column name matches a custom/computed alias, inline its expression; else reference the physical column
            raw_name = _unquote_ident(col)
            expr_override = alias_exprs.get(raw_name)
            if expr_override:
                expr = expr_override
                # Best-effort: normalize any stray alias references to base alias 's'
                try:
                    expr = expr.replace("[u].[", "[s].[")
                    expr = expr.replace(" u.", " s.")
                    expr = expr.replace(" t.", " s.")
                except Exception:
                    pass
                vexpr = f"({expr})"
            else:
                vexpr = _qcol(d, f"s.{col}")
            where_seg = f" WHERE {vexpr} IS NOT NULL" if omit_zero else ""
            parts.append(
                f"SELECT s.*, {label} AS {_qal(d, key_col)}, {vexpr} AS {_qal(d, val_col)} FROM {_qtable(d, source)} AS s{where_seg}"
            )
        if parts:
            union_sql = " UNION ALL ".join(parts)
        else:
            # If no source columns were provided, emit typed NULL for value column to keep aggregations valid
            if d == 'mssql':
                null_value = f"CAST(NULL AS DECIMAL(38,10))"
            elif d in {'postgres', 'duckdb'}:
                null_value = f"CAST(NULL AS DOUBLE PRECISION)"
            elif d in {'mysql'}:
                null_value = f"CAST(NULL AS DECIMAL(38,10))"
            elif d == 'sqlite':
                null_value = f"CAST(NULL AS REAL)"
            else:
                null_value = f"CAST(NULL AS DOUBLE)"
            union_sql = f"SELECT s.*, NULL AS {_qal(d, key_col)}, {null_value} AS {_qal(d, val_col)} FROM {_qtable(d, source)} AS s"
        from_sql = f"FROM ({union_sql}) AS u"
    else:
        from_sql = f"FROM {_qtable(d, source)} AS s"
    if joins:
        for i, j in enumerate(joins):
            jtype = (j.get("joinType") or "left").upper()
            ttable = str(j.get("targetTable") or "")
            skey = str(j.get("sourceKey") or "")
            tkey = str(j.get("targetKey") or "")
            
            # Handle LATERAL joins
            if jtype == "LATERAL":
                lateral_cfg = j.get("lateral") or {}
                correlations = lateral_cfg.get("correlations") or []
                order_by = lateral_cfg.get("orderBy") or []
                limit = lateral_cfg.get("limit")
                subquery_alias = lateral_cfg.get("subqueryAlias") or f"lat{i+1}"
                
                if not ttable:
                    warnings.append("LATERAL join missing targetTable")
                    continue
                
                # Build WHERE clause from correlations
                where_parts = []
                for corr in correlations:
                    src_col = str(corr.get("sourceCol") or "")
                    op = str(corr.get("op") or "eq")
                    tgt_col = str(corr.get("targetCol") or "")
                    if src_col and tgt_col:
                        op_map = {"eq": "=", "ne": "!=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
                        op_sql = op_map.get(op, "=")
                        where_parts.append(f"{_qcol(d, tgt_col)} {op_sql} {_qcol(d, f'{base_alias}.{src_col}')}")
                
                where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
                
                # Build ORDER BY clause
                order_clause = ""
                if order_by:
                    order_items = []
                    for ob in order_by:
                        col = str(ob.get("column") or "")
                        direction = str(ob.get("direction") or "ASC").upper()
                        if col:
                            order_items.append(f"{_qcol(d, col)} {direction}")
                    if order_items:
                        order_clause = f" ORDER BY {', '.join(order_items)}"
                
                # Build LIMIT clause
                limit_clause = f" LIMIT {int(limit)}" if limit else ""
                
                # Get columns to select
                cols = j.get("columns") or []
                if cols:
                    col_list = ", ".join([_qcol(d, str((col or {}).get("name") or "")) for col in cols if (col or {}).get("name")])
                else:
                    col_list = "*"
                
                # Build LATERAL subquery
                lateral_subquery = f"(SELECT {col_list} FROM {_qtable(d, ttable)}{where_clause}{order_clause}{limit_clause})"
                
                # Add LATERAL join to FROM clause
                from_sql += f" LEFT JOIN LATERAL {lateral_subquery} AS {_qal(d, subquery_alias)} ON true"
                
                # Add selected columns to output
                for col in cols:
                    cname = str((col or {}).get("name") or "")
                    alias_col = str((col or {}).get("alias") or cname)
                    if cname:
                        select_cols.append(f"{_qcol(d, f'{subquery_alias}.{cname}')} AS {_qal(d, alias_col)}")
                
                continue
            
            # Regular join handling
            if not (ttable and skey and tkey):
                warnings.append("join missing target/sourceKey/targetKey")
                continue
            alias = f"j{i+1}"
            agg = j.get("aggregate")
            jfilter = j.get("filter") or None
            if agg and isinstance(agg, dict) and agg.get("fn") and agg.get("alias") is not None:
                fn = str(agg.get("fn"))
                col = str(agg.get("column") or "*")
                al = str(agg.get("alias"))
                expr, _warn = _agg_expr(d, fn, col)
                where_sql = ""
                on_extra = ""
                if isinstance(jfilter, dict) and jfilter.get("op") and jfilter.get("left"):
                    raw_lhs = str(jfilter.get("left"))
                    # If filter targets joined table (t.* or unqualified), push into subquery
                    if raw_lhs.startswith("t.") or ("." not in raw_lhs and raw_lhs):
                        filt = dict(jfilter)
                        try:
                            lhs = str(filt.get("left"))
                            # strip any alias (t.) for subquery scope
                            if "." in lhs:
                                lhs = lhs.split(".", 1)[1]
                            # quote simple identifier inside subquery context
                            filt["left"] = _qleft_if_ident(d, lhs)
                        except Exception:
                            pass
                        where_sql = f" WHERE {_cond_sql(filt)}"
                    else:
                        # source-side filter -> attach to ON clause with base alias s.
                        filt = dict(jfilter)
                        try:
                            lhs = str(filt.get("left"))
                            if lhs.startswith("t."):
                                lhs = lhs.replace("t.", f"{alias}.", 1)
                            elif lhs.startswith("s."):
                                lhs = lhs
                            elif "." not in lhs:
                                # unqualified treated as joined alias
                                lhs = f"{alias}.{lhs}"
                            filt["left"] = lhs
                        except Exception:
                            pass
                        on_extra = f" AND ({_cond_sql(filt)})"
                sub = f"(SELECT {_qcol(d, tkey)} AS __k, {expr} AS {al} FROM {_qtable(d, ttable)}{where_sql} GROUP BY {_qcol(d, tkey)})"
                from_sql += f" {jtype} JOIN {sub} AS {alias} ON {_qcol(d, f'{base_alias}.{skey}')} = {alias}.__k{on_extra}"
                select_cols.append(f"{alias}.{_qcol(d, al)} AS {_qal(d, al)}")
            else:
                on_extra = ""
                if isinstance(jfilter, dict) and jfilter.get("op") and jfilter.get("left"):
                    filt = dict(jfilter)
                    # Map 't.' to this join alias; ensure unqualified names refer to joined alias
                    try:
                        lhs = str(filt.get("left"))
                        if lhs.startswith("t."):
                            lhs = lhs.replace("t.", f"{alias}.", 1)
                        elif lhs.startswith("s."):
                            lhs = lhs  # base alias 's'
                        elif "." not in lhs:
                            lhs = f"{alias}.{lhs}"
                        filt["left"] = lhs
                    except Exception:
                        pass
                    # Quote left if simple identifier
                    try:
                        filt["left"] = _qleft_if_ident(d, str(filt.get("left")))
                    except Exception:
                        pass
                    try:
                        if d == 'duckdb' and str(filt.get('op') or '').lower() == 'eq' and isinstance(filt.get('right'), str):
                            on_extra = f" AND (lower(trim({str(filt.get('left'))})) = lower(trim({_lit(str(filt.get('right')))})))"
                        else:
                            on_extra = f" AND ({_cond_sql(filt)})"
                    except Exception:
                        on_extra = f" AND ({_cond_sql(filt)})"
                lhs_on = _qcol(d, f'{base_alias}.{skey}')
                rhs_on = _qcol(d, f'{alias}.{tkey}')
                if d == 'duckdb':
                    # Optimized join: cast both sides to VARCHAR and compare directly
                    # This is much faster than trying multiple type conversions with OR conditions
                    lhs_str = f"CAST({lhs_on} AS VARCHAR)"
                    rhs_str = f"CAST({rhs_on} AS VARCHAR)"
                    on_cmp = f"({lhs_str} = {rhs_str})"
                    from_sql += f" {jtype} JOIN {_qtable(d, ttable)} AS {alias} ON {on_cmp} {on_extra}"
                else:
                    from_sql += f" {jtype} JOIN {_qtable(d, ttable)} AS {alias} ON {lhs_on} = {rhs_on} {on_extra}"
                cols = j.get("columns") or []
                for col in cols:
                    cname = str((col or {}).get("name") or "")
                    alias_col = str((col or {}).get("alias") or cname)
                    if cname:
                        select_cols.append(f"{_qcol(d, f'{alias}.{cname}')} AS {_qal(d, alias_col)}")

    # ORDER BY & LIMIT from defaults
    order_sql = ""
    dfl = defaults or {}
    sort = dfl.get("sort") or {}
    if isinstance(sort, dict) and sort.get("by"):
        by = str(sort.get("by"))
        dir_ = str(sort.get("direction") or "desc").upper()
        if dir_ not in {"ASC", "DESC"}:
            dir_ = "DESC"
        order_sql = f" ORDER BY {_order_token(d, by)} {dir_}"
    topn = dfl.get("limitTopN") or {}
    lim_val = None
    if isinstance(topn, dict) and topn.get("n"):
        lim_val = int(topn.get("n") or 0)
        # If no explicit sort, use TopN's by/direction to define ordering
        if not order_sql and topn.get("by"):
            by2 = str(topn.get("by"))
            dir2 = str(topn.get("direction") or "desc").upper()
            if dir2 not in {"ASC", "DESC"}:
                dir2 = "DESC"
            order_sql = f" ORDER BY {_order_token(d, by2)} {dir2}"
    if limit and (not lim_val or limit < lim_val):
        lim_val = int(limit)

    # Deduplicate identical projections to avoid 'column specified multiple times'
    if select_cols:
        seen: set[str] = set()
        uniq: list[str] = []
        for c in select_cols:
            k = c.strip().lower()
            if k in seen:
                continue
            seen.add(k)
            uniq.append(c)
        select_cols = uniq
    select_sql = ", ".join(select_cols) if select_cols else "*"
    if d == "mssql" and lim_val:
        sql = f"SELECT TOP {int(lim_val)} {select_sql} {from_sql}{order_sql}"
    else:
        sql = f"SELECT {select_sql} {from_sql}{order_sql}"
        if lim_val:
            sql += f" LIMIT {int(lim_val)}"

    # Heuristic list of return columns: strip aliases
    colnames: List[str] = []
    for c in select_cols:
        if " AS " in c.upper():
            # split by last AS
            up = c.upper()
            idx = up.rfind(" AS ")
            colnames.append(_unquote_ident(c[idx+4:].strip()))
        else:
            # bare column or *
            colnames.append(_unquote_ident(c.strip()))
    print(f"[build_sql] Final SELECT columns: {colnames}", flush=True)
    print(f"[build_sql] Generated SQL (first 500 chars): {sql[:500]}", flush=True)
    return sql, colnames, warnings


# Build a DISTINCT query for a single field with optional WHERE (equality, IN, and range ops)
def build_distinct_sql(
    *,
    dialect: str,
    source: str,
    field: str,
    where: Dict[str, Any] | None,
) -> Tuple[str, Dict[str, Any]]:
    d = _dialect_name(dialect)
    qtable = _qtable(d, source)
    # Support derived date parts specified as "BaseField (Part)"
    DERIVED_RE = re.compile(r"^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$")
    m = DERIVED_RE.match(str(field or ""))
    if m:
        base = m.group(1).strip()
        part = m.group(2)
        col = _qcol(d, base)
        # Dialect-aware date-part extraction
        if d == 'mssql':
            if part == 'Year': expr = f"YEAR({col})"
            elif part == 'Quarter': expr = f"DATEPART(quarter, {col})"
            elif part == 'Month': expr = f"MONTH({col})"
            elif part == 'Month Name': expr = f"DATENAME(month, {col})"
            elif part == 'Month Short': expr = f"LEFT(DATENAME(month, {col}), 3)"
            elif part == 'Week': expr = f"DATEPART(iso_week, {col})"
            elif part == 'Day': expr = f"DAY({col})"
            elif part == 'Day Name': expr = f"DATENAME(weekday, {col})"
            elif part == 'Day Short': expr = f"LEFT(DATENAME(weekday, {col}), 3)"
            else: expr = col
        elif d in {'postgres', 'duckdb'}:
            if part == 'Year': expr = f"EXTRACT(year FROM {col})"
            elif part == 'Quarter': expr = f"EXTRACT(quarter FROM {col})"
            elif part == 'Month': expr = f"EXTRACT(month FROM {col})"
            elif part == 'Month Name': expr = f"to_char({col}, 'FMMonth')"
            elif part == 'Month Short': expr = f"to_char({col}, 'Mon')"
            elif part == 'Week': expr = f"EXTRACT(week FROM {col})"
            elif part == 'Day': expr = f"EXTRACT(day FROM {col})"
            elif part == 'Day Name': expr = f"to_char({col}, 'FMDay')"
            elif part == 'Day Short': expr = f"to_char({col}, 'Dy')"
            else: expr = col
        elif d == 'mysql':
            if part == 'Year': expr = f"YEAR({col})"
            elif part == 'Quarter': expr = f"QUARTER({col})"
            elif part == 'Month': expr = f"MONTH({col})"
            elif part == 'Month Name': expr = f"DATE_FORMAT({col}, '%M')"
            elif part == 'Month Short': expr = f"DATE_FORMAT({col}, '%b')"
            elif part == 'Week': expr = f"WEEK({col}, 3)"  # ISO week, Mon start
            elif part == 'Day': expr = f"DAY({col})"
            elif part == 'Day Name': expr = f"DATE_FORMAT({col}, '%W')"
            elif part == 'Day Short': expr = f"DATE_FORMAT({col}, '%a')"
            else: expr = col
        elif d == 'sqlite':
            if part == 'Year': expr = f"CAST(strftime('%Y', {col}) AS INTEGER)"
            elif part == 'Quarter': expr = (
                f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                f"WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 "
                f"WHEN 4 THEN 2 WHEN 5 THEN 2 WHEN 6 THEN 2 "
                f"WHEN 7 THEN 3 WHEN 8 THEN 3 WHEN 9 THEN 3 "
                f"ELSE 4 END"
            )
            elif part == 'Month': expr = f"CAST(strftime('%m', {col}) AS INTEGER)"
            elif part == 'Month Name': expr = (
                f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                f"WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April' "
                f"WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' "
                f"WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December' END"
            )
            elif part == 'Month Short': expr = (
                f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                f"WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr' "
                f"WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' "
                f"WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec' END"
            )
            elif part == 'Week': expr = f"CAST(strftime('%W', {col}) AS INTEGER)"  # Monday start
            elif part == 'Day': expr = f"CAST(strftime('%d', {col}) AS INTEGER)"
            elif part == 'Day Name': expr = (
                f"CASE strftime('%w', {col}) "
                f"WHEN '0' THEN 'Sunday' WHEN '1' THEN 'Monday' WHEN '2' THEN 'Tuesday' WHEN '3' THEN 'Wednesday' "
                f"WHEN '4' THEN 'Thursday' WHEN '5' THEN 'Friday' WHEN '6' THEN 'Saturday' END"
            )
            elif part == 'Day Short': expr = (
                f"CASE strftime('%w', {col}) "
                f"WHEN '0' THEN 'Sun' WHEN '1' THEN 'Mon' WHEN '2' THEN 'Tue' WHEN '3' THEN 'Wed' "
                f"WHEN '4' THEN 'Thu' WHEN '5' THEN 'Fri' WHEN '6' THEN 'Sat' END"
            )
            else: expr = col
        else:
            # Fallback: return unmodified column (client may compute)
            expr = col
        qfield = expr
    else:
        qfield = _qcol(d, field)

    where_clauses: list[str] = []
    params: Dict[str, Any] = {}
    if where:
        for k, v in where.items():
            # Skip global non-column keys if present
            if k in {"start", "startDate", "end", "endDate"}:
                continue
            if v is None:
                where_clauses.append(f"{_qcol(d, k)} IS NULL")
            elif isinstance(v, (list, tuple)):
                if len(v) == 0:
                    continue
                pnames = []
                for i, item in enumerate(v):
                    pname = f"w_{_unquote_ident(k)}_{i}"
                    params[pname] = item
                    pnames.append(f":{pname}")
                where_clauses.append(f"{_qcol(d, k)} IN ({', '.join(pnames)})")
            elif isinstance(k, str) and "__" in k:
                base, op = k.split("__", 1)
                opname = None
                if op == "gte": opname = ">="
                elif op == "gt": opname = ">"
                elif op == "lte": opname = "<="
                elif op == "lt": opname = "<"
                elif op == "ne": opname = "!="
                if opname:
                    pname = f"w_{_unquote_ident(base)}_{op}"
                    params[pname] = v
                    where_clauses.append(f"{_qcol(d, base)} {opname} :{pname}")
                else:
                    pname = f"w_{_unquote_ident(k)}"
                    where_clauses.append(f"{_qcol(d, k)} = :{pname}")
                    params[pname] = v
            else:
                pname = f"w_{_unquote_ident(k)}"
                where_clauses.append(f"{_qcol(d, k)} = :{pname}")
                params[pname] = v

    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    # Alias the output column to the original field name for stable client mapping
    sql = f"SELECT DISTINCT {qfield} AS {_qal(d, field)} FROM {qtable}{where_sql} ORDER BY 1"
    return sql, params
