from __future__ import annotations

import time
from typing import Optional, Any, Dict, Tuple
import decimal
import binascii
import re
import logging

import os
import sys
import math
import threading

logger = logging.getLogger(__name__)

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from ..db import get_active_duck_path, get_duckdb_engine, get_engine_from_dsn, open_duck_native
from ..sqlgen import build_sql, build_distinct_sql
from ..sqlgen_glot import SQLGlotBuilder, should_use_sqlglot
from ..sql_dialect_normalizer import normalize_sql_expression
import json
from dateutil import parser as date_parser
from ..models import SessionLocal, Datasource, User, DatasourceShare, get_share_link_by_public, verify_share_link_token
from ..schemas import QueryRequest, QueryResponse, QuerySpecRequest, DistinctRequest, DistinctResponse, PivotRequest
from ..security import decrypt_text
from ..config import settings
from urllib.parse import unquote, urlparse
from ..metrics import counter_inc, summary_observe, gauge_inc, gauge_dec
from ..metrics_state import touch_actor

try:
    import duckdb as _duckdb
except Exception:  # pragma: no cover
    _duckdb = None

# Note: Ibis can be heavy to import; to keep startup/reload fast, we lazily import
# it inside the /query/spec handler only when needed.

router = APIRouter(prefix="/query", tags=["query"])

# Concurrency limiter for heavy endpoints (configurable via env var HEAVY_QUERY_CONCURRENCY)
_HEAVY_LIMIT = 8
try:
    _HEAVY_LIMIT = int(os.environ.get("HEAVY_QUERY_CONCURRENCY", "8") or "8")
except Exception:
    _HEAVY_LIMIT = 8


# ─── Date preset resolution (server-side) ─────────────────────────────
# Converts symbolic __date_preset values to concrete __gte/__lt date ranges
# at query execution time so cron jobs / alerts always use the current date.
_UI_META_KEYS = frozenset({
    "filterPreset", "filter_preset", "_preset", "_meta",
    "startDate", "endDate", "start", "end",
    "__week_start_day", "__weekends",
})

def _strip_ui_op_keys(where: dict | None) -> dict | None:
    """Strip frontend-only operator hint keys (field__op) that are not real SQL operators."""
    if not where:
        return where
    return {k: v for k, v in where.items() if not (isinstance(k, str) and k.endswith("__op"))}

def _resolve_date_presets(where: dict | None) -> dict | None:
    """Expand any `field__date_preset` entries into `field__gte` / `field__lt` pairs.

    Week-start convention (``__week_start_day`` key in *where* or ``WEEK_START_DAY`` env var):
      0 = Sunday (default), 1 = Monday
    """
    if not where:
        return where
    import os
    from datetime import datetime, timedelta

    # Read __week_start_day BEFORE stripping UI meta keys.
    # Accepts DDD names (SUN/MON/TUE/WED/THU/FRI/SAT) or legacy 0/1 integers.
    # Internal: 0=Sunday-start, 1=Monday-start.
    _WSD_MAP = {"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6}
    _wsd_raw = str(where.get("__week_start_day", os.environ.get("WEEK_START_DAY", "SUN"))).upper().strip()
    if _wsd_raw in _WSD_MAP:
        week_start_dow = _WSD_MAP[_wsd_raw]
    else:
        try:
            week_start_dow = int(_wsd_raw)
        except (ValueError, TypeError):
            week_start_dow = 0  # default Sunday

    # Read __weekends BEFORE stripping UI meta keys.
    # SAT_SUN (default): weekend days are Sat(5) and Sun(6) in Python weekday (Mon=0).
    # FRI_SAT: weekend days are Fri(4) and Sat(5).
    _weekends_raw = str(where.get("__weekends", os.environ.get("WEEKENDS", "SAT_SUN"))).upper().strip()
    _WEEKENDS_MAP = {"SAT_SUN": (5, 6), "FRI_SAT": (4, 5)}
    weekend_days: tuple[int, int] = _WEEKENDS_MAP.get(_weekends_raw, (5, 6))
    # Working week starts on the first non-weekend day (Mon for SAT_SUN, Sun for FRI_SAT)
    _working_week_start_dow = 6 if _weekends_raw == "FRI_SAT" else 0  # Python: Mon=0…Sun=6

    # Read operator hints (field__op) BEFORE stripping, so preset expansion can respect them.
    # e.g. {"Time__op": "lt"} means "Time < preset_date" → only emit the lt bound.
    _op_hints: dict[str, str] = {}
    for _ok, _ov in where.items():
        if isinstance(_ok, str) and _ok.endswith("__op") and isinstance(_ov, str):
            _op_hints[_ok[:-4]] = _ov.lower().strip()  # base_field → operator

    # Strip UI-only meta keys that are not real database columns
    where = {k: v for k, v in where.items() if k not in _UI_META_KEYS}
    # Strip frontend-only operator hint keys (field__op) that are not real SQL operators
    where = _strip_ui_op_keys(where) or {}

    # ── helpers ──────────────────────────────────────────────────────────────

    def _week_start(d: datetime) -> datetime:
        """Most recent week-start day at or before *d* per week_start_dow."""
        # Python weekday(): Mon=0 … Sun=6
        if week_start_dow == 0:          # Sunday-start: offset = (weekday+1) % 7
            offset = (d.weekday() + 1) % 7
        else:                             # Monday-start: offset = weekday()
            offset = d.weekday()
        return d - timedelta(days=offset)

    def _prev_workday(d: datetime) -> datetime:
        """Last working day strictly before *d* (skips weekend_days)."""
        candidate = d - timedelta(days=1)
        while candidate.weekday() in weekend_days:
            candidate -= timedelta(days=1)
        return candidate

    def _working_week_start(d: datetime) -> datetime:
        """Most recent working-week start at or before *d*."""
        candidate = datetime(d.year, d.month, d.day)
        while candidate.weekday() != _working_week_start_dow:
            candidate -= timedelta(days=1)
        return candidate

    # ── preset resolution ────────────────────────────────────────────────────

    expanded: dict = {}
    for k, v in where.items():
        if isinstance(k, str) and k.endswith("__date_preset") and isinstance(v, str):
            base = k[: -len("__date_preset")]
            now = datetime.now()
            today = datetime(now.year, now.month, now.day)
            preset = v.lower().strip()
            gte: datetime | None = None
            lt: datetime | None = None

            if preset == "today":
                gte = today; lt = today + timedelta(days=1)
            elif preset == "yesterday":
                gte = today - timedelta(days=1); lt = today
            elif preset == "day_before_yesterday":
                gte = today - timedelta(days=2); lt = today - timedelta(days=1)
            elif preset == "last_working_day":
                lwd = _prev_workday(today)
                gte = lwd; lt = lwd + timedelta(days=1)
            elif preset == "day_before_last_working_day":
                lwd = _prev_workday(today)
                dlwd = _prev_workday(lwd)
                gte = dlwd; lt = dlwd + timedelta(days=1)
            elif preset == "last_working_week":
                ws = _working_week_start(today)
                gte = ws - timedelta(days=7); lt = ws
            elif preset == "week_before_last_working_week":
                ws = _working_week_start(today)
                gte = ws - timedelta(days=14); lt = ws - timedelta(days=7)
            elif preset == "this_week":
                ws = _week_start(today)
                gte = ws; lt = ws + timedelta(days=7)
            elif preset == "last_week":
                ws = _week_start(today)
                gte = ws - timedelta(days=7); lt = ws
            elif preset == "week_before_last":
                ws = _week_start(today)
                gte = ws - timedelta(days=14); lt = ws - timedelta(days=7)
            elif preset == "this_month":
                gte = datetime(now.year, now.month, 1)
                if now.month == 12:
                    lt = datetime(now.year + 1, 1, 1)
                else:
                    lt = datetime(now.year, now.month + 1, 1)
            elif preset == "last_month":
                first_this = datetime(now.year, now.month, 1)
                lt = first_this
                if now.month == 1:
                    gte = datetime(now.year - 1, 12, 1)
                else:
                    gte = datetime(now.year, now.month - 1, 1)
            elif preset == "this_quarter":
                q = (now.month - 1) // 3
                gte = datetime(now.year, q * 3 + 1, 1)
                nq = q + 1
                if nq > 3:
                    lt = datetime(now.year + 1, 1, 1)
                else:
                    lt = datetime(now.year, nq * 3 + 1, 1)
            elif preset == "last_quarter":
                q = (now.month - 1) // 3
                pq = q - 1
                if pq < 0:
                    gte = datetime(now.year - 1, 10, 1)
                    lt = datetime(now.year, 1, 1)
                else:
                    gte = datetime(now.year, pq * 3 + 1, 1)
                    lt = datetime(now.year, q * 3 + 1, 1)
            elif preset == "this_year":
                gte = datetime(now.year, 1, 1)
                lt = datetime(now.year + 1, 1, 1)
            elif preset == "last_year":
                gte = datetime(now.year - 1, 1, 1)
                lt = datetime(now.year, 1, 1)
            else:
                # Unknown preset – pass through as-is
                expanded[k] = v
                continue
            _bound_op = _op_hints.get(base, '')
            if gte and _bound_op not in ('lt', 'lte'):
                _new_gte = gte.strftime("%Y-%m-%d")
                _exist_gte = expanded.get(f"{base}__gte")
                if _bound_op in ('gte', 'gt') and _exist_gte:
                    # Single-bound preset: take the more restrictive (larger) gte
                    expanded[f"{base}__gte"] = max(_exist_gte, _new_gte)
                else:
                    # Range preset: override completely
                    expanded[f"{base}__gte"] = _new_gte
            if lt and _bound_op not in ('gte', 'gt'):
                _new_lt = lt.strftime("%Y-%m-%d")
                _exist_lt = expanded.get(f"{base}__lt")
                if _bound_op in ('lt', 'lte') and _exist_lt:
                    # Single-bound preset: take the more restrictive (smaller) lt
                    expanded[f"{base}__lt"] = min(_exist_lt, _new_lt)
                else:
                    # Range preset: override completely
                    expanded[f"{base}__lt"] = _new_lt
        else:
            expanded[k] = v
    import sys
    sys.stderr.write(f"[DATE_PRESET_DEBUG] Input WHERE keys: {list(where.items())}\n")
    sys.stderr.write(f"[DATE_PRESET_DEBUG] Output WHERE keys: {list(expanded.items())}\n")
    sys.stderr.flush()
    return expanded


# SQLGlot helper functions (module-level to be reusable across endpoints)
def _build_expr_map_helper(ds: Any, source_name: str, ds_type: str, _apply_scope_func, available_columns: set[str] | None = None) -> dict:
    """
    Build mapping of derived column names to SQL expressions.
    
    Only includes custom columns whose referenced base columns exist in available_columns.
    This prevents errors when custom columns reference columns from joins that aren't present.
    """
    from ..sqlgen import _normalize_expr_idents
    # Note: re is imported at module level
    expr_map = {}
    
    if not ds:
        return expr_map
    
    # Helper to extract column references from an expression
    def extract_column_refs(expr_str: str) -> set[str]:
        """Extract quoted/bracketed column identifiers from expression"""
        refs = set()
        # Match quoted identifiers: "ColumnName", [ColumnName], or `ColumnName`
        for match in re.finditer(r'["\[`]([^"\]`]+)["\]`]', expr_str):
            refs.add(match.group(1).lower())
        return refs
    
    try:
        # Handle both dict and object forms
        if isinstance(ds, dict):
            raw_json = ds.get("options_json") or "{}"
        else:
            raw_json = ds.options_json or "{}"
        opts = json.loads(raw_json)
        ds_transforms = opts.get("transforms")
        if (not isinstance(ds_transforms, dict)) and isinstance(opts, dict):
            if any(k in opts for k in ("customColumns", "transforms", "joins", "defaults")):
                ds_transforms = opts
        ds_transforms = ds_transforms or {}
        ds_transforms = _apply_scope_func(ds_transforms, source_name)
        
        # Normalize available columns for comparison
        avail_lower = {c.lower() for c in (available_columns or set())} if available_columns else None
        
        # From customColumns
        custom_cols = ds_transforms.get("customColumns") or []
        for col in custom_cols:
            if isinstance(col, dict) and col.get("name") and col.get("expr"):
                # If available_columns provided, validate that all referenced columns exist
                if avail_lower is not None:
                    expr_str = str(col.get("expr") or "")
                    refs = extract_column_refs(expr_str)
                    # Skip if any referenced column is missing
                    missing = refs - avail_lower
                    if missing:
                        logger.debug(f"[expr_map] Skipping custom column '{col['name']}': references missing columns {missing}")
                        continue
                
                # Normalize bracket identifiers for target dialect
                expr = _normalize_expr_idents(ds_type, col["expr"])
                expr_map[col["name"]] = expr
                # Debug: Log full expression for CASE statements
                if "CASE" in expr.upper():
                    logger.info(f"[expr_map] Custom column '{col['name']}' CASE expression: {expr}")
                    sys.stderr.write(f"[expr_map] Custom column '{col['name']}' has CASE expression length={len(expr)}\n")
                    sys.stderr.write(f"[expr_map] Expression: {expr}\n")
                    sys.stderr.flush()
        
        # From computed transforms
        transforms = ds_transforms.get("transforms") or []
        for t in transforms:
            if isinstance(t, dict) and t.get("type") == "computed":
                if t.get("name") and t.get("expr"):
                    # If available_columns provided, validate that all referenced columns exist
                    if avail_lower is not None:
                        expr_str = str(t.get("expr") or "")
                        refs = extract_column_refs(expr_str)
                        # Skip if any referenced column is missing
                        missing = refs - avail_lower
                        if missing:
                            logger.debug(f"[expr_map] Skipping computed transform '{t['name']}': references missing columns {missing}")
                            continue
                    
                    # Normalize bracket identifiers for target dialect
                    expr = _normalize_expr_idents(ds_type, t["expr"])
                    expr_map[t["name"]] = expr
    
    except Exception as e:
        logger.error(f"[SQLGlot] Failed to build expr_map: {e}")
    
    return expr_map


def _build_datepart_expr_helper(base_col: str, kind: str, dialect: str) -> str:
    """Build dialect-specific date part expression (e.g., OrderDate (Year)) - module level helper"""
    q = f'"{base_col}"'  # Quoted identifier
    kind_l = kind.lower()
    
    # DuckDB
    if "duckdb" in dialect.lower():
        if kind_l == 'year': return f"EXTRACT(YEAR FROM {q})"
        if kind_l == 'quarter': return f"EXTRACT(QUARTER FROM {q})"
        if kind_l == 'month': return f"EXTRACT(MONTH FROM {q})"
        if kind_l == 'month name': return f"strftime({q}, '%B')"
        if kind_l == 'month short': return f"strftime({q}, '%b')"
        if kind_l == 'week': return f"EXTRACT(WEEK FROM {q})"
        if kind_l == 'day': return f"EXTRACT(DAY FROM {q})"
        if kind_l == 'day name': return f"strftime({q}, '%A')"
        if kind_l == 'day short': return f"strftime({q}, '%a')"
    
    # PostgreSQL
    elif "postgres" in dialect.lower() or "postgre" in dialect.lower():
        if kind_l == 'year': return f"to_char({q}, 'YYYY')"
        if kind_l == 'quarter': return f"to_char({q}, 'YYYY-\"Q\"Q')"
        if kind_l == 'month': return f"to_char({q}, 'YYYY-MM')"
        if kind_l == 'month name': return f"to_char({q}, 'FMMonth')"
        if kind_l == 'month short': return f"to_char({q}, 'Mon')"
        if kind_l == 'week': return f"to_char({q}, 'YYYY') || '-W' || lpad(to_char({q}, 'IW'), 2, '0')"
        if kind_l == 'day': return f"to_char({q}, 'DD')"
        if kind_l == 'day name': return f"to_char({q}, 'FMDay')"
        if kind_l == 'day short': return f"to_char({q}, 'Dy')"
    
    # MSSQL
    elif "mssql" in dialect.lower() or "sqlserver" in dialect.lower():
        if kind_l == 'year': return f"YEAR({q})"
        if kind_l == 'quarter': return f"DATEPART(QUARTER, {q})"
        if kind_l == 'month': return f"MONTH({q})"
        if kind_l == 'month name': return f"DATENAME(MONTH, {q})"
        if kind_l == 'month short': return f"LEFT(DATENAME(MONTH, {q}), 3)"
        if kind_l == 'week': return f"DATEPART(WEEK, {q})"
        if kind_l == 'day': return f"DAY({q})"
        if kind_l == 'day name': return f"DATENAME(WEEKDAY, {q})"
        if kind_l == 'day short': return f"LEFT(DATENAME(WEEKDAY, {q}), 3)"
    
    # MySQL
    elif "mysql" in dialect.lower():
        if kind_l == 'year': return f"YEAR({q})"
        if kind_l == 'quarter': return f"QUARTER({q})"
        if kind_l == 'month': return f"MONTH({q})"
        if kind_l == 'month name': return f"MONTHNAME({q})"
        if kind_l == 'month short': return f"DATE_FORMAT({q}, '%b')"
        if kind_l == 'week': return f"WEEK({q})"
        if kind_l == 'day': return f"DAY({q})"
        if kind_l == 'day name': return f"DAYNAME({q})"
        if kind_l == 'day short': return f"DATE_FORMAT({q}, '%a')"
    
    # Default fallback (DuckDB-like)
    return f"EXTRACT({kind_l.upper()} FROM {q})"


def _resolve_derived_columns_in_where_helper(where: dict, ds: Any, source_name: str, ds_type: str) -> dict:
    """Resolve derived column names to SQL expressions in WHERE clause - module level helper"""
    import sys
    sys.stderr.write(f"[SQLGlot] _resolve_derived_columns_in_where_helper CALLED with where keys: {list(where.keys()) if where else 'None'}\n")
    sys.stderr.flush()
    
    if not where:
        return where
    
    if not ds:
        sys.stderr.write("[SQLGlot] No datasource provided for resolution\n")
        sys.stderr.flush()
        return where
    
    try:
        # Build expr_map from datasource using the module-level helper
        def _apply_scope_for_helper(ds_tr: dict, src: str) -> dict:
            """Apply scope filtering for the helper function"""
            if not isinstance(ds_tr, dict):
                return {}
            def norm(s: str) -> str:
                s = (s or '').strip().strip('[]').strip('"').strip('`')
                parts = s.split('.')
                return parts[-1].lower()
            def filt(arr):
                out = []
                for it in (arr or []):
                    sc = (it or {}).get('scope')
                    if not sc:
                        out.append(it); continue
                    lvl = str(sc.get('level') or '').lower()
                    if lvl == 'datasource':
                        out.append(it)
                    elif lvl == 'table' and sc.get('table') and norm(str(sc.get('table'))) == norm(src):
                        out.append(it)
                return out
            return {
                'customColumns': filt(ds_tr.get('customColumns')),
                'transforms': filt(ds_tr.get('transforms')),
                'joins': filt(ds_tr.get('joins')),
                'defaults': ds_tr.get('defaults') or {},
            }
        
        expr_map = _build_expr_map_helper(ds, source_name, ds_type, _apply_scope_for_helper, None)
        
        # Resolve WHERE clause
        sys.stderr.write(f"[SQLGlot] Built expr_map with {len(expr_map)} entries: {list(expr_map.keys())}\n")
        sys.stderr.write(f"[SQLGlot] WHERE keys to resolve: {list(where.keys())}\n")
        sys.stderr.flush()
        
        resolved = {}
        resolved_count = 0
        for key, value in where.items():
            # Extract base column name (remove operators like __ne, __gte, __in)
            base_key = key.split("__")[0] if "__" in key else key
            op_suffix = key.split("__", 1)[1] if "__" in key else None
            
            # First check if it's a custom column
            if base_key in expr_map:
                expr = expr_map[base_key]
                # Strip table aliases - handle both quoted and unquoted (e.g., s.ClientID or "s"."ClientID" -> ClientID)
                expr = re.sub(r'"[a-z][a-z_]{0,4}"\.', '', expr)  # Quoted aliases like "s".
                expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)  # Unquoted aliases like s.
                # Rebuild key with operator suffix if present
                resolved_key = f"({expr})" if not op_suffix else f"({expr})__{op_suffix}"
                sys.stderr.write(f"[SQLGlot] [OK] Resolved custom column '{key}' -> {resolved_key[:80]}...\n")
                sys.stderr.flush()
                resolved[resolved_key] = value
                resolved_count += 1
            # Check if it's a date part pattern like "OrderDate (Year)"
            elif " (" in base_key and ")" in base_key:
                match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", base_key, flags=re.IGNORECASE)
                if match:
                    base_col = match.group(1).strip()
                    kind = match.group(2).lower()
                    expr = _build_datepart_expr_helper(base_col, kind, ds_type)
                    # Rebuild key with operator suffix if present
                    resolved_key = f"({expr})" if not op_suffix else f"({expr})__{op_suffix}"
                    sys.stderr.write(f"[SQLGlot] [OK] Resolved date part '{key}' -> {resolved_key[:80]}...\n")
                    sys.stderr.flush()
                    resolved[resolved_key] = value
                    resolved_count += 1
                else:
                    resolved[key] = value
            else:
                resolved[key] = value
        
        sys.stderr.write(f"[SQLGlot] Resolution complete: {resolved_count}/{len(where)} columns resolved\n")
        sys.stderr.flush()
        return resolved
        
    except Exception as e:
        logger.error(f"[SQLGlot] Failed to resolve derived columns: {e}", exc_info=True)
        sys.stderr.write(f"[SQLGlot] Failed to resolve derived columns: {e}\n")
        sys.stderr.flush()
        return where


if _HEAVY_LIMIT <= 0:
    _HEAVY_LIMIT = 1
_HEAVY_SEM = threading.BoundedSemaphore(_HEAVY_LIMIT)

# Helper: Resolve table ID to current name
def _resolve_table_name(ds: Any, source_table_id: str | None, source_name: str | None) -> str | None:
    """
    Resolve a table ID to its current name, falling back to source_name if ID not found.
    
    Args:
        ds: Datasource object or dict
        source_table_id: Stable table ID (format: "{datasourceId}__{originalName}")
        source_name: Fallback table name (used if ID not found or not provided)
    
    Returns:
        Current table name, or None if neither ID nor name provided
    """
    # If no table ID provided, use source name as-is
    if not source_table_id:
        return source_name
    
    # Try to resolve ID to current name
    if ds:
        try:
            # Handle both dict and object forms
            if isinstance(ds, dict):
                raw_json = ds.get("options_json") or "{}"
            else:
                raw_json = getattr(ds, "options_json", None) or "{}"
            
            opts = json.loads(raw_json)
            mappings = opts.get("tableIdMappings") or {}
            
            # Look up current name by ID
            if source_table_id in mappings:
                current_name = mappings[source_table_id]
                logger.info(f"[TableID] Resolved {source_table_id} -> {current_name}")
                return current_name
        except Exception as e:
            logger.warning(f"[TableID] Failed to resolve table ID: {e}")
    
    # Fallback to source name
    if source_name:
        logger.info(f"[TableID] No mapping found for {source_table_id}, using source name: {source_name}")
        return source_name
    
    # Last resort: extract original name from ID
    if source_table_id and "__" in source_table_id:
        original_name = source_table_id.split("__", 1)[1]
        logger.warning(f"[TableID] Extracting original name from ID: {original_name}")
        return original_name
    
    return source_name

# Per-actor in-flight limiter (non-reentrant). Default cap via USER_QUERY_CONCURRENCY.
_ACTOR_CAP = 2
try:
    _ACTOR_CAP = int(os.environ.get("USER_QUERY_CONCURRENCY", "2") or "2")
except Exception:
    _ACTOR_CAP = 2
if _ACTOR_CAP <= 0:
    _ACTOR_CAP = 1
_ACTOR_LOCK = threading.Lock()
_ACTOR_SEMS: Dict[str, threading.BoundedSemaphore] = {}

def _actor_sem(actor_id: Optional[str]) -> Optional[threading.BoundedSemaphore]:
    if not actor_id:
        return None
    k = str(actor_id).strip()
    if not k:
        return None
    with _ACTOR_LOCK:
        sem = _ACTOR_SEMS.get(k)
        if sem is None:
            sem = threading.BoundedSemaphore(_ACTOR_CAP)
            _ACTOR_SEMS[k] = sem
        return sem

def _duck_has_table(table: Optional[str]) -> bool:
    if not table:
        return False
    try:
        if _duckdb is None:
            return False
        db_path = settings.duckdb_path
        t = str(table).strip()
        if not t:
            return False
        with open_duck_native(db_path) as conn:
            try:
                # Try as-is first (for schema-qualified names)
                conn.execute(f"SELECT * FROM {t} LIMIT 0")
                return True
            except Exception:
                try:
                    # Try with double quotes (DuckDB standard)
                    conn.execute(f'SELECT * FROM "{t}" LIMIT 0')
                    return True
                except Exception:
                    try:
                        # Try quoting each part for schema.table
                        if '.' in t:
                            parts = [p.strip().strip('"').strip('`').strip('[]') for p in t.split('.')]
                            quoted = '.'.join([f'"{p}"' for p in parts])
                            conn.execute(f'SELECT * FROM {quoted} LIMIT 0')
                            return True
                    except Exception:
                        pass
                    return False
    except Exception:
        return False

_Q_RATE = 0
_Q_BURST = 0
try:
    _Q_RATE = int(os.environ.get("QUERY_RATE_PER_SEC", "0") or "0")
    _Q_BURST = int(os.environ.get("QUERY_BURST", "0") or "0")
except Exception:
    _Q_RATE = 0
    _Q_BURST = 0
_TB_LOCK = threading.Lock()
_TB_STATE: Dict[str, Tuple[float, float]] = {}

# Optional Redis-backed token bucket for multi-process deployments
_REDIS_URL = os.environ.get("REDIS_URL") or None
_REDIS_PREFIX = os.environ.get("REDIS_PREFIX", "ratelimit")
_RL_REDIS = None  # type: ignore
_RL_SHA = None
_RL_LUA = (
    "local key = KEYS[1]\n"
    "local rate = tonumber(ARGV[1])\n"
    "local burst = tonumber(ARGV[2])\n"
    "local now = tonumber(ARGV[3])\n"
    "local state = redis.call('HMGET', key, 'tokens', 'ts')\n"
    "local tokens = tonumber(state[1])\n"
    "local ts = tonumber(state[2])\n"
    "if (not tokens) or (not ts) then tokens = burst; ts = now end\n"
    "if now > ts then\n"
    "  local delta = now - ts\n"
    "  tokens = math.min(burst, tokens + delta * rate)\n"
    "end\n"
    "if tokens >= 1.0 then\n"
    "  tokens = tokens - 1.0\n"
    "  redis.call('HMSET', key, 'tokens', tokens, 'ts', now)\n"
    "  redis.call('EXPIRE', key, math.max(60, math.ceil(burst / rate)))\n"
    "  return -1\n"
    "else\n"
    "  local need = 1.0 - tokens\n"
    "  local wait = math.ceil(need / rate)\n"
    "  redis.call('HMSET', key, 'tokens', tokens, 'ts', now)\n"
    "  redis.call('EXPIRE', key, math.max(60, math.ceil(burst / rate)))\n"
    "  return wait\n"
    "end\n"
)

def _get_redis():
    global _RL_REDIS, _RL_SHA
    if not _REDIS_URL:
        return None
    try:
        if _RL_REDIS is None:
            import redis  # type: ignore
            _RL_REDIS = redis.from_url(_REDIS_URL, decode_responses=False)
            try:
                _RL_SHA = _RL_REDIS.script_load(_RL_LUA)
            except Exception:
                _RL_SHA = None
        return _RL_REDIS
    except Exception:
        return None

def _tb_redis_take(actor_id: Optional[str]) -> Optional[int]:
    if not actor_id:
        return None
    if _Q_RATE <= 0 or _Q_BURST <= 0:
        return None
    r = _get_redis()
    if not r:
        return None
    key = f"{_REDIS_PREFIX}:{str(actor_id).strip()}"
    now = int(time.time())
    try:
        if _RL_SHA:
            res = r.evalsha(_RL_SHA, 1, key, str(_Q_RATE), str(_Q_BURST), str(now))
        else:
            res = r.eval(_RL_LUA, 1, key, str(_Q_RATE), str(_Q_BURST), str(now))
        try:
            val = int(res)
        except Exception:
            try:
                val = int(res.decode("utf-8"))  # type: ignore[attr-defined]
            except Exception:
                val = -1
        # Lua returns -1 when allowed; otherwise number of seconds to wait
        if val == -1:
            return None
        return max(1, int(val))
    except Exception:
        return None

def _throttle_take(actor_id: Optional[str]) -> Optional[int]:
    if not actor_id:
        return None
    if _Q_RATE <= 0 or _Q_BURST <= 0:
        return None
    # Prefer Redis-backed bucket when configured
    if _REDIS_URL:
        _w = _tb_redis_take(actor_id)
        if _w is not None:
            return _w
    now = time.time()
    k = str(actor_id).strip()
    with _TB_LOCK:
        tokens, last = _TB_STATE.get(k, (_Q_BURST * 1.0, now))
        if now > last:
            tokens = min(float(_Q_BURST), tokens + (now - last) * float(_Q_RATE))
        if tokens >= 1.0:
            tokens -= 1.0
            _TB_STATE[k] = (tokens, now)
            return None
        need = 1.0 - tokens
        wait = int(math.ceil(need / float(_Q_RATE))) if _Q_RATE > 0 else 1
        _TB_STATE[k] = (tokens, now)
        return max(1, wait)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _engine_for_datasource(db: Session, datasource_id: Optional[str], actor_id: Optional[str] = None) -> Engine:
    """Return engine for datasource, enforcing access when actor_id is provided.
    Backward-compatible: if actor_id is None, no enforcement (dev/local flows).
    """
    if not datasource_id:
        return get_duckdb_engine()
    # Use short-lived cache to avoid hammering the metadata DB under bursts
    ds_info = _ds_cache_get(str(datasource_id))
    if ds_info is None:
        ds_obj = db.get(Datasource, datasource_id)
        if not ds_obj:
            raise HTTPException(status_code=404, detail="Datasource not found")
        ds_info = {
            "id": ds_obj.id,
            "user_id": ds_obj.user_id,
            "connection_encrypted": ds_obj.connection_encrypted,
            "type": ds_obj.type,
            "options_json": ds_obj.options_json,
        }
        _ds_cache_set(str(datasource_id), ds_info)
    # Enforce that only owner, admin, or shared users can access when actor is provided
    if actor_id:
        u = db.get(User, str(actor_id).strip())
        is_admin = bool(u and (u.role or "user").lower() == "admin")
        if not is_admin and (str(ds_info.get("user_id") or "").strip() != str(actor_id).strip()):
            share = db.query(DatasourceShare).filter(DatasourceShare.datasource_id == str(datasource_id), DatasourceShare.user_id == str(actor_id).strip()).first()
            if not share:
                raise HTTPException(status_code=403, detail="Not allowed to query this datasource")
    if not ds_info.get("connection_encrypted"):
        # Special-case: DuckDB datasource records can intentionally omit a connection URI
        # to indicate use of the local analytical store. Route to the local DuckDB engine.
        try:
            typ = str(ds_info.get("type") or "").lower()
        except Exception:
            typ = ""
        if "duckdb" in typ:
            return get_duckdb_engine()
        raise HTTPException(status_code=400, detail="Datasource has no connection URI")
    dsn = decrypt_text(ds_info.get("connection_encrypted") or "")
    if not dsn:
        raise HTTPException(status_code=400, detail="Invalid connection secret")
    return get_engine_from_dsn(dsn)


# --- Simple in-memory TTL cache (process-local) ---
_CACHE_TTL_SECONDS = 5
_query_cache: Dict[str, Tuple[float, Tuple[list[str], list[list[Any]]]]] = {}

_RESULT_CACHE_MAX_ROWS = 2000
try:
    _RESULT_CACHE_MAX_ROWS = int(os.environ.get("RESULT_CACHE_MAX_ROWS", "2000") or "2000")
except Exception:
    _RESULT_CACHE_MAX_ROWS = 2000

_RESULT_CACHE_MAX_ENTRIES = 200
try:
    _RESULT_CACHE_MAX_ENTRIES = int(os.environ.get("RESULT_CACHE_MAX_ENTRIES", "200") or "200")
except Exception:
    _RESULT_CACHE_MAX_ENTRIES = 200

# Optional Redis-backed result cache
_RC_TTL_SECONDS = 5
try:
    _RC_TTL_SECONDS = int(os.environ.get("RESULT_CACHE_TTL", str(_CACHE_TTL_SECONDS)) or str(_CACHE_TTL_SECONDS))
except Exception:
    _RC_TTL_SECONDS = _CACHE_TTL_SECONDS


def _cache_key(prefix: str, datasource_id: Optional[str], sql_inner: str, params: Dict[str, Any]) -> str:
    ds = datasource_id or "__local__"
    items = ",".join(f"{k}={repr(v)}" for k, v in sorted(params.items()))
    return f"{prefix}|{ds}|{sql_inner}|{items}"


def _cache_get(key: str) -> Optional[Tuple[list[str], list[list[Any]]]]:
    # Prefer Redis if available
    try:
        r = _get_redis()
    except Exception:
        r = None
    if r is not None:
        try:
            raw = r.get("q:" + key)
            if raw:
                try:
                    data = raw if isinstance(raw, (bytes, bytearray)) else raw  # type: ignore
                    import json as _json
                    cols, rows = _json.loads(data if isinstance(data, str) else data.decode("utf-8"))
                    return (list(cols or []), list(rows or []))
                except Exception:
                    pass
        except Exception:
            pass
    # Fallback to process-local cache
    rec = _query_cache.get(key)
    if not rec:
        return None
    ts, payload = rec
    if (time.time() - ts) > _CACHE_TTL_SECONDS:
        _query_cache.pop(key, None)
        return None
    return payload


def _cache_set(key: str, cols: list[str], rows: list[list[Any]]) -> None:
    try:
        if _RESULT_CACHE_MAX_ROWS > 0 and len(rows or []) > _RESULT_CACHE_MAX_ROWS:
            return
    except Exception:
        pass
    # Write-through to Redis if available
    try:
        r = _get_redis()
    except Exception:
        r = None
    if r is not None:
        try:
            import json as _json
            payload = _json.dumps([cols, rows])
            r.setex("q:" + key, max(1, int(_RC_TTL_SECONDS)), payload)
        except Exception:
            pass
    _query_cache[key] = (time.time(), (cols, rows))
    try:
        if _RESULT_CACHE_MAX_ENTRIES > 0 and len(_query_cache) > _RESULT_CACHE_MAX_ENTRIES:
            # Evict oldest
            oldest_key = min(_query_cache.items(), key=lambda kv: kv[1][0])[0]
            _query_cache.pop(oldest_key, None)
    except Exception:
        pass


# --- Datasource TTL cache (process-local) ---
_DS_TTL_SECONDS = 10
_ds_cache: Dict[str, Tuple[float, dict]] = {}

def _ds_cache_get(ds_id: str) -> Optional[dict]:
    rec = _ds_cache.get(str(ds_id))
    if not rec:
        return None
    ts, data = rec
    if (time.time() - ts) > _DS_TTL_SECONDS:
        _ds_cache.pop(str(ds_id), None)
        return None
    return data

def _ds_cache_set(ds_id: str, data: dict) -> None:
    _ds_cache[str(ds_id)] = (time.time(), data)


# --- Helpers ---
def _http_for_db_error(e: Exception) -> HTTPException | None:
    """Classify common DB connectivity errors to clearer HTTP status codes.
    - HYT00/Login timeout → 504 Gateway Timeout
    - 08S01/TCP Provider (SQL Server) → 502 Bad Gateway
    Otherwise: None (caller should re-raise original error).
    """
    try:
        msg = str(e) if e is not None else ""
        up = msg.upper()
        if ("HYT00" in up) or ("LOGIN TIMEOUT" in up):
            return HTTPException(status_code=504, detail="Database connectivity timeout (HYT00)")
        if ("08S01" in up) or ("TCP PROVIDER" in up):
            return HTTPException(status_code=502, detail="Database connection lost (08S01/TCP Provider)")
    except Exception:
        return None
    return None
def _coerce_date_like(v: Any) -> Any:
    """Attempt to parse arbitrary date/time strings into ISO strings.
    Safe no-op if parsing fails or value is not a string.
    Example outputs: '2024-01-15' or '2024-01-15 13:45:00'.
    """
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return v
        # Only parse if it looks like a date (contains date separators or keywords)
        # This prevents parsing short strings like '10', '20', '50' as dates
        if not re.search(r'[-/:T]|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|mon|tue|wed|thu|fri|sat|sun|today|now|yesterday|tomorrow', s, re.IGNORECASE):
            return v
        try:
            dt = date_parser.parse(s)
            # Keep UTC time, don't convert to local timezone
            if getattr(dt, 'tzinfo', None) is not None:
                try:
                    # Convert to UTC and make naive (removes timezone info but keeps UTC time)
                    import datetime
                    dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                except Exception:
                    dt = dt.replace(tzinfo=None)
            if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
                return dt.date().isoformat()
            return dt.replace(microsecond=0).isoformat(sep=' ')
        except Exception:
            return v
    return v


def _json_safe_cell(v: Any) -> Any:
    """Coerce DB values to JSON-serializable primitives.
    - bytes/bytearray/memoryview → hex string (0x...); try utf-8 first for readability
    - Decimal → float (fallback to str if NaN/Inf)
    - Default: return as-is
    """
    try:
        if isinstance(v, (bytes, bytearray, memoryview)):
            try:
                return bytes(v).decode('utf-8')
            except Exception:
                return '0x' + binascii.hexlify(bytes(v)).decode('ascii')
        if isinstance(v, decimal.Decimal):
            try:
                return float(v)
            except Exception:
                return str(v)
    except Exception:
        try:
            return str(v)
        except Exception:
            return None
    return v


def _resolve_duckdb_path_from_engine(engine: Engine) -> str:
    """Best-effort: extract DuckDB file path from an SQLAlchemy engine URL.
    Mirrors logic in datasources.introspect_schema for consistency.
    Fallbacks to settings.duckdb_path.
    """
    try:
        url = str(engine.url)
    except Exception:
        url = "duckdb:///" + settings.duckdb_path
    raw = (url or "").strip()
    raw_l = raw.lower()
    if raw_l.startswith('duckdb:////'):
        path = '/' + raw[len('duckdb:////'):]
    elif raw_l.startswith('duckdb:///'):
        path = raw[len('duckdb:///'):]
    elif raw_l.startswith('duckdb://'):
        path = raw[len('duckdb://'):]
    elif raw_l.startswith('duckdb:'):
        path = raw[len('duckdb:'):]
    else:
        path = settings.duckdb_path
    if '?' in path:
        path = path.split('?', 1)[0]
    path = unquote(path)
    if path in (':memory:', '/:memory:'):
        return ':memory:'
    while path.startswith('//') and path != '://':
        path = path[1:]
    if not path:
        path = settings.duckdb_path
    if path != ':memory:':
        # Normalize filesystem path
        try:
            import os
            path = os.path.abspath(os.path.expanduser(path))
        except Exception:
            pass
    return path


def _norm_name(s: str) -> str:
    """Strip quotes/brackets from identifier, take rightmost segment after dots, and lowercase."""
    return (s or '').strip().strip('[]').strip('"').strip('`').split('.')[-1].lower()


def _auto_correct_column_case(expr: str, schema_cols: set[str]) -> str:
    """Auto-correct column references in expression to match actual schema case.
    For case-insensitive databases like MySQL, ensures expressions use the correct case.
    
    Args:
        expr: SQL expression that may contain column references
        schema_cols: Set of actual column names from schema (with correct case)
    
    Returns:
        Expression with column references corrected to match schema case
    """
    if not expr or not schema_cols:
        return expr
    
    import re as _re
    # Build case-insensitive lookup: lowercase -> actual case
    lookup = {c.lower(): c for c in schema_cols}
    result = expr
    
    # Pattern 1: Bare identifiers (unquoted column names)
    # Match word boundaries, avoid matching inside strings or other quoted contexts
    def replace_bare(match):
        col = match.group(0)
        col_lower = col.lower()
        if col_lower in lookup and lookup[col_lower] != col:
            return lookup[col_lower]
        return col
    
    # Only replace unquoted identifiers that look like column names (start with letter/underscore)
    # Avoid replacing inside string literals (basic heuristic)
    result = _re.sub(r'\b([A-Za-z_][A-Za-z0-9_]*)\b', replace_bare, result)
    
    return result


def _referenced_cols_in_expr(expr: str) -> set[str]:
    """Very simple lexer to extract base column names from a SQL expression.
    Handles patterns like [s].[Col], s.Col, [Col], "s"."Col". Returns lowercased set.
    NOTE: Expression should already be normalized to target dialect before calling this.
    """
    cols: set[str] = set()
    try:
        import re as _re
        # Make a copy of the expression to remove qualified refs from
        remaining = str(expr or "")
        
        # Qualified patterns - extract column name and remove the whole pattern
        # [s].[Col] - MSSQL syntax (in case expression isn't normalized)
        for m in _re.finditer(r"\[s\]\.\[([^\]]+)\]", remaining):
            cols.add(_norm_name(m.group(1)))
        remaining = _re.sub(r"\[s\]\.\[([^\]]+)\]", " ", remaining)
        
        # s.Col (unquoted) - generic syntax
        for m in _re.finditer(r"\bs\.([A-Za-z_][A-Za-z0-9_]*)", remaining):
            cols.add(_norm_name(m.group(1)))
        remaining = _re.sub(r"\bs\.([A-Za-z_][A-Za-z0-9_]*)", " ", remaining)
        
        # "s"."Col" (double-quoted) - DuckDB/Postgres syntax (normalized)
        qualified_matches = list(_re.finditer(r'"s"\."([^"]+)"', remaining))
        for m in qualified_matches:
            cols.add(_norm_name(m.group(1)))
        remaining = _re.sub(r'"s"\."([^"]+)"', " ", remaining)
        
        # DEBUG: Check if we successfully removed qualified references
        if '"s"' in remaining:
            import sys
            sys.stderr.write(f"[DEBUG _referenced_cols_in_expr] WARNING: 's' still in remaining after qualified removal!\n")
            sys.stderr.write(f"[DEBUG _referenced_cols_in_expr] Remaining (first 200 chars): {remaining[:200]}\n")
            sys.stderr.write(f"[DEBUG _referenced_cols_in_expr] Qualified matches found: {len(qualified_matches)}\n")
            sys.stderr.flush()
        
        # Now search for bare identifiers in the remaining expression (after removing qualified refs)
        # Bare bracketed identifiers [Col]
        for m in _re.findall(r"\[([^\]]+)\]", remaining):
            if m.lower() != 's':
                cols.add(_norm_name(m))
        # Bare double-quoted identifiers "Col" (but not "s")
        bare_quoted = _re.findall(r'"([^"]+)"', remaining)
        import sys
        if 's' in [b.lower() for b in bare_quoted]:
            sys.stderr.write(f"[DEBUG _referenced_cols_in_expr] Found 's' in bare quoted identifiers: {bare_quoted}\n")
            sys.stderr.write(f"[DEBUG _referenced_cols_in_expr] Remaining (first 300 chars): {remaining[:300]}\n")
            sys.stderr.flush()
        for m in bare_quoted:
            if m.lower() != 's':
                cols.add(_norm_name(m))
            else:
                sys.stderr.write(f"[DEBUG _referenced_cols_in_expr] Skipping bare 's'\n")
                sys.stderr.flush()

        try:
            expr_no_strings = _re.sub(r"'([^']|'')*'", " ", remaining)
        except Exception:
            expr_no_strings = remaining

        try:
            # Collect from qualified unquoted identifiers like t.Col
            for m in _re.finditer(r"\b[a-z_][a-z0-9_]*\.([A-Za-z_][A-Za-z0-9_]*)\b", expr_no_strings, flags=_re.IGNORECASE):
                c = m.group(1)
                if c and c.lower() != 's':
                    cols.add(_norm_name(c))
        except Exception:
            pass

        sql_keywords = {
            'select','from','where','join','inner','left','right','full','outer','on',
            'and','or','not','case','when','then','else','end',
            'as','distinct','top','limit','offset','group','by','order','asc','desc',
            'null','is','in','like','between','exists','true','false',
            'sum','avg','min','max','count','coalesce','cast','try_cast',
            'regexp_replace','month','year','day','week','quarter','date_trunc','extract',
            'varchar','integer','int','bigint','smallint','tinyint','decimal','numeric',
            'float','double','real','boolean','bool','date','time','timestamp','datetime',
            'text','char','binary','varbinary','blob','clob',
        }
        try:
            for m in _re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", expr_no_strings):
                token = m.group(1)
                tl = token.lower()
                if tl == 's':
                    continue
                if tl in sql_keywords:
                    continue
                cols.add(_norm_name(token))
        except Exception:
            pass
    except Exception:
        return set()
    # Never treat table alias 's' as a referenced column
    if 's' in cols:
        try:
            import sys
            sys.stderr.write(f"[DEBUG _referenced_cols_in_expr] Discarding alias 's' from refs: {cols}\n")
            sys.stderr.flush()
        except Exception:
            pass
        cols.discard('s')
    return cols


def _filter_by_basecols(ds_tr: dict, base_cols: set[str]) -> dict:
    """Drop customColumns/transforms whose referenced columns are unavailable.
    Iteratively accept items so aliases produced by earlier accepted items can be
    referenced by later ones (supports dependencies between custom columns).
    """
    if not isinstance(ds_tr, dict):
        return {}
    base_l = {(_c or '').strip().strip('[]').strip('"').strip('`').lower() for _c in (base_cols or set())}
    ccs = list(ds_tr.get('customColumns') or [])
    trs = list(ds_tr.get('transforms') or [])
    joins = ds_tr.get('joins') or []
    dfl = ds_tr.get('defaults') or {}

    allowed: set[str] = set(base_l)
    # Seed with columns produced by joins (aliases or names), so downstream
    # custom columns / transforms can reference them
    try:
        for j in (joins or []):
            try:
                agg = (j or {}).get('aggregate') or None
                if isinstance(agg, dict):
                    al = str(agg.get('alias') or '').strip()
                    if al:
                        allowed.add(_norm_name(al))
                cols = (j or {}).get('columns') or []
                for c in cols:
                    try:
                        nm = str((c or {}).get('alias') or (c or {}).get('name') or '').strip()
                        if nm:
                            allowed.add(_norm_name(nm))
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception:
        pass
    accepted_cc: list[dict] = []
    accepted_tr: list[dict] = []
    taken_cc: list[bool] = [False] * len(ccs)
    taken_tr: list[bool] = [False] * len(trs)

    def _can_accept_cc(cc: dict) -> tuple[bool, str | None]:
        name = _norm_name(str((cc or {}).get('name') or ''))
        expr = str((cc or {}).get('expr') or '')
        # Auto-correct column case before extracting refs
        expr_corrected = _auto_correct_column_case(expr, base_cols)
        refs = _referenced_cols_in_expr(expr_corrected)
        return ((not refs) or refs.issubset(allowed), name)

    def _can_accept_tr(tr: dict) -> tuple[bool, str | None]:
        t = str((tr or {}).get('type') or '').lower()
        if t == 'computed':
            name = _norm_name(str((tr or {}).get('name') or ''))
            expr = str(tr.get('expr') or '')
            expr_corrected = _auto_correct_column_case(expr, base_cols)
            refs = _referenced_cols_in_expr(expr_corrected)
            can_accept = (not refs) or refs.issubset(allowed)
            print(f"[_filter_by_basecols] Checking computed '{name}': expr='{expr[:50]}', refs={refs}, allowed_sample={list(allowed)[:10]}, can_accept={can_accept}", flush=True)
            return (can_accept, name)
        if t == 'case':
            tgt_name = _norm_name(str((tr or {}).get('target') or ''))
            try:
                for c in (tr.get('cases') or []):
                    left = str((c.get('when') or {}).get('left') or '')
                    l = _norm_name(left)
                    if l and (l not in allowed):
                        return (False, None)
            except Exception:
                return (True, tgt_name)
            return (True, tgt_name)
        if t in {'replace', 'translate', 'nullhandling'}:
            tgt = _norm_name(str((tr or {}).get('target') or ''))
            return ((bool(tgt) and (tgt in allowed)), None)
        # Other transform types: keep by default
        return (True, None)

    progress = True
    passes = 0
    import sys
    print(f"[_filter_by_basecols] Starting with {len(ccs)} custom columns, {len(trs)} transforms", flush=True)
    print(f"[_filter_by_basecols] Custom column names: {[cc.get('name') for cc in ccs]}", flush=True)
    print(f"[_filter_by_basecols] Base columns available: {sorted(base_l)}", flush=True)
    # Up to 5 passes to resolve simple dependency chains
    while progress and passes < 5:
        progress = False
        passes += 1
        # Try accept custom columns
        for i, cc in enumerate(ccs):
            if taken_cc[i]:
                continue
            ok, name = _can_accept_cc(cc)
            if ok:
                accepted_cc.append(cc)
                taken_cc[i] = True
                if name:
                    allowed.add(name)
                    print(f"[_filter_by_basecols] Pass {passes}: Accepted custom column '{name}'", flush=True)
                progress = True
            else:
                print(f"[_filter_by_basecols] Pass {passes}: Rejected custom column '{cc.get('name')}' - missing refs", flush=True)
        # Try accept transforms
        for i, tr in enumerate(trs):
            if taken_tr[i]:
                continue
            ok, prod = _can_accept_tr(tr)
            if ok:
                accepted_tr.append(tr)
                taken_tr[i] = True
                if prod:
                    allowed.add(prod)
                    print(f"[_filter_by_basecols] Pass {passes}: Accepted transform '{prod}'", flush=True)
                progress = True

    # Preserve original order for accepted items
    out_cc = [cc for i, cc in enumerate(ccs) if taken_cc[i]]
    out_tr = [tr for i, tr in enumerate(trs) if taken_tr[i]]
    print(f"[_filter_by_basecols] Final: {len(out_cc)} custom columns, {len(out_tr)} transforms accepted", flush=True)
    dropped_cc = [cc.get('name') for i, cc in enumerate(ccs) if not taken_cc[i]]
    if dropped_cc:
        print(f"[_filter_by_basecols] Dropped custom columns: {dropped_cc}", flush=True)

    return {
        'customColumns': out_cc,
        'transforms': out_tr,
        'joins': joins,
        'defaults': dfl,
    }


@router.post("/pivot", response_model=QueryResponse)
def run_pivot(payload: PivotRequest, db: Session = Depends(get_db), actorId: Optional[str] = None, publicId: Optional[str] = None, token: Optional[str] = None) -> QueryResponse:
    """Server-side pivot aggregation.
    Returns long-form grouped rows: [row_dims..., col_dims..., value].
    """
    import sys
    sys.stderr.write(f"[PIVOT_START] datasourceId={payload.datasourceId}, widgetId={payload.widgetId}, source={payload.source}\n")
    sys.stderr.flush()
    # Resolve date presets at execution time
    if getattr(payload, 'where', None):
        payload.where = _resolve_date_presets(payload.where)
    # Determine datasource; optionally route to DuckDB when globally preferred and the source exists locally
    sys.stderr.write("[DEBUG] Getting engine for datasource...\n")
    sys.stderr.flush()
    engine = _engine_for_datasource(db, payload.datasourceId, actorId)
    sys.stderr.write(f"[DEBUG] Got engine: {engine.dialect.name if engine else 'None'}\n")
    sys.stderr.flush()
    try:
        src = getattr(payload, 'source', None)
        if settings.prefer_local_duckdb and _duck_has_table(src):
            engine = get_duckdb_engine()
    except Exception:
        pass
    if actorId:
        _ra = _throttle_take(actorId)
        if _ra:
            raise HTTPException(status_code=429, detail="Rate limit exceeded", headers={"Retry-After": str(_ra)})
    # Detect type; align builder dialect with likely execution route (DuckDB) to avoid mismatches
    sys.stderr.write("[DEBUG] Detecting datasource type...\n")
    sys.stderr.flush()
    try:
        ds_type = (engine.dialect.name or "").lower()
    except Exception:
        ds_type = ""
    sys.stderr.write(f"[DEBUG] ds_type={ds_type}\n")
    sys.stderr.flush()
    # Only apply query routing logic if the datasource is remote (not already DuckDB)
    try:
        _prefer_duck = bool(settings.prefer_local_duckdb) and ds_type != "duckdb"
    except Exception:
        _prefer_duck = False
    try:
        _src_for_duck = getattr(payload, 'source', None)
    except Exception:
        _src_for_duck = None
    try:
        # Skip routing if already querying a local DuckDB datasource
        if (payload.datasourceId is None) or (_prefer_duck and _duck_has_table(_src_for_duck)):
            ds_type = "duckdb"
    except Exception:
        pass

    def _q_ident(name: str) -> str:
        s = str(name or '').strip('\n\r\t')
        # Drop leading alias (s., u., _base., etc.) for outer queries
        try:
            if '.' in s and '(' not in s and ')' not in s:
                parts = s.split('.')
                # keep last segment as column name
                s = parts[-1]
        except Exception:
            pass
        if not s:
            return s
        if s.startswith('[') and s.endswith(']'):
            return s
        if s.startswith('"') and s.endswith('"'):
            return s
        if s.startswith('`') and s.endswith('`'):
            return s
        d = (ds_type or '').lower()
        if 'mssql' in d or 'sqlserver' in d:
            return f"[{s}]"
        if 'mysql' in d:
            return f"`{s}`"
        return f'"{s}"'

    def _q_source(name: str) -> str:
        s = str(name or '').strip()
        if not s:
            return s
        d = (ds_type or '').lower()
        if 'mssql' in d or 'sqlserver' in d:
            parts = s.split('.')
            return '.'.join([p if (p.startswith('[') and p.endswith(']')) else f"[{p}]" for p in parts])
        if 'mysql' in d:
            parts = s.split('.')
            return '.'.join([p if (p.startswith('`') and p.endswith('`')) else f"`{p}`" for p in parts])
        # Default (DuckDB/Postgres/SQLite): double-quote each part to allow spaces/special chars
        parts = s.split('.')
        return '.'.join([p if ((p.startswith('"') and p.endswith('"')) or (p.startswith('[') and p.endswith(']')) or (p.startswith('`') and p.endswith('`'))) else f'"{p}"' for p in parts])

    def _derived_lhs(name: str) -> str:
        raw = str(name or '').strip()
        m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", raw, flags=re.IGNORECASE)
        if not m:
            return _q_ident(raw)
        base = m.group(1).strip()
        part = m.group(2).strip().lower()
        col = _q_ident(base)
        d = (ds_type or '').lower()
        if 'mssql' in d or 'sqlserver' in d:
            if part == 'year': return f"YEAR({col})"
            if part == 'quarter': return f"DATEPART(quarter, {col})"
            if part == 'month': return f"MONTH({col})"
            if part == 'month name': return f"DATENAME(month, {col})"
            if part == 'month short': return f"LEFT(DATENAME(month, {col}), 3)"
            if part == 'week': return f"DATEPART(iso_week, {col})"
            if part == 'day': return f"DAY({col})"
            if part == 'day name': return f"DATENAME(weekday, {col})"
            if part == 'day short': return f"LEFT(DATENAME(weekday, {col}), 3)"
            return col
        if 'duckdb' in d or 'postgres' in d or 'postgre' in d:
            if part == 'year': return f"EXTRACT(year FROM {col})"
            if part == 'quarter': return f"EXTRACT(quarter FROM {col})"
            if part == 'month': return f"EXTRACT(month FROM {col})"
            if part == 'month name': return f"to_char({col}, 'FMMonth')"
            if part == 'month short': return f"to_char({col}, 'Mon')"
            if part == 'week': return f"EXTRACT(week FROM {col})"
            if part == 'day': return f"EXTRACT(day FROM {col})"
            if part == 'day name': return f"to_char({col}, 'FMDay')"
            if part == 'day short': return f"to_char({col}, 'Dy')"
            return col
        if 'mysql' in d:
            if part == 'year': return f"YEAR({col})"
            if part == 'quarter': return f"QUARTER({col})"
            if part == 'month': return f"MONTH({col})"
            if part == 'month name': return f"DATE_FORMAT({col}, '%M')"
            if part == 'month short': return f"DATE_FORMAT({col}, '%b')"
            if part == 'week': return f"WEEK({col}, 3)"
            if part == 'day': return f"DAY({col})"
            if part == 'day name': return f"DATE_FORMAT({col}, '%W')"
            if part == 'day short': return f"DATE_FORMAT({col}, '%a')"
            return col
        if 'sqlite' in d:
            if part == 'year': return f"CAST(strftime('%Y', {col}) AS INTEGER)"
            if part == 'quarter':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 "
                    f"WHEN 4 THEN 2 WHEN 5 THEN 2 WHEN 6 THEN 2 "
                    f"WHEN 7 THEN 3 WHEN 8 THEN 3 WHEN 9 THEN 3 "
                    f"ELSE 4 END"
                )
            if part == 'month': return f"CAST(strftime('%m', {col}) AS INTEGER)"
            if part == 'month name':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April' "
                    f"WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' "
                    f"WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December' END"
                )
            if part == 'month short':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr' "
                    f"WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' "
                    f"WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec' END"
                )
            if part == 'week': return f"CAST(strftime('%W', {col}) AS INTEGER)"
            if part == 'day': return f"CAST(strftime('%d', {col}) AS INTEGER)"
            if part == 'day name':
                return (
                    f"CASE strftime('%w', {col}) "
                    f"WHEN '0' THEN 'Sunday' WHEN '1' THEN 'Monday' WHEN '2' THEN 'Tuesday' WHEN '3' THEN 'Wednesday' "
                    f"WHEN '4' THEN 'Thursday' WHEN '5' THEN 'Friday' WHEN '6' THEN 'Saturday' END"
                )
            if part == 'day short':
                return (
                    f"CASE strftime('%w', {col}) "
                    f"WHEN '0' THEN 'Sun' WHEN '1' THEN 'Mon' WHEN '2' THEN 'Tue' WHEN '3' THEN 'Wed' "
                    f"WHEN '4' THEN 'Thu' WHEN '5' THEN 'Fri' WHEN '6' THEN 'Sat' END"
                )
            return col
        return col

    # Build FROM with datasource-level transforms if any (reuse logic from spec handler)
    sys.stderr.write("[DEBUG] Starting datasource info loading section...\n")
    sys.stderr.flush()
    ds_info = None
    # FALLBACK: If datasourceId not provided, try to infer from widgetId or source
    datasource_id_to_use = payload.datasourceId
    if not datasource_id_to_use and payload.widgetId:
        # Try to get datasourceId from widget config by querying database directly
        try:
            import json as _json
            from ..models import Widget
            widget = db.query(Widget).filter(Widget.id == payload.widgetId).first()
            if widget and widget.config_json:
                config = _json.loads(widget.config_json)
                query_spec = config.get('querySpec') or {}
                source_table_id = query_spec.get('sourceTableId') or ''
                # sourceTableId format: "{datasourceId}__{tableName}"
                if '__' in source_table_id:
                    datasource_id_to_use = source_table_id.split('__')[0]
                    import sys
                    sys.stderr.write(f"[Pivot] Extracted datasourceId from widget sourceTableId: {datasource_id_to_use}\n")
                    sys.stderr.flush()
        except Exception as e:
            import sys
            sys.stderr.write(f"[Pivot] Failed to extract datasourceId from widget: {e}\n")
            sys.stderr.flush()
    
    # FALLBACK 2: If still no datasourceId, try to find DuckDB datasource that owns this table
    if not datasource_id_to_use and payload.source and ds_type == "duckdb":
        try:
            import sys
            sys.stderr.write(f"[Pivot] Attempting to find DuckDB datasource for table: {payload.source}\n")
            sys.stderr.flush()
            # Extract table name from source (handle schema.table format)
            table_name = payload.source.split('.')[-1].strip('"').strip('`').strip('[').strip(']')
            sys.stderr.write(f"[Pivot] Normalized table name: {table_name}\n")
            sys.stderr.flush()
            # Query all DuckDB datasources
            all_ds = db.query(Datasource).filter(Datasource.type.like('duckdb%')).all()
            sys.stderr.write(f"[Pivot] Found {len(all_ds)} DuckDB datasources to search\n")
            sys.stderr.flush()
            for ds_candidate in all_ds:
                try:
                    opts = json.loads(ds_candidate.options_json or "{}")
                    # Check if this datasource has transforms for this table
                    transforms = opts.get("transforms") or {}
                    custom_cols = transforms.get("customColumns") or []
                    # Check scope of custom columns for table match
                    for col in custom_cols:
                        if isinstance(col, dict):
                            scope = col.get("scope") or {}
                            scope_table = str(scope.get("table") or "").strip()
                            if scope_table:
                                # Normalize scope table name
                                scope_table_norm = scope_table.split('.')[-1].strip('"').strip('`').strip('[').strip(']').lower()
                                if scope_table_norm == table_name.lower():
                                    datasource_id_to_use = ds_candidate.id
                                    sys.stderr.write(f"[Pivot] Found matching datasource: {datasource_id_to_use} (matched table: {scope_table})\n")
                                    sys.stderr.flush()
                                    break
                    if datasource_id_to_use:
                        break
                except Exception as ex:
                    sys.stderr.write(f"[Pivot] Error checking datasource {ds_candidate.id}: {ex}\n")
                    sys.stderr.flush()
                    continue
            if not datasource_id_to_use:
                # FALLBACK 3: If only one DuckDB datasource exists, use it
                if len(all_ds) == 1:
                    datasource_id_to_use = all_ds[0].id
                    sys.stderr.write(f"[Pivot] Using only DuckDB datasource: {datasource_id_to_use}\n")
                    sys.stderr.flush()
                else:
                    sys.stderr.write(f"[Pivot] No matching datasource found for table {table_name} (checked {len(all_ds)} datasources)\n")
                    sys.stderr.flush()
        except Exception as e:
            import sys
            sys.stderr.write(f"[Pivot] Failed to find datasource by table: {e}\n")
            sys.stderr.flush()
    
    import sys
    sys.stderr.write(f"[DEBUG] About to check datasource_id_to_use: {datasource_id_to_use}, type={type(datasource_id_to_use)}, bool={bool(datasource_id_to_use)}\n")
    sys.stderr.flush()
    if datasource_id_to_use:
        sys.stderr.write(f"[DEBUG] Condition is TRUE, entering if block\n")
        sys.stderr.flush()
        import sys
        sys.stderr.write(f"[Pivot] Loading datasource: {datasource_id_to_use}\n")
        sys.stderr.flush()
        ds_info = _ds_cache_get(str(datasource_id_to_use))
        if ds_info is None:
            try:
                ds_obj = db.get(Datasource, datasource_id_to_use)
            except Exception:
                ds_obj = None
            if ds_obj:
                ds_info = {
                    "id": ds_obj.id,
                    "user_id": ds_obj.user_id,
                    "connection_encrypted": ds_obj.connection_encrypted,
                    "type": ds_obj.type,
                    "options_json": ds_obj.options_json,
                }
                _ds_cache_set(str(datasource_id_to_use), ds_info)
        sys.stderr.write(f"[Pivot] Datasource type: {ds_info.get('type') if ds_info else 'None'}\n")
        sys.stderr.flush()

    try:
        if ds_type == "duckdb":
            curr_type = str((ds_info or {}).get("type") or "").lower() if isinstance(ds_info, dict) else ""
            if (ds_info is None) or ("duckdb" not in curr_type):
                best = None
                best_score = -1
                needed = {str(x).strip().lower() for x in (list(payload.rows or []) + list(payload.cols or [])) if str(x).strip()}
                if getattr(payload, 'valueField', None):
                    needed.add(str(getattr(payload, 'valueField') or '').strip().lower())
                cands = db.query(Datasource).filter(Datasource.type.like('duckdb%')).all()
                for ds_candidate in (cands or []):
                    try:
                        opts = json.loads(ds_candidate.options_json or "{}")
                    except Exception:
                        opts = {}
                    tr = (opts or {}).get("transforms") or {}
                    if not isinstance(tr, dict):
                        continue
                    def _matches_table(scope_table: str, source_name: str) -> bool:
                        def norm(s: str) -> str:
                            s = (s or '').strip().strip('[]').strip('"').strip('`')
                            parts = s.split('.')
                            return parts[-1].lower()
                        return norm(scope_table) == norm(source_name)
                    def _scoped(arr):
                        out = []
                        for it in (arr or []):
                            sc = (it or {}).get('scope')
                            if not sc:
                                out.append(it)
                                continue
                            lvl = str(sc.get('level') or '').lower()
                            if lvl == 'datasource':
                                out.append(it)
                            elif lvl == 'table' and sc.get('table') and _matches_table(str(sc.get('table') or ''), payload.source):
                                out.append(it)
                            elif lvl == 'widget':
                                wid = str((sc or {}).get('widgetId') or '').strip()
                                if wid and payload.widgetId and str(payload.widgetId).strip() == wid:
                                    out.append(it)
                        return out
                    scoped = {
                        'customColumns': _scoped(tr.get('customColumns')),
                        'transforms': _scoped(tr.get('transforms')),
                        'joins': _scoped(tr.get('joins')),
                        'defaults': tr.get('defaults') or {},
                    }
                    aliases = set()
                    for j in (scoped.get('joins') or []):
                        for col in ((j or {}).get('columns') or []):
                            al = str((col or {}).get('alias') or (col or {}).get('name') or '').strip()
                            if al:
                                aliases.add(al.lower())
                    for cc in (scoped.get('customColumns') or []):
                        nm = str((cc or {}).get('name') or '').strip()
                        if nm:
                            aliases.add(nm.lower())
                    for t in (scoped.get('transforms') or []):
                        nm = str((t or {}).get('name') or (t or {}).get('target') or '').strip()
                        if nm:
                            aliases.add(nm.lower())
                    score = 0
                    if needed and (needed & aliases):
                        score += 10 * len(needed & aliases)
                    if len(scoped.get('joins') or []) > 0:
                        score += 2
                    if (len(scoped.get('customColumns') or []) + len(scoped.get('transforms') or [])) > 0:
                        score += 1
                    if score > best_score:
                        best_score = score
                        best = (ds_candidate, scoped)
                if best and best_score > 0:
                    ds_candidate, scoped = best
                    ds_info = {
                        "id": ds_candidate.id,
                        "user_id": ds_candidate.user_id,
                        "connection_encrypted": ds_candidate.connection_encrypted,
                        "type": ds_candidate.type,
                        "options_json": ds_candidate.options_json,
                    }
                    sys.stderr.write(f"[Pivot] Using DuckDB transforms from datasource: {ds_candidate.id}\n")
                    sys.stderr.flush()
    except Exception:
        pass
    ds_transforms_all = {}
    ds_transforms = {}
    if ds_info is not None:
        try:
            opts = json.loads((ds_info.get("options_json") or "{}"))
        except Exception:
            opts = {}
        # Apply scope filtering: only transforms/customColumns/joins matching this table or datasource-level
        def _matches_table(scope_table: str, source_name: str) -> bool:
            def norm(s: str) -> str:
                s = (s or '').strip().strip('[]').strip('"').strip('`')
                parts = s.split('.')
                return parts[-1].lower()
            return norm(scope_table) == norm(source_name)
        def _apply_scope(ds_tr: dict, source_name: str) -> dict:
            if not isinstance(ds_tr, dict):
                return {}
            def filt(arr):
                out = []
                for it in (arr or []):
                    sc = (it or {}).get('scope')
                    col_name = (it or {}).get('name', '<unnamed>')
                    if not sc:
                        out.append(it)
                        sys.stderr.write(f"[Pivot] Custom column '{col_name}' has no scope, including it\n")
                        sys.stderr.flush()
                        continue
                    lvl = str(sc.get('level') or '').lower()
                    if lvl == 'datasource':
                        out.append(it)
                        sys.stderr.write(f"[Pivot] Custom column '{col_name}' is datasource-level, including it\n")
                        sys.stderr.flush()
                    elif lvl == 'table' and sc.get('table'):
                        scope_table = str(sc.get('table'))
                        matches = _matches_table(scope_table, payload.source)
                        sys.stderr.write(f"[Pivot] Custom column '{col_name}' scope table '{scope_table}' vs source '{payload.source}': {'MATCH' if matches else 'NO MATCH'}\n")
                        sys.stderr.flush()
                        if matches:
                            out.append(it)
                    elif lvl == 'widget':
                        try:
                            wid = str((sc or {}).get('widgetId') or '').strip()
                            if wid and payload.widgetId and str(payload.widgetId).strip() == wid:
                                out.append(it)
                        except Exception:
                            pass
                return out
            return {
                'customColumns': filt(ds_tr.get('customColumns')),
                'transforms': filt(ds_tr.get('transforms')),
                'joins': filt(ds_tr.get('joins')),
                'defaults': ds_tr.get('defaults') or {},
            }
        sys.stderr.write(f"[Pivot] Applying scope filter for source: {payload.source}\n")
        sys.stderr.flush()
        _tr_dbg = (opts or {}).get("transforms") if isinstance(opts, dict) else None
        if isinstance(_tr_dbg, dict):
            all_custom_cols = _tr_dbg.get('customColumns', [])
        elif isinstance(opts, dict):
            all_custom_cols = (opts or {}).get('customColumns', [])
        else:
            all_custom_cols = []
        sys.stderr.write(f"[Pivot] Total custom columns before scope filter: {len(all_custom_cols)}\n")
        for col in all_custom_cols:
            scope = col.get('scope') or {}
            sys.stderr.write(f"[Pivot]   - {col.get('name')}: level={scope.get('level')}, table={scope.get('table')}\n")
        sys.stderr.flush()
        _raw_tr = (opts or {}).get("transforms")
        if (not isinstance(_raw_tr, dict)) and isinstance(opts, dict):
            if any(k in opts for k in ("customColumns", "transforms", "joins", "defaults")):
                _raw_tr = opts
        ds_transforms_all = _raw_tr or {}
        ds_transforms = _apply_scope(_raw_tr or {}, payload.source)
        if ds_transforms:
            custom_cols_count = len(ds_transforms.get('customColumns', []))
            sys.stderr.write(f"[Pivot] Loaded {custom_cols_count} custom columns from datasource transforms after scope filter\n")
            sys.stderr.flush()
    base_from_sql = f" FROM {_q_source(payload.source)}"
    if ds_transforms:
        # Probe columns and filter joins as in aggregated path
        def _list_cols_for_agg_base() -> set[str]:
            try:
                eng = _engine_for_datasource(db, datasource_id_to_use, actorId)
                with eng.connect() as conn:
                    if (ds_type or '').lower() in ("mssql", "mssql+pymssql", "mssql+pyodbc"):
                        probe = text(f"SELECT TOP 0 * FROM {_q_source(payload.source)} AS s")
                    else:
                        probe = text(f"SELECT * FROM {_q_source(payload.source)} WHERE 1=0")
                    # Apply short statement timeout for probe
                    try:
                        dname = (eng.dialect.name or '').lower()
                        if 'postgres' in dname:
                            conn.execute(text('SET statement_timeout = 15000'))
                        elif ('mysql' in dname) or ('mariadb' in dname):
                            conn.execute(text('SET SESSION MAX_EXECUTION_TIME=15000'))
                        elif ('mssql' in dname) or ('sqlserver' in dname):
                            conn.execute(text('SET LOCK_TIMEOUT 15000'))
                    except Exception:
                        pass
                    res = conn.execute(probe)
                    return set([str(c) for c in res.keys()])
            except Exception:
                return set()
        __joins_all = ds_transforms.get('joins', []) if isinstance(ds_transforms, dict) else []
        # For DuckDB, skip join filtering since we do proper column probing later
        # For other DBs, filter joins based on available columns
        if ds_type and ds_type.lower().startswith('duckdb'):
            print(f"[Pivot] DuckDB detected: keeping all {len(__joins_all)} joins (will probe with joins applied)")
            __joins_eff = list(__joins_all or [])
        else:
            __cols = _list_cols_for_agg_base()
            __cols_lower = {c.lower() for c in __cols}  # Case-insensitive comparison
            print(f"[Pivot] Non-DuckDB: filtering {len(__joins_all)} joins based on {len(__cols)} available columns")
            __joins_eff = []
            for __j in (__joins_all or []):
                try:
                    __skey = str((__j or {}).get('sourceKey') or '').strip()
                    __skey_lower = __skey.lower()
                    # Case-insensitive check
                    if __skey and (__skey_lower in __cols_lower or __skey in __cols):
                        __joins_eff.append(__j)
                except Exception:
                    continue
        # If Unpivot exists but has empty sourceColumns, infer them from alias-producing transforms
        __transforms_all = ds_transforms.get('transforms', []) if isinstance(ds_transforms, dict) else []
        __transforms_eff: list[dict] = []
        try:
            alias_candidates: list[str] = []
            alias_seen: set[str] = set()
            def _add_alias(n: str | None):
                s = str(n or '').strip()
                if not s:
                    return
                sl = s.lower()
                if sl in alias_seen:
                    return
                alias_candidates.append(s)
                alias_seen.add(sl)
            # from customColumns
            for cc in (ds_transforms.get('customColumns') or []):
                try:
                    _add_alias((cc or {}).get('name'))
                except Exception:
                    pass
            # from transforms with target/name
            for __t in (__transforms_all or []):
                try:
                    _tt = str((__t or {}).get('type') or '').lower()
                    if _tt == 'computed':
                        _add_alias((__t or {}).get('name'))
                    elif _tt in ('case','replace','translate','nullhandling'):
                        _add_alias((__t or {}).get('target'))
                except Exception:
                    pass
            # build effective transforms with filled unpivot
            for __t in (__transforms_all or []):
                try:
                    if str((__t or {}).get('type') or '').lower() == 'unpivot':
                        _scols = [str(c).strip() for c in ((__t or {}).get('sourceColumns') or []) if str(c).strip()]
                        if len(_scols) == 0:
                            _kc = str((__t or {}).get('keyColumn') or 'metric').strip() or 'metric'
                            _vc = str((__t or {}).get('valueColumn') or 'value').strip() or 'value'
                            _fill = [c for c in alias_candidates if c.lower() not in {_kc.lower(), _vc.lower()}]
                            _t2 = dict(__t); _t2['sourceColumns'] = _fill
                            __transforms_eff.append(_t2)
                            continue
                except Exception:
                    pass
                __transforms_eff.append(__t)
        except Exception:
            __transforms_eff = list(__transforms_all or [])

        # Filter custom columns to exclude those referencing non-existent base columns
        __custom_cols_all = ds_transforms.get("customColumns", [])
        __custom_cols_eff = []
        __transforms_eff_filtered = []
        
        try:
            # Probe available columns using direct SQL execution
            # Build the transformed source with joins ONLY (no custom columns or transforms)
            print(f"[Pivot] Probing columns from {payload.source}...")
            print(f"[Pivot] Number of joins to apply: {len(__joins_eff)}")
            if __joins_eff:
                print(f"[Pivot] First join: targetTable={__joins_eff[0].get('targetTable')}, sourceKey={__joins_eff[0].get('sourceKey')}, targetKey={__joins_eff[0].get('targetKey')}")
            try:
                # Build SQL with joins but NO custom columns or transforms
                probe_result = build_sql(
                    dialect=ds_type,
                    source=_q_source(payload.source),
                    base_select=["*"],
                    custom_columns=[],  # Don't include custom columns
                    transforms=[],  # Don't include transforms - they may reference missing columns!
                    joins=__joins_eff,
                    defaults={},
                    limit=None,
                )
                if len(probe_result) >= 3:
                    probe_base_sql = probe_result[0]
                else:
                    probe_base_sql = f"SELECT * FROM {_q_source(payload.source)}"
                print(f"[Pivot] Probe SQL preview: {probe_base_sql[:200]}...")
            except Exception as probe_ex:
                print(f"[Pivot] Probe build_sql failed: {probe_ex}, using base table")
                probe_base_sql = f"SELECT * FROM {_q_source(payload.source)}"
            
            probe_sql = f"SELECT * FROM ({probe_base_sql}) AS _probe LIMIT 0"
            with open_duck_native(None) as conn:
                probe_cursor = conn.execute(probe_sql)
                available_cols_lower = {str(col[0]).strip().lower() for col in probe_cursor.description}
                print(f"[Pivot] Probed {len(available_cols_lower)} columns (including joins): {sorted(list(available_cols_lower)[:20])}")
            
            # Helper to extract column references (re is imported at module level)
            def extract_refs(expr_str: str) -> set[str]:
                refs: set[str] = set()
                # Strip single-quoted string literals so we don't treat them as identifiers
                try:
                    expr_no_strings = re.sub(r"'([^']|'')*'", " ", expr_str)
                except Exception:
                    expr_no_strings = expr_str
                # Match qualified identifiers (quoted): "s"."Col" or [s].[Col] - keep only column part
                for match in re.finditer(r'["\[`][^"\]`]+["\]`]\.["\[`]([^"\]`]+)["\]`]', expr_no_strings):
                    refs.add(match.group(1).lower())
                # Match qualified identifiers (unquoted): s.Col - keep only column part
                for match in re.finditer(r'\b[a-z_][a-z0-9_]*\.([A-Za-z_][A-Za-z0-9_]*)\b', expr_no_strings, re.IGNORECASE):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                # Match quoted identifiers: "col", [col], `col`
                for match in re.finditer(r'["\[`]([^"\]`]+)["\]`]', expr_no_strings):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                # Also match parenthesized bare identifiers: (col)
                for match in re.finditer(r'\(([A-Za-z_][A-Za-z0-9_]*)\)', expr_no_strings):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                # NEW: match bare identifiers that look like column names
                # Exclude common SQL keywords and functions to avoid false positives.
                sql_keywords = {
                    'select','from','where','join','inner','left','right','full','outer','on',
                    'and','or','not','case','when','then','else','end',
                    'as','distinct','top','limit','offset','group','by','order','asc','desc',
                    'null','is','in','like','between','exists','true','false',
                    'sum','avg','min','max','count','coalesce','cast','try_cast',
                    'regexp_replace','month','year','day','week','quarter'
                }
                for match in re.finditer(r'\b([A-Za-z_][A-Za-z0-9_]*)\b', expr_no_strings):
                    token = match.group(1)
                    tl = token.lower()
                    if tl == 's':
                        continue
                    if tl in sql_keywords:
                        continue
                    refs.add(tl)
                return refs
            
            # Filter computed transforms and custom columns
            # IMPORTANT: Process computed transforms FIRST so their output names are available
            # when custom columns are checked (custom columns can reference transform outputs)
            available_with_aliases = available_cols_lower.copy()  # Start with base columns
            # Track dependencies between custom columns/transforms (by alias name)
            custom_deps: dict[str, set[str]] = {}
            transform_deps: dict[str, set[str]] = {}
            
            # FIRST: Filter and process computed transforms
            print(f"[Pivot] Filtering {len(__transforms_eff)} transforms (phase 1)...")
            for t in __transforms_eff:
                if not isinstance(t, dict):
                    __transforms_eff_filtered.append(t)
                    continue
                    
                if t.get("type") == "computed":
                    name = t.get("name")
                    expr = t.get("expr")
                    if name and expr:
                        # Normalize SQL dialect before parsing column references
                        expr_normalized = normalize_sql_expression(str(expr), ds_type or 'duckdb')
                        refs = extract_refs(expr_normalized)
                        missing = refs - available_cols_lower
                        if missing:
                            print(f"[Pivot] SKIP computed transform '{name}': references missing columns {missing}")
                            continue
                        # Track dependencies for reachability analysis
                        transform_deps[name.lower()] = set(refs)
                        # Add this transform's output name to available columns for custom column checks
                        available_with_aliases.add(name.lower())
                        print(f"[Pivot] OK including computed transform '{name}' (adds alias to available set)")
                __transforms_eff_filtered.append(t)
            print(f"[Pivot] After transforms: available_with_aliases has {len(available_with_aliases)} entries (base + transform outputs)")
            
            # SECOND: Filter custom columns - now they can reference transform outputs
            # Separate custom columns into two groups:
            # 1. "Leaf" columns: only reference base table columns
            # 2. "Derived" columns: reference other custom column/transform aliases
            print(f"[Pivot] Filtering {len(__custom_cols_all)} custom columns (phase 2)...")
            custom_cols_leaf = []  # Only reference base columns
            custom_cols_derived = []  # Reference other custom columns
            
            for cc in __custom_cols_all:
                if not isinstance(cc, dict):
                    print(f"[Pivot] Skipping non-dict custom column: {type(cc)}")
                    continue
                name = cc.get("name")
                expr = cc.get("expr")
                if not name or not expr:
                    print(f"[Pivot] Skipping custom column with missing name/expr: name={name}, expr={bool(expr)}")
                    continue
                    
                expr_str = str(expr)
                # Normalize SQL dialect before parsing column references
                expr_normalized = normalize_sql_expression(expr_str, ds_type or 'duckdb')
                refs = extract_refs(expr_normalized)
                missing = refs - available_with_aliases
                if missing:
                    # Use ASCII-only markers for Windows console compatibility
                    print(f"[Pivot] SKIP custom column '{name}': expr='{expr_str[:50]}', refs={refs}, missing={missing}")
                    continue
                
                # Track dependencies for reachability analysis
                custom_deps[name.lower()] = set(refs)
                # Check if this column references any custom column aliases (not just base columns)
                refs_custom_aliases = refs - available_cols_lower
                if refs_custom_aliases:
                    # This column references other custom columns/transforms - exclude from _base subquery
                    print(f"[Pivot] OK including custom column '{name}' (derived, will be computed in outer query)")
                    custom_cols_derived.append(cc)
                else:
                    # This column only references base columns - include in _base subquery
                    print(f"[Pivot] OK including custom column '{name}' (leaf)")
                    custom_cols_leaf.append(cc)
                
                # Add this custom column's alias to available columns for subsequent checks
                available_with_aliases.add(name.lower())
            
            # Only leaf columns go into __custom_cols_eff (for _base subquery)
            # NOTE: Also include derived columns (which reference other custom aliases)
            # so measures like 'Total' are materialized and can be aggregated in DuckDB.
            __custom_cols_eff = custom_cols_leaf + custom_cols_derived

            # Reachability pruning: only keep custom columns/transforms that are reachable
            # from current pivot rows/cols/value and filters, plus their dependencies.
            # Build root set from r_dims, c_dims, value field, and where filters.
            used_alias_roots: set[str] = set()
            for d in (r_dims + c_dims):
                if not d:
                    continue
                s = str(d).strip()
                if not s:
                    continue
                used_alias_roots.add(s.lower())
                # Also add base name for derived date parts like "OrderDate (Month)"
                try:
                    m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", s, flags=re.IGNORECASE)
                    if m:
                        used_alias_roots.add(m.group(1).strip().lower())
                except Exception:
                    pass
            if val_field:
                try:
                    used_alias_roots.add(str(val_field).strip().lower())
                except Exception:
                    pass
            if payload.where:
                try:
                    for k in payload.where.keys():
                        if not isinstance(k, str):
                            continue
                        base = k.split("__", 1)[0]
                        used_alias_roots.add(base.strip().lower())
                except Exception:
                    pass

            all_alias_names = set(custom_deps.keys()) | set(transform_deps.keys())
            needed_aliases: set[str] = set(n for n in all_alias_names if n in used_alias_roots)
            # Follow dependencies transitively among aliases
            queue: list[str] = list(needed_aliases)
            while queue:
                cur = queue.pop()
                deps = custom_deps.get(cur, set()) | transform_deps.get(cur, set())
                for dep in deps:
                    dl = dep.lower()
                    if dl in all_alias_names and dl not in needed_aliases:
                        needed_aliases.add(dl)
                        queue.append(dl)

            if needed_aliases:
                before_cc = len(__custom_cols_eff)
                before_tr = len(__transforms_eff_filtered)
                __custom_cols_eff = [
                    cc for cc in __custom_cols_eff
                    if isinstance(cc, dict) and cc.get("name") and cc.get("name").strip().lower() in needed_aliases
                ]
                __transforms_eff = []
                for t in __transforms_eff_filtered:
                    if not isinstance(t, dict) or t.get("type") != "computed":
                        __transforms_eff.append(t)
                        continue
                    name = (t.get("name") or "").strip().lower()
                    if name and (name in needed_aliases):
                        __transforms_eff.append(t)
                print(f"[Pivot] Reachability filter: kept {len(__custom_cols_eff)}/{before_cc} custom columns, {len(__transforms_eff)}/{before_tr} transforms")
            else:
                # No alias-based dependencies needed for this pivot; safe to drop computed
                # custom columns and transforms entirely from _base.
                print(f"[Pivot] Reachability filter: no custom aliases referenced; dropping all customColumns/transforms from _base")
                __custom_cols_eff = []
                __transforms_eff = [t for t in __transforms_eff_filtered if not (isinstance(t, dict) and t.get("type") == "computed")]

            print(f"[Pivot] Final: {len(__custom_cols_eff)} custom columns, {len(__transforms_eff)} transforms")
        except Exception as e:
            import traceback
            print(f"[Pivot] ERROR filtering custom columns: {e}")
            print(f"[Pivot] Traceback: {traceback.format_exc()}")
            print(f"[Pivot] Fallback: using all {len(__custom_cols_all)} custom columns")
            __custom_cols_eff = list(__custom_cols_all)
            # Keep the already-filtered __transforms_eff as-is (don't overwrite)

        print(f"[Pivot] About to call build_sql with {len(__joins_eff)} joins")
        if __joins_eff:
            for idx, j in enumerate(__joins_eff):
                print(f"[Pivot]   Join {idx}: joinType={j.get('joinType')}, targetTable={j.get('targetTable')}, sourceKey={j.get('sourceKey')}")
        result = build_sql(
            dialect=ds_type,
            source=_q_source(payload.source),
            base_select=["*"],
            custom_columns=__custom_cols_eff,
            transforms=__transforms_eff,
            joins=__joins_eff,
            defaults={},
            limit=None,
        )
        # Handle different return value formats (3 or 4 elements)
        if len(result) == 3:
            base_sql, _cols_unused, _warns = result
        elif len(result) == 4:
            base_sql, _cols_unused, _warns, _ = result
        else:
            print(f"[Pivot] Unexpected build_sql return count: {len(result)}")
            base_sql = result[0] if result else ""
        base_from_sql = f" FROM ({base_sql}) AS _base"

    # WHERE
    where_clauses = []
    params: Dict[str, Any] = {}
    def _pname(base: str, suffix: str = "") -> str:
        core = re.sub(r"[^A-Za-z0-9_]", "_", str(base or ''))
        return f"w_{core}{suffix}"
    if payload.where:
        for k, v in payload.where.items():
            if k in ("start", "startDate", "end", "endDate"):
                continue
            if v is None:
                where_clauses.append(f"{_q_ident(k)} IS NULL")
            elif isinstance(k, str) and "__" in k:
                # Check for operators FIRST before checking if value is array
                base, op = k.split("__", 1)
                opname = None
                if op == "gte": opname = ">="
                elif op == "gt": opname = ">"
                elif op == "lte": opname = "<="
                elif op == "lt": opname = "<"
                if opname:
                    pname = _pname(base, f"_{op}")
                    # Extract first element if value is an array (operators expect scalar values)
                    param_val = v[0] if isinstance(v, (list, tuple)) and len(v) > 0 else v
                    params[pname] = param_val
                    where_clauses.append(f"{_derived_lhs(base)} {opname} :{pname}")
                elif op == "ne":
                    # NOT EQUALS: use NOT IN to support multiple exclusions
                    if isinstance(v, (list, tuple)) and len(v) > 0:
                        pnames = []
                        for i, item in enumerate(v):
                            pname = _pname(base, f"_ne_{i}")
                            params[pname] = item
                            pnames.append(f":{pname}")
                        where_clauses.append(f"{_derived_lhs(base)} NOT IN ({', '.join(pnames)})")
                    else:
                        pname = _pname(base, "_ne")
                        params[pname] = v
                        where_clauses.append(f"{_derived_lhs(base)} != :{pname}")
                elif op == "notcontains":
                    # DOESN'T CONTAIN: use NOT LIKE
                    pname = _pname(base, "_notcontains")
                    params[pname] = f"%{v}%"
                    where_clauses.append(f"{_derived_lhs(base)} NOT LIKE :{pname}")
                elif op in {"contains", "startswith", "endswith"}:
                    # String matching operators
                    pname = _pname(base, f"_{op}")
                    if op == "contains":
                        params[pname] = f"%{v}%"
                    elif op == "startswith":
                        params[pname] = f"{v}%"
                    else:  # endswith
                        params[pname] = f"%{v}"
                    where_clauses.append(f"{_derived_lhs(base)} LIKE :{pname}")
                else:
                    pname = _pname(k)
                    where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                    params[pname] = v
            elif isinstance(v, (list, tuple)):
                if len(v) == 0:
                    continue
                pnames = []
                for i, item in enumerate(v):
                    pname = _pname(k, f"_{i}")
                    params[pname] = item
                    pnames.append(f":{pname}")
                where_clauses.append(f"{_derived_lhs(k)} IN ({', '.join(pnames)})")
            else:
                pname = _pname(k)
                where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                params[pname] = v
    # Store WHERE clauses and filter keys before building where_sql
    # We'll split them later based on dimensions
    where_filter_map: Dict[str, str] = {}  # key -> SQL clause
    for clause in where_clauses:
        # Extract the column name from the clause (before operator/IN/etc)
        # This is a simple heuristic - we stored them in order they were added
        pass
    
    # Build preliminary where_sql (will be split later based on dimensions)
    all_where_clauses = list(where_clauses)  # Keep a copy

    # Dimensions (filter out reserved synthetic fields if they slipped into config)
    _reserved = {"__metric__", "value"}
    def _clean_dims(arr):
        out = []
        for n in (arr or []):
            try:
                if (str(n or "").strip().lower() in _reserved):
                    continue
            except Exception:
                pass
            out.append(n)
        return out
    r_dims = _clean_dims(payload.rows)
    c_dims = _clean_dims(payload.cols)
    
    # Split WHERE clauses based on dimensions vs non-dimensions
    # When using FROM (...) AS _base, non-dimension filters must go inside _base
    dimension_names = set()
    for d in (r_dims + c_dims):
        # Handle derived columns like "OrderDate (Year)" -> extract "OrderDate"
        base_name = re.sub(r'\s*\(.*\)$', '', str(d)).strip()
        dimension_names.add(base_name.lower())
        dimension_names.add(d.lower())  # Also add the full name
    
    # Rebuild WHERE clauses: separate dimension vs non-dimension filters
    dim_where_clauses = []
    non_dim_where_clauses = []
    
    if payload.where:
        for k, v in payload.where.items():
            if k in ("start", "startDate", "end", "endDate"):
                continue
            # Extract base column name (remove operators like __ne, __gte)
            base_col = k.split("__")[0] if "__" in k else k
            is_dimension = base_col.lower() in dimension_names
            
            # Find the corresponding WHERE clause we built earlier
            # Match by looking for the column name in the clause
            matching_clause = None
            for clause in all_where_clauses:
                if _q_ident(base_col) in clause or _derived_lhs(base_col) in clause:
                    matching_clause = clause
                    all_where_clauses.remove(clause)  # Remove so we don't match it again
                    break
            
            if matching_clause:
                if is_dimension:
                    dim_where_clauses.append(matching_clause)
                else:
                    non_dim_where_clauses.append(matching_clause)
    
    # Build final where_sql for outer query (dimension filters only)
    where_sql = f" WHERE {' AND '.join(dim_where_clauses)}" if dim_where_clauses else ""
    
    # If we have non-dimension filters and using _base subquery, inject them
    if non_dim_where_clauses and "_base" in base_from_sql:
        non_dim_where_sql = f" WHERE {' AND '.join(non_dim_where_clauses)}"
        # Inject before ) AS _base
        base_from_sql = base_from_sql.replace(") AS _base", f"{non_dim_where_sql}) AS _base")
    
    # Apply groupBy time bucketing to first row dimension if specified
    gb = (getattr(payload, 'groupBy', None) or 'none').lower()
    week_start = (getattr(payload, 'weekStart', None) or 'mon').lower()
    
    # Use original names as aliases (quoted per dialect) so UI can match config fields directly
    r_exprs = []
    for i, n in enumerate(r_dims):
        # Apply groupBy bucketing to first dimension if it's a date field
        if i == 0 and gb in ("day","week","month","quarter","year"):
            # Check if it's a raw date column (not derived)
            is_derived = bool(re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(n), flags=re.IGNORECASE))
            if not is_derived:
                # Apply DATE_TRUNC bucketing
                col_name = n
                if "duckdb" in ds_type or "postgres" in ds_type:
                    if "duckdb" in ds_type:
                        col_ts = f"COALESCE(try_cast({_q_ident(col_name)} AS TIMESTAMP), CAST(try_cast({_q_ident(col_name)} AS DATE) AS TIMESTAMP))"
                    else:
                        col_ts = _q_ident(col_name)
                    if gb == 'week':
                        if week_start == 'sun':
                            expr = f"DATE_TRUNC('week', {col_ts} + INTERVAL '1 day') - INTERVAL '1 day'" if "duckdb" in ds_type else f"date_trunc('week', {col_ts} + interval '1 day') - interval '1 day'"
                        else:
                            expr = f"DATE_TRUNC('week', {col_ts})" if "duckdb" in ds_type else f"date_trunc('week', {col_ts})"
                    else:
                        expr = f"DATE_TRUNC('{gb}', {col_ts})" if "duckdb" in ds_type else f"date_trunc('{gb}', {col_ts})"
                    r_exprs.append((expr, _q_ident(n)))
                    continue
        # Default: use derived_lhs for any derived patterns
        r_exprs.append((_derived_lhs(n), _q_ident(n)))
    
    c_exprs = [(_derived_lhs(n), _q_ident(n)) for i, n in enumerate(c_dims)]

    # Aggregator
    agg = (payload.aggregator or "count").lower()
    val_field = (payload.valueField or "").strip()
    # If Unpivot is present in datasource transforms, FORCE non-count aggregations to target the value column
    unpivot_val_col: str | None = None
    try:
        for _tr in (ds_transforms.get('transforms') or []):  # type: ignore[attr-defined]
            if str((_tr or {}).get('type') or '').lower() == 'unpivot':
                unpivot_val_col = str(((_tr or {}).get('valueColumn') or 'value')).strip() or 'value'
                break
    except Exception:
        unpivot_val_col = None
    if unpivot_val_col and agg in ("sum", "avg", "min", "max", "distinct"):
        val_field = unpivot_val_col
    # If still missing a field for non-count aggregations, degrade to COUNT
    if agg == 'distinct' and not val_field:
        agg = 'count'
    if agg in ("sum", "avg", "min", "max") and not val_field:
        agg = 'count'
    # Sanitize invalid valueField for non-count aggregations (e.g., numeric-only like '2')
    # BUT allow numeric names if they're valid custom column aliases
    try:
        is_numeric_name = bool(re.fullmatch(r"\d+", str(val_field or '').strip()))
        # Check if it's a valid custom column even if numeric (expr_map built later, so assume valid for now)
        is_valid_custom_col = is_numeric_name  # Will be validated later when expr_map is available
    except Exception:
        is_numeric_name = False
        is_valid_custom_col = False
    if agg in ("sum", "avg", "min", "max", "distinct"):
        if (not val_field) or (is_numeric_name and not is_valid_custom_col):
            if unpivot_val_col:
                val_field = unpivot_val_col
            else:
                agg = 'count'

    if agg == 'count':
        value_expr = "COUNT(*)"
    elif agg == 'distinct':
        value_expr = f"COUNT(DISTINCT {_q_ident(val_field)})"
    else:
        # For DuckDB, clean numeric strings robustly before SUM/AVG/MIN/MAX
        if (('duckdb' in (ds_type or '')) and (agg in ("sum", "avg", "min", "max"))):
            y_clean = (
                f"COALESCE("
                f"try_cast(regexp_replace(CAST({_q_ident(val_field)} AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), "
                f"try_cast({_q_ident(val_field)} AS DOUBLE), 0.0)"
            )
            value_expr = f"{agg.upper()}({y_clean})"
        else:
            value_expr = f"{agg.upper()}({_q_ident(val_field)})"

    # Build SELECT parts, avoiding self-aliasing (e.g., "VaultName" AS "VaultName")
    # which causes DuckDB GROUP BY errors when using positional references
    sel_parts = []
    for e, a in (r_exprs + c_exprs):
        # Normalize both expression and alias to check for equality (strip quotes/brackets)
        e_norm = str(e).strip().strip('"').strip('[').strip(']').strip('`')
        a_norm = str(a).strip().strip('"').strip('[').strip(']').strip('`')
        if e_norm.lower() == a_norm.lower():
            # Plain column: no alias needed
            sel_parts.append(e)
        else:
            # Derived/complex expression: add alias
            sel_parts.append(f"{e} AS {a}")
    # Check if SQLGlot should be used
    import sys
    sys.stderr.write(f"[PIVOT] About to check should_use_sqlglot with actorId={actorId}\n")
    sys.stderr.flush()
    print(f"[PIVOT] About to check should_use_sqlglot with actorId={actorId}", flush=True)
    sys.stdout.flush()
    use_sqlglot = should_use_sqlglot(actorId)
    sys.stderr.write(f"[PIVOT] should_use_sqlglot returned: {use_sqlglot}\n")
    sys.stderr.flush()
    print(f"[PIVOT] should_use_sqlglot returned: {use_sqlglot}", flush=True)
    sys.stdout.flush()
    # SQLGlot now properly handles DuckDB custom columns by materializing them in _base subquery (lines 1700-1747)
    inner = None
    
    if use_sqlglot:
        # NEW PATH: SQLGlot pivot query generation
        try:
            sys.stderr.write(f"[SQLGlot] Pivot: ENABLED for user={actorId}, dialect={ds_type}\n")
            sys.stderr.flush()
            print(f"[SQLGlot] Pivot: ENABLED for user={actorId}, dialect={ds_type}", flush=True)
            
            # Initialize transforms early (needed by filtering logic below)
            __transforms_eff = ds_transforms.get('transforms', []) if isinstance(ds_transforms, dict) else []
            
            # Probe available columns from base table, then filter joins, then probe with filtered joins
            # Only probe for DuckDB - for remote datasources, skip probing
            available_cols = set()
            base_cols: set[str] = set()
            probe_joins_filtered = None
            if ds_type == 'duckdb':
                try:
                    # PHASE 1: Probe base table WITHOUT joins to get base columns
                    print(f"[SQLGlot] Pivot: Phase 1 - Probing base table without joins", flush=True)
                    probe_result_base = build_sql(
                        dialect=ds_type,
                        source=_q_source(payload.source),
                        base_select=["*"],
                        custom_columns=[],
                        transforms=[],
                        joins=[],  # NO joins in phase 1
                        defaults={},
                        limit=None,
                    )
                    if len(probe_result_base) == 3:
                        probe_base_sql, _, _ = probe_result_base
                    elif len(probe_result_base) == 4:
                        probe_base_sql, _, _, _ = probe_result_base
                    else:
                        probe_base_sql = probe_result_base[0] if probe_result_base else ""
                    
                    probe_sql_phase1 = f"SELECT * FROM ({probe_base_sql}) AS _probe LIMIT 0"
                    with open_duck_native(None) as conn:
                        probe_cursor = conn.execute(probe_sql_phase1)
                        base_cols = {str(col[0]).strip() for col in probe_cursor.description}
                        print(f"[SQLGlot] Pivot: Phase 1 - Found {len(base_cols)} base columns")
                    
                    # PHASE 2: Filter joins - keep only those whose sourceKey exists in base_cols
                    # LATERAL joins use ON true and don't require sourceKey validation
                    all_joins = ds_transforms.get("joins", []) if ds_transforms else []
                    probe_joins_filtered = []
                    base_cols_lower = {c.lower() for c in base_cols}
                    for join in all_joins:
                        join_type = str((join or {}).get('joinType') or '').strip().lower()
                        source_key = str((join or {}).get('sourceKey') or '').strip()
                        
                        # LATERAL joins always use ON true, so skip sourceKey validation
                        if join_type == 'lateral':
                            probe_joins_filtered.append(join)
                            print(f"[SQLGlot] Pivot: Phase 2 - Including LATERAL join (targetTable='{join.get('targetTable')}')")
                        elif source_key and source_key.lower() in base_cols_lower:
                            probe_joins_filtered.append(join)
                        else:
                            print(f"[SQLGlot] Pivot: Phase 2 - Skipping join (sourceKey '{source_key}' not in base table)")
                    print(f"[SQLGlot] Pivot: Phase 2 - Kept {len(probe_joins_filtered)}/{len(all_joins)} joins")
                    
                    # PHASE 3: Probe WITH filtered joins to get final column list
                    if probe_joins_filtered:
                        print(f"[SQLGlot] Pivot: Phase 3 - Probing with {len(probe_joins_filtered)} filtered joins")
                        probe_result_final = build_sql(
                            dialect=ds_type,
                            source=_q_source(payload.source),
                            base_select=["*"],
                            custom_columns=[],
                            transforms=[],
                            joins=probe_joins_filtered,
                            defaults={},
                            limit=None,
                        )
                        if len(probe_result_final) == 3:
                            probe_final_sql, _, _ = probe_result_final
                        elif len(probe_result_final) == 4:
                            probe_final_sql, _, _, _ = probe_result_final
                        else:
                            probe_final_sql = probe_result_final[0] if probe_result_final else ""
                        
                        probe_sql_phase3 = f"SELECT * FROM ({probe_final_sql}) AS _probe LIMIT 0"
                        with open_duck_native(None) as conn:
                            probe_cursor = conn.execute(probe_sql_phase3)
                            available_cols = {str(col[0]).strip() for col in probe_cursor.description}
                            print(f"[SQLGlot] Pivot: Phase 3 - Found {len(available_cols)} total columns (base + joins)")
                    else:
                        # No valid joins, use base columns only
                        available_cols = base_cols
                        print(f"[SQLGlot] Pivot: Phase 3 - No valid joins, using {len(available_cols)} base columns only")
                        
                except Exception as e:
                    print(f"[SQLGlot] Pivot: Failed to probe columns, skipping validation: {e}")
                    import traceback
                    print(f"[SQLGlot] Pivot: Probe traceback: {traceback.format_exc()}")
            else:
                print(f"[SQLGlot] Pivot: Skipping column probe for remote datasource ({ds_type})")
            
            # Build expr_map for ALL custom columns (including derived ones for resolution)
            # Don't filter by available_cols here - filtering happens later for __custom_cols_sqlglot
            sys.stderr.write(f"[SQLGlot] Pivot: ds_info is None: {ds_info is None}, ds_transforms custom cols: {len(ds_transforms.get('customColumns', [])) if ds_transforms else 0}\n")
            sys.stderr.flush()
            print(f"[SQLGlot] Pivot: ds_info is None: {ds_info is None}, ds_transforms custom cols: {len(ds_transforms.get('customColumns', [])) if ds_transforms else 0}", flush=True)
            
            # Build expr_map from ds_transforms (already scope-filtered) when ds_info is None
            if ds_info:
                sys.stderr.write(f"[SQLGlot] Pivot: Building expr_map from ds_info...\n")
                sys.stderr.flush()
                expr_map = _build_expr_map_helper(ds_info, payload.source, ds_type, _apply_scope, None)
                sys.stderr.write(f"[SQLGlot] Pivot: expr_map built from ds_info, has {len(expr_map)} entries: {list(expr_map.keys())}\n")
                sys.stderr.flush()
            else:
                # Fallback: build expr_map directly from ds_transforms (already loaded and scope-filtered above)
                sys.stderr.write(f"[SQLGlot] Pivot: Building expr_map from ds_transforms (fallback)...\n")
                sys.stderr.flush()
                from ..sqlgen import _normalize_expr_idents
                expr_map = {}
                if ds_transforms:
                    # From customColumns
                    for col in (ds_transforms.get("customColumns") or []):
                        if isinstance(col, dict) and col.get("name") and col.get("expr"):
                            nm = str(col["name"]).strip()
                            if not nm:
                                continue
                            expr = _normalize_expr_idents(ds_type, col["expr"])
                            expr_map[nm] = expr
                            sys.stderr.write(f"[SQLGlot] Pivot: Added custom column '{nm}' to expr_map (len={len(expr)})\n")
                            sys.stderr.flush()
                    # From computed transforms
                    for t in (ds_transforms.get("transforms") or []):
                        if isinstance(t, dict) and t.get("type") == "computed":
                            if t.get("name") and t.get("expr"):
                                nm = str(t["name"]).strip()
                                if not nm:
                                    continue
                                expr = _normalize_expr_idents(ds_type, t["expr"])
                                expr_map[nm] = expr
                                sys.stderr.write(f"[SQLGlot] Pivot: Added computed transform '{nm}' to expr_map (len={len(expr)})\n")
                                sys.stderr.flush()
                sys.stderr.write(f"[SQLGlot] Pivot: expr_map built from ds_transforms, has {len(expr_map)} entries: {list(expr_map.keys())}\n")
                sys.stderr.flush()
            
            print(f"[SQLGlot] Pivot: expr_map has {len(expr_map)} entries: {list(expr_map.keys())}", flush=True)

            try:
                needed_aliases: set[str] = set()
                for f in (payload.rows or []):
                    s = str(f or '').strip()
                    if s:
                        needed_aliases.add(s)
                for f in (payload.cols or []):
                    s = str(f or '').strip()
                    if s:
                        needed_aliases.add(s)
                try:
                    vf = str((payload.valueField or '')).strip()
                    if vf:
                        needed_aliases.add(vf)
                except Exception:
                    pass
                try:
                    for k in ((payload.where or {}) or {}).keys():
                        ks = str(k or '').strip()
                        if not ks:
                            continue
                        base = ks.split('__', 1)[0].strip()
                        if base:
                            needed_aliases.add(base)
                except Exception:
                    pass

                if needed_aliases and isinstance(ds_transforms_all, dict):
                    all_cc = ds_transforms_all.get('customColumns') or []
                    all_tr = ds_transforms_all.get('transforms') or []
                    by_name: dict[str, dict] = {}
                    for it in (all_cc or []):
                        try:
                            nm = str((it or {}).get('name') or '').strip()
                            if nm:
                                by_name[nm.lower()] = it
                        except Exception:
                            continue
                    for it in (all_tr or []):
                        try:
                            if str((it or {}).get('type') or '').lower() != 'computed':
                                continue
                            nm = str((it or {}).get('name') or '').strip()
                            if nm:
                                by_name[nm.lower()] = it
                        except Exception:
                            continue

                    existing_lower = {str(k).strip().lower() for k in (expr_map or {}).keys()}
                    available_cols_lower2 = {c.lower() for c in (available_cols or set())} if available_cols else set()

                    sql_keywords2 = {
                        'select','from','where','join','inner','left','right','full','outer','on',
                        'and','or','not','case','when','then','else','end',
                        'as','distinct','top','limit','offset','group','by','order','asc','desc',
                        'null','is','in','like','between','exists','true','false',
                        'sum','avg','min','max','count','coalesce','cast','try_cast','extract',
                        'regexp_replace','month','year','day','week','quarter','strftime'
                    }

                    def _extract_refs_for_fallback(expr_str: str) -> set[str]:
                        refs: set[str] = set()
                        try:
                            expr_no_strings = re.sub(r"'([^']|'')*'", " ", str(expr_str or ''))
                        except Exception:
                            expr_no_strings = str(expr_str or '')
                        for m in re.finditer(r'["\[`][^"\]`]+["\]`]\.["\[`]([^"\]`]+)["\]`]', expr_no_strings):
                            refs.add(str(m.group(1) or '').strip().lower())
                        for m in re.finditer(r'\b[a-z_][a-z0-9_]*\.([A-Za-z_][A-Za-z0-9_]*)\b', expr_no_strings, re.IGNORECASE):
                            refs.add(str(m.group(1) or '').strip().lower())
                        for m in re.finditer(r'["\[`]([^"\]`]+)["\]`]', expr_no_strings):
                            refs.add(str(m.group(1) or '').strip().lower())
                        cleaned: set[str] = set()
                        for r in refs:
                            if not r:
                                continue
                            if r == 's':
                                continue
                            if r in sql_keywords2:
                                continue
                            cleaned.add(r)
                        return cleaned

                    known_lower = set(available_cols_lower2) | set(existing_lower)
                    for need in (needed_aliases or set()):
                        nl = str(need or '').strip().lower()
                        if not nl or nl in existing_lower:
                            continue
                        it = by_name.get(nl)
                        if not it:
                            continue
                        expr_raw = (it or {}).get('expr')
                        if not expr_raw:
                            continue
                        try:
                            from ..sqlgen import _normalize_expr_idents
                            expr_norm = _normalize_expr_idents(ds_type, str(expr_raw))
                        except Exception:
                            expr_norm = str(expr_raw)
                        refs = _extract_refs_for_fallback(expr_norm)
                        missing = refs - known_lower
                        if missing:
                            continue
                        expr_map[str(need).strip()] = expr_norm
                        try:
                            sys.stderr.write(f"[SQLGlot] Pivot: Injected referenced derived field into expr_map: {str(need).strip()}\n")
                            sys.stderr.flush()
                        except Exception:
                            pass
                        existing_lower.add(nl)
                        known_lower.add(nl)
            except Exception:
                pass
            
            # Helper to extract column references (re is imported at module level)
            def extract_refs_sg(expr_str: str) -> set[str]:
                refs: set[str] = set()
                # Strip single-quoted string literals so we don't treat them as identifiers
                try:
                    expr_no_strings = re.sub(r"'([^']|'')*'", " ", expr_str)
                except Exception:
                    expr_no_strings = expr_str
                # Match qualified identifiers (quoted): "s"."Col" or [s].[Col] - keep only column part
                for match in re.finditer(r'["\[`][^"\]`]+["\]`]\. ["\[`]([^"\]`]+)["\]`]', expr_no_strings):
                    refs.add(match.group(1).lower())
                # Match qualified identifiers (unquoted): s.Col - keep only column part
                for match in re.finditer(r'\b[a-z_][a-z0-9_]*\.([A-Za-z_][A-Za-z0-9_]*)\b', expr_no_strings, re.IGNORECASE):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                # Match quoted identifiers: "col", [col], `col`
                for match in re.finditer(r'["\[`]([^"\]`]+)["\]`]', expr_no_strings):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                # Also match parenthesized bare identifiers: (col)
                for match in re.finditer(r'\(([A-Za-z_][A-Za-z0-9_]*)\)', expr_no_strings):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                # Match bare identifiers that look like column names, excluding SQL keywords/functions
                sql_keywords = {
                    'select','from','where','join','inner','left','right','full','outer','on',
                    'and','or','not','case','when','then','else','end',
                    'as','distinct','top','limit','offset','group','by','order','asc','desc',
                    'null','is','in','like','between','exists','true','false',
                    'sum','avg','min','max','count','coalesce','cast','try_cast',
                    'regexp_replace','month','year','day','week','quarter'
                }
                for match in re.finditer(r'\b([A-Za-z_][A-Za-z0-9_]*)\b', expr_no_strings):
                    token = match.group(1)
                    tl = token.lower()
                    if tl == 's':
                        continue
                    if tl in sql_keywords:
                        continue
                    refs.add(tl)
                return refs

            def extract_transform_refs_sg(tr: Any) -> set[str]:
                """Extract column-like references from a transform definition for SQLGlot pruning.

                Handles computed, case, replace, translate, and nullhandling transforms.
                """
                refs: set[str] = set()
                if not isinstance(tr, dict):
                    return refs
                t_type = str(tr.get("type") or "").lower()
                try:
                    if t_type == "computed":
                        expr = str(tr.get("expr") or "")
                        if expr:
                            refs |= extract_refs_sg(expr)
                    elif t_type == "case":
                        parts: list[str] = []
                        for it in (tr.get("cases") or []):
                            w = dict((it or {}).get("when") or {})
                            lhs = str(w.get("left") or "").strip()
                            if lhs:
                                parts.append(lhs)
                        if parts:
                            refs |= extract_refs_sg(" ".join(parts))
                    elif t_type in {"replace", "translate", "nullhandling"}:
                        target = str(tr.get("target") or "").strip()
                        if target:
                            refs |= extract_refs_sg(target)
                except Exception:
                    # Best-effort only; on failure, return empty set to avoid over-pruning.
                    return set()
                return refs
            
            # Filter custom columns to match available columns
            __custom_cols_sqlglot = []
            # Always validate custom columns if we have probed columns - even for DuckDB
            # This prevents including columns that reference non-existent base columns
            if available_cols:
                available_cols_lower = {c.lower() for c in available_cols}
                # Track available columns including aliases as we add them
                available_with_aliases_sg = available_cols_lower.copy()

                # FIRST: Process transforms and validate that their referenced base columns
                # exist in the probed schema. This applies to all alias-producing transforms
                # (computed, case, replace, translate, nullhandling). Computed transforms
                # also contribute their alias names to available_with_aliases_sg so that
                # custom columns may legally reference them.
                __transforms_eff_sqlglot = []
                for t in __transforms_eff:
                    if not isinstance(t, dict):
                        __transforms_eff_sqlglot.append(t)
                        continue

                    t_type = str(t.get("type") or "").lower()
                    alias_name: str | None = None
                    if t_type == "computed":
                        alias_name = str(t.get("name") or "").strip()
                    elif t_type in {"case", "replace", "translate", "nullhandling"}:
                        alias_name = str(t.get("target") or "").strip()

                    if t_type in {"computed", "case", "replace", "translate", "nullhandling"}:
                        refs = extract_transform_refs_sg(t)
                        # FIX: Check against available_with_aliases_sg (which includes previously processed transforms)
                        # instead of available_cols_lower (which is base columns only).
                        missing = refs - available_with_aliases_sg
                        if missing:
                            label = alias_name or t_type
                            print(f"[SQLGlot] Warning: transform '{label}' references missing columns {missing} - including anyway (trusting inlining)")
                            # We used to skip here, but now we trust sqlgen.py to inline dependencies (e.g. refs to custom columns)
                            # continue

                        # Computed AND other transforms (case, replace, etc.) expose new aliases
                        # that subsequent transforms/custom columns may reference.
                        if alias_name:
                            available_with_aliases_sg.add(alias_name.strip().lower())
                            print(f"[SQLGlot] OK transform '{alias_name}' (adds alias to available set)")

                    __transforms_eff_sqlglot.append(t)

                print(f"[SQLGlot] After transforms: available_with_aliases_sg has {len(available_with_aliases_sg)} entries (base + transform outputs)")
                
                # SECOND: Process custom columns - now they can reference transform outputs
                custom_cols_leaf_sg = []  # This list will now include ALL custom columns (leaf AND derived)
                
                for cc in (ds_transforms.get("customColumns", []) if ds_transforms else []):
                    if isinstance(cc, dict) and cc.get("name") and cc.get("expr"):
                        cc_name = str(cc.get("name") or "").strip()
                        if not cc_name:
                            continue
                        expr_str = str(cc.get("expr") or "")
                        refs = extract_refs_sg(expr_str)
                        missing = refs - available_with_aliases_sg
                        
                        # Check if missing refs are actually other custom columns that we will handle via inlining
                        # If sqlgen.py handles inlining, we don't need to exclude these.
                        # But we should still warn if it references something totally unknown.
                        
                        # We'll trust build_sql to handle inlining of dependencies.
                        # So we include the column even if it references other custom columns.
                        
                        cc_norm = dict(cc)
                        cc_norm["name"] = cc_name
                        print(f"[SQLGlot] Including custom column '{cc_name}' (allowing dependencies for inlining)")
                        custom_cols_leaf_sg.append(cc_norm)
                        
                        # Add this custom column's alias to available columns
                        available_with_aliases_sg.add(cc_name.lower())
                
                # Only leaf columns go into __custom_cols_sqlglot (for _base subquery)
                # But keep ALL custom columns in expr_map (including derived) for resolution
                __custom_cols_sqlglot = custom_cols_leaf_sg
                leaf_names = [cc.get('name') for cc in custom_cols_leaf_sg if isinstance(cc, dict)]
                sys.stderr.write(f"[SQLGlot] Included {len(leaf_names)} columns in _base: {leaf_names}\n")
                sys.stderr.flush()
                print(f"[SQLGlot] Filtered __custom_cols_sqlglot to {len(custom_cols_leaf_sg)} columns")
            else:
                # Probe failed - apply strict filtering to avoid BinderException
                # IMPORTANT: When probing fails, we MUST filter custom columns that reference
                # other custom columns, since they can't be resolved in the _base subquery
                sys.stderr.write(f"[SQLGlot] Probe failed - applying strict custom column filtering\n")
                sys.stderr.flush()
                print(f"[SQLGlot] Probe failed - applying strict custom column filtering", flush=True)
                
                # First, build a set of ALL custom column and transform alias names (lowercase)
                all_custom_col_names_sg: set[str] = set()
                all_transform_names_sg: set[str] = set()
                for cc in (ds_transforms.get("customColumns", []) if ds_transforms else []):
                    if isinstance(cc, dict) and cc.get("name"):
                        all_custom_col_names_sg.add(cc['name'].lower())
                for t in __transforms_eff:
                    if not isinstance(t, dict):
                        continue
                    t_type = str(t.get("type") or "").lower()
                    if t_type == "computed" and t.get("name"):
                        all_transform_names_sg.add(str(t['name']).lower())
                    elif t_type in {"case", "replace", "translate", "nullhandling"} and t.get("target"):
                        all_transform_names_sg.add(str(t['target']).lower())
                all_alias_names_sg = all_custom_col_names_sg | all_transform_names_sg
                print(f"[SQLGlot] All custom column names: {all_custom_col_names_sg}", flush=True)
                print(f"[SQLGlot] All transform names: {all_transform_names_sg}", flush=True)
                
                # Removed strict filtering of non-leaf columns/transforms.
                # sqlgen.py's build_sql now handles dependency inlining correctly, so we can
                # allow custom columns that reference other custom columns/transforms.
                # This ensures columns like 'Brinks' (referencing 'SourceRegion') are included.
                
                __custom_cols_sqlglot = []
                for cc in (ds_transforms.get("customColumns", []) if ds_transforms else []):
                    if isinstance(cc, dict) and cc.get("name") and cc.get("expr"):
                        __custom_cols_sqlglot.append(cc)
                
                print(f"[SQLGlot] Included {len(__custom_cols_sqlglot)} custom columns (allowing dependencies)", flush=True)
                
                __transforms_eff_sqlglot = []
                for t in __transforms_eff:
                    if not isinstance(t, dict):
                        __transforms_eff_sqlglot.append(t)
                        continue
                    # Ensure it has minimal valid structure
                    t_type = str(t.get("type") or "").lower()
                    if t_type == "computed" and not t.get("name"):
                        continue
                    if t_type in {"case", "replace", "translate", "nullhandling"} and not t.get("target"):
                        continue
                    __transforms_eff_sqlglot.append(t)

                print(f"[SQLGlot] Final (before reachability): {len(__custom_cols_sqlglot)} custom columns, {len(__transforms_eff_sqlglot)} transforms")
            
            # Reachability pruning for SQLGlot path: only keep custom columns/transforms
            # that are reachable from current pivot rows/cols/value and filters.
            # Build dependency graph
            custom_deps_sg: dict[str, set[str]] = {}
            transform_deps_sg: dict[str, set[str]] = {}
            for cc in __custom_cols_sqlglot:
                if isinstance(cc, dict) and cc.get("name") and cc.get("expr"):
                    refs = extract_refs_sg(str(cc.get("expr") or ""))
                    custom_deps_sg[str(cc["name"]).strip().lower()] = refs
            for t in __transforms_eff_sqlglot:
                if isinstance(t, dict) and t.get("type") == "computed" and t.get("name") and t.get("expr"):
                    refs = extract_refs_sg(str(t.get("expr") or ""))
                    transform_deps_sg[str(t["name"]).strip().lower()] = refs
            
            # Build root set from r_dims, c_dims, val_field, and where filters
            used_alias_roots_sg: set[str] = set()
            for d in (r_dims + c_dims):
                if not d:
                    continue
                s = str(d).strip()
                if not s:
                    continue
                used_alias_roots_sg.add(s.lower())
                # Also add base name for derived date parts like "OrderDate (Month)"
                try:
                    m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", s, flags=re.IGNORECASE)
                    if m:
                        used_alias_roots_sg.add(m.group(1).strip().lower())
                except Exception:
                    pass
            if val_field:
                try:
                    used_alias_roots_sg.add(str(val_field).strip().lower())
                except Exception:
                    pass
            if payload.where:
                try:
                    for k in payload.where.keys():
                        if not isinstance(k, str):
                            continue
                        base = k.split("__", 1)[0]
                        used_alias_roots_sg.add(base.strip().lower())
                except Exception:
                    pass
            
            sys.stderr.write(f"[SQLGlot] Reachability: root aliases from pivot config: {used_alias_roots_sg}\n")
            sys.stderr.flush()
            print(f"[SQLGlot] Reachability: root aliases from pivot config: {used_alias_roots_sg}")
            
            # Re-add transforms/custom columns that are referenced in WHERE filters but were filtered out
            # This must happen BEFORE reachability analysis so they're included in all_alias_names_sg
            if payload.where:
                where_referenced_cols = set()
                for k in payload.where.keys():
                    if isinstance(k, str):
                        base = k.split("__", 1)[0]
                        where_referenced_cols.add(base.strip().lower())
                
                # Check what's already included
                existing_cc_names = {cc.get("name").strip().lower() for cc in __custom_cols_sqlglot if isinstance(cc, dict) and cc.get("name")}
                existing_transform_names = set()
                for t in __transforms_eff_sqlglot:
                    if not isinstance(t, dict):
                        continue
                    t_type = str(t.get("type") or "").lower()
                    if t_type == "computed" and t.get("name"):
                        existing_transform_names.add(str(t['name']).lower())
                    elif t_type in {"case", "replace", "translate", "nullhandling"} and t.get("target"):
                        existing_transform_names.add(str(t['target']).lower())
                
                # Re-add missing transforms from __transforms_eff (original list before filtering)
                for t in __transforms_eff:
                    if not isinstance(t, dict):
                        continue
                    t_type = str(t.get("type") or "").lower()
                    t_name = None
                    if t_type == "computed" and t.get("name"):
                        t_name = str(t['name']).lower()
                    elif t_type in {"case", "replace", "translate", "nullhandling"} and t.get("target"):
                        t_name = str(t['target']).lower()
                    
                    if t_name and t_name in where_referenced_cols and t_name not in existing_transform_names:
                        __transforms_eff_sqlglot.append(t)
                        transform_deps_sg[t_name] = set()
                        sys.stderr.write(f"[SQLGlot] Re-added WHERE-referenced transform: {t_name} (was filtered by scope)\n")
                        sys.stderr.flush()
                
                # Re-add missing custom columns from ds_transforms
                if ds_transforms:
                    all_custom_cols = ds_transforms.get("customColumns", [])
                    for cc in all_custom_cols:
                        if not isinstance(cc, dict):
                            continue
                        cc_name = (cc.get("name") or "").strip().lower()
                        if cc_name in where_referenced_cols and cc_name not in existing_cc_names:
                            __custom_cols_sqlglot.append(cc)
                            custom_deps_sg[cc_name] = set()
                            sys.stderr.write(f"[SQLGlot] Re-added WHERE-referenced custom column: {cc.get('name')} (was filtered by scope)\n")
                            sys.stderr.flush()
            
            all_alias_names_sg = set(custom_deps_sg.keys()) | set(transform_deps_sg.keys())
            needed_aliases_sg: set[str] = set(n for n in all_alias_names_sg if n in used_alias_roots_sg)
            # Follow dependencies transitively among aliases
            queue_sg: list[str] = list(needed_aliases_sg)
            while queue_sg:
                cur = queue_sg.pop()
                deps = custom_deps_sg.get(cur, set()) | transform_deps_sg.get(cur, set())
                for dep in deps:
                    dl = dep.lower()
                    if dl in all_alias_names_sg and dl not in needed_aliases_sg:
                        needed_aliases_sg.add(dl)
                        queue_sg.append(dl)
            
            sys.stderr.write(f"[SQLGlot] Reachability: needed aliases after transitive closure: {needed_aliases_sg}\n")
            sys.stderr.flush()
            print(f"[SQLGlot] Reachability: needed aliases after transitive closure: {needed_aliases_sg}")
            
            if needed_aliases_sg:
                before_cc_sg = len(__custom_cols_sqlglot)
                before_tr_sg = len(__transforms_eff_sqlglot)
                __custom_cols_sqlglot = [
                    cc for cc in __custom_cols_sqlglot
                    if isinstance(cc, dict) and cc.get("name") and cc.get("name").strip().lower() in needed_aliases_sg
                ]
                __transforms_eff_sqlglot_new = []
                for t in __transforms_eff_sqlglot:
                    if not isinstance(t, dict) or t.get("type") != "computed":
                        __transforms_eff_sqlglot_new.append(t)
                        continue
                    name = (t.get("name") or "").strip().lower()
                    if name and (name in needed_aliases_sg):
                        __transforms_eff_sqlglot_new.append(t)
                __transforms_eff_sqlglot = __transforms_eff_sqlglot_new
                sys.stderr.write(f"[SQLGlot] Reachability filter: kept {len(__custom_cols_sqlglot)}/{before_cc_sg} custom columns, {len(__transforms_eff_sqlglot)}/{before_tr_sg} transforms\n")
                sys.stderr.write(f"[SQLGlot] Reachability: kept custom columns: {[cc.get('name') for cc in __custom_cols_sqlglot if isinstance(cc, dict)]}\n")
                sys.stderr.flush()
                print(f"[SQLGlot] Reachability filter: kept {len(__custom_cols_sqlglot)}/{before_cc_sg} custom columns, {len(__transforms_eff_sqlglot)}/{before_tr_sg} transforms")
            else:
                # No alias-based dependencies needed for this pivot; safe to drop computed
                # custom columns and transforms entirely from _base.
                sys.stderr.write(f"[SQLGlot] Reachability filter: no custom aliases referenced; dropping all customColumns/computed transforms from _base\n")
                sys.stderr.flush()
                print(f"[SQLGlot] Reachability filter: no custom aliases referenced; dropping all customColumns/computed transforms from _base")
                __custom_cols_sqlglot = []
                __transforms_eff_sqlglot = [t for t in __transforms_eff_sqlglot if not (isinstance(t, dict) and t.get("type") == "computed")]
            
            print(f"[SQLGlot] Final (after reachability): {len(__custom_cols_sqlglot)} custom columns, {len(__transforms_eff_sqlglot)} transforms")
            
            # If datasource transforms exist, use transformed subquery as source
            # This ensures custom columns and joins are available to pivot dimensions
            effective_source = payload.source
            if ds_transforms:
                joins_to_use = probe_joins_filtered if (ds_type == 'duckdb' and probe_joins_filtered is not None) else (ds_transforms.get("joins", []) or [])
                print(f"[SQLGlot] Pivot: Using {len(joins_to_use)} joins for final query")
                if joins_to_use:
                    for idx, j in enumerate(joins_to_use):
                        print(f"[SQLGlot] Pivot:   Join {idx}: joinType={j.get('joinType')}, targetTable={j.get('targetTable')}, sourceKey={j.get('sourceKey')}")
                
                # Extract base_sql from legacy builder's construction (lines 1009-1018)
                # This applies custom columns, transforms, and joins.
                #
                # For DuckDB + SQLGlot, we want _base to expose both:
                # - All base table columns (via s.*) so that filters on raw/derived date parts
                #   like "DueDate (Year)" can be expressed against the underlying DueDate column.
                # - Only the filtered custom columns/transforms we kept in __custom_cols_sqlglot
                #   and __transforms_eff_sqlglot.
                #
                # Passing base_select=None tells build_sql to project s.* plus the provided
                # custom_columns/transforms, while our earlier filtering ensures that invalid
                # or unreachable custom columns/transforms (e.g. ones depending on VisitType)
                # are already excluded.
                cc_names_for_build = [cc.get('name') for cc in __custom_cols_sqlglot if isinstance(cc, dict)]
                tr_names_for_build = [t.get('name') or t.get('target') for t in __transforms_eff_sqlglot if isinstance(t, dict)]
                sys.stderr.write(f"[SQLGlot] About to call build_sql with {len(__custom_cols_sqlglot)} custom columns: {cc_names_for_build}\n")
                sys.stderr.write(f"[SQLGlot] About to call build_sql with {len(__transforms_eff_sqlglot)} transforms: {tr_names_for_build}\n")
                sys.stderr.flush()
                base_select_for_build = None
                try:
                    if ds_type == 'duckdb' and base_cols:
                        exclude_lower: set[str] = set()
                        # Exclude join aliases so the joined projection is the only column with that name.
                        for j in (joins_to_use or []):
                            for col in ((j or {}).get('columns') or []):
                                al = str((col or {}).get('alias') or (col or {}).get('name') or '').strip()
                                if al:
                                    exclude_lower.add(al.lower())
                        # Exclude custom/transform aliases that we will materialize explicitly.
                        for cc in __custom_cols_sqlglot:
                            if isinstance(cc, dict) and cc.get('name'):
                                exclude_lower.add(str(cc.get('name') or '').strip().lower())
                        for t in __transforms_eff_sqlglot:
                            if isinstance(t, dict):
                                tt = str(t.get('type') or '').lower()
                                if tt == 'computed' and t.get('name'):
                                    exclude_lower.add(str(t.get('name') or '').strip().lower())
                                elif tt in {'case','replace','translate','nullhandling'} and t.get('target'):
                                    exclude_lower.add(str(t.get('target') or '').strip().lower())
                        base_select_for_build = [c for c in sorted(base_cols) if c and c.lower() not in exclude_lower]
                        
                        # Add transform names to base_select so build_sql knows to materialize them
                        for t in __transforms_eff_sqlglot:
                            if isinstance(t, dict):
                                tt = str(t.get('type') or '').lower()
                                if tt == 'computed' and t.get('name'):
                                    base_select_for_build.append(str(t.get('name') or '').strip())
                                elif tt in {'case','replace','translate','nullhandling'} and t.get('target'):
                                    base_select_for_build.append(str(t.get('target') or '').strip())
                        
                        if not base_select_for_build:
                            base_select_for_build = None
                except Exception:
                    base_select_for_build = None

                result = build_sql(
                    dialect=ds_type,
                    source=_q_source(payload.source),
                    base_select=base_select_for_build,
                    custom_columns=__custom_cols_sqlglot,
                    transforms=__transforms_eff_sqlglot,
                    joins=joins_to_use,
                    defaults={},
                    limit=None,
                )
                # Handle different return value formats (3 or 4 elements)
                if len(result) == 3:
                    base_sql, _cols_unused, _warns = result
                elif len(result) == 4:
                    base_sql, _cols_unused, _warns, _ = result
                else:
                    print(f"[SQLGlot] Pivot: Unexpected build_sql return count: {len(result)}")
                    base_sql = result[0] if result else ""
                effective_source = f"({base_sql}) AS _base"
                # Set use_expr_map to None because custom columns and transforms are already
                # materialized in the _base subquery. build_pivot_query should query them
                # directly from _base, not try to expand them inline.
                use_expr_map = None
                sys.stderr.write(f"[FIXED_CODE_V2] Pivot: Using transformed source (len={len(effective_source)}). use_expr_map set to None (columns materialized in _base).\n")
                sys.stderr.write(f"[FIXED_CODE_V2] Pivot: About to continue to build_pivot_query call...\n")
                sys.stderr.flush()
                print(f"[SQLGlot] Pivot: Using transformed source (has {len(ds_transforms.get('customColumns', []))} customColumns, {len(__joins_eff)} joins)")

                # Optional, detailed join diagnostics for debugging NULL dimensions like VaultName.
                # Guarded by env var to avoid noisy logs / extra queries.
                try:
                    try:
                        _dbg_env = (os.environ.get("PIVOT_JOIN_DEBUG", "") or "").strip().lower()
                    except Exception:
                        _dbg_env = ""
                    _dbg_enabled = bool(getattr(settings, "pivot_join_debug", False)) or (_dbg_env in {"1", "true", "yes", "on"})
                    if ds_type == "duckdb" and _dbg_enabled:
                        # Only run when the pivot actually requests VaultName.
                        _need_vault = any(str(x or "").strip().lower() == "vaultname" for x in (r_dims or []))
                        if _need_vault:
                            sys.stderr.write("[PIVOT_JOIN_DEBUG] enabled\n")
                            sys.stderr.flush()

                            # Resolve DuckDB db file path (match run_query behavior as closely as possible).
                            _db_path = None
                            try:
                                if payload.datasourceId is None:
                                    _db_path = get_active_duck_path()
                                else:
                                    _ds_obj = None
                                    try:
                                        _ds_obj = db.get(Datasource, payload.datasourceId)
                                    except Exception:
                                        _ds_obj = None
                                    _db_path = get_active_duck_path()
                                    if _ds_obj and getattr(_ds_obj, "connection_encrypted", None):
                                        try:
                                            _dsn = decrypt_text(_ds_obj.connection_encrypted)
                                            _p = urlparse(_dsn) if _dsn else None
                                            if _p and (_p.scheme or "").startswith("duckdb"):
                                                _path = unquote(_p.path or "")
                                                if _path.startswith("///"):
                                                    _path = _path[2:]
                                                _db_path = _path or _db_path
                                                if _db_path and _db_path != ":memory:" and _db_path.startswith("/."):
                                                    try:
                                                        _db_path = os.path.abspath(_db_path[1:])
                                                    except Exception:
                                                        pass
                                                if ":memory:" in (_dsn or "").lower():
                                                    _db_path = ":memory:"
                                        except Exception:
                                            pass
                            except Exception:
                                _db_path = settings.duckdb_path

                            sys.stderr.write(f"[PIVOT_JOIN_DEBUG] duckdb_path={_db_path}\n")
                            sys.stderr.flush()

                            # Find the join definition that is expected to produce VaultName.
                            _vault_join = None
                            for _j in (joins_to_use or []):
                                try:
                                    for _c in ((_j or {}).get("columns") or []):
                                        _al = str((_c or {}).get("alias") or (_c or {}).get("name") or "").strip().lower()
                                        if _al == "vaultname":
                                            _vault_join = _j
                                            break
                                    if _vault_join:
                                        break
                                except Exception:
                                    continue

                            if _vault_join:
                                try:
                                    sys.stderr.write(
                                        "[PIVOT_JOIN_DEBUG] VaultName join="
                                        f" targetTable={_vault_join.get('targetTable')}"
                                        f" sourceKey={_vault_join.get('sourceKey')}"
                                        f" targetKey={_vault_join.get('targetKey')}"
                                        f" filter={_vault_join.get('filter')}\n"
                                    )
                                    sys.stderr.flush()
                                except Exception:
                                    pass

                            def _dbg_query(conn, label: str, sql_text: str) -> None:
                                try:
                                    cur = conn.execute(sql_text)
                                    rows = cur.fetchall()
                                    sys.stderr.write(f"[PIVOT_JOIN_DEBUG] {label}: {rows[:10]}\n")
                                    sys.stderr.flush()
                                except Exception as _e:
                                    sys.stderr.write(f"[PIVOT_JOIN_DEBUG] {label} failed: {_e}\n")
                                    sys.stderr.flush()

                            # Run diagnostics against the exact base_sql being used.
                            try:
                                with open_duck_native(_db_path) as _conn:
                                    _stats_sql = (
                                        "SELECT "
                                        "COUNT(*) AS total, "
                                        "SUM(CASE WHEN \"VaultID\" IS NULL THEN 1 ELSE 0 END) AS vaultid_null, "
                                        "SUM(CASE WHEN \"VaultName\" IS NULL THEN 1 ELSE 0 END) AS vaultname_null, "
                                        "SUM(CASE WHEN \"VaultName\" IS NOT NULL THEN 1 ELSE 0 END) AS vaultname_nonnull, "
                                        "COUNT(DISTINCT CAST(\"VaultID\" AS VARCHAR)) AS vaultid_distinct "
                                        f"FROM ({base_sql}) AS _b"
                                    )
                                    _dbg_query(_conn, "base_stats", _stats_sql)

                                    _unmatched_sql = (
                                        "SELECT CAST(\"VaultID\" AS VARCHAR) AS vaultid, COUNT(*) AS cnt "
                                        f"FROM ({base_sql}) AS _b "
                                        "WHERE \"VaultName\" IS NULL AND \"VaultID\" IS NOT NULL "
                                        "GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
                                    )
                                    _dbg_query(_conn, "unmatched_vaultids", _unmatched_sql)

                                    _matched_sql = (
                                        "SELECT \"VaultName\", COUNT(*) AS cnt "
                                        f"FROM ({base_sql}) AS _b "
                                        "WHERE \"VaultName\" IS NOT NULL "
                                        "GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
                                    )
                                    _dbg_query(_conn, "top_vaultnames", _matched_sql)

                                    # Lookup table stats (best-effort; may fail if schema differs).
                                    if _vault_join and _vault_join.get('targetTable'):
                                        _tt = str(_vault_join.get('targetTable') or '').strip()
                                        if _tt:
                                            _dbg_query(
                                                _conn,
                                                "lookup_type_counts",
                                                f"SELECT lower(trim(LKP_Type)) AS lkp_type, COUNT(*) AS cnt FROM {_tt} GROUP BY 1 ORDER BY 2 DESC LIMIT 20",
                                            )
                                            _dbg_query(
                                                _conn,
                                                "lookup_vault_rows",
                                                f"SELECT COUNT(*) AS total, SUM(CASE WHEN lower(trim(LKP_Type))='vault' THEN 1 ELSE 0 END) AS vault_rows FROM {_tt}",
                                            )
                                            _dbg_query(
                                                _conn,
                                                "lookup_vault_sample",
                                                f"SELECT CAST(LKP_ID AS VARCHAR) AS lkp_id, EnName FROM {_tt} WHERE lower(trim(LKP_Type))='vault' LIMIT 10",
                                            )
                            except Exception as _e:
                                sys.stderr.write(f"[PIVOT_JOIN_DEBUG] open/query failed: {_e}\n")
                                sys.stderr.flush()
                except Exception:
                    pass
            else:
                print(f"[SQLGlot] Pivot: Using direct source (no transforms)")
                use_expr_map = expr_map
            
            builder = SQLGlotBuilder(dialect=ds_type)
            
            # Debug logging
            sys.stderr.write(f"[SQLGlot] Pivot: effective_source contains _base: {'_base' in effective_source}\n")
            sys.stderr.write(f"[SQLGlot] Pivot: use_expr_map is None: {use_expr_map is None}\n")
            sys.stderr.write(f"[TRACE] About to process WHERE clause resolution...\n")
            sys.stderr.flush()
            
            if not use_expr_map and expr_map:
                print(f"[SQLGlot] Pivot: NOT passing expr_map to build_pivot_query (custom columns already materialized in _base)")
            
            # Resolve custom columns in WHERE clause (only if not using _base subquery)
            resolved_where = payload.where
            sys.stderr.write(f"[TRACE] About to resolve WHERE clause. payload.where={payload.where is not None}, expr_map={expr_map is not None}, use_expr_map is None={use_expr_map is None}\n")
            sys.stderr.flush()
            # Skip WHERE clause resolution if using _base subquery (use_expr_map is None)
            # because custom columns are already materialized in _base
            if payload.where and expr_map and use_expr_map is not None:
                try:
                    _expr_map_norm = {}
                    for _k, _v in (expr_map or {}).items():
                        try:
                            _nk = str(_k or "").strip().lower()
                            if _nk and _nk not in _expr_map_norm:
                                _expr_map_norm[_nk] = _v
                        except Exception:
                            continue
                except Exception:
                    _expr_map_norm = {}
                print(f"[SQLGlot] Pivot: Resolving WHERE clause with {len(expr_map)} custom columns")
                print(f"[SQLGlot] Pivot: WHERE keys before resolution: {list(payload.where.keys())}")
                resolved = {}
                for key, value in payload.where.items():
                    base_key = key.split("__")[0] if "__" in key else key
                    op_suffix = key.split("__", 1)[1] if "__" in key else None
                    base_key_s = str(base_key or "").strip()
                    expr = None
                    if base_key_s in expr_map:
                        expr = expr_map[base_key_s]
                    else:
                        expr = _expr_map_norm.get(base_key_s.lower())
                    if expr is not None:
                        expr = str(expr)
                        try:
                            def _expand_aliases_where(_expr_s: str, _depth: int = 0) -> str:
                                if _depth > 10:
                                    return _expr_s
                                expanded = _expr_s

                                # Expand quoted identifiers first: "Alias"
                                try:
                                    matches = re.findall(r'"([^"]+)"', expanded)
                                except Exception:
                                    matches = []
                                for match in matches:
                                    m_s = str(match or "").strip()
                                    if not m_s:
                                        continue
                                    alias_expr = None
                                    if m_s in expr_map:
                                        alias_expr = expr_map[m_s]
                                    else:
                                        alias_expr = _expr_map_norm.get(m_s.lower())
                                    if alias_expr is None:
                                        continue
                                    try:
                                        expanded = re.sub(rf'"{re.escape(m_s)}"', f'({str(alias_expr)})', expanded)
                                    except Exception:
                                        expanded = expanded.replace(f'"{m_s}"', f'({str(alias_expr)})')

                                # Expand bare identifiers (unquoted): Alias
                                try:
                                    sql_keywords = {
                                        'select','from','where','join','inner','left','right','full','outer','on',
                                        'and','or','not','case','when','then','else','end',
                                        'as','distinct','top','limit','offset','group','by','order','asc','desc',
                                        'null','is','in','like','between','exists','true','false',
                                        'sum','avg','min','max','count','coalesce','cast','try_cast',
                                        'regexp_replace','month','year','day','week','quarter','extract','strftime'
                                    }
                                    try:
                                        bare = re.findall(r'\b([A-Za-z_][A-Za-z0-9_]*)\b', expanded)
                                    except Exception:
                                        bare = []
                                    for tok in (bare or []):
                                        tl = str(tok or '').strip().lower()
                                        if not tl or tl in sql_keywords:
                                            continue
                                        alias_expr2 = _expr_map_norm.get(tl)
                                        if alias_expr2 is None:
                                            continue
                                        # Avoid replacing function calls like MONTH(...)
                                        try:
                                            if re.search(rf'\b{re.escape(tok)}\s*\(', expanded, flags=re.IGNORECASE):
                                                continue
                                        except Exception:
                                            pass
                                        try:
                                            pat = rf'(?<![A-Za-z0-9_"\[\.]){re.escape(tok)}(?![A-Za-z0-9_"\]\.])'
                                            expanded = re.sub(pat, f'({str(alias_expr2)})', expanded)
                                        except Exception:
                                            continue
                                except Exception:
                                    pass

                                if expanded != _expr_s:
                                    return _expand_aliases_where(expanded, _depth + 1)
                                return expanded

                            expr = _expand_aliases_where(expr)
                        except Exception:
                            pass
                        # Strip table aliases
                        expr = re.sub(r'"[a-z][a-z_]{0,4}"\.', '', expr)
                        expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)
                        resolved_key = f"({expr})" if not op_suffix else f"({expr})__{op_suffix}"
                        resolved[resolved_key] = value
                        print(f"[SQLGlot] Pivot: Resolved '{key}' -> '{resolved_key[:80]}...'")
                    else:
                        resolved[key] = value
                resolved_where = resolved
                print(f"[SQLGlot] Pivot: WHERE keys after resolution: {list(resolved_where.keys())}")

            # Validate dimensions against available columns in _base
            sys.stderr.write(f"[TRACE] Finished WHERE clause resolution. About to validate dimensions...\n")
            sys.stderr.flush()
            
            # Validate dimensions (remove any that were filtered out)
            # This prevents "Column not found" errors when a custom column was excluded
            # from _base due to missing dependencies.
            if use_expr_map is None:
                # Set of valid custom column names that made it into _base
                valid_cc_names = {cc.get('name').lower() for cc in __custom_cols_sqlglot if isinstance(cc, dict) and cc.get('name')}
                # Set of all known custom column names (to distinguish from base columns)
                all_cc_names = {cc.get('name').lower() for cc in (ds_transforms.get("customColumns", []) if ds_transforms else [])}
                
                def filter_dims(dims):
                    valid_dims = []
                    for d in dims:
                        d_lower = str(d).strip().lower()
                        # If it's a custom column but NOT in the valid set, skip it
                        if d_lower in all_cc_names and d_lower not in valid_cc_names:
                            print(f"[SQLGlot] Pivot: Dropping invalid dimension '{d}' (filtered out custom column)")
                            continue
                        valid_dims.append(d)
                    return valid_dims

                sys.stderr.write(f"[SQLGlot] Pivot: Validating dimensions. Rows={len(r_dims)}, Cols={len(c_dims)}\n")
                sys.stderr.write(f"[SQLGlot] Pivot: r_dims before: {r_dims}\n")
                sys.stderr.write(f"[SQLGlot] Pivot: c_dims before: {c_dims}\n")
                sys.stderr.write(f"[SQLGlot] Pivot: valid_cc_names: {valid_cc_names}\n")
                sys.stderr.write(f"[SQLGlot] Pivot: all_cc_names: {all_cc_names}\n")
                sys.stderr.flush()
                r_dims = filter_dims(r_dims)
                c_dims = filter_dims(c_dims)
                sys.stderr.write(f"[SQLGlot] Pivot: After validation. Rows={len(r_dims)}, Cols={len(c_dims)}\n")
                sys.stderr.write(f"[SQLGlot] Pivot: r_dims after: {r_dims}\n")
                sys.stderr.write(f"[SQLGlot] Pivot: c_dims after: {c_dims}\n")
                sys.stderr.flush()

            sys.stderr.write(f"[SQLGlot] About to call build_pivot_query with:\n")
            sys.stderr.write(f"  - rows={r_dims}\n")
            sys.stderr.write(f"  - cols={c_dims}\n")
            sys.stderr.write(f"  - expr_map={'None' if use_expr_map is None else f'Dict with {len(use_expr_map)} keys'}\n")
            sys.stderr.write(f"  - effective_source (first 200 chars): {effective_source[:200]}\n")
            sys.stderr.write(f"  - effective_source contains '_base': {'_base' in effective_source}\n")
            sys.stderr.flush()
            
            # Calculate effective limit to push down to SQLGlot (prevents massive sorts)
            effective_limit = payload.limit
            if effective_limit is None:
                try:
                    pivot_default_limit = int(os.environ.get("PIVOT_DEFAULT_LIMIT", "2000") or "2000")
                except Exception:
                    pivot_default_limit = 2000
                if pivot_default_limit > 0:
                    effective_limit = pivot_default_limit

            inner = builder.build_pivot_query(
                source=effective_source,
                rows=r_dims,
                cols=c_dims,
                value_field=val_field if val_field else None,
                agg=agg,
                where=resolved_where,
                group_by=payload.groupBy if hasattr(payload, 'groupBy') else None,
                week_start=payload.weekStart if hasattr(payload, 'weekStart') else 'mon',
                limit=effective_limit,
                expr_map=use_expr_map,
                ds_type=ds_type,
                date_format=payload.dateFormat if hasattr(payload, 'dateFormat') else None,
                date_columns=payload.dateColumns if hasattr(payload, 'dateColumns') else None,
            )
            print(f"[SQLGlot] Pivot: Generated SQL: {inner[:150]}...")
            sys.stderr.write(f"[DEBUG] Full generated SQL:\n{inner}\n")
            sys.stderr.write(f"[DEBUG] About to execute pivot query via run_query, SQL length: {len(inner)}\n")
            sys.stderr.flush()
            
        except Exception as e:
            print(f"[SQLGlot] Pivot: Error: {e}")
            import traceback
            print(f"[SQLGlot] Pivot: Full traceback:\n{traceback.format_exc()}")
            logger.warning(f"[SQLGlot] Pivot query failed: {e}")
            if not settings.enable_legacy_fallback:
                logger.error(f"[SQLGlot] Pivot: LEGACY FALLBACK DISABLED - Re-raising error")
                raise HTTPException(status_code=500, detail=f"SQLGlot query generation failed: {e}")
            print(f"[SQLGlot] Pivot: Falling back to legacy builder")
            use_sqlglot = False
    
    if not use_sqlglot:
        # LEGACY PATH: String-based SQL building
        # For Sankey charts: if exactly 1 row dim and 1 col dim, use standardized aliases 'x', 'legend', 'value'
        if len(r_exprs) == 1 and len(c_exprs) == 1:
            # Sankey format: source (x), target (legend), value
            r_expr, r_alias = r_exprs[0]
            c_expr, c_alias = c_exprs[0]
            sel = f"{r_expr} AS x, {c_expr} AS legend, {value_expr} AS value"
        else:
            sel = ", ".join(sel_parts + [f"{value_expr} AS value"]) or f"{value_expr} AS value"
        # Use ordinals for DuckDB/Postgres/MySQL/SQLite; use expressions only for SQL Server
        dim_count = len(r_exprs) + len(c_exprs)
        if dim_count > 0:
            if 'mssql' in (ds_type or '') or 'sqlserver' in (ds_type or ''):
                group_by = ", ".join([e for e, _ in (r_exprs + c_exprs)])
                gb_sql = f" GROUP BY {group_by}"
                order_by = f" ORDER BY {group_by}"
            else:
                ordinals = ", ".join(str(i) for i in range(1, dim_count + 1))
                gb_sql = f" GROUP BY {ordinals}"
                order_by = f" ORDER BY {ordinals}"
        else:
            gb_sql = ""
            order_by = ""
        import sys
        sys.stderr.write(f"[DEBUG] base_from_sql = {base_from_sql[:200]}\n")
        sys.stderr.write(f"[DEBUG] ds_transforms exists = {bool(ds_transforms)}, custom_cols = {len(ds_transforms.get('customColumns', [])) if ds_transforms else 0}\n")
        sys.stderr.flush()
        inner = f"SELECT {sel}{base_from_sql}{where_sql}{gb_sql}{order_by}"

    # Delegate execution to /query. If no explicit limit is provided, fetch all pages.
    import sys
    sys.stderr.write(f"[DEBUG] Acquiring semaphore for query execution...\n")
    sys.stderr.flush()
    _HEAVY_SEM.acquire()
    try:
        sys.stderr.write(f"[DEBUG] Semaphore acquired, creating QueryRequest...\n")
        sys.stderr.flush()
        if payload.limit is not None:
            q = QueryRequest(
                sql=inner,
                datasourceId=payload.datasourceId,
                limit=payload.limit,
                offset=0,
                includeTotal=False,
                params=params or None,
            )
            sys.stderr.write(f"[DEBUG] Calling run_query with limit={payload.limit}...\n")
            sys.stderr.flush()
            return run_query(q, db, actorId=actorId, publicId=publicId, token=token)

        # Default behavior: cap pivot results when limit is omitted to avoid buffering large results.
        try:
            pivot_default_limit = int(os.environ.get("PIVOT_DEFAULT_LIMIT", "2000") or "2000")
        except Exception:
            pivot_default_limit = 2000
        if pivot_default_limit <= 0:
            pivot_default_limit = 2000
        q = QueryRequest(
            sql=inner,
            datasourceId=payload.datasourceId,
            limit=pivot_default_limit,
            offset=0,
            includeTotal=False,
            params=params or None,
        )
        return run_query(q, db, actorId=actorId, publicId=publicId, token=token)
    finally:
        _HEAVY_SEM.release()


@router.post("/pivot/sql")
def preview_pivot_sql(payload: PivotRequest, db: Session = Depends(get_db), actorId: Optional[str] = None, publicId: Optional[str] = None, token: Optional[str] = None):
    if actorId:
        _ra = _throttle_take(actorId)
        if _ra:
            raise HTTPException(status_code=429, detail="Rate limit exceeded", headers={"Retry-After": str(_ra)})
    if isinstance(publicId, str) and publicId:
        sl = get_share_link_by_public(db, publicId)
        if not sl:
            raise HTTPException(status_code=404, detail="Not found")
        if not verify_share_link_token(sl, token if isinstance(token, str) else None, settings.secret_key):
            raise HTTPException(status_code=401, detail="Unauthorized")
    # Build FROM base honoring datasource-level transforms and scoping (datasource/table/widget)
    engine = _engine_for_datasource(db, payload.datasourceId, actorId)
    ds_type = None
    try:
        ds_type = (engine.dialect.name or "").lower()
    except Exception:
        ds_type = None
    # Align builder dialect with likely execution route (DuckDB) to avoid mismatches
    try:
        _prefer_duck = bool(settings.prefer_local_duckdb)
    except Exception:
        _prefer_duck = False
    try:
        _src_for_duck = getattr(payload, 'source', None)
    except Exception:
        _src_for_duck = None
    try:
        if (payload.datasourceId is None) or (_prefer_duck and _duck_has_table(_src_for_duck)):
            ds_type = "duckdb"
    except Exception:
        pass
    
    # Check if SQLGlot should be used (same as run_pivot)
    use_sqlglot = should_use_sqlglot(actorId)
    inner = None
    if use_sqlglot:
        try:
            # Generate SQL using SQLGlot (same logic as run_pivot, but without execution)
            print(f"[SQLGlot] /pivot/sql: ENABLED for user={actorId}, dialect={ds_type}")
            
            expr_map = {}
            effective_source = payload.source
            use_expr_map = expr_map

            ds_transforms = {}
            try:
                if payload.datasourceId:
                    ds_info = _ds_cache_get(str(payload.datasourceId))
                    if ds_info is None:
                        try:
                            ds_obj = db.get(Datasource, payload.datasourceId)
                        except Exception:
                            ds_obj = None
                        if ds_obj:
                            ds_info = {
                                "id": ds_obj.id,
                                "user_id": ds_obj.user_id,
                                "connection_encrypted": ds_obj.connection_encrypted,
                                "type": ds_obj.type,
                                "options_json": ds_obj.options_json,
                            }
                            _ds_cache_set(str(payload.datasourceId), ds_info)
                    if ds_info is not None:
                        try:
                            opts = json.loads((ds_info.get("options_json") or "{}"))
                        except Exception:
                            opts = {}
                        ds_tr = ((opts or {}).get("transforms") if isinstance(opts, dict) else None)
                        if (not isinstance(ds_tr, dict)) and isinstance(opts, dict):
                            if any(k in opts for k in ("customColumns", "transforms", "joins", "defaults")):
                                ds_tr = opts
                        ds_tr = ds_tr or {}

                        def _matches_table(scope_table: str, source_name: str) -> bool:
                            def norm(s: str) -> str:
                                s = (s or '').strip().strip('[]').strip('"').strip('`')
                                parts = s.split('.')
                                return parts[-1].lower()
                            return norm(scope_table) == norm(source_name)

                        def _filt(arr):
                            out = []
                            for it in (arr or []):
                                sc = (it or {}).get('scope')
                                if not sc:
                                    out.append(it)
                                    continue
                                lvl = str(sc.get('level') or '').lower()
                                if lvl == 'datasource':
                                    out.append(it)
                                elif lvl == 'table' and sc.get('table'):
                                    if _matches_table(str(sc.get('table') or ''), payload.source):
                                        out.append(it)
                                elif lvl == 'widget':
                                    try:
                                        wid = str((sc or {}).get('widgetId') or '').strip()
                                        if wid and getattr(payload, 'widgetId', None) and str(getattr(payload, 'widgetId')).strip() == wid:
                                            out.append(it)
                                    except Exception:
                                        pass
                            return out

                        ds_transforms = {
                            'customColumns': _filt(ds_tr.get('customColumns')),
                            'transforms': _filt(ds_tr.get('transforms')),
                            'joins': _filt(ds_tr.get('joins')),
                            'defaults': ds_tr.get('defaults') or {},
                        }
            except Exception:
                ds_transforms = {}

            if ds_type == 'duckdb' and not ds_transforms:
                try:
                    best = None
                    best_score = -1
                    needed = {str(x).strip().lower() for x in (list(payload.rows or []) + list(payload.cols or [])) if str(x).strip()}
                    if getattr(payload, 'valueField', None):
                        needed.add(str(getattr(payload, 'valueField') or '').strip().lower())
                    cands = db.query(Datasource).filter(Datasource.type.like('duckdb%')).all()
                    for ds_candidate in (cands or []):
                        try:
                            opts = json.loads(ds_candidate.options_json or "{}")
                        except Exception:
                            opts = {}
                        tr = (opts or {}).get("transforms") or {}
                        if not isinstance(tr, dict):
                            continue
                        def _matches_table(scope_table: str, source_name: str) -> bool:
                            def norm(s: str) -> str:
                                s = (s or '').strip().strip('[]').strip('"').strip('`')
                                parts = s.split('.')
                                return parts[-1].lower()
                            return norm(scope_table) == norm(source_name)
                        def _scoped(arr):
                            out = []
                            for it in (arr or []):
                                sc = (it or {}).get('scope')
                                if not sc:
                                    out.append(it)
                                    continue
                                lvl = str(sc.get('level') or '').lower()
                                if lvl == 'datasource':
                                    out.append(it)
                                elif lvl == 'table' and sc.get('table') and _matches_table(str(sc.get('table') or ''), payload.source):
                                    out.append(it)
                                elif lvl == 'widget':
                                    wid = str((sc or {}).get('widgetId') or '').strip()
                                    if wid and getattr(payload, 'widgetId', None) and str(getattr(payload, 'widgetId')).strip() == wid:
                                        out.append(it)
                            return out
                        scoped = {
                            'customColumns': _scoped(tr.get('customColumns')),
                            'transforms': _scoped(tr.get('transforms')),
                            'joins': _scoped(tr.get('joins')),
                            'defaults': tr.get('defaults') or {},
                        }
                        aliases = set()
                        for j in (scoped.get('joins') or []):
                            for col in ((j or {}).get('columns') or []):
                                al = str((col or {}).get('alias') or (col or {}).get('name') or '').strip()
                                if al:
                                    aliases.add(al.lower())
                        for cc in (scoped.get('customColumns') or []):
                            nm = str((cc or {}).get('name') or '').strip()
                            if nm:
                                aliases.add(nm.lower())
                        score = 0
                        if needed and (needed & aliases):
                            score += 10 * len(needed & aliases)
                        if len(scoped.get('joins') or []) > 0:
                            score += 2
                        if score > best_score:
                            best_score = score
                            best = scoped
                    if best and best_score > 0:
                        ds_transforms = best
                except Exception:
                    pass

            if ds_type == 'duckdb' and ds_transforms:
                try:
                    base_cols: set[str] = set()
                    try:
                        probe_result_base = build_sql(
                            dialect=ds_type,
                            source=_q_source(payload.source),
                            base_select=["*"],
                            custom_columns=[],
                            transforms=[],
                            joins=[],
                            defaults={},
                            limit=None,
                        )
                        if len(probe_result_base) == 3:
                            probe_base_sql, _, _ = probe_result_base
                        elif len(probe_result_base) == 4:
                            probe_base_sql, _, _, _ = probe_result_base
                        else:
                            probe_base_sql = probe_result_base[0] if probe_result_base else ""
                        probe_sql_phase1 = f"SELECT * FROM ({probe_base_sql}) AS _probe LIMIT 0"
                        with open_duck_native(None) as conn:
                            probe_cursor = conn.execute(probe_sql_phase1)
                            base_cols = {str(col[0]).strip() for col in (probe_cursor.description or [])}
                    except Exception:
                        base_cols = set()

                    r_dims = list(payload.rows or [])
                    c_dims = list(payload.cols or [])
                    val_field = (payload.valueField or '').strip()
                    needed = {str(x).strip().lower() for x in (r_dims + c_dims) if str(x).strip()}
                    if val_field:
                        needed.add(val_field.lower())
                    try:
                        for k in (payload.where or {}).keys():
                            if isinstance(k, str) and k:
                                base = k.split('__', 1)[0]
                                if base:
                                    needed.add(base.strip().lower())
                    except Exception:
                        pass

                    custom_cols = [cc for cc in (ds_transforms.get('customColumns') or []) if isinstance(cc, dict) and str(cc.get('name') or '').strip().lower() in needed]
                    transforms = [t for t in (ds_transforms.get('transforms') or []) if isinstance(t, dict) and str(t.get('type') or '').lower() == 'computed' and str(t.get('name') or '').strip().lower() in needed]
                    joins_to_use = list(ds_transforms.get('joins') or [])

                    base_select_for_build = None
                    try:
                        if base_cols:
                            exclude_lower: set[str] = set()
                            for j in (joins_to_use or []):
                                for col in ((j or {}).get('columns') or []):
                                    al = str((col or {}).get('alias') or (col or {}).get('name') or '').strip()
                                    if al:
                                        exclude_lower.add(al.lower())
                            for cc in custom_cols:
                                nm = str((cc or {}).get('name') or '').strip()
                                if nm:
                                    exclude_lower.add(nm.lower())
                            for t in transforms:
                                nm = str((t or {}).get('name') or '').strip()
                                if nm:
                                    exclude_lower.add(nm.lower())
                            base_select_for_build = [c for c in sorted(base_cols) if c and c.lower() not in exclude_lower]
                            if not base_select_for_build:
                                base_select_for_build = None
                    except Exception:
                        base_select_for_build = None

                    result = build_sql(
                        dialect=ds_type,
                        source=_q_source(payload.source),
                        base_select=base_select_for_build,
                        custom_columns=custom_cols,
                        transforms=transforms,
                        joins=joins_to_use,
                        defaults={},
                        limit=None,
                    )
                    if len(result) == 3:
                        base_sql, _cols_unused, _warns = result
                    elif len(result) == 4:
                        base_sql, _cols_unused, _warns, _ = result
                    else:
                        base_sql = result[0] if result else ""
                    effective_source = f"({base_sql}) AS _base"
                    use_expr_map = None
                except Exception:
                    pass
            
            # Prepare dimensions
            r_dims = list(payload.rows or [])
            c_dims = list(payload.cols or [])
            val_field = (payload.valueField or "").strip()
            agg = (payload.aggregator or "count").lower()
            
            # For /pivot/sql with SQLGlot, custom columns are materialized in _base subquery
            # so numeric column names like "200" are valid as-is. Don't apply validation.
            
            # Build SQLGlot pivot query
            from ..sqlgen_glot import SQLGlotBuilder
            builder = SQLGlotBuilder(dialect=ds_type)
            inner = builder.build_pivot_query(
                source=effective_source,
                rows=r_dims,
                cols=c_dims,
                value_field=val_field if val_field else None,
                agg=agg,
                where=payload.where,
                group_by=payload.groupBy if hasattr(payload, 'groupBy') else None,
                week_start=payload.weekStart if hasattr(payload, 'weekStart') else 'mon',
                limit=payload.limit,
                expr_map=use_expr_map,
                ds_type=ds_type,
                date_format=payload.dateFormat if hasattr(payload, 'dateFormat') else None,
                date_columns=payload.dateColumns if hasattr(payload, 'dateColumns') else None,
            )
            print(f"[SQLGlot] /pivot/sql: Generated SQL ({len(inner)} chars)")
            return {"sql": inner}
        except Exception as e:
            print(f"[SQLGlot] /pivot/sql: Error: {e}")
            import traceback
            print(f"[SQLGlot] /pivot/sql: Traceback:\n{traceback.format_exc()}")
            logger.warning(f"[SQLGlot] /pivot/sql failed: {e}")
            # Fall through to legacy path
            use_sqlglot = False
    
    # Helpers duplicated from run_pivot for consistent quoting and derived expressions
    def _q_ident(name: str) -> str:
        s = str(name or '').strip('\n\r\t')
        try:
            if '.' in s and '(' not in s and ')' not in s:
                parts = s.split('.')
                s = parts[-1]
        except Exception:
            pass
        if not s:
            return s
        if s.startswith('[') and s.endswith(']'):
            return s
        if s.startswith('"') and s.endswith('"'):
            return s
        if s.startswith('`') and s.endswith('`'):
            return s
        d = (ds_type or '').lower()
        if 'mssql' in d or 'sqlserver' in d:
            return f"[{s}]"
        if 'mysql' in d:
            return f"`{s}`"
        return f'"{s}"'

    def _q_source(name: str) -> str:
        s = str(name or '').strip()
        if not s:
            return s
        d = (ds_type or '').lower()
        if 'mssql' in d or 'sqlserver' in d:
            parts = s.split('.')
            return '.'.join([p if (p.startswith('[') and p.endswith(']')) else f"[{p}]" for p in parts])
        if 'mysql' in d:
            parts = s.split('.')
            return '.'.join([p if (p.startswith('`') and p.endswith('`')) else f"`{p}`" for p in parts])
        # Default (DuckDB/Postgres/SQLite): double-quote each part so names with spaces/reserved words are valid
        parts = s.split('.')
        return '.'.join([p if ((p.startswith('"') and p.endswith('"')) or (p.startswith('[') and p.endswith(']')) or (p.startswith('`') and p.endswith('`'))) else f'"{p}"' for p in parts])

    def _derived_lhs(name: str) -> str:
        raw = str(name or '').strip()
        m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", raw, flags=re.IGNORECASE)
        if not m:
            return _q_ident(raw)
        base = m.group(1).strip()
        part = m.group(2).strip().lower()
        col = _q_ident(base)
        d = (ds_type or '').lower()
        if 'mssql' in d or 'sqlserver' in d:
            if part == 'year': return f"YEAR({col})"
            if part == 'quarter': return f"DATEPART(quarter, {col})"
            if part == 'month': return f"MONTH({col})"
            if part == 'month name': return f"DATENAME(month, {col})"
            if part == 'month short': return f"LEFT(DATENAME(month, {col}), 3)"
            if part == 'week': return f"DATEPART(iso_week, {col})"
            if part == 'day': return f"DAY({col})"
            if part == 'day name': return f"DATENAME(weekday, {col})"
            if part == 'day short': return f"LEFT(DATENAME(weekday, {col}), 3)"
            return col
        if 'duckdb' in d or 'postgres' in d or 'postgre' in d:
            if part == 'year': return f"EXTRACT(year FROM {col})"
            if part == 'quarter': return f"EXTRACT(quarter FROM {col})"
            if part == 'month': return f"EXTRACT(month FROM {col})"
            if part == 'month name': return f"to_char({col}, 'FMMonth')"
            if part == 'month short': return f"to_char({col}, 'Mon')"
            if part == 'week': return f"EXTRACT(week FROM {col})"
            if part == 'day': return f"EXTRACT(day FROM {col})"
            if part == 'day name': return f"to_char({col}, 'FMDay')"
            if part == 'day short': return f"to_char({col}, 'Dy')"
            return col
        if 'mysql' in d:
            if part == 'year': return f"YEAR({col})"
            if part == 'quarter': return f"QUARTER({col})"
            if part == 'month': return f"MONTH({col})"
            if part == 'month name': return f"DATE_FORMAT({col}, '%M')"
            if part == 'month short': return f"DATE_FORMAT({col}, '%b')"
            if part == 'week': return f"WEEK({col}, 3)"
            if part == 'day': return f"DAY({col})"
            if part == 'day name': return f"DATE_FORMAT({col}, '%W')"
            if part == 'day short': return f"DATE_FORMAT({col}, '%a')"
            return col
        if 'sqlite' in d:
            if part == 'year': return f"CAST(strftime('%Y', {col}) AS INTEGER)"
            if part == 'quarter':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 "
                    f"WHEN 4 THEN 2 WHEN 5 THEN 2 WHEN 6 THEN 2 "
                    f"WHEN 7 THEN 3 WHEN 8 THEN 3 WHEN 9 THEN 3 "
                    f"ELSE 4 END"
                )
            if part == 'month': return f"CAST(strftime('%m', {col}) AS INTEGER)"
            if part == 'month name':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April' "
                    f"WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' "
                    f"WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December' END"
                )
            if part == 'month short':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr' "
                    f"WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' "
                    f"WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec' END"
                )
            if part == 'week': return f"CAST(strftime('%W', {col}) AS INTEGER)"
            if part == 'day': return f"CAST(strftime('%d', {col}) AS INTEGER)"
            if part == 'day name':
                return "CASE CAST(strftime('%w', {col}) AS INTEGER) WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday' WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday' WHEN 6 THEN 'Saturday' END"
            if part == 'day short':
                return "CASE CAST(strftime('%w', {col}) AS INTEGER) WHEN 0 THEN 'Sun' WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue' WHEN 3 THEN 'Wed' WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' WHEN 6 THEN 'Sat' END"
            return col
        return col

    ds: Datasource | None = None
    if payload.datasourceId:
        ds = db.get(Datasource, payload.datasourceId)
    opts = {}
    if ds:
        try:
            opts = json.loads(ds.options_json or "{}")
        except Exception:
            opts = {}
    ds_transforms = None
    if isinstance((opts or {}).get("transforms"), dict):
        ds_tr = (opts or {}).get("transforms") or {}
        all_custom_cols = ds_tr.get('customColumns', [])
        sys.stderr.write(f"[Query] Applying scope filter for source: {payload.source}\n")
        sys.stderr.write(f"[Query] Total custom columns before scope filter: {len(all_custom_cols)}\n")
        for col in all_custom_cols:
            scope = col.get('scope', {})
            sys.stderr.write(f"[Query]   - {col.get('name')}: level={scope.get('level')}, table={scope.get('table')}\n")
        sys.stderr.flush()
        def _apply_scope(model: dict, src: str):
            if not isinstance(model, dict):
                return None
            # Normalize table names for table-level scoping: compare last segment (object name) case-insensitively
            def _matches_table(scope_table: str, source_name: str) -> bool:
                def norm(s: str) -> str:
                    s = (s or '').strip().strip('[]').strip('"').strip('`')
                    parts = s.split('.')
                    return parts[-1].lower()
                return norm(scope_table) == norm(source_name)
            def filt(arr):
                out = []
                for it in (arr or []):
                    sc = (it or {}).get('scope') or {}
                    col_name = (it or {}).get('name', '<unnamed>')
                    lvl = (sc or {}).get('level')
                    if not lvl:
                        out.append(it)
                        sys.stderr.write(f"[Query] Custom column '{col_name}' has no level, including it\n")
                        sys.stderr.flush()
                        continue
                    if str(lvl).lower() == 'datasource':
                        out.append(it)
                        sys.stderr.write(f"[Query] Custom column '{col_name}' is datasource-level, including it\n")
                        sys.stderr.flush()
                    elif str(lvl).lower() == 'table':
                        t = (sc or {}).get('table')
                        if t:
                            matches = _matches_table(str(t), str(src))
                            sys.stderr.write(f"[Query] Custom column '{col_name}' scope table '{t}' vs source '{src}': {'MATCH' if matches else 'NO MATCH'}\n")
                            sys.stderr.flush()
                            if matches:
                                out.append(it)
                    elif str(lvl).lower() == 'widget':
                        try:
                            wid = str((sc or {}).get('widgetId') or '').strip()
                            if wid and getattr(payload, 'widgetId', None) and str(payload.widgetId).strip() == wid:
                                out.append(it)
                        except Exception:
                            pass
                return out
            return {
                'customColumns': filt(ds_tr.get('customColumns')),
                'transforms': filt(ds_tr.get('transforms')),
                'joins': filt(ds_tr.get('joins')),
                'defaults': ds_tr.get('defaults') or {},
            }
        ds_transforms = _apply_scope((opts or {}).get("transforms") or {}, payload.source)
    base_from_sql = f" FROM {_q_source(payload.source)}"
    if ds_transforms:
        def _list_cols_for_agg_base() -> set[str]:
            try:
                eng = _engine_for_datasource(db, payload.datasourceId, actorId)
                with eng.connect() as conn:
                    if (ds_type or '').lower() in ("mssql", "mssql+pymssql", "mssql+pyodbc"):
                        probe = text(f"SELECT TOP 0 * FROM {_q_source(payload.source)} AS s")
                    else:
                        probe = text(f"SELECT * FROM {_q_source(payload.source)} WHERE 1=0")
                    res = conn.execute(probe)
                    return set([str(c) for c in res.keys()])
            except Exception:
                return set()
        __cols = _list_cols_for_agg_base()
        __joins_all = ds_transforms.get('joins', []) if isinstance(ds_transforms, dict) else []
        __joins_eff = []
        for __j in (__joins_all or []):
            try:
                __skey = str((__j or {}).get('sourceKey') or '').strip()
                if __skey and (__skey in __cols or f"[{__skey}]" in __cols or f'"{__skey}"' in __cols):
                    __joins_eff.append(__j)
            except Exception:
                continue
        result = build_sql(
            dialect=ds_type,
            source=_q_source(payload.source),
            base_select=["*"],
            custom_columns=ds_transforms.get("customColumns", []),
            transforms=ds_transforms.get("transforms", []),
            joins=__joins_eff,
            defaults={},
            limit=None,
        )
        # Handle different return value formats (3 or 4 elements)
        if len(result) == 3:
            base_sql, _cols_unused, _warns = result
        elif len(result) == 4:
            base_sql, _cols_unused, _warns, _ = result
        else:
            print(f"[preview_pivot_sql] Unexpected build_sql return count: {len(result)}")
            base_sql = result[0] if result else ""
        base_from_sql = f" FROM ({base_sql}) AS _base"

    # WHERE (same logic as run_pivot)
    where_clauses = []
    params: Dict[str, Any] = {}
    def _pname(base: str, suffix: str = "") -> str:
        core = re.sub(r"[^A-Za-z0-9_]", "_", str(base or ''))
        return f"w_{core}{suffix}"
    if payload.where:
        for k, v in payload.where.items():
            if k in ("start", "startDate", "end", "endDate"):
                continue
            if v is None:
                where_clauses.append(f"{_q_ident(k)} IS NULL")
            elif isinstance(k, str) and "__" in k:
                # Check for operators FIRST before checking if value is array
                base, op = k.split("__", 1)
                opname = None
                if op == "gte": opname = ">="
                elif op == "gt": opname = ">"
                elif op == "lte": opname = "<="
                elif op == "lt": opname = "<"
                if opname:
                    pname = _pname(base, f"_{op}")
                    # Extract first element if value is an array (operators expect scalar values)
                    param_val = v[0] if isinstance(v, (list, tuple)) and len(v) > 0 else v
                    params[pname] = param_val
                    where_clauses.append(f"{_derived_lhs(base)} {opname} :{pname}")
                elif op == "ne":
                    # NOT EQUALS: use NOT IN to support multiple exclusions
                    if isinstance(v, (list, tuple)) and len(v) > 0:
                        pnames = []
                        for i, item in enumerate(v):
                            pname = _pname(base, f"_ne_{i}")
                            params[pname] = item
                            pnames.append(f":{pname}")
                        where_clauses.append(f"{_derived_lhs(base)} NOT IN ({', '.join(pnames)})")
                    else:
                        pname = _pname(base, "_ne")
                        params[pname] = v
                        where_clauses.append(f"{_derived_lhs(base)} != :{pname}")
                elif op == "notcontains":
                    # DOESN'T CONTAIN: use NOT LIKE
                    pname = _pname(base, "_notcontains")
                    params[pname] = f"%{v}%"
                    where_clauses.append(f"{_derived_lhs(base)} NOT LIKE :{pname}")
                elif op in {"contains", "startswith", "endswith"}:
                    # String matching operators
                    pname = _pname(base, f"_{op}")
                    if op == "contains":
                        params[pname] = f"%{v}%"
                    elif op == "startswith":
                        params[pname] = f"{v}%"
                    else:  # endswith
                        params[pname] = f"%{v}"
                    where_clauses.append(f"{_derived_lhs(base)} LIKE :{pname}")
                else:
                    pname = _pname(k)
                    where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                    params[pname] = v
            elif isinstance(v, (list, tuple)):
                pnames = []
                for i, item in enumerate(v):
                    pname = _pname(k, f"_{i}")
                    params[pname] = item
                    pnames.append(f":{pname}")
                where_clauses.append(f"{_derived_lhs(k)} IN ({', '.join(pnames)})")
            else:
                pname = _pname(k)
                where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                params[pname] = v
    
    # Build preliminary where_sql (will be split later based on dimensions)
    all_where_clauses = list(where_clauses)  # Keep a copy

    # Dimensions
    _reserved = {"__metric__", "value"}
    def _clean_dims(arr):
        out = []
        for n in (arr or []):
            try:
                if (str(n or "").strip().lower() in _reserved):
                    continue
            except Exception:
                pass
            out.append(n)
        return out
    r_dims = _clean_dims(payload.rows)
    c_dims = _clean_dims(payload.cols)
    
    # Split WHERE clauses based on dimensions vs non-dimensions (same as run_pivot)
    dimension_names = set()
    for d in (r_dims + c_dims):
        base_name = re.sub(r'\s*\(.*\)$', '', str(d)).strip()
        dimension_names.add(base_name.lower())
        dimension_names.add(d.lower())
    
    dim_where_clauses = []
    non_dim_where_clauses = []
    
    if payload.where:
        for k, v in payload.where.items():
            if k in ("start", "startDate", "end", "endDate"):
                continue
            base_col = k.split("__")[0] if "__" in k else k
            is_dimension = base_col.lower() in dimension_names
            
            matching_clause = None
            for clause in all_where_clauses:
                if _q_ident(base_col) in clause or _derived_lhs(base_col) in clause:
                    matching_clause = clause
                    all_where_clauses.remove(clause)
                    break
            
            if matching_clause:
                if is_dimension:
                    dim_where_clauses.append(matching_clause)
                else:
                    non_dim_where_clauses.append(matching_clause)
    
    # Build final where_sql for outer query (dimension filters only)
    where_sql = f" WHERE {' AND '.join(dim_where_clauses)}" if dim_where_clauses else ""
    
    # If we have non-dimension filters and using _base subquery, inject them
    if non_dim_where_clauses and "_base" in base_from_sql:
        non_dim_where_sql = f" WHERE {' AND '.join(non_dim_where_clauses)}"
        base_from_sql = base_from_sql.replace(") AS _base", f"{non_dim_where_sql}) AS _base")
    
    # Apply groupBy time bucketing to first row dimension if specified
    gb = (getattr(payload, 'groupBy', None) or 'none').lower()
    week_start = (getattr(payload, 'weekStart', None) or 'mon').lower()
    
    # Use original names as aliases (quoted per dialect) so UI can match config fields directly
    r_exprs = []
    for i, n in enumerate(r_dims):
        # Apply groupBy bucketing to first dimension if it's a date field
        if i == 0 and gb in ("day","week","month","quarter","year"):
            # Check if it's a raw date column (not derived)
            is_derived = bool(re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(n), flags=re.IGNORECASE))
            if not is_derived:
                # Apply DATE_TRUNC bucketing
                col_name = n
                if "duckdb" in ds_type or "postgres" in ds_type:
                    if "duckdb" in ds_type:
                        col_ts = f"COALESCE(try_cast({_q_ident(col_name)} AS TIMESTAMP), CAST(try_cast({_q_ident(col_name)} AS DATE) AS TIMESTAMP))"
                    else:
                        col_ts = _q_ident(col_name)
                    if gb == 'week':
                        if week_start == 'sun':
                            expr = f"DATE_TRUNC('week', {col_ts} + INTERVAL '1 day') - INTERVAL '1 day'" if "duckdb" in ds_type else f"date_trunc('week', {col_ts} + interval '1 day') - interval '1 day'"
                        else:
                            expr = f"DATE_TRUNC('week', {col_ts})" if "duckdb" in ds_type else f"date_trunc('week', {col_ts})"
                    else:
                        expr = f"DATE_TRUNC('{gb}', {col_ts})" if "duckdb" in ds_type else f"date_trunc('{gb}', {col_ts})"
                    r_exprs.append((expr, _q_ident(n)))
                    continue
        # Default: use derived_lhs for any derived patterns
        r_exprs.append((_derived_lhs(n), _q_ident(n)))
    
    c_exprs = [(_derived_lhs(n), _q_ident(n)) for i, n in enumerate(c_dims)]

    # Aggregator
    agg = (payload.aggregator or "count").lower()
    val_field = (payload.valueField or "").strip()
    # If Unpivot is present in datasource transforms, FORCE non-count aggregations to target the value column
    unpivot_val_col: str | None = None
    try:
        if isinstance(ds_transforms, dict):
            for _tr in (ds_transforms.get('transforms') or []):
                if str((_tr or {}).get('type') or '').lower() == 'unpivot':
                    unpivot_val_col = str(((_tr or {}).get('valueColumn') or 'value')).strip() or 'value'
                    break
    except Exception:
        unpivot_val_col = None
    if unpivot_val_col and agg in ("sum", "avg", "min", "max", "distinct"):
        val_field = unpivot_val_col
    if agg == 'distinct' and not val_field:
        agg = 'count'
    if agg in ("sum", "avg", "min", "max") and not val_field:
        agg = 'count'
    if agg == 'count':
        value_expr = "COUNT(*)"
    elif agg == 'distinct':
        value_expr = f"COUNT(DISTINCT {_q_ident(val_field)})"
    else:
        # For DuckDB, clean numeric strings robustly before SUM/AVG/MIN/MAX
        if ((ds_type or '').find('duckdb') >= 0) and (agg in ("sum", "avg", "min", "max")):
            y_clean = (
                f"COALESCE("
                f"try_cast(regexp_replace(CAST({_q_ident(val_field)} AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), "
                f"try_cast({_q_ident(val_field)} AS DOUBLE), 0.0)"
            )
            value_expr = f"{agg.upper()}({y_clean})"
        else:
            value_expr = f"{agg.upper()}({_q_ident(val_field)})"

    # Build SELECT parts, avoiding self-aliasing (same logic as run_pivot)
    sel_parts = []
    for e, a in (r_exprs + c_exprs):
        e_norm = str(e).strip().strip('"').strip('[').strip(']').strip('`')
        a_norm = str(a).strip().strip('"').strip('[').strip(']').strip('`')
        if e_norm.lower() == a_norm.lower():
            sel_parts.append(e)
        else:
            sel_parts.append(f"{e} AS {a}")
    sel = ", ".join(sel_parts + [f"{value_expr} AS value"]) or f"{value_expr} AS value"
    # Use ordinals for DuckDB/Postgres/MySQL/SQLite; use expressions for SQL Server
    dim_count = len(r_exprs) + len(c_exprs)
    if dim_count > 0:
        if 'mssql' in (ds_type or '') or 'sqlserver' in (ds_type or ''):
            group_by_exprs = ", ".join([e for e, _ in (r_exprs + c_exprs)])
            gb_sql = f" GROUP BY {group_by_exprs}"
            order_by = f" ORDER BY {group_by_exprs}"
        else:
            ordinals = ", ".join(str(i) for i in range(1, dim_count + 1))
            gb_sql = f" GROUP BY {ordinals}"
            order_by = f" ORDER BY {ordinals}"
    else:
        gb_sql = ""
        order_by = ""
    inner = f"SELECT {sel}{base_from_sql}{where_sql}{gb_sql}{order_by}"
    return {"sql": inner}

@router.post("", response_model=QueryResponse)
def run_query(payload: QueryRequest, db: Session = Depends(get_db), actorId: Optional[str] = None, publicId: Optional[str] = None, token: Optional[str] = None) -> QueryResponse:
    try:
        touch_actor(actorId)
    except Exception:
        pass
    if isinstance(publicId, str) and publicId:
        sl = get_share_link_by_public(db, publicId)
        if not sl:
            raise HTTPException(status_code=404, detail="Not found")
        if not verify_share_link_token(sl, token if isinstance(token, str) else None, settings.secret_key):
            raise HTTPException(status_code=401, detail="Unauthorized")
    # Prefer local DuckDB when enabled and local table exists (tri-state preferLocalDuck)
    try:
        _p = getattr(payload, 'preferLocalDuck', None)
        _prefer_local = True if _p is True else (False if _p is False else bool(settings.prefer_local_duckdb))
    except Exception:
        _prefer_local = bool(settings.prefer_local_duckdb)
    tbl = None
    try:
        tbl = (getattr(payload, 'preferLocalTable', None) or '').strip() or None
    except Exception:
        tbl = None
    if not tbl:
        try:
            m = re.search(r"\bfrom\s+([A-Za-z0-9_\.\[\]`\"]+)", str(payload.sql or ''), flags=re.IGNORECASE)
            if m:
                raw = m.group(1)
                tbl = (raw.split()[0] if raw else None)
        except Exception:
            tbl = None
    # Detect datasource type to route native DuckDB for DuckDB datasources
    ds_obj = None
    ds_type_lower = ""
    try:
        if payload.datasourceId:
            ds_obj = db.get(Datasource, payload.datasourceId)
            ds_type_lower = (ds_obj.type or "").lower() if ds_obj else ""
    except Exception:
        ds_obj = None
        ds_type_lower = ""
    route_duck = (payload.datasourceId is None) or (ds_type_lower == "duckdb") or (_prefer_local and _duck_has_table(tbl))
    if actorId:
        _ra = _throttle_take(actorId)
        if _ra:
            try:
                counter_inc("query_rate_limited_total", {"endpoint": "query"})
            except Exception:
                pass
            raise HTTPException(status_code=429, detail="Rate limit exceeded", headers={"Retry-After": str(_ra)})

    try:
        counter_inc("query_requests_total", {"endpoint": "query"})
    except Exception:
        pass

    # Always enforce a LIMIT/OFFSET via subquery wrapping.
    # Important: some drivers (e.g., DuckDB via duckdb-engine) are unreliable with
    # bound parameters in LIMIT/OFFSET. Inline safe integer literals instead.
    sql_inner = payload.sql
    limit_lit = int(payload.limit or 1000)
    offset_lit = int(payload.offset or 0)
    # Clamp limit to a safe maximum
    try:
        _max_lim_env = int(os.environ.get("QUERY_MAX_LIMIT", "10000") or "10000")
    except Exception:
        _max_lim_env = 10000
    if limit_lit > _max_lim_env:
        limit_lit = _max_lim_env

    __heavy = bool(limit_lit >= 5000 or bool(payload.includeTotal))

    # Collect named params referenced in the inner SQL
    name_order = [m.group(1) for m in re.finditer(r":([A-Za-z_][A-Za-z0-9_]*)", sql_inner)]
    name_set = set(name_order)

    # Build params for data query (exclude pagination since we inlined them)
    params: Dict[str, Any] = {}
    if payload.params:
        for k, v in payload.params.items():
            if k in name_set:
                params[k] = _coerce_date_like(v)

    # Branch: Use native duckdb for DuckDB engines to avoid result processor issues
    start = time.perf_counter()
    if route_duck and _duckdb is not None:
        try:
            gauge_inc("query_inflight", 1.0, {"endpoint": "query", "engine": "duckdb"})
        except Exception:
            pass
        __duck_acq = False
        __actor_acq = False
        __as = _actor_sem(actorId)
        if __heavy:
            _t0 = time.perf_counter()
            _HEAVY_SEM.acquire()
            try:
                summary_observe("query_semaphore_wait_ms", int((time.perf_counter() - _t0) * 1000), {"endpoint": "query", "engine": "duckdb"})
            except Exception:
                pass
            __duck_acq = True
            if __as:
                try:
                    _t1 = time.perf_counter()
                    __as.acquire()
                    try:
                        summary_observe("query_semaphore_wait_ms", int((time.perf_counter() - _t1) * 1000), {"endpoint": "query", "engine": "duckdb", "sem": "actor"})
                    except Exception:
                        pass
                    __actor_acq = True
                except Exception:
                    pass
        try:
            # Resolve DB file path
            if payload.datasourceId is None:
                try:
                    db_path = get_active_duck_path()
                except Exception:
                    db_path = settings.duckdb_path
            else:
                # If this is a DuckDB datasource with a connection URI, try to extract the file path; else default to local
                if ds_obj and getattr(ds_obj, "connection_encrypted", None):
                    try:
                        dsn = decrypt_text(ds_obj.connection_encrypted)
                        p = urlparse(dsn) if dsn else None
                        try:
                            db_path = get_active_duck_path()
                        except Exception:
                            db_path = settings.duckdb_path
                        if p and (p.scheme or "").startswith("duckdb"):
                            _p = unquote(p.path or "")
                            if _p.startswith("///"):
                                _p = _p[2:]
                            try:
                                db_path = _p or get_active_duck_path()
                            except Exception:
                                db_path = _p or settings.duckdb_path
                            # Handle malformed 'duckdb:///.data/...' by treating '/.data' as cwd-relative
                            if db_path and db_path != ":memory:" and db_path.startswith("/."):
                                try:
                                    db_path = os.path.abspath(db_path[1:])
                                except Exception:
                                    pass
                            if ":memory:" in (dsn or "").lower():
                                db_path = ":memory:"
                    except Exception:
                        try:
                            db_path = get_active_duck_path()
                        except Exception:
                            db_path = settings.duckdb_path
                else:
                    try:
                        db_path = get_active_duck_path()
                    except Exception:
                        db_path = settings.duckdb_path

            try:
                sys.stderr.write(f"[DEBUG] DuckDB native db_path={db_path}\n")
                sys.stderr.flush()
            except Exception:
                pass

            cache_ds = f"{payload.datasourceId or '__local__'}@{db_path}"

            # Replace named params in the inner SQL with positional '?' for duckdb
            inner_qm = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", "?", sql_inner)
            sql_native = f"SELECT * FROM ({inner_qm}) AS _q LIMIT {limit_lit} OFFSET {offset_lit}"
            # Build positional values list in order of occurrence
            values = [params.get(nm) for nm in name_order]

            # Cache lookup for data
            key = _cache_key("sql", cache_ds, sql_inner, params)
            cached = _cache_get(key)
            if cached:
                cols, rows = cached
                try:
                    counter_inc("query_cache_hit_total", {"endpoint": "query", "kind": "data"})
                except Exception:
                    pass
            else:
                try:
                    counter_inc("query_cache_miss_total", {"endpoint": "query", "kind": "data"})
                except Exception:
                    pass
                with open_duck_native(db_path) as conn:
                    cur = conn.execute(sql_native, values)
                    desc = getattr(cur, 'description', None) or []
                    cols = [str(col[0]) for col in desc]
                    rows = []
                    try:
                        batch_size = int(os.environ.get("DUCKDB_FETCHMANY", "1000") or "1000")
                    except Exception:
                        batch_size = 1000
                    if batch_size <= 0:
                        batch_size = 1000
                    while True:
                        chunk = cur.fetchmany(batch_size)
                        if not chunk:
                            break
                        for r in chunk:
                            rows.append([_json_safe_cell(x) for x in r])
                _cache_set(key, cols, rows)

            total_rows = None
            if payload.includeTotal:
                cnt_key = _cache_key("count", cache_ds, sql_inner, params)
                cached_cnt = _cache_get(cnt_key)
                if cached_cnt:
                    cnt_rows = cached_cnt[1]
                    try:
                        total_rows = int(cnt_rows[0][0]) if cnt_rows and cnt_rows[0] else 0
                    except Exception:
                        total_rows = None
                    try:
                        counter_inc("query_cache_hit_total", {"endpoint": "query", "kind": "count"})
                    except Exception:
                        pass
                else:
                    try:
                        counter_inc("query_cache_miss_total", {"endpoint": "query", "kind": "count"})
                    except Exception:
                        pass
                    count_text_qm = f"SELECT COUNT(*) AS __cnt FROM ({inner_qm}) AS _q"
                    with open_duck_native(db_path) as conn:
                        cur = conn.execute(count_text_qm, values)
                        cnt_val = cur.fetchone()
                    total_rows = int(cnt_val[0]) if cnt_val and cnt_val[0] is not None else 0
                    _cache_set(cnt_key, ["__cnt"], [[total_rows]])

            elapsed = int((time.perf_counter() - start) * 1000)
            try:
                summary_observe("query_duration_ms", elapsed, {"endpoint": "query", "engine": "duckdb"})
            except Exception:
                pass
            return QueryResponse(columns=cols, rows=rows, elapsedMs=elapsed, totalRows=total_rows)
        finally:
            try:
                gauge_dec("query_inflight", 1.0, {"endpoint": "query", "engine": "duckdb"})
            except Exception:
                pass
            if __duck_acq:
                _HEAVY_SEM.release()
            if __actor_acq and __as:
                try:
                    __as.release()
                except Exception:
                    pass

    # Default path: SQLAlchemy for non-DuckDB engines; attempt with optional engine refresh on HYT00
    # Fallback / default path: use SQLAlchemy for non-DuckDB engines
    __sql_acq = False
    __actor_acq2 = False
    __as = _actor_sem(actorId)
    try:
        gauge_inc("query_inflight", 1.0, {"endpoint": "query", "engine": "sqlalchemy"})
    except Exception:
        pass
    if __heavy:
        _t2 = time.perf_counter()
        _HEAVY_SEM.acquire()
        try:
            summary_observe("query_semaphore_wait_ms", int((time.perf_counter() - _t2) * 1000), {"endpoint": "query", "engine": "sqlalchemy"})
        except Exception:
            pass
        __sql_acq = True
        if __as:
            try:
                _t3 = time.perf_counter()
                __as.acquire()
                try:
                    summary_observe("query_semaphore_wait_ms", int((time.perf_counter() - _t3) * 1000), {"endpoint": "query", "engine": "sqlalchemy", "sem": "actor"})
                except Exception:
                    pass
                __actor_acq2 = True
            except Exception:
                pass
    try:
        engine = _engine_for_datasource(db, payload.datasourceId, actorId)
        last_err = None
        for _attempt in range(2):
            # Refresh engine per attempt in case we disposed it in the previous iteration
            try:
                engine = _engine_for_datasource(db, payload.datasourceId, actorId)
            except Exception as _e:
                last_err = _e
                if _attempt == 0:
                    try:
                        time.sleep(0.2)  # brief pause
                    except Exception:
                        pass
                    continue
                raise
            # Dialect-specific pagination
            is_mssql = False
            try:
                is_mssql = (engine.dialect.name or "").lower() in ("mssql", "mssql+pymssql", "mssql+pyodbc")
            except Exception:
                is_mssql = False
            is_mysql = False
            is_pg = False
            try:
                _dn = (engine.dialect.name or "").lower()
                is_mysql = ("mysql" in _dn) or ("mariadb" in _dn)
                is_pg = ("postgres" in _dn)
            except Exception:
                is_mysql = False
                is_pg = False

            # For SQL Server: provide a safe ORDER BY for OFFSET/FETCH.
            if is_mssql:
                inner_str = (sql_inner or "").strip().rstrip(";").strip()
                m = re.search(r"\sorder\s+by\s+(.+)$", inner_str, flags=re.IGNORECASE)
                if m:
                    # Preserve caller order (strip from inner, apply outer)
                    order_by = m.group(1).strip()
                    sql_base = inner_str[: m.start()].rstrip()
                    outer = (f" ORDER BY {order_by}" if order_by else " ORDER BY 1")
                    sql_text = text(
                        f"SELECT * FROM ({sql_base}) AS _q{outer} OFFSET {offset_lit} ROWS FETCH NEXT {limit_lit} ROWS ONLY"
                    )
                else:
                    # No ORDER BY: wrap with ROW_NUMBER() to ensure a named column for ORDER BY
                    # This avoids MSSQL error 8155 when inner columns lack aliases (e.g., COUNT(*))
                    safe_inner = inner_str
                    try:
                        safe_inner = re.sub(r"(?is)^(\s*select\s+)(count\s*\(\s*\*\s*\))(\s*)(from\b)", r"\1\2 AS __cnt \4", inner_str, count=1)
                    except Exception:
                        safe_inner = inner_str
                    sql_text = text(
                        f"SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS __rn, * FROM ({safe_inner}) AS _x) AS _q "
                        f"ORDER BY __rn OFFSET {offset_lit} ROWS FETCH NEXT {limit_lit} ROWS ONLY"
                    )
            else:
                sql_text = text(f"SELECT * FROM ({sql_inner}) AS _q LIMIT {limit_lit} OFFSET {offset_lit}")
            try:
                with engine.connect() as conn:
                    # Cache lookup for data
                    key = _cache_key("sql", payload.datasourceId, sql_inner, params)
                    cached = _cache_get(key)
                    if cached:
                        cols, rows = cached
                        try:
                            counter_inc("query_cache_hit_total", {"endpoint": "query", "kind": "data"})
                        except Exception:
                            pass
                    else:
                        try:
                            counter_inc("query_cache_miss_total", {"endpoint": "query", "kind": "data"})
                        except Exception:
                            pass
                        try:
                            if is_pg:
                                conn.execute(text("SET statement_timeout = 120000"))
                            elif is_mysql:
                                conn.execute(text("SET SESSION MAX_EXECUTION_TIME=120000"))
                            elif is_mssql:
                                conn.execute(text("SET LOCK_TIMEOUT 120000"))
                        except Exception:
                            pass
                        result = conn.execution_options(stream_results=True).execute(sql_text, params)
                        raw_rows = result.fetchall()
                        cols = list(result.keys())
                        rows = [[_json_safe_cell(x) for x in r] for r in raw_rows]
                        _cache_set(key, cols, rows)

                    total_rows = None
                    if payload.includeTotal:
                        count_key = _cache_key("count", payload.datasourceId, sql_inner, params)
                        cached_cnt = _cache_get(count_key)
                        if cached_cnt:
                            cnt_rows = cached_cnt[1]
                            try:
                                total_rows = int(cnt_rows[0][0]) if cnt_rows and cnt_rows[0] else 0
                            except Exception:
                                total_rows = None
                            try:
                                counter_inc("query_cache_hit_total", {"endpoint": "query", "kind": "count"})
                            except Exception:
                                pass
                        else:
                            try:
                                counter_inc("query_cache_miss_total", {"endpoint": "query", "kind": "count"})
                            except Exception:
                                pass
                            if cached:
                                try:
                                    if is_pg:
                                        conn.execute(text("SET statement_timeout = 30000"))
                                    elif is_mysql:
                                        conn.execute(text("SET SESSION MAX_EXECUTION_TIME=30000"))
                                    elif is_mssql:
                                        conn.execute(text("SET LOCK_TIMEOUT 30000"))
                                except Exception:
                                    pass
                            if is_mssql:
                                count_text = text(f"SELECT COUNT(*) AS __cnt FROM ({sql_inner}) AS _q")
                            else:
                                count_text = text(f"SELECT COUNT(*) AS __cnt FROM ({sql_inner}) AS _q")
                            cnt_res = conn.execute(count_text, params)
                            cnt_val = cnt_res.scalar_one_or_none()
                            total_rows = int(cnt_val) if cnt_val is not None else 0
                            _cache_set(count_key, ["__cnt"], [[total_rows]])
                last_err = None
                break
            except Exception as _e:
                last_err = _e
                if _attempt == 0:
                    # If HYT00 login timeout or pyodbc level login error, dispose engine pool and retry once
                    msg = str(_e)
                    hy = ("HYT00" in msg) or ("Login timeout" in msg) or ("SQLDriverConnect" in msg)
                    if hy:
                        try:
                            from ..db import dispose_engine
                            dispose_engine(engine)
                        except Exception:
                            pass
                    try:
                        time.sleep(0.5)
                    except Exception:
                        pass
                else:
                    mapped = _http_for_db_error(_e)
                    if mapped:
                        raise mapped
                    raise

        elapsed = int((time.perf_counter() - start) * 1000)
        try:
            summary_observe("query_duration_ms", elapsed, {"endpoint": "query", "engine": "sqlalchemy"})
        except Exception:
            pass
        return QueryResponse(columns=cols, rows=rows, elapsedMs=elapsed, totalRows=total_rows)
    finally:
        try:
            gauge_dec("query_inflight", 1.0, {"endpoint": "query", "engine": "sqlalchemy"})
        except Exception:
            pass
        if __sql_acq:
            _HEAVY_SEM.release()
        if __actor_acq2 and __as:
            try:
                __as.release()
            except Exception:
                pass


@router.post("/spec", response_model=QueryResponse)
def run_query_spec(payload: QuerySpecRequest, db: Session = Depends(get_db), actorId: Optional[str] = None, publicId: Optional[str] = None, token: Optional[str] = None) -> QueryResponse:
    """Compile a QuerySpec to SQL and execute via the standard path.

    - DuckDB: use Ibis to compile.
    - Other engines: build a generic SQL SELECT with WHERE and delegate to /query.
    """
    # Debug: Log WHERE clause and incoming X at the start
    import sys
    try:
        sys.stderr.write(
            f"[SPEC_DEBUG] run_query_spec called, source={payload.spec.source}, "
            f"where keys={list(payload.spec.where.keys()) if hasattr(payload.spec, 'where') and payload.spec.where else 'None'}, "
            f"x={getattr(payload.spec, 'x', None)} (type={type(getattr(payload.spec, 'x', None))})\n"
        )
        sys.stderr.flush()
    except Exception:
        pass
    
    # Save __weekends config before _resolve_date_presets strips UI meta keys (needed for avg_wday)
    _spec_weekends = 'SAT_SUN'
    try:
        _ow = (payload.spec.where or {}) if (hasattr(payload, 'spec') and payload.spec) else {}
        _sw = str(_ow.get('__weekends', os.environ.get('WEEKENDS', 'SAT_SUN'))).upper().strip()
        _spec_weekends = _sw if _sw in ('SAT_SUN', 'FRI_SAT') else 'SAT_SUN'
    except Exception:
        pass

    # Resolve date presets (e.g. __date_preset: "today") to concrete __gte/__lt at execution time
    if hasattr(payload, 'spec') and payload.spec and getattr(payload.spec, 'where', None):
        payload.spec.where = _resolve_date_presets(payload.spec.where)
    
    if actorId:
        _ra = _throttle_take(actorId)
        if _ra:
            raise HTTPException(status_code=429, detail="Rate limit exceeded", headers={"Retry-After": str(_ra)})
    # Determine backend
    is_duckdb = payload.datasourceId is None
    ds = None
    if not is_duckdb:
        ds = db.get(Datasource, payload.datasourceId)
        if not ds:
            raise HTTPException(status_code=404, detail="Datasource not found")
        is_duckdb = (ds.type or "").lower() == "duckdb"
        # Enforce access if actor is present
        if actorId:
            u = db.get(User, str(actorId).strip())
            is_admin = bool(u and (u.role or "user").lower() == "admin")
            if not is_admin and (ds.user_id or "").strip() != str(actorId).strip():
                s = db.query(DatasourceShare).filter(DatasourceShare.datasource_id == ds.id, DatasourceShare.user_id == str(actorId).strip()).first()
                if not s:
                    raise HTTPException(status_code=403, detail="Not allowed to query this datasource")
    else:
        # Auto-detect local DuckDB datasource to load transforms/custom columns
        import sys
        base_source_raw = payload.spec.source or ""
        if base_source_raw:
            # Look for a DuckDB datasource that matches the local store
            from sqlalchemy import select
            stmt = select(Datasource).where(Datasource.type == "duckdb")
            if actorId:
                # Filter by actorId or admin
                u = db.get(User, str(actorId).strip())
                is_admin = bool(u and (u.role or "user").lower() == "admin")
                if not is_admin:
                    stmt = stmt.where(Datasource.user_id == str(actorId).strip())
            # Note: If actorId is None, allow any DuckDB datasource (local store is shared)
            local_ds_candidates = list(db.execute(stmt).scalars())
            # Prefer datasource with no connection URI (the default local one) or one matching settings.duckdb_path
            for candidate in local_ds_candidates:
                if not candidate.connection_encrypted:
                    ds = candidate
                    break
                try:
                    dsn = decrypt_text(candidate.connection_encrypted or "")
                    if dsn and settings.duckdb_path in dsn:
                        ds = candidate
                        break
                except Exception:
                    continue
            # Fallback: use first candidate if any
            if not ds and local_ds_candidates:
                ds = local_ds_candidates[0]
            if not ds:
                # No datasource found - log warning but continue (transforms won't be applied)
                pass

    # Global or per-request preference to route to local DuckDB when base table exists (tri-state)
    if getattr(payload, 'preferLocalDuck', None) is True:
        prefer_local = True
    elif getattr(payload, 'preferLocalDuck', None) is False:
        prefer_local = False
    else:
        prefer_local = bool(settings.prefer_local_duckdb)
    base_source = None
    try:
        base_source = (payload.spec.source or None)
    except Exception:
        base_source = None
    if prefer_local and _duck_has_table(base_source):
        # Force execution through local DuckDB by delegating to /query with datasourceId=None
        try:
            sql_delegate = None
            # Build a simple SELECT * wrapper with WHERE derived from spec (non-agg path handled below)
            # For full semantics, continue normal compilation paths but override to Duck when calling run_query later.
        except Exception:
            pass

    # Compute a normalized datasource type string for dialect decisions
    ds_type = ((ds.type or "") if ds else ("duckdb" if is_duckdb else "")).lower()

    lim = payload.spec.limit if payload.spec.limit is not None else payload.limit
    off = payload.spec.offset if payload.spec.offset is not None else payload.offset
    
    # Resolve table ID to current name (supports table renaming)
    source_table_id = getattr(payload.spec, 'sourceTableId', None)
    source_name_original = payload.spec.source
    resolved_source = _resolve_table_name(ds, source_table_id, source_name_original)
    
    # Use resolved source for all subsequent operations
    if resolved_source and resolved_source != source_name_original:
        # Override the spec source with resolved name
        payload.spec.source = resolved_source
        logger.info(f"[TableID] Using resolved table name: {resolved_source} (was: {source_name_original})")
    
    # Validate required fields
    if not (payload.spec.source and str(payload.spec.source).strip()):
        raise HTTPException(status_code=400, detail="spec.source is required for /query/spec")

    # Helpers available to all branches
    def _q_ident(name: str) -> str:
        s = str(name or '').strip('\n\r\t')
        if not s:
            return s
        if s.startswith('[') and s.endswith(']'):
            return s
        if s.startswith('"') and s.endswith('"'):
            return s
        if s.startswith('`') and s.endswith('`'):
            return s
        d = (ds_type or '').lower()
        if 'mssql' in d or 'sqlserver' in d:
            return f"[{s}]"
        if 'mysql' in d:
            return f"`{s}`"
        return f'"{s}"'
    def _q_source(name: str) -> str:
        s = str(name or '').strip()
        if not s:
            return s
        d = (ds_type or '').lower()
        if 'mssql' in d or 'sqlserver' in d:
            parts = s.split('.')
            return '.'.join([p if (p.startswith('[') and p.endswith(']')) else f"[{p}]" for p in parts])
        if 'mysql' in d:
            parts = s.split('.')
            return '.'.join([p if (p.startswith('`') and p.endswith('`')) else f"`{p}`" for p in parts])
        parts = s.split('.')
        return '.'.join([p if ((p.startswith('"') and p.endswith('"')) or (p.startswith('[') and p.endswith(']')) or (p.startswith('`') and p.endswith('`'))) else f'"{p}"' for p in parts])
    def _pname(base: str, suffix: str = "") -> str:
        core = re.sub(r"[^A-Za-z0-9_]", "_", str(base or ''))
        return f"w_{core}{suffix}"
    def _derived_lhs(name: str) -> str:
        """If name matches "Base (Part)", return dialect-specific expr; else return quoted ident.
        Parts: Year, Quarter, Month, Month Name, Month Short, Week, Day, Day Name, Day Short."""
        raw = str(name or '').strip()
        if raw.startswith('"(') and raw.endswith(')"'):
            raw = raw[1:-1]
        if raw.startswith('('):
            return raw
        m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", raw, flags=re.IGNORECASE)
        if not m:
            return _q_ident(raw)
        base = m.group(1).strip()
        part = m.group(2).strip().lower()
        col = _q_ident(base)
        d = (ds_type or '').lower()
        if 'mssql' in d or 'sqlserver' in d:
            if part == 'year': return f"YEAR({col})"
            if part == 'quarter': return f"DATEPART(quarter, {col})"
            if part == 'month': return f"MONTH({col})"
            if part == 'month name': return f"DATENAME(month, {col})"
            if part == 'month short': return f"LEFT(DATENAME(month, {col}), 3)"
            if part == 'week': return f"DATEPART(iso_week, {col})"
            if part == 'day': return f"DAY({col})"
            if part == 'day name': return f"DATENAME(weekday, {col})"
            if part == 'day short': return f"LEFT(DATENAME(weekday, {col}), 3)"
            return col
        if 'duckdb' in d or 'postgres' in d or 'postgre' in d:
            if part == 'year': return f"EXTRACT(year FROM {col})"
            if part == 'quarter': return f"EXTRACT(quarter FROM {col})"
            if part == 'month': return f"EXTRACT(month FROM {col})"
            if part == 'month name': return f"to_char({col}, 'FMMonth')"
            if part == 'month short': return f"to_char({col}, 'Mon')"
            if part == 'week': return f"EXTRACT(week FROM {col})"
            if part == 'day': return f"EXTRACT(day FROM {col})"
            if part == 'day name': return f"to_char({col}, 'FMDay')"
            if part == 'day short': return f"to_char({col}, 'Dy')"
            return col
        if 'mysql' in d:
            if part == 'year': return f"YEAR({col})"
            if part == 'quarter': return f"QUARTER({col})"
            if part == 'month': return f"MONTH({col})"
            if part == 'month name': return f"DATE_FORMAT({col}, '%M')"
            if part == 'month short': return f"DATE_FORMAT({col}, '%b')"
            if part == 'week': return f"WEEK({col}, 3)"
            if part == 'day': return f"DAY({col})"
            if part == 'day name': return f"DATE_FORMAT({col}, '%W')"
            if part == 'day short': return f"DATE_FORMAT({col}, '%a')"
            return col
        if 'sqlite' in d:
            if part == 'year': return f"CAST(strftime('%Y', {col}) AS INTEGER)"
            if part == 'quarter':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 "
                    f"WHEN 4 THEN 2 WHEN 5 THEN 2 WHEN 6 THEN 2 "
                    f"WHEN 7 THEN 3 WHEN 8 THEN 3 WHEN 9 THEN 3 "
                    f"ELSE 4 END"
                )
            if part == 'month': return f"CAST(strftime('%m', {col}) AS INTEGER)"
            if part == 'month name':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April' "
                    f"WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' "
                    f"WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December' END"
                )
            if part == 'month short':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr' "
                    f"WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' "
                    f"WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec' END"
                )
            if part == 'week': return f"CAST(strftime('%W', {col}) AS INTEGER)"
            if part == 'day': return f"CAST(strftime('%d', {col}) AS INTEGER)"
            if part == 'day name':
                return (
                    f"CASE strftime('%w', {col}) "
                    f"WHEN '0' THEN 'Sunday' WHEN '1' THEN 'Monday' WHEN '2' THEN 'Tuesday' WHEN '3' THEN 'Wednesday' "
                    f"WHEN '4' THEN 'Thursday' WHEN '5' THEN 'Friday' WHEN '6' THEN 'Saturday' END"
                )
            if part == 'day short':
                return (
                    f"CASE strftime('%w', {col}) "
                    f"WHEN '0' THEN 'Sun' WHEN '1' THEN 'Mon' WHEN '2' THEN 'Tue' WHEN '3' THEN 'Wed' "
                    f"WHEN '4' THEN 'Thu' WHEN '5' THEN 'Fri' WHEN '6' THEN 'Sat' END"
                )
            return col
        return col

    # Helper: build expression map from datasource transforms
    def _build_expr_map(ds: Any, source_name: str, ds_type: str) -> dict:
        """Build mapping of derived column names to SQL expressions"""
        expr_map = {}
        
        if not ds:
            return expr_map
        
        try:
            raw_json = ds.options_json or "{}"
            opts = json.loads(raw_json)
            raw_transforms = opts.get("transforms") or {}
            
            # Debug: Show custom columns before scope filtering
            import sys
            raw_custom_cols = (raw_transforms.get("customColumns") or [])
            sys.stderr.write(f"[WHERE_DEBUG] source_name='{source_name}', raw custom columns count: {len(raw_custom_cols)}\n")
            sys.stderr.write(f"[WHERE_DEBUG] raw custom column names: {[c.get('name') for c in raw_custom_cols if isinstance(c, dict)]}\n")
            sys.stderr.flush()
            
            ds_transforms = _apply_scope(raw_transforms, source_name)
            
            # Debug: Show custom columns after scope filtering
            filtered_custom_cols = ds_transforms.get("customColumns") or []
            sys.stderr.write(f"[WHERE_DEBUG] After scope filtering, custom columns count: {len(filtered_custom_cols)}\n")
            sys.stderr.write(f"[WHERE_DEBUG] filtered custom column names: {[c.get('name') for c in filtered_custom_cols if isinstance(c, dict)]}\n")
            sys.stderr.flush()
            
            # From customColumns
            custom_cols = ds_transforms.get("customColumns") or []
            for col in custom_cols:
                if isinstance(col, dict) and col.get("name") and col.get("expr"):
                    # Normalize bracket identifiers for target dialect
                    from ..sqlgen import _normalize_expr_idents
                    expr = _normalize_expr_idents(ds_type, col["expr"])
                    expr_map[col["name"]] = expr
            
            # From computed transforms
            transforms = ds_transforms.get("transforms") or []
            sys.stderr.write(f"[WHERE_DEBUG] Processing {len(transforms)} transforms\n")
            sys.stderr.flush()
            for t in transforms:
                if isinstance(t, dict):
                    t_type = t.get("type")
                    t_name = t.get("target") or t.get("name")  # case uses "target", computed uses "name"
                    
                    if t_type == "computed" and t.get("expr"):
                        # Handle computed transforms
                        from ..sqlgen import _normalize_expr_idents
                        expr = _normalize_expr_idents(ds_type, t["expr"])
                        if t_name:
                            expr_map[t_name] = expr
                            sys.stderr.write(f"[WHERE_DEBUG] Added computed transform '{t_name}'\n")
                            sys.stderr.flush()
                    elif t_type == "case":
                        # Handle case transforms
                        sys.stderr.write(f"[WHERE_DEBUG] Building case expression for '{t_name}'\n")
                        sys.stderr.flush()
                        case_expr = _build_case_expression(t)
                        if case_expr and t_name:
                            expr_map[t_name] = case_expr
                            sys.stderr.write(f"[WHERE_DEBUG] Added case transform '{t_name}': {case_expr[:80]}...\n")
                            sys.stderr.flush()
                        else:
                            sys.stderr.write(f"[WHERE_DEBUG] Failed to build case expression for '{t_name}'\n")
                            sys.stderr.flush()
        
        except Exception as e:
            logger.error(f"[SQLGlot] Failed to build expr_map: {e}")
        
        return expr_map
    
    # Helper: resolve derived columns in WHERE clause
    def _resolve_derived_columns_in_where(where: dict, ds: Any, source_name: str, ds_type: str) -> dict:
        """Resolve derived column names to SQL expressions in WHERE clause"""
        import sys
        sys.stderr.write(f"[SQLGlot] _resolve_derived_columns_in_where CALLED with where keys: {list(where.keys()) if where else 'None'}\n")
        sys.stderr.flush()
        
        if not where:
            return where
        
        if not ds:
            sys.stderr.write("[SQLGlot] No datasource provided for resolution\n")
            sys.stderr.flush()
            return where
        
        try:
            # Build expr_map from datasource using the local helper function
            expr_map = _build_expr_map(ds, source_name, ds_type)
            
            # Resolve WHERE clause
            sys.stderr.write(f"[SQLGlot] Built expr_map with {len(expr_map)} entries: {list(expr_map.keys())}\n")
            sys.stderr.write(f"[SQLGlot] WHERE keys to resolve: {list(where.keys())}\n")
            sys.stderr.flush()
            
            resolved = {}
            resolved_count = 0
            for key, value in where.items():
                # Extract base column name (remove operators like __ne, __gte, __in)
                base_key = key.split("__")[0] if "__" in key else key
                op_suffix = key.split("__", 1)[1] if "__" in key else None
                
                # First check if it's a custom column
                if base_key in expr_map:
                    expr = expr_map[base_key]
                    # Strip table aliases - handle both quoted and unquoted (e.g., s.ClientID or "s"."ClientID" -> ClientID)
                    expr = re.sub(r'"[a-z][a-z_]{0,4}"\.', '', expr)  # Quoted aliases like "s".
                    expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)  # Unquoted aliases like s.
                    
                    # Recursively expand nested transform references (e.g., Brinks references SourceRegion/DestRegion)
                    def _expand_nested_transforms(expr_str: str, depth: int = 0) -> str:
                        if depth > 10:
                            sys.stderr.write(f"[SQLGlot] WARNING: Max recursion depth reached in nested transform expansion\n")
                            sys.stderr.flush()
                            return expr_str
                        expanded = expr_str
                        # Find all quoted identifiers in the expression
                        matches = re.findall(r'"([^"]+)"', expr_str)
                        changed = False
                        for match in matches:
                            if match in expr_map:
                                # Get the nested transform's expression
                                nested_expr = expr_map[match]
                                # Strip table aliases from nested expression
                                nested_expr = re.sub(r'"[a-z][a-z_]{0,4}"\.', '', nested_expr)
                                nested_expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', nested_expr)
                                # Replace the reference with the nested expression
                                expanded = expanded.replace(f'"{match}"', f'({nested_expr})')
                                changed = True
                                sys.stderr.write(f"[SQLGlot] Expanded nested transform '{match}' in WHERE clause\n")
                                sys.stderr.flush()
                        # Recurse if we made changes
                        if changed:
                            return _expand_nested_transforms(expanded, depth + 1)
                        return expanded
                    
                    expr = _expand_nested_transforms(expr)
                    
                    # Rebuild key with operator suffix if present
                    resolved_key = f"({expr})" if not op_suffix else f"({expr})__{op_suffix}"
                    sys.stderr.write(f"[SQLGlot] [OK] Resolved custom column '{key}' -> {resolved_key[:80]}...\n")
                    sys.stderr.flush()
                    resolved[resolved_key] = value
                    resolved_count += 1
                # Check if it's a date part pattern like "OrderDate (Year)"
                elif " (" in base_key and ")" in base_key:
                    match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", base_key, flags=re.IGNORECASE)
                    if match:
                        base_col = match.group(1).strip()
                        kind = match.group(2).lower()
                        expr = _build_datepart_expr(base_col, kind, ds_type)
                        # Rebuild key with operator suffix if present
                        resolved_key = f"({expr})" if not op_suffix else f"({expr})__{op_suffix}"
                        sys.stderr.write(f"[SQLGlot] [OK] Resolved date part '{key}' -> {resolved_key[:80]}...\n")
                        sys.stderr.flush()
                        resolved[resolved_key] = value
                        resolved_count += 1
                    else:
                        resolved[key] = value
                else:
                    resolved[key] = value
            
            sys.stderr.write(f"[SQLGlot] Resolution complete: {resolved_count}/{len(where)} columns resolved\n")
            sys.stderr.flush()
            return resolved
            
        except Exception as e:
            logger.error(f"[SQLGlot] Failed to resolve derived columns: {e}", exc_info=True)
            sys.stderr.write(f"[SQLGlot] Failed to resolve derived columns: {e}\n")
            sys.stderr.flush()
            return where
    
    def _build_case_expression(case_transform: dict) -> str:
        """Build SQL CASE expression from transform definition"""
        try:
            target = case_transform.get("target", "")
            cases = case_transform.get("cases", [])
            else_val = case_transform.get("else")
            
            if not target or not cases:
                return ""
            
            sql_parts = ["CASE"]
            for case in cases:
                when_cond = case.get("when", {})
                then_val = case.get("then")
                
                # Build condition
                op = when_cond.get("op", "eq")
                left = when_cond.get("left", target)
                right = when_cond.get("right")
                
                # Simple operators
                if op == "eq":
                    cond = f'"{left}" = \'{right}\''
                elif op == "ne":
                    cond = f'"{left}" != \'{right}\''
                elif op == "gt":
                    cond = f'"{left}" > {right}'
                elif op == "gte":
                    cond = f'"{left}" >= {right}'
                elif op == "lt":
                    cond = f'"{left}" < {right}'
                elif op == "lte":
                    cond = f'"{left}" <= {right}'
                elif op == "in":
                    vals = ", ".join([f"'{v}'" for v in right]) if isinstance(right, list) else f"'{right}'"
                    cond = f'"{left}" IN ({vals})'
                elif op == "like":
                    cond = f'"{left}" LIKE \'{right}\''
                else:
                    continue
                
                sql_parts.append(f"WHEN {cond} THEN '{then_val}'")
            
            if else_val is not None:
                sql_parts.append(f"ELSE '{else_val}'")
            
            sql_parts.append("END")
            return " ".join(sql_parts)
            
        except Exception:
            return ""
    
    def _build_datepart_expr(base_col: str, kind: str, dialect: str) -> str:
        """Build dialect-specific date part expression (e.g., OrderDate (Year))"""
        q = f'"{base_col}"'  # Quoted identifier
        kind_l = kind.lower()
        
        # DuckDB
        if "duckdb" in dialect.lower():
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
        elif "postgres" in dialect.lower() or "postgre" in dialect.lower():
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
        elif "mssql" in dialect.lower() or "sqlserver" in dialect.lower():
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
        elif "mysql" in dialect.lower():
            if kind_l == 'year': return f"DATE_FORMAT({q}, '%Y')"
            if kind_l == 'quarter': return f"CONCAT(DATE_FORMAT({q}, '%Y'), '-Q', QUARTER({q}))"
            if kind_l == 'month': return f"DATE_FORMAT({q}, '%Y-%m')"
            if kind_l == 'month name': return f"DATE_FORMAT({q}, '%M')"
            if kind_l == 'month short': return f"DATE_FORMAT({q}, '%b')"
            if kind_l == 'week': return f"CONCAT(DATE_FORMAT({q}, '%Y'), '-W', LPAD(WEEK({q}, 3), 2, '0'))"
            if kind_l == 'day': return f"DATE_FORMAT({q}, '%Y-%m-%d')"
            if kind_l == 'day name': return f"DATE_FORMAT({q}, '%W')"
            if kind_l == 'day short': return f"DATE_FORMAT({q}, '%a')"
        
        # Fallback: return quoted identifier
        return q

    # Helper: scope filter for datasource-level transforms
    def _matches_table(scope_table: str, source_name: str) -> bool:
        def norm(s: str) -> str:
            s = (s or '').strip().strip('[]').strip('"').strip('`')
            parts = s.split('.')
            return parts[-1].lower()
        return norm(scope_table) == norm(source_name)

    def _apply_scope(ds_tr: dict, source_name: str) -> dict:
        if not isinstance(ds_tr, dict):
            return {}
        def filt(arr):
            out = []
            for it in (arr or []):
                sc = (it or {}).get('scope')
                if not sc:
                    # Treat null scope as datasource-level (legacy transforms)
                    # This prevents table-specific transforms without scope from appearing everywhere
                    lvl = 'datasource'
                else:
                    lvl = str(sc.get('level') or '').lower()
                
                if lvl == 'datasource':
                    out.append(it)
                elif lvl == 'table' and sc and sc.get('table') and _matches_table(str(sc.get('table')), source_name):
                    out.append(it)
                elif lvl == 'widget':
                    try:
                        wid = str((sc or {}).get('widgetId') or '').strip()
                        if wid and getattr(payload, 'widgetId', None) and str(payload.widgetId).strip() == wid:
                            out.append(it)
                    except Exception:
                        pass
            return out
        return {
            'customColumns': filt(ds_tr.get('customColumns')),
            'transforms': filt(ds_tr.get('transforms')),
            'joins': filt(ds_tr.get('joins')),
            'defaults': ds_tr.get('defaults') or {},
        }

    # Resolve WHERE clause early (for both aggregated and non-aggregated paths)
    # This ensures custom columns are expanded to their SQL expressions
    where_resolved = None
    sys.stderr.write(f"[SPEC_DEBUG] About to check WHERE resolution, hasattr={hasattr(payload.spec, 'where')}, where={payload.spec.where if hasattr(payload.spec, 'where') else 'NO ATTR'}\n")
    sys.stderr.flush()
    if hasattr(payload.spec, 'where') and payload.spec.where:
        sys.stderr.write(f"[SPEC_DEBUG] Calling _resolve_derived_columns_in_where with WHERE keys: {list(payload.spec.where.keys())}\n")
        sys.stderr.flush()
        try:
            where_resolved = _resolve_derived_columns_in_where(
                payload.spec.where,
                ds,
                payload.spec.source,
                ds_type
            )
            sys.stderr.write(f"[SPEC_DEBUG] WHERE resolution completed, resolved keys: {list(where_resolved.keys()) if where_resolved else 'None'}\n")
            sys.stderr.flush()
        except Exception as e:
            logger.warning(f"[SQLGlot] Failed to resolve WHERE clause: {e}")
            sys.stderr.write(f"[SQLGlot] Failed to resolve WHERE clause: {e}\n")
            sys.stderr.flush()
            import traceback
            traceback.print_exc()
            where_resolved = None
    else:
        sys.stderr.write(f"[SPEC_DEBUG] Skipping WHERE resolution (no where clause or empty)\n")
        sys.stderr.flush()

    # If chart semantics are provided, build an aggregated SQL (generic) and delegate to /query
    spec = payload.spec
    legend_orig = spec.legend
    
    # Extract agg and y from series[0] if not present at root level
    agg_eff = spec.agg
    y_eff = spec.y
    measure_eff = getattr(spec, 'measure', None)
    
    series_arr = getattr(spec, 'series', None)
    if series_arr and isinstance(series_arr, list) and len(series_arr) > 0:
        s0 = series_arr[0]
        if isinstance(s0, dict):
            if not agg_eff and s0.get('agg'):
                agg_eff = s0.get('agg')
            if not y_eff and s0.get('y'):
                y_eff = s0.get('y')
            if not measure_eff and s0.get('measure'):
                measure_eff = s0.get('measure')
    
    # Override spec with effective values
    if agg_eff or y_eff or measure_eff:
        spec = payload.spec.model_copy(update={
            'agg': agg_eff or spec.agg,
            'y': y_eff or spec.y,
            'measure': measure_eff or getattr(spec, 'measure', None)
        })
    
    agg = (spec.agg or "none").lower()

    # ── Period-average early exit ──────────────────────────────────────────────
    # avg_daily  → numerator / COUNT(DISTINCT DATE(date_col))
    # avg_wday   → numerator / COUNT(DISTINCT CASE WHEN is_workday THEN DATE(date_col) END)
    # avg_weekly → numerator / COUNT(DISTINCT DATE_TRUNC('week', date_col))
    # avg_monthly→ numerator / COUNT(DISTINCT DATE_TRUNC('month', date_col))
    _PERIOD_AGG = frozenset({'avg_daily', 'avg_wday', 'avg_weekly', 'avg_monthly'})
    if agg in _PERIOD_AGG:
        val_field = getattr(spec, 'y', None)
        date_field = getattr(spec, 'avgDateField', None)
        if not val_field or not date_field:
            raise HTTPException(status_code=400, detail="avg_daily/avg_wday/avg_weekly/avg_monthly require both 'y' (value column) and 'avgDateField' (date column)")
        d = (ds_type or '').lower()
        vcol = _q_ident(val_field)
        _dcol_raw = _q_ident(date_field)

        # ── Auto-detect Unix timestamp columns via INFORMATION_SCHEMA ──────────
        _avg_is_unix = False
        if 'mysql' in d:
            try:
                _src_parts = str(spec.source or '').replace('`', '').split('.')
                _tbl_name = _src_parts[-1].strip()
                _sch_name = _src_parts[-2].strip() if len(_src_parts) > 1 else None
                _col_lower = str(date_field).strip().strip('`"[]').lower()
                _probe_sql = (
                    f"SELECT DATA_TYPE FROM information_schema.COLUMNS "
                    f"WHERE TABLE_NAME = '{_tbl_name}' AND LOWER(COLUMN_NAME) = '{_col_lower}'"
                    + (f" AND TABLE_SCHEMA = '{_sch_name}'" if _sch_name else "")
                    + " LIMIT 1"
                )
                _probe_res = run_query(QueryRequest(sql=_probe_sql, datasourceId=payload.datasourceId, limit=1), db)
                if _probe_res.rows:
                    _col_dtype = str(_probe_res.rows[0][0]).lower()
                    _avg_is_unix = _col_dtype in ('int', 'bigint', 'tinyint', 'smallint', 'mediumint', 'integer')
                    print(f"[AvgPeriod] Col '{date_field}' DATA_TYPE={_col_dtype!r} → is_unix={_avg_is_unix}", flush=True)
                else:
                    print(f"[AvgPeriod] INFORMATION_SCHEMA probe returned no rows for col '{date_field}'", flush=True)
            except Exception as _pe:
                print(f"[AvgPeriod] Unix-detection probe failed: {_pe}", flush=True)

        # ── Wrap date column in unix→datetime if integer column ────────────────
        if _avg_is_unix:
            if 'mysql' in d:
                dcol = f"FROM_UNIXTIME({_dcol_raw})"
            elif 'mssql' in d or 'sqlserver' in d:
                dcol = f"DATEADD(second, {_dcol_raw}, CAST('1970-01-01' AS DATETIME))"
            elif 'postgres' in d or 'postgre' in d:
                dcol = f"TO_TIMESTAMP({_dcol_raw})"
            else:
                dcol = f"to_timestamp({_dcol_raw})"
        else:
            dcol = _dcol_raw

        avg_numerator = (getattr(spec, 'avgNumerator', None) or 'sum').lower()
        # ── Numerator expression ───────────────────────────────────────────────
        if avg_numerator == 'count':
            num_expr = f"COUNT({vcol})"
        elif avg_numerator == 'distinct':
            num_expr = f"COUNT(DISTINCT {vcol})"
        else:  # sum (default)
            if 'duckdb' in d:
                y_clean = f"COALESCE(try_cast(regexp_replace(CAST({vcol} AS VARCHAR), '[^0-9.-]', '') AS DOUBLE), try_cast({vcol} AS DOUBLE), 0.0)"
                num_expr = f"SUM({y_clean})"
            else:
                num_expr = f"SUM({vcol})"

        # ── Dialect-aware period bucket (denominator) ──────────────────────────
        if agg in ('avg_daily', 'avg_wday'):
            day_cast = f"CAST({dcol} AS DATE)"
            if agg == 'avg_wday':
                if 'duckdb' in d or 'postgres' in d or 'postgre' in d:
                    dow_expr = f"dayofweek({dcol})" if 'duckdb' in d else f"EXTRACT(DOW FROM {dcol})"
                    wknd_days = "(0, 6)" if _spec_weekends == 'SAT_SUN' else "(5, 6)"
                elif 'mssql' in d or 'sqlserver' in d:
                    dow_expr = f"DATEPART(weekday, {dcol})"
                    wknd_days = "(1, 7)" if _spec_weekends == 'SAT_SUN' else "(6, 7)"
                elif 'mysql' in d:
                    dow_expr = f"DAYOFWEEK({dcol})"
                    wknd_days = "(1, 7)" if _spec_weekends == 'SAT_SUN' else "(6, 7)"
                else:
                    dow_expr = f"dayofweek({dcol})"
                    wknd_days = "(0, 6)" if _spec_weekends == 'SAT_SUN' else "(5, 6)"
                date_trunc = f"CASE WHEN {dow_expr} NOT IN {wknd_days} THEN {day_cast} END"
            else:
                date_trunc = day_cast
        elif agg == 'avg_weekly':
            if 'duckdb' in d or 'postgres' in d or 'postgre' in d:
                date_trunc = f"DATE_TRUNC('week', {dcol})"
            elif 'mssql' in d or 'sqlserver' in d:
                date_trunc = f"DATEADD(week, DATEDIFF(week, 0, {dcol}), 0)"
            elif 'mysql' in d:
                # Match Excel WEEKNUM(date, 11): Monday-start weeks, week 1 always starts Jan 1.
                # Formula: CONCAT(YEAR, '-', CEIL((DAYOFYEAR + WEEKDAY(Jan1)) / 7))
                # where WEEKDAY(Jan1) = 0 for Mon, 2 for Wed, etc.
                date_trunc = (
                    f"CONCAT(YEAR({dcol}), '-',"
                    f" LPAD(CEIL((DAYOFYEAR({dcol}) + WEEKDAY(MAKEDATE(YEAR({dcol}), 1))) / 7), 2, '0'))"
                )
            else:
                date_trunc = f"DATE_TRUNC('week', {dcol})"
        else:  # avg_monthly
            if 'duckdb' in d or 'postgres' in d or 'postgre' in d:
                date_trunc = f"DATE_TRUNC('month', {dcol})"
            elif 'mssql' in d or 'sqlserver' in d:
                date_trunc = f"YEAR({dcol}) * 100 + MONTH({dcol})"
            elif 'mysql' in d:
                date_trunc = f"DATE_FORMAT({dcol}, '%Y-%m')"
            else:
                date_trunc = f"DATE_TRUNC('month', {dcol})"

        den_expr = f"COUNT(DISTINCT {date_trunc})"

        # ── Build WHERE clause ─────────────────────────────────────────────────
        where_parts: list[str] = []
        params_avg: dict = {}
        if where_resolved:
            for _wk, _wv in where_resolved.items():
                _wbase = _wk.split('__')[0] if '__' in _wk else _wk
                _wop   = _wk.split('__', 1)[1] if '__' in _wk else 'eq'
                # Don't re-quote keys that are already resolved SQL expressions (e.g. "(LEFT(CAST(login AS CHAR), 2))")
                _wcol  = _wbase if _wbase.startswith('(') else _q_ident(_wbase)
                _pn    = _pname(_wbase, f"_{_wop}")
                if isinstance(_wv, list):
                    placeholders = ', '.join([f':{_pn}_{i}' for i in range(len(_wv))])
                    where_parts.append(f"{_wcol} IN ({placeholders})")
                    for _i, _v in enumerate(_wv): params_avg[f'{_pn}_{_i}'] = _v
                elif _wop == 'gte': where_parts.append(f"{_wcol} >= :{_pn}"); params_avg[_pn] = _wv
                elif _wop == 'gt':  where_parts.append(f"{_wcol} >  :{_pn}"); params_avg[_pn] = _wv
                elif _wop == 'lte': where_parts.append(f"{_wcol} <= :{_pn}"); params_avg[_pn] = _wv
                elif _wop == 'lt':  where_parts.append(f"{_wcol} <  :{_pn}"); params_avg[_pn] = _wv
                elif _wop == 'ne':  where_parts.append(f"{_wcol} != :{_pn}"); params_avg[_pn] = _wv
                else:               where_parts.append(f"{_wcol} =  :{_pn}"); params_avg[_pn] = _wv
        where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""

        # ── Build final SQL (3 columns for debug logging) ──────────────────────
        sql_avg = (
            f"SELECT {num_expr} AS num_val, {den_expr} AS den_val, "
            f"{num_expr} * 1.0 / NULLIF({den_expr}, 0) AS value "
            f"FROM {_q_source(spec.source)}{where_clause}"
        )
        print(f"[AvgPeriod] agg={agg}, source={spec.source}, val_col={val_field}, date_col={date_field}, is_unix={_avg_is_unix}, numerator={avg_numerator}, weekends={_spec_weekends}", flush=True)
        print(f"[AvgPeriod] SQL: {sql_avg[:800]}", flush=True)
        if params_avg:
            print(f"[AvgPeriod] params: { {k: v for k, v in list(params_avg.items())[:10]} }", flush=True)

        _avg_ds_id = None if (prefer_local and _duck_has_table(spec.source)) else payload.datasourceId
        _avg_req   = QueryRequest(sql=sql_avg, datasourceId=_avg_ds_id, limit=1, offset=0, includeTotal=False, params=params_avg or None)
        _avg_res   = run_query(_avg_req, db)

        # ── Log component values ───────────────────────────────────────────────
        try:
            if _avg_res.rows:
                _r, _c = _avg_res.rows[0], list(_avg_res.columns or [])
                _nv = _r[_c.index('num_val')] if 'num_val' in _c else '?'
                _dv = _r[_c.index('den_val')] if 'den_val' in _c else '?'
                _vv = _r[_c.index('value')]   if 'value'   in _c else '?'
                print(f"[AvgPeriod] num_val={_nv}, den_val={_dv}, result={_vv}", flush=True)
            else:
                print("[AvgPeriod] query returned no rows", flush=True)
        except Exception as _le:
            print(f"[AvgPeriod] result logging failed: {_le}", flush=True)

        return _avg_res
    # ── End period-average early exit ─────────────────────────────────────────

    has_chart_semantics = bool(spec.x or spec.y or spec.measure or spec.legend or (spec.groupBy and spec.groupBy != "none") or (agg and agg != "none"))
    # If there are no chart semantics at all, treat it as a plain SELECT
    if not has_chart_semantics:
        # Load datasource-level transforms if any
        ds_transforms = {}
        if ds is not None:
            try:
                opts = json.loads(ds.options_json or "{}")
            except Exception:
                opts = {}
            ds_transforms = _apply_scope((opts or {}).get("transforms") or {}, spec.source)

        # Build base SELECT with transforms/joins/defaults
        # Important: if WHERE references columns that are not in spec.select, include them so the outer WHERE can bind.
        eff_select = list(spec.select or ["*"])
        if payload.spec.where:
            try:
                present = {str(s).strip().split(".")[-1] for s in eff_select if isinstance(s, str)}
                for k in payload.spec.where.keys():
                    if k in ("start", "startDate", "end", "endDate"):
                        continue
                    base = k.split("__", 1)[0] if isinstance(k, str) and "__" in k else k
                    if isinstance(base, str) and base and base not in present:
                        eff_select.append(base)
            except Exception:
                # Best-effort; if this fails we'll rely on the datasource defaults or '*' to include columns
                pass
        # Filter joins whose sourceKey is not present on the current base source
        def _list_source_columns_for_base() -> set[str]:
            try:
                # Prefer native DuckDB for local store to avoid duckdb-engine hashing issues
                if (ds_type or '').lower().startswith('duckdb') or (payload.datasourceId is None):
                    from ..db import open_duck_native
                    with open_duck_native(settings.duckdb_path) as conn:
                        cur = conn.execute(f"SELECT * FROM {_q_source(spec.source)} WHERE 1=0")
                        desc = getattr(cur, 'description', None) or []
                        return set([str(col[0]) for col in desc])
                # Fallback: SQLAlchemy for external engines
                eng = _engine_for_datasource(db, payload.datasourceId, actorId)
                with eng.connect() as conn:
                    if (ds_type or '').lower() in ("mssql", "mssql+pymssql", "mssql+pyodbc"):
                        probe = text(f"SELECT TOP 0 * FROM {_q_source(spec.source)} AS s")
                    else:
                        probe = text(f"SELECT * FROM {_q_source(spec.source)} WHERE 1=0")
                    res = conn.execute(probe)
                    return set([str(c) for c in res.keys()])
            except Exception:
                return set()
        _base_cols = _list_source_columns_for_base()
        # Drop transforms/custom columns that reference columns not present on base
        ds_transforms = _filter_by_basecols(ds_transforms, _base_cols)
        _joins_all = ds_transforms.get("joins", []) if isinstance(ds_transforms, dict) else []
        _joins_eff = []
        for _j in (_joins_all or []):
            try:
                _skey = str((_j or {}).get("sourceKey") or "").strip()
                if not _skey:
                    continue
                if (_skey in _base_cols) or (f"[{_skey}]" in _base_cols) or (f'"{_skey}"' in _base_cols):
                    _joins_eff.append(_j)
            except Exception:
                continue

        result = build_sql(
            dialect=ds_type,
            source=spec.source,
            base_select=eff_select,
            custom_columns=ds_transforms.get("customColumns", []),
            transforms=ds_transforms.get("transforms", []),
            joins=_joins_eff,
            defaults=ds_transforms.get("defaults", {}),
            limit=None,
        )
        # Handle different return value formats
        if len(result) == 3:
            base_sql, _actual_cols, _warns = result
        elif len(result) == 4:
            base_sql, _actual_cols, _warns, _ = result
        else:
            base_sql = result[0] if result else ""

        # Apply WHERE filters on top of transformed subquery
        # Helpers: quote identifiers for WHERE and sanitize param names
        def _q_ident(name: str) -> str:
            s = str(name or '').strip('\n\r\t')
            if not s:
                return s
            if s.startswith('[') and s.endswith(']'):
                return s
            if s.startswith('"') and s.endswith('"'):
                return s
            if s.startswith('`') and s.endswith('`'):
                return s
            d = (ds_type or '').lower()
            if 'mssql' in d or 'sqlserver' in d:
                return f"[{s}]"
            if 'mysql' in d:
                return f"`{s}`"
            return f'"{s}"'
        def _pname(base: str, suffix: str = "") -> str:
            core = re.sub(r"[^A-Za-z0-9_]", "_", str(base or ''))
            return f"w_{core}{suffix}"
        def _coerce_filter_value(key: str, val: Any) -> Any:
            """Convert filter values to match the type returned by the derived column."""
            m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
            if not m:
                return val
            part = m.group(2).strip().lower()
            if part in ('year', 'quarter', 'month', 'week', 'day'):
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return val
            return str(val) if val is not None else val
        def _where_lhs(key: str) -> str:
            """Get SQL expression for WHERE clause. Expand derived date parts or use quoted column."""
            # If key starts with '(', it's already a resolved expression - but check if subquery has the column
            if str(key).strip().startswith('('):
                # When there's a transformed subquery, resolved expressions won't work because they
                # reference raw columns that aren't exposed. Check if we should use alias instead.
                if ds_transforms and _actual_cols:
                    # Try to find if there's a matching alias in the subquery
                    # e.g., "(EXTRACT(YEAR FROM "OrderDate"))" should use alias "OrderDate (Year)"
                    for col in _actual_cols:
                        if " (" in col and ")" in col:
                            # This is a date part alias - check if it matches
                            m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", col, flags=re.IGNORECASE)
                            if m:
                                expr = _build_datepart_expr_helper(m.group(1).strip(), m.group(2).lower(), ds_type)
                                if f"({expr})" == str(key).strip():
                                    sys.stderr.write(f"[SPEC_DEBUG] Using alias '{col}' instead of resolved expr\n")
                                    return _q_ident(col)
                return str(key)
            # Check if the column (including date part aliases) is already in the transformed subquery
            if ds_transforms and _actual_cols and key in _actual_cols:
                return _q_ident(key)
            m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
            if m:
                # If the alias is already in _actual_cols, use it directly
                if ds_transforms and _actual_cols and key in _actual_cols:
                    return _q_ident(key)
                # Otherwise expand to SQL expressions like EXTRACT(YEAR FROM "OrderDate")
                return _derived_lhs(key)
            return _derived_lhs(key)
        where_clauses = []
        params: Dict[str, Any] = {}
        # Use where_resolved if available (contains resolved custom columns), else fall back to spec.where
        where_to_use = where_resolved if where_resolved else (payload.spec.where if hasattr(payload, 'spec') and hasattr(payload.spec, 'where') else {})
        sys.stderr.write(f"[SPEC_DEBUG] Non-agg query: where_to_use keys = {list(where_to_use.keys()) if where_to_use else 'None'}\n")
        sys.stderr.flush()
        if where_to_use:
            for k, v in where_to_use.items():
                if k in ("start", "startDate", "end", "endDate"):
                    continue
                # Parse operator suffix FIRST (e.g., ClientCode__ne -> base=ClientCode, op=ne)
                base_col = k
                op_suffix = None
                if isinstance(k, str) and "__" in k:
                    base_col, op_suffix = k.split("__", 1)
                if v is None:
                    where_clauses.append(f"{_where_lhs(base_col)} IS NULL")
                elif isinstance(v, (list, tuple)):
                    if len(v) == 0:
                        continue
                    pnames = []
                    for i, item in enumerate(v):
                        pname = _pname(base_col, f"_{i}")
                        params[pname] = _coerce_filter_value(base_col, item)
                        pnames.append(f":{pname}")
                    # Use NOT IN for __ne operator, IN otherwise
                    in_op = "NOT IN" if op_suffix == "ne" else "IN"
                    where_clauses.append(f"{_where_lhs(base_col)} {in_op} ({', '.join(pnames)})")
                elif op_suffix:
                    opname = None
                    if op_suffix == "gte": opname = ">="
                    elif op_suffix == "gt": opname = ">"
                    elif op_suffix == "lte": opname = "<="
                    elif op_suffix == "lt": opname = "<"
                    elif op_suffix == "ne": opname = "!="
                    if opname:
                        pname = _pname(base_col, f"_{op_suffix}")
                        params[pname] = _coerce_filter_value(base_col, v)
                        where_clauses.append(f"{_where_lhs(base_col)} {opname} :{pname}")
                    else:
                        pname = _pname(base_col)
                        where_clauses.append(f"{_where_lhs(base_col)} = :{pname}")
                        params[pname] = _coerce_filter_value(base_col, v)
                else:
                    pname = _pname(base_col)
                    where_clauses.append(f"{_where_lhs(base_col)} = :{pname}")
                    params[pname] = _coerce_filter_value(base_col, v)
        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        sql_inner = f"SELECT * FROM ({base_sql}) AS _base{where_sql}"
        sys.stderr.write(f"[SPEC_DEBUG] Non-agg query: where_clauses = {where_clauses}\n")
        sys.stderr.write(f"[SPEC_DEBUG] Non-agg query: params = {params}\n")
        sys.stderr.write(f"[SPEC_DEBUG] Non-agg query: sql_inner[:200] = {sql_inner[:200]}\n")
        sys.stderr.flush()
        q = QueryRequest(
            sql=sql_inner,
            datasourceId=(None if (prefer_local and _duck_has_table(spec.source)) else payload.datasourceId),
            limit=lim or 1000,
            offset=off or 0,
            includeTotal=payload.includeTotal,
            params=params or None,
            preferLocalDuck=prefer_local,
            preferLocalTable=spec.source,
        )
        return run_query(q, db)

    if has_chart_semantics:
        # Load datasource-level transforms if any; prepare a FROM fragment
        import sys
        ds_transforms = {}
        if ds is not None:
            try:
                opts = json.loads(ds.options_json or "{}")
            except Exception:
                opts = {}
            ds_transforms = _apply_scope((opts or {}).get("transforms") or {}, spec.source)
        base_from_sql = f" FROM {_q_source(spec.source)}"
        if ds_transforms:
            # Filter joins similarly for aggregated/base wrapper
            def _list_cols_for_agg_base() -> set[str]:
                try:
                    # Prefer native DuckDB for local store
                    if (ds_type or '').lower().startswith('duckdb') or (payload.datasourceId is None):
                        from ..db import open_duck_native
                        with open_duck_native(settings.duckdb_path) as conn:
                            cur = conn.execute(f"SELECT * FROM {_q_source(spec.source)} WHERE 1=0")
                            desc = getattr(cur, 'description', None) or []
                            return set([str(col[0]) for col in desc])
                    eng = _engine_for_datasource(db, payload.datasourceId, actorId)
                    with eng.connect() as conn:
                        if (ds_type or '').lower() in ("mssql", "mssql+pymssql", "mssql+pyodbc"):
                            probe = text(f"SELECT TOP 0 * FROM {_q_source(spec.source)} AS s")
                        else:
                            probe = text(f"SELECT * FROM {_q_source(spec.source)} WHERE 1=0")
                        res = conn.execute(probe)
                        return set([str(c) for c in res.keys()])
                except Exception:
                    return set()
            __cols = _list_cols_for_agg_base()
            ds_transforms = _filter_by_basecols(ds_transforms, __cols)
            __joins_all = ds_transforms.get('joins', []) if isinstance(ds_transforms, dict) else []
            __joins_eff = []
            for __j in (__joins_all or []):
                try:
                    __skey = str((__j or {}).get('sourceKey') or '').strip()
                    if __skey and (__skey in __cols or f"[{__skey}]" in __cols or f'"{__skey}"' in __cols):
                        __joins_eff.append(__j)
                except Exception:
                    continue

            result2 = build_sql(
                dialect=ds_type,
                source=_q_source(spec.source),
                base_select=["*"],
                custom_columns=ds_transforms.get("customColumns", []),
                transforms=ds_transforms.get("transforms", []),
                joins=__joins_eff,
                defaults={},  # avoid sort/limit on base for aggregated queries
                limit=None,
            )
            # Handle different return value formats
            if len(result2) == 3:
                base_sql, _actual_cols, _warns2 = result2
            elif len(result2) == 4:
                base_sql, _actual_cols, _warns2, _ = result2
            else:
                base_sql = result2[0] if result2 else ""
            base_from_sql = f" FROM ({base_sql}) AS _base"
            
            # Validate that spec fields (x, y, legend) still exist after applying transforms
            # Use the ACTUAL columns returned by build_sql, plus probed base columns (for s.* cases)
            available_cols = set([_norm_name(c) for c in (_actual_cols or [])])
            try:
                available_cols |= { _norm_name(c) for c in (__cols or set()) }
            except Exception:
                pass
            # Build canonical map from normalized -> actual-cased name to avoid case-sensitive quote mismatches
            canonical_map: Dict[str, str] = {}
            try:
                for c in (_actual_cols or []):
                    canonical_map[_norm_name(c)] = str(c)
                for c in (__cols or set()):
                    k = _norm_name(str(c))
                    if k not in canonical_map:
                        canonical_map[k] = str(c)
            except Exception:
                pass
            
            # Create validated local variables (Pydantic models are immutable)
            _validated_x = spec.x
            _validated_y = spec.y
            _validated_legend = spec.legend
            _validated_series = spec.series if hasattr(spec, 'series') else None
            
            if _validated_x:
                # Allow derived date parts like "OrderDate (Year)" even if base column not in available_cols
                if isinstance(_validated_x, (list, tuple)):
                    parts: list[str] = []
                    for item in _validated_x:
                        if not item:
                            continue
                        s = str(item)
                        is_derived_x = bool(
                            re.match(
                                r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$",
                                s,
                                flags=re.IGNORECASE,
                            )
                        )
                        if is_derived_x:
                            parts.append(s)
                            continue
                        x_norm = _norm_name(s)
                        if x_norm and x_norm not in available_cols:
                            continue
                        parts.append(canonical_map.get(x_norm, s))
                    _validated_x = parts or None
                else:
                    s = str(_validated_x)
                    is_derived_x = bool(
                        re.match(
                            r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$",
                            s,
                            flags=re.IGNORECASE,
                        )
                    )
                    if not is_derived_x:
                        x_norm = _norm_name(s)
                        if x_norm and x_norm not in available_cols:
                            _validated_x = None
                        else:
                            _validated_x = canonical_map.get(x_norm, s)
            if _validated_y:
                y_norm = _norm_name(_validated_y)
                if y_norm and y_norm not in available_cols:
                    _y_soft = re.sub(r"[^a-z0-9]", "", y_norm)
                    _y_soft2 = re.sub(r"[^a-z0-9]", "", str(_validated_y or "").strip().lower())
                    _candidates = []
                    try:
                        for _k, _v in (canonical_map or {}).items():
                            _k_soft = re.sub(r"[^a-z0-9]", "", str(_k or ""))
                            _v_soft = re.sub(r"[^a-z0-9]", "", str(_v or "").strip().lower())
                            if (_y_soft and _k_soft == _y_soft) or (_y_soft2 and _v_soft == _y_soft2):
                                _candidates.append((_k, _v))
                    except Exception:
                        _candidates = []
                    if len(_candidates) == 1:
                        _validated_y = _candidates[0][1]
                    else:
                        _validated_y = None
                else:
                    _validated_y = canonical_map.get(y_norm, _validated_y)
            if _validated_legend:
                # Legend can be a string or an array. Preserve derived tokens and be lenient:
                # do NOT clear legend just because we can't prove availability; canonicalize when possible.
                def _keep_legend_item(item: str) -> bool:
                    try:
                        s = str(item or '').strip()
                        if re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", s, flags=re.IGNORECASE):
                            return True
                        ln = _norm_name(s)
                        return (ln in available_cols)
                    except Exception:
                        return False
                if isinstance(_validated_legend, (list, tuple)):
                    keep = []
                    for it in _validated_legend:
                        if _keep_legend_item(it):
                            s = str(it)
                            nm = _norm_name(s)
                            keep.append(canonical_map.get(nm, s))
                    _validated_legend = keep if keep else None
                else:
                    ln = _norm_name(str(_validated_legend))
                    if ln and (ln not in available_cols):
                        _validated_legend = None
                    else:
                        _validated_legend = canonical_map.get(ln, _validated_legend)

            if _validated_series and isinstance(_validated_series, list):
                _series_out = []
                for _s in (_validated_series or []):
                    if not isinstance(_s, dict):
                        _series_out.append(_s)
                        continue
                    _s2 = dict(_s)
                    if not _s2.get('y'):
                        for _yk in ('field', 'yField', 'y_field', 'valueField', 'value_field', 'value', 'measure', 'column', 'col'):
                            if _s2.get(_yk):
                                _s2['y'] = _s2.get(_yk)
                                break
                    if not _s2.get('agg'):
                        for _ak in ('aggregate', 'aggregation', 'aggFunc', 'agg_func'):
                            if _s2.get(_ak):
                                _s2['agg'] = _s2.get(_ak)
                                break

                    _sy = _s2.get('y')
                    if _sy:
                        _syn = _norm_name(str(_sy))
                        if _syn and _syn in available_cols:
                            _s2['y'] = canonical_map.get(_syn, _sy)
                        else:
                            _y_soft = re.sub(r"[^a-z0-9]", "", _syn)
                            _y_soft2 = re.sub(r"[^a-z0-9]", "", str(_sy or "").strip().lower())
                            _candidates = []
                            try:
                                for _k, _v in (canonical_map or {}).items():
                                    _k_soft = re.sub(r"[^a-z0-9]", "", str(_k or ""))
                                    _v_soft = re.sub(r"[^a-z0-9]", "", str(_v or "").strip().lower())
                                    if (_y_soft and _k_soft == _y_soft) or (_y_soft2 and _v_soft == _y_soft2):
                                        _candidates.append((_k, _v))
                            except Exception:
                                _candidates = []
                            if len(_candidates) == 1:
                                _s2['y'] = _candidates[0][1]

                    _series_out.append(_s2)
                _validated_series = _series_out
            
            # Also validate WHERE clause filters - remove invalid column references
            # IMPORTANT: Use where_resolved (with resolved expressions) instead of spec.where
            _validated_where = {}
            _where_source = where_resolved if where_resolved else spec.where
            if _where_source:
                for k, v in _where_source.items():
                    # Skip special date range keys
                    if k in ("start", "startDate", "end", "endDate"):
                        _validated_where[k] = v
                        continue
                    # Preserve resolved expressions (start with parenthesis)
                    if k.startswith("(") and ")" in k:
                        _validated_where[k] = v
                        continue
                    # Extract base column name (remove __ operators like ClientType__in)
                    base_col = k.split("__")[0] if "__" in k else k
                    # Preserve derived date part filters (e.g., "OrderDate (Year)")
                    if re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(base_col), flags=re.IGNORECASE):
                        _validated_where[k] = v
                        continue
                    col_norm = _norm_name(base_col)
                    # Check if column exists in available_cols OR if it's a computed column in expr_map
                    if col_norm in available_cols or (expr_map and base_col in expr_map):
                        _validated_where[k] = v
            else:
                _validated_where = _where_source
            
            # Override spec fields with validated values for this query
            spec = payload.spec.model_copy(update={
                'x': _validated_x, 
                'y': _validated_y, 
                'legend': _validated_legend,
                'where': _validated_where,
                'series': _validated_series,
            })
            sys.stderr.write(f"[SPEC_DEBUG] After spec copy: _validated_where keys = {list(_validated_where.keys()) if _validated_where else 'None'}\n")
            sys.stderr.flush()
        else:
            # Direct table/view reference; quote per dialect (handles schema-qualified)
            base_from_sql = f" FROM {_q_source(spec.source)}"
            
            # Still validate spec fields even without transforms (custom columns at datasource level)
            # Probe base columns (preserve original case)
            def _list_cols_no_transforms() -> set[str]:
                try:
                    if (ds_type or '').lower().startswith('duckdb') or (payload.datasourceId is None):
                        from ..db import open_duck_native
                        with open_duck_native(settings.duckdb_path) as conn:
                            cur = conn.execute(f"SELECT * FROM {_q_source(spec.source)} WHERE 1=0")
                            desc = getattr(cur, 'description', None) or []
                            return set([str(col[0]) for col in desc])
                    eng = _engine_for_datasource(db, payload.datasourceId, actorId)
                    with eng.connect() as conn:
                        if (ds_type or '').lower() in ("mssql", "mssql+pymssql", "mssql+pyodbc"):
                            probe = text(f"SELECT TOP 0 * FROM {_q_source(spec.source)} AS s")
                        else:
                            probe = text(f"SELECT * FROM {_q_source(spec.source)} WHERE 1=0")
                        res = conn.execute(probe)
                        return set([str(c) for c in res.keys()])
                except Exception:
                    return set()
            
            available_cols_direct = _list_cols_no_transforms()
            available_cols_direct_norm = { _norm_name(c) for c in (available_cols_direct or set()) }
            canonical_direct: Dict[str, str] = { _norm_name(c): c for c in (available_cols_direct or set()) }
            _validated_x = spec.x
            _validated_y = spec.y
            _validated_legend = spec.legend
            _validated_series = spec.series if hasattr(spec, 'series') else None
            
            if _validated_x:
                # Allow derived date parts like "OrderDate (Year)" even if base column not in available_cols
                is_derived_x2 = bool(re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(_validated_x), flags=re.IGNORECASE))
                if is_derived_x2:
                    # Keep the derived field as-is
                    pass
                else:
                    _xn = _norm_name(_validated_x)
                    if _xn not in available_cols_direct_norm:
                        _validated_x = None
                    else:
                        _validated_x = canonical_direct.get(_xn, _validated_x)
            if _validated_y:
                _yn = _norm_name(_validated_y)
                if _yn not in available_cols_direct_norm:
                    _y_soft = re.sub(r"[^a-z0-9]", "", _yn)
                    _y_soft2 = re.sub(r"[^a-z0-9]", "", str(_validated_y or "").strip().lower())
                    _candidates = []
                    try:
                        for _k, _v in (canonical_direct or {}).items():
                            _k_soft = re.sub(r"[^a-z0-9]", "", str(_k or ""))
                            _v_soft = re.sub(r"[^a-z0-9]", "", str(_v or "").strip().lower())
                            if (_y_soft and _k_soft == _y_soft) or (_y_soft2 and _v_soft == _y_soft2):
                                _candidates.append((_k, _v))
                    except Exception:
                        _candidates = []
                    if len(_candidates) == 1:
                        _validated_y = _candidates[0][1]
                    else:
                        _validated_y = None
                else:
                    _validated_y = canonical_direct.get(_yn, _validated_y)
            if _validated_legend:
                def _keep_legend_item2(item: str) -> bool:
                    try:
                        s = str(item or '').strip()
                        # Derived legend tokens allowed even if base col not probed here
                        if re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", s, flags=re.IGNORECASE):
                            return True
                        return (_norm_name(s) in available_cols_direct_norm)
                    except Exception:
                        return False
                if isinstance(_validated_legend, (list, tuple)):
                    keep = []
                    for it in _validated_legend:
                        if _keep_legend_item2(it):
                            s = str(it)
                            keep.append(canonical_direct.get(_norm_name(s), s))
                    _validated_legend = keep if keep else None
                else:
                    # Be lenient for a single legend token: keep it even if not in the probed set; just canonicalize if possible.
                    _validated_legend = canonical_direct.get(_norm_name(str(_validated_legend)), _validated_legend)

            if _validated_series and isinstance(_validated_series, list):
                _series_out = []
                for _s in (_validated_series or []):
                    if not isinstance(_s, dict):
                        _series_out.append(_s)
                        continue
                    _s2 = dict(_s)
                    if not _s2.get('y'):
                        for _yk in ('field', 'yField', 'y_field', 'valueField', 'value_field', 'value', 'measure', 'column', 'col'):
                            if _s2.get(_yk):
                                _s2['y'] = _s2.get(_yk)
                                break
                    if not _s2.get('agg'):
                        for _ak in ('aggregate', 'aggregation', 'aggFunc', 'agg_func'):
                            if _s2.get(_ak):
                                _s2['agg'] = _s2.get(_ak)
                                break

                    _sy = _s2.get('y')
                    if _sy:
                        _syn = _norm_name(str(_sy))
                        if _syn and _syn in available_cols_direct_norm:
                            _s2['y'] = canonical_direct.get(_syn, _sy)
                        else:
                            _y_soft = re.sub(r"[^a-z0-9]", "", _syn)
                            _y_soft2 = re.sub(r"[^a-z0-9]", "", str(_sy or "").strip().lower())
                            _candidates = []
                            try:
                                for _k, _v in (canonical_direct or {}).items():
                                    _k_soft = re.sub(r"[^a-z0-9]", "", str(_k or ""))
                                    _v_soft = re.sub(r"[^a-z0-9]", "", str(_v or "").strip().lower())
                                    if (_y_soft and _k_soft == _y_soft) or (_y_soft2 and _v_soft == _y_soft2):
                                        _candidates.append((_k, _v))
                            except Exception:
                                _candidates = []
                            if len(_candidates) == 1:
                                _s2['y'] = _candidates[0][1]

                    _series_out.append(_s2)
                _validated_series = _series_out
            
            # Also validate WHERE clause filters
            # IMPORTANT: Use where_resolved (with resolved expressions) instead of spec.where
            _validated_where = {}
            _where_source = where_resolved if where_resolved else spec.where
            if _where_source:
                for k, v in _where_source.items():
                    if k in ("start", "startDate", "end", "endDate"):
                        _validated_where[k] = v
                        continue
                    # Preserve resolved expressions (start with parenthesis)
                    if k.startswith("(") and ")" in k:
                        _validated_where[k] = v
                        continue
                    base_col = k.split("__")[0] if "__" in k else k
                    # Preserve derived date part filters (e.g., "OrderDate (Year)")
                    if re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(base_col), flags=re.IGNORECASE):
                        _validated_where[k] = v
                        continue
                    # Check if column exists in available_cols_direct OR if it's a computed column in expr_map
                    if _norm_name(base_col) in available_cols_direct or (expr_map and base_col in expr_map):
                        _validated_where[k] = v
            else:
                _validated_where = _where_source
            
            spec = payload.spec.model_copy(update={
                'x': _validated_x, 
                'y': _validated_y, 
                'legend': _validated_legend,
                'where': _validated_where,
                'series': _validated_series,
            })
            sys.stderr.write(f"[SPEC_DEBUG] After spec copy (no transforms): _validated_where keys = {list(_validated_where.keys()) if _validated_where else 'None'}\n")
            sys.stderr.flush()
        # Handle x as either string or array (extract first element if array)
        x_raw = spec.x or (spec.select[0] if spec.select else None)
        if isinstance(x_raw, (list, tuple)) and len(x_raw) > 0:
            x_col = x_raw[0]
        else:
            x_col = x_raw
        if not x_col and not (spec.legend or legend_orig):
            series_scalar = spec.series if hasattr(spec, 'series') and isinstance(spec.series, list) else None

            def _coerce_filter_value(key: str, val: Any) -> Any:
                m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
                if not m:
                    return val
                part = m.group(2).strip().lower()
                if part in ('year', 'quarter', 'month', 'week', 'day'):
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        return val
                return str(val) if val is not None else val

            def _where_lhs(key: str) -> str:
                if str(key).strip().startswith('('):
                    if ds_transforms and _actual_cols:
                        for col in _actual_cols:
                            if " (" in col and ")" in col:
                                m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", col, flags=re.IGNORECASE)
                                if m:
                                    expr = _build_datepart_expr_helper(m.group(1).strip(), m.group(2).lower(), ds_type)
                                    if f"({expr})" == str(key).strip():
                                        sys.stderr.write(f"[SPEC_DEBUG] Scalar agg: Using alias '{col}' instead of resolved expr\n")
                                        return _q_ident(col)
                    return str(key)
                if ds_transforms and _actual_cols and key in _actual_cols:
                    return _q_ident(key)
                m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
                if m:
                    if ds_transforms and _actual_cols and key in _actual_cols:
                        return _q_ident(key)
                    return _derived_lhs(key)
                return _derived_lhs(key)

            where_clauses = []
            params: Dict[str, Any] = {}
            where_to_use = where_resolved if where_resolved else spec.where
            sys.stderr.write(f"[SPEC_DEBUG] Scalar agg path: where_to_use keys = {list(where_to_use.keys()) if where_to_use else 'None'}, where_resolved = {where_resolved is not None}\n")
            sys.stderr.flush()
            if where_to_use:
                for k, v in where_to_use.items():
                    if k in ("start", "startDate", "end", "endDate"):
                        continue
                    base_col = k
                    op_suffix = None
                    if isinstance(k, str) and "__" in k:
                        base_col, op_suffix = k.split("__", 1)
                    if v is None:
                        where_clauses.append(f"{_where_lhs(base_col)} IS NULL")
                    elif isinstance(v, (list, tuple)):
                        if len(v) == 0:
                            continue
                        pnames = []
                        for i, item in enumerate(v):
                            pname = _pname(base_col, f"_{i}")
                            params[pname] = _coerce_filter_value(base_col, item)
                            pnames.append(f":{pname}")
                        in_op = "NOT IN" if op_suffix == "ne" else "IN"
                        where_clauses.append(f"{_where_lhs(base_col)} {in_op} ({', '.join(pnames)})")
                    elif op_suffix:
                        opname = None
                        if op_suffix == "gte": opname = ">="
                        elif op_suffix == "gt": opname = ">"
                        elif op_suffix == "lte": opname = "<="
                        elif op_suffix == "lt": opname = "<"
                        elif op_suffix == "ne": opname = "!="
                        if opname:
                            pname = _pname(base_col, f"_{op_suffix}")
                            params[pname] = _coerce_filter_value(base_col, v)
                            where_clauses.append(f"{_where_lhs(base_col)} {opname} :{pname}")
                        elif op_suffix in {"contains", "notcontains", "startswith", "endswith"}:
                            if op_suffix == "notcontains":
                                cmp = "NOT LIKE"; patt = f"%{v}%"
                            elif op_suffix == "contains":
                                cmp = "LIKE"; patt = f"%{v}%"
                            elif op_suffix == "startswith":
                                cmp = "LIKE"; patt = f"{v}%"
                            else:
                                cmp = "LIKE"; patt = f"%{v}"
                            pname = _pname(base_col, "_like")
                            params[pname] = patt
                            where_clauses.append(f"{_where_lhs(base_col)} {cmp} :{pname}")
                        else:
                            pname = _pname(base_col, "_eq")
                            where_clauses.append(f"{_where_lhs(base_col)} = :{pname}")
                            params[pname] = _coerce_filter_value(base_col, v)
                    else:
                        pname = _pname(base_col)
                        where_clauses.append(f"{_where_lhs(base_col)} = :{pname}")
                        params[pname] = _coerce_filter_value(base_col, v)
            where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            sys.stderr.write(f"[SCALAR_AGG_DEBUG] where_sql: {where_sql[:300]}\n")
            sys.stderr.write(f"[SCALAR_AGG_DEBUG] params: {params}\n")
            sys.stderr.write(f"[SCALAR_AGG_DEBUG] base_from_sql: {base_from_sql[:200] if base_from_sql else 'None'}\n")
            sys.stderr.write(f"[SCALAR_AGG_DEBUG] series_scalar: {bool(series_scalar and len(series_scalar) > 0)}\n")
            sys.stderr.flush()

            if series_scalar and len(series_scalar) > 0:
                union_parts = []
                for idx, s in enumerate(series_scalar):
                    if not isinstance(s, dict):
                        continue
                    series_name = s.get('name') or s.get('y') or f"Series {idx + 1}"
                    y_s = s.get('y')
                    agg_s = str(s.get('agg') or agg or 'sum').lower()
                    measure_s = s.get('measure')

                    if (agg_s in ("none", "count")) or ((not y_s) and (not measure_s)):
                        value_expr = "COUNT(*)"
                    elif agg_s == "distinct":
                        expr = None
                        if measure_s:
                            expr = str(measure_s).strip()
                            try:
                                expr = re.sub(r"\s+AS\s+.+$", "", expr, flags=re.IGNORECASE).strip() or expr
                            except Exception:
                                expr = expr
                        else:
                            expr = _q_ident(y_s)
                        value_expr = f"COUNT(DISTINCT {expr})"
                    else:
                        if measure_s:
                            measure_str = str(measure_s).strip()
                            try:
                                measure_core = re.sub(r"\s+AS\s+.+$", "", measure_str, flags=re.IGNORECASE).strip()
                            except Exception:
                                measure_core = measure_str
                            if not measure_core:
                                measure_core = measure_str
                            try:
                                already_agg = bool(re.match(r"^\s*(sum|avg|min|max|count)\s*\(", measure_core, flags=re.IGNORECASE))
                            except Exception:
                                already_agg = False
                            if agg_s == "count":
                                value_expr = "COUNT(*)"
                            elif agg_s == "distinct":
                                value_expr = f"COUNT(DISTINCT {measure_core})"
                            elif agg_s in ("sum", "avg", "min", "max"):
                                if "duckdb" in ds_type and not already_agg:
                                    y_clean = f"COALESCE(try_cast(regexp_replace(CAST(({measure_core}) AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), try_cast(({measure_core}) AS DOUBLE), 0.0)"
                                    value_expr = f"{agg_s.upper()}({y_clean})"
                                else:
                                    value_expr = measure_core if already_agg else f"{agg_s.upper()}({measure_core})"
                            else:
                                value_expr = "COUNT(*)"
                        else:
                            if y_s and ("duckdb" in ds_type) and (agg_s in ("sum", "avg", "min", "max")):
                                y_clean = (
                                    f"COALESCE("
                                    f"try_cast(regexp_replace(CAST({_q_ident(y_s)} AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), "
                                    f"try_cast({_q_ident(y_s)} AS DOUBLE), 0.0)"
                                )
                                value_expr = f"{agg_s.upper()}({y_clean})"
                            elif y_s:
                                value_expr = f"{agg_s.upper()}({_q_ident(y_s)})"
                            else:
                                value_expr = "COUNT(*)"

                    union_parts.append(
                        f"SELECT 'total' as x, '{series_name}' as legend, {value_expr} as value{base_from_sql}{where_sql}"
                    )

                if union_parts:
                    sql_inner = " UNION ALL ".join(union_parts) + " ORDER BY 2"
                    eff_limit = lim or 1000
                    q = QueryRequest(
                        sql=sql_inner,
                        datasourceId=(None if (prefer_local and _duck_has_table(spec.source)) else payload.datasourceId),
                        limit=eff_limit,
                        offset=off or 0,
                        includeTotal=payload.includeTotal,
                        params=params or None,
                        preferLocalDuck=prefer_local,
                        preferLocalTable=spec.source,
                    )
                    return run_query(q, db)

            # Fallback: simple total aggregation without x and without legend; label as 'total'
            # Build value_expr robustly (support measure and DuckDB numeric-cleaning)
            if (agg in ("none", "count")) or ((not spec.y) and (not getattr(spec, 'measure', None))):
                value_expr = "COUNT(*)"
            elif agg == "distinct":
                expr = None
                if getattr(spec, 'measure', None):
                    expr = str(spec.measure).strip()
                    try:
                        expr = re.sub(r"\s+AS\s+.+$", "", expr, flags=re.IGNORECASE).strip() or expr
                    except Exception:
                        expr = expr
                else:
                    expr = _q_ident(spec.y)
                value_expr = f"COUNT(DISTINCT {expr})"
            else:
                if getattr(spec, 'measure', None):
                    measure_str = str(spec.measure).strip()
                    try:
                        measure_core = re.sub(r"\s+AS\s+.+$", "", measure_str, flags=re.IGNORECASE).strip()
                    except Exception:
                        measure_core = measure_str
                    if not measure_core:
                        measure_core = measure_str
                    agg_l = str(agg or "").lower()
                    try:
                        already_agg = bool(re.match(r"^\s*(sum|avg|min|max|count)\s*\(", measure_core, flags=re.IGNORECASE))
                    except Exception:
                        already_agg = False
                    if agg_l == "count":
                        value_expr = "COUNT(*)"
                    elif agg_l == "distinct":
                        value_expr = f"COUNT(DISTINCT {measure_core})"
                    elif agg_l in ("sum", "avg", "min", "max"):
                        if "duckdb" in ds_type and not already_agg:
                            y_clean = f"COALESCE(try_cast(regexp_replace(CAST(({measure_core}) AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), try_cast(({measure_core}) AS DOUBLE), 0.0)"
                            value_expr = f"{agg_l.upper()}({y_clean})"
                        else:
                            value_expr = measure_core if already_agg else f"{agg_l.upper()}({measure_core})"
                    else:
                        value_expr = "COUNT(*)"
                else:
                    if ("duckdb" in ds_type) and (agg in ("sum", "avg", "min", "max")):
                        # Clean numeric strings like "1,234.50 ILS" -> 1234.50, then cast
                        # Robust for both string and numeric columns: cast to VARCHAR for regex, fallback to direct DOUBLE cast
                        y_clean = (
                            f"COALESCE("
                            f"try_cast(regexp_replace(CAST({_q_ident(spec.y)} AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), "
                            f"try_cast({_q_ident(spec.y)} AS DOUBLE), 0.0)"
                        )
                        value_expr = f"{agg.upper()}({y_clean})"
                    else:
                        value_expr = f"{agg.upper()}({_q_ident(spec.y)})"
            inner = f"SELECT 'total' as x, {value_expr} as value{base_from_sql}"
            # Apply datasource defaults (order/TopN) when present
            order_seg = " ORDER BY 1"  # only x column exists in this branch
            limit_override: int | None = None
            dfl = (ds_transforms.get("defaults") if isinstance(ds_transforms, dict) else None) or {}
            topn = dfl.get("limitTopN") if isinstance(dfl, dict) else None
            sortd = dfl.get("sort") if isinstance(dfl, dict) else None
            if isinstance(topn, dict) and topn.get("n"):
                by = str(topn.get("by") or "value").lower()
                dir_ = str(topn.get("direction") or "desc").upper()
                if by == "x": order_seg = f" ORDER BY 1 {dir_}"
                elif by == "value": order_seg = f" ORDER BY 2 {dir_}"
                limit_override = int(topn.get("n") or 0)
            elif isinstance(sortd, dict) and sortd.get("by"):
                by = str(sortd.get("by") or "x").lower()
                dir_ = str(sortd.get("direction") or "desc").upper()
                if by == "x": order_seg = f" ORDER BY 1 {dir_}"
                elif by == "value": order_seg = f" ORDER BY 2 {dir_}"
            sql_inner = inner + where_sql + order_seg
            eff_limit = min(int(limit_override or (lim or 1000)), int(lim or 1000)) if (limit_override or lim) else (limit_override or 1000)
            q = QueryRequest(
                sql=sql_inner,
                datasourceId=(None if (prefer_local and _duck_has_table(spec.source)) else payload.datasourceId),
                limit=eff_limit,
                offset=off or 0,
                includeTotal=payload.includeTotal,
                params=params or None,
                preferLocalDuck=prefer_local,
                preferLocalTable=spec.source,
            )
            result = run_query(q, db)
            return result

        # Special case: no X field but legend is present - group by legend only
        # Allow this even when agg is 'none' by defaulting to COUNT(*)
        if not x_col and (spec.legend or legend_orig):
            # Build value expression
            agg_l = str(agg or "").lower()
            if spec.measure:
                measure_str = str(spec.measure).strip()
                if measure_str:
                    try:
                        measure_core = re.sub(r"\s+AS\s+.+$", "", measure_str, flags=re.IGNORECASE).strip() or measure_str
                    except Exception:
                        measure_core = measure_str
                    if agg_l == "count":
                        value_expr = "COUNT(*)"
                    elif agg_l == "distinct":
                        value_expr = f"COUNT(DISTINCT {measure_core})"
                    elif agg_l in ("sum", "avg", "min", "max"):
                        if "duckdb" in ds_type:
                            y_clean = f"COALESCE(try_cast(regexp_replace(CAST(({measure_core}) AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), try_cast(({measure_core}) AS DOUBLE), 0.0)"
                            value_expr = f"{agg_l.upper()}({y_clean})"
                        else:
                            value_expr = f"{agg_l.upper()}({measure_core})"
                    else:
                        # No valid aggregation specified with a measure -> default to COUNT(*)
                        value_expr = "COUNT(*)"
                else:
                    value_expr = "COUNT(*)"
            elif agg_l == "count" and not spec.y:
                value_expr = "COUNT(*)"
            elif agg_l == "distinct" and spec.y:
                value_expr = f"COUNT(DISTINCT {_q_ident(spec.y)})"
            else:
                if spec.y and (agg_l in ("sum", "avg", "min", "max")):
                    if ("duckdb" in ds_type):
                        y_clean = (
                            f"COALESCE("
                            f"try_cast(regexp_replace(CAST({_q_ident(spec.y)} AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), "
                            f"try_cast({_q_ident(spec.y)} AS DOUBLE), 0.0)"
                        )
                        value_expr = f"{agg.upper()}({y_clean})"
                    else:
                        value_expr = f"{agg.upper()}({_q_ident(spec.y)})"
                else:
                    value_expr = "COUNT(*)"
            
            # Build legend expression (reuse logic from later section)
            import sys
            legend_expr_raw = (spec.legend or legend_orig)
            # Handle legend as array or string
            if isinstance(legend_expr_raw, (list, tuple)) and len(legend_expr_raw) > 0:
                legend_expr_raw = legend_expr_raw[0]
            legend_expr = _q_ident(str(legend_expr_raw)) if legend_expr_raw else None
            
            # Build WHERE clause
            def _coerce_filter_value(key: str, val: Any) -> Any:
                m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
                if not m:
                    return val
                part = m.group(2).strip().lower()
                if part in ('year', 'quarter', 'month', 'week', 'day'):
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        return val
                return str(val) if val is not None else val
            
            where_clauses = []
            params: Dict[str, Any] = {}
            # Use where_resolved if available (contains resolved custom columns), else fall back to spec.where
            where_to_use = where_resolved if where_resolved else spec.where
            if where_to_use:
                for k, v in where_to_use.items():
                    if k in ("start", "startDate", "end", "endDate"):
                        continue
                    # Parse operator suffix FIRST (e.g., ClientCode__ne -> base=ClientCode, op=ne)
                    base_col = k
                    op_suffix = None
                    if isinstance(k, str) and "__" in k:
                        base_col, op_suffix = k.split("__", 1)
                    if v is None:
                        where_clauses.append(f"{_derived_lhs(base_col)} IS NULL")
                    elif isinstance(v, (list, tuple)):
                        if len(v) == 0:
                            continue
                        pnames = []
                        for i, item in enumerate(v):
                            pname = _pname(base_col, f"_{i}")
                            params[pname] = _coerce_filter_value(base_col, item)
                            pnames.append(f":{pname}")
                        # Use NOT IN for __ne operator, IN otherwise
                        in_op = "NOT IN" if op_suffix == "ne" else "IN"
                        where_clauses.append(f"{_derived_lhs(base_col)} {in_op} ({', '.join(pnames)})")
                    elif op_suffix:
                        opname = None
                        if op_suffix == "gte": opname = ">="
                        elif op_suffix == "gt": opname = ">"
                        elif op_suffix == "lte": opname = "<="
                        elif op_suffix == "lt": opname = "<"
                        elif op_suffix == "ne": opname = "!="
                        if opname:
                            pname = _pname(base_col, f"_{op_suffix}")
                            params[pname] = _coerce_filter_value(base_col, v)
                            where_clauses.append(f"{_derived_lhs(base_col)} {opname} :{pname}")
                        else:
                            pname = _pname(base_col)
                            where_clauses.append(f"{_derived_lhs(base_col)} = :{pname}")
                            params[pname] = _coerce_filter_value(base_col, v)
                    else:
                        pname = _pname(base_col)
                        where_clauses.append(f"{_derived_lhs(base_col)} = :{pname}")
                        params[pname] = _coerce_filter_value(base_col, v)
            # Filter out NULL legend values
            legend_filter_clauses = list(where_clauses) if where_clauses else []
            if legend_expr:
                legend_filter_clauses.append(f"{legend_expr} IS NOT NULL")
            where_sql_with_legend = f" WHERE {' AND '.join(legend_filter_clauses)}" if legend_filter_clauses else ""

            series_legend = spec.series if hasattr(spec, 'series') and isinstance(spec.series, list) else None
            if series_legend and len(series_legend) > 0:
                union_parts = []
                for idx, s in enumerate(series_legend):
                    if not isinstance(s, dict):
                        continue
                    series_name = s.get('name') or s.get('y') or f"Series {idx + 1}"
                    series_name_sql = str(series_name).replace("'", "''")
                    y_s = s.get('y')
                    agg_s = str(s.get('agg') or agg_l or 'sum').lower()
                    measure_s = s.get('measure')

                    if (agg_s in ("none", "count")) or ((not y_s) and (not measure_s)):
                        value_expr_s = "COUNT(*)"
                    elif agg_s == "distinct":
                        expr = None
                        if measure_s:
                            expr = str(measure_s).strip()
                            try:
                                expr = re.sub(r"\s+AS\s+.+$", "", expr, flags=re.IGNORECASE).strip() or expr
                            except Exception:
                                expr = expr
                        else:
                            expr = _q_ident(y_s)
                        value_expr_s = f"COUNT(DISTINCT {expr})"
                    else:
                        if measure_s:
                            measure_str = str(measure_s).strip()
                            try:
                                measure_core = re.sub(r"\s+AS\s+.+$", "", measure_str, flags=re.IGNORECASE).strip()
                            except Exception:
                                measure_core = measure_str
                            if not measure_core:
                                measure_core = measure_str
                            try:
                                already_agg = bool(re.match(r"^\s*(sum|avg|min|max|count)\s*\(", measure_core, flags=re.IGNORECASE))
                            except Exception:
                                already_agg = False
                            if agg_s == "count":
                                value_expr_s = "COUNT(*)"
                            elif agg_s == "distinct":
                                value_expr_s = f"COUNT(DISTINCT {measure_core})"
                            elif agg_s in ("sum", "avg", "min", "max"):
                                if "duckdb" in ds_type and not already_agg:
                                    y_clean = f"COALESCE(try_cast(regexp_replace(CAST(({measure_core}) AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), try_cast(({measure_core}) AS DOUBLE), 0.0)"
                                    value_expr_s = f"{agg_s.upper()}({y_clean})"
                                else:
                                    value_expr_s = measure_core if already_agg else f"{agg_s.upper()}({measure_core})"
                            else:
                                value_expr_s = "COUNT(*)"
                        else:
                            if y_s and ("duckdb" in ds_type) and (agg_s in ("sum", "avg", "min", "max")):
                                y_clean = (
                                    f"COALESCE("
                                    f"try_cast(regexp_replace(CAST({_q_ident(y_s)} AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), "
                                    f"try_cast({_q_ident(y_s)} AS DOUBLE), 0.0)"
                                )
                                value_expr_s = f"{agg_s.upper()}({y_clean})"
                            elif y_s:
                                value_expr_s = f"{agg_s.upper()}({_q_ident(y_s)})"
                            else:
                                value_expr_s = "COUNT(*)"

                    if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                        union_parts.append(
                            f"SELECT {legend_expr} as x, '{series_name_sql}' as legend, {value_expr_s} as value {base_from_sql}{where_sql_with_legend} GROUP BY {legend_expr}"
                        )
                    else:
                        union_parts.append(
                            f"SELECT {legend_expr} as x, '{series_name_sql}' as legend, {value_expr_s} as value {base_from_sql}{where_sql_with_legend} GROUP BY 1"
                        )

                if union_parts:
                    sql_inner = " UNION ALL ".join(union_parts) + " ORDER BY 1,2"
                    eff_limit = lim or 1000
                    q = QueryRequest(
                        sql=sql_inner,
                        datasourceId=(None if (prefer_local and _duck_has_table(spec.source)) else payload.datasourceId),
                        limit=eff_limit,
                        offset=off or 0,
                        includeTotal=payload.includeTotal,
                        params=params or None,
                        preferLocalDuck=prefer_local,
                        preferLocalTable=spec.source,
                    )
                    return run_query(q, db)
            
            # Build SQL: For legend-only, return x='Total', legend=<category>, value=<count>
            # This allows the frontend to render as a bar/column chart with legend series
            if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                sql_inner = f"SELECT 'Total' as x, {legend_expr} as legend, {value_expr} as value {base_from_sql}{where_sql_with_legend} GROUP BY {legend_expr} ORDER BY {legend_expr}"
            else:
                # GROUP BY positions 2,3 (skip the constant 'Total' in position 1, group by legend and value expression)
                # Actually, value_expr is an aggregate, so we only group by legend (position 2)
                sql_inner = f"SELECT 'Total' as x, {legend_expr} as legend, {value_expr} as value {base_from_sql}{where_sql_with_legend} GROUP BY 2 ORDER BY 2"
            
            eff_limit = lim or 1000
            q = QueryRequest(
                sql=sql_inner,
                datasourceId=(None if (prefer_local and _duck_has_table(spec.source)) else payload.datasourceId),
                limit=eff_limit,
                offset=off or 0,
                includeTotal=payload.includeTotal,
                params=params or None,
                preferLocalDuck=prefer_local,
                preferLocalTable=spec.source,
            )
            return run_query(q, db)

        # Aggregated query when agg != 'none' (with optional legend)
        if agg and agg != "none":
            # Check if SQLGlot should be used (feature flag + user whitelist)
            use_sqlglot = should_use_sqlglot(actorId)
            sql_inner = None
            params = None  # Initialize params for both SQLGlot and legacy paths
            
            # Note: where_resolved is already computed at the top of this function
            
            if use_sqlglot:
                # NEW PATH: SQLGlot SQL generation
                try:
                    logger.info(f"[SQLGlot] ENABLED for user={actorId}, dialect={ds_type}")
                    print(f"[SQLGlot] ENABLED for user={actorId}, dialect={ds_type}")
                    
                    # Build expr_map for custom column resolution
                    expr_map = {}
                    try:
                        ds_tr_expr = ds_transforms if isinstance(ds_transforms, dict) else {}
                        try:
                            base_cols_expr: set[str] = set()
                            if ds_tr_expr:
                                if (ds_type or '').lower().startswith('duckdb') or (payload.datasourceId is None):
                                    with open_duck_native(settings.duckdb_path) as conn:
                                        cur = conn.execute(f"SELECT * FROM {_q_source(spec.source)} WHERE 1=0")
                                        desc = getattr(cur, 'description', None) or []
                                        base_cols_expr = set([str(col[0]) for col in desc])
                                else:
                                    eng = _engine_for_datasource(db, payload.datasourceId, actorId)
                                    with eng.connect() as conn:
                                        if (ds_type or '').lower() in ("mssql", "mssql+pymssql", "mssql+pyodbc"):
                                            probe = text(f"SELECT TOP 0 * FROM {_q_source(spec.source)} AS s")
                                        else:
                                            probe = text(f"SELECT * FROM {_q_source(spec.source)} WHERE 1=0")
                                        res = conn.execute(probe)
                                        base_cols_expr = set([str(c) for c in res.keys()])
                        except Exception:
                            base_cols_expr = set()

                        if ds_tr_expr and base_cols_expr:
                            ds_tr_expr = _filter_by_basecols(ds_tr_expr, base_cols_expr)

                        if isinstance(ds_tr_expr, dict) and ds_tr_expr:
                            from ..sqlgen import _normalize_expr_idents, _case_expr
                            for _cc in (ds_tr_expr.get("customColumns") or []):
                                if isinstance(_cc, dict) and _cc.get("name") and _cc.get("expr"):
                                    expr_map[str(_cc["name"])] = _normalize_expr_idents(ds_type, str(_cc["expr"]))
                            for _tr in (ds_tr_expr.get("transforms") or []):
                                if not isinstance(_tr, dict):
                                    continue
                                _t = str((_tr.get("type") or "")).lower()
                                if _t == "computed":
                                    _nm = str((_tr.get("name") or "")).strip()
                                    _ex = str((_tr.get("expr") or "")).strip()
                                    if _nm and _ex:
                                        expr_map[_nm] = _normalize_expr_idents(ds_type, _ex)
                                elif _t == "case":
                                    _tgt = str((_tr.get("target") or "")).strip()
                                    _cases = (_tr.get("cases") or [])
                                    _else_val = (_tr.get("else") or _tr.get("else_") or None)
                                    if _tgt and _cases:
                                        try:
                                            expr_map[_tgt] = _case_expr(ds_type, _tgt, _cases, _else_val)
                                        except Exception:
                                            pass
                        else:
                            expr_map = _build_expr_map(ds, spec.source, ds_type)
                    except Exception:
                        expr_map = _build_expr_map(ds, spec.source, ds_type)
                    
                    builder = SQLGlotBuilder(dialect=ds_type)
                    
                    # Handle multi-legend (legend could be string or array)
                    legend_field_val = spec.legend if hasattr(spec, 'legend') else None
                    legend_fields_val = None
                    if isinstance(legend_field_val, list):
                        legend_fields_val = legend_field_val
                        legend_field_val = None
                    
                    # Handle multi-series
                    series_val = spec.series if hasattr(spec, 'series') and isinstance(spec.series, list) else None
                    
                    # Debug: Log WHERE before passing to builder
                    sys.stderr.write(f"[SPEC_DEBUG] Passing WHERE to build_aggregation_query: {where_resolved}\n")
                    sys.stderr.flush()
                    
                    # Pass x_raw (full array for multi-level X) instead of x_col (first element only)
                    x_field_for_builder = x_raw if x_raw else (spec.x if hasattr(spec, 'x') else None)
                    print(f"[SQLGlot] x_field_for_builder = {x_field_for_builder}, x_raw = {x_raw}, spec.x = {spec.x if hasattr(spec, 'x') else 'N/A'}")
                    sql_inner = builder.build_aggregation_query(
                        source=spec.source,
                        x_field=x_field_for_builder,  # Pass full array for multi-level X support
                        y_field=spec.y if hasattr(spec, 'y') else None,
                        legend_field=legend_field_val,
                        agg=agg,
                        where=where_resolved,  # Use resolved WHERE with expressions
                        group_by=spec.groupBy if hasattr(spec, 'groupBy') else None,
                        order_by=spec.orderBy if hasattr(spec, 'orderBy') else None,
                        order=spec.order if hasattr(spec, 'order') else 'asc',
                        limit=lim,
                        week_start=spec.weekStart if hasattr(spec, 'weekStart') else 'mon',
                        date_field=x_col,  # For date range filtering (use first x field)
                        expr_map=expr_map,  # Pass custom column mapping
                        ds_type=ds_type,  # Pass dialect for date part resolution
                        series=series_val,  # Multi-series support
                        legend_fields=legend_fields_val,  # Multi-legend support
                    )
                    logger.info(f"[SQLGlot] Generated: {sql_inner[:150]}...")
                    print(f"[SQLGlot] Generated: {sql_inner[:150]}...")
                    
                    # Create query request and execute
                    eff_limit = lim or 1000
                    q = QueryRequest(
                        sql=sql_inner,
                        datasourceId=(None if (prefer_local and _duck_has_table(spec.source)) else payload.datasourceId),
                        limit=eff_limit,
                        offset=off or 0,
                        includeTotal=payload.includeTotal,
                        params=params or None,
                        preferLocalDuck=prefer_local,
                        preferLocalTable=spec.source,
                    )
                    counter_inc("sqlglot_queries_total", {"dialect": ds_type})
                    return run_query(q, db)
                    
                except Exception as e:
                    # SQLGlot failed, fall back to legacy
                    logger.error(f"[SQLGlot] ERROR: {e}")
                    print(f"[SQLGlot] ERROR: {e}")
                    counter_inc("sqlglot_errors_total", {"dialect": ds_type, "error": str(type(e).__name__)})
                    if not settings.enable_legacy_fallback:
                        logger.error(f"[SQLGlot] /query/spec: LEGACY FALLBACK DISABLED - Re-raising error")
                        raise HTTPException(status_code=500, detail=f"SQLGlot query generation failed: {e}")
                    print(f"[SQLGlot] /query/spec: Falling back to legacy SQL builder")
                    use_sqlglot = False
            
            # LEGACY PATH: Continue with existing SQL string building
            if not use_sqlglot:
                counter_inc("legacy_queries_total", {"dialect": ds_type})
            
            if spec.measure:
                # Sanitize raw measure: trim and strip trailing "AS alias" to avoid invalid wrappers
                measure_str = str(spec.measure).strip()
                if measure_str:
                    try:
                        measure_core = re.sub(r"\s+AS\s+.+$", "", measure_str, flags=re.IGNORECASE).strip()
                    except Exception:
                        measure_core = measure_str
                    if not measure_core:
                        # Fallback if empty after sanitization
                        measure_core = measure_str
                    # If an aggregation is requested and the measure isn't already aggregated, wrap it
                    agg_l = str(agg or "").lower()
                    try:
                        already_agg = bool(re.match(r"^\s*(sum|avg|min|max|count)\s*\(", measure_core, flags=re.IGNORECASE))
                    except Exception:
                        already_agg = False
                    if agg_l == "count":
                        value_expr = "COUNT(*)"
                    elif agg_l == "distinct":
                        value_expr = f"COUNT(DISTINCT {measure_core})"
                    elif agg_l in ("sum", "avg", "min", "max"):
                        if "duckdb" in ds_type and not already_agg:
                            y_clean = f"COALESCE(try_cast(regexp_replace(CAST(({measure_core}) AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), try_cast(({measure_core}) AS DOUBLE), 0.0)"
                            value_expr = f"{agg_l.upper()}({y_clean})"
                        else:
                            value_expr = measure_core if already_agg else f"{agg_l.upper()}({measure_core})"
                    else:
                        # Unknown agg: fall back to COUNT(*) for safety
                        value_expr = "COUNT(*)"
                else:
                    # Empty measure string – safe fallback
                    value_expr = "COUNT(*)"
            elif agg == "count" and not spec.y:
                value_expr = "COUNT(*)"
            elif agg == "distinct" and spec.y:
                value_expr = f"COUNT(DISTINCT {_q_ident(spec.y)})"
            else:
                if spec.y:
                    if ("duckdb" in ds_type) and (agg in ("sum", "avg", "min", "max")):
                        # Clean numeric strings like "1,234.50 ILS" -> 1234.50, then cast
                        # Robust for both string and numeric columns: cast to VARCHAR for regex, fallback to direct DOUBLE cast
                        y_clean = (
                            f"COALESCE("
                            f"try_cast(regexp_replace(CAST({_q_ident(spec.y)} AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), "
                            f"try_cast({_q_ident(spec.y)} AS DOUBLE), 0.0)"
                        )
                        value_expr = f"{agg.upper()}({y_clean})"
                    else:
                        value_expr = f"{agg.upper()}({_q_ident(spec.y)})"
                else:
                    value_expr = "COUNT(*)"

            # Reuse helpers from above scope
            def _coerce_filter_value(key: str, val: Any) -> Any:
                """Convert filter values to match the type returned by _derived_lhs() for the given key."""
                # Check if key is a derived date part
                m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
                if not m:
                    return val
                part = m.group(2).strip().lower()
                # Numeric parts: Year, Quarter, Month, Week, Day return integers
                if part in ('year', 'quarter', 'month', 'week', 'day'):
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        return val
                # String parts: Month Name, Month Short, Day Name, Day Short return strings
                return str(val) if val is not None else val
            
            def _where_lhs(key: str) -> str:
                """Get SQL expression for WHERE clause. Expand derived date parts or use quoted column."""
                # If key starts with '(', it's already a resolved expression - but check if subquery has the column
                if str(key).strip().startswith('('):
                    # When there's a transformed subquery, resolved expressions won't work because they
                    # reference raw columns that aren't exposed. Check if we should use alias instead.
                    if ds_transforms and _actual_cols:
                        for col in _actual_cols:
                            if " (" in col and ")" in col:
                                m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", col, flags=re.IGNORECASE)
                                if m:
                                    expr = _build_datepart_expr_helper(m.group(1).strip(), m.group(2).lower(), ds_type)
                                    if f"({expr})" == str(key).strip():
                                        sys.stderr.write(f"[SPEC_DEBUG] X+legend agg: Using alias '{col}' instead of resolved expr\n")
                                        return _q_ident(col)
                    return str(key)
                # Check if the column is already in the transformed subquery
                if ds_transforms and _actual_cols and key in _actual_cols:
                    return _q_ident(key)
                # Check if this is a derived date part
                m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
                if m:
                    if ds_transforms and _actual_cols and key in _actual_cols:
                        return _q_ident(key)
                    return _derived_lhs(key)
                return _derived_lhs(key)
            
            def _is_string_filter(key: str) -> bool:
                """Check if filter is for a string field (not a derived date part)."""
                return not re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
            
            where_clauses = []
            params: Dict[str, Any] = {}
            # Use where_resolved if available (contains resolved custom columns), else fall back to spec.where
            where_to_use = where_resolved if where_resolved else spec.where
            sys.stderr.write(f"[SPEC_DEBUG] X+legend agg path: where_to_use keys = {list(where_to_use.keys()) if where_to_use else 'None'}, where_resolved = {where_resolved is not None}\n")
            sys.stderr.flush()
            if where_to_use:
                for k, v in where_to_use.items():
                    if k in ("start", "startDate", "end", "endDate"):
                        continue
                    if v is None:
                        where_clauses.append(f"{_where_lhs(k)} IS NULL")
                    elif isinstance(k, str) and "__" in k:
                        base, op = k.split("__", 1)
                        opname = None
                        if op == "gte": opname = ">="
                        elif op == "gt": opname = ">"
                        elif op == "lte": opname = "<="
                        elif op == "lt": opname = "<"
                        if opname:
                            # Numeric comparisons stay as-is
                            pname = _pname(base, f"_{op}")
                            params[pname] = _coerce_filter_value(base, v)
                            where_clauses.append(f"{_where_lhs(base)} {opname} :{pname}")
                        elif op == "ne":
                            is_str = _is_string_filter(base)
                            col_expr = f"LOWER({_where_lhs(base)})" if is_str else _where_lhs(base)
                            # Support array for multiple ne values (AND logic: not A and not B)
                            if isinstance(v, (list, tuple)):
                                ne_conds = []
                                for i, item in enumerate(v):
                                    pname = _pname(base, f"_ne_{i}")
                                    params[pname] = _coerce_filter_value(base, str(item).lower() if is_str and isinstance(item, str) else item)
                                    ne_conds.append(f"{col_expr} <> :{pname}")
                                where_clauses.append(f"({' AND '.join(ne_conds)})")
                            else:
                                pname = _pname(base, "_ne")
                                params[pname] = _coerce_filter_value(base, str(v).lower() if is_str and isinstance(v, str) else v)
                                where_clauses.append(f"{col_expr} <> :{pname}")
                        elif op in {"contains", "notcontains", "startswith", "endswith"}:
                            # LIKE operators: case-insensitive with LOWER(), support arrays with OR logic
                            col_expr = f"LOWER({_where_lhs(base)})"
                            # Support array for multiple values (OR logic: contains A or contains B)
                            vals = v if isinstance(v, (list, tuple)) else [v]
                            like_conds = []
                            for i, item in enumerate(vals):
                                if op == "notcontains":
                                    cmp = "NOT LIKE"; patt = f"%{str(item).lower()}%"
                                elif op == "contains":
                                    cmp = "LIKE"; patt = f"%{str(item).lower()}%"
                                elif op == "startswith":
                                    cmp = "LIKE"; patt = f"{str(item).lower()}%"
                                else:
                                    cmp = "LIKE"; patt = f"%{str(item).lower()}"
                                pname = _pname(base, f"_like_{i}")
                                params[pname] = patt
                                like_conds.append(f"{col_expr} {cmp} :{pname}")
                            # OR for contains/startswith/endswith, AND for notcontains
                            join_op = " AND " if op == "notcontains" else " OR "
                            where_clauses.append(f"({join_op.join(like_conds)})")
                        else:
                            is_str = _is_string_filter(base)
                            pname = _pname(base, "_eq")
                            params[pname] = _coerce_filter_value(base, str(v).lower() if is_str and isinstance(v, str) else v)
                            col_expr = f"LOWER({_where_lhs(base)})" if is_str else _where_lhs(base)
                            where_clauses.append(f"{col_expr} = :{pname}")
                    elif isinstance(v, (list, tuple)):
                        # Regular field with multiple values (IN clause)
                        if len(v) == 0:
                            continue
                        pnames = []
                        is_str = _is_string_filter(k)
                        for i, item in enumerate(v):
                            pname = _pname(k, f"_{i}")
                            params[pname] = _coerce_filter_value(k, str(item).lower() if is_str and isinstance(item, str) else item)
                            pnames.append(f":{pname}")
                        col_expr = f"LOWER({_where_lhs(k)})" if is_str else _where_lhs(k)
                        where_clauses.append(f"{col_expr} IN ({', '.join(pnames)})")
                    else:
                        is_str = _is_string_filter(k)
                        pname = _pname(k)
                        params[pname] = _coerce_filter_value(k, str(v).lower() if is_str and isinstance(v, str) else v)
                        col_expr = f"LOWER({_where_lhs(k)})" if is_str else _where_lhs(k)
                        where_clauses.append(f"{col_expr} = :{pname}")
            where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            
            # Filter out NULL legend values when legend is present
            if spec.legend:
                legend_filter_clauses = list(where_clauses) if where_clauses else []
                # Note: legend_expr will be defined later, so we'll add this filter after legend_expr is set
                where_sql_base = where_sql  # Save base WHERE for later

            # Apply either: (a) derived x token like "OrderDate (Month Short)" or
            # (b) groupBy time-bucketing on raw x.
            # Multi-level X support: when x is an array like ["Day", "Year"], concatenate with delimiter
            is_multi_level_x = isinstance(x_raw, (list, tuple)) and len(x_raw) > 1
            multi_level_x_exprs: list = []  # Will hold (expr, order_expr) for each level
            multi_level_group_by: str = ""  # Will hold comma-separated GROUP BY expressions
            x_expr = _q_ident(x_col)
            x_order_expr = None
            gb = (spec.groupBy or 'none').lower()
            week_start = (getattr(spec, 'weekStart', None) or 'mon').lower()
            # Detect derived x pattern (Month/Month Name/Month Short/etc.) and compute a label + order expr
            _m_x = None
            try:
                _m_x = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(x_col or ''), flags=re.IGNORECASE)
            except Exception:
                _m_x = None
            if _m_x:
                base = _m_x.group(1).strip()
                kind = _m_x.group(2).strip().lower()
                col = _q_ident(base)
                if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                    if kind == 'month name': x_expr, x_order_expr = f"DATENAME(month, {col})", f"MONTH({col})"
                    elif kind == 'month short': x_expr, x_order_expr = f"LEFT(DATENAME(month, {col}), 3)", f"MONTH({col})"
                    elif kind == 'month': x_expr, x_order_expr = f"MONTH({col})", f"MONTH({col})"
                    elif kind == 'year': x_expr, x_order_expr = f"CAST(YEAR({col}) AS varchar(10))", f"YEAR({col})"
                    else: x_expr = col
                elif ("duckdb" in ds_type):
                    if kind == 'month name': x_expr, x_order_expr = f"strftime({col}, '%B')", f"EXTRACT(month FROM {col})"
                    elif kind == 'month short': x_expr, x_order_expr = f"strftime({col}, '%b')", f"EXTRACT(month FROM {col})"
                    elif kind == 'month': x_expr, x_order_expr = f"EXTRACT(month FROM {col})", f"EXTRACT(month FROM {col})"
                    elif kind == 'year': x_expr, x_order_expr = f"strftime({col}, '%Y')", f"CAST(EXTRACT(year FROM {col}) AS INTEGER)"
                    else: x_expr = col
                elif ("postgres" in ds_type) or ("postgre" in ds_type):
                    if kind == 'month name': x_expr, x_order_expr = f"to_char({col}, 'FMMonth')", f"EXTRACT(month FROM {col})"
                    elif kind == 'month short': x_expr, x_order_expr = f"to_char({col}, 'Mon')", f"EXTRACT(month FROM {col})"
                    elif kind == 'month': x_expr, x_order_expr = f"EXTRACT(month FROM {col})", f"EXTRACT(month FROM {col})"
                    elif kind == 'year': x_expr, x_order_expr = f"to_char({col}, 'YYYY')", f"EXTRACT(year FROM {col})"
                    else: x_expr = col
                elif ("mysql" in ds_type):
                    if kind == 'month name': x_expr, x_order_expr = f"DATE_FORMAT({col}, '%M')", f"MONTH({col})"
                    elif kind == 'month short': x_expr, x_order_expr = f"DATE_FORMAT({col}, '%b')", f"MONTH({col})"
                    elif kind == 'month': x_expr, x_order_expr = f"MONTH({col})", f"MONTH({col})"
                    elif kind == 'year': x_expr, x_order_expr = f"DATE_FORMAT({col}, '%Y')", f"YEAR({col})"
                    else: x_expr = col
                elif ("sqlite" in ds_type):
                    if kind == 'month name': x_expr, x_order_expr = f"CASE CAST(strftime('%m', {col}) AS INTEGER) WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November' ELSE 'December' END", f"CAST(strftime('%m', {col}) AS INTEGER)"
                    elif kind == 'month short': x_expr, x_order_expr = f"CASE CAST(strftime('%m', {col}) AS INTEGER) WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' ELSE 'Dec' END", f"CAST(strftime('%m', {col}) AS INTEGER)"
                    elif kind == 'month': x_expr, x_order_expr = f"CAST(strftime('%m', {col}) AS INTEGER)", f"CAST(strftime('%m', {col}) AS INTEGER)"
                    elif kind == 'year': x_expr, x_order_expr = f"strftime('%Y', {col})", f"CAST(strftime('%Y', {col}) AS INTEGER)"
                    else: x_expr = col
                else:
                    x_expr = col
            elif gb in ("day","week","month","quarter","year"):
                if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                    if gb == "day":
                        x_expr = f"CAST({_q_ident(x_col)} AS date)"
                    elif gb == "week":
                        if week_start == 'sun':
                            x_expr = f"DATEADD(week, DATEDIFF(week, 0, {_q_ident(x_col)}), 0)"
                        else:
                            x_expr = f"DATEADD(day, 1, DATEADD(week, DATEDIFF(week, 0, DATEADD(day, -1, {_q_ident(x_col)})), 0))"
                    elif gb == "month":
                        x_expr = f"DATEADD(month, DATEDIFF(month, 0, {_q_ident(x_col)}), 0)"
                    elif gb == "quarter":
                        x_expr = f"DATEADD(quarter, DATEDIFF(quarter, 0, {_q_ident(x_col)}), 0)"
                    elif gb == "year":
                        x_expr = f"DATEADD(year, DATEDIFF(year, 0, {_q_ident(x_col)}), 0)"
                elif ("duckdb" in ds_type) or ("postgres" in ds_type):
                    if "duckdb" in ds_type:
                        col_ts = f"COALESCE(try_cast({_q_ident(x_col)} AS TIMESTAMP), CAST(try_cast({_q_ident(x_col)} AS DATE) AS TIMESTAMP))"
                    else:
                        col_ts = _q_ident(x_col)
                    if gb == 'week':
                        if week_start == 'sun':
                            if "duckdb" in ds_type:
                                x_expr = f"DATE_TRUNC('week', {col_ts} + INTERVAL 1 day) - INTERVAL 1 day"
                            else:
                                x_expr = f"date_trunc('week', {col_ts} + interval '1 day') - interval '1 day'"
                        else:
                            if "duckdb" in ds_type:
                                x_expr = f"DATE_TRUNC('week', {col_ts})"
                            else:
                                x_expr = f"date_trunc('week', {col_ts})"
                    else:
                        if "duckdb" in ds_type:
                            x_expr = f"DATE_TRUNC('{gb}', {col_ts})"
                        else:
                            x_expr = f"date_trunc('{gb}', {col_ts})"
                elif ("mysql" in ds_type):
                    if gb == "day":
                        x_expr = f"DATE({_q_ident(x_col)})"
                    elif gb == "week":
                        if week_start == 'sun':
                            x_expr = f"DATE_SUB(DATE({_q_ident(x_col)}), INTERVAL (DAYOFWEEK({_q_ident(x_col)})-1) DAY)"
                        else:
                            x_expr = f"DATE_SUB(DATE({_q_ident(x_col)}), INTERVAL WEEKDAY({_q_ident(x_col)}) DAY)"
                    elif gb == "month":
                        x_expr = f"DATE(DATE_FORMAT({_q_ident(x_col)}, '%Y-%m-01'))"
                    elif gb == "quarter":
                        x_expr = f"DATE_ADD(MAKEDATE(YEAR({_q_ident(x_col)}), 1), INTERVAL QUARTER({_q_ident(x_col)})*3 - 3 MONTH)"
                    elif gb == "year":
                        x_expr = f"MAKEDATE(YEAR({_q_ident(x_col)}), 1)"
                elif ("sqlite" in ds_type):
                    if gb == "day":
                        x_expr = f"date({_q_ident(x_col)})"
                    elif gb == "week":
                        if week_start == 'sun':
                            x_expr = f"date({_q_ident(x_col)}, '-' || CAST(strftime('%w', {_q_ident(x_col)}) AS INTEGER) || ' days')"
                        else:
                            x_expr = f"date({_q_ident(x_col)}, '-' || ((CAST(strftime('%w', {_q_ident(x_col)}) AS INTEGER) + 6) % 7) || ' days')"
                    elif gb == "month":
                        x_expr = f"date(strftime('%Y-%m-01', {_q_ident(x_col)}))"
                    elif gb == "quarter":
                        x_expr = (
                            f"CASE "
                            f"WHEN CAST(strftime('%m', {_q_ident(x_col)}) AS INTEGER) BETWEEN 1 AND 3 THEN date(strftime('%Y-01-01', {_q_ident(x_col)})) "
                            f"WHEN CAST(strftime('%m', {_q_ident(x_col)}) AS INTEGER) BETWEEN 4 AND 6 THEN date(strftime('%Y-04-01', {_q_ident(x_col)})) "
                            f"WHEN CAST(strftime('%m', {_q_ident(x_col)}) AS INTEGER) BETWEEN 7 AND 9 THEN date(strftime('%Y-07-01', {_q_ident(x_col)})) "
                            f"ELSE date(strftime('%Y-10-01', {_q_ident(x_col)})) END"
                        )
                    elif gb == "year":
                        x_expr = f"date(strftime('%Y-01-01', {_q_ident(x_col)}))"
                    else:
                        x_expr = _q_ident(x_col)
                else:
                    x_expr = _q_ident(x_col)

            # Multi-level X: concatenate all x field expressions with '|' delimiter
            # This enables hierarchical axis like Excel (e.g., Day grouped by Year)
            if is_multi_level_x:
                def _build_x_expr_for_field(field_name: str) -> tuple:
                    """Build (expr, order_expr) for a single x field, handling derived date parts."""
                    _m = None
                    try:
                        _m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(field_name or ''), flags=re.IGNORECASE)
                    except Exception:
                        _m = None
                    if _m:
                        base = _m.group(1).strip()
                        kind = _m.group(2).strip().lower()
                        col = _q_ident(base)
                        if "duckdb" in ds_type:
                            if kind == 'day': return f"CAST(EXTRACT(day FROM {col}) AS INTEGER)", f"EXTRACT(day FROM {col})"
                            if kind == 'month': return f"CAST(EXTRACT(month FROM {col}) AS INTEGER)", f"EXTRACT(month FROM {col})"
                            if kind == 'month name': return f"strftime({col}, '%B')", f"EXTRACT(month FROM {col})"
                            if kind == 'month short': return f"strftime({col}, '%b')", f"EXTRACT(month FROM {col})"
                            if kind == 'year': return f"strftime({col}, '%Y')", f"CAST(EXTRACT(year FROM {col}) AS INTEGER)"
                            if kind == 'quarter': return f"CAST(EXTRACT(quarter FROM {col}) AS INTEGER)", f"EXTRACT(quarter FROM {col})"
                            if kind == 'week': return f"CAST(EXTRACT(week FROM {col}) AS INTEGER)", f"EXTRACT(week FROM {col})"
                            if kind == 'day name': return f"strftime({col}, '%A')", f"EXTRACT(dow FROM {col})"
                            if kind == 'day short': return f"strftime({col}, '%a')", f"EXTRACT(dow FROM {col})"
                        elif ("mssql" in ds_type) or ("sqlserver" in ds_type):
                            if kind == 'day': return f"DAY({col})", f"DAY({col})"
                            if kind == 'month': return f"MONTH({col})", f"MONTH({col})"
                            if kind == 'month name': return f"DATENAME(month, {col})", f"MONTH({col})"
                            if kind == 'month short': return f"LEFT(DATENAME(month, {col}), 3)", f"MONTH({col})"
                            if kind == 'year': return f"CAST(YEAR({col}) AS varchar(10))", f"YEAR({col})"
                            if kind == 'quarter': return f"DATEPART(QUARTER, {col})", f"DATEPART(QUARTER, {col})"
                            if kind == 'week': return f"DATEPART(WEEK, {col})", f"DATEPART(WEEK, {col})"
                            if kind == 'day name': return f"DATENAME(weekday, {col})", f"DATEPART(weekday, {col})"
                            if kind == 'day short': return f"LEFT(DATENAME(weekday, {col}), 3)", f"DATEPART(weekday, {col})"
                        return _q_ident(field_name), None
                    return _q_ident(field_name), None
                
                # Build expressions for all x levels
                for x_field in x_raw:
                    expr, order = _build_x_expr_for_field(x_field)
                    multi_level_x_exprs.append((expr, order))
                
                # For DuckDB: concatenate with '|' delimiter using CONCAT
                if "duckdb" in ds_type:
                    concat_parts = " || '|' || ".join([f"CAST({e[0]} AS VARCHAR)" for e in multi_level_x_exprs])
                    x_expr = concat_parts
                elif ("mssql" in ds_type) or ("sqlserver" in ds_type):
                    concat_parts = " + '|' + ".join([f"CAST({e[0]} AS varchar(255))" for e in multi_level_x_exprs])
                    x_expr = concat_parts
                else:
                    # PostgreSQL, MySQL, SQLite: use CONCAT
                    concat_args = ", '|', ".join([f"CAST({e[0]} AS TEXT)" for e in multi_level_x_exprs])
                    x_expr = f"CONCAT({concat_args})"
                
                # Order by the outer level (last field) first, then inner levels
                if any(e[1] for e in multi_level_x_exprs):
                    x_order_expr = ", ".join([e[1] or e[0] for e in reversed(multi_level_x_exprs)])
                
                # For GROUP BY, we need all the base expressions (not concatenated)
                multi_level_group_by = ", ".join([e[0] for e in multi_level_x_exprs])
                
                print(f"[BACKEND] Multi-level X: {x_raw} -> x_expr={x_expr[:100]}..., group_by={multi_level_group_by}, order={x_order_expr}")

            # Legend: allow derived date-part syntax like "OrderDate (Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)"
            legend_expr_raw = spec.legend
            # Handle legend as array or string
            if isinstance(legend_expr_raw, (list, tuple)) and len(legend_expr_raw) > 0:
                legend_expr_raw = legend_expr_raw[0]
            # Default: quote legend identifier to be dialect-safe (e.g., spaces or reserved keywords)
            legend_expr = _q_ident(str(legend_expr_raw)) if legend_expr_raw else None
            try:
                m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(legend_expr_raw or ''), flags=re.IGNORECASE)
            except Exception:
                m = None
            if m:
                base_col = m.group(1).strip()
                kind = m.group(2).lower()
                def _legend_datepart_expr(col: str, kind_l: str) -> str:
                    # MSSQL family
                    if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                        if kind_l == 'year': return f"CAST(YEAR({col}) AS varchar(10))"
                        if kind_l == 'quarter': return f"CAST(YEAR({col}) AS varchar(4)) + '-Q' + CAST(DATEPART(QUARTER, {col}) AS varchar(1))"
                        if kind_l == 'month': return f"CONCAT(CAST(YEAR({col}) AS varchar(4)), '-', RIGHT('0' + CAST(MONTH({col}) AS varchar(2)), 2))"
                        if kind_l == 'month name': return f"DATENAME(month, {col})"
                        if kind_l == 'month short': return f"LEFT(DATENAME(month, {col}), 3)"
                        if kind_l == 'week':
                            wn = f"DATEPART(ISO_WEEK, {col})" if week_start == 'mon' else f"DATEPART(WEEK, {col})"
                            return f"CONCAT(CAST(YEAR({col}) AS varchar(4)), '-W', RIGHT('0' + CAST({wn} AS varchar(2)), 2))"
                        if kind_l == 'day': return f"CONCAT(CAST(YEAR({col}) AS varchar(4)), '-', RIGHT('0'+CAST(MONTH({col}) AS varchar(2)),2), '-', RIGHT('0'+CAST(DAY({col}) AS varchar(2)),2))"
                        if kind_l == 'day name': return f"DATENAME(weekday, {col})"
                        if kind_l == 'day short': return f"LEFT(DATENAME(weekday, {col}), 3)"
                        return col
                    # DuckDB
                    if ("duckdb" in ds_type):
                        if kind_l == 'year': return f"strftime({col}, '%Y')"
                        if kind_l == 'quarter': return f"concat(strftime({col}, '%Y'), '-Q', CAST(EXTRACT(QUARTER FROM {col}) AS INTEGER))"
                        if kind_l == 'month': return f"strftime({col}, '%Y-%m')"
                        if kind_l == 'month name': return f"strftime({col}, '%B')"
                        if kind_l == 'month short': return f"strftime({col}, '%b')"
                        if kind_l == 'week':
                            fmt_week = "%U" if week_start == 'sun' else "%W"
                            return f"concat(strftime({col}, '%Y'), '-W', substr('00' || strftime({col}, '{fmt_week}'), -2))"
                        if kind_l == 'day': return f"strftime({col}, '%Y-%m-%d')"
                        if kind_l == 'day name': return f"strftime({col}, '%A')"
                        if kind_l == 'day short': return f"strftime({col}, '%a')"
                        return col
                    # Postgres family
                    if ("postgres" in ds_type) or ("postgre" in ds_type):
                        if kind_l == 'year': return f"to_char({col}, 'YYYY')"
                        if kind_l == 'quarter': return f"to_char({col}, 'YYYY-\"Q\"Q')"
                        if kind_l == 'month': return f"to_char({col}, 'YYYY-MM')"
                        if kind_l == 'month name': return f"to_char({col}, 'FMMonth')"
                        if kind_l == 'month short': return f"to_char({col}, 'Mon')"
                        if kind_l == 'week':
                            mode = 'IW' if week_start == 'mon' else 'WW'
                            return f"to_char({col}, 'YYYY') || '-W' || lpad(to_char({col}, '{mode}'), 2, '0')"
                        if kind_l == 'day': return f"to_char({col}, 'YYYY-MM-DD')"
                        if kind_l == 'day name': return f"to_char({col}, 'FMDay')"
                        if kind_l == 'day short': return f"to_char({col}, 'Dy')"
                        return col
                    # MySQL family
                    if ("mysql" in ds_type):
                        if kind_l == 'year': return f"DATE_FORMAT({col}, '%Y')"
                        if kind_l == 'quarter': return f"CONCAT(DATE_FORMAT({col}, '%Y'), '-Q', QUARTER({col}))"
                        if kind_l == 'month': return f"DATE_FORMAT({col}, '%Y-%m')"
                        if kind_l == 'month name': return f"DATE_FORMAT({col}, '%M')"
                        if kind_l == 'month short': return f"DATE_FORMAT({col}, '%b')"
                        if kind_l == 'week':
                            mode = 0 if week_start == 'sun' else 3
                            return f"CONCAT(DATE_FORMAT({col}, '%Y'), '-W', LPAD(WEEK({col}, {mode}), 2, '0'))"
                        if kind_l == 'day': return f"DATE_FORMAT({col}, '%Y-%m-%d')"
                        if kind_l == 'day name': return f"DATE_FORMAT({col}, '%W')"
                        if kind_l == 'day short': return f"DATE_FORMAT({col}, '%a')"
                        return col
                    # SQLite fallback
                    if ("sqlite" in ds_type):
                        if kind_l == 'year': return f"strftime('%Y', {col})"
                        if kind_l == 'quarter': return (
                            f"printf('%04d-Q%d', CAST(strftime('%Y', {col}) AS INTEGER), ((CAST(strftime('%m', {col}) AS INTEGER)-1)/3)+1)"
                        )
                        if kind_l == 'month': return f"strftime('%Y-%m', {col})"
                        if kind_l == 'month name': return f"strftime('%m', {col})"  # no names; numeric month
                        if kind_l == 'month short': return f"strftime('%m', {col})"
                        if kind_l == 'week': return f"strftime('%Y', {col}) || '-W' || printf('%02d', CAST(strftime('%W', {col}) AS INTEGER))"
                        if kind_l == 'day': return f"strftime('%Y-%m-%d', {col})"
                        if kind_l == 'day name': return f"strftime('%w', {col})"  # 0-6
                        if kind_l == 'day short': return f"strftime('%w', {col})"
                        return col
                    # Default: passthrough
                    return col
                legend_expr = _legend_datepart_expr(_q_ident(base_col), kind)

                # Special-case: groupBy=month and legend is Year of the same column as x =>
                # use month label (Jan..Dec) for x with numeric month for ordering to yield 12 unique x categories.
                try:
                    same_col = str(base_col) == str(x_col)
                except Exception:
                    same_col = False
                if gb == 'month' and same_col and kind == 'year':
                    col = _q_ident(base_col)
                    if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                        # Label: short month, Order: numeric month
                        x_expr = f"LEFT(DATENAME(month, {col}), 3)"
                        x_order_expr = f"MONTH({col})"
                    elif ("duckdb" in ds_type):
                        col_ts = f"COALESCE(try_cast({col} AS TIMESTAMP), CAST(try_cast({col} AS DATE) AS TIMESTAMP))"
                        x_expr = f"strftime({col_ts}, '%b')"
                        x_order_expr = f"EXTRACT(month FROM {col_ts})"
                    elif ("postgres" in ds_type) or ("postgre" in ds_type):
                        x_expr = f"to_char({col}, 'Mon')"
                        x_order_expr = f"EXTRACT(month FROM {col})"
                    elif ("mysql" in ds_type):
                        x_expr = f"DATE_FORMAT({col}, '%b')"
                        x_order_expr = f"MONTH({col})"
                    elif ("sqlite" in ds_type):
                        x_expr = (
                            f"CASE CAST(strftime('%m', {col}) AS INTEGER) WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' ELSE 'Dec' END"
                        )
                        x_order_expr = f"CAST(strftime('%m', {col}) AS INTEGER)"

            if spec.legend:
                # Filter out NULL legend values
                if legend_expr and 'legend_filter_clauses' in locals():
                    legend_filter_clauses.append(f"{legend_expr} IS NOT NULL")
                    where_sql = f" WHERE {' AND '.join(legend_filter_clauses)}" if legend_filter_clauses else ""
                
                # Determine ordering for Top-N ranking with legend: value is column 3
                _by = str((getattr(spec, 'orderBy', None) or '')).lower()
                _dir = str((getattr(spec, 'order', None) or ('desc' if _by == 'value' else 'asc'))).upper()
                if _by == 'value':
                    order_seg_mssql = f" ORDER BY 3 {_dir}"
                    order_seg_std = f" ORDER BY 3 {_dir}"
                elif _by == 'x':
                    # With legend, prefer stable order by x then legend
                    order_seg_mssql = f" ORDER BY {x_expr} {_dir}, {legend_expr}"
                    order_seg_std = f" ORDER BY 1 {_dir}, 2"
                else:
                    order_seg_mssql = " ORDER BY 1,2"
                    order_seg_std = " ORDER BY 1,2"
                if x_order_expr:
                    # Use a subquery to compute a stable numeric month order to avoid GROUP BY issues
                    if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                        sql_inner = (
                            f"SELECT x, legend, value FROM ("
                            f"SELECT {x_expr} as x, {legend_expr} as legend, {value_expr} as value, {x_order_expr} as _xo "
                            f"{base_from_sql}{where_sql} GROUP BY {x_expr}, {legend_expr}, {x_order_expr}) _t ORDER BY _xo, legend"
                        )
                    else:
                        sql_inner = (
                            f"SELECT x, legend, value FROM ("
                            f"SELECT {x_expr} as x, {legend_expr} as legend, {value_expr} as value, {x_order_expr} as _xo "
                            f"{base_from_sql}{where_sql} GROUP BY 1,2,4) _t ORDER BY _xo, 2"
                        )
                else:
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
                    print(f"[BACKEND] Built SQL with agg={agg}, value_expr={value_expr}, sql_inner={sql_inner[:200]}")
                # For non-MSSQL, apply LIMIT on inner query so ORDER BY is respected before pagination
                if not (("mssql" in ds_type) or ("sqlserver" in ds_type)) and lim:
                    try:
                        sql_inner = f"{sql_inner} LIMIT {int(lim)}"
                    except Exception:
                        pass
            else:
                # Determine ordering for Top-N ranking: order by 'value' or 'x'
                _by = str((getattr(spec, 'orderBy', None) or '')).lower()
                _dir = str((getattr(spec, 'order', None) or ('desc' if _by == 'value' else 'asc'))).upper()
                # When ordering by value in 2-column projection (x, value), value is column 2
                if _by == 'value':
                    order_seg_mssql = f" ORDER BY 2 {_dir}"
                    order_seg_std = f" ORDER BY 2 {_dir}"
                elif _by == 'x':
                    order_seg_mssql = f" ORDER BY {x_expr} {_dir}"
                    order_seg_std = f" ORDER BY 1 {_dir}"
                else:
                    order_seg_mssql = " ORDER BY 1"
                    order_seg_std = " ORDER BY 1"
                # For multi-level X, use the individual expressions for GROUP BY
                group_by_expr = multi_level_group_by if is_multi_level_x else x_expr
                if x_order_expr:
                    if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                        sql_inner = (
                            f"SELECT x, value FROM ("
                            f"SELECT {x_expr} as x, {value_expr} as value, {x_order_expr} as _xo "
                            f"{base_from_sql}{where_sql} GROUP BY {group_by_expr}, {x_order_expr}) _t ORDER BY _xo"
                        )
                    else:
                        # Count how many group by expressions we have for positional references
                        gb_count = len(multi_level_x_exprs) if is_multi_level_x else 1
                        gb_positions = ", ".join(str(i) for i in range(1, gb_count + 1))
                        order_position = gb_count + 2  # +1 for value, +1 for _xo
                        sql_inner = (
                            f"SELECT x, value FROM ("
                            f"SELECT {x_expr} as x, {value_expr} as value, {x_order_expr} as _xo "
                            f"{base_from_sql}{where_sql} GROUP BY {gb_positions}, {order_position}) _t ORDER BY _xo"
                        )
                else:
                    if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                        sql_inner = (
                            f"SELECT {x_expr} as x, {value_expr} as value "
                            f"{base_from_sql}{where_sql} GROUP BY {group_by_expr}{order_seg_mssql}"
                        )
                    else:
                        sql_inner = (
                            f"SELECT {x_expr} as x, {value_expr} as value "
                            f"{base_from_sql}{where_sql} GROUP BY 1{order_seg_std}"
                        )
                # For non-MSSQL, apply LIMIT on inner query so ORDER BY is respected before pagination
                if not (("mssql" in ds_type) or ("sqlserver" in ds_type)) and lim:
                    try:
                        sql_inner = f"{sql_inner} LIMIT {int(lim)}"
                    except Exception:
                        pass

            eff_limit = lim or 1000
            if 'limit_override' in locals() and limit_override:
                try:
                    eff_limit = min(int(eff_limit), int(limit_override))
                except Exception:
                    pass
            q = QueryRequest(
                sql=sql_inner,
                datasourceId=(None if (prefer_local and _duck_has_table(spec.source)) else payload.datasourceId),
                limit=eff_limit,
                offset=off or 0,
                includeTotal=payload.includeTotal,
                params=params or None,
                preferLocalDuck=prefer_local,
                preferLocalTable=spec.source,
            )
            return run_query(q, db)

        # agg == 'none': passthrough raw columns via select/x/y, but derive/quote when needed
        def _select_part(c: str) -> str:
            s = str(c or '').strip()
            # If derived pattern, use expression and alias back to original token (quoted)
            if re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", s, flags=re.IGNORECASE):
                expr = _derived_lhs(s)
                return f"{expr} AS {_q_ident(s)}"
            # If base_from_sql exists, columns are already materialized - just quote the name without table prefix
            if base_from_sql:
                return _q_ident(s)
            # If unquoted identifier includes spaces or special chars (not an expression), quote it
            try:
                is_quoted = (s.startswith('[') and s.endswith(']')) or (s.startswith('"') and s.endswith('"')) or (s.startswith('`') and s.endswith('`'))
                looks_like_expr = ('(' in s) or (')' in s)
                has_dot = ('.' in s)
                has_special = bool(re.search(r"[^A-Za-z0-9_]", s))
                if (not is_quoted) and (not looks_like_expr) and (not has_dot) and has_special:
                    # Example: "Deposit Timestamp" -> [Deposit Timestamp]
                    return _q_ident(s)
            except Exception:
                pass
            # Else return as-is (already safe or intended raw expression)
            return s
        cols = []
        cols.append(_select_part(str(x_col)))
        if spec.y:
            cols.append(_select_part(str(spec.y)))
        # Reuse helpers from above scope
        def _coerce_filter_value(key: str, val: Any) -> Any:
            m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
            if not m:
                return val
            part = m.group(2).strip().lower()
            if part in ('year', 'quarter', 'month', 'week', 'day'):
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return val
            return str(val) if val is not None else val
        
        def _where_lhs(key: str) -> str:
            """Get SQL expression for WHERE clause. Expand derived date parts or use quoted column."""
            # If key starts with '(', it's already a resolved expression - but check if subquery has the column
            if str(key).strip().startswith('('):
                # When there's a transformed subquery, resolved expressions won't work because they
                # reference raw columns that aren't exposed. Check if we should use alias instead.
                if ds_transforms and _actual_cols:
                    for col in _actual_cols:
                        if " (" in col and ")" in col:
                            m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", col, flags=re.IGNORECASE)
                            if m:
                                expr = _build_datepart_expr_helper(m.group(1).strip(), m.group(2).lower(), ds_type)
                                if f"({expr})" == str(key).strip():
                                    sys.stderr.write(f"[SPEC_DEBUG] Final path: Using alias '{col}' instead of resolved expr\n")
                                    return _q_ident(col)
                return str(key)
            # Check if the column is already in the transformed subquery
            if ds_transforms and _actual_cols and key in _actual_cols:
                return _q_ident(key)
            m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
            if m:
                if ds_transforms and _actual_cols and key in _actual_cols:
                    return _q_ident(key)
                return _derived_lhs(key)
            return _derived_lhs(key)
        
        where_clauses = []
        params: Dict[str, Any] = {}
        # Use where_resolved if available (contains resolved custom columns), else fall back to spec.where
        where_to_use = where_resolved if where_resolved else spec.where
        if where_to_use:
            for k, v in where_to_use.items():
                if k in ("start", "startDate", "end", "endDate"):
                    continue
                # Parse operator suffix FIRST (e.g., ClientCode__ne -> base=ClientCode, op=ne)
                base_col = k
                op_suffix = None
                if isinstance(k, str) and "__" in k:
                    base_col, op_suffix = k.split("__", 1)
                if v is None:
                    where_clauses.append(f"{_where_lhs(base_col)} IS NULL")
                elif isinstance(v, (list, tuple)):
                    if len(v) == 0:
                        continue
                    pnames = []
                    for i, item in enumerate(v):
                        pname = _pname(base_col, f"_{i}")
                        params[pname] = _coerce_filter_value(base_col, item)
                        pnames.append(f":{pname}")
                    # Use NOT IN for __ne operator, IN otherwise
                    in_op = "NOT IN" if op_suffix == "ne" else "IN"
                    where_clauses.append(f"{_where_lhs(base_col)} {in_op} ({', '.join(pnames)})")
                elif op_suffix:
                    opname = None
                    if op_suffix == "gte": opname = ">="
                    elif op_suffix == "gt": opname = ">"
                    elif op_suffix == "lte": opname = "<="
                    elif op_suffix == "lt": opname = "<"
                    if opname:
                        pname = _pname(base_col, f"_{op_suffix}")
                        params[pname] = _coerce_filter_value(base_col, v)
                        where_clauses.append(f"{_where_lhs(base_col)} {opname} :{pname}")
                    elif op_suffix == "ne":
                        pname = _pname(base_col, "_ne")
                        params[pname] = _coerce_filter_value(base_col, v)
                        where_clauses.append(f"{_where_lhs(base_col)} <> :{pname}")
                    elif op_suffix in {"contains", "notcontains", "startswith", "endswith"}:
                        if op_suffix == "notcontains":
                            cmp = "NOT LIKE"; patt = f"%{v}%"
                        elif op_suffix == "contains":
                            cmp = "LIKE"; patt = f"%{v}%"
                        elif op_suffix == "startswith":
                            cmp = "LIKE"; patt = f"{v}%"
                        else:
                            cmp = "LIKE"; patt = f"%{v}"
                        pname = _pname(base_col, "_like")
                        params[pname] = patt
                        where_clauses.append(f"{_where_lhs(base_col)} {cmp} :{pname}")
                    else:
                        pname = _pname(base_col, "_eq")
                        where_clauses.append(f"{_where_lhs(base_col)} = :{pname}")
                        params[pname] = _coerce_filter_value(base_col, v)
                else:
                    pname = _pname(base_col)
                    where_clauses.append(f"{_where_lhs(base_col)} = :{pname}")
                    params[pname] = _coerce_filter_value(base_col, v)
        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        sql_inner = f"SELECT {', '.join(cols)}{base_from_sql}{where_sql}"
        q = QueryRequest(
            sql=sql_inner,
            datasourceId=payload.datasourceId,
            limit=lim or 1000,
            offset=off or 0,
            includeTotal=payload.includeTotal,
            params=params or None,
        )
        return run_query(q, db)


@router.post("/distinct")
def distinct_values(payload: DistinctRequest, db: Session = Depends(get_db), actorId: Optional[str] = None, publicId: Optional[str] = None, token: Optional[str] = None) -> DistinctResponse:
    # Resolve date presets at execution time
    if getattr(payload, 'where', None):
        payload.where = _resolve_date_presets(payload.where)
    try:
        touch_actor(actorId)
    except Exception:
        pass
    try:
        gauge_inc("query_inflight", 1.0, {"endpoint": "distinct"})
    except Exception:
        pass
    if actorId:
        _ra = _throttle_take(actorId)
        if _ra:
            try:
                counter_inc("query_rate_limited_total", {"endpoint": "distinct"})
            except Exception:
                pass
            raise HTTPException(status_code=429, detail="Rate limit exceeded", headers={"Retry-After": str(_ra)})
    if isinstance(publicId, str) and publicId:
        sl = get_share_link_by_public(db, publicId)
        if not sl:
            raise HTTPException(status_code=404, detail="Not found")
        if not verify_share_link_token(sl, token if isinstance(token, str) else None, settings.secret_key):
            raise HTTPException(status_code=401, detail="Unauthorized")
    """Return distinct values for a column (including derived date parts) with optional WHERE.

    - Omits datasource defaults (TopN/sort) to ensure completeness
    - Supports equality, IN, and range ops in WHERE
    - Derived parts supported via sqlgen.build_distinct_sql
    """
    if not payload.source or not payload.field:
        raise HTTPException(status_code=400, detail="source and field are required")
    # Resolve datasource transforms so alias names are valid in DISTINCT and WHERE
    ds_info = None
    ds_type = ""
    if payload.datasourceId:
        ds_info = _ds_cache_get(str(payload.datasourceId))
        if ds_info is None:
            ds_obj = db.get(Datasource, payload.datasourceId)
            if ds_obj:
                ds_info = {
                    "id": ds_obj.id,
                    "user_id": ds_obj.user_id,
                    "connection_encrypted": ds_obj.connection_encrypted,
                    "type": ds_obj.type,
                    "options_json": ds_obj.options_json,
                }
                _ds_cache_set(str(payload.datasourceId), ds_info)
        if ds_info:
            try:
                ds_type = (ds_info.get("type") or "").lower()
            except Exception:
                ds_type = ""
    
    # Auto-detect DuckDB datasource if missing (copied from run_query_spec)
    if (not payload.datasourceId) and (ds_info is None):
        import sys
        print(f"[Distinct] Attempting to auto-detect DuckDB datasource for source '{payload.source}'...", file=sys.stderr)
        try:
            from sqlalchemy import select
            stmt = select(Datasource).where(Datasource.type == "duckdb")
            if actorId:
                u = db.get(User, str(actorId).strip())
                is_admin = bool(u and (u.role or "user").lower() == "admin")
                if not is_admin:
                    stmt = stmt.where(Datasource.user_id == str(actorId).strip())
            
            local_ds_candidates = list(db.execute(stmt).scalars())
            print(f"[Distinct] Found {len(local_ds_candidates)} DuckDB candidates", file=sys.stderr)
            
            ds_obj = None
            for candidate in local_ds_candidates:
                if not candidate.connection_encrypted:
                    ds_obj = candidate
                    break
                try:
                    dsn = decrypt_text(candidate.connection_encrypted or "")
                    if dsn and settings.duckdb_path in dsn:
                        ds_obj = candidate
                        break
                except Exception:
                    continue
            if not ds_obj and local_ds_candidates:
                ds_obj = local_ds_candidates[0]
            
            if ds_obj:
                ds_info = {
                    "id": ds_obj.id,
                    "user_id": ds_obj.user_id,
                    "connection_encrypted": ds_obj.connection_encrypted,
                    "type": ds_obj.type,
                    "options_json": ds_obj.options_json,
                }
                ds_type = "duckdb"
                print(f"[Distinct] Auto-detected DuckDB datasource: {ds_obj.id}", file=sys.stderr)
            else:
                print(f"[Distinct] No suitable DuckDB datasource found", file=sys.stderr)
        except Exception as e:
            print(f"[Distinct] Failed to auto-detect datasource: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            pass

    # Decide routing/dialect without creating a DuckDB SA engine
    route_duck = False
    dialect = "unknown"
    if (payload.datasourceId is None) or ((ds_info or {}).get("type", "").lower() == "duckdb"):
        route_duck = True
        dialect = "duckdb"
    else:
        engine = _engine_for_datasource(db, payload.datasourceId, actorId)
        try:
            dialect = (engine.dialect.name or "").lower()
        except Exception:
            dialect = "unknown"
    # Build base SQL with transforms/joins applied (scoped and dependency-filtered),
    # then select DISTINCT from that projection
    base_from_sql = None
    if ds_info is not None:
        try:
            opts = json.loads((ds_info.get("options_json") or "{}"))
        except Exception:
            opts = {}
        # Apply scope filtering to only include datasource/table/widget level items relevant to this source
        def _matches_table(scope_table: str, source_name: str) -> bool:
            def norm(s: str) -> str:
                s = (s or '').strip().strip('[]').strip('"').strip('`')
                parts = s.split('.')
                return parts[-1].lower()
            return norm(scope_table) == norm(source_name)
        def _apply_scope(model: dict, src: str) -> dict:
            if not isinstance(model, dict):
                return {}
            def filt(arr):
                out = []
                for it in (arr or []):
                    sc = (it or {}).get('scope')
                    if not sc:
                        out.append(it); continue
                    lvl = str(sc.get('level') or '').lower()
                    if lvl == 'datasource':
                        out.append(it)
                    elif lvl == 'table' and sc.get('table') and _matches_table(str(sc.get('table')), src):
                        out.append(it)
                    elif lvl == 'widget':
                        try:
                            wid = str((sc or {}).get('widgetId') or '').strip()
                            # Include widget-scoped items if widgetId matches
                            if payload.widgetId and wid == str(payload.widgetId):
                                out.append(it)
                        except Exception:
                            pass
                return out
            return {
                'customColumns': filt(model.get('customColumns')),
                'transforms': filt(model.get('transforms')),
                'joins': filt(model.get('joins')),
                'defaults': model.get('defaults') or {},
            }
        # Keep original transforms for build_sql (before filtering)
        ds_transforms_original = _apply_scope((opts or {}).get("transforms") or {}, str(payload.source))
        ds_transforms = ds_transforms_original
        # Probe base columns to filter out transforms/custom columns with missing dependencies
        def _list_cols_for_base() -> set[str]:
            try:
                if (ds_type or '').lower().startswith('duckdb') or (payload.datasourceId is None):
                    from ..db import open_duck_native
                    with open_duck_native(settings.duckdb_path) as conn:
                        cur = conn.execute(f"SELECT * FROM {str(payload.source)} WHERE 1=0")
                        desc = getattr(cur, 'description', None) or []
                        return set([str(col[0]) for col in desc])
                eng = _engine_for_datasource(db, payload.datasourceId, actorId)
                with eng.connect() as conn:
                    if (ds_type or '').lower() in ("mssql", "mssql+pymssql", "mssql+pyodbc"):
                        probe = text(f"SELECT TOP 0 * FROM {str(payload.source)} AS s")
                    else:
                        probe = text(f"SELECT * FROM {str(payload.source)} WHERE 1=0")
                    res = conn.execute(probe)
                    return set([str(c) for c in res.keys()])
            except Exception:
                return set()
        __cols = _list_cols_for_base()
        ds_transforms = _filter_by_basecols(ds_transforms, __cols)

        # After base-column filtering, further restrict custom columns/transforms
        # to only those needed for the requested DISTINCT field. This prevents
        # unused transforms (e.g. ones depending on VisitType) from being
        # included and causing Binder errors on DuckDB when their dependencies
        # are not present in the underlying table.
        try:
            ds_ccs = list((ds_transforms or {}).get('customColumns') or [])
            ds_trs = list((ds_transforms or {}).get('transforms') or [])

            # Collect alias names produced by custom columns and transforms
            custom_aliases: set[str] = set()
            transform_aliases: set[str] = set()
            for cc in ds_ccs:
                if isinstance(cc, dict) and cc.get('name'):
                    custom_aliases.add(_norm_name(str(cc.get('name') or '')))
            for tr in ds_trs:
                if not isinstance(tr, dict):
                    continue
                ttype = str((tr.get('type') or '')).lower()
                if ttype == 'computed' and tr.get('name'):
                    transform_aliases.add(_norm_name(str(tr.get('name') or '')))
                elif ttype in {'case', 'replace', 'translate', 'nullhandling'} and tr.get('target'):
                    transform_aliases.add(_norm_name(str(tr.get('target') or '')))
            alias_all: set[str] = custom_aliases | transform_aliases

            # Build dependency graph: alias -> other aliases it depends on
            custom_deps: dict[str, set[str]] = {}
            transform_deps: dict[str, set[str]] = {}

            for cc in ds_ccs:
                if isinstance(cc, dict) and cc.get('name') and cc.get('expr'):
                    nm = _norm_name(str(cc.get('name') or ''))
                    refs = _referenced_cols_in_expr(str(cc.get('expr') or ''))
                    custom_deps[nm] = {r for r in refs if r in alias_all}

            for tr in ds_trs:
                if not isinstance(tr, dict):
                    continue
                ttype = str((tr.get('type') or '')).lower()
                alias_name: str | None = None
                deps: set[str] = set()
                if ttype == 'computed' and tr.get('name') and tr.get('expr'):
                    alias_name = _norm_name(str(tr.get('name') or ''))
                    refs = _referenced_cols_in_expr(str(tr.get('expr') or ''))
                    deps = {r for r in refs if r in alias_all}
                elif ttype == 'case' and tr.get('target'):
                    alias_name = _norm_name(str(tr.get('target') or ''))
                    try:
                        for c in (tr.get('cases') or []):
                            left = str((c.get('when') or {}).get('left') or '')
                            nm = _norm_name(left)
                            if nm in alias_all:
                                deps.add(nm)
                    except Exception:
                        deps = set()
                # replace/translate/nullhandling do not introduce alias deps in expressions
                if alias_name:
                    transform_deps[alias_name] = deps

            # Determine which aliases are actually needed for this DISTINCT field
            root_alias = _norm_name(str(payload.field or ''))
            needed_aliases: set[str] = set()
            if root_alias in alias_all:
                needed_aliases.add(root_alias)
                queue: list[str] = [root_alias]
                while queue:
                    cur = queue.pop()
                    deps = custom_deps.get(cur, set()) | transform_deps.get(cur, set())
                    for dep in deps:
                        if dep in alias_all and dep not in needed_aliases:
                            needed_aliases.add(dep)
                            queue.append(dep)
            else:
                # Field is a base column or derived date part only; we do not need
                # any alias-based custom columns/transforms in the _base projection.
                needed_aliases = set()

            if needed_aliases:
                ds_ccs = [
                    cc for cc in ds_ccs
                    if isinstance(cc, dict)
                    and cc.get('name')
                    and _norm_name(str(cc.get('name') or '')) in needed_aliases
                ]
                ds_trs = [
                    tr for tr in ds_trs
                    if isinstance(tr, dict)
                    and (
                        (
                            str((tr.get('type') or '')).lower() == 'computed'
                            and tr.get('name')
                            and _norm_name(str(tr.get('name') or '')) in needed_aliases
                        )
                        or (
                            str((tr.get('type') or '')).lower() in {'case', 'replace', 'translate', 'nullhandling'}
                            and tr.get('target')
                            and _norm_name(str(tr.get('target') or '')) in needed_aliases
                        )
                    )
                ]
            else:
                # No alias-based dependencies are required for this field; drop all
                # custom columns/transforms to avoid bringing in unused ones.
                ds_ccs = []
                ds_trs = []

            ds_transforms = {
                'customColumns': ds_ccs,
                'transforms': ds_trs,
                'joins': (ds_transforms.get('joins') or []) if isinstance(ds_transforms, dict) else [],
                'defaults': (ds_transforms.get('defaults') or {}) if isinstance(ds_transforms, dict) else {},
            }
        except Exception:
            # On any error, fall back to base-column-filtered transforms
            pass

        # Filter joins to only those whose sourceKey exists on base
        __joins_all = ds_transforms.get('joins', []) if isinstance(ds_transforms, dict) else []
        __joins_eff = []
        for __j in (__joins_all or []):
            try:
                __skey = str((__j or {}).get('sourceKey') or '').strip()
                if __skey and (__skey in __cols or f"[{__skey}]" in __cols or f'"{__skey}"' in __cols):
                    __joins_eff.append(__j)
            except Exception:
                continue
        # Build base_select: ALWAYS include the field being queried
        # The legacy builder will handle it if it's a custom column, date part, or regular column
        field_name = str(payload.field)
        base_select_list = ["*", field_name]
        print(f"[Legacy] /distinct: Adding '{field_name}' to base_select (will be materialized if it's custom/datepart)")
        
        result = build_sql(
            dialect=ds_type or dialect,
            source=str(payload.source),
            base_select=base_select_list,
            custom_columns=(ds_transforms.get("customColumns", []) if isinstance(ds_transforms, dict) else []),
            transforms=(ds_transforms.get("transforms", []) if isinstance(ds_transforms, dict) else []),
            joins=__joins_eff,
            defaults={},  # do not apply defaults like TopN
            limit=None,
        )
        # Handle different return value formats
        if len(result) == 3:
            base_sql, _unused_cols, _warns = result
        elif len(result) == 4:
            base_sql, _unused_cols, _warns, _ = result
        else:
            base_sql = result[0] if result else ""
        base_from_sql = f"({base_sql}) AS _base"
    # If no datasource or no transforms, select directly from source
    effective_source = base_from_sql or str(payload.source)
    
    # Check if SQLGlot should be used
    use_sqlglot = should_use_sqlglot(actorId)
    sql = None
    params = {}
    
    if use_sqlglot:
        try:
            print(f"[SQLGlot] ENABLED for /distinct endpoint, dialect={dialect}")
            
            # Build expr_map from datasource so SQLGlot can expand custom columns
            # like ClientCode/ClientType when DISTINCTing over them. We do this
            # even when base_from_sql exists; in that case the expressions will
            # reference base columns that are already projected in the subquery.
            print(f"[SQLGlot] /distinct: base_from_sql={'EXISTS' if base_from_sql else 'NONE'}")
            print(f"[SQLGlot] /distinct: effective_source preview: {effective_source[:200] if effective_source else 'NONE'}...")
            expr_map = {}
            if ds_info:
                ds_obj = db.get(Datasource, ds_info.get("id"))
                if ds_obj:
                    # Create a pass-through scope function for _build_expr_map_helper
                    def _distinct_scope(ds_tr: dict, source_name: str) -> dict:
                        return ds_tr if isinstance(ds_tr, dict) else {}
                    expr_map = _build_expr_map_helper(ds_obj, payload.source, ds_type, _distinct_scope, None)
            
            # Resolve WHERE clause (similar to /query/spec)
            # IMPORTANT: Exclude the field we're querying from WHERE to avoid circular filtering
            where_resolved = None
            if payload.where:
                field_str = str(payload.field)
                print(f"[SQLGlot] /distinct: Field='{field_str}', WHERE keys={list(payload.where.keys())}")
                where_without_field = {k: v for k, v in payload.where.items() if k != field_str}
                print(f"[SQLGlot] /distinct: Excluded '{field_str}' from WHERE (original had {len(payload.where)} filters, now {len(where_without_field)})")

                if where_without_field:
                    if base_from_sql:
                        where_resolved = None
                    else:
                        where_resolved = _resolve_derived_columns_in_where_helper(
                            where_without_field,
                            db.get(Datasource, ds_info.get("id")) if ds_info else None,
                            payload.source,
                            ds_type
                        )
            
            builder = SQLGlotBuilder(dialect=dialect)
            sql = builder.build_distinct_query(
                source=effective_source,
                field=str(payload.field),
                where=where_resolved,
                limit=None,  # No limit - get all distinct values
                expr_map=expr_map,
                ds_type=ds_type,
            )
            print(f"[SQLGlot] Generated DISTINCT SQL: {sql[:150]}...")
            
        except Exception as e:
            print(f"[SQLGlot] ERROR in /distinct: {e}")
            logger.error(f"[SQLGlot] ERROR in /distinct: {e}", exc_info=True)
            if not settings.enable_legacy_fallback:
                logger.error(f"[SQLGlot] /distinct: LEGACY FALLBACK DISABLED - Re-raising error")
                raise HTTPException(status_code=500, detail=f"SQLGlot query generation failed: {e}")
            print(f"[SQLGlot] /distinct: Falling back to legacy builder")
            use_sqlglot = False
    
    if not use_sqlglot:
        # IMPORTANT: Exclude the field we're querying from WHERE to avoid circular filtering
        where_for_legacy = dict(payload.where or {})
        
        # Remove the field being queried
        if str(payload.field) in where_for_legacy:
            del where_for_legacy[str(payload.field)]
            print(f"[Legacy] /distinct: Excluded '{payload.field}' from WHERE to get all distinct values")
        
        # When base_from_sql exists, also skip other filters to avoid "column not found" errors
        if base_from_sql:
            print(f"[Legacy] base_from_sql exists, skipping remaining WHERE filters (custom columns in subquery)")
            where_for_legacy = {}  # Skip WHERE filtering when using transformed subquery
        
        sql, params = build_distinct_sql(
            dialect=dialect,
            source=effective_source,
            field=str(payload.field),
            where=where_for_legacy,
        )
    # Cache lookup (short TTL shared with /query)
    try:
        counter_inc("query_requests_total", {"endpoint": "distinct"})
    except Exception:
        pass
    _start = time.perf_counter()
    key = _cache_key("distinct", payload.datasourceId, sql, params)
    cached = _cache_get(key)
    if cached:
        rows = cached[1]
        try:
            values_cached = [r[0] for r in rows]
        except Exception:
            values_cached = [list(r)[0] for r in rows if r]
        try:
            counter_inc("query_cache_hit_total", {"endpoint": "distinct", "kind": "distinct"})
        except Exception:
            pass
        try:
            summary_observe("query_duration_ms", int((time.perf_counter() - _start) * 1000), {"endpoint": "distinct"})
        except Exception:
            pass
        return DistinctResponse(values=[v for v in values_cached if v is not None])

    values: list[Any] = []
    _HEAVY_SEM.acquire()
    __actor_acq = False
    __as = _actor_sem(actorId)
    if __as:
        try:
            __as.acquire(); __actor_acq = True
        except Exception:
            pass
    try:
        if route_duck and _duckdb is not None:
            # Execute with native DuckDB
            name_order = [m.group(1) for m in re.finditer(r":([A-Za-z_][A-Za-z0-9_]*)", sql)]
            sql_qm = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", "?", sql)
            vals = [params.get(nm) for nm in name_order]
            # Resolve DB path
            db_path = settings.duckdb_path
            if payload.datasourceId and ds_info and (ds_info.get("connection_encrypted")):
                try:
                    dsn = decrypt_text(ds_info.get("connection_encrypted"))
                    p = urlparse(dsn) if dsn else None
                    if p and (p.scheme or "").startswith("duckdb"):
                        _p = unquote(p.path or "")
                        if _p.startswith("///"):
                            _p = _p[2:]
                        db_path = _p or db_path
                        if db_path and db_path != ":memory:" and db_path.startswith("/."):
                            try:
                                db_path = os.path.abspath(db_path[1:])
                            except Exception:
                                pass
                        if ":memory:" in (dsn or "").lower():
                            db_path = ":memory:"
                except Exception:
                    pass
            with open_duck_native(db_path) as conn:
                cur = conn.execute(sql_qm, vals)
                rows = cur.fetchall()
            out_vals: list[Any] = []
            for row in rows:
                try:
                    v = row[0]
                except Exception:
                    v = list(row)[0] if row else None
                if v is not None:
                    out_vals.append(v)
            values = out_vals
            try:
                _cache_set(key, ["__val"], [[v] for v in values])
            except Exception:
                pass
            try:
                counter_inc("query_cache_miss_total", {"endpoint": "distinct", "kind": "distinct"})
            except Exception:
                pass
            try:
                summary_observe("query_duration_ms", int((time.perf_counter() - _start) * 1000), {"endpoint": "distinct"})
            except Exception:
                pass
            return DistinctResponse(values=values)
        else:
            # Execute with SQLAlchemy for external engines
            engine = _engine_for_datasource(db, payload.datasourceId, actorId)
            with engine.connect() as conn:
                # Dialect-specific statement timeouts
                try:
                    if "postgres" in dialect:
                        conn.execute(text("SET statement_timeout = 30000"))
                    elif ("mysql" in dialect) or ("mariadb" in dialect):
                        conn.execute(text("SET SESSION MAX_EXECUTION_TIME=30000"))
                    elif ("mssql" in dialect) or ("sqlserver" in dialect):
                        conn.execute(text("SET LOCK_TIMEOUT 30000"))
                except Exception:
                    pass
                result = conn.execute(text(sql), params)
                for row in result.fetchall():
                    try:
                        v = row[0]
                    except Exception:
                        v = list(row)[0] if row else None
                    if v is not None:
                        values.append(v)
            # Store in cache as a single-column table shape
            try:
                _cache_set(key, ["__val"], [[v] for v in values])
            except Exception:
                pass
            try:
                counter_inc("query_cache_miss_total", {"endpoint": "distinct", "kind": "distinct"})
            except Exception:
                pass
            try:
                summary_observe("query_duration_ms", int((time.perf_counter() - _start) * 1000), {"endpoint": "distinct"})
            except Exception:
                pass
            return DistinctResponse(values=values)
    finally:
        try:
            gauge_dec("query_inflight", 1.0, {"endpoint": "distinct"})
        except Exception:
            pass
        _HEAVY_SEM.release()
        if __actor_acq and __as:
            try:
                __as.release()
            except Exception:
                pass


# --- Period totals helper ---
@router.post("/period-totals")
def period_totals(payload: dict, db: Session = Depends(get_db), actorId: Optional[str] = None, publicId: Optional[str] = None, token: Optional[str] = None) -> dict:
    # Resolve date presets at execution time
    if payload.get("where"):
        payload["where"] = _resolve_date_presets(payload["where"])
    try:
        touch_actor(actorId)
    except Exception:
        pass
    try:
        gauge_inc("query_inflight", 1.0, {"endpoint": "period_totals"})
    except Exception:
        pass
    if actorId:
        _ra = _throttle_take(actorId)
        if _ra:
            try:
                counter_inc("query_rate_limited_total", {"endpoint": "period_totals"})
            except Exception:
                pass
            raise HTTPException(status_code=429, detail="Rate limit exceeded", headers={"Retry-After": str(_ra)})
    if isinstance(publicId, str) and publicId:
        sl = get_share_link_by_public(db, publicId)
        if not sl:
            raise HTTPException(status_code=404, detail="Not found")
        if not verify_share_link_token(sl, token if isinstance(token, str) else None, settings.secret_key):
            raise HTTPException(status_code=401, detail="Unauthorized")
    """Compute aggregated totals for a period (start..end), optionally grouped by a legend column.

    Payload keys:
    - source: str (table or view name)
    - datasourceId: Optional[str]
    - y: Optional[str]
    - measure: Optional[str]
    - agg: Optional[str] in ['none','count','distinct','avg','sum','min','max']
    - dateField: str
    - start: str (ISO datetime)
    - end: str (ISO datetime)
    - where: Optional[Dict[str, Any]] equality / IN only
    - legend: Optional[str]
    """
    source = payload.get("source")
    if not source:
        raise HTTPException(status_code=400, detail="source is required")
    datasource_id = payload.get("datasourceId")
    # Decide routing based on datasource type
    ds = None
    ds_type = ""
    if datasource_id:
        ds = db.get(Datasource, datasource_id)
        if ds:
            try:
                ds_type = (ds.type or "").lower()
            except Exception:
                ds_type = ""
    route_duck = (datasource_id is None) or (ds_type == "duckdb")
    
    # Auto-detect local DuckDB datasource when datasourceId is None (same logic as run_query_spec)
    if datasource_id is None and not ds:
        try:
            # Case-insensitive match on type startswith 'duckdb'
            candidates = db.query(Datasource).all()
            duck_list = [c for c in (candidates or []) if str(getattr(c, 'type', '') or '').lower().startswith('duckdb')]
            # Prefer the one without external connection (local shared DuckDB)
            for candidate in duck_list:
                try:
                    if not getattr(candidate, 'connection_encrypted', None):
                        ds = candidate
                        ds_type = 'duckdb'
                        break
                except Exception:
                    continue
            if not ds and duck_list:
                ds = duck_list[0]
                ds_type = 'duckdb'
        except Exception:
            pass
    
    # Load datasource transforms (custom columns) - needed to resolve legend/y fields
    ds_transforms: dict = {}
    expr_map: dict[str, str] = {}
    import sys
    
    # Get legend early for debugging
    legend = payload.get("legend")
    y = payload.get("y")
    
    sys.stderr.write(f"[PT] datasource_id={datasource_id}, ds={ds}, source={source}, legend={legend}\n")
    sys.stderr.flush()
    if ds:
        try:
            opts = json.loads(ds.options_json or "{}")
            raw_transforms = opts.get("transforms") or {}
            sys.stderr.write(f"[PT] raw_transforms has {len(raw_transforms.get('customColumns', []))} custom columns\n")
            sys.stderr.flush()
            # Apply scope filtering (table-specific transforms)
            def _apply_scope_pt(transforms_dict: dict, source_name: str) -> dict:
                if not isinstance(transforms_dict, dict):
                    return {}
                
                def _matches_table(scope_table: str, source_name: str) -> bool:
                    def norm(s: str) -> str:
                        s = (s or '').strip().strip('[]').strip('"').strip('`')
                        parts = s.split('.')
                        return parts[-1].lower()
                    return norm(scope_table) == norm(source_name)
                
                def _filter_list(items):
                    out = []
                    for it in (items or []):
                        sc = (it or {}).get('scope')
                        if not sc:
                            # No scope means datasource-level
                            out.append(it)
                            continue
                        lvl = str(sc.get('level') or '').lower()
                        if lvl == 'datasource':
                            out.append(it)
                        elif lvl == 'table' and sc.get('table') and _matches_table(str(sc.get('table')), source_name):
                            out.append(it)
                        elif lvl == 'widget':
                            # Widget-level transforms not applicable in pivot
                            pass
                    return out
                
                return {
                    'customColumns': _filter_list(transforms_dict.get('customColumns', [])),
                    'transforms': _filter_list(transforms_dict.get('transforms', [])),
                    'joins': transforms_dict.get('joins', []),
                    'defaults': transforms_dict.get('defaults', {}),
                }
            ds_transforms = _apply_scope_pt(raw_transforms, source)
            
            # Build expr_map for custom column expansions
            for cc in (ds_transforms.get('customColumns') or []):
                if isinstance(cc, dict) and cc.get('name') and cc.get('expr'):
                    expr_map[cc['name']] = cc['expr']
                    sys.stderr.write(f"[PT] Added custom column '{cc.get('name')}': {cc.get('expr')[:50]}...\n")
                    sys.stderr.flush()
            sys.stderr.write(f"[PT] Loaded {len(expr_map)} custom columns: {list(expr_map.keys())}, legend='{legend}'\n")
            sys.stderr.flush()
        except Exception as e:
            sys.stderr.write(f"[PT] Failed to load custom columns: {e}\n")
            sys.stderr.flush()
            pass
    
    # y and legend already defined above for debugging
    measure = payload.get("measure")
    agg = (payload.get("agg") or "count").lower()
    date_field = payload.get("dateField")
    start = payload.get("start")
    end = payload.get("end")
    base_where = payload.get("where") or {}
    
    print(f"[DEBUG period_totals] dateField={date_field}, start={start}, end={end}")
    print(f"[DEBUG period_totals] base_where keys: {list(base_where.keys())}")

    if not date_field or not start or not end:
        raise HTTPException(status_code=400, detail="dateField, start, end are required")

    # For MSSQL: resolve y to canonical column if user-provided name differs by spacing/case
    def _resolve_mssql_column_name(src: str, col: str) -> str:
        try:
            dname = (engine.dialect.name or "").lower()
        except Exception:
            dname = "unknown"
        if not (col and ("mssql" in dname or "sqlserver" in dname) and isinstance(src, str)):
            return col
        # Unquote and split schema.table
        def _unq(s: str) -> str:
            s = s.strip()
            if (s.startswith('[') and s.endswith(']')) or (s.startswith('`') and s.endswith('`')) or (s.startswith('"') and s.endswith('"')):
                return s[1:-1]
            return s
        raw = src.strip()
        parts = [p for p in raw.split('.') if p]
        if len(parts) >= 2:
            schema = _unq(parts[-2])
            table = _unq(parts[-1])
        else:
            schema = 'dbo'
            table = _unq(parts[-1]) if parts else ''
        if not table:
            return col
        # Fetch real column names
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT c.name FROM sys.columns c "
                    "JOIN sys.objects o ON c.object_id=o.object_id "
                    "JOIN sys.schemas s ON o.schema_id=s.schema_id "
                    "WHERE s.name=:schema AND o.name=:table"
                ), {"schema": schema, "table": table}).fetchall()
            names = [str(r[0]) for r in rows]
            # Normalize by removing spaces/underscores and uppercasing
            def key(s: str) -> str:
                return ''.join(ch for ch in s if ch.isalnum()).upper()
            target = key(str(col))
            for n in names:
                if key(n) == target:
                    return n
        except Exception:
            return col
        return col

    if y:
        y = _resolve_mssql_column_name(str(source), str(y))

    # Build value expression (with safe fallback for non-numeric targets)
    # Accept an optional raw measure expression, but guard against empty/aliased strings
    measure_str = str(measure).strip() if (measure is not None) else ""
    if measure_str:
        try:
            # Strip trailing alias if present (e.g., "SUM(x) AS v") to keep a pure expression
            measure_core = re.sub(r"\s+AS\s+.+$", "", measure_str, flags=re.IGNORECASE).strip()
        except Exception:
            measure_core = measure_str
        if not measure_core:
            # Fall back if expression is empty after sanitization
            measure_core = measure_str
        value_expr = f"({measure_core})"
    else:
        # Local helpers using detected dialect
        try:
            dialect_name = "duckdb" if route_duck else (engine.dialect.name or "").lower()  # type: ignore[name-defined]
        except Exception:
            dialect_name = "duckdb" if route_duck else "unknown"
        def _q_ident_local(name: str) -> str:
            s = str(name or '').strip('\n\r\t')
            if not s:
                return s
            if s.startswith('[') and s.endswith(']'):
                return s
            if s.startswith('"') and s.endswith('"'):
                return s
            if s.startswith('`') and s.endswith('`'):
                return s
            if ("mssql" in dialect_name) or ("sqlserver" in dialect_name):
                return f"[{s}]"
            if "mysql" in dialect_name:
                return f"`{s}`"
            return f'"{s}"'
        def _q_source_local(name: str) -> str:
            s = str(name or '').strip()
            if not s:
                return s
            if ("mssql" in dialect_name) or ("sqlserver" in dialect_name):
                parts = s.split('.')
                return '.'.join([p if (p.startswith('[') and p.endswith(']')) else f"[{p}]" for p in parts])
            if "mysql" in dialect_name:
                parts = s.split('.')
                return '.'.join([p if (p.startswith('`') and p.endswith('`')) else f"`{p}`" for p in parts])
            # Default: double-quote each part for DuckDB/Postgres/SQLite
            parts = s.split('.')
            return '.'.join([p if ((p.startswith('"') and p.endswith('"')) or (p.startswith('[') and p.endswith(']')) or (p.startswith('`') and p.endswith('`'))) else f'"{p}"' for p in parts])
        # Expand y if it's a custom column, otherwise quote it
        if y:
            if y in expr_map:
                qy = f"({expr_map[y]})"
            else:
                qy = _q_ident_local(y)
        else:
            qy = None
        
        if agg == "count":
            value_expr = "COUNT(*)"
        elif agg == "distinct" and qy:
            value_expr = f"COUNT(DISTINCT {qy})"
        elif agg in ("avg", "sum", "min", "max") and qy:
            # For DuckDB, cast string numerics (e.g., "1,234.50 ILS") before aggregation
            if route_duck:
                # Try direct cast first; if it's a string, clean it with regexp_replace
                y_clean = f"COALESCE(try_cast({qy} AS DOUBLE), try_cast(regexp_replace(CAST({qy} AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), 0.0)"
                value_expr = f"{agg.upper()}({y_clean})"
            else:
                # Probe numeric for MSSQL only; other engines will error if non-numeric
                is_numeric = True
                try:
                    with engine.connect() as conn:
                        if "mssql" in dialect_name or "sqlserver" in dialect_name:
                            probe_sql = text(f"SELECT TOP 1 TRY_CAST({qy} AS DECIMAL(18,4)) FROM {_q_source_local(source)} WHERE {qy} IS NOT NULL")
                            probe = conn.execute(probe_sql).scalar_one_or_none()
                            is_numeric = probe is not None
                        else:
                            is_numeric = True
                except Exception:
                    is_numeric = True
                value_expr = f"{agg.upper()}({qy})" if is_numeric else "COUNT(*)"
        else:
            # Default to SUM on y if present, else COUNT(*)
            if qy:
                if route_duck:
                    # Try direct cast first; if it's a string, clean it with regexp_replace
                    y_clean = f"COALESCE(try_cast({qy} AS DOUBLE), try_cast(regexp_replace(CAST({qy} AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), 0.0)"
                    value_expr = f"SUM({y_clean})"
                else:
                    value_expr = f"SUM({qy})"
            else:
                value_expr = "COUNT(*)"

    # Determine dialect for quoting/deriving
    try:
        dialect_name = "duckdb" if route_duck else (engine.dialect.name or "").lower()  # type: ignore[name-defined]
    except Exception:
        dialect_name = "duckdb" if route_duck else "unknown"
    def _quote_ident(name: str) -> str:
        s = str(name or '').strip()
        if not s:
            return s
        if s.startswith('[') and s.endswith(']'):
            return s
        if s.startswith('"') and s.endswith('"'):
            return s
        if s.startswith('`') and s.endswith('`'):
            return s
        if ("mssql" in dialect_name) or ("sqlserver" in dialect_name):
            return f"[{s}]"
        if "mysql" in dialect_name:
            return f"`{s}`"
        return f'"{s}"'
    def _derived_lhs2(name: str) -> str:
        raw = str(name or '').strip()
        m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", raw, flags=re.IGNORECASE)
        if not m:
            return _quote_ident(raw)
        base = m.group(1).strip()
        part = m.group(2).strip().lower()
        col = _quote_ident(base)
        if ("mssql" in dialect_name) or ("sqlserver" in dialect_name):
            if part == 'year': return f"YEAR({col})"
            if part == 'quarter': return f"DATEPART(quarter, {col})"
            if part == 'month': return f"MONTH({col})"
            if part == 'month name': return f"DATENAME(month, {col})"
            if part == 'month short': return f"LEFT(DATENAME(month, {col}), 3)"
            if part == 'week': return f"DATEPART(iso_week, {col})"
            if part == 'day': return f"DAY({col})"
            if part == 'day name': return f"DATENAME(weekday, {col})"
            if part == 'day short': return f"LEFT(DATENAME(weekday, {col}), 3)"
            return col
        if ("duckdb" in dialect_name):
            # Return string tokens to safely compare against incoming date-like strings
            if part == 'year': return f"strftime({col}, '%Y')"
            if part == 'quarter': return f"concat(strftime({col}, '%Y'), '-Q', CAST(EXTRACT(QUARTER FROM {col}) AS INTEGER))"
            if part == 'month': return f"strftime({col}, '%Y-%m')"
            if part == 'month name': return f"strftime({col}, '%B')"
            if part == 'month short': return f"strftime({col}, '%b')"
            if part == 'week':
                fmt_week = "%U" if str(payload.get("weekStart") or "mon").lower() == 'sun' else "%W"
                return f"concat(strftime({col}, '%Y'), '-W', substr('00' || strftime({col}, '{fmt_week}'), -2))"
            if part == 'day': return f"strftime({col}, '%Y-%m-%d')"
            if part == 'day name': return f"strftime({col}, '%A')"
            if part == 'day short': return f"strftime({col}, '%a')"
            return col
        if ("postgres" in dialect_name) or ("postgre" in dialect_name):
            if part == 'year': return f"EXTRACT(year FROM {col})"
            if part == 'quarter': return f"EXTRACT(quarter FROM {col})"
            if part == 'month': return f"EXTRACT(month FROM {col})"
            if part == 'month name': return f"to_char({col}, 'FMMonth')"
            if part == 'month short': return f"to_char({col}, 'Mon')"
            if part == 'week': return f"EXTRACT(week FROM {col})"
            if part == 'day': return f"EXTRACT(day FROM {col})"
            if part == 'day name': return f"to_char({col}, 'FMDay')"
            if part == 'day short': return f"to_char({col}, 'Dy')"
            return col
        if "mysql" in dialect_name:
            if part == 'year': return f"YEAR({col})"
            if part == 'quarter': return f"QUARTER({col})"
            if part == 'month': return f"MONTH({col})"
            if part == 'month name': return f"DATE_FORMAT({col}, '%M')"
            if part == 'month short': return f"DATE_FORMAT({col}, '%b')"
            if part == 'week': return f"WEEK({col}, 3)"
            if part == 'day': return f"DAY({col})"
            if part == 'day name': return f"DATE_FORMAT({col}, '%W')"
            if part == 'day short': return f"DATE_FORMAT({col}, '%a')"
            return col
        if "sqlite" in dialect_name:
            if part == 'year': return f"CAST(strftime('%Y', {col}) AS INTEGER)"
            if part == 'quarter':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 "
                    f"WHEN 4 THEN 2 WHEN 5 THEN 2 WHEN 6 THEN 2 "
                    f"WHEN 7 THEN 3 WHEN 8 THEN 3 WHEN 9 THEN 3 "
                    f"ELSE 4 END"
                )
            if part == 'month': return f"CAST(strftime('%m', {col}) AS INTEGER)"
            if part == 'month name':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April' "
                    f"WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' "
                    f"WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December' END"
                )
            if part == 'month short':
                return (
                    f"CASE CAST(strftime('%m', {col}) AS INTEGER) "
                    f"WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr' "
                    f"WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' "
                    f"WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec' END"
                )
            if part == 'week': return f"CAST(strftime('%W', {col}) AS INTEGER)"
            if part == 'day': return f"CAST(strftime('%d', {col}) AS INTEGER)"
            if part == 'day name':
                return (
                    f"CASE strftime('%w', {col}) "
                    f"WHEN '0' THEN 'Sunday' WHEN '1' THEN 'Monday' WHEN '2' THEN 'Tuesday' WHEN '3' THEN 'Wednesday' "
                    f"WHEN '4' THEN 'Thursday' WHEN '5' THEN 'Friday' WHEN '6' THEN 'Saturday' END"
                )
            if part == 'day short':
                return (
                    f"CASE strftime('%w', {col}) "
                    f"WHEN '0' THEN 'Sun' WHEN '1' THEN 'Mon' WHEN '2' THEN 'Tue' WHEN '3' THEN 'Wed' "
                    f"WHEN '4' THEN 'Thu' WHEN '5' THEN 'Fri' WHEN '6' THEN 'Sat' END"
                )
            return col
        return col
    def _pname2(base: str, suffix: str = "") -> str:
        return "w_" + re.sub(r"[^A-Za-z0-9_]", "_", str(base or '')) + suffix

    # Cast to TIMESTAMP for DuckDB to ensure proper date comparison
    where_clauses = [f"{_quote_ident(date_field)} >= CAST(:_start AS TIMESTAMP)", f"{_quote_ident(date_field)} < CAST(:_end AS TIMESTAMP)"]
    params: Dict[str, Any] = {"_start": _coerce_date_like(start), "_end": _coerce_date_like(end)}
    for k, v in base_where.items():
        if v is None:
            where_clauses.append(f"{_derived_lhs2(k)} IS NULL")
        elif isinstance(v, (list, tuple)):
            if len(v) == 0:
                where_clauses.append("1=0")
            else:
                pnames = []
                for i, item in enumerate(v):
                    pname = _pname2(k, f"_{i}")
                    params[pname] = _coerce_date_like(item)
                    pnames.append(f":{pname}")
                where_clauses.append(f"{_derived_lhs2(k)} IN ({', '.join(pnames)})")
        elif isinstance(k, str) and "__" in k:
            base, op = k.split("__", 1)
            opname = None
            if op == "gte": opname = ">="
            elif op == "gt": opname = ">"
            elif op == "lte": opname = "<="
            elif op == "lt": opname = "<"
            elif op == "ne": opname = "!="
            if opname:
                pname = _pname2(base, f"_{op}")
                params[pname] = _coerce_date_like(v)
                where_clauses.append(f"{_derived_lhs2(base)} {opname} :{pname}")
            else:
                pname = _pname2(k)
                where_clauses.append(f"{_derived_lhs2(k)} = :{pname}")
                params[pname] = _coerce_date_like(v)
        else:
            pname = _pname2(k)
            where_clauses.append(f"{_derived_lhs2(k)} = :{pname}")
            params[pname] = _coerce_date_like(v)
    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    # Apply datasource transforms when available so aliases/custom columns resolve
    effective_source = str(source)
    # ds and ds_type already computed above
    if ds is not None:
        try:
            opts = json.loads(ds.options_json or "{}")
        except Exception:
            opts = {}
        # Apply scope filtering: only transforms/customColumns/joins matching this table or datasource-level, or widget-level when widgetId matches
        def _matches_table(_scope_table: str, _source_name: str) -> bool:
            def _norm(_s: str) -> str:
                _s = (_s or '').strip().strip('[]').strip('"').strip('`')
                _parts = _s.split('.')
                return _parts[-1].lower()
            return _norm(_scope_table) == _norm(_source_name)
        def _apply_scope(_ds_tr: dict, _source_name: str) -> dict:
            if not isinstance(_ds_tr, dict):
                return {}
            def _filt(arr):
                out = []
                for it in (arr or []):
                    sc = (it or {}).get('scope')
                    if not sc:
                        out.append(it); continue
                    lvl = str(sc.get('level') or '').lower()
                    if lvl == 'datasource':
                        out.append(it)
                    elif lvl == 'table' and sc.get('table') and _matches_table(str(sc.get('table')), _source_name):
                        out.append(it)
                    elif lvl == 'widget':
                        try:
                            wid = str((sc or {}).get('widgetId') or '').strip()
                            if wid and str(payload.get('widgetId') or '').strip() == wid:
                                out.append(it)
                        except Exception:
                            pass
                return out
            return {
                'customColumns': _filt(_ds_tr.get('customColumns')),
                'transforms': _filt(_ds_tr.get('transforms')),
                'joins': _filt(_ds_tr.get('joins')),
                'defaults': _ds_tr.get('defaults') or {},
            }
        ds_tr_all = _apply_scope((opts or {}).get("transforms") or {}, str(source))
        # Detect if legend references an alias produced by datasource transforms; if so, retain all transforms
        def _collect_aliases(_ds_tr: dict) -> set[str]:
            out: set[str] = set()
            try:
                for cc in (_ds_tr.get('customColumns') or []):
                    nm = str((cc or {}).get('name') or '').strip()
                    if nm: out.add(_norm_name(nm))
            except Exception:
                pass
            try:
                for tr in (_ds_tr.get('transforms') or []):
                    t = str((tr or {}).get('type') or '').lower()
                    nm = ''
                    if t == 'computed': nm = str((tr or {}).get('name') or '')
                    elif t in {'case','replace','translate','nullhandling'}: nm = str((tr or {}).get('target') or '')
                    if nm: out.add(_norm_name(nm))
            except Exception:
                pass
            try:
                for j in (_ds_tr.get('joins') or []):
                    agg = (j or {}).get('aggregate') or None
                    if isinstance(agg, dict):
                        al = str(agg.get('alias') or '').strip()
                        if al: out.add(_norm_name(al))
                    for c in ((j or {}).get('columns') or []):
                        nm = str((c or {}).get('alias') or (c or {}).get('name') or '').strip()
                        if nm: out.add(_norm_name(nm))
            except Exception:
                pass
            return out
        _alias_all = _collect_aliases(ds_tr_all)
        try:
            dialect = ds_type or dialect_name
        except Exception:
            dialect = ds_type or dialect_name
        
        # Probe source table to get base columns for validation
        def _list_source_columns() -> set[str]:
            try:
                # For DuckDB, use native DuckDB connection
                if ds_type == "duckdb" and _duckdb is not None:
                    db_path = settings.duckdb_path
                    if ds and getattr(ds, "connection_encrypted", None):
                        try:
                            dsn = decrypt_text(ds.connection_encrypted)
                            p = urlparse(dsn) if dsn else None
                            if p and (p.scheme or "").startswith("duckdb"):
                                _p = unquote(p.path or "")
                                if _p.startswith("///"):
                                    _p = _p[2:]
                                db_path = _p or db_path
                        except Exception:
                            pass
                    with open_duck_native(db_path) as conn:
                        probe_sql = f"SELECT * FROM {_q_source_local(str(source))} WHERE 1=0"
                        _cur = conn.execute(probe_sql)
                        # DuckDB native cursor follows DB-API: column metadata on .description
                        cols: set[str] = set()
                        try:
                            desc = getattr(conn, 'description', None) or getattr(_cur, 'description', None)
                            if desc:
                                for d in desc:
                                    try:
                                        name = d[0] if isinstance(d, (list, tuple)) else getattr(d, 'name', None)
                                        if name is not None:
                                            cols.add(str(name))
                                    except Exception:
                                        continue
                        except Exception:
                            pass
                        return cols
                else:
                    # For other datasources, use SQLAlchemy engine
                    with engine.connect() as conn:
                        if (ds_type or '').lower() in ("mssql", "mssql+pymssql", "mssql+pyodbc"):
                            probe = text(f"SELECT TOP 0 * FROM {_q_source_local(str(source))} AS s")
                        else:
                            probe = text(f"SELECT * FROM {_q_source_local(str(source))} WHERE 1=0")
                        res = conn.execute(probe)
                        return set([str(c) for c in res.keys()])
            except Exception as e:
                print(f"[WARN] Failed to probe source columns: {e}", file=sys.stderr)
                return set()
        
        _base_cols = _list_source_columns()
        # Drop transforms/custom columns that reference columns not present on base
        ds_transforms = _filter_by_basecols(ds_tr_all, _base_cols)
        # If legend is a plain alias present in transforms, but got filtered out, keep all transforms
        try:
            def _norm_legend_token(x: str) -> str:
                s = str(x or '').strip().strip('"').strip('`').strip('[]')
                if '.' in s and '(' not in s and ')' not in s:
                    try:
                        s = s.split('.')[-1]
                    except Exception:
                        pass
                return s.lower()
            legend_names: set[str] = set()
            if isinstance(legend, (list, tuple)):
                for it in legend:
                    legend_names.add(_norm_legend_token(str(it)))
            elif isinstance(legend, str) and legend:
                legend_names.add(_norm_legend_token(legend))
            # compute aliases produced after filtering
            _alias_filtered = _collect_aliases(ds_transforms)
            if legend_names and any((nm in _alias_all) for nm in legend_names) and not any((nm in _alias_filtered) for nm in legend_names):
                ds_transforms = ds_tr_all
        except Exception:
            pass
        
        result = build_sql(
            dialect=ds_type or dialect,
            source=_q_source_local(str(source)),
            base_select=["*"],
            custom_columns=ds_transforms.get("customColumns", []),
            transforms=ds_transforms.get("transforms", []),
            joins=ds_transforms.get("joins", []),
            defaults={},  # avoid sort/limit in totals context
            limit=None,
        )
        # Handle different return value formats
        if len(result) == 3:
            base_sql, _unused_cols, _warns = result
        elif len(result) == 4:
            base_sql, _unused_cols, _warns, _ = result
        else:
            base_sql = result[0] if result else ""
        effective_source = f"({base_sql}) AS _base"
    else:
        # Local DuckDB or unspecified datasource: quote source to allow spaces/special chars
        effective_source = _q_source_local(str(source))

    # Determine dialect for MSSQL-specific GROUP BY handling
    try:
        dialect_name = (engine.dialect.name or "").lower()
    except Exception:
        dialect_name = "unknown"

    # Normalize legend into a valid SQL expression if provided
    legend_expr = None
    if legend:
        week_start = str(payload.get("weekStart") or "mon").lower()

        # Quote identifiers safely
        def quote_ident(name: str) -> str:
            s = str(name or '').strip()
            if not s:
                return s
            if ("mssql" in dialect_name) or ("sqlserver" in dialect_name):
                return "[" + s.replace("]", "]]" ) + "]"
            if "mysql" in dialect_name:
                return "`" + s.replace("`", "``") + "`"
            # Default (DuckDB/Postgres/SQLite): always double-quote to preserve case and match subquery aliases
            return '"' + s.replace('"', '""') + '"'

        # Map derived date-part like "OrderDate (Year)" to SQL expression per dialect
        def datepart_expr(col: str, kind_l: str) -> str:
            # Always quote the identifier so space/odd names are safe inside functions
            q = quote_ident(col)
            # MSSQL
            if ("mssql" in dialect_name) or ("sqlserver" in dialect_name):
                if kind_l == 'year': return f"CAST(YEAR({q}) AS varchar(10))"
                if kind_l == 'quarter': return f"CAST(YEAR({q}) AS varchar(4)) + '-Q' + CAST(DATEPART(QUARTER, {q}) AS varchar(1))"
                if kind_l == 'month': return f"CONCAT(CAST(YEAR({q}) AS varchar(4)), '-', RIGHT('0' + CAST(MONTH({q}) AS varchar(2)), 2))"
                if kind_l == 'month name': return f"DATENAME(month, {q})"
                if kind_l == 'month short': return f"LEFT(DATENAME(month, {q}), 3)"
                if kind_l == 'week':
                    wn = f"DATEPART(ISO_WEEK, {q})" if week_start == 'mon' else f"DATEPART(WEEK, {q})"
                    return f"CONCAT(CAST(YEAR({q}) AS varchar(4)), '-W', RIGHT('0' + CAST({wn} AS varchar(2)), 2))"
                if kind_l == 'day': return f"CONCAT(CAST(YEAR({q}) AS varchar(4)), '-', RIGHT('0'+CAST(MONTH({q}) AS varchar(2)),2), '-', RIGHT('0'+CAST(DAY({q}) AS varchar(2)),2))"
                if kind_l == 'day name': return f"DATENAME(weekday, {q})"
                if kind_l == 'day short': return f"LEFT(DATENAME(weekday, {q}), 3)"
                return q
            # DuckDB
            if ("duckdb" in dialect_name):
                if kind_l == 'year': return f"strftime({q}, '%Y')"
                if kind_l == 'quarter': return f"concat(strftime({q}, '%Y'), '-Q', CAST(EXTRACT(QUARTER FROM {q}) AS INTEGER))"
                if kind_l == 'month': return f"strftime({q}, '%Y-%m')"
                if kind_l == 'month name': return f"strftime({q}, '%B')"
                if kind_l == 'month short': return f"strftime({q}, '%b')"
                if kind_l == 'week':
                    fmt_week = "%U" if week_start == 'sun' else "%W"
                    return f"concat(strftime({q}, '%Y'), '-W', substr('00' || strftime({q}, '{fmt_week}'), -2))"
                if kind_l == 'day': return f"strftime({q}, '%Y-%m-%d')"
                if kind_l == 'day name': return f"strftime({q}, '%A')"
                if kind_l == 'day short': return f"strftime({q}, '%a')"
                return q
            # Postgres
            if ("postgres" in dialect_name) or ("postgre" in dialect_name):
                if kind_l == 'year': return f"to_char({q}, 'YYYY')"
                if kind_l == 'quarter': return f"to_char({q}, 'YYYY-\"Q\"Q')"
                if kind_l == 'month': return f"to_char({q}, 'YYYY-MM')"
                if kind_l == 'month name': return f"to_char({q}, 'FMMonth')"
                if kind_l == 'month short': return f"to_char({q}, 'Mon')"
                if kind_l == 'week':
                    mode = 'IW' if week_start == 'mon' else 'WW'
                    return f"to_char({q}, 'YYYY') || '-W' || lpad(to_char({q}, '{mode}'), 2, '0')"
                if kind_l == 'day': return f"to_char({q}, 'YYYY-MM-DD')"
                if kind_l == 'day name': return f"to_char({q}, 'FMDay')"
                if kind_l == 'day short': return f"to_char({q}, 'Dy')"
                return q
            # MySQL
            if ("mysql" in dialect_name):
                if kind_l == 'year': return f"DATE_FORMAT({q}, '%Y')"
                if kind_l == 'quarter': return f"CONCAT(DATE_FORMAT({q}, '%Y'), '-Q', QUARTER({q}))"
                if kind_l == 'month': return f"DATE_FORMAT({q}, '%Y-%m')"
                if kind_l == 'month name': return f"DATE_FORMAT({q}, '%M')"
                if kind_l == 'month short': return f"DATE_FORMAT({q}, '%b')"
                if kind_l == 'week':
                    mode = 0 if week_start == 'sun' else 3
                    return f"CONCAT(DATE_FORMAT({q}, '%Y'), '-W', LPAD(WEEK({q}, {mode}), 2, '0'))"
                if kind_l == 'day': return f"DATE_FORMAT({q}, '%Y-%m-%d')"
                if kind_l == 'day name': return f"DATE_FORMAT({q}, '%W')"
                if kind_l == 'day short': return f"DATE_FORMAT({q}, '%a')"
                return q
            # SQLite
            if ("sqlite" in dialect_name):
                if kind_l == 'year': return f"strftime('%Y', {q})"
                if kind_l == 'quarter': return f"printf('%04d-Q%d', CAST(strftime('%Y', {q}) AS INTEGER), ((CAST(strftime('%m', {q}) AS INTEGER)-1)/3)+1)"
                if kind_l == 'month': return f"strftime('%Y-%m', {q})"
                if kind_l == 'month name': return f"strftime('%m', {q})"
                if kind_l == 'month short': return f"strftime('%m', {q})"
                if kind_l == 'week': return f"strftime('%Y', {q}) || '-W' || printf('%02d', CAST(strftime('%W', {q}) AS INTEGER))"
                if kind_l == 'day': return f"strftime('%Y-%m-%d', {q})"
                if kind_l == 'day name': return f"strftime('%w', {q})"
                if kind_l == 'day short': return f"strftime('%w', {q})"
                return q
            return q

        # Build a raw expression for a part: derived date-part, custom column, or quoted identifier
        def part_expr(p: str) -> str:
            s = str(p).strip()
            # Unwrap identifier quoting wrappers if present
            if (s.startswith('[') and s.endswith(']')) or (s.startswith('`') and s.endswith('`')) or (s.startswith('"') and s.endswith('"')):
                s = s[1:-1]
            # Also strip single-quoted wrapper often present in stringified arrays
            if s.startswith("'") and s.endswith("'"):
                s = s[1:-1]
            # Drop simple alias prefixes like "s." or "dbo." if present
            if '.' in s:
                parts = s.split('.')
                if len(parts) == 2 and parts[0] and parts[1]:
                    s = parts[1]
            m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", s, flags=re.IGNORECASE)
            if m:
                base = m.group(1).strip()
                kind = m.group(2).lower()
                # Check if base is a custom column before applying date part
                if base in expr_map:
                    # Expand custom column first, then apply date part
                    expanded = expr_map[base]
                    return datepart_expr(expanded, kind)
                return datepart_expr(base, kind)
            # Check if this is a custom column and expand it
            if s in expr_map:
                return f"({expr_map[s]})"
            return quote_ident(s)

        # Legend may be list or string (or stringified list)
        parts: list[str] = []
        if isinstance(legend, (list, tuple)):
            parts = [part_expr(str(x)) for x in legend]
        elif isinstance(legend, str):
            s = legend.strip()
            if s.startswith("[") and s.endswith("]"):
                inner = s[1:-1]
                items = [seg.strip().strip("'\"") for seg in inner.split(",") if seg.strip()]
                parts = [part_expr(x) for x in items]
            else:
                parts = [part_expr(s)]

        # Concatenate expressions according to dialect
        parts = [p for p in parts if p]
        if parts:
            if ("mssql" in dialect_name) or ("sqlserver" in dialect_name):
                if len(parts) == 1:
                    legend_expr = parts[0]
                else:
                    args: list[str] = []
                    for i, e in enumerate(parts):
                        if i > 0:
                            args.append("' • '")
                        args.append(e)
                    legend_expr = f"CONCAT({', '.join(args)})"
            elif "mysql" in dialect_name:
                legend_expr = f"CONCAT_WS(' • ', {', '.join(parts)})"
            else:
                exprs: list[str] = []
                for i, e in enumerate(parts):
                    if i > 0:
                        exprs.append("' • '")
                    exprs.append(f"COALESCE({e}, '')")
                legend_expr = " || ".join(exprs)

    # Check if SQLGlot should be used
    use_sqlglot = should_use_sqlglot(actorId)
    sql_inner = None
    
    if use_sqlglot:
        # NEW PATH: SQLGlot SQL generation for period totals
        try:
            print(f"[SQLGlot] Period-totals: ENABLED for user={actorId}, dialect={dialect_name}")
            
            # Build expr_map for custom / computed columns using shared helper
            expr_map_local: dict[str, str] = {}
            if ds:
                try:
                    def _pt_apply_scope(ds_tr: dict, source_name: str) -> dict:
                        """Apply basic datasource/table scoping for period_totals.

                        We don't have widgetId here, so we include datasource-level and table-level
                        items, similar to pivot but without widget filtering.
                        """
                        if not isinstance(ds_tr, dict):
                            return {}
                        def norm(s: str) -> str:
                            s = (s or '').strip().strip('[]').strip('"').strip('`')
                            parts = s.split('.')
                            return parts[-1].lower() if parts else ''
                        def filt(arr):
                            out = []
                            for it in (arr or []):
                                sc = (it or {}).get('scope')
                                if not sc:
                                    out.append(it); continue
                                lvl = str(sc.get('level') or '').lower()
                                if lvl == 'datasource':
                                    out.append(it)
                                elif lvl == 'table' and sc.get('table') and norm(str(sc.get('table'))) == norm(source_name):
                                    out.append(it)
                            return out
                        return {
                            'customColumns': filt(ds_tr.get('customColumns')),
                            'transforms': filt(ds_tr.get('transforms')),
                            'joins': ds_tr.get('joins') or [],
                            'defaults': ds_tr.get('defaults') or {},
                        }
                    expr_map_local = _build_expr_map_helper(ds, source, dialect_name, _pt_apply_scope, None)
                    print(f"[SQLGlot] Period-totals: expr_map has {len(expr_map_local)} entries for legend='{legend}'")
                except Exception as e:
                    print(f"[SQLGlot] Period-totals: expr_map build failed: {e}")
                    expr_map_local = {}
            
            # Build the aggregation query using SQLGlot
            builder = SQLGlotBuilder(dialect=dialect_name)
            
            # Handle legend: can be string, list, or None
            legend_field_arg = None
            legend_fields_arg = None

            # Normalize legend for validation
            legend_eff = legend
            legend_names: list[str] = []
            if legend_eff:
                if isinstance(legend_eff, list):
                    legend_names = [str(x) for x in legend_eff]
                else:
                    legend_names = [str(legend_eff)]

            # If legend requests ClientCode but this view has no such custom column,
            # drop legend entirely to avoid DuckDB Binder errors. ClientCode exists
            # as a custom column only on some views (e.g. vault reports), not on
            # main.View_CIT_Invoice_Details_PriceList3.
            if legend_names and "ClientCode" in legend_names and "ClientCode" not in (expr_map_local or {}):
                print("[SQLGlot] Period-totals: legend 'ClientCode' not available on this source; dropping legend for period_totals")
                legend_eff = None
                legend_names = []

            if legend_eff:
                if isinstance(legend_eff, list):
                    if len(legend_eff) == 1:
                        legend_field_arg = legend_eff[0]
                    else:
                        legend_fields_arg = legend_eff
                else:
                    legend_field_arg = legend_eff
            
            # Add date range filters to where clause for SQLGlot
            where_with_dates = {**base_where}
            if date_field and start and end:
                # Use comparison operators for date range
                where_with_dates[f"{date_field}__gte"] = start
                where_with_dates[f"{date_field}__lt"] = end
                print(f"[SQLGlot] Period-totals: Added date filters: {date_field}__gte={start}, {date_field}__lt={end}")
                print(f"[SQLGlot] Period-totals: where_with_dates keys: {list(where_with_dates.keys())}")
            
            # Period totals is essentially an aggregation with optional legend
            sql_inner = builder.build_aggregation_query(
                source=source,  # Fix: was spec_source, should be source
                x_field=None,  # No x-axis for period totals
                y_field=y,
                legend_field=legend_field_arg,
                legend_fields=legend_fields_arg,
                agg=agg,
                where=where_with_dates,  # Now includes date range filters
                group_by=None,  # No time bucketing (already filtered by date range)
                order_by=None,
                order='asc',
                limit=None,
                week_start='mon',
                date_field=None,
                expr_map=expr_map_local,
                ds_type=dialect_name,
            )

            # Safety patch for DuckDB: if legend is a custom column, make sure the
            # generated SQL uses the expanded expression instead of a bare column
            # reference like "ClientCode" which may not exist physically.
            try:
                if legend and isinstance(legend, str) and isinstance(expr_map_local, dict) and legend in expr_map_local:
                    if ("duckdb" in str(dialect_name or "").lower()) and isinstance(sql_inner, str):
                        expr = expr_map_local.get(legend)
                        if expr:
                            # Try a few common patterns produced by SQLGlot
                            patterns = [
                                f'"{legend}" AS legend',
                                f'"{legend}" AS "legend"',
                            ]
                            for pat in patterns:
                                if pat in sql_inner:
                                    patched = sql_inner.replace(pat, f"{expr} AS legend", 1)
                                    print(f"[SQLGlot] Period-totals: Patched legend '{legend}' to expression in SQL")
                                    sql_inner = patched
                                    break
            except Exception as e:
                print(f"[SQLGlot] Period-totals: Legend patch skipped due to error: {e}")

            print(f"[SQLGlot] Period-totals: Generated SQL: {sql_inner[:150]}...")
            
        except Exception as e:
            print(f"[SQLGlot] Period-totals: Error: {e}")
            logger.warning(f"[SQLGlot] Period-totals query failed: {e}")
            if not settings.enable_legacy_fallback:
                logger.error(f"[SQLGlot] Period-totals: LEGACY FALLBACK DISABLED - Re-raising error")
                raise HTTPException(status_code=500, detail=f"SQLGlot query generation failed: {e}")
            print(f"[SQLGlot] Period-totals: Falling back to legacy builder")
            use_sqlglot = False
    
    if not use_sqlglot:
        if legend_expr:
            if "mssql" in dialect_name or "sqlserver" in dialect_name:
                # SQL Server: GROUP BY explicit expression
                sql_inner = f"SELECT {legend_expr} as k, {value_expr} as v FROM {effective_source}{where_sql} GROUP BY {legend_expr}"
            else:
                sql_inner = f"SELECT {legend_expr} as k, {value_expr} as v FROM {effective_source}{where_sql} GROUP BY 1"
        else:
            sql_inner = f"SELECT {value_expr} as v FROM {effective_source}{where_sql}"

    # Caching key
    try:
        counter_inc("query_requests_total", {"endpoint": "period_totals"})
    except Exception:
        pass
    _pt_start = time.perf_counter()
    cache_key = _cache_key("pt", payload.get("datasourceId"), sql_inner, params)
    
    # Debug: Log request details before cache check
    import sys
    # print(f"[DEBUG period_totals] Request: start={start}, end={end}, y={y}, agg={agg}", file=sys.stderr)
    # print(f"[DEBUG period_totals] Params: {params}", file=sys.stderr)
    
    cached = _cache_get(cache_key)
    if cached:
        # print(f"[DEBUG period_totals] CACHE HIT - returning cached data: {cached}", file=sys.stderr)
        cols, rows = cached
        has_legend_rows = bool(rows and rows[0] and len(rows[0]) >= 2)
        if legend and has_legend_rows:
            # Legend output: expect (k, v) per row
            try:
                out = {"totals": {str(r[0]): float(r[1] or 0) for r in (rows or [])}}
                try:
                    counter_inc("query_cache_hit_total", {"endpoint": "period_totals", "kind": "data"})
                except Exception:
                    pass
                try:
                    summary_observe("query_duration_ms", int((time.perf_counter() - _pt_start) * 1000), {"endpoint": "period_totals"})
                except Exception:
                    pass
                return out
            except Exception:
                pass
        # Fallback: treat as single aggregated total
        try:
            v = float(rows[0][0] or 0) if rows and rows[0] else 0.0
            out = {"total": v}
            try:
                counter_inc("query_cache_hit_total", {"endpoint": "period_totals", "kind": "data"})
            except Exception:
                pass
            try:
                summary_observe("query_duration_ms", int((time.perf_counter() - _pt_start) * 1000), {"endpoint": "period_totals"})
            except Exception:
                pass
            return out
        except Exception:
            pass

    _HEAVY_SEM.acquire()
    __actor_acq = False
    __as = _actor_sem(actorId)
    if __as:
        try:
            __as.acquire(); __actor_acq = True
        except Exception:
            pass
    try:
        if route_duck and _duckdb is not None:
            # Native DuckDB execution
            name_order = [m.group(1) for m in re.finditer(r":([A-Za-z_][A-Za-z0-9_]*)", sql_inner)]
            sql_qm = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", "?", sql_inner)
            vals = [params.get(nm) for nm in name_order]
            # Resolve DB path
            db_path = settings.duckdb_path
            if datasource_id and ds and getattr(ds, "connection_encrypted", None):
                try:
                    dsn = decrypt_text(ds.connection_encrypted)
                    p = urlparse(dsn) if dsn else None
                    if p and (p.scheme or "").startswith("duckdb"):
                        _p = unquote(p.path or "")
                        if _p.startswith("///"):
                            _p = _p[2:]
                        db_path = _p or db_path
                        if db_path and db_path != ":memory:" and db_path.startswith("/."):
                            try:
                                db_path = os.path.abspath(db_path[1:])
                            except Exception:
                                pass
                        if ":memory:" in (dsn or "").lower():
                            db_path = ":memory:"
                except Exception:
                    pass
            with open_duck_native(db_path) as conn:
                # Debug: Log SQL and params
                import sys
                # print(f"[DEBUG period_totals] Params: {vals}", file=sys.stderr)
                # print(f"[DEBUG period_totals] Date range: {start} to {end}", file=sys.stderr)
                cur = conn.execute(sql_qm, vals)
                rows = cur.fetchall()
                # print(f"[DEBUG period_totals] Rows returned: {len(rows)}, First row: {rows[0] if rows else 'None'}", file=sys.stderr)
            has_legend_rows = bool(rows and rows[0] and len(rows[0]) >= 2)
            if legend and has_legend_rows:
                # Legend output: expect (k, v)
                out = {"totals": {str(r[0]): float(r[1] or 0) for r in rows}}
                try:
                    _cache_set(cache_key, ["k","v"], [[str(r[0]), float(r[1] or 0)] for r in rows])
                except Exception:
                    pass
            else:
                # Single aggregated total
                val = float(rows[0][0] or 0) if rows and rows[0] else 0.0
                out = {"total": val}
                try:
                    _cache_set(cache_key, ["v"], [[val]])
                except Exception:
                    pass
            try:
                counter_inc("query_cache_miss_total", {"endpoint": "period_totals", "kind": "data"})
            except Exception:
                pass
            try:
                summary_observe("query_duration_ms", int((time.perf_counter() - _pt_start) * 1000), {"endpoint": "period_totals"})
            except Exception:
                pass
            return out
        else:
            # External engines via SQLAlchemy
            engine = _engine_for_datasource(db, datasource_id, actorId)
            with engine.connect() as conn:
                # Dialect-specific statement timeouts
                try:
                    if "postgres" in dialect_name:
                        conn.execute(text("SET statement_timeout = 30000"))
                    elif ("mysql" in dialect_name) or ("mariadb" in dialect_name):
                        conn.execute(text("SET SESSION MAX_EXECUTION_TIME=30000"))
                    elif ("mssql" in dialect_name) or ("sqlserver" in dialect_name):
                        conn.execute(text("SET LOCK_TIMEOUT 30000"))
                except Exception:
                    pass
                result = conn.execute(text(sql_inner), params)
                rows = result.fetchall()
                has_legend_rows = bool(rows and rows[0] and len(rows[0]) >= 2)
                if legend and has_legend_rows:
                    out = {"totals": {str(r[0]): float(r[1] or 0) for r in rows}}
                    try:
                        _cache_set(cache_key, ["k","v"], [[str(r[0]), float(r[1] or 0)] for r in rows])
                    except Exception:
                        pass
                else:
                    val = float(rows[0][0] or 0) if rows and rows[0] else 0.0
                    out = {"total": val}
                    try:
                        _cache_set(cache_key, ["v"], [[val]])
                    except Exception:
                        pass
            try:
                counter_inc("query_cache_miss_total", {"endpoint": "period_totals", "kind": "data"})
            except Exception:
                pass
            try:
                summary_observe("query_duration_ms", int((time.perf_counter() - _pt_start) * 1000), {"endpoint": "period_totals"})
            except Exception:
                pass
            return out
    finally:
        _HEAVY_SEM.release()
        if __actor_acq and __as:
            try:
                __as.release()
            except Exception:
                pass

# --- Period totals batch: accept multiple requests and return a keyed map ---
@router.post("/period-totals/batch")
def period_totals_batch(payload: dict, db: Session = Depends(get_db), actorId: Optional[str] = None, publicId: Optional[str] = None, token: Optional[str] = None) -> dict:
    try:
        touch_actor(actorId)
    except Exception:
        pass
    try:
        gauge_inc("query_inflight", 1.0, {"endpoint": "period_totals_batch"})
    except Exception:
        pass
    if actorId:
        _ra = _throttle_take(actorId)
        if _ra:
            raise HTTPException(status_code=429, detail="Rate limit exceeded", headers={"Retry-After": str(_ra)})
    if isinstance(publicId, str) and publicId:
        sl = get_share_link_by_public(db, publicId)
        if not sl:
            raise HTTPException(status_code=404, detail="Not found")
        if not verify_share_link_token(sl, token if isinstance(token, str) else None, settings.secret_key):
            raise HTTPException(status_code=401, detail="Unauthorized")
    """Batch variant of period-totals.

    Payload:
      { "requests": [ { key?: str, ...period-totals payload... }, ... ] }

    Returns:
      { "results": { [key]: period_totals_result, ... } }
    """
    try:
        counter_inc("query_requests_total", {"endpoint": "period_totals_batch"})
    except Exception:
        pass
    _bt_start = time.perf_counter()
    reqs = payload.get("requests") or []
    if not isinstance(reqs, list):
        raise HTTPException(status_code=400, detail="requests must be an array")
    results: Dict[str, Any] = {}
    for i, item in enumerate(reqs):
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or i)
        # Reuse the same logic as single endpoint
        results[key] = period_totals(item, db, actorId)
    try:
        summary_observe("query_duration_ms", int((time.perf_counter() - _bt_start) * 1000), {"endpoint": "period_totals_batch"})
    except Exception:
        pass
    try:
        gauge_dec("query_inflight", 1.0, {"endpoint": "period_totals_batch"})
    except Exception:
        pass
    return {"results": results}


# --- Period totals compare: return cur and prev in one call ---
@router.post("/period-totals/compare")
def period_totals_compare(payload: dict, db: Session = Depends(get_db), actorId: Optional[str] = None, publicId: Optional[str] = None, token: Optional[str] = None) -> dict:
    # Resolve date presets at execution time
    if payload.get("where"):
        payload["where"] = _resolve_date_presets(payload["where"])
    try:
        touch_actor(actorId)
    except Exception:
        pass
    try:
        gauge_inc("query_inflight", 1.0, {"endpoint": "period_totals_compare"})
    except Exception:
        pass
    if actorId:
        _ra = _throttle_take(actorId)
        if _ra:
            raise HTTPException(status_code=429, detail="Rate limit exceeded", headers={"Retry-After": str(_ra)})
    if isinstance(publicId, str) and publicId:
        sl = get_share_link_by_public(db, publicId)
        if not sl:
            raise HTTPException(status_code=404, detail="Not found")
        if not verify_share_link_token(sl, token if isinstance(token, str) else None, settings.secret_key):
            raise HTTPException(status_code=401, detail="Unauthorized")
    """Compare variant: computes current and previous windows in one call.

    Payload keys include single-call keys plus prevStart, prevEnd.
    { source, datasourceId?, y?, measure?, agg?, dateField, start, end, prevStart, prevEnd, where?, legend?, weekStart? }
    """
    required = ["source", "dateField", "start", "end", "prevStart", "prevEnd"]
    for k in required:
        if payload.get(k) is None:
            raise HTTPException(status_code=400, detail=f"{k} is required")
    base: Dict[str, Any] = {
        "source": payload.get("source"),
        "datasourceId": payload.get("datasourceId"),
        "y": payload.get("y"),
        "measure": payload.get("measure"),
        "agg": payload.get("agg"),
        "dateField": payload.get("dateField"),
        "where": payload.get("where"),
        "legend": payload.get("legend"),
        "weekStart": payload.get("weekStart"),
    }
    cur_payload = { **base, "start": payload.get("start"), "end": payload.get("end") }
    prev_payload = { **base, "start": payload.get("prevStart"), "end": payload.get("prevEnd") }
    try:
        counter_inc("query_requests_total", {"endpoint": "period_totals_compare"})
    except Exception:
        pass
    _cmp_start = time.perf_counter()
    cur = period_totals(cur_payload, db, actorId)
    prev = period_totals(prev_payload, db, actorId)
    try:
        summary_observe("query_duration_ms", int((time.perf_counter() - _cmp_start) * 1000), {"endpoint": "period_totals_compare"})
    except Exception:
        pass
    try:
        gauge_dec("query_inflight", 1.0, {"endpoint": "period_totals_compare"})
    except Exception:
        pass
    return { "cur": cur, "prev": prev }
