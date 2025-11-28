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

from ..db import get_duckdb_engine, get_engine_from_dsn, open_duck_native
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
        ds_transforms = opts.get("transforms") or {}
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
    return (s or '').strip().strip('[]').strip('"').strip('`').split('.')[-1].lower()


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
        refs = _referenced_cols_in_expr(expr)
        return ((not refs) or refs.issubset(allowed), name)

    def _can_accept_tr(tr: dict) -> tuple[bool, str | None]:
        t = str((tr or {}).get('type') or '').lower()
        if t == 'computed':
            name = _norm_name(str((tr or {}).get('name') or ''))
            refs = _referenced_cols_in_expr(str(tr.get('expr') or ''))
            return ((not refs) or refs.issubset(allowed), name)
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
        s = str(name or '').strip()
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
        all_custom_cols = ((opts or {}).get("transforms") or {}).get('customColumns', [])
        sys.stderr.write(f"[Pivot] Total custom columns before scope filter: {len(all_custom_cols)}\n")
        for col in all_custom_cols:
            scope = col.get('scope') or {}
            sys.stderr.write(f"[Pivot]   - {col.get('name')}: level={scope.get('level')}, table={scope.get('table')}\n")
        sys.stderr.flush()
        ds_transforms = _apply_scope((opts or {}).get("transforms") or {}, payload.source)
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
                # Match qualified identifiers (quoted): "s"."Col" or [s].[Col] - keep only column part
                for match in re.finditer(r'["\[`][^"\]`]+["\]`]\.["\[`]([^"\]`]+)["\]`]', expr_str):
                    refs.add(match.group(1).lower())
                # Match qualified identifiers (unquoted): s.Col - keep only column part
                for match in re.finditer(r'\b[a-z_][a-z0-9_]*\.([A-Za-z_][A-Za-z0-9_]*)\b', expr_str, re.IGNORECASE):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                # Match quoted identifiers: "col", [col], `col`
                for match in re.finditer(r'["\[`]([^"\]`]+)["\]`]', expr_str):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                # Also match parenthesized bare identifiers: (col)
                for match in re.finditer(r'\(([A-Za-z_][A-Za-z0-9_]*)\)', expr_str):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                return refs
            
            # Filter custom columns - track aliases as they're added
            # Separate custom columns into two groups:
            # 1. "Leaf" columns: only reference base table columns
            # 2. "Derived" columns: reference other custom column aliases
            print(f"[Pivot] Filtering {len(__custom_cols_all)} custom columns...")
            available_with_aliases = available_cols_lower.copy()  # Start with base columns
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
                
                # Check if this column references any custom column aliases (not just base columns)
                refs_custom_aliases = refs - available_cols_lower
                if refs_custom_aliases:
                    # This column references other custom columns - exclude from _base subquery
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
        
            # Also filter computed transforms
            print(f"[Pivot] Filtering {len(__transforms_eff)} transforms...")
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
                        print(f"[Pivot] OK including computed transform '{name}'")
                __transforms_eff_filtered.append(t)
            __transforms_eff = __transforms_eff_filtered
            print(f"[Pivot] Final: {len(__custom_cols_eff)} custom columns, {len(__transforms_eff)} transforms")
        except Exception as e:
            import traceback
            print(f"[Pivot] ERROR filtering custom columns: {e}")
            print(f"[Pivot] Traceback: {traceback.format_exc()}")
            print(f"[Pivot] Fallback: using all {len(__custom_cols_all)} custom columns")
            __custom_cols_eff = list(__custom_cols_all)
            # Keep the already-filtered __transforms_eff as-is (don't overwrite)

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
        # Check if it's a valid custom column even if numeric
        is_valid_custom_col = is_numeric_name and (val_field in (expr_map or {}))
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
    print(f"[PIVOT] About to check should_use_sqlglot with actorId={actorId}")
    import sys
    sys.stdout.flush()
    use_sqlglot = should_use_sqlglot(actorId)
    print(f"[PIVOT] should_use_sqlglot returned: {use_sqlglot}")
    sys.stdout.flush()
    # SQLGlot now properly handles DuckDB custom columns by materializing them in _base subquery (lines 1700-1747)
    inner = None
    
    if use_sqlglot:
        # NEW PATH: SQLGlot pivot query generation
        try:
            print(f"[SQLGlot] Pivot: ENABLED for user={actorId}, dialect={ds_type}")
            
            # Initialize transforms early (needed by filtering logic below)
            __transforms_eff = ds_transforms.get('transforms', []) if isinstance(ds_transforms, dict) else []
            
            # Probe available columns from base table, then filter joins, then probe with filtered joins
            # Only probe for DuckDB - for remote datasources, skip probing
            available_cols = set()
            probe_joins_filtered = []
            if ds_type == 'duckdb':
                try:
                    # PHASE 1: Probe base table WITHOUT joins to get base columns
                    print(f"[SQLGlot] Pivot: Phase 1 - Probing base table without joins")
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
                    all_joins = ds_transforms.get("joins", []) if ds_transforms else []
                    base_cols_lower = {c.lower() for c in base_cols}
                    for join in all_joins:
                        source_key = str((join or {}).get('sourceKey') or '').strip()
                        if source_key and source_key.lower() in base_cols_lower:
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
            print(f"[SQLGlot] Pivot: ds_info is None: {ds_info is None}, ds_transforms custom cols: {len(ds_transforms.get('customColumns', [])) if ds_transforms else 0}", flush=True)
            
            # Build expr_map from ds_transforms (already scope-filtered) when ds_info is None
            if ds_info:
                expr_map = _build_expr_map_helper(ds_info, payload.source, ds_type, _apply_scope, None)
            else:
                # Fallback: build expr_map directly from ds_transforms (already loaded and scope-filtered above)
                from ..sqlgen import _normalize_expr_idents
                expr_map = {}
                if ds_transforms:
                    # From customColumns
                    for col in (ds_transforms.get("customColumns") or []):
                        if isinstance(col, dict) and col.get("name") and col.get("expr"):
                            expr = _normalize_expr_idents(ds_type, col["expr"])
                            expr_map[col["name"]] = expr
                    # From computed transforms
                    for t in (ds_transforms.get("transforms") or []):
                        if isinstance(t, dict) and t.get("type") == "computed":
                            if t.get("name") and t.get("expr"):
                                expr = _normalize_expr_idents(ds_type, t["expr"])
                                expr_map[t["name"]] = expr
            
            print(f"[SQLGlot] Pivot: expr_map has {len(expr_map)} entries: {list(expr_map.keys())}", flush=True)
            
            # Helper to extract column references (re is imported at module level)
            def extract_refs_sg(expr_str: str) -> set[str]:
                refs = set()
                # Match qualified identifiers (quoted): "s"."Col" or [s].[Col] - keep only column part
                for match in re.finditer(r'["\[`][^"\]`]+["\]`]\.["\[`]([^"\]`]+)["\]`]', expr_str):
                    refs.add(match.group(1).lower())
                # Match qualified identifiers (unquoted): s.Col - keep only column part
                for match in re.finditer(r'\b[a-z_][a-z0-9_]*\.([A-Za-z_][A-Za-z0-9_]*)\b', expr_str, re.IGNORECASE):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                # Match quoted identifiers: "col", [col], `col`
                for match in re.finditer(r'["\[`]([^"\]`]+)["\]`]', expr_str):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                # Also match parenthesized bare identifiers: (col)
                for match in re.finditer(r'\(([A-Za-z_][A-Za-z0-9_]*)\)', expr_str):
                    col = match.group(1).lower()
                    if col != 's':  # Skip table alias
                        refs.add(col)
                return refs
            
            # Filter custom columns to match available columns
            __custom_cols_sqlglot = []
            # Always validate custom columns if we have probed columns - even for DuckDB
            # This prevents including columns that reference non-existent base columns
            if available_cols:
                available_cols_lower = {c.lower() for c in available_cols}
                # Track available columns including aliases as we add them
                available_with_aliases_sg = available_cols_lower.copy()
                custom_cols_leaf_sg = []  # Only reference base columns
                
                for cc in (ds_transforms.get("customColumns", []) if ds_transforms else []):
                    if isinstance(cc, dict) and cc.get("name") and cc.get("expr"):
                        expr_str = str(cc.get("expr") or "")
                        refs = extract_refs_sg(expr_str)
                        missing = refs - available_with_aliases_sg
                        print(f"[SQLGlot] Column '{cc['name']}': expr='{expr_str[:60]}', refs={refs}, missing={missing}")
                        if missing:
                            print(f"[SQLGlot] SKIP custom column '{cc['name']}': references missing columns {missing}")
                            continue
                        
                        # Check if this column references any custom column aliases (not just base columns)
                        refs_custom_aliases = refs - available_cols_lower
                        if refs_custom_aliases:
                            # This column references other custom columns - exclude from _base subquery
                            print(f"[SQLGlot]  Including custom column '{cc['name']}' (derived, will be computed in outer query)")
                        else:
                            # This column only references base columns - include in _base subquery
                            print(f"[SQLGlot]  Including custom column '{cc['name']}' (leaf)")
                            custom_cols_leaf_sg.append(cc)
                        
                        # Add this custom column's alias to available columns for subsequent checks
                        available_with_aliases_sg.add(cc['name'].lower())
                
                # Only leaf columns go into __custom_cols_sqlglot (for _base subquery)
                # But keep ALL custom columns in expr_map (including derived) for resolution
                __custom_cols_sqlglot = custom_cols_leaf_sg
                print(f"[SQLGlot] Filtered __custom_cols_sqlglot to {len(custom_cols_leaf_sg)} leaf columns (derived columns will be resolved in outer query)")
                
                # Also filter computed transforms
                __transforms_eff_sqlglot = []
                for t in __transforms_eff:
                    if isinstance(t, dict) and t.get("type") == "computed":
                        if t.get("name") and t.get("expr"):
                            refs = extract_refs_sg(str(t.get("expr") or ""))
                            missing = refs - available_cols_lower
                            if missing:
                                print(f"[SQLGlot] Skipping computed transform '{t['name']}': references missing columns {missing}")
                                continue
                    __transforms_eff_sqlglot.append(t)
            else:
                # Probe failed - apply leaf/derived separation without column checking
                print(f"[SQLGlot] Probe failed - including all custom columns without base column validation")
                available_with_aliases_sg = set()
                custom_cols_leaf_sg = []
                
                for cc in (ds_transforms.get("customColumns", []) if ds_transforms else []):
                    if isinstance(cc, dict) and cc.get("name") and cc.get("expr"):
                        expr_str = str(cc.get("expr") or "")
                        refs = extract_refs_sg(expr_str)
                        print(f"[SQLGlot] Column '{cc['name']}': expr='{expr_str[:60]}', refs={refs}", flush=True)
                        # Check if this column references any previously seen custom column aliases
                        refs_custom_aliases = refs & available_with_aliases_sg
                        if refs_custom_aliases:
                            # This column references other custom columns - need to materialize in ORDER
                            print(f"[SQLGlot]  Including custom column '{cc['name']}' (derived from {refs_custom_aliases})", flush=True)
                            custom_cols_leaf_sg.append(cc)  # Include derived columns too!
                        else:
                            # This column only references base columns - include in _base subquery
                            print(f"[SQLGlot]  Including custom column '{cc['name']}' (leaf)", flush=True)
                            custom_cols_leaf_sg.append(cc)
                        # Add this custom column's alias to available columns for subsequent checks
                        available_with_aliases_sg.add(cc['name'].lower())
                
                # For pivot, ALL custom columns must be materialized in the inner _base subquery
                # (including derived ones) so they can be used in GROUP BY and aggregations
                __custom_cols_sqlglot = custom_cols_leaf_sg
                print(f"[SQLGlot] Including {len(custom_cols_leaf_sg)} custom columns in _base subquery", flush=True)
                
                # Filter transforms - even for DuckDB synced tables, validate against probed columns if available
                __transforms_eff_sqlglot = []
                if available_cols:
                    # We have probed columns - use them to filter transforms
                    available_cols_lower = {c.lower() for c in available_cols}
                    print(f"[SQLGlot] Filtering {len(__transforms_eff)} transforms using {len(available_cols)} probed columns...")
                    for t in __transforms_eff:
                        if not isinstance(t, dict):
                            __transforms_eff_sqlglot.append(t)
                            continue
                            
                        if t.get("type") == "computed":
                            name = t.get("name")
                            expr = t.get("expr")
                            if name and expr:
                                expr_normalized = normalize_sql_expression(str(expr), ds_type or 'duckdb')
                                refs = extract_refs_sg(expr_normalized)
                                missing = refs - available_cols_lower
                                if missing:
                                    print(f"[SQLGlot] SKIP computed transform '{name}': references missing columns {missing}")
                                    continue
                                print(f"[SQLGlot] OK including computed transform '{name}'")
                        __transforms_eff_sqlglot.append(t)
                else:
                    # No probed columns - can't validate, use all transforms
                    print(f"[SQLGlot] No probed columns available - using all {len(__transforms_eff)} transforms without validation")
                    __transforms_eff_sqlglot = __transforms_eff
                print(f"[SQLGlot] Final: {len(__custom_cols_sqlglot)} custom columns, {len(__transforms_eff_sqlglot)} transforms")
            
            # If datasource transforms exist, use transformed subquery as source
            # This ensures custom columns and joins are available to pivot dimensions
            effective_source = payload.source
            if ds_transforms:
                # Use filtered joins from probe phase (if available), otherwise use unfiltered joins
                # probe_joins_filtered will be populated for DuckDB if probe succeeded
                joins_to_use = probe_joins_filtered if (ds_type == 'duckdb' and probe_joins_filtered is not None) else (ds_transforms.get("joins", []) or [])
                print(f"[SQLGlot] Pivot: Using {len(joins_to_use)} joins for final query")
                
                # Extract base_sql from legacy builder's construction (lines 1009-1018)
                # This applies custom columns, transforms, and joins
                result = build_sql(
                    dialect=ds_type,
                    source=_q_source(payload.source),
                    base_select=["*"],
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
                print(f"[SQLGlot] Pivot: Using transformed source (has {len(ds_transforms.get('customColumns', []))} customColumns, {len(__joins_eff)} joins)")
            else:
                print(f"[SQLGlot] Pivot: Using direct source (no transforms)")
            
            builder = SQLGlotBuilder(dialect=ds_type)
            # If source is a transformed subquery (_base), custom columns are already materialized
            # Don't pass expr_map to avoid double-expansion in outer query
            use_expr_map = expr_map if "_base" not in effective_source else None
            print(f"[SQLGlot] Pivot: effective_source contains _base: {'_base' in effective_source}")
            print(f"[SQLGlot] Pivot: use_expr_map is None: {use_expr_map is None}")
            print(f"[SQLGlot] Pivot: expr_map keys: {list(expr_map.keys()) if expr_map else 'None'}")
            if not use_expr_map and expr_map:
                print(f"[SQLGlot] Pivot: NOT passing expr_map to build_pivot_query (custom columns already materialized in _base)")
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
            )
            print(f"[SQLGlot] Pivot: Generated SQL: {inner[:150]}...")
            import sys
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

        # Unlimited mode: page through results until exhaustion
        page_size = 50000
        all_rows: list[list[Any]] = []
        cols: list[str] | None = None
        offset = 0
        start_time = time.perf_counter()
        sys.stderr.write(f"[DEBUG] Starting unlimited mode pagination with page_size={page_size}...\n")
        sys.stderr.flush()
        while True:
            q = QueryRequest(
                sql=inner,
                datasourceId=payload.datasourceId,
                limit=page_size,
                offset=offset,
                includeTotal=False,
                params=params or None,
            )
            sys.stderr.write(f"[DEBUG] Calling run_query for page at offset={offset}...\n")
            sys.stderr.flush()
            res = run_query(q, db, actorId=actorId, publicId=publicId, token=token)
            sys.stderr.write(f"[DEBUG] Got {len(res.rows or [])} rows from run_query\n")
            sys.stderr.flush()
            if cols is None:
                cols = list(res.columns or [])
            page_rows = list(res.rows or [])
            all_rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            offset += page_size
        elapsed = int((time.perf_counter() - start_time) * 1000)
        return QueryResponse(columns=cols or [], rows=all_rows, elapsedMs=elapsed, totalRows=len(all_rows))
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
            
            # For remote datasources, custom columns are materialized in _base subquery
            # so we don't need expr_map. For local DuckDB, we also don't build it here.
            expr_map = {}
            
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
                source=payload.source,
                rows=r_dims,
                cols=c_dims,
                value_field=val_field if val_field else None,
                agg=agg,
                where=payload.where,
                group_by=payload.groupBy if hasattr(payload, 'groupBy') else None,
                week_start=payload.weekStart if hasattr(payload, 'weekStart') else 'mon',
                limit=payload.limit,
                expr_map=expr_map,
                ds_type=ds_type,
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
        s = str(name or '').strip()
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
                db_path = settings.duckdb_path
            else:
                # If this is a DuckDB datasource with a connection URI, try to extract the file path; else default to local
                if ds_obj and getattr(ds_obj, "connection_encrypted", None):
                    try:
                        dsn = decrypt_text(ds_obj.connection_encrypted)
                        p = urlparse(dsn) if dsn else None
                        db_path = settings.duckdb_path
                        if p and (p.scheme or "").startswith("duckdb"):
                            _p = unquote(p.path or "")
                            if _p.startswith("///"):
                                _p = _p[2:]
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
                        db_path = settings.duckdb_path
                else:
                    db_path = settings.duckdb_path

            # Replace named params in the inner SQL with positional '?' for duckdb
            inner_qm = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", "?", sql_inner)
            sql_native = f"SELECT * FROM ({inner_qm}) AS _q LIMIT {limit_lit} OFFSET {offset_lit}"
            # Build positional values list in order of occurrence
            values = [params.get(nm) for nm in name_order]

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
                with open_duck_native(db_path) as conn:
                    cur = conn.execute(sql_native, values)
                    raw_rows = cur.fetchall()
                    desc = getattr(cur, 'description', None) or []
                    cols = [str(col[0]) for col in desc]
                rows = [[_json_safe_cell(x) for x in r] for r in raw_rows]
                _cache_set(key, cols, rows)

            total_rows = None
            if payload.includeTotal:
                cnt_key = _cache_key("count", payload.datasourceId, sql_inner, params)
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
        s = str(name or '').strip()
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
            ds_transforms = opts.get("transforms") or {}
            ds_transforms = _apply_scope(ds_transforms, source_name)
            
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
            for t in transforms:
                if isinstance(t, dict) and t.get("type") == "computed":
                    if t.get("name") and t.get("expr"):
                        # Normalize bracket identifiers for target dialect
                        from ..sqlgen import _normalize_expr_idents
                        expr = _normalize_expr_idents(ds_type, t["expr"])
                        expr_map[t["name"]] = expr
        
        except Exception as e:
            logger.error(f"[SQLGlot] Failed to build expr_map: {e}")
        
        return expr_map
    
    # Helper: resolve derived columns in WHERE clause
    def _resolve_derived_columns_in_where(where: dict, ds: Any, source_name: str, ds_type: str) -> dict:
        """Resolve derived column names to SQL expressions in WHERE clause"""
        print(f"[SQLGlot] _resolve_derived_columns_in_where CALLED with where keys: {list(where.keys()) if where else 'None'}")
        
        if not where:
            return where
        
        if not ds:
            print("[SQLGlot] No datasource provided for resolution")
            return where
        
        try:
            # Build expr_map from datasource
            expr_map = _build_expr_map(ds, source_name, ds_type)
            
            # Resolve WHERE clause
            print(f"[SQLGlot] Built expr_map with {len(expr_map)} entries: {list(expr_map.keys())}")
            print(f"[SQLGlot] WHERE keys to resolve: {list(where.keys())}")
            
            resolved = {}
            resolved_count = 0
            for key, value in where.items():
                # First check if it's a custom column
                if key in expr_map:
                    expr = expr_map[key]
                    # Strip table aliases - handle both quoted and unquoted (e.g., s.ClientID or "s"."ClientID" -> ClientID)
                    expr = re.sub(r'"[a-z][a-z_]{0,4}"\.', '', expr)  # Quoted aliases like "s".
                    expr = re.sub(r'\b[a-z][a-z_]{0,4}\.', '', expr)  # Unquoted aliases like s.
                    print(f"[SQLGlot] [OK] Resolved custom column '{key}' -> {expr[:80]}...")
                    resolved[f"({expr})"] = value
                    resolved_count += 1
                # Check if it's a date part pattern like "OrderDate (Year)"
                elif " (" in key and ")" in key:
                    match = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", key, flags=re.IGNORECASE)
                    if match:
                        base_col = match.group(1).strip()
                        kind = match.group(2).lower()
                        expr = _build_datepart_expr(base_col, kind, ds_type)
                        print(f"[SQLGlot] [OK] Resolved date part '{key}' -> {expr[:80]}...")
                        resolved[f"({expr})"] = value
                        resolved_count += 1
                    else:
                        resolved[key] = value
                else:
                    resolved[key] = value
            
            print(f"[SQLGlot] Resolution complete: {resolved_count}/{len(where)} columns resolved")
            return resolved
            
        except Exception as e:
            logger.error(f"[SQLGlot] Failed to resolve derived columns: {e}", exc_info=True)
            print(f"[SQLGlot] Failed to resolve derived columns: {e}")
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
                    out.append(it); continue
                lvl = str(sc.get('level') or '').lower()
                if lvl == 'datasource':
                    out.append(it)
                elif lvl == 'table' and sc.get('table') and _matches_table(str(sc.get('table')), source_name):
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
            s = str(name or '').strip()
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
            """Get SQL expression for WHERE clause. Use quoted column if it exists in transformed base."""
            m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
            if m and ds_transforms and _actual_cols and key in _actual_cols:
                return _q_ident(key)
            return _derived_lhs(key)
        where_clauses = []
        params: Dict[str, Any] = {}
        if payload.spec.where:
            for k, v in payload.spec.where.items():
                if k in ("start", "startDate", "end", "endDate"):
                    continue
                if v is None:
                    where_clauses.append(f"{_where_lhs(k)} IS NULL")
                elif isinstance(v, (list, tuple)):
                    if len(v) == 0:
                        continue
                    pnames = []
                    for i, item in enumerate(v):
                        pname = _pname(k, f"_{i}")
                        params[pname] = _coerce_filter_value(k, item)
                        pnames.append(f":{pname}")
                    where_clauses.append(f"{_where_lhs(k)} IN ({', '.join(pnames)})")
                elif isinstance(k, str) and "__" in k:
                    base, op = k.split("__", 1)
                    opname = None
                    if op == "gte": opname = ">="
                    elif op == "gt": opname = ">"
                    elif op == "lte": opname = "<="
                    elif op == "lt": opname = "<"
                    elif op == "ne": opname = "!="
                    if opname:
                        pname = _pname(base, f"_{op}")
                        params[pname] = _coerce_filter_value(base, v)
                        where_clauses.append(f"{_where_lhs(base)} {opname} :{pname}")
                    else:
                        pname = _pname(k)
                        where_clauses.append(f"{_where_lhs(k)} = :{pname}")
                        params[pname] = _coerce_filter_value(k, v)
                else:
                    pname = _pname(k)
                    where_clauses.append(f"{_where_lhs(k)} = :{pname}")
                    params[pname] = _coerce_filter_value(k, v)
        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        sql_inner = f"SELECT * FROM ({base_sql}) AS _base{where_sql}"
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
            
            if _validated_x:
                # Allow derived date parts like "OrderDate (Year)" even if base column not in available_cols
                is_derived_x = bool(re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(_validated_x), flags=re.IGNORECASE))
                if is_derived_x:
                    # Keep the derived field as-is
                    pass
                else:
                    x_norm = _norm_name(_validated_x)
                    if x_norm and x_norm not in available_cols:
                        _validated_x = None
                    else:
                        _validated_x = canonical_map.get(x_norm, _validated_x)
            if _validated_y:
                y_norm = _norm_name(_validated_y)
                if y_norm and y_norm not in available_cols:
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
                    _validated_legend = canonical_map.get(ln, _validated_legend)
            
            # Also validate WHERE clause filters - remove invalid column references
            _validated_where = {}
            if spec.where:
                for k, v in spec.where.items():
                    # Skip special date range keys
                    if k in ("start", "startDate", "end", "endDate"):
                        _validated_where[k] = v
                        continue
                    # Extract base column name (remove __ operators like ClientType__in)
                    base_col = k.split("__")[0] if "__" in k else k
                    # Preserve derived date part filters (e.g., "OrderDate (Year)")
                    if re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(base_col), flags=re.IGNORECASE):
                        _validated_where[k] = v
                        continue
                    col_norm = _norm_name(base_col)
                    if col_norm in available_cols:
                        _validated_where[k] = v
            else:
                _validated_where = spec.where
            
            # Override spec fields with validated values for this query
            spec = payload.spec.model_copy(update={
                'x': _validated_x, 
                'y': _validated_y, 
                'legend': _validated_legend,
                'where': _validated_where
            })
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
            
            # Also validate WHERE clause filters
            _validated_where = {}
            if spec.where:
                for k, v in spec.where.items():
                    if k in ("start", "startDate", "end", "endDate"):
                        _validated_where[k] = v
                        continue
                    base_col = k.split("__")[0] if "__" in k else k
                    # Preserve derived date part filters (e.g., "OrderDate (Year)")
                    if re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(base_col), flags=re.IGNORECASE):
                        _validated_where[k] = v
                        continue
                    if _norm_name(base_col) in available_cols_direct:
                        _validated_where[k] = v
            else:
                _validated_where = spec.where
            
            spec = payload.spec.model_copy(update={
                'x': _validated_x, 
                'y': _validated_y, 
                'legend': _validated_legend,
                'where': _validated_where
            })
        # Handle x as either string or array (extract first element if array)
        x_raw = spec.x or (spec.select[0] if spec.select else None)
        if isinstance(x_raw, (list, tuple)) and len(x_raw) > 0:
            x_col = x_raw[0]
        else:
            x_col = x_raw
        if not x_col and not (spec.legend or legend_orig):
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
                """Get SQL expression for WHERE clause. Use quoted column if it exists in transformed base."""
                m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
                if m and ds_transforms and _actual_cols and key in _actual_cols:
                    return _q_ident(key)
                return _derived_lhs(key)
            
            where_clauses = []
            params: Dict[str, Any] = {}
            if spec.where:
                for k, v in spec.where.items():
                    # Skip global keys that aren't real columns
                    if k in ("start", "startDate", "end", "endDate"):
                        continue
                    if v is None:
                        where_clauses.append(f"{_where_lhs(k)} IS NULL")
                    elif isinstance(v, (list, tuple)):
                        if len(v) == 0:
                            # Ignore empty lists: no-op filter (UI chip added but no values selected)
                            continue
                        pnames = []
                        for i, item in enumerate(v):
                            pname = _pname(k, f"_{i}")
                            params[pname] = _coerce_filter_value(k, item)
                            pnames.append(f":{pname}")
                        where_clauses.append(f"{_where_lhs(k)} IN ({', '.join(pnames)})")
                    elif isinstance(k, str) and "__" in k:
                        base, op = k.split("__", 1)
                        opname = None
                        if op == "gte": opname = ">="
                        elif op == "gt": opname = ">"
                        elif op == "lte": opname = "<="
                        elif op == "lt": opname = "<"
                        elif op == "ne": opname = "!="
                        if opname:
                            pname = _pname(base, f"_{op}")
                            params[pname] = _coerce_filter_value(base, v)
                            where_clauses.append(f"{_where_lhs(base)} {opname} :{pname}")
                        elif op in {"contains", "notcontains", "startswith", "endswith"}:
                            if op == "notcontains":
                                cmp = "NOT LIKE"; patt = f"%{v}%"
                            elif op == "contains":
                                cmp = "LIKE"; patt = f"%{v}%"
                            elif op == "startswith":
                                cmp = "LIKE"; patt = f"{v}%"
                            else:
                                cmp = "LIKE"; patt = f"%{v}"
                            pname = _pname(base, "_like")
                            params[pname] = patt
                            where_clauses.append(f"{_where_lhs(base)} {cmp} :{pname}")
                        else:
                            pname = _pname(base, "_eq")
                            where_clauses.append(f"{_where_lhs(base)} = :{pname}")
                            params[pname] = _coerce_filter_value(base, v)
                    else:
                        pname = _pname(k)
                        where_clauses.append(f"{_where_lhs(k)} = :{pname}")
                        params[pname] = _coerce_filter_value(k, v)
            where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
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
            return run_query(q, db)

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
            if spec.where:
                for k, v in spec.where.items():
                    if k in ("start", "startDate", "end", "endDate"):
                        continue
                    if v is None:
                        where_clauses.append(f"{_derived_lhs(k)} IS NULL")
                    elif isinstance(v, (list, tuple)):
                        if len(v) == 0:
                            continue
                        pnames = []
                        for i, item in enumerate(v):
                            pname = _pname(k, f"_{i}")
                            params[pname] = _coerce_filter_value(k, item)
                            pnames.append(f":{pname}")
                        where_clauses.append(f"{_derived_lhs(k)} IN ({', '.join(pnames)})")
                    elif isinstance(k, str) and "__" in k:
                        base, op = k.split("__", 1)
                        opname = None
                        if op == "gte": opname = ">="
                        elif op == "gt": opname = ">"
                        elif op == "lte": opname = "<="
                        elif op == "lt": opname = "<"
                        elif op == "ne": opname = "!="
                        if opname:
                            pname = _pname(base, f"_{op}")
                            params[pname] = _coerce_filter_value(base, v)
                            where_clauses.append(f"{_derived_lhs(base)} {opname} :{pname}")
                        else:
                            pname = _pname(k)
                            where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                            params[pname] = _coerce_filter_value(k, v)
                    else:
                        pname = _pname(k)
                        where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                        params[pname] = _coerce_filter_value(k, v)
            # Filter out NULL legend values
            legend_filter_clauses = list(where_clauses) if where_clauses else []
            if legend_expr:
                legend_filter_clauses.append(f"{legend_expr} IS NOT NULL")
            where_sql_with_legend = f" WHERE {' AND '.join(legend_filter_clauses)}" if legend_filter_clauses else ""
            
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
            
            if use_sqlglot:
                # Build expr_map from datasource transforms
                expr_map = _build_expr_map(ds, spec.source, ds_type) if ds else {}
                print(f"[SQLGlot] Built expr_map with {len(expr_map)} entries: {list(expr_map.keys())}")
                
                # Resolve WHERE clause
                where_resolved = None
                if hasattr(spec, 'where') and spec.where:
                    where_resolved = _resolve_derived_columns_in_where(
                        spec.where,
                        ds,
                        spec.source,
                        ds_type
                    )
                else:
                    where_resolved = None
                
            if use_sqlglot:
                # NEW PATH: SQLGlot SQL generation
                try:
                    logger.info(f"[SQLGlot] ENABLED for user={actorId}, dialect={ds_type}")
                    print(f"[SQLGlot] ENABLED for user={actorId}, dialect={ds_type}")
                    
                    builder = SQLGlotBuilder(dialect=ds_type)
                    
                    # Handle multi-legend (legend could be string or array)
                    legend_field_val = spec.legend if hasattr(spec, 'legend') else None
                    legend_fields_val = None
                    if isinstance(legend_field_val, list):
                        legend_fields_val = legend_field_val
                        legend_field_val = None
                    
                    # Handle multi-series
                    series_val = spec.series if hasattr(spec, 'series') and isinstance(spec.series, list) else None
                    
                    sql_inner = builder.build_aggregation_query(
                        source=spec.source,
                        x_field=x_col,
                        y_field=spec.y if hasattr(spec, 'y') else None,
                        legend_field=legend_field_val,
                        agg=agg,
                        where=where_resolved,  # Use resolved WHERE with expressions
                        group_by=spec.groupBy if hasattr(spec, 'groupBy') else None,
                        order_by=spec.orderBy if hasattr(spec, 'orderBy') else None,
                        order=spec.order if hasattr(spec, 'order') else 'asc',
                        limit=lim,
                        week_start=spec.weekStart if hasattr(spec, 'weekStart') else 'mon',
                        date_field=x_col,  # For date range filtering
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
                """Get SQL expression for WHERE clause left-hand side.
                If transforms created a derived column, use it directly instead of extracting."""
                # Check if this is a derived date part
                m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
                if m and ds_transforms:
                    # Check if the derived column exists in transformed base
                    if _actual_cols and key in _actual_cols:
                        return _q_ident(key)
                # Fall back to extraction or simple quoting
                return _derived_lhs(key)
            
            def _is_string_filter(key: str) -> bool:
                """Check if filter is for a string field (not a derived date part)."""
                return not re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
            
            where_clauses = []
            params: Dict[str, Any] = {}
            if spec.where:
                for k, v in spec.where.items():
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
                if x_order_expr:
                    if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                        sql_inner = (
                            f"SELECT x, value FROM ("
                            f"SELECT {x_expr} as x, {value_expr} as value, {x_order_expr} as _xo "
                            f"{base_from_sql}{where_sql} GROUP BY {x_expr}, {x_order_expr}) _t ORDER BY _xo"
                        )
                    else:
                        sql_inner = (
                            f"SELECT x, value FROM ("
                            f"SELECT {x_expr} as x, {value_expr} as value, {x_order_expr} as _xo "
                            f"{base_from_sql}{where_sql} GROUP BY 1,3) _t ORDER BY _xo"
                        )
                else:
                    if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                        sql_inner = (
                            f"SELECT {x_expr} as x, {value_expr} as value "
                            f"{base_from_sql}{where_sql} GROUP BY {x_expr}{order_seg_mssql}"
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
            """Get SQL expression for WHERE clause. Use quoted column if it exists in transformed base."""
            m = re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", str(key), flags=re.IGNORECASE)
            if m and ds_transforms and _actual_cols and key in _actual_cols:
                return _q_ident(key)
            return _derived_lhs(key)
        
        where_clauses = []
        params: Dict[str, Any] = {}
        if spec.where:
            for k, v in spec.where.items():
                if k in ("start", "startDate", "end", "endDate"):
                    continue
                if v is None:
                    where_clauses.append(f"{_where_lhs(k)} IS NULL")
                elif isinstance(v, (list, tuple)):
                    if len(v) == 0:
                        continue
                    pnames = []
                    for i, item in enumerate(v):
                        pname = _pname(k, f"_{i}")
                        params[pname] = _coerce_filter_value(k, item)
                        pnames.append(f":{pname}")
                    where_clauses.append(f"{_where_lhs(k)} IN ({', '.join(pnames)})")
                elif isinstance(k, str) and "__" in k:
                    base, op = k.split("__", 1)
                    opname = None
                    if op == "gte": opname = ">="
                    elif op == "gt": opname = ">"
                    elif op == "lte": opname = "<="
                    elif op == "lt": opname = "<"
                    if opname:
                        pname = _pname(base, f"_{op}")
                        params[pname] = _coerce_filter_value(base, v)
                        where_clauses.append(f"{_where_lhs(base)} {opname} :{pname}")
                    elif op == "ne":
                        pname = _pname(base, "_ne")
                        params[pname] = _coerce_filter_value(base, v)
                        where_clauses.append(f"{_where_lhs(base)} <> :{pname}")
                    elif op in {"contains", "notcontains", "startswith", "endswith"}:
                        like = "LIKE"
                        if op == "notcontains":
                            cmp = "NOT LIKE"; patt = f"%{v}%"
                        elif op == "contains":
                            cmp = "LIKE"; patt = f"%{v}%"
                        elif op == "startswith":
                            cmp = "LIKE"; patt = f"{v}%"
                        else:
                            cmp = "LIKE"; patt = f"%{v}"
                        pname = _pname(base, "_like")
                        params[pname] = patt
                        where_clauses.append(f"{_where_lhs(base)} {cmp} :{pname}")
                    else:
                        pname = _pname(base, "_eq")
                        where_clauses.append(f"{_where_lhs(base)} = :{pname}")
                        params[pname] = _coerce_filter_value(base, v)
                else:
                    pname = _pname(k)
                    where_clauses.append(f"{_where_lhs(k)} = :{pname}")
                    params[pname] = _coerce_filter_value(k, v)
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
                            # distinct endpoint has no widgetId context; include only non-widget or matching ones (none here)
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
            
            # Build expr_map from datasource
            # Only if there's NO base_from_sql (custom columns already materialized in subquery)
            print(f"[SQLGlot] /distinct: base_from_sql={'EXISTS' if base_from_sql else 'NONE'}")
            print(f"[SQLGlot] /distinct: effective_source preview: {effective_source[:200] if effective_source else 'NONE'}...")
            expr_map = {}
            if ds_info and not base_from_sql:
                ds_obj = db.get(Datasource, ds_info.get("id"))
                if ds_obj:
                    expr_map = _build_expr_map(ds_obj, payload.source, ds_type)
            
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
                        where_resolved = _resolve_derived_columns_in_where(
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
                expr_map=expr_map if not base_from_sql else {},  # No resolution needed if subquery
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
                def _matches(scope: str) -> bool:
                    if not scope:
                        return True  # datasource-level
                    s_norm = scope.strip().strip('[]').strip('"').strip('`').lower()
                    t_norm = str(source_name or '').strip().strip('[]').strip('"').strip('`').lower()
                    return s_norm == t_norm
                def _filter_list(items):
                    return [item for item in (items or []) if _matches(item.get('table', ''))]
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
