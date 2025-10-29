from __future__ import annotations

import time
from typing import Optional, Any, Dict, Tuple
import decimal
import binascii
import re

import os
import math
import threading

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from ..db import get_duckdb_engine, get_engine_from_dsn, open_duck_native
from ..sqlgen import build_sql, build_distinct_sql
import json
from dateutil import parser as date_parser
from ..models import SessionLocal, Datasource, User, get_share_link_by_public, verify_share_link_token
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
if _HEAVY_LIMIT <= 0:
    _HEAVY_LIMIT = 1
_HEAVY_SEM = threading.BoundedSemaphore(_HEAVY_LIMIT)

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
        t = str(table).strip().strip('"').strip('`').strip('[]')
        if not t:
            return False
        with open_duck_native(db_path) as conn:
            try:
                conn.execute(f"SELECT * FROM {t} LIMIT 0")
                return True
            except Exception:
                try:
                    # Try quoted
                    conn.execute(f'SELECT * FROM "{t}" LIMIT 0')
                    return True
                except Exception:
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
    # Enforce that only owner or admin can access this datasource when actor is provided
    if actor_id:
        u = db.get(User, str(actor_id).strip())
        is_admin = bool(u and (u.role or "user").lower() == "admin")
        if not is_admin and (str(ds_info.get("user_id") or "").strip() != str(actor_id).strip()):
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
            # Normalize to naive local time without microseconds
            if getattr(dt, 'tzinfo', None) is not None:
                try:
                    dt = dt.astimezone().replace(tzinfo=None)
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
    Handles patterns like [s].[Col], s.Col, [Col]. Returns lowercased set.
    """
    cols: set[str] = set()
    try:
        import re as _re
        for m in _re.findall(r"\[s\]\.\[([^\]]+)\]", expr or ""):
            cols.add(_norm_name(m))
        for m in _re.findall(r"\bs\.([A-Za-z_][A-Za-z0-9_]*)", expr or ""):
            cols.add(_norm_name(m))
        # Bare bracketed identifiers
        for m in _re.findall(r"\[([^\]]+)\]", expr or ""):
            if m.lower() != 's':
                cols.add(_norm_name(m))
    except Exception:
        return set()
    return cols


def _filter_by_basecols(ds_tr: dict, base_cols: set[str]) -> dict:
    """Drop customColumns/transforms that reference columns not present on base source."""
    if not isinstance(ds_tr, dict):
        return {}
    base_l = {(_c or '').strip().strip('[]').strip('"').strip('`').lower() for _c in (base_cols or set())}
    def keep_cc(cc: dict) -> bool:
        expr = str((cc or {}).get('expr') or '')
        refs = _referenced_cols_in_expr(expr)
        return (not refs) or refs.issubset(base_l)
    def keep_tr(tr: dict) -> bool:
        t = str((tr or {}).get('type') or '').lower()
        if t == 'computed':
            refs = _referenced_cols_in_expr(str(tr.get('expr') or ''))
            return (not refs) or refs.issubset(base_l)
        if t in {'case', 'replace', 'translate', 'nullhandling'}:
            # check target (and case WHEN lefts)
            tgt = str(tr.get('target') or '')
            tgt_n = _norm_name(tgt)
            if tgt and tgt_n and (tgt_n not in base_l):
                return False
            if t == 'case':
                try:
                    for c in (tr.get('cases') or []):
                        left = str((c.get('when') or {}).get('left') or '')
                        # accept s.Col, [s].[Col], bare Col
                        l = _norm_name(left)
                        if l and l not in base_l:
                            return False
                except Exception:
                    return True
            return True
        return True
    return {
        'customColumns': [cc for cc in (ds_tr.get('customColumns') or []) if keep_cc(cc)],
        'transforms': [tr for tr in (ds_tr.get('transforms') or []) if keep_tr(tr)],
        'joins': ds_tr.get('joins') or [],
        'defaults': ds_tr.get('defaults') or {},
    }


@router.post("/pivot", response_model=QueryResponse)
def run_pivot(payload: PivotRequest, db: Session = Depends(get_db), actorId: Optional[str] = None, publicId: Optional[str] = None, token: Optional[str] = None) -> QueryResponse:
    """Server-side pivot aggregation.
    Returns long-form grouped rows: [row_dims..., col_dims..., value].
    """
    # Determine datasource; optionally route to DuckDB when globally preferred and the source exists locally
    engine = _engine_for_datasource(db, payload.datasourceId, actorId)
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
    try:
        ds_type = (engine.dialect.name or "").lower()
    except Exception:
        ds_type = ""
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
    ds_info = None
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
                    if not sc:
                        out.append(it); continue
                    lvl = str(sc.get('level') or '').lower()
                    if lvl == 'datasource':
                        out.append(it)
                    elif lvl == 'table' and sc.get('table') and _matches_table(str(sc.get('table')), payload.source):
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
        ds_transforms = _apply_scope((opts or {}).get("transforms") or {}, payload.source)
    base_from_sql = f" FROM {_q_source(payload.source)}"
    if ds_transforms:
        # Probe columns and filter joins as in aggregated path
        def _list_cols_for_agg_base() -> set[str]:
            try:
                eng = _engine_for_datasource(db, payload.datasourceId, actorId)
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
        base_sql, _cols_unused, _warns = build_sql(
            dialect=ds_type,
            source=_q_source(payload.source),
            base_select=["*"],
            custom_columns=ds_transforms.get("customColumns", []),
            transforms=__transforms_eff,
            joins=__joins_eff,
            defaults={},
            limit=None,
        )
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
            elif isinstance(v, (list, tuple)):
                if len(v) == 0:
                    continue
                pnames = []
                for i, item in enumerate(v):
                    pname = _pname(k, f"_{i}")
                    params[pname] = item
                    pnames.append(f":{pname}")
                where_clauses.append(f"{_derived_lhs(k)} IN ({', '.join(pnames)})")
            elif isinstance(k, str) and "__" in k:
                base, op = k.split("__", 1)
                opname = None
                if op == "gte": opname = ">="
                elif op == "gt": opname = ">"
                elif op == "lte": opname = "<="
                elif op == "lt": opname = "<"
                if opname:
                    pname = _pname(base, f"_{op}")
                    params[pname] = v
                    where_clauses.append(f"{_derived_lhs(base)} {opname} :{pname}")
                else:
                    pname = _pname(k)
                    where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                    params[pname] = v
            else:
                pname = _pname(k)
                where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                params[pname] = v
    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

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
    # Use original names as aliases (quoted per dialect) so UI can match config fields directly
    r_exprs = [(_derived_lhs(n), _q_ident(n)) for i, n in enumerate(r_dims)]
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
    try:
        is_numeric_name = bool(re.fullmatch(r"\d+", str(val_field or '').strip()))
    except Exception:
        is_numeric_name = False
    if agg in ("sum", "avg", "min", "max", "distinct"):
        if (not val_field) or is_numeric_name:
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

    sel_parts = [f"{e} AS {a}" for e, a in (r_exprs + c_exprs)]
    sel = ", ".join(sel_parts + [f"{value_expr} AS value"]) or f"{value_expr} AS value"
    # Use expressions in GROUP BY for cross-dialect compatibility (SQL Server disallows aliases here)
    group_by = ", ".join([e for e, _ in (r_exprs + c_exprs)])
    gb_sql = f" GROUP BY {group_by}" if group_by else ""
    order_by = f" ORDER BY {group_by}" if group_by else ""
    inner = f"SELECT {sel}{base_from_sql}{where_sql}{gb_sql}{order_by}"

    # Delegate execution to /query. If no explicit limit is provided, fetch all pages.
    _HEAVY_SEM.acquire()
    try:
        if payload.limit is not None:
            q = QueryRequest(
                sql=inner,
                datasourceId=payload.datasourceId,
                limit=payload.limit,
                offset=0,
                includeTotal=False,
                params=params or None,
            )
            return run_query(q, db, actorId=actorId, publicId=publicId, token=token)

        # Unlimited mode: page through results until exhaustion
        page_size = 50000
        all_rows: list[list[Any]] = []
        cols: list[str] | None = None
        offset = 0
        start_time = time.perf_counter()
        while True:
            q = QueryRequest(
                sql=inner,
                datasourceId=payload.datasourceId,
                limit=page_size,
                offset=offset,
                includeTotal=False,
                params=params or None,
            )
            res = run_query(q, db, actorId=actorId, publicId=publicId, token=token)
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
        def _apply_scope(model: dict, src: str):
            if not isinstance(model, dict):
                return None
            def filt(arr):
                out = []
                for it in (arr or []):
                    sc = (it or {}).get('scope') or {}
                    lvl = (sc or {}).get('level')
                    if not lvl:
                        out.append(it)
                        continue
                    if lvl == 'datasource':
                        out.append(it)
                    elif lvl == 'table':
                        t = (sc or {}).get('table')
                        if t and str(t).strip().lower() == str(src or '').strip().lower():
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
        base_sql, _cols_unused, _warns = build_sql(
            dialect=ds_type,
            source=_q_source(payload.source),
            base_select=["*"],
            custom_columns=ds_transforms.get("customColumns", []),
            transforms=ds_transforms.get("transforms", []),
            joins=__joins_eff,
            defaults={},
            limit=None,
        )
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
            elif isinstance(v, (list, tuple)):
                if len(v) == 0:
                    continue
                pnames = []
                for i, item in enumerate(v):
                    pname = _pname(k, f"_{i}")
                    params[pname] = item
                    pnames.append(f":{pname}")
                where_clauses.append(f"{_derived_lhs(k)} IN ({', '.join(pnames)})")
            elif isinstance(k, str) and "__" in k:
                base, op = k.split("__", 1)
                opname = None
                if op == "gte": opname = ">="
                elif op == "gt": opname = ">"
                elif op == "lte": opname = "<="
                elif op == "lt": opname = "<"
                if opname:
                    pname = _pname(base, f"_{op}")
                    params[pname] = v
                    where_clauses.append(f"{_derived_lhs(base)} {opname} :{pname}")
                else:
                    pname = _pname(k)
                    where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                    params[pname] = v
            else:
                pname = _pname(k)
                where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                params[pname] = v
    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

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
    r_exprs = [(_derived_lhs(n), _q_ident(n)) for i, n in enumerate(r_dims)]
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

    sel_parts = [f"{e} AS {a}" for e, a in (r_exprs + c_exprs)]
    sel = ", ".join(sel_parts + [f"{value_expr} AS value"]) or f"{value_expr} AS value"
    # Use expressions in GROUP BY for cross-dialect compatibility (avoid alias usage in SQL Server)
    group_by = ", ".join([e for e, _ in (r_exprs + c_exprs)])
    gb_sql = f" GROUP BY {group_by}" if group_by else ""
    order_by = f" ORDER BY {group_by}" if group_by else ""
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
                                conn.execute(text("SET statement_timeout = 30000"))
                            elif is_mysql:
                                conn.execute(text("SET SESSION MAX_EXECUTION_TIME=30000"))
                            elif is_mssql:
                                conn.execute(text("SET LOCK_TIMEOUT 30000"))
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
                raise HTTPException(status_code=403, detail="Not allowed to query this datasource")

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

        base_sql, _cols_unused, _warns = build_sql(
            dialect=ds_type,
            source=spec.source,
            base_select=eff_select,
            custom_columns=ds_transforms.get("customColumns", []),
            transforms=ds_transforms.get("transforms", []),
            joins=_joins_eff,
            defaults=ds_transforms.get("defaults", {}),
            limit=None,
        )

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
        where_clauses = []
        params: Dict[str, Any] = {}
        if payload.spec.where:
            for k, v in payload.spec.where.items():
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
                        params[pname] = item
                        pnames.append(f":{pname}")
                    where_clauses.append(f"{_derived_lhs(k)} IN ({', '.join(pnames)})")
                elif isinstance(k, str) and "__" in k:
                    base, op = k.split("__", 1)
                    opname = None
                    if op == "gte": opname = ">="
                    elif op == "gt": opname = ">"
                    elif op == "lte": opname = "<="
                    elif op == "lt": opname = "<"
                    if opname:
                        pname = _pname(base, f"_{op}")
                        params[pname] = v
                        where_clauses.append(f"{_derived_lhs(base)} {opname} :{pname}")
                    else:
                        pname = _pname(k)
                        where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                        params[pname] = v
                else:
                    pname = _pname(k)
                    where_clauses.append(f"{_q_ident(k)} = :{pname}")
                    params[pname] = v
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

            base_sql, _cols_unused2, _warns2 = build_sql(
                dialect=ds_type,
                source=_q_source(spec.source),
                base_select=["*"],
                custom_columns=ds_transforms.get("customColumns", []),
                transforms=ds_transforms.get("transforms", []),
                joins=__joins_eff,
                defaults={},  # avoid sort/limit on base for aggregated queries
                limit=None,
            )
            base_from_sql = f" FROM ({base_sql}) AS _base"
        else:
            # Direct table/view reference; quote per dialect (handles schema-qualified)
            base_from_sql = f" FROM {_q_source(spec.source)}"
        x_col = spec.x or (spec.select[0] if spec.select else None)
        if not x_col:
            # Fallback: simple total aggregation without x; label as 'total'
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
            where_clauses = []
            params: Dict[str, Any] = {}
            if spec.where:
                for k, v in spec.where.items():
                    # Skip global keys that aren't real columns
                    if k in ("start", "startDate", "end", "endDate"):
                        continue
                    if v is None:
                        where_clauses.append(f"{_q_ident(k)} IS NULL")
                    elif isinstance(v, (list, tuple)):
                        if len(v) == 0:
                            # Ignore empty lists: no-op filter (UI chip added but no values selected)
                            continue
                        pnames = []
                        for i, item in enumerate(v):
                            pname = _pname(k, f"_{i}")
                            params[pname] = item
                            pnames.append(f":{pname}")
                        where_clauses.append(f"{_q_ident(k)} IN ({', '.join(pnames)})")
                    elif isinstance(k, str) and "__" in k:
                        base, op = k.split("__", 1)
                        opname = None
                        if op == "gte": opname = ">="
                        elif op == "gt": opname = ">"
                        elif op == "lte": opname = "<="
                        elif op == "lt": opname = "<"
                        if opname:
                            pname = _pname(base, f"_{op}")
                            params[pname] = v
                            where_clauses.append(f"{_q_ident(base)} {opname} :{pname}")
                        else:
                            pname = _pname(k)
                            where_clauses.append(f"{_q_ident(k)} = :{pname}")
                            params[pname] = v
                    else:
                        pname = _pname(k)
                        where_clauses.append(f"{_derived_lhs(k)} = :{pname}")
                        params[pname] = v
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

        # Aggregated query when agg != 'none' (with optional legend)
        if agg and agg != "none":
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
            where_clauses = []
            params: Dict[str, Any] = {}
            if spec.where:
                for k, v in spec.where.items():
                    if k in ("start", "startDate", "end", "endDate"):
                        continue
                    if v is None:
                        where_clauses.append(f"{_q_ident(k)} IS NULL")
                    elif isinstance(v, (list, tuple)):
                        if len(v) == 0:
                            continue
                        pnames = []
                        for i, item in enumerate(v):
                            pname = _pname(k, f"_{i}")
                            params[pname] = item
                            pnames.append(f":{pname}")
                        where_clauses.append(f"{_q_ident(k)} IN ({', '.join(pnames)})")
                    elif isinstance(k, str) and "__" in k:
                        base, op = k.split("__", 1)
                        opname = None
                        if op == "gte": opname = ">="
                        elif op == "gt": opname = ">"
                        elif op == "lte": opname = "<="
                        elif op == "lt": opname = "<"
                        if opname:
                            pname = _pname(base, f"_{op}")
                            params[pname] = v
                            where_clauses.append(f"{_q_ident(base)} {opname} :{pname}")
                        else:
                            pname = _pname(k)
                            where_clauses.append(f"{_q_ident(k)} = :{pname}")
                            params[pname] = v
                    else:
                        pname = _pname(k)
                        where_clauses.append(f"{_q_ident(k)} = :{pname}")
                        params[pname] = v
            where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

            # Apply optional groupBy to x (dialect-aware)
            x_expr = _q_ident(x_col)
            gb = (spec.groupBy or 'none').lower()
            week_start = (getattr(spec, 'weekStart', None) or 'mon').lower()
            if gb in ("day","week","month","quarter","year"):
                if ("mssql" in ds_type) or ("sqlserver" in ds_type):
                    if gb == "day":
                        x_expr = f"CAST({_q_ident(x_col)} AS date)"
                    elif gb == "week":
                        # Week start control: 'sun' vs 'mon'
                        if week_start == 'sun':
                            # Sunday start-of-week at 00:00
                            x_expr = f"DATEADD(week, DATEDIFF(week, 0, {_q_ident(x_col)}), 0)"
                        else:
                            # Monday start-of-week: shift by -1 day, truncate to week, then +1 day
                            x_expr = f"DATEADD(day, 1, DATEADD(week, DATEDIFF(week, 0, DATEADD(day, -1, {_q_ident(x_col)})), 0))"
                    elif gb == "month":
                        x_expr = f"DATEADD(month, DATEDIFF(month, 0, {_q_ident(x_col)}), 0)"
                    elif gb == "quarter":
                        x_expr = f"DATEADD(quarter, DATEDIFF(quarter, 0, {_q_ident(x_col)}), 0)"
                    elif gb == "year":
                        x_expr = f"DATEADD(year, DATEDIFF(year, 0, {_q_ident(x_col)}), 0)"
                elif ("duckdb" in ds_type) or ("postgres" in ds_type):
                    # For DuckDB, some sources store dates as VARCHAR. Cast safely before date_trunc.
                    # Use try_cast to avoid binder errors; NULLs propagate safely through DATE_TRUNC.
                    if "duckdb" in ds_type:
                        col_ts = f"COALESCE(try_cast({_q_ident(x_col)} AS TIMESTAMP), CAST(try_cast({_q_ident(x_col)} AS DATE) AS TIMESTAMP))"
                    else:
                        col_ts = _q_ident(x_col)
                    if gb == 'week':
                        if week_start == 'sun':
                            # Adjust so Sunday is start-of-week
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
                        # Week start control
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
                        # Compute start-of-week date using strftime('%w'): 0=Sun..6=Sat
                        # sun: subtract w days; mon: subtract (w+6)%7 days
                        if week_start == 'sun':
                            x_expr = f"date({_q_ident(x_col)}, '-' || CAST(strftime('%w', {_q_ident(x_col)}) AS INTEGER) || ' days')"
                        else:
                            x_expr = f"date({_q_ident(x_col)}, '-' || ((CAST(strftime('%w', {_q_ident(x_col)}) AS INTEGER) + 6) % 7) || ' days')"
                    elif gb == "month":
                        x_expr = f"date(strftime('%Y-%m-01', {_q_ident(x_col)}))"
                    elif gb == "quarter":
                        # Map to first day of quarter
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
                    # Fallback: pass-through (caller should pre-aggregate appropriately)
                    x_expr = _q_ident(x_col)

            # Legend: allow derived date-part syntax like "OrderDate (Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)"
            legend_expr_raw = spec.legend
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

            if spec.legend:
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
                datasourceId=payload.datasourceId,
                limit=eff_limit,
                offset=off or 0,
                includeTotal=payload.includeTotal,
                params=params or None,
            )
            return run_query(q, db)

        # agg == 'none': passthrough raw columns via select/x/y, but derive/quote when needed
        def _select_part(c: str) -> str:
            s = str(c or '').strip()
            # If derived pattern, use expression and alias back to original token (quoted)
            if re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", s, flags=re.IGNORECASE):
                expr = _derived_lhs(s)
                return f"{expr} AS {_q_ident(s)}"
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
        where_clauses = []
        params: Dict[str, Any] = {}
        if spec.where:
            for k, v in spec.where.items():
                if k in ("start", "startDate", "end", "endDate"):
                    continue
                if v is None:
                    where_clauses.append(f"{_q_ident(k)} IS NULL")
                elif isinstance(v, (list, tuple)):
                    if len(v) == 0:
                        continue
                    pnames = []
                    for i, item in enumerate(v):
                        pname = _pname(k, f"_{i}")
                        params[pname] = item
                        pnames.append(f":{pname}")
                    where_clauses.append(f"{_q_ident(k)} IN ({', '.join(pnames)})")
                elif isinstance(k, str) and "__" in k:
                    base, op = k.split("__", 1)
                    opname = None
                    if op == "gte": opname = ">="
                    elif op == "gt": opname = ">"
                    elif op == "lte": opname = "<="
                    elif op == "lt": opname = "<"
                    if opname:
                        pname = _pname(base, f"_{op}")
                        params[pname] = v
                        where_clauses.append(f"{_q_ident(base)} {opname} :{pname}")
                    elif op == "ne":
                        pname = _pname(base, "_ne")
                        params[pname] = v
                        where_clauses.append(f"{_q_ident(base)} <> :{pname}")
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
                        where_clauses.append(f"{_q_ident(base)} {cmp} :{pname}")
                    else:
                        pname = _pname(base, "_eq")
                        where_clauses.append(f"{_q_ident(base)} = :{pname}")
                        params[pname] = v
                else:
                    pname = _pname(k)
                    where_clauses.append(f"{_q_ident(k)} = :{pname}")
                    params[pname] = v
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
    # Build base SQL with transforms/joins applied, then select DISTINCT from that projection
    base_from_sql = None
    if ds_info is not None:
        try:
            opts = json.loads((ds_info.get("options_json") or "{}"))
        except Exception:
            opts = {}
        ds_transforms = (opts or {}).get("transforms") or {}
        base_sql, _unused_cols, _warns = build_sql(
            dialect=ds_type or dialect,
            source=str(payload.source),
            base_select=["*"],
            custom_columns=ds_transforms.get("customColumns", []),
            transforms=ds_transforms.get("transforms", []),
            joins=ds_transforms.get("joins", []),
            defaults={},  # do not apply defaults like TopN
            limit=None,
        )
        base_from_sql = f"({base_sql}) AS _base"
    # If no datasource or no transforms, select directly from source
    effective_source = base_from_sql or str(payload.source)
    sql, params = build_distinct_sql(
        dialect=dialect,
        source=effective_source,
        field=str(payload.field),
        where=dict(payload.where or {}),
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
    y = payload.get("y")
    measure = payload.get("measure")
    agg = (payload.get("agg") or "count").lower()
    date_field = payload.get("dateField")
    start = payload.get("start")
    end = payload.get("end")
    legend = payload.get("legend")
    base_where = payload.get("where") or {}

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
        qy = _q_ident_local(y) if y else None
        if agg == "count":
            value_expr = "COUNT(*)"
        elif agg == "distinct" and qy:
            value_expr = f"COUNT(DISTINCT {qy})"
        elif agg in ("avg", "sum", "min", "max") and qy:
            # For DuckDB, cast string numerics (e.g., "1,234.50 ILS") before aggregation
            if route_duck:
                y_clean = f"COALESCE(try_cast(regexp_replace({qy}, '[^0-9\\.-]', '') AS DOUBLE), try_cast({qy} AS DOUBLE), 0.0)"
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
                    y_clean = f"COALESCE(try_cast(regexp_replace({qy}, '[^0-9\\.-]', '') AS DOUBLE), try_cast({qy} AS DOUBLE), 0.0)"
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
        if ("duckdb" in dialect_name) or ("postgres" in dialect_name) or ("postgre" in dialect_name):
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

    where_clauses = [f"{_quote_ident(date_field)} >= :_start", f"{_quote_ident(date_field)} < :_end"]
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
        ds_transforms = (opts or {}).get("transforms") or {}
        try:
            dialect = ds_type or dialect_name
        except Exception:
            dialect = ds_type or dialect_name
        base_sql, _unused_cols, _warns = build_sql(
            dialect=ds_type or dialect,
            source=_q_source_local(str(source)),
            base_select=["*"],
            custom_columns=ds_transforms.get("customColumns", []),
            transforms=ds_transforms.get("transforms", []),
            joins=ds_transforms.get("joins", []),
            defaults={},  # avoid sort/limit in totals context
            limit=None,
        )
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
            s = str(name).strip()
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", s):
                return s
            if ("mssql" in dialect_name) or ("sqlserver" in dialect_name):
                return "[" + s.replace("]", "]]" ) + "]"
            if "mysql" in dialect_name:
                return "`" + s.replace("`", "``") + "`"
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

        # Build a raw expression for a part: derived date-part or quoted identifier
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
                return datepart_expr(base, kind)
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
    cached = _cache_get(cache_key)
    if cached:
        cols, rows = cached
        if legend:
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
        else:
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
                cur = conn.execute(sql_qm, vals)
                rows = cur.fetchall()
            if legend:
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
                if legend:
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
