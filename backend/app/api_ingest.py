from __future__ import annotations

import os
import re
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
import base64
import csv
import io

import requests
from sqlalchemy import text
from urllib.parse import unquote
from sqlalchemy.engine import Engine
import logging
try:
    import duckdb as _duckdb  # type: ignore
except Exception:
    _duckdb = None
from .config import settings
from .db import open_duck_native

# -----------------------------
# Placeholder & macro utilities
# -----------------------------
_DATE_MACRO_RE = re.compile(r"^(today|yesterday|startOfDay|startOfWeek|startOfMonth|startOfQuarter|startOfYear|endOfDay|endOfMonth|endOfYear|eom|eoy)([+-]\d+[dhwmy])?$", re.IGNORECASE)
_OFFSET_RE = re.compile(r"([+-])(\d+)([dhwmy])", re.IGNORECASE)

logger = logging.getLogger(__name__)
# Ensure logger emits even when root is configured differently by Uvicorn
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    logger.addHandler(_h)
try:
    logger.setLevel(logging.INFO)
except Exception:
    pass
logger.propagate = False

DEBUG = str(os.getenv("API_SYNC_DEBUG", "1")).strip().lower() in ("1", "true", "on")


def _apply_offset(dt: datetime, offset: Optional[str]) -> datetime:
    if not offset:
        return dt
    m = _OFFSET_RE.match(offset)
    if not m:
        return dt
    sign, num_s, unit = m.groups()
    num = int(num_s)
    if sign == "-":
        num = -num
    unit = unit.lower()
    if unit == "d":
        return dt + timedelta(days=num)
    if unit == "h":
        return dt + timedelta(hours=num)
    if unit == "w":
        return dt + timedelta(weeks=num)
    if unit == "m":
        # month approximation: 30 days
        return dt + timedelta(days=30 * num)
    if unit == "y":
        return dt + timedelta(days=365 * num)
    return dt


def _start_of_week(dt: datetime) -> datetime:
    # ISO Monday=0
    return (dt - timedelta(days=dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)


def _parse_date_macro(val: str, tz: timezone | None = None) -> datetime:
    tz = tz or timezone.utc
    now = datetime.now(tz)
    v = (val or "").strip()
    m = _DATE_MACRO_RE.match(v)
    if not m:
        # try ISO parse
        try:
            return datetime.fromisoformat(v)
        except Exception:
            return now
    base, off = m.groups()
    base = base.lower()
    if base == "today":
        dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif base == "yesterday":
        dt = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif base == "startofday":
        dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif base == "startofweek":
        dt = _start_of_week(now)
    elif base == "startofmonth":
        dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif base == "startofquarter":
        month = ((now.month - 1) // 3) * 3 + 1
        dt = now.replace(month=month, day=1, hour=0, minute=0, second=0, microsecond=0)
    elif base == "startofyear":
        dt = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    elif base == "endofday":
        dt = now.replace(hour=23, minute=59, second=59, microsecond=999000)
    elif base in ("endofmonth", "eom"):
        # last day of current month
        # compute first day of next month then subtract 1 day
        if now.month == 12:
            nm = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            nm = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
        dt = (nm - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif base in ("endofyear", "eoy"):
        dt = now.replace(month=12, day=31, hour=0, minute=0, second=0, microsecond=0)
    else:
        dt = now
    return _apply_offset(dt, off)


def _resolve_secret_refs(s: str) -> str:
    # Replace {{secret:NAME}} with env value
    def repl(m: re.Match[str]) -> str:
        key = (m.group(1) or "").strip()
        return os.environ.get(key, "")
    try:
        return re.sub(r"\{\{\s*secret:([^}]+)\}\}", repl, s)
    except Exception:
        return s


def _normalize_format(fmt: Optional[str]) -> Optional[str]:
    """Convert common friendly formats (YYYY-MM-DD, DD/MM/YYYY, etc.) to strftime tokens.
    If fmt already contains '%', assume strftime and return as-is.
    Accept 'YYY' as alias for 'YYYY'.
    """
    if not fmt:
        return fmt
    s = str(fmt)
    if '%"' in s or '%' in s:
        return s
    # normalize tokens
    # order matters: replace longer tokens first
    repl = [
        ("YYYY", "%Y"),
        ("YYY", "%Y"),
        ("YY", "%y"),
        ("MM", "%m"),  # month
        ("DD", "%d"),
        ("HH", "%H"),
        ("mm", "%M"),  # minutes (lowercase)
        ("ss", "%S"),
    ]
    out = s
    for pat, r in repl:
        out = out.replace(pat, r)
    return out


def _format_value(name: str, kind: str, value: Optional[str], fmt: Optional[str], tz: timezone | None) -> str:
    if kind == "date":
        dt = _parse_date_macro(value or "today", tz)
        if fmt:
            try:
                return dt.strftime(_normalize_format(fmt) or "%Y-%m-%d")
            except Exception:
                pass
        return dt.strftime("%Y-%m-%d")
    # static or others
    v = value or ""
    v = _resolve_secret_refs(v)
    return v


def _token_replace(template: str, ctx: Dict[str, str]) -> str:
    s = template or ""
    for k, v in ctx.items():
        s = s.replace("{" + k + "}", v)
    return _resolve_secret_refs(s)


# -----------------------------
# HTTP + pagination
# -----------------------------

class HttpError(Exception):
    pass


def _http_request(method: str, url: str, headers: Dict[str, str], params: Dict[str, str], body: Optional[str], timeout: int = 30) -> Tuple[int, Dict[str, str], str]:
    try:
        r = requests.request(method=method.upper(), url=url, headers=headers, params=params, data=(body if body and method.upper() != 'GET' else None), timeout=timeout)
        return r.status_code, dict(r.headers or {}), r.text or ""
    except requests.RequestException as e:
        raise HttpError(str(e))


# -----------------------------
# JSON handling
# -----------------------------

def _get_json_root(doc: Any, root: str | None) -> List[Any]:
    try:
        obj = json.loads(doc) if isinstance(doc, str) else doc
    except Exception:
        return []
    if not root:
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            # pick first array value if any
            for v in obj.values():
                if isinstance(v, list):
                    return v
            return [obj]
        return []
    path = root.strip()
    if path.startswith("$."):
        path = path[2:]
    cur = obj
    for part in path.split('.'):
        if isinstance(cur, list):
            # Not supporting arrays in path except terminal
            break
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            cur = None
        if cur is None:
            return []
    if isinstance(cur, list):
        return cur
    if cur is None:
        return []
    return [cur]


# -----------------------------
# CSV handling
# -----------------------------

def _parse_csv(text: str) -> List[Dict[str, Any]]:
    """Parse CSV text into a list of dicts using an explicit header row.
    - Sniff delimiter; fallback to comma
    - Skip preamble/comment lines starting with '#', '//', or empty
    - Synthesize field names for empty headers: col1, col2, ...
    - Coalesce duplicate headers by appending numeric suffixes
    - Empty strings -> None
    """
    try:
        raw = text or ""
        # Handle potential BOM
        raw = raw.lstrip("\ufeff")
        sample = raw[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.excel
        # Build a reader over non-comment lines
        lines = [ln for ln in io.StringIO(raw).read().splitlines()]
        useful: List[str] = []
        for ln in lines:
            t = (ln or "").strip()
            if not t:
                continue
            if t.startswith("#") or t.startswith("//"):
                continue
            useful.append(ln)
        if not useful:
            return []
        rdr = csv.reader(io.StringIO("\n".join(useful)), dialect=dialect)
        try:
            header = next(rdr)
        except StopIteration:
            return []
        # Normalize headers
        norm: List[str] = []
        seen: Dict[str, int] = {}
        for i, h in enumerate(header):
            name = (h or "").strip()
            if not name:
                name = f"col{i+1}"
            # Avoid duplicates
            base = name
            if base in seen:
                seen[base] += 1
                name = f"{base}_{seen[base]}"
            else:
                seen[base] = 1
            norm.append(name)
        out: List[Dict[str, Any]] = []
        for row in rdr:
            try:
                # Pad/truncate row to header length
                vals = list(row) + [None] * (len(norm) - len(row))
                vals = vals[:len(norm)]
                obj = { norm[i]: (vals[i] if (vals[i] is not None and vals[i] != "") else None) for i in range(len(norm)) }
                out.append(obj)
            except Exception:
                continue
        return out
    except Exception:
        return []


def _is_csv_format(resp_headers: Dict[str, str], params: Dict[str, str], cfg: Dict[str, Any]) -> bool:
    """Heuristics to decide if the HTTP response should be parsed as CSV.
    Priority: explicit cfg.parse/format === 'csv' > response content-type includes csv > query param format=csv
    """
    try:
        hint = str((cfg.get('parse') or cfg.get('format') or '')).strip().lower()
        if hint == 'csv':
            return True
    except Exception:
        pass
    try:
        ct = str((resp_headers.get('content-type') or '')).lower()
        if 'text/csv' in ct or 'application/csv' in ct or 'csv' in ct:
            return True
    except Exception:
        pass
    try:
        fmt = str((params.get('format') or params.get('FORMAT') or '')).strip().lower()
        if fmt == 'csv':
            return True
    except Exception:
        pass
    return False


def _flatten_record(rec: Any, prefix: str = "", out: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    # Important: do not use `out = out or {}` because an empty dict is falsy
    # and would cause a fresh dict to be created at each recursion, losing accumulated values.
    if out is None:
        out = {}
    if isinstance(rec, dict):
        for k, v in rec.items():
            _flatten_record(v, f"{prefix}{k}_" if prefix else f"{k}_", out)
    elif isinstance(rec, list):
        # store arrays as JSON string
        out[prefix[:-1]] = json.dumps(rec)
    else:
        out[prefix[:-1]] = rec
    return out


def _infer_duck_type(value: Any) -> str:
    if value is None:
        return "VARCHAR"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, (datetime,)):
        return "TIMESTAMP"
    # try date string
    if isinstance(value, str):
        s = value.strip()
        # ISO date
        try:
            datetime.fromisoformat(s)
            if 'T' in s or ' ' in s:
                return "TIMESTAMP"
            return "DATE"
        except Exception:
            return "VARCHAR"
    return "VARCHAR"


def _sanitize_col(name: str) -> str:
    n = re.sub(r"[^A-Za-z0-9_]+", "_", name.strip())
    if not n:
        n = "col"
    if n[0].isdigit():
        n = "c_" + n
    return n


def _ensure_table_schema(engine: Engine, table: str, sample: Dict[str, Any]) -> Tuple[List[str], Dict[str, str]]:
    # returns (columns, types)
    cols: Dict[str, str] = {}
    for k, v in sample.items():
        cols[_sanitize_col(k)] = _infer_duck_type(v)
    # Always prefer native duckdb path when available to avoid duckdb-engine hashing bugs
    if _duckdb is not None:
        try:
            if DEBUG:
                print(f"[api_ingest] ensure_table_schema: using native duckdb for table={table}", flush=True)
            con = open_duck_native(settings.duckdb_path)
            try:
                existing: Dict[str, str] = {}
                try:
                    info = con.execute(f"PRAGMA table_info('{table}')").fetchall()
                    for row in info:
                        existing[str(row[1])] = str(row[2])
                except Exception:
                    existing = {}
                if not existing:
                    cols_sql = ", ".join([f'"{c}" {t}' for c, t in cols.items()])
                    con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols_sql})")
                else:
                    for c, t in cols.items():
                        if c not in existing:
                            try:
                                con.execute(f'ALTER TABLE {table} ADD COLUMN "{c}" {t}')
                            except Exception:
                                pass
                con.commit()
            finally:
                try:
                    con.close()
                except Exception:
                    pass
            return list(cols.keys()), cols
        except Exception as e:
            try:
                logger.exception("native ensure_table_schema failed for %s: %s", table, str(e))
                if DEBUG:
                    print(f"[api_ingest] native ensure_table_schema failed: {e}", flush=True)
            except Exception:
                pass
            # Fall through to SQLAlchemy path as last resort
            pass
    try:
        with engine.connect() as conn:
            # check existing columns
            existing: Dict[str, str] = {}
            try:
                info = conn.exec_driver_sql(f"PRAGMA table_info('{table}')").fetchall()
                for row in info:
                    existing[str(row[1])] = str(row[2])
            except Exception:
                existing = {}
            if not existing:
                # create table
                cols_sql = ", ".join([f'"{c}" {t}' for c, t in cols.items()])
                conn.exec_driver_sql(f"CREATE TABLE IF NOT EXISTS {table} ({cols_sql})")
            else:
                # add missing columns
                for c, t in cols.items():
                    if c not in existing:
                        try:
                            conn.exec_driver_sql(f'ALTER TABLE {table} ADD COLUMN "{c}" {t}')
                        except Exception:
                            pass
            conn.commit()
        return list(cols.keys()), cols
    except Exception as e:
        # Fallback to native duckdb if available
        if _duckdb is not None and ("DuckDBPyType" in str(e) or "unhashable type" in str(e)):
            try:
                con = open_duck_native(settings.duckdb_path)
                try:
                    # check existing
                    existing: Dict[str, str] = {}
                    try:
                        info = con.execute(f"PRAGMA table_info('{table}')").fetchall()
                        for row in info:
                            existing[str(row[1])] = str(row[2])
                    except Exception:
                        existing = {}
                    if not existing:
                        cols_sql = ", ".join([f'"{c}" {t}' for c, t in cols.items()])
                        con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols_sql})")
                    else:
                        for c, t in cols.items():
                            if c not in existing:
                                try:
                                    con.execute(f'ALTER TABLE {table} ADD COLUMN "{c}" {t}')
                                except Exception:
                                    pass
                    con.commit()
                finally:
                    try:
                        con.close()
                    except Exception:
                        pass
                return list(cols.keys()), cols
            except Exception:
                pass
        raise


def _delete_window(engine: Engine, table: str, date_field: str, start: str, end: str) -> None:
    sql = f"DELETE FROM {table} WHERE {date_field} >= ? AND {date_field} <= ?"
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql(sql, (start, end))
            conn.commit()
            return
    except Exception as e:
        if _duckdb is not None and ("DuckDBPyType" in str(e) or "unhashable type" in str(e)):
            try:
                con = open_duck_native(settings.duckdb_path)
                try:
                    con.execute(sql, (start, end))
                    con.commit()
                    return
                finally:
                    try: con.close()
                    except Exception: pass
            except Exception:
                pass
        raise


def _insert_rows(engine: Engine, table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    cols = sorted({_sanitize_col(k) for r in rows for k in r.keys()})
    # sanitize keys and align order for parameter tuples
    ordered_cols = cols
    qmarks = ",".join(["?" for _ in ordered_cols])
    sql = f"INSERT INTO {table} (" + ",".join([f'\"{c}\"' for c in ordered_cols]) + ") VALUES (" + qmarks + ")"
    payload_vals = []
    for r in rows:
        item = { _sanitize_col(k): v for k, v in r.items() }
        row_tuple = tuple(item.get(c, None) for c in ordered_cols)
        payload_vals.append(row_tuple)
    # Always prefer native duckdb path when available to avoid duckdb-engine hashing bugs
    if _duckdb is not None:
        try:
            if DEBUG:
                print(f"[api_ingest] insert_rows: using native duckdb for table={table} rows={len(rows)}", flush=True)
            con = open_duck_native(settings.duckdb_path)
            try:
                con.executemany(sql, payload_vals)
                con.commit()
                return len(rows)
            finally:
                try:
                    con.close()
                except Exception:
                    pass
        except Exception as e:
            try:
                logger.exception("native insert_rows failed for %s: %s", table, str(e))
                if DEBUG:
                    print(f"[api_ingest] native insert_rows failed: {e}", flush=True)
            except Exception:
                pass
            # Fall through to SQLAlchemy path as last resort
            pass
    try:
        with engine.connect() as conn:
            # executemany via exec_driver_sql with list of tuples
            conn.exec_driver_sql(sql, payload_vals)
            conn.commit()
            return len(rows)
    except Exception as e:
        # Fallback to native duckdb
        if _duckdb is not None and ("DuckDBPyType" in str(e) or "unhashable type" in str(e)):
            try:
                con = open_duck_native(settings.duckdb_path)
                try:
                    # prepare once
                    con.executemany(sql, payload_vals)
                    con.commit()
                    return len(rows)
                finally:
                    try:
                        con.close()
                    except Exception:
                        pass
            except Exception as _:
                pass
        raise


def _max_date(engine: Engine, table: str, date_field: str) -> Optional[str]:
    sql = f"SELECT MAX({date_field}) FROM {table}"
    try:
        with engine.connect() as conn:
            v = conn.exec_driver_sql(sql).scalar()
            return v
    except Exception as e:
        if _duckdb is not None and ("DuckDBPyType" in str(e) or "unhashable type" in str(e)):
            try:
                con = open_duck_native(settings.duckdb_path)
                try:
                    v = con.execute(sql).fetchone()
                    return v[0] if v else None
                finally:
                    try: con.close()
                    except Exception: pass
            except Exception:
                pass
        return None


def _apply_gap_fill(engine: Engine, table: str, date_field: str, key_fields_csv: str) -> None:
    # Create or replace a filled table <table>_filled with forward-filled values for non-key numeric columns
    key_fields = [f.strip() for f in (key_fields_csv or '').split(',') if f.strip()]
    if not key_fields:
        return
    filled = f"{table}_filled"
    keys = ", ".join(key_fields)
    with engine.connect() as conn:
        # Build list of columns
        cols = [row[1] for row in conn.execute(text(f"PRAGMA table_info('{table}')")).fetchall()]
        non_keys = [c for c in cols if c not in key_fields + [date_field]]
        select_cols = ", ".join([keys, date_field] + [
            f"last_value({c} ignore nulls) over (partition by {keys} order by {date_field} rows between unbounded preceding and current row) as {c}"
            for c in non_keys
        ])
        join_cond = " and ".join([f"d.{k}=a.{k}" for k in key_fields] + [f"d.{date_field}=a.{date_field}"])
        sql = f"""
        create or replace table {filled} as
        with d as (select * from {table}),
        all_days as (
          select {keys}, d::date as {date_field}
          from (select distinct {keys} from d),
               generate_series((select min({date_field}) from d), (select max({date_field}) from d), interval 1 day) as g(d)
        ),
        joined as (
          select a.{keys}, a.{date_field}, d.* exclude ({keys}, {date_field})
          from all_days a
          left join d on {join_cond}
        )
        select {select_cols} from joined
        """
        conn.execute(text("SET threads TO 1"))
        conn.execute(text(sql))
        conn.commit()


def _truncate_table(engine: Engine, table: str) -> None:
    sql = f"DELETE FROM {table}"
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql(sql)
            conn.commit()
            return
    except Exception as e:
        if _duckdb is not None and ("DuckDBPyType" in str(e) or "unhashable type" in str(e)):
            try:
                con = open_duck_native(settings.duckdb_path)
                try:
                    con.execute(sql)
                    con.commit()
                    return
                finally:
                    try: con.close()
                    except Exception: pass
            except Exception:
                pass
        # ignore if table doesn't exist
        return


# -----------------------------
# Public runner
# -----------------------------

def run_api_sync(
    duck_engine: Engine,
    options_api: Dict[str, Any],
    dest_table: str,
    mode: str = 'snapshot',
) -> Dict[str, Any]:
    """Run one API sync for a single endpoint definition stored in options_api.
    Returns { row_count, windowStart, windowEnd }.
    """
    cfg = options_api or {}
    try:
        logger.info("run_api_sync start: dest_table=%s mode=%s", dest_table, mode)
        if DEBUG:
            print(f"[api_ingest] run_api_sync start dest={dest_table} mode={mode}", flush=True)
    except Exception:
        pass
    endpoint = cfg.get('endpoint') or cfg.get('urlTemplate') or ''
    method = (cfg.get('method') or 'GET').upper()
    headers = cfg.get('headers') or []  # [{key, value}]
    query = cfg.get('query') or []      # [{key, value}]
    body = cfg.get('body') or cfg.get('bodyTemplate') or ''
    placeholders = cfg.get('placeholders') or []  # [{name, kind, value, format}]
    json_root = cfg.get('jsonRoot') or ''
    gap_fill = (cfg.get('gapFill') or {})
    seq = (cfg.get('sequence') or {})
    tz = timezone.utc
    auth = cfg.get('auth') or {}
    pagination = cfg.get('pagination') or {}

    # Resolve base placeholders
    ctx: Dict[str, str] = {}
    for p in placeholders:
        name = (p.get('name') or '').strip()
        if not name:
            continue
        kind = (p.get('kind') or 'static').strip()
        val = p.get('value')
        fmt = p.get('format')
        ctx[name] = _format_value(name, kind, val, fmt, tz)

    # Auth flows (simple)
    def _apply_auth(hdrs_out: Dict[str, str], params_out: Dict[str, str]) -> None:
        atype = (auth.get('type') or '').lower()
        if atype in ('', 'none'):
            return
        if atype in ('bearer', 'bearerstatic'):
            token = _resolve_secret_refs(str(auth.get('token') or auth.get('tokenTemplate') or ''))
            if token:
                hdrs_out['Authorization'] = f"Bearer {token}"
            return
        if atype == 'apikeyheader':
            hk = str(auth.get('header') or '').strip()
            hv = _token_replace(str(auth.get('valueTemplate') or auth.get('value') or ''), ctx)
            if hk:
                hdrs_out[hk] = hv
            return
        if atype == 'apikeyquery':
            pk = str(auth.get('param') or '').strip()
            pv = _token_replace(str(auth.get('valueTemplate') or auth.get('value') or ''), ctx)
            if pk:
                params_out[pk] = pv
            return
        if atype == 'basic':
            user = _resolve_secret_refs(str(auth.get('username') or ''))
            pwd = _resolve_secret_refs(str(auth.get('password') or ''))
            b = base64.b64encode(f"{user}:{pwd}".encode('utf-8')).decode('utf-8')
            hdrs_out['Authorization'] = f"Basic {b}"
            return
        if atype in ('oauth2_client_credentials', 'oauth_client_credentials'):
            token_url = str(auth.get('tokenUrl') or '')
            cid = _resolve_secret_refs(str(auth.get('clientId') or ''))
            csec = _resolve_secret_refs(str(auth.get('clientSecret') or ''))
            scope = str(auth.get('scope') or '')
            if token_url and cid and csec:
                try:
                    data = {'grant_type': 'client_credentials'}
                    if scope:
                        data['scope'] = scope
                    r = requests.post(token_url, data=data, auth=(cid, csec), timeout=30)
                    js = r.json()
                    tok = js.get('access_token')
                    if tok:
                        hdrs_out['Authorization'] = f"Bearer {tok}"
                except Exception:
                    pass

    # Sequence window (date-range only for now)
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    if bool(seq.get('enabled')) and (seq.get('mode') in (None, '', 'date-range', 'dateRange')):
        date_field = (seq.get('dateField') or 'date').strip()
        last = _max_date(duck_engine, dest_table, date_field)
        if last:
            try:
                last_dt = datetime.fromisoformat(str(last))
            except Exception:
                last_dt = _parse_date_macro(str(last))
        else:
            # Fallback: start 30 days ago
            last_dt = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=30)
        win_days = max(1, int(seq.get('windowDays') or 7))
        start_dt = (last_dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
        if start_dt > end_dt:
            # nothing new
            return {"row_count": 0, "windowStart": None, "windowEnd": None}
        # Limit window to win_days
        cap_end = min(end_dt, start_dt + timedelta(days=win_days - 1))
        window_start = start_dt.strftime('%Y-%m-%d')
        window_end = cap_end.strftime('%Y-%m-%d')
        # inject into context
        ctx['start'] = window_start
        ctx['end'] = window_end

    # Build request parts
    url = _token_replace(str(endpoint or ''), ctx)
    hdrs: Dict[str, str] = {}
    for h in headers:
        k = str(h.get('key') or '').strip()
        v = _token_replace(str(h.get('value') or ''), ctx)
        if k:
            hdrs[k] = v
    params: Dict[str, str] = {}
    for q in query:
        k_raw = str(q.get('key') or '').strip()
        k = unquote(k_raw) if '%' in k_raw else k_raw
        v = _token_replace(str(q.get('value') or ''), ctx)
        if k:
            params[k] = v
    # Apply auth (may set headers/params)
    _apply_auth(hdrs, params)
    # Ensure start/end params for sequence
    if window_start and (seq.get('startParam')):
        params[str(seq.get('startParam'))] = window_start
    if window_end and (seq.get('endParam')):
        params[str(seq.get('endParam'))] = window_end
    req_body = None
    if method != 'GET' and body:
        req_body = _token_replace(str(body), ctx)

    # Log request summary (mask secrets)
    try:
        def _mask_params(d: Dict[str, str]) -> Dict[str, str]:
            out: Dict[str, str] = {}
            for kk, vv in d.items():
                lk = kk.lower()
                if any(s in lk for s in ("key", "token", "secret", "password", "auth")):
                    out[kk] = "***"
                else:
                    out[kk] = vv
            return out
        logger.info(
            "HTTP request prepared: method=%s url=%s headers=%s params=%s",
            method,
            url,
            list(hdrs.keys()),
            _mask_params(params),
        )
        if DEBUG:
            print(f"[api_ingest] request {method} {url} params={_mask_params(params)}", flush=True)
        if window_start or window_end:
            logger.info("sequence window: start=%s end=%s", window_start, window_end)
            if DEBUG:
                print(f"[api_ingest] sequence window: {window_start}..{window_end}", flush=True)
    except Exception:
        pass

    # Pagination support (simple)
    items: List[Dict[str, Any]] = []
    pagetype = (pagination.get('type') or 'none').lower()
    if pagetype in ('none', ''):
        status, rh, text_body = _http_request(method, url, hdrs, params, req_body)
        if status >= 400:
            raise HttpError(f"HTTP {status}: {text_body[:200]}")
        try:
            ct = str((rh.get('content-type') or ''))
            hint = str((cfg.get('parse') or cfg.get('format') or '')).strip().lower()
            fmt_param = str((params.get('format') or params.get('FORMAT') or '')).strip().lower()
            is_csv = _is_csv_format(rh, params, cfg)
            logger.info("HTTP response: status=%s ct=%s len=%s csv_hint=%s fmt_param=%s -> is_csv=%s",
                        status, ct, len(text_body or ''), hint, fmt_param, is_csv)
            if DEBUG:
                print(f"[api_ingest] response status={status} ct={ct} len={len(text_body or '')} hint={hint} fmt={fmt_param} is_csv={is_csv}", flush=True)
        except Exception:
            is_csv = _is_csv_format(rh, params, cfg)
        if is_csv:
            items = _parse_csv(text_body)
            try:
                logger.info("parsed CSV rows: %d", len(items))
                if DEBUG:
                    print(f"[api_ingest] parsed CSV rows: {len(items)}", flush=True)
                    if items:
                        try:
                            print(f"[api_ingest] csv first row keys: {list(items[0].keys())[:15]}", flush=True)
                        except Exception:
                            pass
            except Exception:
                pass
        else:
            try:
                obj = json.loads(text_body)
            except Exception:
                obj = None
            items = _get_json_root(obj, json_root)
            try:
                logger.info("parsed JSON items: %d", len(items))
                if DEBUG:
                    print(f"[api_ingest] parsed JSON items: {len(items)}", flush=True)
            except Exception:
                pass
    elif pagetype == 'page':
        page_param = pagination.get('pageParam') or 'page'
        size_param = pagination.get('pageSizeParam') or 'limit'
        page_size = int(pagination.get('pageSize') or 100)
        page_start = int(pagination.get('pageStart') or 1)
        max_pages = int(pagination.get('maxPages') or 10)
        for page in range(page_start, page_start + max_pages):
            params[page_param] = str(page)
            params[size_param] = str(page_size)
            status, rh, text_body = _http_request(method, url, hdrs, params, req_body)
            if status >= 400:
                raise HttpError(f"HTTP {status}: {text_body[:200]}")
            try:
                ct = str((rh.get('content-type') or ''))
                hint = str((cfg.get('parse') or cfg.get('format') or '')).strip().lower()
                fmt_param = str((params.get('format') or params.get('FORMAT') or '')).strip().lower()
                is_csv = _is_csv_format(rh, params, cfg)
                logger.info("page %s: status=%s ct=%s len=%s csv_hint=%s fmt_param=%s -> is_csv=%s",
                            page, status, ct, len(text_body or ''), hint, fmt_param, is_csv)
            except Exception:
                is_csv = _is_csv_format(rh, params, cfg)
            if is_csv:
                part = _parse_csv(text_body)
                try:
                    logger.info("parsed CSV rows (page %s): %d", page, len(part))
                except Exception:
                    pass
            else:
                try:
                    obj = json.loads(text_body)
                except Exception:
                    obj = None
                part = _get_json_root(obj, json_root)
                try:
                    logger.info("parsed JSON items (page %s): %d", page, len(part))
                except Exception:
                    pass
            if not part:
                break
            items.extend(part)
            if len(part) < page_size:
                break
    elif pagetype == 'cursor':
        cursor_param = pagination.get('cursorParam') or 'cursor'
        next_cursor_path = str(pagination.get('nextCursorPath') or '').strip()
        max_pages = int(pagination.get('maxPages') or 10)
        cur_url = url
        cur_params = dict(params)
        for _ in range(max_pages):
            status, rh, text_body = _http_request(method, cur_url, hdrs, cur_params, req_body)
            if status >= 400:
                raise HttpError(f"HTTP {status}: {text_body[:200]}")
            # Cursor pagination assumes JSON body to extract nextCursorPath.
            # If CSV is detected, parse current page and stop (no cursor token extraction possible).
            if _is_csv_format(rh, cur_params, cfg):
                part = _parse_csv(text_body)
                items.extend(part)
                try:
                    logger.info("cursor page: parsed CSV rows: %d", len(part))
                except Exception:
                    pass
                break
            try:
                obj = json.loads(text_body)
            except Exception:
                obj = None
            part = _get_json_root(obj, json_root)
            if not part:
                break
            items.extend(part)
            try:
                logger.info("cursor page: parsed JSON items: %d", len(part))
            except Exception:
                pass
            if not next_cursor_path:
                break
            # Extract next cursor
            nxt = obj
            for token in next_cursor_path.replace('$.', '').split('.'):
                if nxt is None:
                    break
                if isinstance(nxt, dict):
                    nxt = nxt.get(token)
                else:
                    nxt = None
            if not nxt:
                break
            cur_params[cursor_param] = str(nxt)
    else:
        # Unsupported: fall back to single request
        status, _rh, text_body = _http_request(method, url, hdrs, params, req_body)
        if status >= 400:
            raise HttpError(f"HTTP {status}: {text_body[:200]}")
        items = _get_json_root(text_body, json_root)
    # Flatten records
    flat: List[Dict[str, Any]] = []
    for it in items:
        try:
            rec = _flatten_record(it)
            flat.append(rec)
        except Exception:
            continue

    if not flat:
        try:
            logger.info("flattened rows: 0 (no data parsed)")
            if DEBUG:
                print("[api_ingest] flattened rows: 0", flush=True)
        except Exception:
            pass
        return {"row_count": 0, "windowStart": window_start, "windowEnd": window_end}

    # Ensure table
    # pick a non-empty sample for schema inference
    sample = None
    for d in flat:
        if isinstance(d, dict) and d:
            sample = d
            break
    if sample is None:
        # All rows are empty dicts; nothing to insert
        try:
            logger.info("no non-empty rows after parsing; nothing to insert")
            if DEBUG:
                print("[api_ingest] no non-empty rows after parsing", flush=True)
        except Exception:
            pass
        return {"row_count": 0, "windowStart": window_start, "windowEnd": window_end}
    try:
        if DEBUG:
            print(f"[api_ingest] sample keys={list(sample.keys())[:10]}", flush=True)
    except Exception:
        pass
    try:
        cols, types = _ensure_table_schema(duck_engine, dest_table, sample)
    except Exception as e:
        try:
            sample = flat[0] if flat else {}
            sample_types = { str(k): type(v).__name__ for k, v in (sample.items() if isinstance(sample, dict) else []) }
            logger.exception("ensure_table_schema failed for %s: %s; sample_keys=%s; sample_types=%s", dest_table, str(e), list(sample.keys())[:10] if isinstance(sample, dict) else None, sample_types)
            if DEBUG:
                print(f"[api_ingest] ensure_table_schema failed: {e}", flush=True)
                print(f"[api_ingest] sample_types: {sample_types}", flush=True)
        except Exception:
            pass
        raise
    try:
        logger.info("ensure table %s: %d cols (sample keys=%s)", dest_table, len(cols), list(flat[0].keys())[:10])
        if DEBUG:
            print(f"[api_ingest] ensure table {dest_table}: {len(cols)} cols", flush=True)
    except Exception:
        pass

    # For sequence: delete current window before insert (by date field)
    if window_start and window_end and (seq.get('dateField')):
        _delete_window(duck_engine, dest_table, str(seq.get('dateField')), window_start, window_end)
    # For snapshot replace mode: truncate before insert
    write_mode = (cfg.get('writeMode') or '').lower()
    if (not window_start) and write_mode in ('replace', 'truncate_insert'):
        _truncate_table(duck_engine, dest_table)

    # Insert
    row_count = _insert_rows(duck_engine, dest_table, flat)
    try:
        logger.info("inserted rows: %d into %s", row_count, dest_table)
        if DEBUG:
            print(f"[api_ingest] inserted rows: {row_count} into {dest_table}", flush=True)
    except Exception:
        pass

    # Gap fill (optional) -> create <dest>_filled
    gf = gap_fill or {}
    if bool(gf.get('enabled')):
        df = (gf.get('dateField') or seq.get('dateField') or 'date')
        kf = (gf.get('keyFields') or '').strip()
        if df and kf:
            _apply_gap_fill(duck_engine, dest_table, str(df), str(kf))

    return {"row_count": row_count, "windowStart": window_start, "windowEnd": window_end}
