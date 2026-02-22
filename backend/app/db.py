from __future__ import annotations

# Optional native duckdb for write paths
try:
    import duckdb as _duckdb  # type: ignore
except Exception:  # pragma: no cover
    _duckdb = None


class _DuckNativeAdapter:
    """Minimal adapter exposing exec_driver_sql/commit/ctx for native duckdb.Connection."""
    def __init__(self, db_path: str):
        if _duckdb is None:
            raise RuntimeError("duckdb module not available")
        # Open a true native connection to avoid duckdb-engine hashing/init quirks.
        # If an existing SQLAlchemy connection conflicts, dispose the engine and retry.
        from .db import dispose_duck_engine as _dispose
        # Normalize target path
        target = db_path or ""
        try:
            if target and target != ":memory:" and target.startswith("/."):
                target = os.path.abspath(target[1:])
            elif target and target != ":memory:" and not os.path.isabs(target):
                target = os.path.abspath(target)
        except Exception:
            pass
        try:
            self._con = _duckdb.connect(target or ":memory:")
        except Exception as e:
            if "different configuration" in str(e).lower():
                try:
                    _dispose()
                except Exception:
                    pass
                self._con = _duckdb.connect(target or ":memory:")
            else:
                raise
        try:
            _apply_duck_pragmas(self._con)
        except Exception:
            pass

    def exec_driver_sql(self, sql: str, params=None):
        if params is None:
            return self._con.execute(sql)
        # list of tuples => executemany
        if isinstance(params, list):
            return self._con.executemany(sql, params)
        # single tuple
        return self._con.execute(sql, params)

    def commit(self):
        try:
            self._con.commit()
        except Exception:
            pass

    def close(self):
        try:
            self._con.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self._con.commit()
        except Exception:
            pass
        try:
            self._con.close()
        except Exception:
            pass
        return False


class _DuckSharedAdapter:
    """Adapter exposing exec_driver_sql/commit for the shared connection.
    Close is a no-op; used to conform to context manager usage.
    """
    def __init__(self):
        self._con = _get_duck_shared(settings.duckdb_path)
    def exec_driver_sql(self, sql: str, params=None):
        if params is None:
            return self._con.execute(sql)
        if isinstance(params, list):
            return self._con.executemany(sql, params)
        return self._con.execute(sql, params)
    def commit(self):
        try:
            self._con.commit()
        except Exception:
            pass
    def close(self):
        # Do not close shared connection
        pass
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        try:
            self.commit()
        except Exception:
            pass
        return False


def _open_duck_write_conn(duck_engine: Engine):
    """Return a context manager suitable for DuckDB writes using a shared connection."""
    if _duckdb is not None:
        try:
            return _DuckSharedAdapter()
        except Exception:
            pass
    # Fallback to SQLAlchemy transactional connection
    return duck_engine.begin()

import os
from pathlib import Path
import random
import socket
import time
import calendar
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, Callable

from sqlalchemy import create_engine, text, event


def _apply_mysql_keepalive(dbapi_conn, connection_record):
    """Enable TCP keepalive on every new MySQL connection.
    This sends periodic OS-level heartbeat packets that prevent firewalls,
    NAT gateways, and load-balancer proxies from silently killing idle TCP
    connections while MySQL is computing a long-running query.
    """
    try:
        sock = getattr(dbapi_conn, '_sock', None)
        if sock is None:
            inner = getattr(dbapi_conn, 'connection', dbapi_conn)
            sock = getattr(inner, '_sock', inner)
        if not hasattr(sock, 'setsockopt'):
            return
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Linux uses TCP_KEEPIDLE; macOS uses TCP_KEEPALIVE for the same purpose
        _idle_const = getattr(socket, 'TCP_KEEPIDLE', None) or getattr(socket, 'TCP_KEEPALIVE', None)
        if _idle_const is not None:
            sock.setsockopt(socket.IPPROTO_TCP, _idle_const, 60)
        if hasattr(socket, 'TCP_KEEPINTVL'):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
        if hasattr(socket, 'TCP_KEEPCNT'):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6)
        print("[SYNC] TCP keepalive enabled on MySQL connection", flush=True)
    except Exception:
        pass
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ArgumentError
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import re
from sqlalchemy.engine import Engine

from .config import settings


_DATA_DIR = Path(settings.duckdb_path).resolve().parent
_DATA_DIR.mkdir(parents=True, exist_ok=True)
# Use metadata DB directory for app-scoped files
_APP_DATA_DIR = Path(settings.metadata_db_path).resolve().parent
try:
    _APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    pass
_ACTIVE_DUCK_FILE = _APP_DATA_DIR / "duckdb.active"

# --- SQL helpers (dialect + quoting) ---
def _dialect_name(engine: Engine) -> str:
    try:
        return str(getattr(getattr(engine, 'dialect', None), 'name', '') or '').lower()
    except Exception:
        return ''


def _quote_ident(name: str, dialect: str) -> str:
    if (dialect or '').startswith('mssql'):
        return '[' + str(name).replace(']', ']]') + ']'
    return name


def _compose_table_name(schema: str | None, table: str, dialect: str) -> str:
    if (dialect or '').startswith('mssql'):
        if schema:
            return f"{_quote_ident(schema, dialect)}.{_quote_ident(table, dialect)}"
        return _quote_ident(table, dialect)
    return f"{(schema + '.') if schema else ''}{table}"


def _quote_duck_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'

# Module-level cached engines
# DuckDB engine is cached separately from external engines
_DUCK_ENGINE: Engine | None = None
_DUCK_ENGINE_PATH: str | None = None
# Cache for external datasource engines, keyed by DSN (normalized)
_ENGINE_CACHE: dict[str, Engine] = {}
_ENGINE_REVERSE: dict[int, str] = {}
_DUCK_CONFIGURED: bool = False

# Single shared native DuckDB connection (Option A)
_DUCK_SHARED_CONN = None
_DUCK_SHARED_PATH: str | None = None


def _compute_duck_config() -> dict:
    """Compute a consistent DuckDB connect-time config from env and defaults."""
    # Threads
    threads_env = os.getenv("DUCKDB_THREADS")
    try:
        n_threads = int(str(threads_env).strip()) if threads_env is not None and str(threads_env).strip() != '' else None
    except Exception:
        n_threads = None
    if n_threads is None:
        try:
            n_threads = max(1, min(32, int(os.cpu_count() or 4)))
        except Exception:
            n_threads = 4
    # Memory limit
    mem_limit = None
    ml_env = os.getenv("DUCKDB_MEMORY_LIMIT")
    if ml_env and str(ml_env).strip():
        mem_limit = str(ml_env).strip()
    else:
        mm_env = os.getenv("DUCKDB_MEM_MB")
        try:
            if mm_env is not None and str(mm_env).strip() != '':
                mv = int(str(mm_env).strip())
                if mv > 0:
                    mem_limit = f"{mv}MB"
        except Exception:
            pass
    # Temp directory
    td_env = os.getenv("DUCKDB_TEMP_DIR")
    try:
        tdir = str(td_env).strip() if td_env and str(td_env).strip() else str((_DATA_DIR / "duckdb_tmp").resolve())
    except Exception:
        tdir = str((_DATA_DIR / "duckdb_tmp").resolve())
    try:
        Path(tdir).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    cfg: dict = {
        "threads": int(n_threads),
        "enable_object_cache": True,
    }
    if mem_limit:
        cfg["memory_limit"] = str(mem_limit)
    if tdir:
        cfg["temp_directory"] = str(tdir)
    return cfg


def _normalize_duck_path(p: str) -> str:
    t = p or ""
    try:
        if t and t != ":memory:" and t.startswith("/."):
            t = os.path.abspath(t[1:])
        elif t and t != ":memory:" and not os.path.isabs(t):
            t = os.path.abspath(t)
    except Exception:
        pass
    return t or ":memory:"


def init_duck_shared(db_path: str | None = None) -> None:
    """Initialize a single shared native DuckDB connection for the process."""
    global _DUCK_SHARED_CONN, _DUCK_SHARED_PATH
    if _duckdb is None:
        raise RuntimeError("duckdb module not available")
    target = _normalize_duck_path(db_path or settings.duckdb_path)
    if _DUCK_SHARED_CONN is not None:
        # If already open to the same target, do nothing
        if _DUCK_SHARED_PATH == target:
            return
        # Close and reopen if path changed
        try:
            _DUCK_SHARED_CONN.close()
        except Exception:
            pass
        _DUCK_SHARED_CONN = None
        _DUCK_SHARED_PATH = None
    # Open with small retry if engine conflicts exist
    attempts = 0
    while True:
        try:
            _DUCK_SHARED_CONN = _duckdb.connect(target)
            _DUCK_SHARED_PATH = target
            break
        except Exception as e:
            if ("different configuration" in str(e).lower()) and attempts < 2:
                try:
                    dispose_duck_engine()
                except Exception:
                    pass
                try:
                    time.sleep(0.05 + 0.02 * attempts)
                except Exception:
                    pass
                attempts += 1
                continue
            raise
    try:
        _apply_duck_pragmas(_DUCK_SHARED_CONN)
    except Exception:
        pass


def _get_duck_shared(db_path: str | None = None):
    if _DUCK_SHARED_CONN is None:
        init_duck_shared(db_path)
    return _DUCK_SHARED_CONN


def open_duck_native(db_path: str | None = None):
    """Return a context manager yielding a duckdb.Cursor.
    - If db_path is None or equals the shared connection path, use the shared connection.
    - If db_path differs, open a temporary native connection to that path and close it on exit.
    """
    if _duckdb is None:
        raise RuntimeError("duckdb module not available")
    try:
        active_default = get_active_duck_path()
    except Exception:
        active_default = settings.duckdb_path
    target = _normalize_duck_path(db_path or (_DUCK_SHARED_PATH or active_default))
    use_shared = (_DUCK_SHARED_CONN is not None and _normalize_duck_path(_DUCK_SHARED_PATH or '') == target)
    if use_shared:
        con = _get_duck_shared(target)
        cur = con.cursor()
        class _CursorWrap:
            def __init__(self, c): self._c = c
            def __getattr__(self, name): return getattr(self._c, name)
            def __enter__(self): return self._c
            def __exit__(self, exc_type, exc, tb):
                try: self._c.close()
                except Exception: pass
                return False
        return _CursorWrap(cur)
    # Open an ephemeral connection to the requested path
    con = _duckdb.connect(target)
    try:
        _apply_duck_pragmas(con)
    except Exception:
        pass
    cur = con.cursor()
    class _TmpCursorWrap:
        def __init__(self, c, conn): self._c = c; self._conn = conn
        def __getattr__(self, name): return getattr(self._c, name)
        def __enter__(self): return self._c
        def __exit__(self, exc_type, exc, tb):
            try: self._c.close()
            except Exception: pass
            try: self._conn.close()
            except Exception: pass
            return False
    return _TmpCursorWrap(cur, con)


def close_duck_shared() -> None:
    """Close the shared DuckDB connection (used on app shutdown)."""
    global _DUCK_SHARED_CONN, _DUCK_SHARED_PATH
    try:
        if _DUCK_SHARED_CONN is not None:
            _DUCK_SHARED_CONN.close()
    except Exception:
        pass
    _DUCK_SHARED_CONN = None
    _DUCK_SHARED_PATH = None


def get_active_duck_path() -> str:
    """Return the normalized path of the currently active DuckDB store.
    Prefers the shared connection target when initialized; otherwise falls back to settings.duckdb_path.
    """
    # Reconcile with persisted active path across processes
    try:
        if _ACTIVE_DUCK_FILE.exists():
            try:
                persisted = _ACTIVE_DUCK_FILE.read_text(encoding="utf-8").strip()
            except Exception:
                persisted = ""
            if persisted:
                target = _normalize_duck_path(persisted)
                cur = _normalize_duck_path(_DUCK_SHARED_PATH or "") if _DUCK_SHARED_PATH else None
                # If no shared conn or mismatch, reinitialize to the persisted target
                if (cur is None) or (cur != target):
                    try:
                        init_duck_shared(target)
                    except Exception:
                        pass
                return target
        p = _DUCK_SHARED_PATH or _normalize_duck_path(settings.duckdb_path)
    except Exception:
        p = settings.duckdb_path
    try:
        return _normalize_duck_path(p)
    except Exception:
        return (p or ":memory:")


def set_active_duck_path(new_path: str) -> str:
    """Switch the global active DuckDB store to new_path at runtime.
    This updates settings.duckdb_path, reinitializes the shared native connection,
    and disposes the cached SQLAlchemy engine so subsequent calls use the new path.
    Returns the normalized path actually set.
    """
    raw = (new_path or settings.duckdb_path or '').strip()
    # Accept DSN formats like duckdb:///path or duckdb:////abs, and extract the path
    low = raw.lower()
    if low.startswith('duckdb:////'):
        pth = '/' + raw[len('duckdb:////'):]
    elif low.startswith('duckdb:///'):
        pth = raw[len('duckdb:///'):]
    elif low.startswith('duckdb://'):
        pth = raw[len('duckdb://'):]
    elif low.startswith('duckdb:'):
        pth = raw[len('duckdb:'):]
    else:
        pth = raw
    p = _normalize_duck_path(pth or settings.duckdb_path)
    # Update settings and reload shared connection
    try:
        settings.duckdb_path = p  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        # Dispose previous engine before re-init
        dispose_duck_engine()
    except Exception:
        pass
    try:
        init_duck_shared(p)
    except Exception:
        # Best-effort: if shared cannot reinit, leave path updated; engine will re-create on demand
        pass
    # Persist selection so it survives reloads
    try:
        _ACTIVE_DUCK_FILE.write_text(p, encoding="utf-8")
    except Exception:
        pass
    return p


# Bootstrap: if an active path was persisted previously, ensure settings/shared reflect it on import
try:
    if _ACTIVE_DUCK_FILE.exists():
        _persisted = _ACTIVE_DUCK_FILE.read_text(encoding="utf-8").strip()
        if _persisted:
            _p = _normalize_duck_path(_persisted)
            try:
                settings.duckdb_path = _p  # type: ignore[attr-defined]
            except Exception:
                pass
            try:
                if str(os.getenv("BAYAN_INIT_DUCK_ON_IMPORT", "0")).strip().lower() in ("1", "true", "yes", "on"):
                    init_duck_shared(_p)
            except Exception:
                pass
except Exception:
    pass


def _apply_duck_pragmas(conn) -> None:
    """Apply performance-related PRAGMAs on an open DuckDB connection.
    Safe to call on both native duckdb connections and SQLAlchemy DB-API connections.
    """
    cfg = _compute_duck_config()
    try:
        th = cfg.get("threads")
        if th:
            conn.execute(f"PRAGMA threads={int(th)}")
    except Exception:
        pass
    try:
        ml = cfg.get("memory_limit")
        if ml:
            conn.execute(f"PRAGMA memory_limit='{ml}'")
    except Exception:
        pass
    try:
        if cfg.get("enable_object_cache"):
            conn.execute("PRAGMA enable_object_cache=true")
    except Exception:
        pass
    try:
        td = cfg.get("temp_directory")
        if td:
            conn.execute(f"PRAGMA temp_directory='{td}'")
    except Exception:
        pass


def get_duckdb_engine() -> Engine:
    """Return a cached SQLAlchemy engine for the local DuckDB store."""
    global _DUCK_ENGINE, _DUCK_ENGINE_PATH, _DUCK_CONFIGURED
    try:
        desired_path = get_active_duck_path()
    except Exception:
        desired_path = _normalize_duck_path(settings.duckdb_path)
    if _DUCK_ENGINE is not None and _DUCK_ENGINE_PATH:
        try:
            if _normalize_duck_path(_DUCK_ENGINE_PATH) != _normalize_duck_path(desired_path):
                dispose_duck_engine()
        except Exception:
            pass
    if _DUCK_ENGINE is None:
        # Build an absolute-path DSN so SQLAlchemy/duckdb-engine doesn't treat relative '.data' as '/.data'
        try:
            path = desired_path
            if path and path != ":memory:" and path.startswith("/."):
                path = os.path.abspath(path[1:])
            elif path and path != ":memory:" and not os.path.isabs(path):
                path = os.path.abspath(path)
            if path == ":memory":
                dsn = "duckdb:///:memory:"
            else:
                dsn = f"duckdb:////{path}"
        except Exception:
            dsn = f"duckdb:///{settings.duckdb_path}"
        _DUCK_ENGINE = create_engine(dsn)
        try:
            _DUCK_ENGINE_PATH = desired_path
        except Exception:
            _DUCK_ENGINE_PATH = None
        try:
            # Ensure every new DB-API connection applies PRAGMAs
            event.listen(_DUCK_ENGINE, "connect", lambda dbapi_conn, conn_record: _apply_duck_pragmas(dbapi_conn))
        except Exception:
            pass
        _DUCK_CONFIGURED = True
    return _DUCK_ENGINE


def get_engine_from_dsn(dsn: str) -> Engine:
    """Create (and cache) an engine from a SQLAlchemy DSN.

    Normalizes MySQL DSNs to pymysql. Applies conservative pool settings to avoid
    pool exhaustion and stale connections, and reuses engines across requests.
    """
    d = (dsn or "").strip()
    low = d.lower()
    if low.startswith("mysql://"):
        d = "mysql+pymysql://" + d[len("mysql://"):]

    # Return cached engine if present (cache key is the original DSN string after minimal normalization above)
    eng = _ENGINE_CACHE.get(d)
    if eng is not None:
        return eng

    # Extract optional SQLAlchemy pool tuning params from DSN query: sa_pool_size, sa_max_overflow, sa_pool_timeout, sa_pool_clamp
    # These are our custom keys and will be stripped from the DSN passed to create_engine.
    sa_params: dict[str, str] = {}
    d_clean = d
    try:
        parts = urlparse(d)
        raw_q = parts.query or ""
        # If odbc_connect is present, avoid double-encoding by removing sa_* via regex on the raw query
        if re.search(r"\bodbc_connect=", raw_q, flags=re.IGNORECASE):
            # Capture sa_* params and remove them without touching the odbc_connect value
            q2 = re.sub(r"(&?(?:sa_pool_clamp|sa_pool_size|sa_max_overflow|sa_pool_timeout)=[^&]*)", "", raw_q, flags=re.IGNORECASE)
            # Also collect values for kwargs if present (non-critical)
            for m in re.finditer(r"(?:^|&)sa_([a-z_]+)=([^&]*)", raw_q, flags=re.IGNORECASE):
                sa_params[m.group(1).lower()] = m.group(2)
            # Normalize leading '&'
            if q2.startswith('&'):
                q2 = q2[1:]
            d_clean = urlunparse(parts._replace(query=q2))
        else:
            # Safe path: parse and rebuild query excluding sa_* keys
            qs_pairs = parse_qsl(raw_q, keep_blank_values=True)
            keep_pairs: list[tuple[str, str]] = []
            for k, v in qs_pairs:
                kl = (k or '').lower()
                if kl in {"sa_pool_size", "sa_max_overflow", "sa_pool_timeout", "sa_pool_clamp"}:
                    sa_params[kl] = v
                else:
                    keep_pairs.append((k, v))
            if sa_params:
                new_q = urlencode(keep_pairs)
                d_clean = urlunparse(parts._replace(query=new_q))
    except Exception:
        d_clean = d
        sa_params = {}

    # Pool tuning
    kwargs: dict = {"pool_pre_ping": True}
    if low.startswith("sqlite"):
        # SQLite in multithreaded FastAPI: allow cross-thread connections
        kwargs.update({
            "connect_args": {"check_same_thread": False},
            # Keep a modest pool; SQLite serializes writes anyway
            "pool_size": 5,
            "max_overflow": 10,
        })
    elif low.startswith("duckdb"):
        # For DuckDB, apply PRAGMAs after connect rather than passing connect-time config
        # to avoid configuration mismatch across concurrent connections
        pass
    else:
        # Network DBs (Postgres/MySQL/MSSQL/etc.)
        kwargs.update({
            "pool_size": 5,
            "max_overflow": 20,
            "pool_recycle": 1800,  # seconds
        })
        # For MySQL/PyMySQL: set client-side socket timeouts so large result
        # transfers don't fail silently mid-way.
        if 'mysql' in low or 'pymysql' in low:
            ca = dict(kwargs.get('connect_args') or {})
            ca.setdefault('read_timeout', 7200)
            ca.setdefault('write_timeout', 7200)
            ca.setdefault('connect_timeout', 30)
            kwargs['connect_args'] = ca
        # For SQL Server via pyodbc, set login timeout to mitigate HYT00 (Login timeout expired)
        if "mssql+pyodbc" in low:
            ca = dict(kwargs.get("connect_args") or {})
            # Prefer setting SQL_ATTR_LOGIN_TIMEOUT via attrs_before when pyodbc is present
            try:
                import pyodbc as _pyodbc  # type: ignore
                attrs = dict(ca.get("attrs_before") or {})
                # Only set if not already specified
                attrs.setdefault(_pyodbc.SQL_ATTR_LOGIN_TIMEOUT, 30)
                ca["attrs_before"] = attrs
            except Exception:
                # Fallback: not as reliable for login, but better than nothing
                ca.setdefault("timeout", 30)
            kwargs["connect_args"] = ca
            # Optional: clamp SQLAlchemy pool via DSN query params
            try:
                clamp = str(sa_params.get("sa_pool_clamp", "")).strip().lower() in {"1", "true", "yes"}
                ps = int(sa_params.get("sa_pool_size", "1" if clamp else "5"))
                mo = int(sa_params.get("sa_max_overflow", "0" if clamp else "20"))
                pt = int(sa_params.get("sa_pool_timeout", "5" if clamp else "30"))
                if clamp:
                    kwargs["pool_size"] = max(0, ps)
                    kwargs["max_overflow"] = max(0, mo)
                    kwargs["pool_timeout"] = max(0, pt)
            except Exception:
                pass

    try:
        eng = create_engine(d_clean, **kwargs)
    except ArgumentError:
        # Fallback to original DSN if cleaned value failed to parse
        eng = create_engine(d, **kwargs)
    try:
        if low.startswith("duckdb"):
            event.listen(eng, "connect", lambda dbapi_conn, conn_record: _apply_duck_pragmas(dbapi_conn))
        elif 'mysql' in low or 'pymysql' in low:
            event.listen(eng, "connect", _apply_mysql_keepalive)
    except Exception:
        pass
    _ENGINE_CACHE[d] = eng
    _ENGINE_REVERSE[id(eng)] = d
    return eng


def test_engine_connection(engine: Engine) -> tuple[bool, Optional[str]]:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except Exception as e:  # pragma: no cover - basic smoke test only
        return False, str(e)


def dispose_engine_by_key(dsn_key: str) -> bool:
    """Dispose and remove a cached engine by its DSN cache key (original DSN string after minimal normalization)."""
    eng = _ENGINE_CACHE.pop(dsn_key, None)
    if eng is None:
        return False
    try:
        eng.dispose()
    except Exception:
        pass
    try:
        _ENGINE_REVERSE.pop(id(eng), None)
    except Exception:
        pass
    return True


def dispose_engine(engine: Engine) -> bool:
    """Dispose an engine instance and remove it from cache if present."""
    try:
        key = _ENGINE_REVERSE.pop(id(engine), None)
        if key is not None:
            _ENGINE_CACHE.pop(key, None)
        try:
            engine.dispose()
        except Exception:
            pass
        return True
    except Exception:
        try:
            engine.dispose()
        except Exception:
            pass
        return False


def dispose_all_engines() -> int:
    """Dispose and clear all cached external engines."""
    count = 0
    try:
        keys = list(_ENGINE_CACHE.keys())
        for k in keys:
            try:
                _ENGINE_CACHE[k].dispose()
                count += 1
            except Exception:
                pass
        _ENGINE_CACHE.clear()
        _ENGINE_REVERSE.clear()
    except Exception:
        pass
    return count


def dispose_duck_engine() -> bool:
    """Dispose DuckDB engine and clear its cache."""
    global _DUCK_ENGINE, _DUCK_ENGINE_PATH, _DUCK_CONFIGURED
    if _DUCK_ENGINE is None:
        return False
    try:
        _DUCK_ENGINE.dispose()
    except Exception:
        pass
    _DUCK_ENGINE = None
    _DUCK_ENGINE_PATH = None
    _DUCK_CONFIGURED = False
    return True


def seed_duckdb_sample(engine: Engine) -> None:
    """Create sample tables and a view with varied data types for testing.

    This is idempotent: it creates tables if they do not exist and inserts rows
    only if the tables are empty.
    """
    return None
    ddl_customers = text(
        """
        CREATE TABLE IF NOT EXISTS customers (
          id INTEGER PRIMARY KEY,
          name TEXT,
          email TEXT,
          signup_date DATE,
          vip BOOLEAN,
          segment TEXT
        );
        """
    )
    ddl_products = text(
        """
        CREATE TABLE IF NOT EXISTS products (
          id INTEGER PRIMARY KEY,
          name TEXT,
          category TEXT,
          price DECIMAL(10,2),
          color TEXT
        );
        """
    )
    ddl_orders = text(
        """
        CREATE TABLE IF NOT EXISTS orders (
          id INTEGER PRIMARY KEY,
          customer_id INTEGER,
          product_id INTEGER,
          order_date TIMESTAMP,
          quantity INTEGER,
          unit_price DECIMAL(10,2),
          discount DOUBLE,
          status TEXT,
          notes TEXT
        );
        """
    )

    with engine.begin() as conn:
        # Create tables
        conn.execute(ddl_customers)
        conn.execute(ddl_products)
        conn.execute(ddl_orders)

        # Seed customers if empty
        cnt = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar() or 0
        if cnt == 0:
            conn.execute(text(
                """
                INSERT INTO customers (id, name, email, signup_date, vip, segment) VALUES
                (1, 'Alice Johnson', 'alice@example.com', DATE '2023-01-15', true, 'consumer'),
                (2, 'Bob Smith', 'bob@example.com', DATE '2023-03-22', false, 'business'),
                (3, 'Charlie Lee', 'charlie@example.com', DATE '2023-06-10', true, NULL),
                (4, 'Dana Kim', 'dana@example.com', DATE '2024-02-05', false, 'consumer'),
                (5, 'Evan Patel', 'evan@example.com', DATE '2024-07-19', false, 'enterprise');
                """
            ))
        # Seed products if empty
        cnt = conn.execute(text("SELECT COUNT(*) FROM products")).scalar() or 0
        if cnt == 0:
            conn.execute(text(
                """
                INSERT INTO products (id, name, category, price, color) VALUES
                (101, 'Widget A', 'Gadgets', 19.99, 'red'),
                (102, 'Widget B', 'Gadgets', 24.50, 'blue'),
                (103, 'Gizmo C', 'Tools', 49.00, NULL),
                (104, 'Gizmo D', 'Tools', 99.95, 'black'),
                (105, 'Doohickey E', 'Accessories', 9.99, 'green'),
                (106, 'Doohickey F', 'Accessories', 12.49, 'yellow');
                """
            ))

        # Seed orders if empty
        cnt = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() or 0
        if cnt == 0:
            conn.execute(text(
                """
                INSERT INTO orders (id, customer_id, product_id, order_date, quantity, unit_price, discount, status, notes) VALUES
                (1001, 1, 101, TIMESTAMP '2024-01-03 10:15:00', 2, 19.99, 0.0, 'shipped', NULL),
                (1002, 2, 103, TIMESTAMP '2024-01-05 12:30:00', 1, 49.00, 0.10, 'processing', 'promo applied'),
                (1003, 1, 102, TIMESTAMP '2024-02-14 09:05:00', 3, 24.50, 0.05, 'delivered', 'gift'),
                (1004, 3, 104, TIMESTAMP '2024-03-01 16:45:00', 1, 99.95, 0.0, 'cancelled', NULL),
                (1005, 4, 105, TIMESTAMP '2024-04-20 14:20:00', 5, 9.99, 0.15, 'delivered', 'bulk order'),
                (1006, 5, 106, TIMESTAMP '2024-05-08 11:10:00', 2, 12.49, 0.0, 'shipped', NULL),
                (1007, 2, 101, TIMESTAMP '2024-06-12 08:55:00', 4, 19.99, 0.20, 'delivered', 'clearance'),
                (1008, 3, 102, TIMESTAMP '2024-06-28 19:30:00', 1, 24.50, 0.0, 'processing', NULL),
                (1009, 4, 103, TIMESTAMP '2024-07-04 13:00:00', 2, 49.00, 0.0, 'shipped', NULL),
                (1010, 1, 106, TIMESTAMP '2024-08-15 17:40:00', 1, 12.49, 0.0, 'delivered', 'priority'),
                
                (1018, 1, 106, TIMESTAMP '2024-11-08 09:05:00', 3, 12.49, 0.05, 'delivered', 'clearance'),
                (1019, 2, 103, TIMESTAMP '2024-11-20 14:00:00', 2, 49.00, 0.0, 'shipped', NULL),
                (1020, 3, 104, TIMESTAMP '2024-12-01 13:00:00', 1, 99.95, 0.20, 'processing', NULL),
                (1021, 4, 105, TIMESTAMP '2024-12-15 17:40:00', 2, 12.49, 0.15, 'delivered', 'priority'),
                (1022, 1, 101, TIMESTAMP '2025-01-03 10:15:00', 2, 19.99, 0.0, 'shipped', NULL),
                (1023, 2, 103, TIMESTAMP '2025-01-05 12:30:00', 1, 49.00, 0.10, 'processing', 'promo applied'),
                (1024, 1, 102, TIMESTAMP '2025-01-14 09:05:00', 3, 24.50, 0.05, 'delivered', 'gift'),
                (1025, 3, 104, TIMESTAMP '2025-01-28 19:30:00', 1, 99.95, 0.0, 'cancelled', NULL),
                (1026, 4, 105, TIMESTAMP '2025-02-08 11:10:00', 5, 9.99, 0.15, 'delivered', 'bulk order'),
                (1027, 5, 106, TIMESTAMP '2025-02-15 17:40:00', 2, 12.49, 0.0, 'shipped', NULL),
                (1028, 2, 101, TIMESTAMP '2025-03-12 08:55:00', 4, 19.99, 0.20, 'delivered', 'clearance'),
                (1029, 3, 102, TIMESTAMP '2025-03-20 14:00:00', 1, 24.50, 0.05, 'shipped', NULL),
                (1030, 4, 103, TIMESTAMP '2025-04-01 13:00:00', 2, 49.00, 0.0, 'shipped', NULL),
                (1031, 1, 106, TIMESTAMP '2025-05-15 17:40:00', 1, 12.49, 0.0, 'delivered', 'priority')
                """
            ))

        # Create or replace view aggregating sales
        conn.execute(text(
            """
            CREATE OR REPLACE VIEW sales_view AS
            SELECT
              o.id as order_id,
              o.order_date,
              c.id as customer_id,
              c.name as customer_name,
              p.id as product_id,
              p.name as product_name,
              p.category,
              o.quantity,
              o.unit_price,
              o.discount,
              CAST((o.quantity * o.unit_price * (1 - o.discount)) AS DECIMAL(18,2)) as total_amount,
              o.status
            FROM orders o
            JOIN customers c ON c.id = o.customer_id
            JOIN products p ON p.id = o.product_id;
            """
        ))

# --- Minimal sync runners (MVP) ---

def _infer_duck_type_value(v) -> str:
    try:
        if v is None:
            return "TEXT"
        if isinstance(v, bool):
            return "BOOLEAN"
        if isinstance(v, int):
            return "BIGINT"
        if isinstance(v, float):
            return "DOUBLE"
        if isinstance(v, Decimal):
            # Wide default to be safe when Decimal is used
            return "DECIMAL(38,10)"
        if isinstance(v, datetime):
            return "TIMESTAMP"
        if isinstance(v, date):
            return "DATE"
        # Strings: best-effort parse for ISO date/time or numeric
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return "TEXT"
            # Try datetime
            try:
                # datetime.fromisoformat handles both date and timestamp when formatted
                dt = datetime.fromisoformat(s)
                # Heuristic: presence of 'T' or space -> timestamp, else date
                if ('T' in s) or (' ' in s):
                    return "TIMESTAMP"
                return "DATE"
            except Exception:
                # Not ISO-like -> treat as TEXT
                return "TEXT"
        # Fallback
        return "TEXT"
    except Exception:
        return "TEXT"


def _mssql_type_to_duck(data_type: str, precision: object = None, scale: object = None) -> str:
    try:
        t = str(data_type or "").strip().lower()
    except Exception:
        t = ""
    if t in {"bigint"}:
        return "BIGINT"
    if t in {"int", "integer", "smallint", "tinyint"}:
        return "BIGINT"
    if t in {"bit", "boolean"}:
        return "BOOLEAN"
    if t in {"float", "real"}:
        return "DOUBLE"
    if t in {"decimal", "numeric", "money", "smallmoney"}:
        try:
            p = int(precision) if precision is not None else None
        except Exception:
            p = None
        try:
            s = int(scale) if scale is not None else None
        except Exception:
            s = None
        if p is not None and s is not None and p > 0 and s >= 0:
            try:
                p = max(1, min(38, int(p)))
                s = max(0, min(int(p), int(s)))
                return f"DECIMAL({p},{s})"
            except Exception:
                return "DECIMAL(38,10)"
        return "DECIMAL(38,10)"
    if t in {"date"}:
        return "DATE"
    if t in {"datetime", "datetime2", "smalldatetime", "datetimeoffset", "timestamp"}:
        return "TIMESTAMP"
    if t in {"time"}:
        return "TIME"
    if t in {"uniqueidentifier"}:
        return "TEXT"
    if t in {"char", "nchar", "varchar", "nvarchar", "text", "ntext", "xml", "sysname"}:
        return "TEXT"
    if t in {"binary", "varbinary", "image"}:
        return "BLOB"
    return "TEXT"


def _fetch_mssql_column_types(src_conn, source_schema: str | None, source_table: str) -> dict[str, str]:
    try:
        sch = (source_schema or "dbo").strip() or "dbo"
        tbl = (source_table or "").strip()
        if not tbl:
            return {}
        sql = text(
            "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = :sch AND TABLE_NAME = :tbl "
            "ORDER BY ORDINAL_POSITION"
        )
        rows = src_conn.execute(sql, {"sch": sch, "tbl": tbl}).fetchall()
        out: dict[str, str] = {}
        for r in rows:
            try:
                col = str(r[0])
                dt = r[1]
                prec = r[2] if len(r) > 2 else None
                sca = r[3] if len(r) > 3 else None
                out[col] = _mssql_type_to_duck(dt, prec, sca)
            except Exception:
                continue
        return out
    except Exception:
        return {}


def _duck_table_types(conn, table_name: str) -> dict[str, str]:
    try:
        t = str(table_name or "").strip()
        if not t:
            return {}
        info = conn.exec_driver_sql(f"PRAGMA table_info('{t}')").fetchall()
        out: dict[str, str] = {}
        for row in info:
            try:
                out[str(row[1])] = str(row[2] or "").upper()
            except Exception:
                continue
        return out
    except Exception:
        return {}


def _create_table_typed(conn, table_name: str, columns: list[str], sample_rows: list[list[object]] | None = None, preferred_types: dict[str, str] | None = None) -> None:
    """Create a DuckDB table with inferred column types from sample rows.
    Falls back to TEXT when inference is not possible.
    """
    qtable = _quote_duck_ident(table_name)
    # Default all TEXT; upgrade when sample allows
    types: dict[str, str] = {c: "TEXT" for c in columns}
    try:
        if preferred_types:
            for c in columns:
                pt = preferred_types.get(c)
                if pt:
                    types[c] = str(pt)
    except Exception:
        pass
    try:
        if sample_rows:
            # Look at up to first 200 non-null examples per column
            lim = min(200, len(sample_rows))
            # Transpose-like scan
            for idx, col in enumerate(columns):
                if preferred_types and (col in preferred_types):
                    continue
                inferred: str | None = None
                for r in sample_rows[:lim]:
                    try:
                        v = r[idx] if idx < len(r) else None
                    except Exception:
                        v = None
                    if v is None:
                        continue
                    t = _infer_duck_type_value(v)
                    inferred = t
                    # Early-exit on strong types
                    if t in ("TIMESTAMP", "DATE", "DOUBLE", "DECIMAL(38,10)", "BIGINT", "BOOLEAN"):
                        break
                if inferred:
                    types[col] = inferred
    except Exception:
        # keep TEXT defaults
        pass
    
    # Log inferred types for debugging
    try:
        import sys
        sys.stderr.write(f"[SYNC] Creating table {table_name} with inferred types:\n")
        for c, t in types.items():
            sys.stderr.write(f"  {c}: {t}\n")
        sys.stderr.flush()
    except Exception:
        pass
    
    cols_sql = ", ".join(f"{_quote_duck_ident(c)} {types[c]}" for c in columns)
    conn.exec_driver_sql(f"CREATE TABLE IF NOT EXISTS {qtable} ({cols_sql})")
    return types


# Numeric types that require value sanitization
_NUMERIC_TYPES = {"DOUBLE", "BIGINT", "DECIMAL(38,10)", "INTEGER", "FLOAT", "REAL"}
_DATE_TYPES = {"DATE", "TIMESTAMP", "TIME"}


def _is_valid_numeric(val: str) -> bool:
    """Check if a string can be safely converted to a numeric type."""
    if not val or not val.strip():
        return False
    s = val.strip()
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def _is_valid_date_str(val: str, col_type: str) -> bool:
    """Check if a string is a valid date/timestamp value for DuckDB."""
    s = val.strip()
    if not s:
        return False
    try:
        from datetime import datetime as _dt
        if col_type == "TIME":
            return True  # Let DuckDB validate TIME strings
        _dt.fromisoformat(s)
        return True
    except Exception:
        return False


def _sanitize_row_for_types(row: list, columns: list[str], col_types: dict[str, str]) -> list:
    """Convert invalid values to None for typed columns to avoid DuckDB conversion errors.
    
    This handles:
    - Empty strings ('') -> None for numeric, date, and timestamp columns
    - Non-numeric strings (e.g., '39(40)') -> None for numeric columns
    - Invalid date strings (e.g., '') -> None for DATE/TIMESTAMP columns
    """
    sanitized = []
    for idx, val in enumerate(row):
        col_name = columns[idx] if idx < len(columns) else None
        col_type = col_types.get(col_name, "TEXT") if col_name else "TEXT"

        if col_type in _NUMERIC_TYPES:
            if val is None:
                sanitized.append(None)
            elif isinstance(val, (int, float)):
                sanitized.append(val)
            elif isinstance(val, str):
                sanitized.append(val if _is_valid_numeric(val) else None)
            else:
                sanitized.append(val)
        elif col_type in _DATE_TYPES:
            if val is None:
                sanitized.append(None)
            elif isinstance(val, str):
                sanitized.append(val if _is_valid_date_str(val, col_type) else None)
            else:
                # datetime/date objects — pass through as-is
                sanitized.append(val)
        else:
            sanitized.append(val)
    return sanitized


def _table_exists(conn, table_name: str) -> bool:
    try:
        qtable = _quote_duck_ident(table_name)
        conn.exec_driver_sql(f"SELECT 1 FROM {qtable} LIMIT 1")
        return True
    except Exception:
        return False


def _create_table_text(conn, table_name: str, columns: list[str]) -> None:
    qtable = _quote_duck_ident(table_name)
    cols = ", ".join(_quote_duck_ident(c) + " TEXT" for c in columns)
    conn.exec_driver_sql(f"CREATE TABLE IF NOT EXISTS {qtable} ({cols})")


def _insert_rows(conn, table_name: str, columns: list[str], rows: list[list[object]]) -> int:
    if not rows:
        return 0
    qtable = _quote_duck_ident(table_name)
    qcols = ", ".join(_quote_duck_ident(c) for c in columns)
    # Fast path: DuckDB Appender API — bypasses SQL parsing, ~10-50x faster than executemany
    native = getattr(conn, '_con', None)
    if native is not None and hasattr(native, 'appender'):
        try:
            with native.appender(table_name) as app:
                for row in rows:
                    app.append_row(*row)
            return len(rows)
        except Exception:
            pass
    # Fallback: executemany
    qmarks = ", ".join(["?" for _ in range(len(columns))])
    sql = f"INSERT INTO {qtable} ({qcols}) VALUES ({qmarks})"
    values = [tuple(r[i] for i in range(len(columns))) for r in rows]
    conn.exec_driver_sql(sql, values)
    return len(rows)


def _delete_by_pk(conn, table_name: str, pk_columns: list[str], rows: list[list[object]], columns: list[str]) -> int:
    if not rows or not pk_columns:
        return 0
    idx = [columns.index(pk) for pk in pk_columns]
    qtable = _quote_duck_ident(table_name)
    native = getattr(conn, '_con', None)
    if native is not None and len(pk_columns) == 1:
        # Fast path: single bulk DELETE via unnest — one query instead of N queries
        try:
            pk_idx = idx[0]
            pk_vals = [r[pk_idx] for r in rows]
            qpk = _quote_duck_ident(pk_columns[0])
            native.execute(f"DELETE FROM {qtable} WHERE {qpk} IN (SELECT unnest(?))", [pk_vals])
            return len(rows)
        except Exception:
            pass
    # Fallback: per-row executemany (composite PK or no native conn)
    where = " AND ".join([f"{_quote_duck_ident(pk)} = ?" for pk in pk_columns])
    sql = f"DELETE FROM {qtable} WHERE {where}"
    params = [tuple(r[j] for j in idx) for r in rows]
    conn.exec_driver_sql(sql, params)
    return len(rows)


def run_sequence_sync(source_engine: Engine, duck_engine: Engine, *,
                      source_schema: str | None,
                      source_table: str,
                      dest_table: str,
                      sequence_column: str,
                      pk_columns: list[str] | None,
                      batch_size: int = 10000,
                      last_sequence_value: int | None = None,
                      max_batches: int = 1_000_000,
                      on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
                      select_columns: Optional[list[str]] = None,
                      should_abort: Optional[Callable[[], bool]] = None,
                      on_phase: Optional[Callable[[str], None]] = None,
                      custom_query: Optional[str] = None) -> dict:
    """Incremental append+upsert by monotonic sequence. Naive row-by-row DML (MVP).
    Returns {row_count, last_sequence_value}.
    """
    pk_columns = pk_columns or []
    src_dialect = _dialect_name(source_engine)
    # Determine the FROM clause: custom query as derived table, or plain table reference
    if custom_query and custom_query.strip():
        q_from = f"({custom_query.strip().rstrip(';').strip()}) AS _src"
    else:
        q_from = _compose_table_name(source_schema, source_table, src_dialect)
    seq = int(last_sequence_value or 0)
    total_rows = 0
    max_seq_seen = seq
    fetched = 0
    # Build SELECT column list outside the connection loop
    pk_columns = pk_columns or []
    sel_set = set([c.strip() for c in (select_columns or []) if c and isinstance(c, str)])
    required = set(pk_columns + [sequence_column])
    use_star = len(sel_set) == 0
    if use_star:
        sel_clause = "*"
    else:
        final_cols = list(required | sel_set)
        if src_dialect.startswith('mssql'):
            sel_clause = ", ".join([_quote_ident(c, src_dialect) for c in final_cols])
        else:
            sel_clause = ", ".join(final_cols)

    seq_col_q = _quote_ident(sequence_column, src_dialect)

    # Flush stale pooled connections (broken SSCursor leftovers from previous runs)
    try:
        source_engine.dispose()
        print("[SYNC] Disposed stale connection pool", flush=True)
    except Exception:
        pass

    # Quick row-count estimate: information_schema only (instant, no table scan).
    total_rows_to_copy = None
    try:
        with source_engine.connect() as _cnt_conn:
            if src_dialect.startswith('mysql') and source_table and not custom_query:
                _schema_expr = f"'{source_schema}'" if source_schema else "DATABASE()"
                _fast = _cnt_conn.execute(
                    text(f"SELECT TABLE_ROWS FROM information_schema.TABLES "
                         f"WHERE TABLE_SCHEMA = {_schema_expr} AND TABLE_NAME = :t"),
                    {"t": source_table}
                ).scalar()
                if _fast is not None:
                    total_rows_to_copy = int(_fast)
                    print(f"[SYNC] Estimated rows: {total_rows_to_copy}", flush=True)
    except Exception as e:
        print(f"[SYNC] Row-count estimate failed: {e}", flush=True)
        total_rows_to_copy = None

    if on_progress:
        try:
            on_progress(0, (int(total_rows_to_copy) if total_rows_to_copy is not None else None))
        except Exception:
            pass

    copied = 0
    columns: list = []
    col_types: dict = {}

    # Paginated batches on a SINGLE reused connection.
    #
    # Why single connection: opening a fresh socket per batch exhausts macOS ephemeral
    # ports ([Errno 49] Can't assign requested address) after ~16k batches since sockets
    # linger in TIME_WAIT for ~60s. With LIMIT 1000 queries running back-to-back there is
    # zero idle gap between queries so no proxy/firewall can classify the connection as
    # idle. On disconnect we reconnect once and resume from the last committed seq value.
    _mysql_batch_cap = 1000
    _fetch_size = min(int(batch_size), _mysql_batch_cap) if src_dialect.startswith('mysql') else int(batch_size)
    if src_dialect.startswith('mssql'):
        batch_sql = f"SELECT {sel_clause} FROM {q_from} WHERE {seq_col_q} > :last ORDER BY {seq_col_q} OFFSET 0 ROWS FETCH NEXT :lim ROWS ONLY"
    else:
        batch_sql = f"SELECT {sel_clause} FROM {q_from} WHERE {seq_col_q} > :last ORDER BY {seq_col_q} LIMIT :lim"

    print(f"[SYNC] Paginated mode (single conn): fetch_size={_fetch_size}, starting seq={seq}", flush=True)

    def _open_src_conn():
        conn = source_engine.connect()
        if src_dialect.startswith('mysql'):
            try:
                conn.execute(text("SET SESSION net_read_timeout=7200, net_write_timeout=7200, wait_timeout=7200"))
            except Exception:
                pass
        return conn

    batches = 0
    _MAX_RECONNECTS = 3
    reconnects = 0

    with _open_duck_write_conn(duck_engine) as duck:
        src = _open_src_conn()
        print(f"[SYNC] Source connection opened", flush=True)
        try:
            while batches < max_batches:
                if should_abort:
                    try:
                        if should_abort():
                            print(f"[ABORT] Stopping. batches={batches}, rows={total_rows}", flush=True)
                            return {"row_count": total_rows, "last_sequence_value": max_seq_seen, "aborted": True}
                    except Exception:
                        pass
                batches += 1

                if on_phase:
                    try: on_phase('fetch')
                    except Exception: pass

                t0 = time.time()
                try:
                    res = src.execute(text(batch_sql), {"last": seq, "lim": _fetch_size})
                    rows = res.fetchall()
                    if not columns:
                        columns = [c.strip() for c in res.keys()]
                    elapsed = time.time() - t0
                    print(f"[SYNC] Batch {batches}: {len(rows)} rows in {elapsed:.1f}s (seq>{seq})", flush=True)
                except Exception as e:
                    elapsed = time.time() - t0
                    print(f"[SYNC] Batch {batches} FAILED after {elapsed:.1f}s: {e}", flush=True)
                    # Close broken connection and try to reconnect
                    try: src.close()
                    except Exception: pass
                    try: source_engine.dispose()
                    except Exception: pass
                    reconnects += 1
                    if reconnects > _MAX_RECONNECTS:
                        raise
                    wait = 2 ** (reconnects - 1)
                    print(f"[SYNC] Reconnecting in {wait}s (attempt {reconnects}/{_MAX_RECONNECTS})…", flush=True)
                    time.sleep(wait)
                    src = _open_src_conn()
                    print(f"[SYNC] Reconnected. Resuming from seq>{seq}", flush=True)
                    continue  # retry this batch on new connection

                if not rows:
                    print(f"[SYNC] No more rows. Done. batches={batches} total={total_rows}", flush=True)
                    break

                if on_phase:
                    try: on_phase('insert')
                    except Exception: pass

                if not _table_exists(duck, dest_table):
                    preferred = {}
                    try:
                        if src_dialect.startswith('mssql'):
                            with source_engine.connect() as _meta_conn:
                                preferred = _fetch_mssql_column_types(_meta_conn, source_schema, source_table)
                    except Exception:
                        preferred = {}
                    col_types = _create_table_typed(duck, dest_table, columns, [list(r) for r in rows], preferred_types=preferred)
                elif not col_types:
                    col_types = _duck_table_types(duck, dest_table)
                    for c in columns:
                        if c not in col_types:
                            col_types[c] = "TEXT"

                if pk_columns:
                    _delete_by_pk(duck, dest_table, pk_columns, rows, columns)
                sanitized_rows = [_sanitize_row_for_types(list(r), columns, col_types) for r in rows]
                inserted = _insert_rows(duck, dest_table, columns, sanitized_rows)
                total_rows += inserted
                copied += inserted

                if on_progress:
                    try:
                        on_progress(int(copied), (int(total_rows_to_copy) if total_rows_to_copy is not None else None))
                    except Exception:
                        pass

                try:
                    seq_idx = columns.index(sequence_column)
                    seq_vals = [int(r[seq_idx]) for r in rows if r[seq_idx] is not None]
                    if seq_vals:
                        max_seq_seen = max(max_seq_seen, max(seq_vals))
                        seq = max_seq_seen
                except Exception:
                    pass

                if should_abort:
                    try:
                        if should_abort():
                            return {"row_count": total_rows, "last_sequence_value": max_seq_seen, "aborted": True}
                    except Exception:
                        pass

                if len(rows) < _fetch_size:
                    print(f"[SYNC] Last batch ({len(rows)} rows) — done.", flush=True)
                    break
        finally:
            try: src.close()
            except Exception: pass

    return {"row_count": total_rows, "last_sequence_value": max_seq_seen}


def run_snapshot_sync(source_engine: Engine, duck_engine: Engine, *,
                      source_schema: str | None,
                      source_table: str,
                      dest_table: str,
                      batch_size: int = 50000,
                      on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
                      select_columns: Optional[list[str]] = None,
                      should_abort: Optional[Callable[[], bool]] = None,
                      on_phase: Optional[Callable[[str], None]] = None,
                      custom_query: Optional[str] = None) -> dict:
    """Full rebuild into a staging table, then swap. Streams full result once via fetchmany to avoid O(N²) OFFSET re-scans."""
    src_dialect = _dialect_name(source_engine)
    # Determine the FROM clause: custom query as derived table, or plain table reference
    if custom_query and custom_query.strip():
        q_source = f"({custom_query.strip().rstrip(';').strip()}) AS _src"
    else:
        q_source = _compose_table_name(source_schema, source_table, src_dialect)
    stg = f"stg_{dest_table}"
    total_rows = 0
    staging_created = False
    with source_engine.connect() as src, _open_duck_write_conn(duck_engine) as duck:
        # Drop stale staging if exists
        try:
            duck.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote_duck_ident(stg)}")
        except Exception:
            pass
        
        try:
            # Determine columns to select
            sel_set = set([c.strip() for c in (select_columns or []) if c and isinstance(c, str)])
            if len(sel_set) == 0:
                sel_clause = "*"
            else:
                if src_dialect.startswith('mssql'):
                    sel_clause = ", ".join([_quote_ident(c, src_dialect) for c in sorted(sel_set)])
                else:
                    sel_clause = ", ".join(sorted(sel_set))
            # Discover columns via a zero-row probe using the same selection
            if src_dialect.startswith('mssql'):
                probe = src.execute(text(f"SELECT TOP 0 {sel_clause} FROM {q_source}"))
            else:
                probe = src.execute(text(f"SELECT {sel_clause} FROM {q_source} LIMIT 0"))
            columns = [c.strip() for c in probe.keys()]
            # Sample a small batch to infer types for staging table
            try:
                if src_dialect.startswith('mssql'):
                    sample_sql = text(f"SELECT {sel_clause} FROM {q_source} ORDER BY (SELECT 1) OFFSET 0 ROWS FETCH NEXT :lim ROWS ONLY")
                    sample_rows = src.execute(sample_sql, {"lim": 64}).fetchall()
                else:
                    sample_sql = text(f"SELECT {sel_clause} FROM {q_source} LIMIT :lim OFFSET :off")
                    sample_rows = src.execute(sample_sql, {"lim": 64, "off": 0}).fetchall()
            except Exception:
                sample_rows = []
            preferred = {}
            try:
                if src_dialect.startswith('mssql'):
                    preferred = _fetch_mssql_column_types(src, source_schema, source_table)
            except Exception:
                preferred = {}
            col_types = _create_table_typed(duck, stg, columns, [list(r) for r in (sample_rows or [])], preferred_types=preferred)
            staging_created = True
            # Chunked copy
            total_rows_source = None
            try:
                total_rows_source = src.execute(text(f"SELECT COUNT(*) FROM {q_source}")).scalar()
                total_rows_source = int(total_rows_source) if total_rows_source is not None else None
            except Exception:
                total_rows_source = None
            # Emit initial progress tick to expose totals early
            if on_progress:
                try:
                    on_progress(0, (int(total_rows_source) if total_rows_source is not None else None))
                except Exception:
                    pass
            # Stream the full result in one shot — avoids O(N²) OFFSET re-scans
            if src_dialect.startswith('mssql'):
                full_sql = text(f"SELECT {sel_clause} FROM {q_source} ORDER BY (SELECT 1)")
            else:
                full_sql = text(f"SELECT {sel_clause} FROM {q_source}")
            try:
                stream_res = src.execution_options(stream_results=True).execute(full_sql)
            except Exception:
                stream_res = src.execute(full_sql)
            while True:
                if should_abort:
                    try:
                        abort_flag = should_abort()
                        if abort_flag:
                            print(f"[ABORT] Snapshot sync detected abort flag=True, stopping sync. total_rows={total_rows}", flush=True)
                            return {"row_count": total_rows, "aborted": True}
                    except Exception as e:
                        print(f"[ABORT] Error checking abort in snapshot sync: {e}", flush=True)
                        pass
                if on_phase:
                    try: on_phase('fetch')
                    except Exception: pass
                rows = stream_res.fetchmany(int(batch_size))
                if not rows:
                    break
                # Emit progress after fetch, before insert to reflect fetch progress
                if on_progress:
                    try:
                        on_progress(int(total_rows + len(rows)), (int(total_rows_source) if total_rows_source is not None else None))
                    except Exception:
                        pass
                if on_phase:
                    try: on_phase('insert')
                    except Exception: pass
                # Sanitize rows: convert empty strings to None for numeric columns
                sanitized_rows = [_sanitize_row_for_types(list(r), columns, col_types) for r in rows]
                _insert_rows(duck, stg, columns, sanitized_rows)
                total_rows += len(rows)
                if should_abort:
                    try:
                        if should_abort():
                            return {"row_count": total_rows, "aborted": True}
                    except Exception:
                        pass
            # Swap
            if _table_exists(duck, dest_table):
                duck.exec_driver_sql(f"DROP TABLE {_quote_duck_ident(dest_table)}")
            duck.exec_driver_sql(f"ALTER TABLE {_quote_duck_ident(stg)} RENAME TO {_quote_duck_ident(dest_table)}")
            staging_created = False  # Successfully renamed, no cleanup needed
        finally:
            # Cleanup: if staging table exists and wasn't renamed, drop it
            if staging_created:
                try:
                    duck.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote_duck_ident(stg)}")
                except Exception:
                    pass
    return {"row_count": total_rows}
