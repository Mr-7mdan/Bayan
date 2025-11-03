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
import time
import calendar
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, Callable

from sqlalchemy import create_engine, text, event
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
    target = _normalize_duck_path(db_path or (_DUCK_SHARED_PATH or settings.duckdb_path))
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
    global _DUCK_ENGINE, _DUCK_CONFIGURED
    if _DUCK_ENGINE is None:
        # Build an absolute-path DSN so SQLAlchemy/duckdb-engine doesn't treat relative '.data' as '/.data'
        try:
            path = settings.duckdb_path
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
    global _DUCK_ENGINE, _DUCK_CONFIGURED
    if _DUCK_ENGINE is None:
        return False
    try:
        _DUCK_ENGINE.dispose()
    except Exception:
        pass
    _DUCK_ENGINE = None
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
        # Strings: best-effort parse for ISO date/time
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return "TEXT"
            # Try datetime first
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


def _create_table_typed(conn, table_name: str, columns: list[str], sample_rows: list[list[object]] | None = None) -> None:
    """Create a DuckDB table with inferred column types from sample rows.
    Falls back to TEXT when inference is not possible.
    """
    qtable = _quote_duck_ident(table_name)
    # Default all TEXT; upgrade when sample allows
    types: dict[str, str] = {c: "TEXT" for c in columns}
    try:
        if sample_rows:
            # Look at up to first 200 non-null examples per column
            lim = min(200, len(sample_rows))
            # Transpose-like scan
            for idx, col in enumerate(columns):
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
    cols_sql = ", ".join(f"{_quote_duck_ident(c)} {types[c]}" for c in columns)
    conn.exec_driver_sql(f"CREATE TABLE IF NOT EXISTS {qtable} ({cols_sql})")
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
    qmarks = ", ".join(["?" for _ in range(len(columns))])
    qtable = _quote_duck_ident(table_name)
    qcols = ", ".join(_quote_duck_ident(c) for c in columns)
    sql = f"INSERT INTO {qtable} ({qcols}) VALUES ({qmarks})"
    values = [tuple(r[i] for i in range(len(columns))) for r in rows]
    conn.exec_driver_sql(sql, values)
    return len(rows)


def _delete_by_pk(conn, table_name: str, pk_columns: list[str], rows: list[list[object]], columns: list[str]) -> int:
    if not rows or not pk_columns:
        return 0
    idx = [columns.index(pk) for pk in pk_columns]
    qtable = _quote_duck_ident(table_name)
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
                      max_batches: int = 10,
                      on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
                      select_columns: Optional[list[str]] = None,
                      should_abort: Optional[Callable[[], bool]] = None,
                      on_phase: Optional[Callable[[str], None]] = None) -> dict:
    """Incremental append+upsert by monotonic sequence. Naive row-by-row DML (MVP).
    Returns {row_count, last_sequence_value}.
    """
    pk_columns = pk_columns or []
    src_dialect = _dialect_name(source_engine)
    q_source = _compose_table_name(source_schema, source_table, src_dialect)
    seq = int(last_sequence_value or 0)
    total_rows = 0
    max_seq_seen = seq
    fetched = 0
    with source_engine.connect() as src, _open_duck_write_conn(duck_engine) as duck:
        # For first batch, inspect columns
        # 
        # Build SELECT column list: ensure pk and sequence columns are included
        pk_columns = pk_columns or []
        sel_set = set([c.strip() for c in (select_columns or []) if c and isinstance(c, str)])
        required = set(pk_columns + [sequence_column])
        final_cols = list(required | sel_set) if sel_set else list(required)  # if empty, we'll still select required then insert those
        # If no select specified beyond required, we will expand to '*' for full copy
        use_star = len(sel_set) == 0
        if use_star:
            sel_clause = "*"
        else:
            if src_dialect.startswith('mssql'):
                sel_clause = ", ".join([_quote_ident(c, src_dialect) for c in final_cols])
            else:
                sel_clause = ", ".join(final_cols)
        total_rows_to_copy = None
        try:
            # Estimate total rows to copy based on sequence threshold
            q_source = _compose_table_name(source_schema, source_table, src_dialect)
            seq_col = _quote_ident(sequence_column, src_dialect)
            cnt = src.execute(text(f"SELECT COUNT(*) FROM {q_source} WHERE {seq_col} > :last"), {"last": seq}).scalar()
            if cnt is not None:
                total_rows_to_copy = int(cnt)
        except Exception:
            total_rows_to_copy = None
        # Emit initial progress tick to expose totals early
        if on_progress:
            try:
                on_progress(0, (int(total_rows_to_copy) if total_rows_to_copy is not None else None))
            except Exception:
                pass
        copied = 0
        batches = 0
        while batches < max_batches:
            if should_abort:
                try:
                    if should_abort():
                        return {"row_count": total_rows, "last_sequence_value": max_seq_seen, "aborted": True}
                except Exception:
                    pass
            batches += 1
            seq_col = _quote_ident(sequence_column, src_dialect)
            if src_dialect.startswith('mssql'):
                sql = f"SELECT {sel_clause} FROM {q_source} WHERE {seq_col} > :last ORDER BY {seq_col} OFFSET 0 ROWS FETCH NEXT :lim ROWS ONLY"
            else:
                sql = f"SELECT {sel_clause} FROM {q_source} WHERE {seq_col} > :last ORDER BY {seq_col} LIMIT :lim"
            if on_phase:
                try: on_phase('fetch')
                except Exception: pass
            res = src.execute(text(sql), {"last": seq, "lim": int(batch_size)})
            rows = res.fetchall()
            if not rows:
                break
            # Report fetch progress before any insert/upsert work
            try:
                fetched += len(rows)
            except Exception:
                fetched = (len(rows) if 'fetched' in locals() else len(rows))
            if on_progress:
                try:
                    on_progress(int(fetched), (int(total_rows_to_copy) if total_rows_to_copy is not None else None))
                except Exception:
                    pass
            columns = list(res.keys())
            # Ensure destination exists
            if not _table_exists(duck, dest_table):
                # Create typed table using the first fetched batch as sample
                _create_table_typed(duck, dest_table, columns, [list(r) for r in rows])
            # Upsert: delete existing pk matches, then insert
            if on_phase:
                try: on_phase('insert')
                except Exception: pass
            if pk_columns:
                _delete_by_pk(duck, dest_table, pk_columns, rows, columns)
            inserted = _insert_rows(duck, dest_table, columns, [list(r) for r in rows])
            total_rows += inserted
            copied += inserted
            if on_progress:
                try:
                    on_progress(int(copied), (int(total_rows_to_copy) if total_rows_to_copy is not None else None))
                except Exception:
                    pass
            if should_abort:
                try:
                    if should_abort():
                        return {"row_count": total_rows, "last_sequence_value": max_seq_seen, "aborted": True}
                except Exception:
                    pass
            # Advance sequence
            try:
                seq_idx = columns.index(sequence_column)
                seq_vals = [int(r[seq_idx]) for r in rows if r[seq_idx] is not None]
                if seq_vals:
                    max_seq_seen = max(max_seq_seen, max(seq_vals))
                    seq = max_seq_seen
            except Exception:
                # Best effort; leave seq unchanged
                pass
            if len(rows) < int(batch_size):
                break
    return {"row_count": total_rows, "last_sequence_value": max_seq_seen}


def run_snapshot_sync(source_engine: Engine, duck_engine: Engine, *,
                      source_schema: str | None,
                      source_table: str,
                      dest_table: str,
                      batch_size: int = 50000,
                      on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
                      select_columns: Optional[list[str]] = None,
                      should_abort: Optional[Callable[[], bool]] = None,
                      on_phase: Optional[Callable[[str], None]] = None) -> dict:
    """Full rebuild into a staging table, then swap. Naive chunked copy via OFFSET/LIMIT (MVP)."""
    src_dialect = _dialect_name(source_engine)
    q_source = _compose_table_name(source_schema, source_table, src_dialect)
    stg = f"stg_{dest_table}"
    total_rows = 0
    with source_engine.connect() as src, _open_duck_write_conn(duck_engine) as duck:
        # Drop stale staging if exists
        try:
            duck.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote_duck_ident(stg)}")
        except Exception:
            pass
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
        columns = list(probe.keys())
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
        _create_table_typed(duck, stg, columns, [list(r) for r in (sample_rows or [])])
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
        offset = 0
        while True:
            if should_abort:
                try:
                    if should_abort():
                        return {"row_count": total_rows, "aborted": True}
                except Exception:
                    pass
            if on_phase:
                try: on_phase('fetch')
                except Exception: pass
            if src_dialect.startswith('mssql'):
                sql = f"SELECT {sel_clause} FROM {q_source} ORDER BY (SELECT 1) OFFSET :off ROWS FETCH NEXT :lim ROWS ONLY"
            else:
                sql = f"SELECT {sel_clause} FROM {q_source} LIMIT :lim OFFSET :off"
            res = src.execute(text(sql), {"lim": int(batch_size), "off": int(offset)})
            rows = res.fetchall()
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
            _insert_rows(duck, stg, columns, [list(r) for r in rows])
            total_rows += len(rows)
            if should_abort:
                try:
                    if should_abort():
                        return {"row_count": total_rows, "aborted": True}
                except Exception:
                    pass
            if len(rows) < int(batch_size):
                break
            offset += int(batch_size)
        # Swap
        if _table_exists(duck, dest_table):
            duck.exec_driver_sql(f"DROP TABLE {_quote_duck_ident(dest_table)}")
        duck.exec_driver_sql(f"ALTER TABLE {_quote_duck_ident(stg)} RENAME TO {_quote_duck_ident(dest_table)}")
    return {"row_count": total_rows}
