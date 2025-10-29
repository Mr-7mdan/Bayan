#!/usr/bin/env python3
import os
import sys
import platform
import logging
import time
import socket
import traceback
from typing import Dict, List, Tuple
from urllib.parse import urlparse, parse_qs, unquote

# Third-party
import pyodbc  # pip install pyodbc
from sqlalchemy import create_engine, text  # pip install sqlalchemy

# --------------------------
# Logging
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("mssql-test")

# --------------------------
# Input: paste your URL here (can be overridden via env TEST_MSSQL_URL)
# --------------------------
URL = os.environ.get("TEST_MSSQL_URL") or \
      "mssql+pyodbc://sa:just4pal%40123@172.16.1.30:1433/ERPEasyCIT?driver=ODBC%20Driver%2018%20for%20SQL%20Server&Encrypt=no&TrustServerCertificate=yes&LoginTimeout=15&ConnectRetryCount=2&ConnectRetryInterval=5&Pooling=False"

# --------------------------
# Helpers
# --------------------------
def mask(s: str) -> str:
    return (s or "").replace(unquote(parsed.password) if parsed.password else "", "••••")

def mask_conn_str(cs: str) -> str:
    # Mask PWD=...; also mask ;Password=...
    import re
    cs2 = re.sub(r"(PWD|Password)=([^;]*)", r"\1=••••", cs, flags=re.IGNORECASE)
    return cs2

def check_socket(host: str, port: int, timeout: float = 5.0) -> Tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, "TCP reachable"
    except Exception as e:
        return False, f"TCP connect failed: {e}"

def list_env() -> Dict[str, str]:
    keys = ["ODBCSYSINI", "ODBCINI", "DYLD_LIBRARY_PATH", "LD_LIBRARY_PATH", "PATH"]
    return {k: os.environ.get(k) or "" for k in keys}

def list_drivers() -> List[str]:
    try:
        return pyodbc.drivers()
    except Exception as e:
        log.warning("pyodbc.drivers() failed: %s", e)
        return []

def build_odbc_conn_str(
    server_host: str,
    server_port: int,
    database: str,
    user: str,
    password: str,
    driver: str = "ODBC Driver 18 for SQL Server",
    extras: Dict[str, str] | None = None,
) -> str:
    # For SQL Server, ODBC expects host,port (comma separator)
    server = f"{server_host},{server_port}" if server_port else server_host
    drv_clean = driver.strip().strip("{}")  # sanitize braces, then re-wrap
    parts = [
        f"DRIVER={{{{}}}}".format(drv_clean),
        f"SERVER={server}",
        f"DATABASE={database}",
        f"UID={user}",
        f"PWD={password}",
    ]
    for k, v in (extras or {}).items():
        parts.append(f"{k}={v}")
    return ";".join(parts)

def try_pyodbc(conn_str: str, label: str) -> None:
    log.info("== pyodbc test: %s", label)
    log.info("Connection string:\n%s", mask_conn_str(conn_str))
    t0 = time.perf_counter()
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            row = cur.fetchone()
            log.info("SELECT 1 => %s", row[0] if row else None)
            # Optional server info
            try:
                cur.execute("SELECT CONVERT(varchar(128), SERVERPROPERTY('ProductVersion'))")
                ver = cur.fetchone()
                log.info("Server version => %s", ver[0] if ver else None)
            except Exception as e:
                log.debug("Server version query failed: %s", e)
        log.info("Result: SUCCESS (%.0f ms)", (time.perf_counter() - t0) * 1000)
    except Exception as e:
        log.error("Result: FAILURE (%.0f ms)", (time.perf_counter() - t0) * 1000)
        log.error("Exception: %s", repr(e))
        log.debug("Traceback:\n%s", traceback.format_exc())

def try_sqlalchemy(sa_url: str, label: str) -> None:
    log.info("== SQLAlchemy test: %s", label)
    # Mask only in logs
    log.info("URL:\n%s", sa_url.replace(":" + (unquote(parsed.password) or ""), ":••••") if parsed.password else sa_url)
    t0 = time.perf_counter()
    try:
        engine = create_engine(sa_url, pool_pre_ping=True, pool_recycle=300)
        with engine.connect() as conn:
            res = conn.execute(text("SELECT 1"))
            val = res.scalar()
            log.info("SELECT 1 => %s", val)
        log.info("Result: SUCCESS (%.0f ms)", (time.perf_counter() - t0) * 1000)
    except Exception as e:
        log.error("Result: FAILURE (%.0f ms)", (time.perf_counter() - t0) * 1000)
        log.error("Exception: %s", repr(e))
        log.debug("Traceback:\n%s", traceback.format_exc())

# --------------------------
# Parse provided URL
# --------------------------
parsed = urlparse(URL)
if parsed.scheme.lower() != "mssql+pyodbc":
    log.warning("Unexpected scheme %s; proceeding anyway.", parsed.scheme)

username = unquote(parsed.username or "")
password = unquote(parsed.password or "")
host = parsed.hostname or ""
port = int(parsed.port or 1433)
database = unquote(parsed.path or "").lstrip("/") or ""
qs = parse_qs(parsed.query or "")
driver = unquote((qs.get("driver") or ["ODBC Driver 18 for SQL Server"])[0])

# --------------------------
# Diagnostics
# --------------------------
log.info("Python      : %s", sys.version.split()[0])
log.info("Platform    : %s | %s", platform.system(), platform.machine())
log.info("pyodbc      : %s", getattr(pyodbc, "__version__", "unknown"))
try:
    import sqlalchemy as _sa
    log.info("SQLAlchemy  : %s", getattr(_sa, "__version__", "unknown"))
except Exception:
    pass
log.info("ODBC drivers: %s", list_drivers())
envs = list_env()
for k, v in envs.items():
    log.info("ENV %-14s: %s", k, v)

ok, note = check_socket(host, port, timeout=5.0)
log.info("Socket %s:%s => %s (%s)", host, port, "OK" if ok else "FAIL", note)

# --------------------------
# Incremental pyodbc tests
# --------------------------
steps: List[Tuple[str, Dict[str, str]]] = [
    ("A) Bare minimum (no TLS params)", {}),
    ("B) Add Encrypt=no", {"Encrypt": "no"}),
    ("C) Add TrustServerCertificate=yes", {"Encrypt": "no", "TrustServerCertificate": "yes"}),
    ("D) Add timeouts", {"Encrypt": "no", "TrustServerCertificate": "yes", "LoginTimeout": "15"}),
    ("E) Add retry & pooling", {
        "Encrypt": "no", "TrustServerCertificate": "yes",
        "LoginTimeout": "15", "ConnectRetryCount": "2", "ConnectRetryInterval": "5",
        "Pooling": "False",
    }),
    ("F) Try Encrypt=yes + TrustServerCertificate=yes", {"Encrypt": "yes", "TrustServerCertificate": "yes", "LoginTimeout": "15"}),
]

for label, extras in steps:
    cs = build_odbc_conn_str(host, port, database, username, password, driver=driver, extras=extras)
    try_pyodbc(cs, label)

# --------------------------
# SQLAlchemy URL (as-given)
# --------------------------
try_sqlalchemy(URL, "G) SQLAlchemy URL (as provided)")

# --------------------------
# SQLAlchemy via odbc_connect (encoded)
# --------------------------
final_extras = {"Encrypt": "no", "TrustServerCertificate": "yes", "LoginTimeout": "15", "ConnectRetryCount": "2", "ConnectRetryInterval": "5", "Pooling": "False"}
odbc_raw = build_odbc_conn_str(host, port, database, username, password, driver=driver, extras=final_extras)
import urllib.parse as _u
odbc_enc = _u.quote(odbc_raw)
odbc_url = f"mssql+pyodbc:///?odbc_connect={odbc_enc}"
try_sqlalchemy(odbc_url, "H) SQLAlchemy via odbc_connect (encoded)")

log.info("Done.")