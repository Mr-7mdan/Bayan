from __future__ import annotations

import os
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from .config import settings
from .schemas import (
    HealthResponse,
    TestConnectionRequest,
    TestConnectionResponse,
    BrandingOut,
    DetectRequest,
    DetectResponse,
)
from pathlib import Path
from uuid import uuid4
import json
from .db import get_engine_from_dsn, test_engine_connection
from .db import init_duck_shared, close_duck_shared, open_duck_native
from .models import init_db, SessionLocal, User
from .security import hash_password
from .routers import datasources as ds_router
from .routers import query as query_router
from .routers import dashboards as dashboards_router
from .routers import periods as periods_router
from .routers import ai as ai_router
from .routers import users as users_router
from .routers import admin as admin_router
from .scheduler import ensure_scheduler_started, schedule_all_jobs, schedule_all_alert_jobs, shutdown_scheduler
from .routers import alerts as alerts_router
from .routers import snapshot as snapshot_router
from .routers import contacts as contacts_router
from .routers import metrics as metrics_router
from .routers import updates as updates_router
from .metrics import counter_inc, gauge_inc, gauge_dec, summary_observe, render_prometheus

app = FastAPI(title=settings.app_name)

# CORS: always use explicit origins to ensure ACAO is set with credentials
origins = settings.cors_origins_list
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    # Regex safety net for common local dev hosts (Next.js dev server)
    allow_origin_regex=r"http://(localhost|127\.0\.0\.1):3000",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Respect X-Forwarded-* headers when running behind a reverse proxy (e.g., Nginx)
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")

# Basic request duration middleware (ms) and active requests gauge
@app.middleware("http")
async def _metrics_mw(request: Request, call_next):
    path = request.url.path or ""
    method = request.method or "GET"
    # Track only API endpoints
    is_api = path.startswith("/api/")
    if is_api:
        gauge_inc("app_active_requests", 1.0, {"path": path, "method": method})
    import time as _t
    _s = _t.perf_counter()
    try:
        resp: Response = await call_next(request)
        return resp
    finally:
        _e = int((_t.perf_counter() - _s) * 1000)
        if is_api:
            gauge_dec("app_active_requests", 1.0, {"path": path, "method": method})
            summary_observe("app_request_duration_ms", _e, {"path": path, "method": method})

@app.on_event("startup")
async def _startup():
    init_db()
    try:
        email = (settings.admin_email or "").strip().lower()
        password = (settings.admin_password or "").strip()
        name = (settings.admin_name or "") or (email.split("@")[0] if email else "")
        if email and password:
            db = SessionLocal()
            try:
                has_admin = db.query(User).filter((User.role == "admin")).first()
                if not has_admin:
                    existing = db.query(User).filter(User.email == email).first()
                    if existing is None:
                        u = User(id=str(uuid4()), name=name or email, email=email, password_hash=hash_password(password), role="admin", active=True)
                        db.add(u)
                        db.commit()
                        db.refresh(u)
            finally:
                db.close()
    except Exception:
        pass
    # Initialize a single shared DuckDB connection (Option A)
    try:
        init_duck_shared()
    except Exception:
        # Non-fatal in dev; continue startup
        pass
    # Start background scheduler and load jobs from DB (sync + alerts)
    try:
        run_sched = str(os.getenv("RUN_SCHEDULER", "1")).strip().lower() in ("1", "true", "yes", "on")
        if run_sched:
            ensure_scheduler_started()
            schedule_all_jobs()
            schedule_all_alert_jobs()
    except Exception:
        # Non-fatal; admin can refresh via API later
        pass

app.include_router(ds_router.router, prefix="/api")
app.include_router(query_router.router, prefix="/api")
app.include_router(dashboards_router.router, prefix="/api")
app.include_router(periods_router.router, prefix="/api")
app.include_router(users_router.router, prefix="/api")
app.include_router(admin_router.router, prefix="/api")
app.include_router(ai_router.router, prefix="/api")
app.include_router(alerts_router.router, prefix="/api")
app.include_router(snapshot_router.router, prefix="/api")
app.include_router(contacts_router.router, prefix="/api")
app.include_router(metrics_router.router, prefix="/api")
app.include_router(updates_router.router, prefix="/api")


@app.get("/api/healthz", response_model=HealthResponse)
async def healthz() -> HealthResponse:
    return HealthResponse(status="ok", app=settings.app_name, env=settings.environment)


@app.post("/api/test-connection", response_model=TestConnectionResponse)
async def test_connection(payload: TestConnectionRequest) -> TestConnectionResponse:
    if payload.dsn:
        engine = get_engine_from_dsn(payload.dsn)
        ok, err = test_engine_connection(engine)
        return TestConnectionResponse(ok=ok, error=err)
    # Local DuckDB: use shared native connection
    try:
        with open_duck_native(None) as cur:
            cur.execute("SELECT 1")
        return TestConnectionResponse(ok=True, error=None)
    except Exception as e:
        return TestConnectionResponse(ok=False, error=str(e))


def _parse_engine_from_version_string(s: str) -> str | None:
    sl = (s or "").lower()
    if "postgres" in sl or "postgresql" in sl:
        return "postgres"
    if "mysql" in sl or "mariadb" in sl:
        return "mysql"
    if "microsoft sql server" in sl or "sql server" in sl or "mssql" in sl:
        return "mssql"
    if "oracle" in sl:
        return "oracle"
    if "duckdb" in sl:
        return "duckdb"
    if "sqlite" in sl:
        return "sqlite"
    return None


def _guess_from_dsn_string(dsn: str | None) -> str | None:
    if not dsn:
        return None
    d = dsn.strip().lower()
    if d.startswith("postgresql") or d.startswith("postgres"):
        return "postgres"
    if d.startswith("mysql") or d.startswith("mariadb"):
        return "mysql"
    if d.startswith("mssql") or d.startswith("sqlserver") or ("+pyodbc" in d and "mssql" in d) or ("+pytds" in d and "mssql" in d):
        return "mssql"
    if d.startswith("oracle") or d.startswith("cx_oracle"):
        return "oracle"
    if d.startswith("duckdb"):
        return "duckdb"
    if d.startswith("sqlite"):
        return "sqlite"
    return None


@app.post("/api/detect-db", response_model=DetectResponse)
async def detect_db(payload: DetectRequest) -> DetectResponse:
    # 1) If DSN provided, try to connect and run version queries
    if payload.dsn:
        try:
            engine = get_engine_from_dsn(payload.dsn)
            version_str = None
            try:
                with engine.connect() as conn:
                    for q in ("SELECT version()", "SELECT @@version", "SELECT * FROM v$version"):
                        try:
                            res = conn.execution_options(stream_results=False).execute(q)  # type: ignore[arg-type]
                            row = res.fetchone()
                            if row is None:
                                continue
                            # Row may be a tuple or mapping; join into a single string
                            parts = []
                            try:
                                parts = [str(col) for col in row]
                            except Exception:
                                try:
                                    parts = [str(v) for v in row._mapping.values()]  # type: ignore[attr-defined]
                                except Exception:
                                    parts = [str(row[0])] if len(row) else []  # type: ignore[index]
                            version_str = " ".join(parts)
                            break
                        except Exception:
                            continue
            except Exception as e:
                # Could not run queries; try to infer from DSN text
                guess = _guess_from_dsn_string(payload.dsn)
                return DetectResponse(ok=True, detected=guess or "unknown", method="dsn", versionString=None, candidates=None, error=str(e) if guess is None else None)
            if version_str:
                detected = _parse_engine_from_version_string(version_str) or "unknown"
                return DetectResponse(ok=True, detected=detected, method="version_query", versionString=version_str)
            # Fallback to DSN guess
            guess = _guess_from_dsn_string(payload.dsn)
            return DetectResponse(ok=True, detected=guess or "unknown", method="dsn", versionString=None)
        except Exception as e:
            guess = _guess_from_dsn_string(payload.dsn)
            return DetectResponse(ok=False, detected=guess or None, method="dsn", versionString=None, error=str(e))

    # 2) If host/port provided, do socket-based hints (and MySQL handshake sniff)
    import socket
    host = (payload.host or "").strip()
    if not host:
        return DetectResponse(ok=False, detected=None, method=None, error="host or dsn is required")
    ports = [int(payload.port)] if payload.port else [5432, 3306, 1433, 1521]
    timeout = float(payload.timeout or 3)
    candidates: list[str] = []
    version_str: str | None = None
    detected: str | None = None
    for p in ports:
        try:
            with socket.create_connection((host, p), timeout=timeout) as s:
                # If MySQL (3306), try to read initial handshake which includes version string
                if p == 3306:
                    s.settimeout(timeout)
                    try:
                        data = s.recv(128)
                        if data:
                            try:
                                txt = data.decode(errors="ignore")
                            except Exception:
                                txt = str(data)
                            version_str = txt
                            if _parse_engine_from_version_string(txt) == "mysql" or "mysql" in txt.lower():
                                detected = "mysql"
                                break
                    except Exception:
                        pass
                # If we connected successfully but no handshake, infer by port mapping
                if p == 5432:
                    candidates.append("postgres")
                elif p == 3306 and detected is None:
                    candidates.append("mysql")
                elif p == 1433:
                    candidates.append("mssql")
                elif p == 1521:
                    candidates.append("oracle")
        except Exception:
            continue
    if detected:
        return DetectResponse(ok=True, detected=detected, method="handshake", versionString=version_str)
    if len(candidates) == 1:
        return DetectResponse(ok=True, detected=candidates[0], method="port_hint", candidates=candidates, versionString=version_str)
    if candidates:
        return DetectResponse(ok=True, detected=None, method="port_hint", candidates=candidates, versionString=version_str)
    return DetectResponse(ok=False, detected=None, method="port_hint", candidates=[], versionString=None, error="No known DB ports open or reachable")


@app.get("/api/branding", response_model=BrandingOut)
async def get_branding() -> BrandingOut:
    # Default theme
    base_fonts = {"primary": "Inter", "code": "ui-monospace"}
    base_palette = {
        "background": "210 0% 98%",
        "foreground": "222 47% 11%",
        "muted": "210 0% 96%",
        "muted-foreground": "215 16% 47%",
        "card": "0 0% 100%",
        "card-foreground": "222 47% 11%",
        "popover": "0 0% 100%",
        "popover-foreground": "222 47% 11%",
        "border": "214 32% 91%",
        "ring": "222 84% 58%",
        "secondary": "214 32% 94%",
        "secondary-foreground": "222 47% 11%",
        "accent": "210 40% 96%",
        "accent-foreground": "222 47% 11%",
        "destructive": "0 84% 60%",
        "destructive-foreground": "210 40% 98%",
        "topbar-bg": "0 0% 100%",
        "topbar-fg": "222 47% 11%",
        "surface-1": "0 0% 100%",
        "surface-2": "210 0% 98%",
        "surface-3": "210 0% 96%",
        "header-accent": "220 70% 50%",
    }
    # Load org overrides from JSON file next to metadata DB
    overrides = {}
    try:
        data_dir = Path(settings.metadata_db_path).resolve().parent
        f = data_dir / "branding.json"
        if f.exists():
            overrides = json.loads(f.read_text(encoding="utf-8")) or {}
    except Exception:
        overrides = {}
    return BrandingOut(
        fonts=base_fonts,
        palette=base_palette,
        orgName=overrides.get("orgName"),
        logoLight=overrides.get("logoLight"),
        logoDark=overrides.get("logoDark"),
        favicon=overrides.get("favicon"),
    )


# Root for convenience
@app.get("/")
async def root():
    return {"ok": True, "app": settings.app_name}


@app.get("/api/metrics")
async def metrics() -> Response:
    body = render_prometheus()
    return Response(content=body, media_type="text/plain; version=0.0.4; charset=utf-8")


@app.on_event("shutdown")
async def _shutdown():
    try:
        close_duck_shared()
    except Exception:
        pass
    # Ensure BackgroundScheduler threads are terminated to avoid GIL crash on Windows
    try:
        shutdown_scheduler(wait=True)
    except Exception:
        pass
