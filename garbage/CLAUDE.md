# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Backend (from `backend/`)
```bash
# Setup
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt

# Dev server (port 8000, auto-reload)
./run_dev.sh

# Or directly:
./venv/bin/python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000

# Production
./run_prod_gunicorn.sh    # Linux/macOS
./run_prod_waitress.sh    # Alternative
```

### Frontend (from `frontend/`)
```bash
npm install
npm run dev       # Next.js dev server
npm run build     # Production build (--no-lint)
npm run lint      # ESLint
npm run clean     # Clear .next cache
```

### Build Artifacts (from repo root)
```bash
bash scripts/build_frontend_artifact.sh
bash scripts/build_backend_artifact.sh
```

### No test suite exists yet. No pytest/jest configuration is present.

## Architecture

Bayan is a self-hosted analytics platform: FastAPI backend + Next.js 15 App Router frontend.

### Dual Database System
- **SQLite** (`backend/.data/meta.sqlite`) — metadata store: users, datasources, dashboards, sync tasks, alert rules, configs. Managed via SQLAlchemy ORM (`backend/app/models.py`).
- **DuckDB** (`backend/.data/local.duckdb`) — analytical data warehouse. External datasources (Postgres, MySQL, MSSQL) are synced into DuckDB tables. Has a thread-safe read pool and shared write connection (`backend/app/db.py`).

### SQL Generation (Dual-Mode)
- **Legacy builder** (`backend/app/sqlgen.py`) — hand-crafted multi-dialect SQL string builder. Production default.
- **SQLGlot builder** (`backend/app/sqlgen_glot.py`) — newer AST-based builder using SQLGlot library. Gated behind `ENABLE_SQLGLOT` flag and `SQLGLOT_USERS` per-user targeting.
- Both run in parallel when SQLGlot is enabled; legacy fallback via `ENABLE_LEGACY_FALLBACK`.

### Query Execution Pipeline (`backend/app/routers/query.py`)
Request flow: auth check → permission check → datasource resolution → SQL generation → execution with timeout → optional caching (Redis or in-memory) → response. Includes per-actor concurrency semaphores and token-bucket rate limiting.

### Data Sync Pipeline
External databases sync into DuckDB via two modes:
- **Sequence** (incremental) — tracks max timestamp, syncs new rows only
- **Snapshot** (full) — truncates and re-inserts all data
- **API ingest** (`backend/app/api_ingest.py`) — HTTP endpoint polling with CSV/JSON parsing

Managed by APScheduler (`backend/app/scheduler.py`) with cron expressions.

### Frontend Structure
- **App Router** pages under `frontend/src/app/(app)/` — builder, dashboards, datasources, alerts, admin
- **Widget system** (`frontend/src/types/widgets.ts`) — KPI, Chart, Table, Heatmap, Gantt, Composition, Report, Text, Spacer, Tracker
- **Widget renderers** in `frontend/src/components/widgets/` — each type has a `*Card.tsx` component
- **Dashboard builder** — uses `react-grid-layout` for drag-drop; `ConfiguratorPanel` and `ConfiguratorPanelV2` for widget configuration
- **API client** (`frontend/src/lib/api.ts`) — typed `Api` class, all backend communication
- **Data fetching** — TanStack React Query v5 (30s staleTime, 10min gcTime)
- **UI libraries** — MUI + Radix UI + Tremor; charts via ECharts and Plotly

### Frontend-Backend Communication
Next.js rewrites `/api/*` requests to the backend (`next.config.js`). Backend API base URL configured via `NEXT_PUBLIC_API_BASE_URL`.

### Authentication & Security
- Password hashing via HMAC-SHA256 (`backend/app/security.py`)
- Datasource credentials encrypted with Fernet
- Embed tokens: short-lived HMAC-SHA256 signed tokens for public widget embedding
- Dashboard/datasource sharing with per-user permission model (ro|rw)

## Key Patterns

- **Path alias**: Frontend uses `@/*` for `frontend/src/*` imports
- **Providers**: Auth, Query, Theme, Filters, Environment, Branding providers wrap the app (`frontend/src/components/providers/`)
- **Config**: Backend settings via Pydantic `Settings` class (`backend/app/config.py`), loaded from env vars and `.env` file
- **Routers**: 14 FastAPI routers under `backend/app/routers/`, all prefixed with `/api`
- **Background jobs**: APScheduler runs sync and alert jobs; scheduled on app startup from DB state
- **Middleware**: CORS, proxy headers, metrics tracking, global exception handler reporting to issues router

## Environment Variables (Backend)
See `backend/.env.example`. Key vars: `APP_ENV`, `SECRET_KEY`, `DUCKDB_PATH`, `CORS_ORIGINS`, `REDIS_URL`, `QUERY_RATE_PER_SEC`, `QUERY_BURST`, `USER_QUERY_CONCURRENCY`.

## Prerequisites
- Node.js 20+, npm
- Python 3.11+, venv
- Git, GitHub CLI (`gh`)
- Optional: ODBC drivers for MSSQL datasources
