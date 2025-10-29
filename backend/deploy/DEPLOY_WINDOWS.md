# Deploying the Bayan API on Windows (Service + Reverse Proxy)

This guide shows two supported Windows setups and how to run them as a Windows Service using NSSM, plus IIS reverse proxy.

- Option A: Waitress (WSGI) — recommended on Windows, simple and robust.
- Option B: Uvicorn (ASGI) — for async/WebSockets; use 1 worker by default.

Both options load environment from `backend/.env` via Pydantic settings.

## 1) Prerequisites

- Windows Server 2019/2022 (or Windows 10/11 for tests)
- Python 3.10+
- Git (optional)

Create the virtual environment and install dependencies:

```powershell
cd C:\Path\To\Project
python -m venv backend\venv
backend\venv\Scripts\pip install -r backend\requirements.txt
```

## 2) Configure environment

Create `backend\.env`:

```
APP_ENV=prod
CORS_ORIGINS=https://app.example.com
SECRET_KEY=change-me

DUCKDB_PATH=C:\Bayan\data\local.duckdb
METADATA_DB_PATH=C:\Bayan\data\meta.sqlite

FRONTEND_BASE_URL=https://app.example.com
BACKEND_BASE_URL=https://api.example.com/api

RUN_SCHEDULER=1
HOST=127.0.0.1
PORT=8000

AI_CONCURRENCY=2
AI_TIMEOUT_SECONDS=30
```

Create data directory:

```powershell
New-Item -ItemType Directory -Force C:\Bayan\data | Out-Null
```

Avoid spaces in deployment path; e.g., `C:\Bayan\backend`.

## 3) Option A — Waitress (Recommended)

Launchers are included in the repo:
- `backend\run_prod_waitress_windows.bat`

### Manual run

```bat
cd C:\Path\To\Project\backend
run_prod_waitress_windows.bat
```

Optional overrides before running:

```bat
set HOST=127.0.0.1
set PORT=8000
set THREADS=8
set RUN_SCHEDULER=1
run_prod_waitress_windows.bat
```

### Windows Service with NSSM

1) Install NSSM from https://nssm.cc/download and ensure `nssm.exe` is in PATH.

2) Create the service (CLI example):

```powershell
nssm install ReportingAPIWaitress "C:\Windows\System32\cmd.exe" /c "C:\Bayan\backend\run_prod_waitress_windows.bat"
nssm set ReportingAPIWaitress AppDirectory C:\Bayan\backend
# Optional logging
nssm set ReportingAPIWaitress AppStdout C:\Bayan\logs\reporting-api.out.log
nssm set ReportingAPIWaitress AppStderr C:\Bayan\logs\reporting-api.err.log
nssm start ReportingAPIWaitress
```

3) Alternatively use the NSSM GUI to set the same fields. If not using `.env`, set environment keys on the Environment tab.

## 4) Option B — Uvicorn (ASGI)

Launchers are included in the repo:
- `backend\run_prod_uvicorn_windows.bat`

### Manual run

```bat
cd C:\Path\To\Project\backend
set WORKERS=1
set RUN_SCHEDULER=1
run_prod_uvicorn_windows.bat
```

If you set `WORKERS>1`, disable the in-app scheduler in that process:

```bat
set WORKERS=4
set RUN_SCHEDULER=0
run_prod_uvicorn_windows.bat
```

### Windows Service with NSSM

```powershell
nssm install ReportingAPIUvicorn "C:\Windows\System32\cmd.exe" /c "C:\Bayan\backend\run_prod_uvicorn_windows.bat"
nssm set ReportingAPIUvicorn AppDirectory C:\Bayan\backend
# Optionally set env in NSSM (Environment tab): WORKERS, RUN_SCHEDULER, HOST, PORT, AI_CONCURRENCY, AI_TIMEOUT_SECONDS
nssm start ReportingAPIUvicorn
```

## 5) Reverse proxy with IIS + ARR

1) Install IIS, URL Rewrite, and Application Request Routing (ARR).

2) Create a site (e.g., `api.example.com`) and add a Reverse Proxy rule to `http://127.0.0.1:8000`.

3) Ensure forwarded headers are passed. The app already respects `X-Forwarded-*`.

4) Set proxy/read timeouts to accommodate long-running requests.

5) Terminate TLS in IIS.

## 6) Verification and monitoring

- Health check:

```powershell
Invoke-WebRequest http://127.0.0.1:8000/api/healthz -UseBasicParsing
```

- Metrics (Prometheus text):

```
http://127.0.0.1:8000/api/metrics
```

- Logs: if configured in NSSM (`AppStdout`/`AppStderr`), view the files under `C:\Bayan\logs`. Otherwise, check Windows Event Viewer.

## 7) Troubleshooting

- Port in use: change `PORT` in `.env` and in IIS reverse proxy.
- `.env` not applied: ensure service `AppDirectory` is `backend\` so `.env` is discovered.
- Scheduler duplication: only one process should run with `RUN_SCHEDULER=1`.
- Permissions: ensure the service account can read the repo and write to `C:\Bayan\data`.
- Missing packages: reinstall with `backend\venv\Scripts\pip install -r backend\requirements.txt`.

## 8) Quick reference (service control)

```powershell
nssm start  ReportingAPIWaitress
nssm stop   ReportingAPIWaitress
nssm edit   ReportingAPIWaitress
nssm start  ReportingAPIUvicorn
nssm stop   ReportingAPIUvicorn
nssm edit   ReportingAPIUvicorn
```
