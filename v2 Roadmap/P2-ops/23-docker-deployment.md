---
id: 23-docker-deployment
title: Containerized deployment path
priority: P2
effort: M
depends_on: []
area: ops
---
## Problem
No Docker/compose path exists. Deployment is bespoke: shell scripts + systemd on Linux (`backend/run_prod_gunicorn.sh`, `backend/deploy/reporting-api*.service`), `.bat` scripts on Windows, and manual nginx config. New environments (staging, demos, customer trials) require hand-running installers. A containerized path gives one-command bring-up while the existing systemd path stays for bare-metal installs.

## Current State
- **No** `Dockerfile`, `docker-compose.yml`, `.dockerignore`, or `Procfile` anywhere in the repo.
- Backend prod entry: `backend/run_prod_gunicorn.sh:81-92` — gunicorn `app.main:app` with `--worker-class uvicorn.workers.UvicornWorker`, binds `${HOST:-0.0.0.0}:${PORT:-8000}`, workers `2*CPU+1` (or `WEB_CONCURRENCY`), `--timeout 180 --keep-alive 75 --max-requests 1000`. Also `run_prod_waitress.sh` + `wsgi.py` (AsgiToWsgi wrapper) and Windows `.bat` variants.
- Scheduler gate: `backend/app/main.py:157` — `RUN_SCHEDULER` env (default "1"). The gunicorn script exports `RUN_SCHEDULER=0` by default (`run_prod_gunicorn.sh:33`) because multi-worker + scheduler = duplicate jobs.
- Health endpoint: `backend/app/main.py:195` — `@app.get("/api/healthz")`.
- Data paths (the persistent state):
  - `backend/app/config.py:27` — `duckdb_path: str = Field(default=".data/local.duckdb")` (env `DUCKDB_PATH`)
  - `backend/app/config.py:30` — `metadata_db_path: str = Field(default=".data/meta.sqlite")` (env `METADATA_DB_PATH`)
  - `backend/app/db.py:169,174` — the parent dir of `metadata_db_path` is the app data dir; it also holds the `duckdb.active` pointer file and `duckdb_tmp/`. **Both stores must live in the same mounted dir.**
  - Note: `backend/bayan.db` exists on disk but is NOT referenced by any code — ignore it.
- Env reference: `backend/deploy/reporting-api.env.example` — keys `APP_ENV, CORS_ORIGINS, SECRET_KEY, DUCKDB_PATH, METADATA_DB_PATH, FRONTEND_BASE_URL, BACKEND_BASE_URL, SNAPSHOT_ACTOR_ID, RUN_SCHEDULER, HOST, PORT, SCHEDULER_TIMEZONE`, optional `POSTGRES_DSN` etc. (SECRET_KEY value present in `backend/.env`; never bake into image.)
- Native deps required by `backend/requirements.txt`: `pyodbc` (needs unixODBC + `msodbcsql18` — install logic already exists in `scripts/install_mssql_odbc_linux.sh`), `playwright>=1.55.0` (chromium needed at runtime by `backend/app/alerts_service.py` and `backend/app/routers/snapshot.py` for dashboard snapshots).
- Frontend: Next.js 15.5.3 / React 18 (`frontend/package.json`), `npm run build` = `next build --no-lint`. `frontend/next.config.js:28-37` proxies `/api/:path*` to `NEXT_PUBLIC_API_BASE_URL` via rewrites. **No `output: 'standalone'`** — must be added for a slim runtime image.
- Reverse proxy template: `backend/deploy/nginx-reporting-api.conf` — routes `/api/` → :8000, everything else → :3000, healthcheck block for `/api/healthz`. Reusable nearly verbatim inside a nginx container.

## Desired State
`docker compose up -d` from repo root brings up backend (gunicorn+uvicorn), frontend (Next.js standalone), and nginx on port 80, with all SQLite/DuckDB state in a named volume, healthchecks on `/api/healthz`, and env via `.env` file. Systemd/bare-metal path untouched and documented as still supported.

## Implementation Plan

1. **Enable Next.js standalone output** — `frontend/next.config.js`: add `output: 'standalone',` to `nextConfig` (next to `reactStrictMode`). Harmless for the existing `npm run start` bare-metal flow.

2. **Create `backend/Dockerfile`** (multi-stage not needed; single stage, `python:3.12-slim`):
   ```dockerfile
   FROM python:3.12-slim
   ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
   # unixODBC + msodbcsql18 for pyodbc (mirrors scripts/install_mssql_odbc_linux.sh, Debian 12 path)
   RUN apt-get update && apt-get install -y --no-install-recommends curl gnupg2 ca-certificates unixodbc \
     && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
     && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
     && apt-get update && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
     && rm -rf /var/lib/apt/lists/*
   WORKDIR /app
   COPY requirements.txt .
   RUN pip install -r requirements.txt
   # Chromium for snapshot service (alerts_service.py, routers/snapshot.py)
   ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
   RUN playwright install --with-deps chromium
   COPY . .
   ENV DUCKDB_PATH=/data/local.duckdb METADATA_DB_PATH=/data/meta.sqlite \
       HOST=0.0.0.0 PORT=8000 RUN_SCHEDULER=1 WEB_CONCURRENCY=1
   VOLUME /data
   EXPOSE 8000
   CMD gunicorn app.main:app --worker-class uvicorn.workers.UvicornWorker \
       --bind ${HOST}:${PORT} --workers ${WEB_CONCURRENCY} \
       --keep-alive 75 --timeout 180 --graceful-timeout 30 \
       --max-requests 1000 --max-requests-jitter 100 \
       --access-logfile /dev/null --error-logfile - --log-level warning
   ```
   Default `WEB_CONCURRENCY=1` + `RUN_SCHEDULER=1` matches the single-scheduler constraint from `run_prod_gunicorn.sh:31-33`. Document in compose comments: if raising workers >1, set `RUN_SCHEDULER=0` and run a second backend container with `WEB_CONCURRENCY=1 RUN_SCHEDULER=1` as the dedicated scheduler.

3. **Create `backend/.dockerignore`**: `.data/`, `.env`, `venv/`, `__pycache__/`, `*.pyc`, `bayan.db`, `*.duckdb`, `logs/`, `.pytest_cache/`.

4. **Create `frontend/Dockerfile`** (multi-stage, `node:20-alpine`):
   ```dockerfile
   FROM node:20-alpine AS build
   WORKDIR /app
   COPY package.json package-lock.json ./
   RUN npm ci
   COPY . .
   # NEXT_PUBLIC_ vars are inlined at build time; rewrite dest is read at runtime too
   ARG NEXT_PUBLIC_API_BASE_URL=http://backend:8000/api
   ENV NEXT_PUBLIC_API_BASE_URL=$NEXT_PUBLIC_API_BASE_URL
   RUN npm run build

   FROM node:20-alpine
   WORKDIR /app
   ENV NODE_ENV=production HOSTNAME=0.0.0.0 PORT=3000
   COPY --from=build /app/.next/standalone ./
   COPY --from=build /app/.next/static ./.next/static
   COPY --from=build /app/public ./public
   EXPOSE 3000
   CMD ["node", "server.js"]
   ```
   Important: because `NEXT_PUBLIC_API_BASE_URL` feeds the rewrite in `next.config.js:29`, browser calls go to same-origin `/api/*` and Next proxies them server-side to `http://backend:8000/api` inside the compose network — no CORS changes needed beyond defaults, but set `CORS_ORIGINS` on backend to the public origin anyway.

5. **Create `frontend/.dockerignore`**: `node_modules/`, `.next/`, `.env.local`, `tsconfig.tsbuildinfo`.

6. **Create `deploy/docker/nginx.conf`** — copy `backend/deploy/nginx-reporting-api.conf` and change only the upstream servers: `server backend:8000;` and `server frontend:3000;`, `server_name _;`.

7. **Create `docker-compose.yml`** at repo root:
   ```yaml
   services:
     backend:
       build: ./backend
       env_file: .env.docker          # SECRET_KEY, APP_ENV, CORS_ORIGINS, SCHEDULER_TIMEZONE, ADMIN_* ...
       environment:
         FRONTEND_BASE_URL: http://frontend:3000     # snapshot service target
         BACKEND_BASE_URL: http://backend:8000/api
       volumes:
         - bayan-data:/data           # meta.sqlite + local.duckdb + duckdb.active + duckdb_tmp
       healthcheck:
         test: ["CMD", "python3", "-c", "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/api/healthz')"]
         interval: 30s
         timeout: 5s
         retries: 3
         start_period: 20s
       restart: unless-stopped
     frontend:
       build: ./frontend
       depends_on:
         backend:
           condition: service_healthy
       restart: unless-stopped
     nginx:
       image: nginx:1.27-alpine
       ports: ["80:80"]
       volumes:
         - ./deploy/docker/nginx.conf:/etc/nginx/conf.d/default.conf:ro
       depends_on: [backend, frontend]
       healthcheck:
         test: ["CMD", "wget", "-qO-", "http://127.0.0.1/api/healthz"]
         interval: 30s
         timeout: 5s
         retries: 3
       restart: unless-stopped
   volumes:
     bayan-data:
   ```

8. **Create `.env.docker.example`** at repo root — same keys as `backend/deploy/reporting-api.env.example` minus HOST/PORT/waitress keys, all placeholder values. Add `.env.docker` to root `.gitignore`.

9. **Document** — append a "Docker" section to `backend/deploy/DEPLOY.md`: `docker compose up -d --build`, where the volume lives, how to migrate an existing bare-metal `.data/` dir into the volume (`docker compose cp backend/.data/. backend:/data/`), the multi-worker + scheduler caveat, and an explicit note that the systemd + nginx bare-metal path (`reporting-api*.service`, `run_prod_gunicorn.sh`) remains supported and unchanged.

## Files to Modify
- `frontend/next.config.js` — add `output: 'standalone'`
- `backend/Dockerfile` — new
- `backend/.dockerignore` — new
- `frontend/Dockerfile` — new
- `frontend/.dockerignore` — new
- `deploy/docker/nginx.conf` — new (derived from `backend/deploy/nginx-reporting-api.conf`)
- `docker-compose.yml` — new, repo root
- `.env.docker.example` — new, repo root
- `.gitignore` — add `.env.docker`
- `backend/deploy/DEPLOY.md` — add Docker section

## Acceptance Criteria
- [ ] `docker compose up -d --build` succeeds from a clean checkout with only `.env.docker` (copied from example, SECRET_KEY set) present
- [ ] `curl http://localhost/api/healthz` returns `{"status":"ok",...}` via nginx
- [ ] `curl http://localhost/` serves the Next.js app; login page loads and API calls succeed (same-origin `/api`)
- [ ] `docker compose down && docker compose up -d` preserves `meta.sqlite` and `local.duckdb` (named volume survives)
- [ ] Backend container is `healthy` in `docker compose ps`
- [ ] Dashboard snapshot/alert render works inside the container (Playwright chromium present)
- [ ] `bare-metal` flow untouched: `backend/run_prod_gunicorn.sh` and systemd units unmodified; `npm run build && npm run start` still works with `output: 'standalone'` set
- [ ] No secret values committed (`.env.docker` gitignored; images contain no `.env`)

## Verification
```bash
cp .env.docker.example .env.docker   # set SECRET_KEY, ADMIN_EMAIL/PASSWORD
docker compose up -d --build
docker compose ps                     # all healthy
curl -sf http://localhost/api/healthz
curl -sI http://localhost/ | head -1  # 200
docker compose exec backend ls /data  # meta.sqlite, local.duckdb appear after first boot
docker compose restart backend && curl -sf http://localhost/api/healthz
docker compose down && docker compose up -d && docker compose exec backend ls /data  # state persisted
# bare-metal regression:
cd frontend && npm run build && npm run start  # still serves on :3000
```
Manual: log in via browser at http://localhost, create a datasource, run a widget query, trigger an alert snapshot.

## Out of Scope
- CI image publishing / registry push (`scripts/build_and_push.sh` stays as-is)
- TLS termination (add certbot/Traefik later; nginx template already notes TLS goes in a separate block)
- Kubernetes/Helm
- Multi-backend horizontal scaling (SQLite/DuckDB are single-writer; compose runs one backend)
- Replacing Windows `.bat` / waitress deployment
- Migrating the existing self-update mechanism (`app/routers/updates.py`) to image-based updates
