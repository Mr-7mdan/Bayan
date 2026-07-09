# Docker deployment (Bayan)

Containerized bring-up. **Additive** — the existing systemd + nginx bare-metal
path (`backend/run_prod_gunicorn.sh`, `backend/deploy/reporting-api*.service`,
`backend/deploy/nginx-reporting-api.conf`) is unchanged and still supported.

## Layout

- `backend/Dockerfile` — `python:3.12-slim`, unixODBC + msodbcsql18 (pyodbc),
  Playwright chromium (snapshots/alerts), gunicorn+uvicorn, non-root `bayan`.
- `frontend/Dockerfile` — multi-stage `node:20-alpine`, Next.js **standalone**
  server, non-root `node`.
- `docker-compose.yml` — `backend` + `frontend` services, one named volume,
  `/api/healthz` healthcheck.

## Required source change (NOT made here)

`docker compose build` for the frontend will **fail** until this one-line change
lands in `frontend/next.config.js` — the frontend Dockerfile copies
`.next/standalone`, which Next only emits when standalone output is enabled:

```js
const nextConfig = {
  reactStrictMode: true,
  output: 'standalone',   // <-- add this
  ...
}
```

It is harmless to the bare-metal `npm run build && npm run start` flow.

## First run

```bash
# backend/.env must exist with SECRET_KEY set (already used by the bare-metal
# path). It is mounted via env_file — never baked into the image.
docker compose up -d --build
docker compose ps                       # backend should be "healthy"
curl -sf http://localhost/api/healthz   # {"status":"ok",...} via Next /api proxy
curl -sI http://localhost/ | head -1    # 200 — Next app
```

Log in at http://localhost, create a datasource, run a widget, trigger an alert
snapshot to confirm chromium works in-container.

### env note

`backend/.env` is reused as-is. `docker-compose.yml` **overrides** the
container-critical keys in its `environment:` block (`HOST=0.0.0.0`, `PORT=8000`,
`DUCKDB_PATH`, `METADATA_DB_PATH`, `PROMETHEUS_MULTIPROC_DIR`, `WEB_CONCURRENCY`,
`RUN_SCHEDULER`) so a bare-metal-tuned `.env` (e.g. `HOST=127.0.0.1`,
`DUCKDB_PATH=/var/lib/...`) cannot misroute paths or binding. Everything else
(SECRET_KEY, CORS_ORIGINS, SCHEDULER_TIMEZONE, DSNs) flows through from `.env`.

## Persistence

Named volume `bayan-data` mounted at `/data` holds **all** durable state:
`meta.sqlite`, `local.duckdb`, `duckdb.active`, `duckdb_tmp/`, and
`prom_multiproc/`. Survives `docker compose down && docker compose up -d`.

Migrate an existing bare-metal `.data/` into the volume (containers created,
not yet running heavy load):

```bash
docker compose create backend
docker compose cp backend/.data/. backend:/data/
docker compose up -d
```

## Networking / TLS

The Next.js standalone server proxies `/api/*` to the backend via the
`next.config.js` rewrite, so `frontend` is the single public entrypoint (host
`:80`). Browser calls stay same-origin `/api`; no CORS change needed, but set
`CORS_ORIGINS` in `.env` to the public origin anyway. For TLS, front the stack
with an external nginx/Traefik terminating HTTPS → `frontend:3000` (or reuse
`backend/deploy/nginx-reporting-api.conf` in a separate nginx container pointed
at `backend:8000` / `frontend:3000`).

## Scaling caveat (single writer)

DuckDB and SQLite are single-writer, so compose runs **one** backend. To add web
workers: set `WEB_CONCURRENCY>1` and `RUN_SCHEDULER=0` on `backend`, then add a
second backend service with `WEB_CONCURRENCY=1 RUN_SCHEDULER=1` as the dedicated
scheduler (same volume). Do not run the scheduler in every worker — duplicate
jobs.

## Out of scope

Registry/CI push, TLS automation, Kubernetes, horizontal backend scaling,
Windows/waitress, image-based self-update.
