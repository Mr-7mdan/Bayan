# Deploying the Reporting API (FastAPI) behind Nginx + systemd

This guide provides two production setups:

- Option A: Waitress (WSGI) — simple, thread-based server (no WebSockets/HTTP2).
- Option B: Gunicorn + Uvicorn (ASGI) — true async workers with uvloop, supports WebSockets.

Pick one option. Both are fronted by Nginx and managed by systemd.

## 1) Repository layout

- Backend root: `backend/`
- App entry: `backend/app/main.py` exposes `app` (FastAPI)
- WSGI adapter: `backend/wsgi.py` exposes `application` for Waitress
- Prod scripts: `backend/run_prod_waitress.sh`, `backend/run_prod_gunicorn.sh`
- Deploy templates: `backend/deploy/`

## 2) Install and verify

```bash
# From repo root
python3 -m venv backend/venv
./backend/venv/bin/pip install -r backend/requirements.txt
# Optional smoke test
RUN_SCHEDULER=1 HOST=127.0.0.1 PORT=8000 ./backend/run_prod_waitress.sh
# Visit http://127.0.0.1:8000/api/healthz
```

## 3) Configure environment

- App settings are in `backend/app/config.py` (Pydantic v2). Key env vars:
  - `APP_ENV` (prod|dev)
  - `CORS_ORIGINS` (comma-separated)
  - `SECRET_KEY`
  - `DUCKDB_PATH`, `METADATA_DB_PATH`
  - `FRONTEND_BASE_URL`, `SNAPSHOT_ACTOR_ID`, `BACKEND_BASE_URL`
  - Optional DSNs: `POSTGRES_DSN`, `SUPABASE_POSTGRES_DSN`
- Server env knobs are read by the respective run scripts:
  - Shared:
    - `RUN_SCHEDULER` (only 1 instance should set this to 1; set 0 on all others)
    - `HOST`, `PORT`
  - Waitress (`run_prod_waitress.sh`):
    - `THREADS`, `CONN_LIMIT`, `BACKLOG`, `CHANNEL_TIMEOUT`, `IDENT`
  - Gunicorn+Uvicorn (`run_prod_gunicorn.sh`):
    - `WEB_CONCURRENCY` (workers; defaults to 2*CPU+1)
    - `KEEP_ALIVE`, `TIMEOUT`, `GRACEFUL_TIMEOUT`, `MAX_REQUESTS`, `MAX_REQUESTS_JITTER`, `LOG_LEVEL`
  - AI features:
    - `AI_CONCURRENCY` (default 2) caps concurrent AI calls per process
    - `AI_TIMEOUT_SECONDS` (default 30) HTTP timeout for AI provider calls

Use the example env file:

- `backend/deploy/reporting-api.env.example` → copy to a secure path, edit values
- If using systemd, point `EnvironmentFile=` to it.

## 4) systemd unit (Choose one)

1. Waitress (WSGI):
   - `backend/deploy/reporting-api.service` → `/etc/systemd/system/reporting-api.service`
   - Set `WorkingDirectory` and `ExecStart` to your absolute `backend/` path
   - Optionally set `EnvironmentFile=/etc/reporting/reporting-api.env`
   - Ensure the service user (`User=/Group=`) has read/execute access

2. Gunicorn + Uvicorn (ASGI):
   - `backend/deploy/reporting-api-gunicorn.service` → `/etc/systemd/system/reporting-api.service`
   - Set `WorkingDirectory` and `ExecStart` to your absolute `backend/` path
   - Ensure `RUN_SCHEDULER=0` here if you run a separate scheduler leader

3. Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable reporting-api
sudo systemctl start reporting-api
sudo systemctl status reporting-api -n 100
```

3. Logs:

```bash
journalctl -u reporting-api -f
```

## 5) Nginx reverse proxy

1. Place site config:
   - `backend/deploy/nginx-reporting-api.conf` → `/etc/nginx/sites-available/reporting-api.conf`
   - Edit `server_name` and backend address/port if needed

2. Enable and reload:

```bash
sudo ln -sf /etc/nginx/sites-available/reporting-api.conf /etc/nginx/sites-enabled/reporting-api.conf
sudo nginx -t
sudo systemctl reload nginx
```

3. TLS (recommended):

```bash
# Example with certbot (adjust domain)
sudo certbot --nginx -d api.example.com
```

The app automatically trusts `X-Forwarded-*` via `ProxyHeadersMiddleware` so HTTPS scheme is preserved.

## 6) Concurrency and tuning

- Waitress threads: `THREADS` per instance. Start with 8–16.
- Gunicorn+Uvicorn workers: `WEB_CONCURRENCY`. Start with `2*CPU+1` or 4–8.
- SQLAlchemy pools (per datasource DSN):
  - Add DSN params to scale pool size, e.g. `...?sa_pool_size=10&sa_max_overflow=30&sa_pool_timeout=30`
- Heavy query limiter:
  - `HEAVY_QUERY_CONCURRENCY` limits expensive pivot paths (default 8)
- AI calls:
  - `AI_CONCURRENCY` per-process semaphore (default 2)
  - `AI_TIMEOUT_SECONDS` total HTTP timeout for AI requests
- Scheduler leader:
  - Exactly one instance with `RUN_SCHEDULER=1` to avoid duplicate jobs
- Timeouts:
  - Nginx: `proxy_read_timeout`/`send_timeout` in site config
  - DB statement timeouts per dialect are applied in the app (PG/MySQL/MSSQL)

## 7) File ownership and paths

- Store DB files under a directory owned by the service user:
  - `DUCKDB_PATH=/var/lib/reporting/local.duckdb`
  - `METADATA_DB_PATH=/var/lib/reporting/meta.sqlite`
- Ensure the directory exists and is writable by the service user.

## 8) Health checks

- Use `GET /api/healthz` for LB health.
- Consider adding `/api/admin/stats` later for deeper monitoring.
 - Metrics: `GET /api/metrics` exposes Prometheus text. Includes `ai_requests_total` and `ai_request_duration_ms` labeled by provider and endpoint.

## 10) AI providers

- Supported providers in `backend/app/routers/ai.py`:
  - `gemini` (Google Generative Language API)
  - `openai` (Chat Completions)
  - `mistral` (Chat Completions)
- Provide the API key in the request body or store it via `PUT /api/ai/config`.

## 9) Rolling out multiple instances

- Behind Nginx or a Load Balancer, run N replicas.
- Set `RUN_SCHEDULER=0` on all but one.
- Size DB pools and Waitress threads accordingly.

## 11) Notes on ASGI vs WSGI

- Waitress (WSGI) is thread-based and does not support WebSockets/HTTP2.
- Gunicorn + Uvicorn (ASGI) provides true async workers (uvloop) and WebSockets support.
- The app’s endpoints remain compatible with both; choose ASGI if you rely on async and WebSockets, or need better concurrency under mixed workloads.

## 10) Known limitations with Waitress

- No WebSockets / HTTP2. For those, use an ASGI server.

---

If you need a ready-to-paste `systemd` or Nginx config with your actual paths and domains, provide them and we’ll tailor these templates.
