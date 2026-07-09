# Observability: metrics & health

## Metrics endpoint

`GET /api/metrics` — Prometheus exposition format (`text/plain; version=0.0.4`).
**Unauthenticated today** (public). Adding auth is out of scope for this spec.

Backed by `prometheus_client`. In production (gunicorn, multiple uvicorn
workers) metrics aggregate across all workers and survive worker recycles
(`--max-requests`) via mmap files.

## Environment

- `PROMETHEUS_MULTIPROC_DIR` — directory for per-process mmap `.db` files.
  - Set by `run_prod_gunicorn.sh` (default `backend/.prom_multiproc`).
  - Must be exported **before** the app imports `app.metrics` — `prometheus_client`
    selects its value class (mmap vs in-memory) at import time.
  - Unset (dev `run_dev.sh`, Windows waitress) = single-process mode, default registry.
    Nothing to configure.
- Master cleanup / dead-worker reaping is wired in `backend/gunicorn_conf.py`
  (`on_starting` wipes stale files; `child_exit` marks dead workers).

## Sample Prometheus scrape config

```yaml
scrape_configs:
  - job_name: bayan-backend
    metrics_path: /api/metrics
    static_configs: [{ targets: ["<host>:8000"] }]
```

## Metric inventory

| Metric | Type | Labels |
| --- | --- | --- |
| `app_request_duration_ms` | summary | `path` (templated route), `method` |
| `app_active_requests` | gauge | `method` |
| `query_duration_ms` | summary | — |
| `query_inflight` | gauge | `endpoint`, `engine` |
| `query_semaphore_wait_ms` | summary | `endpoint`, `engine`, `sem` |
| `query_cache_hit_total` / `query_cache_miss_total` | counter | — |
| `query_rate_limited_total` | counter | — |
| `query_pool_max` | gauge | — |
| `sync_runs_total` | counter | `status` (success\|error) |
| `sync_duration_ms` | summary | — |
| `sync_lock_busy_total` / `sync_lock_acquired_total` | counter | — |
| `sqlglot_queries_total` / `sqlglot_errors_total` | counter | — |
| `ai_requests_total` | counter | — |
| `ai_request_duration_ms` | summary | — |
| `notifications_{email,sms}_{sent,failed}_total` | counter | — |
| `scheduler_running` | gauge | — |

Saturation = `query_inflight` / `query_pool_max`.

## Health endpoint

`GET /api/healthz` — dependency-aware.

- `200` with `status: "ok"` when SQLite and DuckDB checks pass.
- `503` with `status: "degraded"` when SQLite or DuckDB fails.
- Body includes `checks`: `{sqlite, duckdb, scheduler}` where each value is
  `"ok"`, `"disabled"`, `"not running"`, or a truncated error string.
- `scheduler` is informational (never degrades health); reports `"disabled"`
  when `RUN_SCHEDULER` is falsy (prod web workers).
- Backward compatible: existing `status`/`app`/`env` fields unchanged.
