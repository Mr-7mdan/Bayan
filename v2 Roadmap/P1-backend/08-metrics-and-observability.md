---
id: 08-metrics-and-observability
title: Durable, multi-worker-safe metrics and health
priority: P1
effort: M
depends_on: []
area: backend
---

## Problem

Metrics are a hand-rolled, in-process registry rendered to Prometheus text by string
concatenation. Every gunicorn worker (prod runs `2*CPU+1` uvicorn workers via
`backend/run_prod_gunicorn.sh`) holds its own dicts, so `GET /api/metrics` returns only the
metrics of whichever worker happened to answer the scrape; values reset on every worker
recycle (`--max-requests 1000` guarantees frequent recycling). `GET /api/healthz` is a static
"ok" that checks nothing — the process can be up while DuckDB, SQLite, or the scheduler are
dead. There is no sync success/failure metric, no pool-saturation gauge, and no scrape
documentation.

A sibling has the same per-worker-dict flaw: `backend/app/metrics_state.py` (recent actors,
open dashboards) — noted, but out of scope here (see Out of Scope).

## Current State

All refs verified 2026-07-07.

- `backend/app/metrics.py:10-13` — module-level dicts guarded by a `threading.Lock`:
  ```python
  _counters: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float] = {}
  _gauges: ...
  _summaries: ...  # (sum, count)
  ```
  Public facade: `counter_inc` (:21), `gauge_set` (:27), `gauge_inc` (:33), `gauge_dec` (:39),
  `summary_observe` (:43), `render_prometheus` (:57, manual `# TYPE` lines + trailing `# EOF`),
  `snapshot` (:78, returns `{counters, gauges, summaries}` lists of `{name, labels, value|sum,count}`).
- `backend/app/main.py:41` — imports the facade. `main.py:420-423` — `GET /api/metrics` returns
  `render_prometheus()` as `text/plain; version=0.0.4`.
- `backend/app/main.py:75-92` — HTTP middleware `_metrics_mw`: `gauge_inc/gauge_dec("app_active_requests", {"path": raw_path, "method"})`
  and `summary_observe("app_request_duration_ms", ms, {"path": raw_path, "method"})`. Raw path
  labels (`/api/dashboards/<uuid>` etc.) are a cardinality bomb.
- `backend/app/main.py:195-197` — `GET /api/healthz` returns
  `HealthResponse(status="ok", app=..., env=...)`; `HealthResponse` at `backend/app/schemas.py:9-12`
  has only `status/app/env`.
- `backend/app/main.py:162-173` — scheduler watchdog thread calls `ensure_scheduler_started()`
  every 5 min (self-heal). `backend/app/scheduler.py:19-35` — module global `_scheduler`,
  `ensure_scheduler_started()` restarts if `not _scheduler.running`. No side-effect-free
  "is it running" accessor exists.
- Call sites of the facade (all must keep working unchanged):
  - `backend/app/routers/query.py:35` import; counters/gauges/summaries at
    1780, 1786, 1822, 1832, 1841, 1983, 1988, 2062, 2067, 2084, 2090, 2107, 2114, 2123, 2217,
    2222, 2251, 2256, 2303, 2309, 4882, 4889, 8603, 9718, 9759, 9776, 9823.
  - `backend/app/routers/ai.py:310-486` — `ai_requests_total`, `ai_request_duration_ms`.
  - `backend/app/routers/datasources.py:790,799` — `sync_lock_busy_total`, `sync_lock_acquired_total`.
  - `backend/app/alerts_service.py:1597,1604,1838,1844` — `notifications_{email,sms}_{sent,failed}_total`.
- **Inconsistent label sets on the same metric name** (breaks `prometheus_client`, which fixes
  labelnames at metric creation):
  - `query_inflight`: `{endpoint, engine}` at query.py:1822/2090/2107/2309 vs `{endpoint}` only
    at query.py:8603/9718/9759/9776/9823.
  - `query_semaphore_wait_ms`: `{endpoint, engine}` at query.py:1832/2114 vs
    `{endpoint, engine, sem}` at query.py:1841/2123.
- `backend/app/routers/admin.py:13` — `from ..metrics import snapshot as metrics_snapshot`;
  `GET /admin/metrics-live` (admin.py:52-140) sums by name/labels from `snapshot()` — its
  output shape is a contract for the admin UI.
- `backend/app/routers/datasources.py:655` `run_sync_now(...)` — the sync executor. `SyncRun`
  row created at :899; failure path sets `run.error` at :1064; `finally` at :1069 clears
  `in_progress`. No success/failure counter emitted.
- `backend/app/query_pool.py:19` — `QUERY_POOL_SIZE` env (default 16), `get_query_executor()` (:25).
- `backend/app/models.py:180` — `SessionLocal` (SQLite metadata DB). `backend/app/db.py:471`
  `init_duck_shared`, `:525` `open_duck_native` (shared DuckDB native connection).
- `backend/requirements.txt` — no `prometheus-client`. Gunicorn launch:
  `backend/run_prod_gunicorn.sh` (no `--config`, no `PROMETHEUS_MULTIPROC_DIR`).
- `backend/dist/backend-4.5/` is packaged build output — never edit it.

## Desired State

- `backend/app/metrics.py` keeps the **same public facade** (`counter_inc`, `gauge_set`,
  `gauge_inc`, `gauge_dec`, `summary_observe`, `render_prometheus`, `snapshot`) but is backed by
  `prometheus_client`, with multiprocess aggregation when `PROMETHEUS_MULTIPROC_DIR` is set.
  Zero changes at the ~40 call sites; `admin.py` `/metrics-live` untouched.
- `GET /api/metrics` returns aggregated metrics across all gunicorn workers, durable across
  worker recycles (mmap files survive `--max-requests` restarts; cleaned only at master start).
- `GET /api/healthz` reports dependency checks (SQLite, DuckDB, scheduler) with `status: ok|degraded`
  and HTTP 503 when a critical dependency fails, while staying backward compatible (existing
  `status/app/env` fields unchanged).
- New core metrics: `sync_runs_total{status}`, `sync_duration_ms` summary, `query_pool_max` gauge,
  `scheduler_running` gauge. Existing request-latency/query-duration/cache-hit metrics preserved
  under the same names.
- Scraping documented in `backend/deploy/observability.md`.

## Implementation Plan

1. **Dependency**: append `prometheus-client>=0.20.0` to `backend/requirements.txt`.
   `pip install prometheus-client` into `backend/venv`.

2. **Rewrite `backend/app/metrics.py`** as a facade over `prometheus_client` (keep the module
   path and all seven public function signatures identical):
   - Imports: `from prometheus_client import Counter, Gauge, Summary, CollectorRegistry, generate_latest, REGISTRY` and `from prometheus_client import multiprocess`.
     Note: `prometheus_client` selects its value class (mmap vs in-memory) **at import time**
     based on `PROMETHEUS_MULTIPROC_DIR` — the env var must be exported before the app imports
     this module (handled in step 4; dev without the var falls back to single-process mode
     automatically, nothing to do).
   - Lazy metric registry: `_metrics: Dict[str, Any]` + `threading.Lock`. On first use of a
     name, create `Counter`/`Gauge`/`Summary` with `labelnames` from that call's label keys
     (sorted). `prometheus_client` strips/re-adds the `_total` suffix on counters, so existing
     names like `query_cache_hit_total` are preserved on the wire.
   - **Fixed labelnames table** for the two metrics with historically inconsistent label sets
     (values verified above):
     ```python
     _LABELNAMES = {
         "query_inflight": ("endpoint", "engine"),
         "query_semaphore_wait_ms": ("endpoint", "engine", "sem"),
     }
     ```
     In the facade, resolve labelnames as `_LABELNAMES.get(name)` or sorted first-call keys.
     On every call, pad missing labels with `""` and drop keys not in the metric's labelnames
     (log once per name at WARNING). This makes query.py:8603 etc. work unchanged and keeps
     admin.py's `sum_gauge("query_inflight", endpoint="period_totals")` matching (it only
     compares the keys it passes).
   - Gauges: create with `multiprocess_mode="livesum"` (correct for inflight/active-request
     style gauges: sums live workers, forgets dead ones). Pass the kwarg only when
     `PROMETHEUS_MULTIPROC_DIR` is in `os.environ`? No — `prometheus_client` accepts it always;
     pass unconditionally, it is ignored in single-process mode.
   - `render_prometheus() -> str`:
     ```python
     if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
         reg = CollectorRegistry()
         multiprocess.MultiProcessCollector(reg)
     else:
         reg = REGISTRY
     return generate_latest(reg).decode("utf-8")
     ```
     The old trailing `# EOF <ts>` line is dropped (it was non-standard); output is now valid
     exposition format including `# HELP` lines. `main.py:420-423` needs no change.
   - `snapshot() -> dict`: iterate `reg.collect()` (same registry selection as above) and map
     back to the exact legacy shape consumed by admin.py:
     - family type `counter` → samples named `<name>_total` → `counters: [{name: sample.name, labels, value}]`
       (keep the `_total`-suffixed sample name so admin.py's `sum_counter("query_cache_hit_total")` matches).
     - type `gauge` → `gauges: [{name, labels, value}]`.
     - type `summary` → pair `_sum`/`_count` samples by (base name, labels) →
       `summaries: [{name, labels, sum, count}]` with `name` = base name (matches
       admin.py `sum_summary("query_duration_ms")`).
     - Strip padding labels with value `""` from the returned label dicts so admin.py label
       equality (`engine` absent on period_totals) behaves as before.
   - Keep `gauge_set/gauge_inc/gauge_dec/counter_inc/summary_observe` semantics 1:1
     (`.labels(**padded).inc(amount)` / `.set(value)` / `.observe(value)`).
   - Delete the old dict implementation.

3. **Gunicorn multiprocess wiring** — new file `backend/gunicorn_conf.py`:
   ```python
   import os, glob
   _mp = os.environ.get("PROMETHEUS_MULTIPROC_DIR")

   def on_starting(server):  # master, before workers fork
       if _mp:
           os.makedirs(_mp, exist_ok=True)
           for f in glob.glob(os.path.join(_mp, "*.db")):
               os.unlink(f)

   def child_exit(server, worker):  # reap dead worker's mmap files
       if _mp:
           from prometheus_client import multiprocess
           multiprocess.mark_process_dead(worker.pid)
   ```

4. **Edit `backend/run_prod_gunicorn.sh`**:
   - After the `.env` sourcing block add:
     `export PROMETHEUS_MULTIPROC_DIR="${PROMETHEUS_MULTIPROC_DIR:-$SCRIPT_DIR/.prom_multiproc}"`
   - Add `--config "$SCRIPT_DIR/gunicorn_conf.py"` to `BASE_CMD`.
   - Dev (`run_dev.sh`) and Windows waitress scripts: leave untouched — single process, the
     facade falls back to the default registry.

5. **Fix path-label cardinality in `backend/app/main.py:75-92`** (`_metrics_mw`):
   - `app_active_requests`: label with `{"method": method}` only (raw path label removed —
     nothing consumes it; the templated route is unknown before routing, and mismatched
     inc/dec labels would leak).
   - `app_request_duration_ms`: in the `finally`, use the templated route:
     `route = request.scope.get("route"); path = getattr(route, "path", None) or path` so labels
     become e.g. `path="/api/dashboards/{dashboard_id}"`.

6. **New metrics** (all via the existing facade, no new imports beyond what each file already has):
   - `backend/app/routers/datasources.py` `run_sync_now` — in the `except` at :1062-1067 add
     `counter_inc("sync_runs_total", {"status": "error"})`; add a success-path counter
     `counter_inc("sync_runs_total", {"status": "success"})` immediately before the `except`'s
     try block ends (i.e., last line of the outer `try` that starts at :902), and in the
     `finally` at :1069 add `summary_observe("sync_duration_ms", elapsed_ms)` computed from a
     `time.perf_counter()` captured right before the `SyncRun` row is created (:899).
     `counter_inc` is already imported at datasources.py:52; add `summary_observe` to that import.
   - `backend/app/query_pool.py` `get_query_executor()` (:25) — on pool creation:
     `from .metrics import gauge_set; gauge_set("query_pool_max", float(QUERY_POOL_SIZE))`.
     (Saturation = `query_inflight` / `query_pool_max`; wait time already exists as
     `query_semaphore_wait_ms`.)
   - `backend/app/main.py` scheduler watchdog (:164-171) — inside the loop after
     `ensure_scheduler_started()` add `gauge_set("scheduler_running", 1.0)`, and in its
     `except` set `0.0`. Add `gauge_set` to the main.py:41 import.

7. **Enrich `GET /api/healthz`**:
   - `backend/app/schemas.py:9-12` — extend `HealthResponse` (backward compatible):
     ```python
     class HealthResponse(BaseModel):
         status: str = "ok"          # "ok" | "degraded"
         app: str
         env: str
         checks: Optional[Dict[str, str]] = None   # name -> "ok" | "disabled" | error string
     ```
   - `backend/app/scheduler.py` — add a side-effect-free accessor:
     ```python
     def scheduler_is_running() -> bool:
         return bool(_scheduler is not None and _scheduler.running)
     ```
   - `backend/app/main.py:195-197` — replace body:
     - `sqlite`: `from .models import SessionLocal`; open session, `execute(text("SELECT 1"))`, close.
     - `duckdb`: `with open_duck_native(None) as cur: cur.execute("SELECT 1")` (already imported
       at main.py:22; same pattern as `/api/test-connection` at :206-212).
     - `scheduler`: if `RUN_SCHEDULER` env is falsy (prod web workers run with `RUN_SCHEDULER=0`
       per run_prod_gunicorn.sh) report `"disabled"`; else `"ok"`/`"not running"` via
       `scheduler_is_running()`.
     - `status = "degraded"` if `sqlite` or `duckdb` check failed (scheduler is informational,
       never degrades). Return via `JSONResponse(status_code=503, content=HealthResponse(...).model_dump())`
       when degraded, plain `HealthResponse` (200) otherwise. Drop `response_model=` or keep it —
       either works; keep the decorator arg and return `JSONResponse` only on the 503 path.
     - Each check wrapped in try/except capturing `str(e)` (truncate to 200 chars). All three
       are sub-millisecond; no timeout machinery needed.

8. **Documentation** — new `backend/deploy/observability.md` (~40 lines):
   - Endpoint: `GET /api/metrics` (unauthenticated today — note that; auth is out of scope).
   - Env: `PROMETHEUS_MULTIPROC_DIR` (default `backend/.prom_multiproc` in prod script;
     unset = single-process mode for dev/waitress/Windows).
   - Sample scrape config:
     ```yaml
     scrape_configs:
       - job_name: bayan-backend
         metrics_path: /api/metrics
         static_configs: [{ targets: ["<host>:8000"] }]
     ```
   - Metric inventory table: `app_request_duration_ms{path,method}`, `app_active_requests{method}`,
     `query_duration_ms`, `query_inflight`, `query_semaphore_wait_ms`, `query_cache_{hit,miss}_total`,
     `query_rate_limited_total`, `query_pool_max`, `sync_runs_total{status}`, `sync_duration_ms`,
     `sync_lock_{busy,acquired}_total`, `sqlglot_{queries,errors}_total`, `ai_requests_total`,
     `ai_request_duration_ms`, `notifications_{email,sms}_{sent,failed}_total`, `scheduler_running`.
   - Health: `GET /api/healthz` — 200 ok / 503 degraded, `checks` map.
   - Add `.prom_multiproc/` to `backend/.gitignore` (or root `.gitignore` if backend has none).

9. **Test** — new `backend/tests/test_metrics.py` (stdlib pytest, no fixtures):
   - `counter_inc("t_c_total", {"a":"x"})` twice → `render_prometheus()` contains `t_c_total{a="x"} 2.0`.
   - `summary_observe("t_s_ms", 5)` → snapshot has `{name:"t_s_ms", sum:5.0, count:1}`.
   - Mixed labels on `query_inflight` (with and without `engine`) both succeed and `snapshot()`
     rows omit the padded empty label.
   - `TestClient(app).get("/api/healthz")` → 200, body has `checks` with `sqlite: "ok"`.

Order matters: 1 → 2 → (3,4 together) → 5-7 in any order → 8-9.

## Files to Modify

- `backend/requirements.txt` — add `prometheus-client>=0.20.0`.
- `backend/app/metrics.py` — replace dict registry with prometheus_client facade; same public API.
- `backend/gunicorn_conf.py` — NEW; multiproc dir cleanup + `mark_process_dead`.
- `backend/run_prod_gunicorn.sh` — export `PROMETHEUS_MULTIPROC_DIR`, add `--config gunicorn_conf.py`.
- `backend/app/main.py` — middleware label fixes (:75-92), healthz dependency checks (:195-197),
  `gauge_set` import (:41), scheduler_running gauge in watchdog (:164-171).
- `backend/app/schemas.py` — `HealthResponse.checks` optional field (:9-12).
- `backend/app/scheduler.py` — add `scheduler_is_running()`.
- `backend/app/routers/datasources.py` — `sync_runs_total` + `sync_duration_ms` in `run_sync_now`
  (:899, :1062-1070); extend import at :52.
- `backend/app/query_pool.py` — `query_pool_max` gauge in `get_query_executor()`.
- `backend/deploy/observability.md` — NEW; scrape + metric inventory doc.
- `backend/tests/test_metrics.py` — NEW.
- `backend/.gitignore` (or root) — ignore `.prom_multiproc/`.

Do NOT touch `backend/dist/**` (packaged copies) or `backend/app/metrics_state.py`.

## Acceptance Criteria

- [ ] `GET /api/metrics` under gunicorn with ≥4 workers returns one aggregated series per
      metric/label-set (no per-worker duplicates), and counters are monotonic across repeated
      scrapes that hit different workers.
- [ ] Counter values survive a worker recycle (kill one worker; totals do not drop).
- [ ] All existing metric names appear unchanged on the wire (`query_cache_hit_total`,
      `app_request_duration_ms_sum/_count`, etc.).
- [ ] `GET /admin/metrics-live` returns the same JSON shape as before (inflight/cache/durations
      populated) with zero changes to `backend/app/routers/admin.py`.
- [ ] `app_request_duration_ms` uses templated route paths (`/api/dashboards/{dashboard_id}`),
      not raw IDs.
- [ ] `sync_runs_total{status="success"|"error"}` and `sync_duration_ms` emitted by manual sync.
- [ ] `GET /api/healthz` returns 200 with `checks: {sqlite: ok, duckdb: ok, scheduler: ...}` when
      healthy; returns 503 with `status: "degraded"` when DuckDB or SQLite check fails.
- [ ] Dev mode (`run_dev.sh`, no `PROMETHEUS_MULTIPROC_DIR`) works in single-process fallback.
- [ ] `backend/tests/test_metrics.py` passes.
- [ ] No secret values quoted anywhere (`.env` contents only referenced by variable name).

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/backend
venv/bin/pip install prometheus-client
venv/bin/python -m pytest tests/test_metrics.py -q

# Multi-worker aggregation
WEB_CONCURRENCY=4 ./run_prod_gunicorn.sh &   # RUN_SCHEDULER defaults to 0 here
sleep 3
for i in 1 2 3 4 5; do curl -s localhost:8000/api/healthz > /dev/null; done
curl -s localhost:8000/api/metrics | grep app_request_duration_ms_count   # one line per label set
curl -s localhost:8000/api/metrics | sort | uniq -d | grep -v '^#' | wc -l  # expect 0 duplicate series

# Durability across worker death
PID=$(pgrep -f "gunicorn.*worker" | head -1); kill $PID; sleep 2
curl -s localhost:8000/api/metrics | grep app_request_duration_ms_count   # count did not reset

# Health
curl -si localhost:8000/api/healthz | head -1          # HTTP/1.1 200
curl -s  localhost:8000/api/healthz | python3 -m json.tool  # checks.sqlite == "ok"
# Degraded path: temporarily rename bayan.duckdb (or point DUCKDB path env wrong), restart, expect 503

# Sync metrics: trigger a sync from the UI or POST the run-now endpoint, then
curl -s localhost:8000/api/metrics | grep sync_runs_total

# Exposition validity (if promtool available)
curl -s localhost:8000/api/metrics | promtool check metrics
```

## Out of Scope

- `backend/app/metrics_state.py` (recent actors / open dashboards) — same per-worker flaw;
  needs a SQLite-backed store to be correct under gunicorn. Separate spec.
- Authentication on `/api/metrics` (it is public today; unchanged).
- Histograms/quantiles (Summary sum/count preserved for admin-UI compatibility;
  upgrade `query_duration_ms` to Histogram later if p95s are needed).
- Push-gateway / OTLP export, Grafana dashboards, alerting rules.
- Windows waitress/uvicorn scripts (single-process; fallback mode already covers them).
- Editing packaged copies under `backend/dist/**`.
