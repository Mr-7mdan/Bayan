---
id: 12-rate-limiting-defaults
title: Enable and tune query rate limiting
priority: P1
effort: M
depends_on: []
area: backend
---

## Problem

A token-bucket rate limiter for the heavy query endpoints exists but is **disabled by default** (`QUERY_RATE_PER_SEC` defaults to `"0"`), and even when enabled it has three defects:

1. **Trivially bypassable**: every check is gated by `if actorId:` — `actorId` is a client-supplied query parameter with default `None`. Omit it and you get unlimited queries.
2. **Double/triple charging**: `/query/period-totals/batch` and `/compare` throttle once at the top, then call `period_totals()` internally per item, which throttles again — a batch of N items consumes 2N+1 tokens and can 429 mid-batch, failing the whole request.
3. **No global limit**: only per-actor buckets exist; N actors can jointly flood the DuckDB/thread-pool backend.

Multi-worker deployments (`run_prod_gunicorn.sh` computes `WORKERS = CPU*2+1`) silently multiply the effective limit unless `REDIS_URL` is set, and this is nowhere documented.

## Current State

All rate-limit code lives in `backend/app/routers/query.py` (no rate config in `backend/app/config.py` — env is read directly via `os.environ`, consistent with `HEAVY_QUERY_CONCURRENCY` at query.py:78).

- **Config read, disabled by default** — query.py:592-599:
  ```python
  _Q_RATE = int(os.environ.get("QUERY_RATE_PER_SEC", "0") or "0")
  _Q_BURST = int(os.environ.get("QUERY_BURST", "0") or "0")
  ```
- **In-process bucket** — query.py:680-703 `_throttle_take(actor_id)`: token bucket keyed by actor string, uses module globals `_Q_RATE`/`_Q_BURST`, returns `None` (allowed) or wait-seconds. Returns `None` (no limiting) when `actor_id` is falsy or rate/burst ≤ 0.
- **Redis bucket** — query.py:603-678: `_RL_LUA` script + `_tb_redis_take(actor_id)`; used when `REDIS_URL` env is set; key `f"{REDIS_PREFIX}:{actor_id}"`. `redis>=5.0.1` already in `backend/requirements.txt:16`.
- **7 copy-pasted call sites**, all shaped `if actorId: _ra = _throttle_take(actorId); if _ra: raise HTTPException(429, ..., headers={"Retry-After": str(_ra)})`:
  - query.py:1776-1783 — inside sync `run_query` (impl for `POST /query`)
  - query.py:2372-2375 — inside sync `run_query_spec` (impl for `POST /query/spec`)
  - query.py:5678 — `POST /query/distinct` (sync route handler `distinct_values`)
  - query.py:6335 — `POST /query/pivot` (sync route handler `run_pivot`)
  - query.py:8607 — `POST /query/period-totals` (sync route handler `period_totals`)
  - query.py:9722 — `POST /query/period-totals/batch` (calls `period_totals(item, db, actorId)` per item at 9753 → double charge)
  - query.py:9780 — `POST /query/period-totals/compare` (calls `period_totals` twice at 9816-9817 → triple charge)
- **Async wrappers with `Request`**: `run_query_endpoint` (query.py:1705-1725) and `run_query_spec_endpoint` (query.py:2321-2342) already take `request: Request` and dispatch the sync impl to the heavy pool via `_run_cancellable_in_pool`.
- **Internal `run_query` callers without actorId** (query.py:3140, 3310, 3350, 3547, 3815, 4436, 4529, 4736, 4758, 4883, 5520, 5660) — currently skip throttling via the `if actorId:` gate; must stay unthrottled (they run inside an already-admitted request).
- **Metrics**: `counter_inc` imported at query.py:35 from `app/metrics.py:21`; `query_rate_limited_total` already emitted at query.py:1780, 5683, 8610.
- **Sibling limits (not rate limiting, leave alone)**: `HEAVY_QUERY_CONCURRENCY=8` (query.py:78), `SPEC_QUERY_CONCURRENCY=7` (query.py:86), per-actor semaphore `USER_QUERY_CONCURRENCY=1` (query.py:413-431), `QUERY_POOL_SIZE=16` (`backend/app/query_pool.py:19`).
- **Frontend already handles 429**: `frontend/src/lib/api.ts:413-434` (GET) and :472-493 (mutations) retry up to 2 times, honor `Retry-After` (integer seconds), and dispatch a `rate-limit` CustomEvent. No frontend change needed.
- **Env files**: `backend/.env` has `QUERY_RATE_PER_SEC=4`, `QUERY_BURST=12`, `REDIS_URL`/`REDIS_PREFIX` present (values not quoted here). `backend/.env.example:20-21` suggests `5`/`10`, `REDIS_URL=` empty at :17.
- **Auth context**: spec 02 (P0-security) introduces an authenticated-user dependency that stamps the request; until it lands, `actorId` remains the best available key. `settings.snapshot_actor_id` (config.py:39, default `dev_user`) is what server-side snapshot calls send as actor.

## Desired State

- Rate limiting is **on by default** with safe production values; `0` remains the explicit disable switch.
- One shared enforcement helper, keyed by (in precedence order): authenticated user id from `request.state` (spec 02) → `actorId` param → client IP. Anonymous callers are no longer unlimited.
- A **global** bucket (all callers combined) in front of the per-user bucket.
- Exactly **one token charged per HTTP request** — internal fan-out (`batch` → `period_totals`) no longer double-charges.
- Documented 429 contract (already what the frontend consumes): status `429`, header `Retry-After: <int seconds ≥1>`, body `{"detail": "Rate limit exceeded"}`.
- Redis vs in-process tradeoff documented in `.env.example`: in-process buckets are per-worker, so with `WORKERS=N` the effective limit is N× the configured value; set `REDIS_URL` for correct shared limits under gunicorn multi-worker (waitress/uvicorn single-worker deployments don't need it).

Defaults (chosen so a 20-widget dashboard load survives the burst, matching the working values in `backend/.env`):

| Env key | Old default | New default | Meaning |
|---|---|---|---|
| `QUERY_RATE_PER_SEC` | 0 (off) | 5 | per-user refill rate |
| `QUERY_BURST` | 0 (off) | 20 | per-user bucket size |
| `QUERY_RATE_GLOBAL_PER_SEC` | — (new) | 50 | global refill rate |
| `QUERY_BURST_GLOBAL` | — (new) | 100 | global bucket size |

## Implementation Plan

All steps in `backend/app/routers/query.py` unless noted.

1. **Change defaults + add global config** at query.py:592-599:
   ```python
   _Q_RATE = int(os.environ.get("QUERY_RATE_PER_SEC", "5") or "0")
   _Q_BURST = int(os.environ.get("QUERY_BURST", "20") or "0")
   _Q_RATE_GLOBAL = int(os.environ.get("QUERY_RATE_GLOBAL_PER_SEC", "50") or "0")
   _Q_BURST_GLOBAL = int(os.environ.get("QUERY_BURST_GLOBAL", "100") or "0")
   ```
   Keep the existing try/except-to-0 wrapper. `0` in either member of a pair disables that bucket (existing semantics in `_throttle_take` line 683 — preserve).

2. **Parametrize the buckets.** Change signatures (same file, no behavior change beyond the params):
   - `_tb_redis_take(key: str, rate: int, burst: int)` (query.py:651) — replace `_Q_RATE`/`_Q_BURST` reads at :663/:665 with the params; key already comes in as `f"{_REDIS_PREFIX}:{key}"` at :659 (keep prefixing inside).
   - `_throttle_take(key: str, rate: int, burst: int)` (query.py:680) — replace module-global reads at :683, :693, :695, :701 with params. Drop the `if not actor_id: return None` bypass (callers now always pass a non-empty key).

3. **Add key-resolution + enforcement helpers** directly below `_throttle_take` (~query.py:704):
   ```python
   def _rl_key(request: Optional[Request], actor_id: Optional[str]) -> str:
       # spec 02 auth middleware stamps request.state.user_id; fall back gracefully until it lands
       uid = getattr(getattr(request, "state", None), "user_id", None) if request is not None else None
       if uid:
           return f"u:{uid}"
       if actor_id and str(actor_id).strip():
           return f"u:{str(actor_id).strip()}"
       host = request.client.host if (request is not None and request.client) else "unknown"
       return f"ip:{host}"

   def _enforce_rate_limit(request: Optional[Request], actor_id: Optional[str], endpoint: str) -> None:
       # ponytail: global checked first — a rejected per-user call wastes one global token; negligible
       wait = _throttle_take("__global__", _Q_RATE_GLOBAL, _Q_BURST_GLOBAL)
       if wait is None:
           wait = _throttle_take(_rl_key(request, actor_id), _Q_RATE, _Q_BURST)
       if wait:
           try:
               counter_inc("query_rate_limited_total", {"endpoint": endpoint})
           except Exception:
               pass
           raise HTTPException(status_code=429, detail="Rate limit exceeded",
                               headers={"Retry-After": str(wait)})
   ```
   `Request` is already imported (used by `run_query_endpoint`); verify, else add to the fastapi import.

4. **Move enforcement to the HTTP boundary (one charge per request):**
   - `/query`: delete the block at query.py:1776-1783 from sync `run_query`; add `_enforce_rate_limit(request, actorId, "query")` as the first statement of `run_query_endpoint` (query.py:1706, before `_run_cancellable_in_pool`). Internal `run_query(...)` callers (3140, 3310, … 8488, 8505) are now never charged — correct, they run inside an admitted request.
   - `/query/spec`: delete query.py:2372-2375 from `run_query_spec`; add `_enforce_rate_limit(request, actorId, "spec")` at the top of `run_query_spec_endpoint` (query.py:2322).
   - `/query/distinct` (query.py:5663): add `request: Request` param after `payload` in `distinct_values`; replace the inline block at :5678-5686 with `_enforce_rate_limit(request, actorId, "distinct")`. (FastAPI injects `Request` into sync handlers fine.)
   - `/query/pivot` (query.py:6311): same — add `request: Request`, replace :6335-6337 with `_enforce_rate_limit(request, actorId, "pivot")`.
   - `/query/period-totals` (query.py:8593): `period_totals` is both a route handler and an internal helper (called at 9753, 9816, 9817). Split it: rename the existing function to `_period_totals_impl` (unchanged signature, delete the throttle block at :8607-8613 from its body) and add a thin route handler:
     ```python
     @router.post("/period-totals")
     def period_totals(payload: dict, request: Request, db: Session = Depends(get_db), actorId: Optional[str] = None, publicId: Optional[str] = None, token: Optional[str] = None) -> dict:
         _enforce_rate_limit(request, actorId, "period_totals")
         return _period_totals_impl(payload, db, actorId, publicId, token)
     ```
   - `/query/period-totals/batch` (query.py:9711): add `request: Request`; replace :9722-9724 with `_enforce_rate_limit(request, actorId, "period_totals_batch")`; change the per-item call at :9753 to `_period_totals_impl(item, db, actorId)`.
   - `/query/period-totals/compare` (query.py:9766): add `request: Request`; replace :9780-9782 with `_enforce_rate_limit(request, actorId, "period_totals_compare")`; change :9816-9817 to call `_period_totals_impl`.
   - Grep `grep -n "_throttle_take" backend/app/routers/query.py` afterwards — only the definition and the two calls inside `_enforce_rate_limit` may remain.

5. **Redis global key**: no extra work — `_tb_redis_take` receives `"__global__"` as the key and the global rate/burst as args; the existing Lua script (query.py:608-633) is parameterized already. Verify the `EXPIRE` math (`burst / rate`) can't divide by zero: it can't, since `_throttle_take` returns early when rate/burst ≤ 0 before hitting Redis.

6. **Document in `backend/.env.example`** (replace lines 17-21 area):
   ```
   # Query rate limiting (token bucket). 0 disables a bucket. Defaults: 5/20 per-user, 50/100 global.
   QUERY_RATE_PER_SEC=5
   QUERY_BURST=20
   QUERY_RATE_GLOBAL_PER_SEC=50
   QUERY_BURST_GLOBAL=100
   # In-process buckets are PER WORKER: with gunicorn WORKERS=N the effective limit is N x configured.
   # Set REDIS_URL to share buckets across workers (required for correct limits with run_prod_gunicorn.sh).
   # Single-worker deployments (waitress, uvicorn --workers 1) do not need Redis.
   REDIS_URL=
   REDIS_PREFIX=ratelimit
   ```

7. **Test** — add `backend/tests/test_rate_limit.py` (pytest, in-process path only; no Redis in CI):
   - monkeypatch module globals `_Q_RATE=2, _Q_BURST=3, _Q_RATE_GLOBAL=100, _Q_BURST_GLOBAL=100` and clear `_TB_STATE`;
   - `_throttle_take("u:a", 2, 3)` allows 3 immediate calls, 4th returns wait ≥ 1;
   - `_rl_key(None, "alice") == "u:alice"`; `_rl_key(None, None) == "ip:unknown"`; a stub request with `state.user_id="bob"` wins over `actorId`;
   - global bucket: with `_Q_RATE_GLOBAL=1, _Q_BURST_GLOBAL=1`, two different keys → second call raises via `_enforce_rate_limit` with `HTTPException.status_code == 429` and integer `Retry-After` header.

8. **Spec 02 alignment note**: when spec 02's auth dependency lands, it must set `request.state.user_id` (or this helper's `getattr` chain updated to its chosen attribute) — leave a one-line comment in `_rl_key` pointing at spec 02. No hard dependency: until then keys fall back to `actorId`/IP.

Backward compat: deployments with explicit `QUERY_RATE_PER_SEC=0` stay disabled; existing `.env` values (4/12) keep working. Behavior change to announce in release notes: anonymous/no-actorId callers are now limited per-IP, and batch endpoints charge 1 token instead of 2N+1 (users may lower `QUERY_BURST` from inflated values). Snapshot service traffic keys as `u:dev_user` (`settings.snapshot_actor_id`) — burst 20 covers a snapshot render; raise per-user values if snapshot jobs ever 429 (visible in `query_rate_limited_total`).

## Files to Modify

- `backend/app/routers/query.py` — defaults (592-599), parametrize `_tb_redis_take`/`_throttle_take` (651, 680), add `_rl_key` + `_enforce_rate_limit` (~704), move enforcement into 7 route handlers, split `period_totals` into route + `_period_totals_impl`, fix internal calls at 9753/9816-9817.
- `backend/.env.example` — new keys + Redis multi-worker doc comment.
- `backend/tests/test_rate_limit.py` — new test file.

## Acceptance Criteria

- [ ] Fresh install with no rate env vars set enforces 5 req/s (burst 20) per user and 50 req/s (burst 100) global on all 7 query endpoints.
- [ ] `QUERY_RATE_PER_SEC=0` disables the per-user bucket; `QUERY_RATE_GLOBAL_PER_SEC=0` disables the global bucket (each independently).
- [ ] A request with no `actorId` is rate-limited by client IP, not unlimited.
- [ ] 429 response: body `{"detail": "Rate limit exceeded"}`, header `Retry-After` with integer seconds ≥ 1 — unchanged shape (frontend api.ts retry logic keeps working with zero frontend changes).
- [ ] `/query/period-totals/batch` with N items consumes exactly 1 token (no mid-batch 429 from internal `period_totals` calls).
- [ ] Internal `run_query(...)` helper calls (e.g. query.py:3140) never hit the limiter.
- [ ] With `REDIS_URL` set, per-user and global buckets are shared across gunicorn workers (Lua bucket keyed `ratelimit:u:<id>` / `ratelimit:__global__`).
- [ ] `query_rate_limited_total` counter emitted with an `endpoint` label on every 429.
- [ ] `grep -c "_throttle_take" backend/app/routers/query.py` shows only definition + `_enforce_rate_limit` usages.
- [ ] `backend/tests/test_rate_limit.py` passes.

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/backend
# unit tests
venv/bin/python -m pytest tests/test_rate_limit.py -q

# manual: start backend, hammer /query as one anonymous client (no actorId)
for i in $(seq 1 30); do
  curl -s -o /dev/null -w "%{http_code} " -X POST http://localhost:8000/api/query \
    -H 'Content-Type: application/json' -d '{"sql":"SELECT 1"}'
done; echo
# expect: first ~20 return 200, then 429s

# verify Retry-After header present on a 429
curl -si -X POST http://localhost:8000/api/query -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT 1"}' | grep -i "HTTP/\|retry-after"

# verify batch charges once: with QUERY_BURST=20, a 15-item period-totals batch must return 200, not 429

# frontend sanity: load a dashboard at :3000 — no 429 errors in devtools network tab under normal use;
# api.ts auto-retry (rate-limit CustomEvent in console listeners) fires only under deliberate flooding
```

## Out of Scope

- Concurrency limits (`HEAVY_QUERY_CONCURRENCY`, `SPEC_QUERY_CONCURRENCY`, `USER_QUERY_CONCURRENCY`, `QUERY_POOL_SIZE`) — separate admission-control layer, untouched.
- Rate limiting non-query routes (auth/login throttling belongs to P0-security specs).
- Migrating rate config into `config.py` `Settings` — module-level `os.environ` reads are the established pattern in query.py.
- Frontend changes — existing 429/Retry-After handling in `frontend/src/lib/api.ts` already satisfies the contract.
- Per-endpoint differentiated limits, quota tiers, or admin-configurable limits UI.
- Removing the client-supplied `actorId` parameter itself (spec 02 owns authenticated identity).
