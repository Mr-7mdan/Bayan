---
id: 25-test-coverage-foundation
title: Test coverage foundation and cleanup
priority: P2
effort: L
depends_on: ['22-ci-cd-pipeline']
area: fullstack
---

## Problem

Testing is sparse and partly broken. Backend has 3 real test files with no conftest, no pytest config, and pytest not in any requirements file — the suite currently has **3 failing tests**. Three scratch scripts squat at the backend root pretending to be tests (`test_deposits.py` has hardcoded `C:/Bayan` Windows paths). The frontend has **zero** test setup — no test runner, no test script in package.json — despite pure, highly testable modules (formula engine, date presets, API concurrency layer). Nothing runs in CI.

## Current State

All refs verified 2026-07-07 on branch `feature/alpha-themes-foundation`.

**Real backend tests** (`backend/tests/`, no `conftest.py`, no `__init__.py`):
- `tests/test_date_presets.py` (641 lines) — unit tests for `app.date_presets` resolver.
- `tests/test_holidays.py` (93 lines) — holiday rules + `materialize_holidays`.
- `tests/test_sqlglot_builder.py` (324 lines) — `SQLGlotBuilder`, `should_use_sqlglot`, `validate_sql` from `app.sqlgen_glot`.

**Baseline run** (`cd backend && venv/bin/python -m pytest tests/ -q`): `3 failed, 111 passed`. Failures:
1. `test_date_presets.py:55` `test_legacy_map_covers_all_old_presets` — expects `day_before_yesterday` in `LEGACY_PRESET_MAP`; key absent from `app/date_presets.py`.
2. `test_date_presets.py:176` `test_day_before_last_working_day` — expects `datetime(2026,3,15)`, resolver returns `datetime(2026,3,13)` (raw offset vs workday-aware offset disagreement).
3. `test_sqlglot_builder.py:221` `test_derived_columns_no_validation` — asserts `"IN" in sql.upper()`; generated SQL uses OR-expanded predicates, not `IN`.

**Scratch scripts at backend root** (nothing imports them; verified via grep):
- `backend/test_deposits.py` (116 lines) — manual DuckDB probe, hardcoded `C:/Bayan/backend/.data/...` path. Dead.
- `backend/test_mssql_conn.py` (192 lines) — standalone pyodbc/SQLAlchemy MSSQL connectivity diagnostic. Useful as an ops tool, not a test.
- `backend/test_normalizer.py` (30 lines) — print-only exercise of `app.sql_dialect_normalizer.auto_normalize`; no asserts.

**Backend infra facts**:
- No pytest config anywhere (`pytest.ini`/`pyproject.toml`/`setup.cfg` absent in `backend/`). No `requirements-dev.txt`; `backend/requirements.txt` lacks pytest (has `httpx>=0.27.0`, which TestClient needs). Venv at `backend/venv/` has pytest 9.1.1 installed ad hoc.
- `backend/app/config.py:30` — `metadata_db_path` (env `METADATA_DB_PATH`), `:27` `duckdb_path` (env `DUCKDB_PATH`). `settings = Settings()` instantiated at import (`config.py:100`), reads `.env` in CWD; real env vars override `.env` values.
- `backend/app/models.py:173-180` — `engine_meta` + `SessionLocal` bound to `settings.metadata_db_path` **at import time**. Test env vars must be set before any `app.*` import.
- `backend/app/main.py:44-56` — refuses placeholder `SECRET_KEY` unless `environment` in `dev/development/test`.
- `backend/app/main.py:107-176` — startup runs `init_db()`, admin bootstrap from `ADMIN_EMAIL`/`ADMIN_PASSWORD`, `init_duck_shared()`, and starts the APScheduler **unless `RUN_SCHEDULER=0`** (`main.py:157`).
- `def get_db()` is copy-pasted 10x across routers (`routers/users.py:55`, `admin.py:23`, `dashboards.py:72`, `datasources.py:77`, `query.py:63` and `:705`, `alerts.py:31`, `ai.py:87`, `contacts.py:29`, `updates.py:20`) — all construct `SessionLocal()`, so pointing `METADATA_DB_PATH` at a temp file covers them all without dependency_overrides.
- Auth primitives to test: `backend/app/security.py` — `hash_password`/`verify_password`/`needs_rehash` (41-85), `sign_reset_token`/`verify_reset_token` (89-119), `sign_embed_token`/`verify_embed_token` (132-163). Login endpoint: `backend/app/routers/users.py:227` (returns user, no token, no `active` check — spec 02 changes this).
- Legacy SQL builder: `backend/app/sqlgen.py` (1626 lines), entry points `build_sql` (:435) and `build_distinct_sql` (:1492) — **zero tests** despite being the default path (`enable_sqlglot` defaults False, `config.py:65`).
- Sync watermark logic is inline in route handlers (`routers/datasources.py:655` `run_sync_now`, watermark init/reset at 842-892, 1001, 1468 inside `flush_sync_task`:1383) — not unit-testable until spec 10 refactors it.

**Frontend facts**:
- `frontend/package.json` — no `test` script (scripts: dev/build/start/lint/clean), no vitest/jest/RTL/playwright in deps. React 18.2.0, Next 15.5.3, TS ^5.5.4.
- `frontend/tsconfig.json:21-24` — path alias `@/*` → `src/*`.
- Prime pure-function targets: `frontend/src/lib/formula.ts` (210 lines — `parseReferences`:147, `compileFormula`:155, `evalRow`:207, FN table:79-142), `frontend/src/lib/datePresets.ts` (`parseLegacyPreset`:191, `isLastNDaysPreset`:202, `matchQuickPick`:210, `presetConfigToLabel`:227).
- `frontend/src/lib/api.ts` (1311 lines) — concurrency limiter `_acquireWidgetSlot`:325 (`MAX_WIDGET_CONCURRENCY`:322), GET dedup `_inflightGet`:345, TTL cache `_recentGetCache`:346 + `_cacheTtlForPath`:348, 429 retry loop:412-434 — all module-private state, tested through the exported `api` object with a mocked `fetch`.

**CI**: no `.github/workflows/` exists yet; spec 22 creates it.

## Desired State

- `backend/tests/` is the only test location; scratch scripts removed or relocated; suite is green.
- `pytest` runs from `backend/` with zero args: shared `conftest.py` provides temp SQLite/DuckDB env, a FastAPI `TestClient`, and a seeded admin user. `requirements-dev.txt` pins test deps.
- New backend tests cover: security primitives, login API, legacy `sqlgen.build_sql` (parity with existing sqlglot tests), `sql_dialect_normalizer`, and sync-task API surface (endpoint-level).
- Frontend has Vitest + React Testing Library wired (`npm test`), with unit tests for `formula.ts`, `datePresets.ts`, and `api.ts` GET dedup/retry behavior.
- Both suites run in the spec-22 CI workflow with coverage reporting; backend gated at `--cov-fail-under=25` (ratchet later).

## Implementation Plan

### Phase 1 — cleanup (backend root)

1. Delete `backend/test_deposits.py` (dead, Windows-only paths).
2. Move `backend/test_mssql_conn.py` → `backend/scripts/diagnose_mssql.py` (create `backend/scripts/` if absent). No code changes; it is an ops diagnostic.
3. Delete `backend/test_normalizer.py` after Phase 3 step 8 converts its 3 cases into real tests.

### Phase 2 — backend test infrastructure

4. Create `backend/requirements-dev.txt`:
   ```
   -r requirements.txt
   pytest>=8.0
   pytest-cov>=5.0
   ```
5. Create `backend/pytest.ini`:
   ```ini
   [pytest]
   testpaths = tests
   addopts = -q
   ```
6. Create `backend/tests/conftest.py`. Critical ordering: set env **at module top, before any `app.*` import**, because `app/config.py:100` and `app/models.py:173` bind at import:
   ```python
   import os, tempfile, uuid
   _tmp = tempfile.mkdtemp(prefix="bayan-test-")
   os.environ["METADATA_DB_PATH"] = os.path.join(_tmp, "meta.sqlite")
   os.environ["DUCKDB_PATH"] = os.path.join(_tmp, "test.duckdb")
   os.environ["RUN_SCHEDULER"] = "0"
   os.environ["APP_ENV"] = "test"
   os.environ["SECRET_KEY"] = "test-secret-key-not-a-placeholder"
   os.environ.pop("ADMIN_EMAIL", None); os.environ.pop("ADMIN_PASSWORD", None)

   import pytest

   @pytest.fixture(scope="session")
   def client():
       from fastapi.testclient import TestClient
       from app.main import app
       with TestClient(app) as c:   # context manager runs startup: init_db()
           yield c

   @pytest.fixture()
   def db():
       from app.models import SessionLocal
       s = SessionLocal()
       try: yield s
       finally: s.close()

   @pytest.fixture()
   def admin_user(db):
       from app.models import User
       from app.security import hash_password
       u = db.query(User).filter(User.email == "admin@test.local").first()
       if not u:
           u = User(id=str(uuid.uuid4()), name="Admin", email="admin@test.local",
                    password_hash=hash_password("test-password-123"), role="admin", active=True)
           db.add(u); db.commit(); db.refresh(u)
       return u
   ```
   Note: existing pure-unit test files (`test_date_presets.py` etc.) import `app.*` directly and stay untouched — env vars are harmless to them.
7. Fix the 3 failing baseline tests (root-cause each; likely test drift, but verify):
   - `test_legacy_map_covers_all_old_presets`: check git history of `LEGACY_PRESET_MAP` in `app/date_presets.py` — if `day_before_yesterday` was intentionally removed, drop it from the test's expected set; if it was lost in a refactor, restore the map entry.
   - `test_day_before_last_working_day` (`tests/test_date_presets.py:176`): resolver returns Mar 13 (Fri) where test expects Mar 15 (Sun) — determine whether `offset` for this preset is meant to be workday-aware; align test or `resolve_preset` accordingly and document the chosen semantics in the test docstring.
   - `test_derived_columns_no_validation` (`tests/test_sqlglot_builder.py:221`): sqlglot now expands `IN` to OR chains; relax assertion to accept either form (assert the filter values appear in the SQL instead of the literal `IN` keyword).

### Phase 3 — priority backend tests

8. `backend/tests/test_sql_dialect_normalizer.py` — convert the 3 print-cases from `backend/test_normalizer.py` into asserts against `app.sql_dialect_normalizer.auto_normalize`: bracket→duckdb quoting, missing-space-before-END repair, complex bracket idents to duckdb and mysql.
9. `backend/tests/test_security.py` — pure unit tests, no fixtures:
   - `hash_password`/`verify_password` round-trip; wrong password False; `needs_rehash` on a legacy-format hash.
   - `sign_reset_token`/`verify_reset_token`: valid round-trip returns user_id; expired (`ttl_seconds=-1`) returns None; tampered token returns None.
   - `sign_embed_token`/`verify_embed_token`: valid, expired, wrong `public_id` all covered.
10. `backend/tests/test_auth_api.py` — uses `client` + `admin_user` fixtures:
    - `POST /api/users/login` correct creds → 200 + user shape; wrong password → 4xx.
    - Document current gap with a test marked `xfail(reason="spec 02: login must reject inactive users")` for an `active=False` user.
    - Admin-gated endpoint (e.g. `GET /api/users?actorId=`) with admin id vs non-admin id vs missing — pin current authz behavior so spec 02/04 refactors have a regression net.
11. `backend/tests/test_sqlgen.py` — legacy builder parity with `tests/test_sqlglot_builder.py`: for each core scenario (simple sum agg, legend field, filters incl. quoting, distinct query via `build_distinct_sql`), call `app.sqlgen.build_sql` with equivalent args and assert the same structural properties (SUM/GROUP BY presence, alias `x`/`value`, identifier quoting per dialect duckdb/mssql/mysql). Read `build_sql`'s signature at `app/sqlgen.py:435` for exact kwargs.
12. `backend/tests/test_sync_api.py` — endpoint-level only (watermark internals wait for spec 10): using `client`, create a datasource + sync task via `POST /api/datasources` and `POST /api/datasources/{id}/sync-tasks`, then assert `GET .../sync/status` shape, `POST .../sync-tasks/{task_id}/flush` (see `flush_sync_task` at `routers/datasources.py:1383`) resets watermark state, and `DELETE` removes the task. No live source connection — use a `duckdb` type datasource pointing at the temp DuckDB file.

### Phase 4 — frontend testing

13. Install dev deps in `frontend/`:
    ```bash
    npm i -D vitest @vitest/coverage-v8 jsdom @testing-library/react@^14 @testing-library/jest-dom @testing-library/user-event
    ```
    (RTL 14, not 16 — React is pinned 18.2.0.)
14. Create `frontend/vitest.config.ts`:
    ```ts
    import { defineConfig } from 'vitest/config'
    import path from 'path'
    export default defineConfig({
      test: { environment: 'jsdom', globals: true, include: ['src/**/*.test.{ts,tsx}'] },
      resolve: { alias: { '@': path.resolve(__dirname, 'src') } },
    })
    ```
15. Add scripts to `frontend/package.json`: `"test": "vitest run"`, `"test:watch": "vitest"`, `"test:coverage": "vitest run --coverage"`.
16. `frontend/src/lib/formula.test.ts`:
    - `parseReferences`: `[@col]` → row refs, `[col]` → range refs, dedup, mixed.
    - `compileFormula`/`evalRow`: arithmetic with `[@a]+[@b]`, `^` → power, `SUM([col])`/`AVG`/`COUNTIF` with predicate strings (`">10"`, `"<>0"`, `"=x"`), `IF`/`AND`/`OR`/`COALESCE`/`ISBLANK`, `LIKE` `%`/`_` patterns, case-insensitive function names (the `FN_CI` proxy, `formula.ts:177`).
    - Date fns: `YEAR`/`MONTH`/`WEEKNUM` on ISO string, epoch seconds, epoch ms, `MM/DD/YYYY`; null on garbage.
    - `exec` returns `null` on runtime error; `execDebug` throws (`formula.ts:187-203`).
17. `frontend/src/lib/datePresets.test.ts`: `parseLegacyPreset` (legacy string keys + passthrough of `PresetConfig` objects + null on unknown), `isLastNDaysPreset("last_30_days")`, `matchQuickPick`, `presetConfigToLabel`.
18. `frontend/src/lib/api.test.ts` — module state means each test needs `vi.resetModules()` + dynamic `await import('@/lib/api')`; stub `global.fetch` with `vi.fn()`:
    - GET dedup: two concurrent identical GETs → one fetch call (`_inflightGet`).
    - Recent-cache TTL: sequential identical GETs within `NEXT_PUBLIC_GET_CACHE_MS` window → one fetch; after `vi.advanceTimersByTime` past TTL → second fetch.
    - 429 retry: fetch returns 429 with `retry-after` then 200 → resolves, exactly 2 fetch calls, `rate-limit` CustomEvent dispatched (`api.ts:423-431`).
    - Non-OK → rejects with `HTTP <status>` message.
19. Component tests and Playwright e2e (login + dashboard load): **defer** — see Out of Scope.

### Phase 5 — CI wiring (extends spec 22's workflow)

20. In the spec-22 workflow (`.github/workflows/ci.yml`), add/confirm two jobs:
    - backend: `pip install -r backend/requirements-dev.txt && cd backend && pytest --cov=app --cov-report=term --cov-fail-under=25`
    - frontend: `cd frontend && npm ci && npm run test:coverage`
    No hard frontend coverage gate yet — report only. Ratchet `--cov-fail-under` upward as specs 02/04/10/11 land tests.

## Files to Modify

- `backend/test_deposits.py` — delete
- `backend/test_normalizer.py` — delete (after conversion)
- `backend/test_mssql_conn.py` — move to `backend/scripts/diagnose_mssql.py`
- `backend/requirements-dev.txt` — new: pytest, pytest-cov
- `backend/pytest.ini` — new: testpaths config
- `backend/tests/conftest.py` — new: env bootstrap + `client`/`db`/`admin_user` fixtures
- `backend/tests/test_date_presets.py` — fix 2 failing tests (lines 55, 176 area)
- `backend/tests/test_sqlglot_builder.py` — fix 1 failing test (line ~221)
- `backend/tests/test_sql_dialect_normalizer.py` — new
- `backend/tests/test_security.py` — new
- `backend/tests/test_auth_api.py` — new
- `backend/tests/test_sqlgen.py` — new
- `backend/tests/test_sync_api.py` — new
- `frontend/package.json` — add devDeps + test scripts
- `frontend/vitest.config.ts` — new
- `frontend/src/lib/formula.test.ts` — new
- `frontend/src/lib/datePresets.test.ts` — new
- `frontend/src/lib/api.test.ts` — new
- `.github/workflows/ci.yml` — add test+coverage jobs (file created by spec 22)

## Acceptance Criteria

- [ ] `backend/test_deposits.py` and `backend/test_normalizer.py` gone; `backend/scripts/diagnose_mssql.py` exists; no `test_*.py` at backend root.
- [ ] `cd backend && pytest` (venv) collects only `backend/tests/` and passes with **0 failures** — including the 3 previously failing tests, each fixed at root cause with a comment explaining the chosen semantics.
- [ ] `conftest.py` isolates state: no test touches `backend/.data/meta.sqlite` or the dev DuckDB file (verify mtimes unchanged after a run).
- [ ] Scheduler does not start during tests (`RUN_SCHEDULER=0` respected; no APScheduler log lines in pytest output).
- [ ] New backend tests exist and pass: security primitives, login API (incl. one `xfail` documenting the inactive-user gap), legacy `sqlgen` parity, dialect normalizer, sync-task API.
- [ ] `cd frontend && npm test` passes with tests for `formula.ts`, `datePresets.ts`, `api.ts` (dedup, TTL cache, 429 retry).
- [ ] `npm run build` still succeeds (vitest config and test files excluded from Next build — confirm `*.test.ts` don't break `next build --no-lint`).
- [ ] CI workflow runs both suites; backend coverage gate `--cov-fail-under=25` passes.
- [ ] No secret values appear in test fixtures or CI config (test SECRET_KEY is an obvious dummy).

## Verification

```bash
# Backend: clean run + coverage
cd /Users/mohammed/Documents/Bayan/backend
venv/bin/pip install -r requirements-dev.txt
venv/bin/python -m pytest --cov=app --cov-report=term --cov-fail-under=25
# Expect: 0 failed; >= ~130 passed (111 baseline + new); coverage >= 25%

# Isolation check: dev databases untouched
stat -f "%m" .data/meta.sqlite   # before and after pytest — identical

# Root scripts gone
ls test_*.py 2>&1 | grep -q "No such file" && echo OK

# Frontend
cd /Users/mohammed/Documents/Bayan/frontend
npm test                    # all green
npm run test:coverage       # coverage table prints
npm run build               # Next build unaffected

# CI (after push)
gh run watch                # both test jobs green
```

## Out of Scope

- Component tests for ChartCard/AlertDialog — blocked on their decomposition (specs 13/14); testing 2000-line components now is wasted effort.
- Playwright e2e (login + dashboard) — add after spec 02 lands real auth; e2e against actorId-param auth would be rewritten immediately.
- Unit tests for sync watermark internals — logic is inline in `run_sync_now` (`routers/datasources.py:655`); spec 10 extracts it, tests come with that refactor.
- Consolidating the 10 duplicated `get_db()` helpers — worth doing, but belongs to spec 02/04's router touch-ups.
- Raising coverage gates beyond 25% backend / none frontend — ratchet in follow-up specs.
- Testing `sqlgen_glot.py` beyond existing coverage — spec 11 (query-engine-refactor-sqlglot) owns that.
