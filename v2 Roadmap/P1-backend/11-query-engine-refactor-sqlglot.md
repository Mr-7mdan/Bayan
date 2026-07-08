---
id: 11-query-engine-refactor-sqlglot
title: Decompose query.py and complete SQLGlot migration
priority: P1
effort: XL
depends_on: ['03-sql-injection-hardening']
area: backend
---

## Problem

`backend/app/routers/query.py` is 9,826 lines — every query endpoint, plus its own
rate limiter, result cache, concurrency guards, DuckDB-attach plumbing, and
expression-resolution helpers, all in one file. SQL generation is dual-path:
legacy string-building (`app/sqlgen.py`, default) vs `SQLGlotBuilder`
(`app/sqlgen_glot.py`), gated per-request by `should_use_sqlglot(actorId)`.
The SQLGlot flag is OFF by default, the gate + try/except-fallback block is
copy-pasted at 5 endpoint sites, and `should_use_sqlglot` writes debug lines to
stderr on **every request**. Two builders must be kept in sync forever unless the
migration is finished.

## Current State

All refs verified 2026-07-07 on branch `feature/alpha-themes-foundation`.

**Config flags** — `backend/app/config.py:65-79`:
```python
enable_sqlglot: bool = Field(default=False, validation_alias=AliasChoices("ENABLE_SQLGLOT"), ...)
sqlglot_users: str = Field(default="", ...)   # comma-separated ids, ""=none, "*"=all
enable_legacy_fallback: bool = Field(default=True, ...)
```

**Gate** — `backend/app/sqlgen_glot.py:2090-2135` `should_use_sqlglot(user_id)`;
lines 2109-2126 do unconditional `sys.stderr.write(...)` per call (hot path noise).
`validate_sql` at 2138. `SQLGlotBuilder` (line 31) public methods:
`build_aggregation_query` (66), `build_distinct_query` (531),
`build_period_totals_query` (682), `build_pivot_query` (1556).

**Legacy builder** — `backend/app/sqlgen.py`: `build_sql` (435), `build_distinct_sql`
(1492), plus private helpers imported by query.py (`_normalize_expr_idents`,
`_case_expr` — e.g. query.py:143, 4803).

**Dual-path (gated) sites in query.py** — each repeats the same pattern:
`use_sqlglot = should_use_sqlglot(actorId)` → try SQLGlot → on exception, if
`not settings.enable_legacy_fallback` raise HTTP 500, else `use_sqlglot = False`
and fall through to legacy:
- `/query/spec` aggregation: gate 4763, builder 4830-4866, fallback 4885-4894, legacy from 4897
- `/query/distinct`: gate 6077, builder 6120-6128, fallback 6131-6138
- `/query/pivot`: gate 7371, builder 8240ff, fallback 8433
- `/query/period-totals`: gate 9389, builder 9438, fallback 9526
- `/query/pivot/sql` (8511): SQLGlot-only, **no gate, no fallback** (8570)

Metrics already emitted: `counter_inc("sqlglot_queries_total", ...)` (4882),
`legacy_queries_total` (4898), `sqlglot_errors_total` (4889); `counter_inc` at
`backend/app/metrics.py:21`.

**Ungated legacy `build_sql` call sites in query.py** (run regardless of flag —
mostly pivot base-CTE/probe builds and previews): 1373 (`preview_pivot_sql`),
3108, 3434, 3635, 3868, 6054, 6864, 7104, 7398, 7443, 8066, 9204. Also
`backend/app/routers/datasources.py:46` imports `build_sql` for preview.

**query.py internal infrastructure** (line anchors for the decomposition):
- Source validation: `_SAFE_SOURCE_RE`/`_validate_source` 47-61
- Concurrency: `_HEAVY_LIMIT` 76, `_SPEC_LIMIT` 84, `_HEAVY_SEM` 386,
  `_spec_concurrency_guard` 395, `_actor_sem` 418-432
- Expr/derived-column helpers: `_build_expr_map_helper` 136, `_build_datepart_expr_helper`
  225, `_resolve_derived_columns_in_where_helper` 282, `_auto_correct_column_case` 981,
  `_referenced_cols_in_expr` 1016, `_filter_by_basecols` 1122, `_resolve_table_name` 1260
- Date presets/holidays: `_strip_ui_op_keys` 98, `_load_holidays` 105,
  `_resolve_date_presets` 130
- DuckDB/MySQL attach plumbing: `_parse_mysql_dsn` 434, `_build_mysql_attach_str` 451,
  `_duck_attach_type` 465, `_apply_duck_mysql_attachments` 475, `_duck_has_table` 526,
  `_resolve_join_catalog` 560, `_resolve_duckdb_path_from_engine` 936
- Rate limiting (token bucket + optional Redis Lua): 592-703 (`_throttle_take` 680)
- Result cache + ds cache: 758-862 (`_cache_key/_cache_get/_cache_set`, `_ds_cache_*`)
- Result shaping: `_http_for_db_error` 864, `_coerce_date_like` 880, `_json_safe_cell` 911
- Async pool bridge: `_run_cancellable_in_pool` 1615 (uses existing
  `app/query_pool.py` + `app/cancellation.py` — these already exist, do NOT recreate)
- Duplicate `get_db` defined twice: lines 63 and 705
- Endpoints: `run_query` 1705/1731, `run_query_spec` 2321/2345, `distinct_values`
  5663, `run_pivot` 6310, `pivot_sql` 8511, `period_totals` 8593,
  `period_totals_batch` 9711, `period_totals_compare` 9766

**External importers that constrain the decomposition:**
- `backend/app/main.py:26` — `from .routers import query as query_router`
- `backend/app/routers/alerts.py:23` — `from ..routers.query import run_query_spec, run_pivot, period_totals`
- `backend/app/alerts_service.py:220` — `from .routers.query import run_query_spec, _resolve_date_presets`
- `backend/app/alerts_service.py:2266,3211` — lazy `from .routers.query import run_pivot`

**Tests:** `backend/tests/test_sqlglot_builder.py` (324 lines: builder unit tests,
`TestShouldUseSQLGlot`, `TestValidateSQL`). Runner script `backend/test_sqlglot.sh`.
sqlglot pinned in `backend/requirements.txt:9` (`sqlglot>=25.0.0`).

## Desired State

1. `routers/query.py` replaced by a `routers/query/` package of focused modules
   (largest module ≤ ~2,500 lines), with `__init__.py` re-exporting the public
   surface so `main.py`, `alerts.py`, and `alerts_service.py` need **zero changes**.
2. One shared `execute_build()` facade owning the SQLGlot-vs-legacy decision,
   fallback, and metrics — the 5 copy-pasted gate blocks deleted.
3. SQLGlot default-ON via config default flip, legacy retained only behind
   `ENABLE_LEGACY_FALLBACK` until the parity matrix is green, then the gated
   legacy branches removed. `sqlgen.py` shrinks to the ungated internal uses
   (pivot base-CTE/probe, previews) with a follow-up note, not deleted in this spec.
4. Hot-path stderr debug spam removed from `should_use_sqlglot`.

## Implementation Plan

### Phase A — decompose query.py into a package (no behavior change)

1. Create `backend/app/routers/query/` and move code by responsibility. Moves are
   mechanical cut-paste; keep function names and signatures identical. Target modules:
   - `common.py` — one `get_db` (dedupe lines 63/705), `_validate_source` +
     `_SAFE_SOURCE_RE`, `_engine_for_datasource` (713), `_resolve_table_name`,
     `_http_for_db_error`, `_coerce_date_like`, `_json_safe_cell`, shared imports.
   - `caching.py` — result cache (758-845) + ds cache (846-862) with their module
     globals and TTL constants.
   - `ratelimit.py` — token bucket, Redis client/Lua (592-703).
   - `concurrency.py` — `_HEAVY_SEM`, `_SPEC_SEM`, `_spec_concurrency_guard`,
     `_actor_sem`, `_run_cancellable_in_pool` (1615-1703). Re-use `app/query_pool.py`
     and `app/cancellation.py` as today.
   - `duck_attach.py` — MySQL DSN/attach helpers, `_duck_has_table`,
     `_resolve_join_catalog`, `_resolve_duckdb_path_from_engine` (434-560, 936-975).
   - `exprs.py` — expr-map/derived-column/case helpers (136-384, 976-1258:
     `_build_expr_map_helper`, `_build_datepart_expr_helper`,
     `_resolve_derived_columns_in_where_helper`, `_auto_correct_column_case`,
     `_referenced_cols_in_expr`, `_filter_by_basecols`).
   - `dates.py` — `_strip_ui_op_keys`, `_load_holidays`, `_resolve_date_presets` (98-134).
   - `sqlbuild.py` — NEW facade (see step 3).
   - `raw.py` — `run_query` + `run_query_endpoint` (1705-2320).
   - `spec.py` — `run_query_spec` + endpoint (2321-5662).
   - `distinct.py` — `distinct_values` (5663-6309).
   - `pivot.py` — `run_pivot` + `pivot_sql` (6310-8592).
   - `period_totals.py` — `period_totals` + `/batch` + `/compare` (8593-9826).
2. Routing: keep ONE `router = APIRouter(prefix="/query", tags=["query"])` defined
   in `common.py`; every endpoint module imports it and registers with the same
   decorators/paths as today (no subrouter `include_router` — identical OpenAPI paths).
3. `__init__.py` re-exports for backward compat (verbatim import surface):
   ```python
   from .common import router, get_db
   from .raw import run_query
   from .spec import run_query_spec
   from .pivot import run_pivot, pivot_sql
   from .period_totals import period_totals
   from .dates import _resolve_date_presets
   ```
   This keeps `main.py:26` (`query_router.router`), `alerts.py:23`, and
   `alerts_service.py:220/2266/3211` working unchanged. Delete the old
   `routers/query.py` file in the same commit (a leftover module shadows the package).
4. Circular-import guard: `spec.py`/`pivot.py` call `run_query` from `raw.py` —
   import it at module top (`from .raw import run_query`); `raw.py` must not import
   the endpoint modules. If a cycle appears, move the shared piece into `common.py`,
   don't add lazy imports.
5. Run the app + tests (see Verification). Commit Phase A alone — pure move,
   reviewable as `git diff --stat` shrinkage.

### Phase B — single SQL-build facade

6. In `sqlbuild.py` add one function per shape, owning gate + fallback + metrics:
   ```python
   def build_with_fallback(kind: str, dialect: str, actor_id: str | None,
                           sqlglot_fn: Callable[[], str],
                           legacy_fn: Callable[[], tuple]) -> tuple[str | tuple, bool]:
       """Returns (sql_or_legacy_result, used_sqlglot). Raises HTTP 500 when
       SQLGlot fails and enable_legacy_fallback is False."""
   ```
   Callers wrap their existing SQLGlot-builder call and legacy call in closures.
   Replace the 5 gate blocks (spec 4763-4898, distinct 6077-6151, pivot 7371/8433,
   period-totals 9389-9531; leave `pivot_sql` as-is, it is SQLGlot-only). Keep the
   existing counters: `sqlglot_queries_total`, `legacy_queries_total`,
   `sqlglot_errors_total` with the same label keys.
7. In `sqlgen_glot.py`, delete the unconditional `sys.stderr.write` calls in
   `should_use_sqlglot` (2109-2126) — replace with a single `logger.debug`.

### Phase C — parity test matrix

8. Add `backend/tests/test_sqlglot_parity.py`. Fixture: a temp DuckDB file with one
   table `t` (~50 rows) covering: date col, numeric col, text col with quotes/NULLs,
   a col name needing quoting (`"Order Count"`). For each case in the matrix, build
   SQL via `sqlgen.build_sql(...)` AND `SQLGlotBuilder(dialect="duckdb")`, execute
   both against the fixture, and assert **result-set equality** (sorted rows, float
   tolerance 1e-9). Matrix (≥ 20 cases):
   - agg: sum/avg/min/max/count/distinct-count × with/without legend
   - time bucketing: day/week(mon+sun)/month/quarter/year on the date col
   - where: scalar eq, IN-list, ranges (`gte/lte`), expression-key filters,
     mixed column+expression
   - orderBy value/x asc/desc, limit/offset
   - expr_map: custom column referenced in x, in where, in y
   - multi-legend (list) and multi-series
   - distinct: `build_distinct_sql` vs `build_distinct_query` (with expr_map)
   For non-DuckDB dialects (postgres/mysql/mssql), assert SQLGlot output parses via
   `sqlgen_glot.validate_sql(sql, dialect)` (no live DBs in CI) — extend the
   existing pattern in `test_sqlglot_builder.py:140-166`.
9. Any parity failure = fix `SQLGlotBuilder`, never the test's legacy expectation.
   Record known intentional differences (e.g. quoting style) in the test docstring.

### Phase D — staged rollout (config only, no code)

10. **Stage 1 (pilot, ~1 week):** in production `.env`: `ENABLE_SQLGLOT=true`,
    `SQLGLOT_USERS=<2-3 heavy-user ids>`. Watch `sqlglot_errors_total` and the
    `legacy_queries_total`-after-fallback rate via `/api/metrics`.
11. **Stage 2 (default-on):** flip code defaults in `config.py`:
    `enable_sqlglot` default `True`, and treat empty `sqlglot_users` as `*` (it
    already does — `sqlgen_glot.py:2124`). Keep `enable_legacy_fallback=True`.
    Ship; watch error counter for ≥ 1 release cycle.
12. **Stage 3 (hard cutover):** set `ENABLE_LEGACY_FALLBACK=false` in prod env.
    SQLGlot failures now surface as HTTP 500 with detail instead of silently
    falling back. Hold until `sqlglot_errors_total` is flat at 0.

### Phase E — retire the gated legacy path

13. Delete: the `legacy_fn` branches at the 5 facade call sites, the
    `build_with_fallback` fallback arm (rename to `build_sqlglot`),
    `should_use_sqlglot`, and the three config flags (`enable_sqlglot`,
    `sqlglot_users`, `enable_legacy_fallback`) from `config.py:64-79`. Remove
    `legacy_queries_total` counter emissions.
14. `sqlgen.py` stays for the **ungated** internal uses (pivot base-CTE/probe
    builds at query.py old-lines 3108/3434/3635/3868/6864/7104/7398/7443/8066/9204,
    `preview_pivot_sql` 1373, period-totals legacy shape 9204, and
    `datasources.py:46` preview). Migrating those to SQLGlot is a follow-up spec —
    add a `# ponytail: legacy build_sql retained for pivot base-CTE/probe paths;
    migrate in spec 12` comment at the top of `sqlgen.py`.
15. Update `tests/test_sqlglot_builder.py`: drop `TestShouldUseSQLGlot` (263-300)
    when the gate is deleted.

## Files to Modify

- `backend/app/routers/query.py` — DELETED (replaced by package)
- `backend/app/routers/query/__init__.py` — NEW; router + backward-compat re-exports
- `backend/app/routers/query/{common,caching,ratelimit,concurrency,duck_attach,exprs,dates,sqlbuild,raw,spec,distinct,pivot,period_totals}.py` — NEW; code moved per Phase A map
- `backend/app/sqlgen_glot.py` — remove stderr debug in `should_use_sqlglot` (2109-2126); Phase E: delete `should_use_sqlglot`
- `backend/app/config.py` — Phase D2: flip `enable_sqlglot` default; Phase E: delete lines 64-79 flags
- `backend/app/sqlgen.py` — Phase E: add retention comment; no functional change
- `backend/tests/test_sqlglot_parity.py` — NEW parity matrix
- `backend/tests/test_sqlglot_builder.py` — Phase E: drop `TestShouldUseSQLGlot`
- `backend/app/routers/alerts.py`, `backend/app/alerts_service.py`, `backend/app/main.py` — NO changes (re-exports preserve imports); verify only

## Acceptance Criteria

- [ ] `routers/query.py` no longer exists; `routers/query/` package modules each ≤ ~2,500 lines
- [ ] `from app.routers.query import run_query_spec, run_pivot, period_totals, _resolve_date_presets` still works (alerts.py/alerts_service.py untouched and importable)
- [ ] OpenAPI paths unchanged: `/api/query`, `/query/spec`, `/query/distinct`, `/query/pivot`, `/query/pivot/sql`, `/query/period-totals[/batch|/compare]`
- [ ] Exactly one gate/fallback implementation (`sqlbuild.build_with_fallback`); zero inline `should_use_sqlglot` calls in endpoint modules
- [ ] `should_use_sqlglot` emits nothing to stderr
- [ ] Parity suite ≥ 20 cases, all green on DuckDB result-set equality; pg/mysql/mssql outputs validate via `validate_sql`
- [ ] Stage 2 shipped: SQLGlot on by default with fallback; `sqlglot_errors_total` observable at `/api/metrics`
- [ ] Phase E (after soak): gated legacy branches, gate function, and the 3 config flags deleted; `sqlgen.py` retained with follow-up comment
- [ ] Existing `tests/test_sqlglot_builder.py` still passes at every phase

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/backend

# Unit + parity tests
PYTHONPATH=. pytest tests/test_sqlglot_builder.py tests/test_sqlglot_parity.py -q

# Import surface intact (Phase A gate)
PYTHONPATH=. python -c "
from app.routers.query import router, run_query_spec, run_pivot, period_totals, _resolve_date_presets
from app.routers import alerts; import app.alerts_service; import app.main
print('imports OK, routes:', len(router.routes))"

# Boot + smoke each endpoint (legacy default, then SQLGlot)
uvicorn app.main:app --port 8000 &   # then:
curl -s -X POST localhost:8000/api/query/spec -H 'Content-Type: application/json' \
  -d '{"source":"<existing duck table>","x":"<date col>","y":"<num col>","agg":"sum"}' | head -c 300
ENABLE_SQLGLOT=true SQLGLOT_USERS='*' uvicorn app.main:app --port 8000 &  # repeat curls
curl -s localhost:8000/api/metrics | grep -E 'sqlglot_(queries|errors)_total|legacy_queries_total'

# Manual: dashboard with chart + pivot + distinct filter dropdown + period-totals KPI
# renders identically with flag off vs on (frontend :3000).

# Phase E gate: no gated legacy remnants
grep -rn "should_use_sqlglot\|enable_legacy_fallback" app/ && echo "FAIL: remnants" || echo OK
```

## Out of Scope

- Decomposing `backend/app/alerts_service.py` (3,487 lines) — separate spec
- Migrating the ungated `sqlgen.build_sql` uses (pivot base-CTE/probes, `preview_pivot_sql`, `datasources.py:46` preview) and deleting `sqlgen.py` — follow-up spec after Phase E soak
- SQL-injection parameterization itself (spec 03; this spec must rebase on it)
- Removing the `print()`/stderr debug scattered through endpoint bodies beyond `should_use_sqlglot` (harmless; clean opportunistically during moves)
- Runtime shadow-execution mode (parity matrix + counters cover it)
- Any frontend changes
