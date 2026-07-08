---
id: 07-structured-logging
title: Replace print-logging with the logging module
priority: P1
effort: L
depends_on: []
area: backend
---

## Problem

The backend has no logging configuration. All diagnostics go through `print(..., flush=True)` (~394 calls) and `sys.stderr.write(...)` (~519 calls) scattered across `backend/app/`. Consequences:

- No log levels — debug spam cannot be turned off in production. `should_use_sqlglot()` writes 2–4 stderr lines on **every** query request, and the SQLGlot builder dumps the full generated SQL to stderr per query.
- No timestamps, no logger names, no structured output — logs are unparseable by journald/Loki/CloudWatch.
- ~657 `except Exception: pass` blocks silently swallow failures (admin bootstrap, DuckDB init, scheduler startup) with zero trace.
- Committed log files bloat the repo: `backend/temp.logs.log` is 9.9 MB.

## Current State

All refs verified 2026-07-07 on branch `feature/alpha-themes-foundation`.

**No logging config anywhere.** `grep -rn "basicConfig\|dictConfig" backend/app` → zero hits. Six files already create module loggers but they emit through the unconfigured root (default WARNING, no handler format):
- `backend/app/sqlgen_glot.py:16`, `backend/app/routers/query.py:17`, `backend/app/api_ingest.py:31`, `backend/app/routers/snapshot.py:14`, `backend/app/alerts_service.py:1384` — `logging.getLogger(__name__)`
- `backend/app/routers/datasources.py:63` — `_log = logging.getLogger("app.sync")`

**Per-query stderr spam** in `backend/app/sqlgen_glot.py`:
- Lines 6–7 and 13–14: module-import-time writes:
  ```python
  sys.stderr.write("[SQLGlot] sqlgen_glot.py MODULE LOADED \n")
  sys.stderr.flush()
  ```
- `should_use_sqlglot()` at line 2090; lines 2109–2126 write up to 3 stderr lines per call. Called per request from `backend/app/routers/query.py:4763`, `:6077`, `:7371`, `:9389`.
- Line 2085–2086: full generated SQL dumped to stderr per query.
- 109 `sys.stderr.write` calls total in this file (build path: lines 132, 213–218, 228–229, 238–239, 265–275, 283–285, 299–305, 326–327, 367–377, 419–431, …).

**Wrapper spam** in `backend/app/routers/query.py:7367–7374` — both stderr AND print for the same message:
```python
sys.stderr.write(f"[PIVOT] About to check should_use_sqlglot with actorId={actorId}\n")
...
print(f"[PIVOT] About to check should_use_sqlglot with actorId={actorId}", flush=True)
```

**print/stderr census** (grep counts per file):

| File | print | stderr |
|---|---|---|
| `backend/app/routers/query.py` | 186 | 160 |
| `backend/app/routers/datasources.py` | 110 | 0 |
| `backend/app/sqlgen_glot.py` | 0 | 109 |
| `backend/app/sqlgen.py` | 30 | 0 |
| `backend/app/api_ingest.py` | 18 | 0 |
| `backend/app/routers/updates.py` | 16 | 0 |
| `backend/app/db.py` | 15 | 2 |
| `backend/app/scheduler.py` | 11 | 0 |
| `backend/app/main.py` | 6 | 0 |
| `backend/app/routers/issues.py` | 2 | 0 |

**Silent swallows in startup path** — `backend/app/main.py`:
- Lines 147–148: admin bootstrap `except Exception: pass`
- Lines 150–154: `init_duck_shared()` failure → `pass` ("Non-fatal in dev")
- Lines 174–176: entire scheduler startup block → `pass`
Repo-wide: `except Exception:` followed by bare `pass` ≈ 657 sites in `backend/app/`.

**Legit stderr that must stay stderr**: `backend/app/main.py:44–56` — placeholder SECRET_KEY fatal check prints to stderr before exit. (SECRET_KEY value lives in `backend/.env`; never quote it.)

**Committed log files** (`git ls-files | grep '\.log$'`): `backend/backend.log`, `backend/temp.logs.log` (9.9 MB), `backend/temp2.logs.log` (1.9 MB), `logs/backend.log`, `logs/backend_clean.log`, `logs/gunicorn.log`, `logs/gunicorn_new.log`, plus 9 `.playwright-mcp/console-*.log`. `.gitignore` has **no** `*.log` rule.

**Existing env plumbing**: `backend/run_prod_gunicorn.sh:37` already reads `LOG_LEVEL="${LOG_LEVEL:-info}"` (passed to gunicorn only). `backend/app/config.py` is a Pydantic v2 `Settings` (`model_config` at line 15, `extra="ignore"`).

## Desired State

- One `configure_logging()` call at process start: level from `LOG_LEVEL`, plain-text or JSON lines via `LOG_FORMAT`, stdout by default (systemd/docker-friendly), optional `RotatingFileHandler` via `LOG_FILE`.
- All hot-path diagnostics (`sqlgen_glot.py`, `query.py`) at DEBUG — silent at the default INFO level.
- `print(`/`sys.stderr.write(` count in `backend/app/` = 0 (single exception: the SECRET_KEY fatal block in `main.py`, which runs pre-logging-friendly and must hit stderr).
- Named startup/scheduler swallows log at WARNING with `exc_info=True`. Policy documented for the remaining ~650.
- No `.log` files tracked by git; `*.log` ignored.

## Implementation Plan

### Step 1 — Logging config module

Create `backend/app/logging_setup.py` (stdlib only, no new dependency):

```python
import json, logging, logging.handlers, os, sys, time

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        d = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            d["exc"] = self.formatException(record.exc_info)
        return json.dumps(d, ensure_ascii=False, default=str)

def configure_logging() -> None:
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    fmt = os.getenv("LOG_FORMAT", "text").lower()
    handler: logging.Handler = logging.StreamHandler(sys.stdout)
    log_file = os.getenv("LOG_FILE", "").strip()
    if log_file:
        # ponytail: fixed 10MB x 5 backups; make configurable only if someone asks
        handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # uvicorn/gunicorn loggers propagate to root; drop their duplicate handlers
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers[:] = []
        lg.propagate = True
```

Do **not** add these keys to `config.py` `Settings` — plain `os.getenv` is enough and avoids import-order issues (logging must configure before `settings` side effects log anything). `extra="ignore"` at `config.py:15` means the new env vars are harmless.

### Step 2 — Wire into main.py

In `backend/app/main.py`, immediately after the imports block (before the SECRET_KEY check at line 43):

```python
from .logging_setup import configure_logging
configure_logging()
logger = logging.getLogger(__name__)
```

Leave the SECRET_KEY stderr block (lines 44–56) as-is — it must be visible even if logging misconfigures.

Convert the 6 prints and 3 named swallows:
- Line 125 `print(f"[startup] anyio thread limiter ...` → `logger.info(...)`
- Line 128 fallback → `logger.warning(...)`
- Line 147–148 admin bootstrap → `except Exception: logger.warning("admin bootstrap failed", exc_info=True)`
- Line 152–154 `init_duck_shared` → `except Exception: logger.warning("DuckDB shared init failed (continuing)", exc_info=True)`
- Line 171 watchdog print → `logger.error("scheduler watchdog error", exc_info=True)` (inside the loop's except)
- Line 174–176 scheduler startup → `except Exception: logger.warning("scheduler startup failed", exc_info=True)`

### Step 3 — Kill per-query stderr spam in sqlgen_glot.py

`backend/app/sqlgen_glot.py` already has `logger` at line 16.
1. Delete lines 6–7 and 13–14 (module-load writes). Keep the `import sys` only if still used after conversion; otherwise remove.
2. `should_use_sqlglot()` (2090–2135): delete ALL stderr writes (2109–2110, 2114–2115, 2119–2120, 2125–2126). The flag check needs zero logging; if trace is ever wanted, one `logger.debug` suffices — do not re-add per-call INFO.
3. Line 2085–2086 full-SQL dump → `logger.debug("final SQL: %s", final_sql)`.
4. Remaining ~100 `sys.stderr.write(f"[SQLGlot] ...")` + paired `sys.stderr.flush()` → `logger.debug(...)`, drop the flush lines. Mechanical conversion (see Step 5 regex).

### Step 4 — query.py hot path

`backend/app/routers/query.py` already has `logger` at line 17.
1. Lines 7367–7374: delete the duplicated `[PIVOT]` stderr+print pairs entirely (4 statements → 0; the call at 7371 stays).
2. Convert remaining 186 prints + 160 stderr writes → `logger.debug(...)` by default; anything printed inside an `except` block → `logger.warning(...)` (keep `exc_info=True` where the message interpolates the exception).

### Step 5 — Mechanical conversion of remaining files

Order (by count): `routers/datasources.py`, `sqlgen.py`, `api_ingest.py`, `routers/updates.py`, `db.py`, `scheduler.py`, `routers/issues.py`.

Per file: ensure `logger = logging.getLogger(__name__)` exists near the top (datasources.py: reuse existing `_log = logging.getLogger("app.sync")` at line 63 for sync-related messages, add a module `logger` for the rest). Then apply these rules — regex-assisted, but **review each hunk**, do not blind-sed (f-strings with nested quotes and multi-line prints break naive regex):

| Pattern | Replacement |
|---|---|
| `print(f"...", flush=True)` / `print("...", flush=True)` | `logger.info(...)` if startup/lifecycle msg, else `logger.debug(...)` |
| `print(..., file=sys.stderr)` | `logger.warning(...)` |
| `sys.stderr.write(f"...\n")` | `logger.debug(...)` |
| standalone `sys.stderr.flush()` | delete |
| print inside `except` block | `logger.warning(...)` or `logger.error(..., exc_info=True)` |

Level heuristic by existing tag: `[startup]`, `[SCHEDULER]`, `[SYNC]` lifecycle → INFO; `[SQLGlot]`, `[PIVOT]`, `[DEBUG]` → DEBUG; anything with "error"/"failed"/"FATAL" → WARNING/ERROR. Keep messages verbatim otherwise — no rewording pass.

After each file: `python -c "import ast; ast.parse(open('backend/app/<file>').read())"` then the grep check in Verification.

### Step 6 — Bare-except policy (do NOT convert all 657 now)

Policy going forward, applied only where touched:
- Swallow in a **startup, scheduler, or sync** path → `logger.warning("<what failed>", exc_info=True)` instead of `pass`.
- Swallow in a **per-row/per-item best-effort loop** (formatting, optional metadata) → leave `pass`, it's intentional.
- New code: bare `except Exception: pass` requires a `# ponytail:`-style justification comment or a `logger.debug`.

This spec converts only the three `main.py` sites (Step 2). Record the policy in `backend/app/logging_setup.py` module docstring so it travels with the code.

### Step 7 — Purge committed logs

```bash
cd /Users/mohammed/Documents/Bayan
git rm --cached backend/backend.log backend/temp.logs.log backend/temp2.logs.log \
  logs/backend.log logs/backend_clean.log logs/gunicorn.log logs/gunicorn_new.log \
  .playwright-mcp/console-*.log
rm backend/temp.logs.log backend/temp2.logs.log backend/backend.log
```
Append to `.gitignore`:
```
*.log
logs/
.playwright-mcp/
```
(History rewrite for the 9.9 MB blob is out of scope.)

### Step 8 — Env documentation

Add to `backend/.env` example/README section (do not touch real `.env` values):
```
LOG_LEVEL=INFO        # DEBUG|INFO|WARNING|ERROR
LOG_FORMAT=text       # text|json
LOG_FILE=             # empty = stdout; path enables 10MB x5 rotating file
```
Note: `run_prod_gunicorn.sh:37` already exports `LOG_LEVEL` — the same var now drives app logging too, which is the desired behavior.

## Files to Modify

- `backend/app/logging_setup.py` — **new**: `configure_logging()` + `JsonFormatter` + swallow policy docstring
- `backend/app/main.py` — call `configure_logging()` first; convert 6 prints; log 3 startup swallows
- `backend/app/sqlgen_glot.py` — delete import-time + `should_use_sqlglot` stderr writes; convert ~100 writes to `logger.debug`
- `backend/app/routers/query.py` — delete `[PIVOT]` duplicate spam (7367–7374); convert 186 prints + 160 stderr writes
- `backend/app/routers/datasources.py` — convert 110 prints (reuse `app.sync` logger for sync paths)
- `backend/app/sqlgen.py` — convert 30 prints
- `backend/app/api_ingest.py` — convert 18 prints (logger exists at :31)
- `backend/app/routers/updates.py` — convert 16 prints
- `backend/app/db.py` — convert 15 prints + 2 stderr writes
- `backend/app/scheduler.py` — convert 11 prints
- `backend/app/routers/issues.py` — convert 2 prints
- `.gitignore` — add `*.log`, `logs/`, `.playwright-mcp/`
- delete tracked: `backend/backend.log`, `backend/temp.logs.log`, `backend/temp2.logs.log`, `logs/*.log`, `.playwright-mcp/console-*.log`

## Acceptance Criteria

- [ ] `grep -rn "print(" backend/app --include='*.py' | grep -v "secrets; print"` returns 0 hits (the SECRET_KEY block's heredoc example string is the only allowed match)
- [ ] `grep -rn "sys.stderr" backend/app --include='*.py'` matches only `backend/app/main.py` SECRET_KEY block (≤3 hits)
- [ ] `LOG_LEVEL=INFO`: issuing a widget query produces **zero** `[SQLGlot]`/`[PIVOT]` log lines
- [ ] `LOG_LEVEL=DEBUG`: the same query produces the SQLGlot trace lines via logger (timestamped, named `app.sqlgen_glot`)
- [ ] `LOG_FORMAT=json` emits one valid JSON object per line (parses with `jq .`)
- [ ] `LOG_FILE=/tmp/bayan.log` writes there and rotates config is `RotatingFileHandler(maxBytes=10MB, backupCount=5)`
- [ ] Startup with unreachable DuckDB path logs a WARNING with traceback instead of silent pass
- [ ] `git ls-files | grep '\.log$'` returns nothing
- [ ] All existing tests pass: `cd backend && python -m pytest tests/ -x -q`

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/backend

# 1. Static: no prints/stderr left (expect only main.py SECRET_KEY block)
grep -rn "print(" app --include='*.py' | grep -v "secrets; print"
grep -rn "sys.stderr" app --include='*.py'

# 2. Import smoke test
python -c "from app.logging_setup import configure_logging; configure_logging(); import app.main"

# 3. JSON format check
LOG_FORMAT=json LOG_LEVEL=INFO python -c "
import logging
from app.logging_setup import configure_logging
configure_logging()
logging.getLogger('t').info('hello')" | python -c "import json,sys; json.loads(sys.stdin.read()); print('json ok')"

# 4. Runtime: start dev server, run a dashboard query, watch output
LOG_LEVEL=INFO ./run_dev.sh   # expect no [SQLGlot] lines on query
LOG_LEVEL=DEBUG ./run_dev.sh  # expect timestamped app.sqlgen_glot DEBUG lines

# 5. Tests
python -m pytest tests/ -x -q

# 6. Repo hygiene
git ls-files | grep '\.log$'   # empty
```

## Out of Scope

- Converting the remaining ~650 `except Exception: pass` sites (policy defined; applied opportunistically)
- Git history rewrite to purge the 9.9 MB log blob (needs `git filter-repo`, coordinate separately)
- Request-ID / correlation-ID middleware and contextvars-based log enrichment
- Frontend `console.log` cleanup
- Centralized log shipping (Loki/CloudWatch) — JSON output is the enabler, shipping is ops
- Adding `python-json-logger` or `structlog` — stdlib formatter covers current needs
