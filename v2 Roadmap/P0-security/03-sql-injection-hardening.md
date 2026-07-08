---
id: 03-sql-injection-hardening
title: Eliminate SQL injection in query engine and ATTACH strings
priority: P0
effort: L
depends_on: ['02-authentication-tokens-and-sessions']
area: backend
---

## Problem

Widget specs, custom columns, filters, sort fields, and datasource names all
originate from user input and are interpolated into raw SQL via f-strings. Three
classes of live injection vectors exist:

1. **Identifier injection.** The quoting helpers wrap identifiers in quotes but
   never escape an embedded closing quote. A column/table named `x" ; DROP TABLE
   users; --` becomes `"x" ; DROP TABLE users; --"`, breaking out of the quoted
   identifier. Affects `_quote_segment` (sqlgen.py) and the four+ copies of
   `_q_ident` / `_q_source` in query.py.
2. **Raw expression injection.** Custom-column `expr` strings are free-form SQL
   inlined verbatim into the SELECT list (sqlgen.py `build_sql`). There is no
   parse/validation gate; a crafted `expr` injects arbitrary SQL by design of the
   current code.
3. **ATTACH credential injection + credential log leak.** Remote MySQL/Postgres
   ATTACH strings are built by f-string-embedding host/user/password/database with
   no escaping (query.py `_build_mysql_attach_str`, datasources.py two inline
   copies). A password/host containing a single quote or a DuckDB connection-string
   delimiter breaks the `ATTACH '...'` literal. On failure the full exception —
   which can contain the connection string with the plaintext password — is printed
   to stdout/stderr and thus to logs.

Value literals are mostly safe already: filters use SQLAlchemy `:named` params and
DuckDB positional `?` binding, and `sqlgen._lit` doubles single quotes. This spec
does **not** re-plumb value binding; it closes identifier, raw-expression, and
ATTACH holes.

## Current State

Verified against the repo at time of writing.

**Identifier quoting — no internal-quote escaping (the core defect):**

- `backend/app/sqlgen.py:34` `_quote_segment(d, seg)` — wraps `seg` in `` ` ``,
  `[]`, or `"` per dialect but never doubles an embedded closing quote. Feeds
  `_qtable` (`:59`), `_qcol` (`:73`), `_qal` (`:106`), `_qleft_if_ident` (`:96`).
- `backend/app/routers/query.py:2526` `def _q_ident` — same pattern, no escaping:
  ```python
  d = (ds_type or '').lower()
  if 'mssql' in d or 'sqlserver' in d: return f"[{s}]"
  if 'mysql' in d and 'duckdb' not in d: return f"`{s}`"
  return f'"{s}"'
  ```
- Duplicate `_q_ident` copies with identical no-escape logic at
  `query.py:3664`, `query.py:6363`, and `query.py:8820` (`_q_ident_local`).
- `_q_source` copies at `query.py:2542`, `query.py:6388`, `query.py:8835`
  (`_q_source_local`) — split on `.`, wrap each part, no escaping.
- `_derived_lhs` copies at `query.py:2558`, `query.py:6403` build date-part
  expressions around `_q_ident(base)`.

**Identifier interpolation sites (representative, all user-derived):**

- `query.py:1575` `f"COUNT(DISTINCT {_q_ident(val_field)})"`
- `query.py:1581-1586` numeric-clean expr around `_q_ident(val_field)`
- `query.py:1596` `f"{e} AS {a}"` and `:1597` `sel` join
- `query.py:1612` `inner = f"SELECT {sel}{base_from_sql}{where_sql}{gb_sql}{order_by}"`
- `query.py:1349` `base_from_sql = f" FROM {_q_source(payload.source)}"`
- `query.py:3290-3303` `last_daily_sum` SQL built from `vcol`/`dcol` (`_q_ident`)
- `query.py:3327-3342` avg-period SQL from `vcol`, `num_expr`, `den_expr`,
  `_period_from_sql` (`_q_source`)
- `query.py:3791` `f" ORDER BY {_q_ident(_sort_col)} {_sort_dir}"` (sort dir already
  allowlisted to ASC/DESC at `:3789`)
- `query.py:539` `_duck_has_table`: `conn.execute(f"SELECT * FROM {t} LIMIT 0")`
  where `t` is the raw unquoted table string (bare interpolation, no quoting at all)

**Raw custom-column expression inlining:**

- `backend/app/sqlgen.py:707-710` `normalized_expr = _normalize_expr_idents(d, expr,
  ...)` then `normalize_sql_expression(...)` — `expr` is user free-form SQL, stored
  in `cc_expr_map[name]`.
- `sqlgen.py:616` `select_cols.append(f"{expr} AS {_qal(d, token)}")` and the
  transform inlining at `:775`, `:1005`, `:1165-1175` — the normalized expr is
  emitted into SELECT verbatim. `_normalize_expr_idents` (`:226`) only rewrites
  bracket/quote styles; it is **not** a validation gate and does not reject
  statement terminators, comments, or subquery/DDL keywords.

**ATTACH string construction + credential leak:**

- `query.py:434` `_parse_mysql_dsn` → `query.py:451` `_build_mysql_attach_str`
  builds `host=... port=... user=... password=... database=...` via f-strings, no
  escaping. Consumed at `query.py:511-514`:
  ```python
  attach_str = _build_mysql_attach_str(info, db_override)
  conn.execute(f"ATTACH '{attach_str}' AS \"{alias}\" (TYPE {attach_type})")
  ```
  On failure, `query.py:520` writes `f"[RemoteAttach] ATTACH '{alias}' failed: {e}"`
  — `e` from DuckDB commonly echoes the connection string (with password).
- `backend/app/routers/datasources.py:1979-1997` `_introspect_attach_remotes`:
  inline duplicate of the same f-string builder (`parts = [f"host={host}", ...]`,
  `attach_str = ' '.join(parts)`, `attach_sql = f"ATTACH '{attach_str}' AS
  \"{alias}\" (TYPE {attach_type})"`). Leaks on failure at `:2005`
  `print(f"[Introspect] ATTACH '{alias}' failed: {e}")`.
- `backend/app/routers/datasources.py:2297-2320` `/tables` handler — second inline
  duplicate, identical build + leak at `:2330`.
- `attach_sql` is persisted verbatim into the replay registry via
  `db.register_duck_attach(alias, attach_sql)` (`db.py:239`) and re-executed on pool
  connections in `_replay_attaches_on_conn` (`db.py:249`), so any fix to the builder
  automatically propagates through replay — but the registry then holds the password
  in memory (acceptable; already required for replay).

**Existing reusable pieces:**

- `sqlglot` + `from sqlglot import exp` already imported in
  `backend/app/sqlgen_glot.py:11-12` (`SQLGlotBuilder`, `:31`). `sqlglot` provides
  `exp.to_identifier(name, quoted=True).sql(dialect=...)` for correct,
  escape-doubling identifier quoting, and `sqlglot.parse_one` for expression
  validation.
- `sqlgen._lit` (`sqlgen.py:22`) already doubles single quotes for value literals —
  reuse its escaping idea for identifiers.

## Desired State

- One shared identifier-quoting helper that **doubles the internal quote character**
  before wrapping, so no identifier can break out of its quotes, in any dialect.
- Optional schema allowlist: when the resolved column/table set for a datasource is
  known, reject identifiers not in that set before they reach SQL. Fail closed
  (400) rather than silently continuing.
- Custom-column raw expressions gated by a SQLGlot parse + denylist so a spec cannot
  smuggle statement terminators, comments, DDL/DML keywords, or stacked queries into
  the SELECT list.
- ATTACH strings built by a single credential-safe helper that rejects/encodes
  values containing DuckDB connection-string delimiters and single quotes, and that
  NEVER lets a credential-bearing string reach a log line. Failure logs show alias +
  host only.

## Implementation Plan

Ordered. Each step is independently shippable and testable.

### Step 1 — Central safe identifier quoting (`backend/app/sql_ident.py`, new)

Create a small module (no new dependency; `sqlglot` already present):

```python
from sqlglot import exp

# Map internal ds_type substrings -> sqlglot dialect for quoting
def _dialect(ds_type: str) -> str:
    d = (ds_type or '').lower()
    if 'mssql' in d or 'sqlserver' in d: return 'tsql'
    if 'mysql' in d and 'duckdb' not in d: return 'mysql'
    if 'postgres' in d: return 'postgres'
    return 'duckdb'

def quote_ident(name: str, ds_type: str = '') -> str:
    """Quote a single identifier segment, doubling any embedded quote char.
    Idempotent: already-quoted input is unwrapped first."""
    s = str(name or '').strip().strip('\n\r\t')
    if not s:
        return s
    # unwrap one existing quote layer so we re-quote consistently
    for lq, rq in (('"','"'), ('`','`'), ('[',']')):
        if s.startswith(lq) and s.endswith(rq) and len(s) >= 2:
            s = s[1:-1]
            break
    return exp.to_identifier(s, quoted=True).sql(dialect=_dialect(ds_type))

def quote_source(name: str, ds_type: str = '') -> str:
    """Quote a possibly dotted table ref (schema.table / catalog.schema.table)."""
    s = str(name or '').strip()
    if not s or '(' in s or ')' in s:  # subquery/expression: leave as-is
        return s
    return '.'.join(quote_ident(p, ds_type) for p in s.split('.'))
```

`exp.to_identifier(...).sql(...)` doubles the internal quote per dialect (`"a""b"`,
`` `a``b` ``, `[a]]b]`) — this is the escape the current helpers lack.

### Step 2 — Route all identifier quoting through the shared helper

Replace the bodies of the duplicated helpers so behavior is centralized (keep the
nested-function signatures to avoid touching every call site):

- `sqlgen.py:34` `_quote_segment` → delegate to `quote_ident(seg, d)` (map `d` is
  already a dialect name; pass through). Keep the "already an expression" guards in
  `_qcol`/`_qtable` unchanged.
- `query.py:2526`, `:3664`, `:6363`, `:8820` `_q_ident` bodies → `return
  quote_ident(name, ds_type)`.
- `query.py:2542`, `:6388`, `:8835` `_q_source` bodies → `return quote_source(name,
  ds_type)`.
- `query.py:539` `_duck_has_table`: replace `f"SELECT * FROM {t} LIMIT 0"` first
  attempt with `f"SELECT * FROM {quote_source(t)} LIMIT 0"`; the fallback branches at
  `:544-552` become redundant — collapse to the single quoted attempt.

`_derived_lhs` (`:2558`, `:6403`) needs no change beyond calling the updated
`_q_ident`. Sort direction is already allowlisted (`query.py:3789`) — leave it.

### Step 3 — Optional schema allowlist for identifiers

Where the resolved column set is already probed (e.g. `_list_cols_for_agg_base` at
`query.py:1351`, and the `WHERE 1=0` probes at `:1358`, `:3600`, `:3839`, `:4091`),
build a `set[str]` of known columns and pass it down. Add a guard helper in
`sql_ident.py`:

```python
class UnknownIdentifier(ValueError): ...

def require_known(name: str, allowed: set[str] | None) -> str:
    if allowed is not None:
        base = str(name).strip().strip('"`[]').split('.')[-1].lower()
        if base and base not in {a.lower() for a in allowed}:
            raise UnknownIdentifier(name)
    return name
```

Apply at the points where `val_field`, `date_field`, dimension names, and
`orderBy` are first read (before quoting) in the aggregation/pivot/avg branches.
Fail closed: catch `UnknownIdentifier` at the router entry and return
`HTTPException(400, "unknown field")`. Where the column set cannot be cheaply
resolved, pass `allowed=None` (quoting from Step 2 still holds the line). This is
defense-in-depth, not the primary control — do not block the release on 100%
coverage of allowlist wiring.

### Step 4 — Gate custom-column raw expressions

In `sqlgen.py`, add a validation function and call it before any `expr` enters
`cc_expr_map` (sites `:707`, `:775`, `:1005`, `:1165`, `:1175`) and before the
projection at `:616`:

```python
import sqlglot
_EXPR_DENY = re.compile(r';|--|/\*|\bDROP\b|\bDELETE\b|\bINSERT\b|\bUPDATE\b|'
                        r'\bALTER\b|\bATTACH\b|\bCREATE\b|\bCOPY\b|\bPRAGMA\b|'
                        r'\bGRANT\b|\bINTO\b', re.IGNORECASE)

def validate_expr(expr: str, dialect: str) -> str:
    e = str(expr or '').strip()
    if not e:
        return e
    if _EXPR_DENY.search(e):
        raise ValueError(f"disallowed token in expression")
    # Must parse as a single scalar expression, not a statement/stacked query
    try:
        trees = sqlglot.parse(e, read=dialect)
    except Exception:
        raise ValueError("unparseable expression")
    if len(trees) != 1 or isinstance(trees[0], (exp.Command, exp.DDL, exp.Insert,
            exp.Delete, exp.Update, exp.Drop)):
        raise ValueError("expression is not a scalar")
    return e
```

Call `validate_expr(expr, d)` at the top of each custom-column/transform ingestion
loop. Raise → surface as `HTTPException(400, "invalid custom column expression")`
in the router (`build_sql` callers at `query.py:1373`, `:3635`, `:6054`, `:7104`
and `datasources.py:1812`). Keep `_normalize_expr_idents` / `normalize_sql_expression`
as-is; they run **after** the gate.

### Step 5 — Credential-safe ATTACH + no-leak logging

Add to `backend/app/routers/query.py` (or a shared `sql_ident.py`) one builder and
use it in all three sites:

```python
_ATTACH_BAD = re.compile(r"[']")  # single quote breaks the ATTACH '...' literal

def build_attach_string(info: dict, db_override: str = '') -> str:
    def _safe(v, field):
        s = str(v)
        if _ATTACH_BAD.search(s) or '\n' in s or '\x00' in s:
            raise ValueError(f"invalid character in connection {field}")
        return s
    parts = [f"host={_safe(info.get('host','localhost'),'host')}",
             f"port={int(info.get('port',3306))}"]
    if info.get('user'):     parts.append(f"user={_safe(info['user'],'user')}")
    if info.get('password'): parts.append(f"password={_safe(info['password'],'password')}")
    db = (db_override or info.get('database') or '').strip()
    if db: parts.append(f"database={_safe(db,'database')}")
    return ' '.join(parts)
```

- Replace `query.py:451` `_build_mysql_attach_str` body with a call to
  `build_attach_string`.
- Replace the inline builders at `datasources.py:1979-1986` and `:2304-2311` with
  `build_attach_string({...})` calls.
- Alias quoting in the ATTACH statement: `f'... AS "{alias}" ...'` at `query.py:513`,
  `datasources.py:1995`, `:2320` → use `quote_ident(alias)` so a malicious alias
  cannot break out.
- **No-leak logging.** At every failure log that currently prints `e` —
  `query.py:520`, `datasources.py:2005`, `:2330` (and `db.py:265-287` replay
  fallback) — scrub before logging. Add a helper:
  ```python
  def _scrub(msg: str, secrets: list[str]) -> str:
      out = str(msg)
      for s in secrets:
          if s: out = out.replace(s, '***')
      return out
  ```
  Log `f"[RemoteAttach] ATTACH '{alias}' failed: {_scrub(e, [password, attach_str])}"`,
  or simpler: log only `type(e).__name__` + alias + host, never the exception text
  verbatim. Do not log `attach_str` or `attach_sql` anywhere (grep for existing
  `attach_sql` / `attach_str` in log lines and scrub each).

### Step 6 — Regression self-check

Add `backend/tests/test_sql_ident.py` (pytest, no fixtures):

```python
def test_quote_ident_escapes():
    from app.sql_ident import quote_ident
    assert quote_ident('a"b') == '"a""b"'                       # duckdb default
    assert quote_ident('x`y', 'mysql') == '`x``y`'
    assert quote_ident('a]b', 'mssql') == '[a]]b]'
    # breakout attempt cannot escape the quotes
    q = quote_ident('x" ; DROP TABLE t; --')
    assert q.count('"') % 2 == 0 and ';' in q  # ';' stays inside the quotes

def test_validate_expr_blocks_stacked():
    import pytest
    from app.sqlgen import validate_expr
    with pytest.raises(ValueError): validate_expr("1); DROP TABLE t; --", "duckdb")
    with pytest.raises(ValueError): validate_expr("(SELECT 1) /* x */", "duckdb")
    assert validate_expr("amount * 1.2", "duckdb")

def test_attach_rejects_quote():
    import pytest
    from app.routers.query import build_attach_string
    with pytest.raises(ValueError):
        build_attach_string({'host':'h','port':3306,'user':'u',"password":"p' OR '1"})
```

## Files to Modify

- `backend/app/sql_ident.py` — new: `quote_ident`, `quote_source`, `require_known`,
  `UnknownIdentifier`, `_scrub`, `build_attach_string`.
- `backend/app/sqlgen.py` — `_quote_segment` (`:34`) delegates to `quote_ident`; add
  `validate_expr` and call it before every `cc_expr_map`/projection ingestion
  (`:616`, `:707`, `:775`, `:1005`, `:1165`, `:1175`).
- `backend/app/routers/query.py` — repoint `_q_ident` (`:2526`, `:3664`, `:6363`,
  `:8820`) and `_q_source` (`:2542`, `:6388`, `:8835`) to shared helpers; fix
  `_duck_has_table` (`:539`); replace `_build_mysql_attach_str` (`:451`) with
  `build_attach_string`; quote alias + scrub logs at `:513`/`:520`; wire allowlist +
  400 handling at `build_sql` call sites.
- `backend/app/routers/datasources.py` — replace inline ATTACH builders
  (`:1979-1997`, `:2297-2320`) with `build_attach_string` + `quote_ident(alias)`;
  scrub failure logs (`:2005`, `:2330`).
- `backend/app/db.py` — scrub the replay fallback log path (`:265-287`); do not log
  `sql`.
- `backend/tests/test_sql_ident.py` — new regression check.

## Acceptance Criteria

- [ ] `quote_ident` doubles the embedded quote for duckdb/mysql/mssql/postgres; a
      breakout payload stays inside the quoted token.
- [ ] All `_q_ident`/`_q_source`/`_quote_segment` copies route through the shared
      helper; no remaining f-string wraps identifiers without escaping (grep clean).
- [ ] `_duck_has_table` quotes its table argument; the try/except quoting ladder is
      gone.
- [ ] Custom-column `expr` containing `;`, `--`, `/* */`, or a DDL/DML keyword is
      rejected with HTTP 400; a legitimate arithmetic/CASE expr still builds.
- [ ] ATTACH string builder rejects single-quote / newline / NUL in any
      host/user/password/database field; alias is quoted in the ATTACH statement.
- [ ] No log line (query.py, datasources.py, db.py) prints the ATTACH connection
      string, `attach_sql`, or a raw DuckDB exception that could echo the password.
- [ ] `pytest backend/tests/test_sql_ident.py` passes.
- [ ] Existing dashboards (aggregation, pivot, avg-period, remote MySQL JOIN) still
      render — no functional regression.

## Verification

```bash
# 1. Unit regression
cd backend && python -m pytest tests/test_sql_ident.py -q

# 2. No un-escaped identifier f-strings remain (should return only the shared helper)
grep -rnE 'f".*\{.*\}"' backend/app/routers/query.py backend/app/sqlgen.py \
  | grep -E '\[\{|`\{|"\{[a-z_]+\}"' ; echo "review the above by hand"

# 3. No credential-bearing log lines
grep -rn 'attach_str\|attach_sql' backend/app/routers/*.py backend/app/db.py \
  | grep -iE 'print|write|log'   # expect: none, or scrubbed only

# 4. Manual injection probes (dev server on :8000, authed session from spec 02):
#   a) Widget spec with val_field = 'x" ; DROP TABLE users; --'  -> query runs
#      against a nonexistent column (error) or 400, never executes the DROP.
#   b) Custom column expr = '1); DROP TABLE users; --'  -> HTTP 400.
#   c) Create a MySQL datasource whose password contains a single quote,
#      trigger /introspect -> 400 "invalid character", NOT a broken ATTACH,
#      and the server log shows no password.

# 5. Regression smoke: open an existing dashboard with a remote MySQL JOIN widget
#    and a pivot widget; both render identical data to pre-change.
```

## Out of Scope

- Re-plumbing value binding (filters already use `:named` / `?` params; `_lit`
  already escapes) — not touched here.
- The SQLGlot migration itself (`sqlgen_glot.py`) — we only borrow its `exp` quoting
  and parser; the legacy builder stays.
- Encrypting the in-memory ATTACH replay registry (`db.py`) — it must hold the
  connection string to replay; out of scope for injection hardening.
- Rate-limiting / auth on the query endpoints — covered by spec 02.
- DuckDB `enable_external_access` / filesystem sandboxing — separate ops hardening.
