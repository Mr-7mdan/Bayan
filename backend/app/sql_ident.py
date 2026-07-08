"""Central SQL-injection-hardening helpers.

Single source of truth for:
  - safe identifier / source quoting (doubles the internal quote char so an
    identifier can never break out of its quotes, in any dialect),
  - an optional schema allowlist guard for identifiers,
  - a credential-safe DuckDB ATTACH connection-string builder,
  - a secret-scrubbing helper for log lines.

No new dependency: ``sqlglot`` is already used by ``sqlgen_glot.py``.
"""

from __future__ import annotations

import re

from sqlglot import exp


# Map internal ds_type / dialect substrings -> sqlglot dialect for quoting.
def _dialect(ds_type: str = '') -> str:
    d = (ds_type or '').lower()
    if 'mssql' in d or 'sqlserver' in d or 'tsql' in d:
        return 'tsql'
    if 'mysql' in d and 'duckdb' not in d:
        return 'mysql'
    if 'mariadb' in d and 'duckdb' not in d:
        return 'mysql'
    if 'postgres' in d:
        return 'postgres'
    return 'duckdb'


def quote_ident(name: str, ds_type: str = '') -> str:
    """Quote a single identifier segment, doubling any embedded quote char.

    Idempotent: one existing quote layer is unwrapped first so we re-quote
    consistently (``"a""b"``, ``` `a``b` ```, ``[a]]b]``)."""
    s = str(name or '').strip().strip('\n\r\t')
    if not s:
        return s
    # unwrap one existing quote layer so we re-quote consistently
    for lq, rq in (('"', '"'), ('`', '`'), ('[', ']')):
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


class UnknownIdentifier(ValueError):
    """Raised when an identifier is not in the resolved allowlist for a datasource."""


class InvalidExpression(ValueError):
    """Raised when a custom-column / transform expression fails validation."""


def require_known(name: str, allowed: "set[str] | None") -> str:
    """Fail-closed allowlist guard. ``allowed=None`` disables the check."""
    if allowed is not None:
        base = str(name).strip().strip('"`[]').split('.')[-1].lower()
        if base and base not in {a.lower() for a in allowed}:
            raise UnknownIdentifier(name)
    return name


# ── Credential-safe ATTACH ───────────────────────────────────────────
# A single quote breaks out of the DuckDB ``ATTACH '...'`` literal; newline /
# NUL corrupt the connection string. Reject rather than silently mangle.
_ATTACH_BAD = re.compile(r"[']")


def build_attach_string(info: dict, db_override: str = '') -> str:
    """Build a DuckDB MySQL/Postgres ATTACH connection string safely.

    Raises ``ValueError`` if any credential field contains a character that
    could break out of the ``ATTACH '...'`` literal."""
    def _safe(v, field):
        s = str(v)
        if _ATTACH_BAD.search(s) or '\n' in s or '\x00' in s:
            raise ValueError(f"invalid character in connection {field}")
        return s

    parts = [
        f"host={_safe(info.get('host', 'localhost'), 'host')}",
        f"port={int(info.get('port', 3306))}",
    ]
    if info.get('user'):
        parts.append(f"user={_safe(info['user'], 'user')}")
    if info.get('password'):
        parts.append(f"password={_safe(info['password'], 'password')}")
    db = (db_override or info.get('database') or '').strip()
    if db:
        parts.append(f"database={_safe(db, 'database')}")
    return ' '.join(parts)


def scrub(msg, secrets: "list[str]") -> str:
    """Redact secret substrings from a log message."""
    out = str(msg)
    for s in secrets:
        if s:
            out = out.replace(str(s), '***')
    return out
