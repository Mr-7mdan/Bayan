---
id: 05-audit-logging
title: Audit log for security-relevant actions
priority: P0
effort: M
depends_on: ['02-authentication-tokens-and-sessions']
area: backend
---

## Problem

Bayan has no audit trail. Logins, password changes, permission grants, datasource
credential edits, dashboard sharing/publishing, admin actions, and data exports leave
no record. Corporate deployments require an append-only log of who did what, to what,
when, and from where — plus an admin API to read it.

## Current State

Verified against the working tree (branch `feature/alpha-themes-foundation`):

- **No audit table or helper exists.** `backend/app/models.py` defines `User` (line 22),
  `Datasource` (line 59), `DatasourceShare` (line 73), `SharePermission` (line 85),
  `ShareLink` (line 115), `EmbedToken` (line 126) — nothing audit-related.
- **Schema migrations are lightweight and inline**: `init_db()` in
  `backend/app/models.py:183-284` runs `Base.metadata.create_all` then ad-hoc
  `PRAGMA table_info` / `ALTER TABLE` / `CREATE TABLE IF NOT EXISTS` statements. New
  tables are picked up automatically by `create_all` — no Alembic.
- **Security-relevant endpoints, none instrumented:**
  - `backend/app/routers/users.py`: `login` (line 227, also silently rehashes legacy
    hashes), `signup` (212), `change_password` (241), `request_password_reset` (255),
    `confirm_password_reset` (286), `reset_password` admin-direct (301),
    `bootstrap_admin` (315), `admin_create_user` (346), `admin_set_active` (364),
    `admin_set_password` (376). Admin check helper `_require_admin` at line 63.
  - `backend/app/routers/datasources.py`: `create_ds` (97 — stores encrypted
    connection URI), `patch_ds` (341 — can rewrite `connection_encrypted`; `put_ds`
    at 429 delegates to it), `add_ds_share` (388), `remove_ds_share` (415),
    `delete_ds` (2594), `export_datasources` (2630 — **decrypts and returns plaintext
    connection URIs** via `_to_export_item`, line 2606), `export_datasource` (2671),
    `import_datasources` (2705).
  - `backend/app/routers/dashboards.py`: `save_dash` (236), `delete_embed_token`
    (341), `delete_share` (384), `create_embed_token` (405), `delete_dash` (422),
    `publish_dash` (441), `unpublish_dash` (457), `set_publish_token` (484),
    `export_dashboards` (594 — includes decrypted datasource URIs when
    `includeDatasources=true`), `export_dashboard` (653), `import` (703).
  - `backend/app/routers/users.py` `add_dashboard_collection` (89) is where
    cross-user dashboard sharing grants `SharePermission` (via
    `grant_share_permission`, models.py:664).
  - `backend/app/routers/admin.py`: `scheduler_refresh` (45), `duckdb_set_active`
    (160 — switches the active analytics DB), `update_branding` (190),
    `reset_branding` (231). Admin check helper `_is_admin` at line 31.
- **IP is obtainable**: `ProxyHeadersMiddleware` is installed in
  `backend/app/main.py:72` (trusted_hosts="*"), so `request.client.host` reflects
  `X-Forwarded-For`. Precedent: `backend/app/routers/dashboards.py:508`.
- **Config** is pydantic-settings: `backend/app/config.py` `Settings` class; new keys
  read from env automatically.
- **Scheduler** exists for the retention job: APScheduler `BackgroundScheduler` in
  `backend/app/scheduler.py` (`ensure_scheduler_started` line 22,
  `schedule_all_jobs` line 67, `add_job(..., id=..., replace_existing=True)` pattern
  at line 105).
- Actor identity is currently an unauthenticated `actorId` query param. Spec
  `02-authentication-tokens-and-sessions` replaces this with a server-verified
  session/current-user dependency — the audit helper takes a plain `actor_id: str`
  so it works with either.
- Spec `07` (structured logging) is being drafted in parallel; if its logger module
  exists when this is implemented (`backend/app/logging_setup.py` or similar), the
  audit helper should ALSO emit a structured log line per event. If it does not
  exist yet, use stdlib `logging.getLogger("bayan.audit")` — do not block on 07.

## Desired State

- `audit_log` SQLite table (append-only at the app level: no update/delete endpoints,
  only a retention purge).
- One helper module `backend/app/audit.py` with a single `audit(...)` function that
  never raises and never blocks the business transaction.
- All endpoints listed above call `audit(...)` after their success commit; auth
  failures (bad login) are also recorded.
- Admin-only read API: `GET /api/admin/audit-logs` with filters + pagination.
- Daily retention purge (default 365 days, configurable via `AUDIT_RETENTION_DAYS`).
- **Never** store secrets: no passwords, hashes, connection URIs (plain or encrypted),
  share tokens, or embed tokens in `details_json`. Record field names only
  (e.g. `{"changed": ["connectionUri"]}`).

## Implementation Plan

1. **Model** — add to `backend/app/models.py` (near `UserNotification`, ~line 148):

   ```python
   class AuditLog(Base):
       __tablename__ = "audit_log"

       id: Mapped[str] = mapped_column(String, primary_key=True)
       ts: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), index=True)
       actor_id: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)
       actor_email: Mapped[Optional[str]] = mapped_column(String, nullable=True)  # snapshot at event time
       action: Mapped[str] = mapped_column(String, nullable=False, index=True)     # dot-namespaced, see step 3
       target_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)  # 'user'|'datasource'|'dashboard'|'share'|'system'
       target_id: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)
       ip: Mapped[Optional[str]] = mapped_column(String, nullable=True)
       user_agent: Mapped[Optional[str]] = mapped_column(String, nullable=True)
       status: Mapped[str] = mapped_column(String, nullable=False, default="success")  # 'success'|'failure'
       details_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)   # JSON: sanitized before/after/extra
   ```

   `init_db()` needs no new migration code — `create_all` creates the table. Add one
   line inside the existing `try` block of `init_db()` for index safety on upgraded
   DBs is NOT needed (new table, mapped indexes are created by `create_all`).

2. **Helper** — new file `backend/app/audit.py`:

   ```python
   from __future__ import annotations
   import json
   import logging
   from typing import Optional, Any
   from uuid import uuid4
   from fastapi import Request

   from .models import SessionLocal, AuditLog, User

   log = logging.getLogger("bayan.audit")  # replace with spec-07 logger if present

   # ponytail: fields stripped by name; add nested-path redaction only if details ever nest secrets
   _REDACT = {"password", "newPassword", "oldPassword", "connectionUri", "token", "password_hash", "connection_encrypted", "api_key", "apiKey"}

   def _sanitize(d: Any) -> Any:
       if isinstance(d, dict):
           return {k: ("[REDACTED]" if k in _REDACT else _sanitize(v)) for k, v in d.items()}
       if isinstance(d, list):
           return [_sanitize(v) for v in d]
       return d

   def audit(
       action: str,
       actor_id: Optional[str] = None,
       target_type: Optional[str] = None,
       target_id: Optional[str] = None,
       request: Optional[Request] = None,
       status: str = "success",
       details: Optional[dict] = None,
   ) -> None:
       """Append one audit row. Own session; never raises."""
       try:
           db = SessionLocal()
           try:
               email = None
               if actor_id:
                   u = db.query(User).filter(User.id == actor_id).first()
                   email = u.email if u else None
               row = AuditLog(
                   id=str(uuid4()),
                   actor_id=actor_id,
                   actor_email=email,
                   action=action,
                   target_type=target_type,
                   target_id=target_id,
                   ip=(request.client.host if request and request.client else None),
                   user_agent=(request.headers.get("user-agent") if request else None),
                   status=status,
                   details_json=(json.dumps(_sanitize(details)) if details else None),
               )
               db.add(row)
               db.commit()
           finally:
               db.close()
           log.info("audit %s actor=%s target=%s/%s status=%s", action, actor_id, target_type, target_id, status)
       except Exception:
           log.exception("audit write failed for action=%s", action)
   ```

   Uses its own `SessionLocal` so an audit failure can never roll back the business
   transaction, and vice versa. Call it AFTER the endpoint's own `db.commit()`.

3. **Action taxonomy** (string constants, just use literals at call sites):

   | action | where |
   |---|---|
   | `auth.login.success` / `auth.login.failure` | users.py `login` (227) — failure logged before raising 401, with `details={"email": email}` |
   | `auth.signup` | users.py `signup` (212) |
   | `auth.password.change` | users.py `change_password` (241) |
   | `auth.password.reset_request` | users.py `request_password_reset` (255) — log even when user not found (`status="failure"`, details `{"email": ...}`); do not change the anti-enumeration response |
   | `auth.password.reset_confirm` | users.py `confirm_password_reset` (286) |
   | `user.password.admin_set` | users.py `reset_password` (301) and `admin_set_password` (376) |
   | `user.create` | users.py `admin_create_user` (346), `bootstrap_admin` (315) with `details={"role": ..., "bootstrap": true}` |
   | `user.set_active` | users.py `admin_set_active` (364), `details={"active": bool}` |
   | `datasource.create` | datasources.py `create_ds` (97), `details={"name":..., "type":..., "hasCredentials": bool(enc)}` |
   | `datasource.update` | datasources.py `patch_ds` (341), `details={"changed": [names of non-None payload fields]}` — never the values |
   | `datasource.delete` | datasources.py `delete_ds` (2594) |
   | `datasource.share.grant` / `datasource.share.revoke` | datasources.py `add_ds_share` (388) / `remove_ds_share` (415), `details={"targetUserId":..., "permission":...}` |
   | `datasource.export` | datasources.py `export_datasources` (2630) and `export_datasource` (2671), `details={"ids": [...], "includesCredentials": true}` |
   | `datasource.import` | datasources.py `import_datasources` (2705), `details={"count": n}` |
   | `dashboard.create` / `dashboard.update` | dashboards.py `save_dash` (236) — branch on `payload.id` |
   | `dashboard.delete` | dashboards.py `delete_dash` (422) |
   | `dashboard.share.grant` | users.py `add_dashboard_collection` (89) when `payload.permission` is set, and models-level grants stay untouched — instrument the endpoint, not `grant_share_permission` (it's also called from dashboards flows; endpoint-level gives correct actor) |
   | `dashboard.share.revoke` | dashboards.py `delete_share` (384) |
   | `dashboard.publish` / `dashboard.unpublish` | dashboards.py `publish_dash` (441) / `unpublish_dash` (457), publish details `{"publicId": ...}` |
   | `dashboard.publish.token_set` | dashboards.py `set_publish_token` (484), `details={"protected": bool}` — never the token |
   | `dashboard.embed_token.create` / `.revoke` | dashboards.py `create_embed_token` (405) / `delete_embed_token` (341) |
   | `dashboard.export` / `dashboard.import` | dashboards.py 594/653/703, export details `{"includeDatasources": bool}` |
   | `admin.duckdb.set_active` | admin.py `duckdb_set_active` (160), `details={"path": path_set}` |
   | `admin.branding.update` / `admin.branding.reset` | admin.py 190 / 231 |
   | `admin.scheduler.refresh` | admin.py 45 |

4. **Instrumentation mechanics** (repeat per endpoint, ~2 lines each):
   - Add `request: Request` parameter to endpoints that lack it (FastAPI injects it;
     import `Request` from `fastapi`). `dashboards.py` already imports it.
   - `from ..audit import audit` in each router.
   - Actor id: use the endpoint's existing `actorId` / `payload.userId` resolution
     (after spec 02 lands, switch to the current-user dependency it introduces —
     keep the `audit()` call unchanged, only the actor variable changes).
   - Place the call after the success commit / just before `return`. For
     `login` failure, call before `raise HTTPException(401)`.

5. **Read API** — append to `backend/app/routers/admin.py` (reuse `_is_admin`, line 31):

   ```python
   @router.get("/audit-logs")
   async def audit_logs(
       actorId: str | None = Query(default=None),
       action: str | None = Query(default=None),        # prefix match, e.g. "auth."
       actor: str | None = Query(default=None),          # filter by actor_id or actor_email
       targetId: str | None = Query(default=None),
       since: str | None = Query(default=None),          # ISO datetime
       until: str | None = Query(default=None),
       limit: int = Query(default=100, le=1000),
       offset: int = Query(default=0),
       db: Session = Depends(get_db),
   ):
       if not _is_admin(db, actorId):
           raise HTTPException(status_code=403, detail="Forbidden")
       q = db.query(AuditLog)
       if action: q = q.filter(AuditLog.action.like(action + "%"))
       if actor: q = q.filter((AuditLog.actor_id == actor) | (AuditLog.actor_email == actor))
       if targetId: q = q.filter(AuditLog.target_id == targetId)
       if since: q = q.filter(AuditLog.ts >= datetime.fromisoformat(since))
       if until: q = q.filter(AuditLog.ts <= datetime.fromisoformat(until))
       total = q.count()
       rows = q.order_by(AuditLog.ts.desc()).offset(offset).limit(limit).all()
       return {"total": total, "items": [
           {"id": r.id, "ts": r.ts.isoformat() + "Z", "actorId": r.actor_id,
            "actorEmail": r.actor_email, "action": r.action, "targetType": r.target_type,
            "targetId": r.target_id, "ip": r.ip, "userAgent": r.user_agent,
            "status": r.status, "details": json.loads(r.details_json or "null")}
           for r in rows]}
   ```

   Import `AuditLog` in admin.py's models import (line 9) and `datetime` is already
   imported (line 4). No update/delete endpoints — append-only.

6. **Retention** — add to `backend/app/config.py` `Settings`:
   `audit_retention_days: int = Field(default=365, validation_alias=AliasChoices("AUDIT_RETENTION_DAYS"))`.
   In `backend/app/scheduler.py` `schedule_all_jobs()` (line 67), after the sync-job
   upsert loop, register a static daily job:

   ```python
   def _purge_audit_logs() -> None:
       from .models import SessionLocal, AuditLog
       from .config import settings
       from datetime import datetime, timedelta
       db = SessionLocal()
       try:
           cutoff = datetime.utcnow() - timedelta(days=int(settings.audit_retention_days))
           db.query(AuditLog).filter(AuditLog.ts < cutoff).delete(synchronize_session=False)
           db.commit()
       finally:
           db.close()

   sched.add_job(func=_purge_audit_logs, trigger=CronTrigger.from_crontab("30 3 * * *", timezone=_SCHEDULER_TZ),
                 id="audit:purge", replace_existing=True, coalesce=True, misfire_grace_time=3600)
   ```

   The purge itself is deliberately NOT audited (noise); it logs via
   `logging.getLogger("bayan.audit")`.

7. **Self-check** — add `backend/tests/test_audit.py` (pytest exists?
   If no test harness in repo, add a `python -m app.audit` style check instead):
   minimal test that (a) `audit("test.event", actor_id="x", details={"password": "s3cret", "name": "ok"})`
   writes a row, (b) `details_json` contains `"[REDACTED]"` and not the secret,
   (c) a second call with a failing DB (monkeypatch `SessionLocal` to raise) does
   not raise.

## Files to Modify

- `backend/app/models.py` — add `AuditLog` model (new table, created by `create_all`).
- `backend/app/audit.py` — **new**: `audit()` helper + `_sanitize()`.
- `backend/app/routers/users.py` — instrument 10 endpoints (login success+failure, signup, change/reset password x4, bootstrap, admin create/set-active/set-password, share via collections).
- `backend/app/routers/datasources.py` — instrument create/patch/delete/shares/export/import (9 call sites; `put_ds` needs nothing, it delegates to `patch_ds`).
- `backend/app/routers/dashboards.py` — instrument save/delete/shares/publish/unpublish/token/embed-tokens/export/import (11 call sites).
- `backend/app/routers/admin.py` — instrument 4 admin endpoints; add `GET /admin/audit-logs`.
- `backend/app/config.py` — add `audit_retention_days` setting.
- `backend/app/scheduler.py` — add `_purge_audit_logs` + `audit:purge` daily job in `schedule_all_jobs`.
- `backend/tests/test_audit.py` — **new**: sanitization + no-raise test.

## Acceptance Criteria

- [ ] `audit_log` table exists after startup on a fresh AND an existing `.data/meta.sqlite` (no manual migration).
- [ ] Successful login writes `auth.login.success` with actor_id, actor_email, ip, user_agent; failed login writes `auth.login.failure` with the attempted email in details.
- [ ] Datasource create/update/delete/share/export each write a row; `details_json` for datasource events NEVER contains a connection URI (plain or encrypted).
- [ ] Dashboard share grant/revoke, publish/unpublish, embed-token create/revoke, export/import each write a row; publish-token event stores only `protected: bool`, never the token.
- [ ] Admin user management (create, set-active, set-password, direct reset) each write a row with target user id.
- [ ] `GET /api/admin/audit-logs` returns 403 for non-admin `actorId`; supports `action` prefix, `actor`, `targetId`, `since`/`until`, `limit`/`offset`; newest first.
- [ ] No API exists to update or delete individual audit rows.
- [ ] An audit-write failure (e.g. locked DB) does not fail the business request.
- [ ] `audit:purge` job appears in `GET /api/admin/scheduler/jobs` and deletes rows older than `AUDIT_RETENTION_DAYS` (default 365).
- [ ] Grep guard: `grep -rn "connectionUri\|password" backend/app/audit.py` shows them only inside the `_REDACT` set.

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/backend
# 1) Start backend (uses .env; SECRET_KEY present in backend/.env)
uvicorn app.main:app --port 8000 &

# 2) Failed + successful login
curl -s -X POST localhost:8000/api/users/login -H 'Content-Type: application/json' \
  -d '{"email":"nobody@example.com","password":"wrong"}'          # expect 401
curl -s -X POST localhost:8000/api/users/login -H 'Content-Type: application/json' \
  -d '{"email":"<admin-email>","password":"<admin-pass>"}'        # expect 200, note the id

# 3) Read audit log as admin
curl -s "localhost:8000/api/admin/audit-logs?actorId=<admin-id>&action=auth." | python3 -m json.tool
# expect both auth.login.failure and auth.login.success entries with ip set

# 4) Non-admin blocked
curl -s -o /dev/null -w '%{http_code}' "localhost:8000/api/admin/audit-logs?actorId=bogus"   # 403

# 5) Secret redaction: create a datasource then inspect
curl -s -X POST localhost:8000/api/datasources -H 'Content-Type: application/json' \
  -d '{"name":"t","type":"duckdb","connectionUri":"sekret://x","userId":"<admin-id>"}'
sqlite3 .data/meta.sqlite "SELECT action, details_json FROM audit_log WHERE action LIKE 'datasource.%';"
# details must not contain 'sekret'

# 6) Purge job registered
curl -s "localhost:8000/api/admin/scheduler/jobs?actorId=<admin-id>" | grep audit:purge

# 7) Unit test
pytest backend/tests/test_audit.py -q
```

## Out of Scope

- Frontend admin UI page for browsing the audit log (P1 follow-up; the read API is designed for it).
- Cryptographic tamper-evidence (hash chaining) / shipping to an external SIEM — revisit if a compliance requirement names it.
- Auditing read-only data queries (`/api/query/*`) — high volume, covered by metrics; only exports are audited.
- Replacing `actorId` query-param auth — that is spec `02-authentication-tokens-and-sessions`; this spec only consumes whatever actor identity is available.
- Auditing sync runs (already recorded in `sync_runs`, models.py:785) and alert runs (`alert_runs`, models.py:848).
