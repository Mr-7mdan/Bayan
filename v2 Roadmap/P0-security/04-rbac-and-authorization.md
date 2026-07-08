---
id: 04-rbac-and-authorization
title: Role-based access control and per-resource authorization
priority: P0
effort: L
depends_on: ['02-authentication-tokens-and-sessions']
area: backend
---

## Problem

Authorization is ad-hoc and full of holes. Roles are a bare string (`admin`|`user`), the "actor" is a client-supplied `actorId` query param (trivially spoofable — spec 02 fixes identity; this spec fixes what an authenticated identity is *allowed to do*), `_is_admin` is copy-pasted into 5+ files with inconsistent semantics, and several routers perform **zero** authorization: any caller can list/edit/delete every contact, alert rule, holiday rule, and another user's favorites/collections.

## Current State

All paths relative to `/Users/mohammed/Documents/Bayan`.

**Role model** — `backend/app/models.py:30`:
```python
role: Mapped[str] = mapped_column(String, nullable=False, default="user")  # 'admin' | 'user'
```

**Share models** — `backend/app/models.py:73-81` (`DatasourceShare`, `permission` = `'ro'|'rw'`), `backend/app/models.py:85-93` (`SharePermission` for dashboards, `permission` = `'ro'|'rw'`). Helper `get_share_permission(db, dashboard_id, user_id)` at `models.py:693`.

**Duplicated admin checks** (same logic, 5 copies):
- `backend/app/routers/admin.py:31` `_is_admin`
- `backend/app/routers/ai.py:119` `_is_admin` (defined *inside* the endpoint function)
- `backend/app/routers/dashboards.py:80` `_is_admin` (also grants admin to `settings.snapshot_actor_id` at lines 84-86)
- `backend/app/routers/datasources.py:232` `_is_admin`
- `backend/app/routers/users.py:63` `_require_admin`
- Inline copies in `backend/app/routers/updates.py:210-216, 392-394` and `backend/app/routers/query.py:2385-2389`.

**Routers with NO authorization at all:**
- `backend/app/routers/contacts.py` — all 10 endpoints (`grep actorId contacts.py` → zero hits). `Contact.user_id` exists (`models.py:40`) but is never read or written. Anyone can export all contacts (`contacts.py:238`) or mass-send email/SMS (`contacts.py:248,356`).
- `backend/app/routers/alerts.py:138-208` — full CRUD on `AlertRule` with no actor. `AlertRule` (`models.py:799-813`) has **no `user_id` column**, so ownership cannot even be expressed.
- `backend/app/routers/holidays.py` — full CRUD + upload, no actor.
- `backend/app/routers/users.py:70,89,142,149,185,389,414,424` — favorites/collections/notifications/counts keyed by `{user_id}` **path param** with no check that the caller is that user.
- `backend/app/routers/alerts.py:2859-2916` — email/SMS config GET/PUT (global secrets) with no admin gate; `alerts.py:2942,2951` test-email/test-sms unauthenticated.
- `backend/app/routers/date_presets.py:98`, `periods.py:80`, `metrics.py:16,24`, `issues.py:195,248`, `snapshot.py:207`, `updates.py:95,164` — no actor (mostly low-risk compute/info, but `snapshot.py:207` renders any widget for any caller by defaulting `actor_id` to `settings.snapshot_actor_id`).

**Optional enforcement (auth only when actor volunteered):**
- `backend/app/routers/query.py:713` `_engine_for_datasource(db, datasource_id, actor_id=None)` — docstring says "if actor_id is None, no enforcement". Every `/api/query*` endpoint is open by omitting `actorId`.
- `backend/app/routers/datasources.py:273-300` `list_ds` — falls back to listing `dev_user` datasources when no uid.
- `backend/app/routers/dashboards.py:236-268` `save_dash` — actor defaults to `"dev_user"` (`dashboards.py:244`).

**Router mounting** — `backend/app/main.py:178-192`, all under `/api` prefix, no router-level dependencies.

**Spec 02 contract assumed** (dependency): `backend/app/auth.py` provides `get_current_user` (FastAPI dependency → returns active `User` or raises 401) and `get_current_user_optional` (returns `User | None`, for public/embed paths). This spec builds on those; it does not re-implement authentication.

## Desired State

- One module `backend/app/authz.py` owning: role constants, `require_user` / `require_admin` dependencies, and per-resource permission functions for dashboards, datasources, alerts, contacts.
- Roles stay `admin` | `user` (no schema change to `users.role`). Viewer/editor distinction is already carried by `ro`/`rw` share rows — formalized as `Permission` enum `NONE < VIEW < EDIT < OWNER < ADMIN`.
- Every endpoint in every router has an explicit authorization requirement (matrix below). No endpoint trusts a client-supplied `actorId`/`userId` as identity.
- `AlertRule` and `Contact` gain enforced ownership (`user_id` set on create, filtered on list, checked on mutate).
- Global config endpoints (email/SMS/AI config, branding, scheduler, updates, holidays, duckdb toggles) are admin-only via one shared dependency.
- Public/embed paths (`/dashboards/public/{public_id}`, embed-token query flows, snapshot service account) keep working, but the snapshot bypass is server-side only (spec 02's service token), never a query-param actor.

## Implementation Plan

### Step 1 — `backend/app/authz.py` (new, ~120 lines)

```python
from enum import IntEnum
from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session
from .models import User, Dashboard, Datasource, AlertRule, Contact, DatasourceShare, get_share_permission
from .auth import get_current_user, get_current_user_optional  # from spec 02

ROLE_ADMIN = "admin"
ROLE_USER = "user"

class Permission(IntEnum):
    NONE = 0; VIEW = 1; EDIT = 2; OWNER = 3; ADMIN = 4

def is_admin(user: User | None) -> bool:
    return bool(user and (user.role or ROLE_USER).lower() == ROLE_ADMIN)

def require_admin(user: User = Depends(get_current_user)) -> User:
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="Admin required")
    return user

def dashboard_permission(db: Session, user: User | None, dash: Dashboard) -> Permission:
    if is_admin(user): return Permission.ADMIN
    if not user: return Permission.NONE
    if not dash.user_id or dash.user_id == user.id: return Permission.OWNER  # legacy NULL-owner rows: treat as owned by requester (matches current behavior at dashboards.py:277)
    perm = get_share_permission(db, dash.id, user.id)  # models.py:693
    return {"rw": Permission.EDIT, "ro": Permission.VIEW}.get(perm or "", Permission.NONE)

def datasource_permission(db: Session, user: User | None, ds: Datasource) -> Permission:
    # same shape, reads DatasourceShare instead of SharePermission

def require_dashboard(db, user, dash_id, need: Permission) -> Dashboard:
    # 404 if missing, 403 if permission < need; returns the row

def require_datasource(db, user, ds_id, need: Permission) -> Datasource:
    # same

def require_owned(db, user, model, obj_id) -> object:
    # generic owner-or-admin fetch for AlertRule / Contact (user_id == user.id or admin)
```
Delete the five `_is_admin`/`_require_admin` copies and the inline checks in `updates.py`/`query.py:2385-2389`; import from `authz` instead. Move the snapshot-actor bypass out of `dashboards._is_admin` — spec 02's service-token identity resolves to a synthetic admin user for the snapshot service, so no query-param bypass remains.

### Step 2 — ownership columns (SQLite lightweight migration)

Follow the existing PRAGMA pattern in `init_db()` (`backend/app/models.py:186-232`):
- Add `user_id: Mapped[Optional[str]]` to `AlertRule` (`models.py:~805`).
- In `init_db()`: `PRAGMA table_info(alert_rules)`; if `user_id` missing → `ALTER TABLE alert_rules ADD COLUMN user_id TEXT`, then backfill: `UPDATE alert_rules SET user_id = (SELECT user_id FROM dashboards WHERE dashboards.id = alert_rules.dashboard_id) WHERE user_id IS NULL`.
- `Contact.user_id` already exists (`models.py:40`) — no migration, just start writing/filtering it. Backfill existing NULL contacts to the first admin user's id in the same `init_db()` block (visible to admins either way).
- Legacy NULL `user_id` rows on alerts/contacts after backfill: readable/writable by admin only.

### Step 3 — apply the matrix router by router

Replace every `actorId: str | None = Query(...)` with `user: User = Depends(get_current_user)` (or `require_admin`, or `get_current_user_optional` for the public rows). Delete `userId` query params used as identity; keep them only as admin-side filters (admin listing "as user X" stays: `dashboards.py:184`, `datasources.py:275`).

**Endpoint → permission matrix** (this is the whole audit; implementer applies mechanically):

| Router / endpoints | Requirement |
|---|---|
| `users.py` `/signup`, `/login`, `/request-password-reset`, `/confirm-password-reset`, `/reset-password`, `/bootstrap-admin` (315: keep existing empty-DB guard) | public |
| `users.py` `/change-password` (241) | authed; may only change own password |
| `users.py` `/{user_id}/counts,collections,notifications,collections/items,favorites` (70,89,142,149,185,389,414,424) | authed AND (`user.id == user_id` OR admin) — add this check, it does not exist today |
| `users.py` `/admin/*` (336,346,364,376) | `require_admin` (keep, port to dependency) |
| `dashboards.py` list/save/get/delete/export/import (175,236,269,422,594,653,703) | authed; VIEW to read, EDIT to save existing (owner check already at 247-249 — port to `require_dashboard`), OWNER to delete |
| `dashboards.py` shares & embed-tokens & publish (324,341,366,384,441,457,473,484) | OWNER (sharing/publishing is owner+admin only) |
| `dashboards.py` `/public/{public_id}` (504) and `/public/{public_id}/embed-token` (405) | public — keep existing `verify_share_link_token`/`verify_embed_token` gate (`models.py:541`, `security.py:143`) |
| `datasources.py` create (97) | authed; force `user_id = user.id` |
| `datasources.py` list/get (273,303) | authed; owner+shared+admin (logic exists at 273-300 — keep, drop the `dev_user` fallback) |
| `datasources.py` mutate: patch/put/activate/deactivate/delete/transforms PUT (340,429,459,479,2594,1751) | EDIT (owner, `rw` share, admin) |
| `datasources.py` shares (368,388,415) | OWNER |
| `datasources.py` sync-tasks/sync/local/* (500-1720, 2888-3134) | EDIT — these mutate the local store |
| `datasources.py` schema/tables/transforms GET/export (1724,1768,1857,1908,2043,2362,2630,2671) | VIEW |
| `datasources.py` `/engines/dispose-all` (453), engine dispose (435) | `require_admin` |
| `datasources.py` import (2705) | authed; force `user_id = user.id` |
| `query.py` all 8 endpoints (1705,2321,5663,6310,8511,8593,9711,9766) | authed OR valid public/embed token (`publicId`+`token` path already threaded through `run_query` at 1731); make `actor_id` **mandatory** in `_engine_for_datasource` (713) unless a verified public token is present — remove the "no actor → no enforcement" branch |
| `alerts.py` CRUD/run/runs (138-287) | authed; list → filter `user_id == user.id` unless admin; get/put/delete/run → `require_owned(AlertRule)`; create → set `user_id = user.id` |
| `alerts.py` evaluate/evaluate-v2 (319,561) | authed; datasource resolved inside must go through `_engine_for_datasource` with the real actor |
| `alerts.py` config email/sms + test-email/test-sms (2859,2876,2895,2903,2942,2951) | `require_admin` |
| `alerts.py` report-pdf (2918) | authed; VIEW on `dashboard_id` |
| `contacts.py` all (137,167,187,204,214,223,238,248,356,500) | authed; list/export filter `Contact.user_id == user.id` OR admin; create/import set `user_id = user.id`; mutate via `require_owned(Contact)`; send-email/send-sms require ownership of every referenced contact |
| `admin.py` all 7 (38,45,52,153,160,190,231) | `require_admin` — apply as router-level `dependencies=[Depends(require_admin)]` in `admin.py`'s `APIRouter(...)`, delete per-endpoint checks |
| `ai.py` config GET/PUT (109,117) | `require_admin` |
| `ai.py` describe/enhance/plan/suggest (276,314,357,435) | authed |
| `updates.py` version/check (95,164) | authed |
| `updates.py` apply/promote (204,383) | `require_admin` (replaces inline checks at 210-216, 392-394) |
| `holidays.py` GET (47) | authed |
| `holidays.py` POST/PUT/DELETE/upload (52,61,73,83) | `require_admin` (global config) |
| `date_presets.py` preview (98), `periods.py` resolve (80) | authed |
| `metrics.py` open/close (16,24) | authed (identity from token, not body) |
| `issues.py` report/test (195,248) | authed; stamp `userId` from token |
| `snapshot.py` `/widget` (207) | authed with VIEW on the dashboard, OR valid `publicId`+`token`, OR spec 02 snapshot service identity. Remove the unconditional `actor_id=actorId or settings.snapshot_actor_id` default (line 231) — that currently grants everyone the snapshot actor's access |

### Step 4 — frontend compatibility shim (backend-side, transitional)

Frontend still sends `actorId=`/`userId=` query params everywhere. Do NOT break it in this spec: keep the params in signatures where the frontend sends them but **ignore them for identity** (identity = spec 02 token). `userId` remains meaningful only as an admin filter. Mark each with `# ponytail: actorId param kept for old clients, identity comes from token; drop param in v2.1`.

### Step 5 — regression test

`backend/tests/test_authz.py` (new; follow existing test layout under `backend/tests/` if present, else create): using `fastapi.testclient` with two users (admin, user) + one foreign resource each for dashboard/datasource/alert/contact, assert:
- 401 with no token on every non-public route group (parametrize one representative endpoint per router).
- 403 for user on: another user's dashboard GET (no share), datasource PATCH (ro share), alert DELETE, contact PUT, `/api/admin/metrics-live`, `/api/holidays` POST, `/api/alerts/config/email` PUT, `/api/users/{other_id}/favorites`.
- 200 for: ro-share dashboard GET, rw-share dashboard POST save, admin on all of the above.
- `/api/dashboards/public/{public_id}` still 200 without auth when published.

### Phase 2 (explicitly deferred, design note only)

Add `viewer` as a third `users.role` value (read-only everywhere) and an `orgs`/`org_members` pair with `org_id` on Dashboard/Datasource for team scoping. `Permission` enum and `*_permission()` functions are the single choke point — Phase 2 only edits those. Do not build now.

## Files to Modify

- `backend/app/authz.py` — NEW: roles, `Permission`, `require_admin`, `dashboard_permission`, `datasource_permission`, `require_dashboard`, `require_datasource`, `require_owned`
- `backend/app/models.py` — `AlertRule.user_id` column; `init_db()` PRAGMA migration + backfill for `alert_rules.user_id` and NULL contacts
- `backend/app/routers/admin.py` — router-level `require_admin`; delete `_is_admin`
- `backend/app/routers/ai.py` — delete inline `_is_admin`; guards per matrix
- `backend/app/routers/alerts.py` — ownership on CRUD; admin on config; actor into evaluate paths
- `backend/app/routers/contacts.py` — ownership everywhere (currently none)
- `backend/app/routers/dashboards.py` — port checks to `require_dashboard`; delete `_is_admin` + snapshot-actor bypass
- `backend/app/routers/datasources.py` — port to `require_datasource`; delete `_is_admin`; drop `dev_user` fallback
- `backend/app/routers/query.py` — `_engine_for_datasource` mandatory actor-or-public-token; delete inline admin check
- `backend/app/routers/users.py` — self-or-admin on `{user_id}` routes; port `_require_admin`
- `backend/app/routers/updates.py`, `holidays.py`, `issues.py`, `metrics.py`, `snapshot.py`, `date_presets.py`, `periods.py` — guards per matrix
- `backend/tests/test_authz.py` — NEW

## Acceptance Criteria

- [ ] `grep -rn "def _is_admin\|def _require_admin" backend/app/routers/` returns nothing; all admin checks route through `authz.require_admin`/`authz.is_admin`
- [ ] Every endpoint in the matrix carries its listed requirement; no endpoint reads `actorId` as identity
- [ ] `alert_rules.user_id` exists after `init_db()` on an old DB and is backfilled from the linked dashboard's owner
- [ ] Contacts, alerts, holidays, users/{user_id} sub-resources return 401 without a token and 403 across users (non-admin)
- [ ] `_engine_for_datasource` raises 403 for a non-owner non-shared authenticated user and 401-path is unreachable without token or verified public/embed token
- [ ] Public dashboard (`/api/dashboards/public/{public_id}`) and embed-token flows work unauthenticated
- [ ] Snapshot rendering works via the spec 02 service identity; passing `actorId=<snapshot_actor_id>` as a query param grants nothing
- [ ] `backend/tests/test_authz.py` passes

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/backend
uv run pytest tests/test_authz.py -v          # or: python -m pytest
# migration check on a copy of the metadata DB:
python -c "from app.models import init_db, engine_meta; init_db(); \
import sqlalchemy as sa; \
print([r[1] for r in engine_meta.connect().execute(sa.text('PRAGMA table_info(alert_rules)'))])"
# manual smoke (server on :8000, spec 02 tokens in $ADMIN_TOK/$USER_TOK):
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8000/api/contacts                          # 401
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $USER_TOK" http://localhost:8000/api/admin/metrics-live   # 403
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $ADMIN_TOK" http://localhost:8000/api/admin/metrics-live  # 200
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8000/api/alerts                            # 401 (was 200)
```
Frontend regression: log in as non-admin in the UI (localhost:3000), confirm dashboards/datasources lists, save, and query still work; admin pages hidden/403 as before.

## Out of Scope

- Authentication itself (tokens, sessions, password flows) — spec 02
- `viewer` role, org/team scoping, per-widget permissions — Phase 2 (design note above)
- Frontend changes beyond continuing to send existing params (frontend token wiring is spec 02's frontend task)
- Rate limiting / audit logging
- Row-level data security inside DuckDB query results
