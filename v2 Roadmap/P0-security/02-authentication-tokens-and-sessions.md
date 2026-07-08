---
id: 02-authentication-tokens-and-sessions
title: Replace actorId-query-param auth with real token/session auth
priority: P0
effort: L
depends_on: []
area: fullstack
---

## Problem

There is no real authentication. `POST /api/users/login` verifies the password but issues no token or session — it just returns the user object. Every "authorization" check on the backend trusts a client-supplied `?actorId=<uuid>` query param. Anyone who knows (or guesses) another user's UUID can impersonate them, including admins, by editing a query string. Frontend "auth" is a `localStorage.auth_user` read with zero server-side route protection.

## Current State

All refs verified 2026-07-07 on branch `feature/alpha-themes-foundation`.

**Login issues no token** — `backend/app/routers/users.py:227-238`:
```python
@router.post("/login", response_model=UserOut)
def login(payload: LoginRequest, db: Session = Depends(get_db)) -> UserOut:
    ...
    return UserOut(id=u.id, name=u.name, email=u.email, role=u.role)
```
Note: login also never checks `u.active` — deactivated users can still log in.

**Authorization = trusted client query param**:
- `backend/app/routers/admin.py:31-35` — `_is_admin(db, actor_id)` looks up the user row for whatever UUID the client sent. Used at lines 39-55, 154, 161, 191, 232.
- `backend/app/routers/users.py:63-67` — `_require_admin(db, actor_id)`, same pattern. Used at 302-304, 337-338, 347-348, 365-366, 377-378. User-scoped endpoints (`/{user_id}/counts` :70, `/{user_id}/collections` :89, `/{user_id}/notifications` :142, `/{user_id}/favorites` :389-425) take `user_id` from the path with no auth at all.
- `backend/app/routers/dashboards.py` — its own `_is_admin` copy; `actorId` trusted at 179-184, 237-244 (`save_dash`: `actor = payload.userId or actorId or "dev_user"`), 270-276, 325-348, 367-391, 406-414, 423-428, 442-446, 458-462, 485-489, 595-624, 654-674, 704-737.
- `backend/app/routers/datasources.py` — `actorId` trusted at 274-280, 304-309, 369-394, 416, plus activate/deactivate/sync endpoints.
- `backend/app/routers/ai.py:118-124`, `backend/app/routers/alerts.py:320,562`, `backend/app/routers/snapshot.py:217-230`, `backend/app/routers/updates.py:207-216,388-397`, `backend/app/routers/query.py:1710,1731,2326,2345` (query.py also uses `actorId` for per-actor throttling at 421-711 and datasource access checks at 713-738).
- Routers with NO auth of any kind: `contacts.py`, `issues.py`, `holidays.py`, `date_presets.py`, `periods.py`, `metrics.py`.

**Existing reusable crypto** — `backend/app/security.py`:
- argon2id password hashing: `hash_password`/`verify_password`/`needs_rehash` (lines 41-85).
- HMAC-signed time-limited tokens already exist twice: `sign_reset_token`/`verify_reset_token` (89-119, payload `reset:{user_id}:{exp}`) and `sign_embed_token`/`verify_embed_token` (132-163). `_b64url_encode`/`_b64url_decode` helpers at 123-129. Secret is `settings.secret_key` (`backend/app/config.py:24`, default `"BayanSecretKey-CHANGE-ME"` — real value must be present in `backend/.env` in production; never quote it).

**Frontend**:
- `frontend/src/components/providers/AuthProvider.tsx:33-52` — reads/writes `auth_user` JSON in localStorage/sessionStorage; purely client-side.
- `frontend/src/lib/api.ts:361-` — single `http<T>()` wrapper; all requests flow through it (fetch calls at 417 and 476). No auth header. `Api.login` at 828-829 returns `UserOut`.
- `frontend/src/middleware.ts:1-16` — only rewrites `/` → `/home`, matcher `['/']`. No route protection.
- App routes under `frontend/src/app/(app)/*` (admin, dashboards, datasources, builder, users, alerts, contacts, home, about). Public routes: `/login`, `/logout`, `/reset-password`, `/v/[publicId]` (published dashboards), `/render/*` (embeds), `/themes`, `/demos`.

## Desired State

- Login returns an HMAC-signed, time-limited session token (same proven pattern as `sign_reset_token` — no new dependency, no JWT lib).
- A FastAPI dependency resolves the current `User` from `Authorization: Bearer <token>`; all admin checks and actor-identity resolution go through it.
- Tokens are stateless but self-invalidating: payload embeds a fingerprint of the password hash, so changing the password (or admin reset) kills existing tokens; `user.active` is re-checked on every request.
- Back-compat window controlled by `settings.auth_enforce` (default `false`): when off, endpoints fall back to the legacy `actorId` param so old clients keep working during rollout; when on, `actorId` is ignored for identity and unauthenticated requests get 401.
- Frontend stores the token, sends it on every `http()` call, mirrors it into a cookie, and `middleware.ts` redirects unauthenticated users to `/login` for all protected routes.

## Implementation Plan

### Backend

1. **`backend/app/security.py` — session token functions** (append after `verify_reset_token`, reuse `_b64url_encode/_b64url_decode`):
   ```python
   def _pw_fingerprint(password_hash: str) -> str:
       return hashlib.sha256((password_hash or "").encode()).hexdigest()[:12]

   def sign_session_token(user_id: str, password_hash: str, ttl_seconds: int) -> str:
       exp = int(time.time()) + ttl_seconds
       payload = f"sess:{user_id}:{exp}:{_pw_fingerprint(password_hash)}".encode()
       sig = hmac.new(settings.secret_key.encode(), payload, hashlib.sha256).digest()
       return f"{_b64url_encode(payload)}.{_b64url_encode(sig)}"

   def verify_session_token(token: str) -> Optional[tuple[str, str]]:
       """Returns (user_id, pw_fingerprint) if signature+expiry valid, else None."""
   ```
   `verify_session_token` mirrors `verify_reset_token` (lines 98-119): split on `.`, constant-time `hmac.compare_digest`, check prefix `sess`, check expiry, return `(user_id, fp)`. Payload split: `prefix, user_id, exp_s, fp = payload.split(":", 3)`.

2. **`backend/app/config.py` — settings** (next to `secret_key` at line 24):
   ```python
   auth_enforce: bool = Field(default=False)
   session_ttl_seconds: int = Field(default=60 * 60 * 24 * 30)  # 30 days
   ```

3. **`backend/app/auth.py` — NEW file, the single auth dependency module**:
   ```python
   from fastapi import Depends, HTTPException, Request
   from sqlalchemy.orm import Session
   from .models import SessionLocal, User
   from .security import verify_session_token, _pw_fingerprint
   from .config import settings

   def get_db(): ...  # same yield pattern as routers

   def get_current_user_optional(request: Request, db: Session = Depends(get_db)) -> User | None:
       auth = request.headers.get("authorization") or ""
       token = auth[7:] if auth.lower().startswith("bearer ") else request.cookies.get("bayan_session")
       if token:
           res = verify_session_token(token)
           if res:
               user_id, fp = res
               u = db.get(User, user_id)
               if u and bool(u.active) and _pw_fingerprint(u.password_hash) == fp:
                   return u
       if not settings.auth_enforce:  # legacy fallback during migration window
           actor = request.query_params.get("actorId")
           if actor:
               u = db.get(User, actor.strip())
               if u and bool(u.active):
                   return u
       return None

   def get_current_user(user: User | None = Depends(get_current_user_optional)) -> User:
       if not user:
           raise HTTPException(status_code=401, detail="Not authenticated")
       return user

   def require_admin(user: User = Depends(get_current_user)) -> User:
       if (user.role or "user").lower() != "admin":
           raise HTTPException(status_code=403, detail="Admin required")
       return user
   ```
   ponytail: stateless HMAC tokens, no session table; add a `sessions` table only if per-device revocation is ever required.

4. **`backend/app/schemas.py`** — add `token: str | None = None` to `UserOut` (line 383). Keeps `response_model=UserOut` and the frontend `Api.login` return type stable.

5. **`backend/app/routers/users.py` — login/signup issue tokens** (lines 212-238):
   - In `login`: reject inactive users (`if not bool(u.active): raise HTTPException(403, "Account disabled")`), then `return UserOut(..., token=sign_session_token(u.id, u.password_hash, settings.session_ttl_seconds))`. Do the rehash BEFORE signing so the fingerprint matches the stored hash.
   - Same token issuance in `signup` (:212) and `bootstrap_admin` (:315).
   - `change_password` (:241): derive user from `Depends(get_current_user)` instead of trusting `payload.userId`; return a fresh token in the response so the client can swap it (old tokens die via fingerprint).

6. **Replace admin checks with the dependency** (mechanical sweep — delete local `_is_admin`/`_require_admin` and the `actorId` param from each signature, add `admin: User = Depends(require_admin)`):
   - `users.py`: 302, 337, 347, 365, 377 (delete `_require_admin` at 63).
   - `admin.py`: 39, 46, 53, 154, 161, 191, 232 (delete `_is_admin` at 31).
   - `ai.py`: 118.
   - Keep accepting-and-ignoring a leftover `actorId` query param is unnecessary — FastAPI ignores unknown query params by default; just remove it.

7. **Replace actor-identity resolution** (endpoints where `actorId` is the identity, not just an admin gate). Pattern: add `user: User | None = Depends(get_current_user_optional)`, then `actor = user.id if user else None`, and where the old code fell back to `"dev_user"` keep that fallback ONLY when `not settings.auth_enforce`:
   - `dashboards.py`: 179, 237, 270, 325, 342, 367, 385, 406, 423, 442, 458, 485, 595, 654, 704. Keep its `_is_admin` call sites but feed them `user` (`is_admin = bool(user and user.role == "admin")`). In `save_dash` (:237-244) stop trusting `payload.userId` when enforcing.
   - `datasources.py`: 274, 304, 369, 389, 416 and the activate/deactivate/sync/status endpoints (grep `actorId` in the file for the full list).
   - `query.py`: endpoints at 1705-1710 and 2321-2326 (also `/distinct` :5663, `/pivot` :6310, `/period-totals*` :8593+ if they take `actorId`) — resolve `actorId = user.id if user else None` before passing into `run_query`/`run_query_spec`. IMPORTANT: preserve the unauthenticated public path — requests carrying valid `publicId` + embed `token` (verified via `verify_embed_token`) must keep working with `user=None`; do not put `get_current_user` (the raising variant) on query endpoints.
   - `alerts.py`: 320, 562. `snapshot.py`: 217. `updates.py`: 207, 388 (its inline user lookup at 211-216 becomes the dependency).
   - `users.py` user-scoped endpoints (:70, :89, :142, :149, :185, :389, :414, :424): when `settings.auth_enforce` and `user_id` path param != current user id and current user is not admin → 403.

8. **Do NOT touch in this pass**: `contacts.py`, `issues.py`, `holidays.py`, `date_presets.py`, `periods.py`, `metrics.py` have no auth today; adding `Depends(get_current_user)` to them is desirable but belongs in the enforcement flip (step 12) after the frontend sends tokens, otherwise they break mid-migration.

### Frontend

9. **`frontend/src/lib/api.ts`** — token storage + header:
   ```ts
   export function getAuthToken(): string | null {
     try { return localStorage.getItem('auth_token') || sessionStorage.getItem('auth_token') } catch { return null }
   }
   ```
   In `http()` (line ~373 where `headers` is built): `const tok = typeof window !== 'undefined' ? getAuthToken() : null; if (tok && !headers['Authorization']) headers['Authorization'] = 'Bearer ' + tok`. On `res.status === 401` (both fetch branches, ~435 and ~494): clear stored token/user, and if `window.location.pathname` is not under `/login`, `/v/`, `/render/`, redirect to `/login`.

10. **`frontend/src/components/providers/AuthProvider.tsx`**:
    - `login()` (:43-52): `Api.login` now returns `token` — store it in the same storage as `auth_user` (`localStorage` when remember, else `sessionStorage`) under `auth_token`, and mirror to a cookie for middleware gating: `document.cookie = 'bayan_session=' + token + '; path=/; SameSite=Lax; max-age=' + (remember ? 2592000 : '')` (session cookie when not remembering).
    - `logout()` (:54-60): remove `auth_token` from both storages and expire the cookie (`max-age=0`).
    - ponytail: non-httpOnly cookie because frontend (:3000) and backend (:8000) are separate origins so the backend can't set a cookie the Next middleware sees; the token already lives in web storage, so XSS exposure is unchanged. Upgrade path: proxy `/api` through Next and switch to httpOnly.

11. **`frontend/src/middleware.ts`** — server-side route gating (presence check only; the API verifies authoritatively):
    ```ts
    const PUBLIC = [/^\/login/, /^\/logout/, /^\/reset-password/, /^\/v\//, /^\/render\//, /^\/themes/, /^\/demos/]
    export function middleware(req: NextRequest) {
      const { pathname } = req.nextUrl
      if (pathname === '/') { /* keep existing '/'→'/home' redirect */ }
      if (PUBLIC.some(r => r.test(pathname))) return NextResponse.next()
      if (!req.cookies.get('bayan_session')) {
        const url = req.nextUrl.clone()
        url.pathname = '/login'
        url.searchParams.set('next', pathname)
        return NextResponse.redirect(url)
      }
      return NextResponse.next()
    }
    export const config = { matcher: ['/((?!_next|api|favicon.ico|.*\\..*).*)'] }
    ```
    Login page: after successful login, honor `?next=` redirect.

### Enforcement flip (separate deploy, after frontend ships)

12. Set `AUTH_ENFORCE=true` in `backend/.env`. This kills the `actorId` fallback in `auth.py`. Then (same PR or follow-up): add `Depends(get_current_user)` to the six unauthenticated routers listed in step 8, and delete remaining `actorId` query params + `"dev_user"` fallbacks. Keep `settings.snapshot_actor_id` for internal scheduler/snapshot jobs (`alerts.py:512`, `snapshot.py:230`) — those are server-initiated, not client requests.

### Phase 2 (follow-on spec, depends on this one): SSO / OIDC

Not in this spec's scope; recorded so the token layer is designed for it:
- Add OIDC Authorization-Code flow (library: `authlib` — the only new dependency, added then): `GET /api/auth/oidc/login` → redirect to IdP, `GET /api/auth/oidc/callback` → validate ID token, upsert `User` by email, then issue the SAME Bayan session token from step 1. The session layer built here is the integration point; SSO only replaces the credential check.
- Config keys: `oidc_issuer`, `oidc_client_id`, `oidc_client_secret`, `oidc_redirect_uri` in `config.py` + `backend/.env`.
- SAML only if a customer demands it (via `python3-saml`); prefer OIDC.

## Files to Modify

- `backend/app/security.py` — add `_pw_fingerprint`, `sign_session_token`, `verify_session_token`
- `backend/app/auth.py` — NEW: `get_current_user_optional`, `get_current_user`, `require_admin`
- `backend/app/config.py` — add `auth_enforce`, `session_ttl_seconds`
- `backend/app/schemas.py` — `UserOut.token: str | None = None`
- `backend/app/routers/users.py` — issue tokens on login/signup/bootstrap; active check; admin deps; path-user ownership checks
- `backend/app/routers/admin.py` — replace `_is_admin` with `require_admin` dep (7 endpoints)
- `backend/app/routers/dashboards.py` — actor from dep (15 endpoints)
- `backend/app/routers/datasources.py` — actor from dep
- `backend/app/routers/ai.py` — `require_admin` on config write
- `backend/app/routers/alerts.py`, `snapshot.py`, `updates.py`, `query.py` — actor from dep; preserve publicId/embed-token public path in query.py
- `frontend/src/lib/api.ts` — Authorization header in `http()`, 401 handling, `getAuthToken`
- `frontend/src/components/providers/AuthProvider.tsx` — store token + cookie on login, clear on logout
- `frontend/src/middleware.ts` — route gating + expanded matcher
- `frontend/src/app/login/*` — honor `?next=` redirect
- `backend/.env` — later: `AUTH_ENFORCE=true` (SECRET_KEY must already be set to a non-default value; verify, don't quote)

## Acceptance Criteria

- [ ] `POST /api/users/login` returns `token`; inactive users get 403
- [ ] Requests with `Authorization: Bearer <token>` resolve the correct user; expired/tampered tokens are rejected
- [ ] Changing a user's password (self, admin set-password, or reset flow) invalidates all previously issued tokens for that user
- [ ] With `AUTH_ENFORCE=true`: admin endpoints return 401 without a token and 403 with a non-admin token; `?actorId=<admin-uuid>` alone grants nothing
- [ ] With `AUTH_ENFORCE=false` (default): legacy `actorId` clients still work (migration window)
- [ ] Published dashboards (`/v/{publicId}`) and embed-token widget rendering still work with no login
- [ ] Frontend sends the token on every API call through `http()`; a 401 clears state and lands on `/login`
- [ ] Navigating to any protected route (e.g. `/home`, `/admin/metrics`) without the `bayan_session` cookie redirects to `/login?next=...`
- [ ] `/login`, `/reset-password`, `/v/*`, `/render/*` reachable while logged out
- [ ] No secret values appear in code, spec, or logs

## Verification

```bash
# Backend up (uvicorn on :8000). 1) Login issues token:
TOK=$(curl -s -X POST http://localhost:8000/api/users/login -H 'Content-Type: application/json' \
  -d '{"email":"<admin-email>","password":"<pw>"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')

# 2) Token works on an admin endpoint:
curl -s -H "Authorization: Bearer $TOK" http://localhost:8000/api/users/admin/list | head -c 200

# 3) Tampered token rejected (expect 401 when AUTH_ENFORCE=true):
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer ${TOK}x" http://localhost:8000/api/users/admin/list

# 4) actorId impersonation dead when enforcing (expect 401/403):
curl -s -o /dev/null -w '%{http_code}\n' "http://localhost:8000/api/users/admin/list?actorId=<any-admin-uuid>"

# 5) Password change kills token: change pw, then repeat step 2 → 401.

# 6) Public dashboard unaffected: open http://localhost:3000/v/<publicId> logged out → renders.

# 7) Middleware gating: clear cookies, open http://localhost:3000/home → redirected to /login?next=/home.

# 8) Frontend build clean:
cd /Users/mohammed/Documents/Bayan/frontend && npx tsc --noEmit && npm run build
```

Minimal backend check (add as `backend/tests/test_auth_tokens.py` if a tests dir exists, else run inline):
```python
from app.security import sign_session_token, verify_session_token, _pw_fingerprint
t = sign_session_token("u1", "$argon2id$fakehash", 60)
uid, fp = verify_session_token(t)
assert uid == "u1" and fp == _pw_fingerprint("$argon2id$fakehash")
assert verify_session_token(t[:-2] + "xx") is None          # tampered sig
assert verify_session_token(sign_session_token("u1", "h", -10)) is None  # expired
```

## Out of Scope

- SSO/OIDC/SAML implementation (Phase 2 spec; this spec only ensures the session layer it plugs into)
- Refresh tokens / sliding expiry / per-device session revocation (stateless fingerprint invalidation is the ceiling here; add a `sessions` table if revocation UX is demanded)
- Adding auth to the six currently-open routers (`contacts`, `issues`, `holidays`, `date_presets`, `periods`, `metrics`) — done at the enforcement flip, step 12
- httpOnly cookie via Next `/api` proxy (upgrade path noted in step 10)
- Rate limiting / lockout on the login endpoint (separate P0 spec if not already covered)
- Object-level permission redesign (dashboard/datasource sharing model stays as-is)
