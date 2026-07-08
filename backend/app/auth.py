from __future__ import annotations

from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session

from .models import SessionLocal, User
from .security import verify_session_token, _pw_fingerprint
from .config import settings


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


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


def actor_id_optional(request: Request, user: User | None = Depends(get_current_user_optional)) -> str | None:
    """Resolve the acting identity for endpoints that previously trusted a raw
    ?actorId= query param. Drop-in replacement for that param's default.

    - Authenticated (token, or a legacy actorId that resolves to a real active
      user): return that user's id.
    - auth_enforce OFF and no resolved user: pass the raw ?actorId= through
      unchanged, so the migration window is fully non-breaking — including
      server-initiated snapshot/scheduler jobs and the "dev_user" default that
      may not correspond to a real User row.
    - auth_enforce ON and unauthenticated: return None (a forged actorId grants
      nothing).
    """
    if user:
        return user.id
    if not settings.auth_enforce:
        return (request.query_params.get("actorId") or None)
    return None


def get_current_user(user: User | None = Depends(get_current_user_optional)) -> User:
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if (user.role or "user").lower() != "admin":
        raise HTTPException(status_code=403, detail="Admin required")
    return user
