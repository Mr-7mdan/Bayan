from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from ..config import settings
from ..models import User, SessionLocal
from sqlalchemy.orm import Session

router = APIRouter(prefix="/updates", tags=["updates"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class VersionOut(BaseModel):
    backend: Optional[str] = None
    frontend: Optional[str] = None


class UpdateAsset(BaseModel):
    url: str
    sha256: Optional[str] = None


class UpdateManifest(BaseModel):
    version: str
    type: str  # 'auto' | 'manual'
    requiresMigrations: bool = False
    releaseNotes: Optional[str] = None
    assets: Optional[Dict[str, UpdateAsset]] = None  # keys: 'backend', 'frontend'


class UpdateCheckOut(BaseModel):
    enabled: bool
    component: str
    currentVersion: Optional[str] = None
    latestVersion: Optional[str] = None
    updateType: Optional[str] = None
    requiresMigrations: Optional[bool] = None
    releaseNotes: Optional[str] = None
    manifestUrl: Optional[str] = None


class ApplyResult(BaseModel):
    ok: bool
    component: str
    version: str
    stagedPath: Optional[str] = None
    requiresRestart: bool = True


_DATA_DIR = Path(settings.metadata_db_path).resolve().parent
_FRONTEND_VERSION_FILE = _DATA_DIR / "frontend_version.txt"


def _read_or_seed_frontend_version() -> Optional[str]:
    try:
        if _FRONTEND_VERSION_FILE.exists():
            v = _FRONTEND_VERSION_FILE.read_text(encoding="utf-8").strip()
            return v or None
        # Seed from env if provided
        if settings.frontend_version_env:
            _FRONTEND_VERSION_FILE.parent.mkdir(parents=True, exist_ok=True)
            val = str(settings.frontend_version_env).strip()
            _FRONTEND_VERSION_FILE.write_text(val, encoding="utf-8")
            return val or None
    except Exception:
        return None
    return None


@router.get("/version", response_model=VersionOut)
async def get_version() -> VersionOut:
    backend_v = settings.app_version
    frontend_v = _read_or_seed_frontend_version()
    return VersionOut(backend=backend_v, frontend=frontend_v)


async def _github_latest_release() -> Dict[str, Any]:
    if not settings.updates_enabled or not settings.update_repo_owner or not settings.update_repo_name:
        raise HTTPException(status_code=400, detail="Updates not configured")
    base = f"https://api.github.com/repos/{settings.update_repo_owner}/{settings.update_repo_name}"
    headers = {"Accept": "application/vnd.github+json"}
    if settings.github_token:
        headers["Authorization"] = f"Bearer {settings.github_token}"
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        # Prefer latest stable, or latest prerelease if channel != stable
        releases = (await client.get(f"{base}/releases", headers=headers)).json()
        if not isinstance(releases, list) or not releases:
            raise HTTPException(status_code=502, detail="No releases found")
        if (settings.update_channel or "stable").lower() == "stable":
            # first non-prerelease
            for r in releases:
                if not r.get("prerelease"):  # stable
                    return r
            return releases[0]
        else:
            # beta: first prerelease
            for r in releases:
                if r.get("prerelease"):
                    return r
            return releases[0]


async def _download_manifest_for_release(release: Dict[str, Any]) -> tuple[UpdateManifest, str]:
    assets = release.get("assets") or []
    manifest_name = settings.update_manifest_name or "bayan-manifest.json"
    asset = next((a for a in assets if str(a.get("name", "")).lower() == manifest_name.lower()), None)
    if not asset:
        raise HTTPException(status_code=502, detail="Manifest asset not found in release")
    url = asset.get("browser_download_url")
    if not url:
        raise HTTPException(status_code=502, detail="Manifest download URL missing")
    headers = {}
    if settings.github_token:
        headers["Authorization"] = f"Bearer {settings.github_token}"
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        r = await client.get(url, headers=headers, follow_redirects=True)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Failed to fetch manifest: {r.text}")
        data = r.json()
    try:
        mf = UpdateManifest(**data)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Invalid manifest: {e}")
    return mf, url


@router.get("/check", response_model=UpdateCheckOut)
async def check_updates(component: str = Query(default="backend", pattern=r"^(backend|frontend|both)$")) -> UpdateCheckOut:
    enabled = bool(settings.updates_enabled and settings.update_repo_owner and settings.update_repo_name)
    current_backend = settings.app_version or None
    current_frontend = _read_or_seed_frontend_version()
    if not enabled:
        return UpdateCheckOut(enabled=False, component=component, currentVersion=(current_backend if component != "frontend" else current_frontend))
    release = await _github_latest_release()
    mf, mf_url = await _download_manifest_for_release(release)
    cur = current_backend if component == "backend" else current_frontend
    return UpdateCheckOut(
        enabled=True,
        component=component,
        currentVersion=cur,
        latestVersion=mf.version,
        updateType=mf.type,
        requiresMigrations=bool(mf.requiresMigrations),
        releaseNotes=mf.releaseNotes,
        manifestUrl=mf_url,
    )


def _stage_dir(component: str, version: str) -> Path:
    d = _DATA_DIR / "updates" / component / version
    d.mkdir(parents=True, exist_ok=True)
    return d


async def _download_asset(url: str, dest: Path) -> None:
    headers = {}
    if settings.github_token:
        headers["Authorization"] = f"Bearer {settings.github_token}"
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        async with client.stream("GET", url, headers=headers, follow_redirects=True) as r:
            if r.status_code != 200:
                raise HTTPException(status_code=502, detail=f"Failed to download asset: {r.status_code}")
            with open(dest, "wb") as f:
                async for chunk in r.aiter_bytes():
                    f.write(chunk)


@router.post("/apply", response_model=ApplyResult)
async def apply_update(
    component: str = Query(default="backend", pattern=r"^(backend|frontend)$"),
    actorId: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
) -> ApplyResult:
    # Simple admin gate (reuse existing roles)
    if actorId:
        u = db.query(User).filter(User.id == str(actorId).strip()).first()
        if not (u and (u.role or "user").lower() == "admin"):
            raise HTTPException(status_code=403, detail="Admin required")
    else:
        raise HTTPException(status_code=403, detail="actorId is required for updates")

    if not settings.updates_enabled:
        raise HTTPException(status_code=400, detail="Updates are disabled")

    release = await _github_latest_release()
    mf, _ = await _download_manifest_for_release(release)
    if (mf.type or "").lower() != "auto" or bool(mf.requiresMigrations):
        raise HTTPException(status_code=400, detail="This update requires manual intervention")

    asset = (mf.assets or {}).get(component)
    if not asset or not asset.url:
        raise HTTPException(status_code=400, detail=f"No asset for component: {component}")

    sd = _stage_dir(component, mf.version)
    # Download file into stage
    # Guess extension
    ext = ".tar.gz" if asset.url.endswith(".tar.gz") else (".zip" if asset.url.endswith(".zip") else ".bin")
    dest = sd / f"{component}{ext}"
    await _download_asset(asset.url, dest)

    # Optionally extract archives for convenience
    try:
        if dest.suffixes[-2:] == [".tar", ".gz"]:
            import tarfile
            with tarfile.open(dest, "r:gz") as tf:
                tf.extractall(sd / "extracted")
        elif dest.suffix == ".zip":
            import zipfile
            with zipfile.ZipFile(dest, 'r') as zf:
                zf.extractall(sd / "extracted")
    except Exception:
        # Non-fatal
        pass

    # Persist applied frontend version for display
    if component == "frontend":
        try:
            _FRONTEND_VERSION_FILE.parent.mkdir(parents=True, exist_ok=True)
            _FRONTEND_VERSION_FILE.write_text(str(mf.version).strip(), encoding="utf-8")
        except Exception:
            pass
    return ApplyResult(ok=True, component=component, version=mf.version, stagedPath=str(sd), requiresRestart=True)
