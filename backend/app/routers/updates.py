from __future__ import annotations

import os
import sys
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


class PromoteResult(BaseModel):
    ok: bool
    component: str
    version: str
    releasesPath: Optional[str] = None
    currentPath: Optional[str] = None
    restarted: bool = False
    message: Optional[str] = None


_DATA_DIR = Path(settings.metadata_db_path).resolve().parent
_FRONTEND_VERSION_FILE = _DATA_DIR / "frontend_version.txt"


def _read_or_seed_frontend_version() -> Optional[str]:
    try:
        if _FRONTEND_VERSION_FILE.exists():
            v = _FRONTEND_VERSION_FILE.read_text(encoding="utf-8").strip()
            return v or None
        # Seed from env if provided
        if getattr(settings, 'frontend__env', None):
            _FRONTEND_VERSION_FILE.parent.mkdir(parents=True, exist_ok=True)
            val = str(getattr(settings, 'frontend__env')).strip()
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
    # Do NOT use GITHUB_TOKEN for updates - public releases don't need authentication
    # (The token is for bug reporting only)
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        # Prefer latest stable, or latest prerelease if channel != stable
        try:
            response = await client.get(f"{base}/releases", headers=headers)
            if response.status_code == 404:
                raise HTTPException(status_code=502, detail=f"Repository not found: {settings.update_repo_owner}/{settings.update_repo_name}")
            elif response.status_code == 401:
                raise HTTPException(status_code=502, detail="GitHub API requires authentication for this repository (it may be private)")
            elif response.status_code == 403:
                raise HTTPException(status_code=502, detail="GitHub API rate limit exceeded or repository access denied")
            elif response.status_code != 200:
                raise HTTPException(status_code=502, detail=f"GitHub API error: {response.status_code} - {response.text[:200]}")
            releases = response.json()
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"Failed to reach GitHub API: {str(e)}")
        
        if not isinstance(releases, list) or not releases:
            raise HTTPException(status_code=502, detail="No releases found in repository")
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
    # Don't use authentication for public release assets
    headers = {"Accept": "application/json"}
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
    # Don't use authentication for public release assets
    headers = {"Accept": "application/octet-stream"}
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


def _win_path(p: Path) -> str:
    return str(p).replace('/', '\\')


def _find_root_install_dir() -> Path:
    # app/routers/updates.py -> app/routers -> backend/app -> backend -> root
    here = Path(__file__).resolve()
    # parents[0]=.../routers, [1]=.../app, [2]=.../backend, [3]=.../root
    try:
        return here.parents[3]
    except Exception:
        return here.parents[2]


def _copy_tree(src: Path, dst: Path) -> None:
    import shutil
    dst.mkdir(parents=True, exist_ok=True)
    # Python 3.8+: copytree with dirs_exist_ok
    shutil.copytree(src, dst, dirs_exist_ok=True)


def _copy_overlay(src: Path, dst: Path, ignore_names: Optional[set[str]] = None) -> None:
    import shutil
    ignore = set(ignore_names or set())
    dst.mkdir(parents=True, exist_ok=True)
    for p in src.iterdir():
        name = p.name
        if name in ignore:
            continue
        if p.is_dir():
            # Only skip if explicitly in ignore list (removed hardcoded blacklist)
            # The caller should specify what to ignore via ignore_names parameter
            shutil.copytree(p, dst / name, dirs_exist_ok=True)
        else:
            shutil.copy2(p, dst / name)


def _ensure_current_pointer(component_dir: Path, version_dir: Path) -> Path:
    current = component_dir / 'current'
    try:
        if current.exists() or current.is_symlink():
            # Remove existing link or dir
            if os.name == 'nt':
                # Use rmdir for junctions/symlinks
                import subprocess
                subprocess.run(['cmd', '/c', 'rmdir', '/S', '/Q', _win_path(current)], check=False)
            else:
                import shutil
                shutil.rmtree(current, ignore_errors=True)
    except Exception:
        pass
    # Create symlink/junction pointing to version_dir
    try:
        if os.name == 'nt':
            import subprocess
            subprocess.run(['cmd', '/c', 'mklink', '/J', _win_path(current), _win_path(version_dir)], check=True)
        else:
            current.symlink_to(version_dir, target_is_directory=True)
    except Exception:
        # Fallback: create a marker file with the path
        try:
            (component_dir / 'CURRENT.txt').write_text(str(version_dir), encoding='utf-8')
        except Exception:
            pass
    return current


def _nssm_path_candidates() -> list[str]:
    cands = ['nssm']
    for p in (
        'C\\Program Files\\nssm\\nssm.exe',
        'C\\Program Files (x86)\\nssm\\nssm.exe',
    ):
        try:
            if os.path.exists(p.replace('\\', '\\\\')):
                cands.append(p)
        except Exception:
            pass
    return cands


def _run_nssm(args: list[str]) -> bool:
    import subprocess
    for exe in _nssm_path_candidates():
        try:
            r = subprocess.run([exe] + args, capture_output=True, text=True)
            if r.returncode == 0:
                return True
        except Exception:
            continue
    return False


def _restart_service_win(name: str) -> bool:
    ok1 = _run_nssm(['stop', name])
    ok2 = _run_nssm(['start', name])
    return bool(ok1 and ok2)


@router.post("/promote", response_model=PromoteResult)
async def promote_update(
    component: str = Query(default="backend", pattern=r"^(backend|frontend)$"),
    version: Optional[str] = Query(default=None),
    restart: bool = Query(default=True),
    actorId: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
) -> PromoteResult:
    # Admin gate
    if actorId:
        u = db.query(User).filter(User.id == str(actorId).strip()).first()
        if not (u and (u.role or "user").lower() == "admin"):
            raise HTTPException(status_code=403, detail="Admin required")
    else:
        raise HTTPException(status_code=403, detail="actorId is required for updates")

    # Determine version if not provided
    if not version:
        release = await _github_latest_release()
        mf, _ = await _download_manifest_for_release(release)
        if (mf.type or "").lower() != "auto" or bool(mf.requiresMigrations):
            raise HTTPException(status_code=400, detail="This update requires manual intervention")
        version = mf.version

    sd = _stage_dir(component, version)
    if not sd.exists():
        raise HTTPException(status_code=404, detail=f"Staged version not found: {component} {version}")

    # Source dir to copy: prefer extracted/
    src = sd / 'extracted'
    if not src.exists():
        src = sd
    # Resolve payload dir when archives include top-level component folder
    payload = src
    if (src / 'backend').exists() and component == 'backend':
        payload = src / 'backend'
    if (src / 'frontend').exists() and component == 'frontend':
        payload = src / 'frontend'

    root = _find_root_install_dir()
    releases_dir = root / 'releases' / component
    target = releases_dir / str(version)
    try:
        _copy_tree(payload, target)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to copy to releases: {e}")

    # Update 'current' pointer
    current = _ensure_current_pointer(releases_dir, target)

    restarted = False
    msg = None
    if component == 'backend':
        backend_dir = root / 'backend'
        # Backup current live backend (lightweight overlay backup)
        try:
            from datetime import datetime as _dt
            backups_dir = root / 'backups' / 'backend'
            backup_target = backups_dir / _dt.now().strftime('%Y%m%d-%H%M%S')
            _copy_overlay(backend_dir, backup_target, ignore_names={'.data', 'logs', 'venv', '__pycache__'})
        except Exception:
            pass
        if os.name == 'nt' and restart:
            _run_nssm(['stop', 'BayanAPIUvicorn'])
        try:
            # Copy all files from release to live backend, preserving user config (.env)
            # and runtime data (.data, logs, venv)
            _copy_overlay(current, backend_dir, ignore_names={'.env', '.data', 'logs', 'venv', '__pycache__', 'dist'})
        except Exception as e:
            print(f"[WARN] Failed to copy files during promote: {e}", file=sys.stderr)
        # Update APP_VERSION in .env to reflect the promoted version
        try:
            env_file = backend_dir / '.env'
            if env_file.exists():
                content = env_file.read_text(encoding='utf-8')
                import re
                updated = re.sub(r'^APP_VERSION=.*$', f'APP_VERSION={version}', content, flags=re.MULTILINE)
                if updated != content:
                    env_file.write_text(updated, encoding='utf-8')
        except Exception as e:
            print(f"[WARN] Failed to update APP_VERSION in .env: {e}", file=sys.stderr)
        if os.name == 'nt' and restart:
            restarted = _restart_service_win('BayanAPIUvicorn')
        elif restart:
            msg = 'Promoted. Please restart the backend service to apply.'
    else:
        frontend_dir = root / 'frontend'
        # Backup current live frontend (lightweight overlay backup)
        try:
            from datetime import datetime as _dt
            backups_dir = root / 'backups' / 'frontend'
            backup_target = backups_dir / _dt.now().strftime('%Y%m%d-%H%M%S')
            _copy_overlay(frontend_dir, backup_target, ignore_names={'node_modules', '.next', 'logs'})
        except Exception:
            pass
        if os.name == 'nt' and restart:
            _run_nssm(['stop', 'BayanUI'])
        try:
            _copy_overlay(current, frontend_dir, ignore_names={})
        except Exception:
            pass
        if os.name == 'nt' and restart:
            restarted = _restart_service_win('BayanUI')
        elif restart:
            msg = 'Promoted. Please restart the frontend service to apply.'

    return PromoteResult(ok=True, component=component, version=str(version), releasesPath=str(releases_dir), currentPath=str(current), restarted=bool(restarted), message=msg)
