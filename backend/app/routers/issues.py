from __future__ import annotations

import asyncio
import hashlib
import json
import re
import traceback
from typing import Any, Dict, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from ..config import settings

router = APIRouter(prefix="/issues", tags=["issues"]) 


class IssueReportIn(BaseModel):
    kind: str
    errorName: Optional[str] = None
    message: Optional[str] = None
    stack: Optional[str] = None
    componentStack: Optional[str] = None
    file: Optional[str] = None
    line: Optional[int] = None
    column: Optional[int] = None
    url: Optional[str] = None
    appVersion: Optional[str] = None
    environment: Optional[str] = None
    browser: Optional[str] = None
    userId: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    occurredAt: Optional[str] = None


def _redact(value: Any, depth: int = 0) -> Any:
    try:
        if depth > 6:
            return "[truncated]"
        if isinstance(value, dict):
            out: Dict[str, Any] = {}
            for k, v in value.items():
                if re.search(r"pass(word)?|token|secret|key|authorization|cookie|set-cookie", str(k), re.I):
                    out[k] = "[redacted]"
                else:
                    out[k] = _redact(v, depth + 1)
            return out
        if isinstance(value, (list, tuple)):
            return [ _redact(v, depth + 1) for v in value[:200] ]
        if isinstance(value, str):
            s = value if len(value) <= 4000 else (value[:4000] + "…[truncated]")
            return s
        return value
    except Exception:
        return "[unserializable]"


def _fingerprint(parts: Tuple[str, ...]) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update((p or "").encode("utf-8", errors="ignore"))
        h.update(b"|")
    return h.hexdigest()[:20]


def _title(kind: str, version: Optional[str], file: Optional[str], line: Optional[int], err_name: Optional[str]) -> str:
    k = "Frontend" if str(kind).lower().startswith("front") else "Backend"
    v = version or settings.app_version or ""
    loc = f"{file}:{line}" if file else (file or "?")
    en = err_name or "Error"
    return f"[Bug][{k}][{v}][{loc}][{en}]"


async def _github_request(method: str, url: str, json_body: Optional[dict] = None) -> dict:
    token = (settings.github_token or "").strip()
    owner = (settings.update_repo_owner or "").strip()
    repo = (settings.update_repo_name or "").strip()
    if not token or not owner or not repo:
        raise RuntimeError("GitHub issue reporting not configured")
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    base = f"https://api.github.com/repos/{owner}/{repo}"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.request(method, base + url, headers=headers, json=json_body)
        if r.status_code >= 400:
            raise RuntimeError(f"GitHub API {r.status_code}: {r.text}")
        try:
            return r.json()
        except Exception:
            return {}


async def _find_issue_by_label(fp_label: str) -> Optional[dict]:
    try:
        token = (settings.github_token or "").strip()
        owner = (settings.update_repo_owner or "").strip()
        repo = (settings.update_repo_name or "").strip()
        if not token or not owner or not repo:
            return None
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        q = f"repo:{owner}/{repo} label:{fp_label} state:open"
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://api.github.com/search/issues", headers=headers, params={"q": q})
            if r.status_code >= 400:
                return None
            data = r.json()
            items = data.get("items") or []
            return items[0] if items else None
    except Exception:
        return None


async def _create_issue(title: str, body: str, labels: list[str]) -> dict:
    return await _github_request("POST", "/issues", {"title": title, "body": body, "labels": labels})


async def _comment_issue(number: int, body: str) -> dict:
    return await _github_request("POST", f"/issues/{number}/comments", {"body": body})


def _format_body(summary: Dict[str, Any], payload: Dict[str, Any]) -> str:
    env_lines = [
        f"Version: {summary.get('version') or ''}",
        f"Environment: {summary.get('environment') or ''}",
        f"Kind: {summary.get('kind')}",
        f"User: {summary.get('userId') or ''}",
        f"URL: {summary.get('url') or ''}",
        f"Fingerprint: {summary.get('fingerprint')}",
    ]
    parts = [
        "## Summary",
        "- " + "\n- ".join([l for l in env_lines if l]),
        "\n## Stack",
        "```\n" + (summary.get("stack") or "") + "\n```",
        ("\n## Component Stack\n```\n" + summary.get("componentStack", "") + "\n```") if summary.get("componentStack") else "",
        "\n## Context",
        "```json\n" + json.dumps(_redact(payload), ensure_ascii=False, indent=2) + "\n```",
    ]
    return "\n".join([p for p in parts if p])


async def report_issue(kind: str, message: str | None, error_name: str | None, stack: str | None, file: str | None, line: int | None, url: str | None, version: str | None, extra: Dict[str, Any] | None) -> Dict[str, Any]:
    version = (version or settings.app_version or "").strip()
    normalized_msg = (message or "").strip()[:500]
    fp = _fingerprint((kind or "", version or "", (file or "").lower(), str(line or ""), (error_name or "").lower(), normalized_msg.lower()))
    fp_label = f"bug-fp-{fp}"
    title = _title(kind, version, file, line, error_name)
    body = _format_body({
        "kind": kind,
        "version": version,
        "environment": settings.environment,
        "userId": (extra or {}).get("userId"),
        "url": url,
        "stack": stack or "",
        "componentStack": (extra or {}).get("componentStack"),
        "fingerprint": fp,
    }, {
        "message": message,
        "stack": stack,
        "file": file,
        "line": line,
        "url": url,
        "version": version,
        "extra": _redact(extra or {}),
    })
    token = (settings.github_token or "").strip()
    owner = (settings.update_repo_owner or "").strip()
    repo = (settings.update_repo_name or "").strip()
    if not token or not owner or not repo:
        return {"ok": False, "message": "issue reporting disabled", "fingerprint": fp}
    labels = ["bug", ("frontend" if kind.lower().startswith("front") else "backend"), fp_label]
    if version:
        labels.append(f"version:v{version}")
    existing = await _find_issue_by_label(fp_label)
    if existing:
        num = existing.get("number")
        url2 = existing.get("html_url")
        try:
            await _comment_issue(int(num), f"New occurrence on {settings.environment} v{version}\n\n" + body[:6000])
        except Exception:
            pass
        return {"ok": True, "deduped": True, "issueNumber": num, "issueUrl": url2, "fingerprint": fp}
    created = await _create_issue(title, body, labels)
    return {"ok": True, "deduped": False, "issueNumber": created.get("number"), "issueUrl": created.get("html_url"), "fingerprint": fp}


@router.post("/report")
async def report(payload: IssueReportIn):
    try:
        res = await report_issue(
            kind=payload.kind or "frontend",
            message=payload.message,
            error_name=payload.errorName,
            stack=payload.stack,
            file=payload.file,
            line=payload.line,
            url=payload.url,
            version=payload.appVersion,
            extra={
                "componentStack": payload.componentStack,
                "browser": payload.browser,
                "userId": payload.userId,
                "metadata": _redact(payload.metadata or {}),
            },
        )
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def report_backend_exception(exc: BaseException, request: Request) -> None:
    try:
        tb = exc.__traceback__
        file_path = None
        line_no: Optional[int] = None
        if tb is not None:
            frames = traceback.extract_tb(tb)
            if frames:
                last = frames[-1]
                file_path = last.filename
                line_no = last.lineno
        msg = str(exc)
        err_name = exc.__class__.__name__
        url = str(request.url) if request else None
        version = settings.app_version
        extra = {
            "method": getattr(request, "method", None),
            "path": getattr(request.url, "path", None) if request else None,
            "client": getattr(getattr(request, "client", None), "host", None) if request else None,
            "headers": {k.decode(): v.decode() for k, v in getattr(request, "scope", {}).get("headers", []) if k.decode().lower() not in ("authorization", "cookie", "set-cookie")},
        }
        try:
            await report_issue("backend", msg, err_name, "\n".join(traceback.format_exception(type(exc), exc, exc.__traceback__)), file_path, line_no, url, version, extra)
        except Exception:
            pass
    except Exception:
        pass


@router.post("/test")
async def test_issue():
    token = (settings.github_token or "").strip()
    owner = (settings.update_repo_owner or "").strip()
    repo = (settings.update_repo_name or "").strip()
    if not token or not owner or not repo:
        raise HTTPException(status_code=400, detail="issue reporting disabled")
    title = f"[Test][Issues][{settings.environment}]"
    body = f"Test issue from environment panel. Version: {settings.app_version} Environment: {settings.environment}"
    created = await _create_issue(title, body, ["bug"])
    return {"ok": True, "issueNumber": created.get("number"), "issueUrl": created.get("html_url")}
