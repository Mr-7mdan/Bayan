#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -lt 3 ]; then
  echo "Usage: $0 OWNER REPO TAG" >&2
  exit 1
fi
OWNER="$1"
REPO="$2"
TAG="$3"
if command -v gh >/dev/null 2>&1; then
  RID="$(gh api "repos/${OWNER}/${REPO}/releases/tags/${TAG}" --jq '.id' 2>/dev/null || echo "")"
  if [ -n "$RID" ]; then
    gh api -X DELETE "repos/${OWNER}/${REPO}/releases/${RID}" >/dev/null 2>&1 || true
  fi
  gh api -X DELETE "repos/${OWNER}/${REPO}/git/refs/tags/${TAG}" >/dev/null 2>&1 || true
  echo "Rolled back ${OWNER}/${REPO} ${TAG}"
  exit 0
fi
TOKEN="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
if [ -z "$TOKEN" ]; then
  echo "Error: gh not found and GITHUB_TOKEN/GH_TOKEN not set" >&2
  exit 1
fi
HDR=(-H "Authorization: Bearer ${TOKEN}" -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28")
RID=$(curl -fsS "https://api.github.com/repos/${OWNER}/${REPO}/releases/tags/${TAG}" "${HDR[@]}" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("id",""))' 2>/dev/null || true)
if [ -n "$RID" ]; then
  curl -fsS -X DELETE "https://api.github.com/repos/${OWNER}/${REPO}/releases/${RID}" "${HDR[@]}" >/dev/null 2>&1 || true
fi
curl -fsS -X DELETE "https://api.github.com/repos/${OWNER}/${REPO}/git/refs/tags/${TAG}" "${HDR[@]}" >/dev/null 2>&1 || true
echo "Rolled back ${OWNER}/${REPO} ${TAG}"
