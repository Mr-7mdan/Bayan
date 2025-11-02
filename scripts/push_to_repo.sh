#!/usr/bin/env bash
set -euo pipefail

# Usage: scripts/push_to_repo.sh [owner] [repo]
# Env overrides: UPDATE_REPO_OWNER, UPDATE_REPO_NAME

OWNER="${1:-${UPDATE_REPO_OWNER:-Mr-7mdan}}"
REPO="${2:-${UPDATE_REPO_NAME:-Bayan}}"
# Prefer SSH to avoid HTTPS TLS/CA issues when GIT_USE_SSH is enabled
if [ "${GIT_USE_SSH:-0}" = "1" ] || [ "${GIT_USE_SSH:-}" = "true" ] || [ "${GIT_USE_SSH:-}" = "yes" ] || [ "${GIT_USE_SSH:-}" = "on" ]; then
  REMOTE_URL="git@github.com:${OWNER}/${REPO}.git"
else
  REMOTE_URL="https://github.com/${OWNER}/${REPO}.git"
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/scripts/out"

# Ensure gh and git exist
command -v git >/dev/null 2>&1 || { echo "[push] git not found" >&2; exit 1; }
command -v gh >/dev/null 2>&1 || { echo "[push] GitHub CLI 'gh' not found. Install and run 'gh auth login'." >&2; exit 1; }

# Verify assets exist
if [ ! -f "${OUT_DIR}/frontend-latest.json" ] || [ ! -f "${OUT_DIR}/backend-latest.json" ]; then
  echo "[push] Missing latest json files in ${OUT_DIR}. Build frontend and backend first." >&2
  exit 1
fi

# Derive version from latest json
VER=$(node -e "const fs=require('fs');const p1='${OUT_DIR}/backend-latest.json';const p2='${OUT_DIR}/frontend-latest.json';const p=fs.existsSync(p1)?p1:p2;console.log(JSON.parse(fs.readFileSync(p,'utf8')).version)")
TAG="v${VER}"

FRONT_ART="${OUT_DIR}/frontend-${VER}.tar.gz"
BACK_ART="${OUT_DIR}/backend-${VER}.tar.gz"
MANIFEST="${OUT_DIR}/bayan-manifest.json"

for f in "$FRONT_ART" "$BACK_ART" "$MANIFEST"; do
  [ -f "$f" ] || { echo "[push] Missing asset: $f" >&2; exit 1; }
done

echo "[push] Target repo: ${OWNER}/${REPO}"
echo "[push] Version: ${VER} (${TAG})"

# Ensure git repo is initialized
cd "$ROOT_DIR"
if [ ! -d .git ]; then
  echo "[push] Initializing git repo"
  git init
fi

# Ensure default branch is main
if ! git rev-parse --abbrev-ref HEAD >/dev/null 2>&1; then
  git checkout -b main || git switch -c main || true
fi

# Ensure remote origin
if ! git remote get-url origin >/dev/null 2>&1; then
  echo "[push] Adding remote origin -> ${REMOTE_URL}"
  git remote add origin "$REMOTE_URL" || true
else
  CURRENT_URL=$(git remote get-url origin 2>/dev/null || echo "")
  if [ "${CURRENT_URL}" != "${REMOTE_URL}" ]; then
    echo "[push] Updating remote origin URL -> ${REMOTE_URL} (was: ${CURRENT_URL})"
    git remote set-url origin "$REMOTE_URL" || true
  fi
fi

# Create initial commit if repo has no commits
if ! git rev-parse HEAD >/dev/null 2>&1; then
  echo "[push] Creating initial commit"
  git add -A || true
  git commit -m "chore: initial commit" || true
fi

CHANGES="$(git status --porcelain || true)"
if [ -n "$CHANGES" ]; then
  echo "[push] Committing pending changes"
  git add -A
  git commit -m "chore: release ${VER} - include pending changes" || true
fi

REWROTE=false
# Preflight: detect tracked banned paths and handle first-commit rewrite safely
BAN_REGEX='(^|/)node_modules/|(^|/)\.next/|^scripts/out/|(^|/)dist/'
TRACKED_BANNED=$(git ls-files -z | tr '\0' '\n' | grep -E "${BAN_REGEX}" || true)
if [ -n "${TRACKED_BANNED}" ]; then
  COMMITS=$(git rev-list --count HEAD 2>/dev/null || echo 0)
  if [ "${COMMITS}" -le 1 ]; then
    echo "[push] Preflight: initial commit contains banned paths; amending initial commit to untrack them"
    # Remove heavy/build paths from index (keep files on disk)
    git rm -r --cached -q -- node_modules || true
    git rm -r --cached -q -- frontend/node_modules || true
    git rm -r --cached -q -- scripts/out || true
    git rm -r --cached -q -- .next || true
    git rm -r --cached -q -- frontend/.next || true
    git rm -r --cached -q -- dist || true
    git rm -r --cached -q -- frontend/dist || true
    git rm -r --cached -q -- backend/dist || true
    # Re-stage ignores and everything else, then amend the initial commit in-place
    git add .gitignore frontend/.gitignore || true
    git add -A
    GIT_COMMITTER_DATE="$(git show -s --format=%cI HEAD)" \
    GIT_AUTHOR_DATE="$(git show -s --format=%aI HEAD)" \
    git commit --amend -m "chore: initial commit (pruned node_modules, .next, scripts/out, dist)" || true
    REWROTE=true
  else
    echo "[push] Detected banned paths tracked in git:" >&2
    echo "${TRACKED_BANNED}" | sed 's/^/  - /' >&2
    echo "[push] Aborting to avoid rewriting history. Please remove these paths from git history or run manual cleanup (git rm --cached ...) and commit, then retry." >&2
    exit 1
  fi
fi

# Push main branch (handles empty remote)
if git ls-remote --heads origin main >/dev/null 2>&1; then
  echo "[push] Syncing with remote main (rebase)"
  git pull --rebase origin main || { echo "[push] Pull --rebase failed; please resolve conflicts and retry." >&2; exit 1; }
fi
echo "[push] Pushing main branch to origin"
if ! git push -u origin main; then
  if [ "${GIT_FORCE_PUSH:-0}" = "1" ] || [ "${GIT_FORCE_PUSH:-}" = "true" ] || [ "${GIT_FORCE_PUSH:-}" = "yes" ] || [ "${GIT_FORCE_PUSH:-}" = "on" ]; then
    echo "[push] Force pushing main (with lease)"
    git push -u --force-with-lease origin main || { echo "[push] Force push failed" >&2; exit 1; }
  else
    echo "[push] Push rejected. Run 'git pull --rebase origin main' or set GIT_FORCE_PUSH=1 to override." >&2
    exit 1
  fi
fi

# Create/retag to current HEAD if we rewrote initial commit
if [ "$REWROTE" = true ]; then
  if git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "[push] Retagging ${TAG} to current HEAD"
    git tag -f -a "$TAG" -m "Release ${VER}" || true
  else
    echo "[push] Creating tag ${TAG}"
    git tag -a "$TAG" -m "Release ${VER}" || true
  fi
else
  if ! git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "[push] Creating tag ${TAG}"
    git tag -a "$TAG" -m "Release ${VER}" || true
  fi
fi
echo "[push] Pushing tag ${TAG}"
git push origin "$TAG" || true

# Create release if not exists
if ! gh release view "$TAG" --repo "${OWNER}/${REPO}" >/dev/null 2>&1; then
  echo "[push] Creating GitHub release ${TAG}"
  gh release create "$TAG" \
    --repo "${OWNER}/${REPO}" \
    --title "Release ${VER}" \
    --notes "Release ${VER}" || true
fi

# Upload (or overwrite) assets
echo "[push] Uploading assets"
gh release upload "$TAG" \
  "$FRONT_ART" \
  "$BACK_ART" \
  "$MANIFEST" \
  --repo "${OWNER}/${REPO}" \
  --clobber || true

echo "[push] Done"