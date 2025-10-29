#!/usr/bin/env bash
set -euo pipefail

# Build a Next.js production artifact and bump version by 0.1
# Outputs: scripts/out/frontend-<version>.tar.gz and .sha256

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUT_DIR="${ROOT_DIR}/scripts/out"
VER_FILE="${ROOT_DIR}/scripts/VERSION_FRONTEND"

mkdir -p "${OUT_DIR}"

# Read current version and bump by 0.1 (one decimal)
CURR_VER="0.0"
if [ -f "${VER_FILE}" ]; then
  CURR_VER="$(cat "${VER_FILE}" | tr -d '\r' || true)"
fi
NEW_VER="$(awk -v v="${CURR_VER}" 'BEGIN{printf "%.1f\n", v + 0.1}')"
echo "${NEW_VER}" > "${VER_FILE}"

echo "[frontend] Building version ${NEW_VER}"

cd "${ROOT_DIR}/frontend"
if [ ! -f package.json ]; then
  echo "[frontend] package.json not found in ${ROOT_DIR}/frontend" >&2
  exit 1
fi

# Clean and build
rm -rf .next dist
# Pre-clean node_modules to avoid ENOTEMPTY issues (robust)
purge_dir() {
  local dir="$1"
  if [ -d "$dir" ]; then
    chmod -R u+w "$dir" 2>/dev/null || true
    rm -rf "$dir" 2>/dev/null || true
    if [ -d "$dir" ]; then
      echo "[frontend] rm -rf failed; moving $dir aside and retrying"
      local tmp="$dir.__purge__.$(date +%s)"
      mv "$dir" "$tmp" 2>/dev/null || true
      if [ -d "$tmp" ]; then
        chmod -R u+w "$tmp" 2>/dev/null || true
        rm -rf "$tmp" 2>/dev/null || true
        if [ -d "$tmp" ]; then
          # Last resort: delete contents with find, then remove dir
          find "$tmp" -mindepth 1 -exec rm -rf {} + 2>/dev/null || true
          rmdir "$tmp" 2>/dev/null || true
        fi
      fi
    fi
  fi
}

if [ -d node_modules ]; then
  echo "[frontend] Pre-clean node_modules"
  purge_dir node_modules
fi
# Install deps (retry once if ENOTEMPTY or similar)
set +e
npm ci
NPM_RC=$?
if [ $NPM_RC -ne 0 ]; then
  echo "[frontend] First npm ci failed (code $NPM_RC). Cleaning cache and retrying once..."
  npm cache clean --force || true
  rm -rf node_modules || true
  set -e
  npm ci
else
  set -e
fi
npm run build
# Keep only production deps to minimize artifact size
npm prune --omit=dev || true

# Stage files (exclude build/artifact directories)
STAGE_DIR="${ROOT_DIR}/frontend/dist/frontend-${NEW_VER}"
mkdir -p "${STAGE_DIR}"
if command -v rsync >/dev/null 2>&1; then
  rsync -a --delete \
    --exclude ".next" \
    --exclude "node_modules" \
    --exclude "dist" \
    --exclude ".git" \
    --exclude ".DS_Store" \
    ./ "${STAGE_DIR}/"
else
  # Portable fallback: tar stream copy with excludes
  tar -cf - \
    --exclude "./.next" \
    --exclude "./node_modules" \
    --exclude "./dist" \
    --exclude "./.git" \
    --exclude "./.DS_Store" \
    . | tar -xf - -C "${STAGE_DIR}"
fi
# Add version marker inside artifact
printf "%s" "${NEW_VER}" > "${STAGE_DIR}/VERSION"

# Pack
ART_PATH="${OUT_DIR}/frontend-${NEW_VER}.tar.gz"
rm -f "${ART_PATH}"
tar -C "${ROOT_DIR}/frontend/dist" -czf "${ART_PATH}" "frontend-${NEW_VER}"

# Hash (prefer shasum on macOS; fallback to sha256sum)
if command -v shasum >/dev/null 2>&1; then
  SHA256=$(shasum -a 256 "${ART_PATH}" | awk '{print $1}')
else
  SHA256=$(sha256sum "${ART_PATH}" | awk '{print $1}')
fi
printf "%s  %s\n" "${SHA256}" "$(basename "${ART_PATH}")" > "${ART_PATH}.sha256"

echo "[frontend] Built: ${ART_PATH}"
echo "[frontend] SHA256: ${SHA256}"

echo "{\"artifact\":\"$(basename "${ART_PATH}")\",\"version\":\"${NEW_VER}\",\"sha256\":\"${SHA256}\"}" > "${OUT_DIR}/frontend-latest.json"
