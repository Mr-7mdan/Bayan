#!/usr/bin/env bash
set -euo pipefail

# Build a backend (FastAPI) artifact and bump version by 0.1
# Outputs: scripts/out/backend-<version>.tar.gz and .sha256

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUT_DIR="${ROOT_DIR}/scripts/out"
VER_FILE="${ROOT_DIR}/scripts/VERSION_BACKEND"
BACKEND_DIR="${ROOT_DIR}/backend"

mkdir -p "${OUT_DIR}"

# Read current version and bump by 0.1 (one decimal)
CURR_VER="0.0"
if [ -f "${VER_FILE}" ]; then
  CURR_VER="$(cat "${VER_FILE}" | tr -d '\r' || true)"
fi
NEW_VER="$(awk -v v="${CURR_VER}" 'BEGIN{printf "%.1f\n", v + 0.1}')"
echo "${NEW_VER}" > "${VER_FILE}"

echo "[backend] Building version ${NEW_VER}"

cd "${BACKEND_DIR}"
# Stage content (no venv)
STAGE_DIR="${BACKEND_DIR}/dist/backend-${NEW_VER}"
rm -rf "${BACKEND_DIR}/dist"
mkdir -p "${STAGE_DIR}"

# Required app code & config
cp -r app "${STAGE_DIR}/"
[ -d scripts ] && cp -r scripts "${STAGE_DIR}/"
[ -f requirements.txt ] && cp requirements.txt "${STAGE_DIR}/"
[ -f wsgi.py ] && cp wsgi.py "${STAGE_DIR}/"
[ -f asgi.py ] && cp asgi.py "${STAGE_DIR}/"
# Helpful run scripts (optional)
[ -f run_prod_gunicorn.sh ] && cp run_prod_gunicorn.sh "${STAGE_DIR}/"
[ -f run_prod_waitress.sh ] && cp run_prod_waitress.sh "${STAGE_DIR}/"
[ -f run_prod_uvicorn_windows.bat ] && cp run_prod_uvicorn_windows.bat "${STAGE_DIR}/"
[ -f run_prod_waitress_windows.bat ] && cp run_prod_waitress_windows.bat "${STAGE_DIR}/"
# Version marker
printf "%s" "${NEW_VER}" > "${STAGE_DIR}/VERSION"

# Pack
ART_PATH="${OUT_DIR}/backend-${NEW_VER}.tar.gz"
rm -f "${ART_PATH}"
tar -C "${BACKEND_DIR}/dist" -czf "${ART_PATH}" "backend-${NEW_VER}"

# Hash
if command -v shasum >/dev/null 2>&1; then
  SHA256=$(shasum -a 256 "${ART_PATH}" | awk '{print $1}')
else
  SHA256=$(sha256sum "${ART_PATH}" | awk '{print $1}')
fi
printf "%s  %s\n" "${SHA256}" "$(basename "${ART_PATH}")" > "${ART_PATH}.sha256"

echo "[backend] Built: ${ART_PATH}"
echo "[backend] SHA256: ${SHA256}"

echo "{\"artifact\":\"$(basename "${ART_PATH}")\",\"version\":\"${NEW_VER}\",\"sha256\":\"${SHA256}\"}" > "${OUT_DIR}/backend-latest.json"
