#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}/frontend"
PORT="${PORT:-3000}"
HOST="${HOST:-0.0.0.0}"
export NODE_ENV=production
if [ ! -f package.json ]; then
  echo "package.json not found in ${ROOT_DIR}/frontend" >&2
  exit 1
fi
if [ ! -d node_modules ]; then
  npm ci
fi
rm -rf .next
if [ ! -d .next ]; then
  npm run build
fi
exec npm run start -- -p "${PORT}" -H "${HOST}"
