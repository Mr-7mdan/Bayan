#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUT_DIR="${ROOT_DIR}/scripts/out"
FRONT_JSON="${OUT_DIR}/frontend-latest.json"
BACK_JSON="${OUT_DIR}/backend-latest.json"

if [ ! -f "${FRONT_JSON}" ] || [ ! -f "${BACK_JSON}" ]; then
  echo "[manifest] Missing ${FRONT_JSON} or ${BACK_JSON}. Build both artifacts first." >&2
  exit 1
fi

node_field() {
  node -e "const fs=require('fs');const p=process.argv[1];const k=process.argv[2];const o=JSON.parse(fs.readFileSync(p,'utf8'));process.stdout.write(String(o[k]??''));" "$1" "$2"
}

FE_VER="$(node_field "${FRONT_JSON}" version)"
FE_ART="$(node_field "${FRONT_JSON}" artifact)"
FE_SHA="$(node_field "${FRONT_JSON}" sha256)"
BE_VER="$(node_field "${BACK_JSON}" version)"
BE_ART="$(node_field "${BACK_JSON}" artifact)"
BE_SHA="$(node_field "${BACK_JSON}" sha256)"

# Determine manifest version
VER="${MANIFEST_VERSION:-}"
if [ -z "${VER}" ]; then
  if [ -n "${BE_VER}" ] && [ -n "${FE_VER}" ] && [ "${BE_VER}" = "${FE_VER}" ]; then
    VER="${BE_VER}"
  else
    VER="${BE_VER:-${FE_VER}}"
  fi
fi
if [ -z "${VER}" ]; then
  echo "[manifest] Could not determine version. Set MANIFEST_VERSION or ensure *-latest.json contain 'version'." >&2
  exit 1
fi

OWNER="${1:-${UPDATE_REPO_OWNER:-}}"
REPO="${2:-${UPDATE_REPO_NAME:-}}"
if [ -z "${OWNER}" ] || [ -z "${REPO}" ]; then
  echo "Usage: $0 <owner> <repo> [tag]" >&2
  echo "Or set UPDATE_REPO_OWNER/UPDATE_REPO_NAME env vars." >&2
  exit 1
fi
TAG_IN="${3:-${MANIFEST_TAG:-v${VER}}}"

TYPE="${UPDATE_TYPE:-${TYPE:-auto}}"
REQ_MIG="${REQUIRES_MIGRATIONS:-0}"
NOTES="${RELEASE_NOTES:-Release ${VER}}"
DEST="${MANIFEST_DEST:-${OUT_DIR}/bayan-manifest.json}"

# Normalize booleans
if [ "${REQ_MIG}" = "1" ] || [ "${REQ_MIG}" = "true" ]; then REQ_MIG=true; else REQ_MIG=false; fi

FRONT_URL="https://github.com/${OWNER}/${REPO}/releases/download/${TAG_IN}/${FE_ART}"
BACK_URL="https://github.com/${OWNER}/${REPO}/releases/download/${TAG_IN}/${BE_ART}"

mkdir -p "${OUT_DIR}"
VER_ENV="${VER}" TYPE_ENV="${TYPE}" REQ_MIG_ENV="${REQ_MIG}" NOTES_ENV="${NOTES}" FRONT_URL_ENV="${FRONT_URL}" FE_SHA_ENV="${FE_SHA}" BACK_URL_ENV="${BACK_URL}" BE_SHA_ENV="${BE_SHA}" DEST_ENV="${DEST}" node - <<'NODE'
const fs = require('fs')
const env = process.env
const requires = String(env.REQ_MIG_ENV || '').toLowerCase() === 'true'
const obj = {
  version: env.VER_ENV || '',
  type: env.TYPE_ENV || 'auto',
  requiresMigrations: requires,
  releaseNotes: env.NOTES_ENV || '',
  assets: {
    frontend: {
      url: env.FRONT_URL_ENV || '',
      sha256: env.FE_SHA_ENV || ''
    },
    backend: {
      url: env.BACK_URL_ENV || '',
      sha256: env.BE_SHA_ENV || ''
    }
  }
}
fs.writeFileSync(env.DEST_ENV, JSON.stringify(obj, null, 2))
NODE

echo "[manifest] Wrote ${DEST}"
