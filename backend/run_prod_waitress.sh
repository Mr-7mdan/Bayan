#!/usr/bin/env bash
set -Eeuo pipefail
# Resolve script directory (handles spaces)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Optional: activate venv
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  . "$SCRIPT_DIR/venv/bin/activate"
fi

export PYTHONUNBUFFERED=1
# Gate background scheduler; set to 0 on non-leader instances
export RUN_SCHEDULER="${RUN_SCHEDULER:-1}"

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
THREADS="${THREADS:-8}"
CONN_LIMIT="${CONN_LIMIT:-1024}"
BACKLOG="${BACKLOG:-2048}"
CHANNEL_TIMEOUT="${CHANNEL_TIMEOUT:-120}"
IDENT="${IDENT:-reporting-api}"

# Preflight: terminate any process listening on PORT (opt-in)
KILL_OLD="${KILL_OLD:-1}"
if [ "${KILL_OLD}" = "1" ]; then
  if command -v lsof >/dev/null 2>&1; then
    PIDS=$(lsof -t -iTCP:"${PORT}" -sTCP:LISTEN || true)
    if [ -n "${PIDS:-}" ]; then
      echo "[preflight] Terminating existing process(es) on ${PORT}: ${PIDS}"
      kill ${PIDS} || true
      sleep 1
    fi
  elif command -v fuser >/dev/null 2>&1; then
    if fuser "${PORT}/tcp" >/dev/null 2>&1; then
      echo "[preflight] Terminating existing process on ${PORT} (fuser)"
      fuser -k "${PORT}/tcp" || true
      sleep 1
    fi
  fi
fi

# Start Waitress serving the ASGI app via WSGI adapter (wsgi:application)
HOT_RELOAD="${HOT_RELOAD:-0}"
BASE_CMD=(waitress-serve \
  --listen="${HOST}:${PORT}" \
  --threads="${THREADS}" \
  --connection-limit="${CONN_LIMIT}" \
  --backlog="${BACKLOG}" \
  --channel-timeout="${CHANNEL_TIMEOUT}" \
  --ident="${IDENT}" \
  wsgi:application)

if [ "${HOT_RELOAD}" = "1" ]; then
  if command -v watchmedo >/dev/null 2>&1; then
    echo "[watch] Using watchmedo auto-restart for hot reload (waitress)"
    exec watchmedo auto-restart \
      --directory=app \
      --patterns="*.py;*.html;*.css;*.js" \
      --recursive -- "${BASE_CMD[@]}"
  else
    echo "[watch] watchmedo not found; please pip install watchdog to enable hot reload"
    exec "${BASE_CMD[@]}"
  fi
else
  exec "${BASE_CMD[@]}"
fi
