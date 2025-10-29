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
# IMPORTANT: with multiple Gunicorn workers, do NOT run the scheduler in each worker
# Set RUN_SCHEDULER=0 here and run a single separate scheduler instance if needed
export RUN_SCHEDULER="${RUN_SCHEDULER:-0}"

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
LOG_LEVEL="${LOG_LEVEL:-info}"

# Determine CPU count via Python (portable)
CPU_COUNT=$(python3 - <<'PY'
import os
print(os.cpu_count() or 2)
PY
)
# Workers: WEB_CONCURRENCY overrides, else 2*CPU + 1
if [ -n "${WEB_CONCURRENCY:-}" ]; then
  WORKERS="${WEB_CONCURRENCY}"
else
  WORKERS=$(( CPU_COUNT * 2 + 1 ))
fi

KEEP_ALIVE="${KEEP_ALIVE:-75}"
TIMEOUT="${TIMEOUT:-60}"
GRACEFUL_TIMEOUT="${GRACEFUL_TIMEOUT:-30}"
MAX_REQUESTS="${MAX_REQUESTS:-1000}"
MAX_REQUESTS_JITTER="${MAX_REQUESTS_JITTER:-100}"

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

# Start Gunicorn with Uvicorn workers (ASGI)
HOT_RELOAD="${HOT_RELOAD:-0}"
BASE_CMD=(gunicorn app.main:app \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind "${HOST}:${PORT}" \
  --workers "${WORKERS}" \
  --keep-alive "${KEEP_ALIVE}" \
  --timeout "${TIMEOUT}" \
  --graceful-timeout "${GRACEFUL_TIMEOUT}" \
  --max-requests "${MAX_REQUESTS}" \
  --max-requests-jitter "${MAX_REQUESTS_JITTER}" \
  --access-logfile '-' \
  --error-logfile '-' \
  --log-level "${LOG_LEVEL}")

if [ "${HOT_RELOAD}" = "1" ]; then
  if command -v watchmedo >/dev/null 2>&1; then
    echo "[watch] Using watchmedo auto-restart for hot reload"
    exec watchmedo auto-restart \
      --directory=app \
      --patterns="*.py;*.html;*.css;*.js" \
      --recursive -- "${BASE_CMD[@]}"
  else
    echo "[watch] watchmedo not found; falling back to gunicorn --reload"
    exec "${BASE_CMD[@]}" --reload
  fi
else
  exec "${BASE_CMD[@]}"
fi
