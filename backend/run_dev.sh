#!/usr/bin/env bash
set -Eeuo pipefail
# Resolve script directory (handles spaces)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Optional: activate venv for environment variables and tooling
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  . "$SCRIPT_DIR/venv/bin/activate"
fi

# Ensure venv python exists
if [ ! -x "$SCRIPT_DIR/venv/bin/python" ]; then
  echo "Error: venv python not found at $SCRIPT_DIR/venv/bin/python" >&2
  echo "Tip: create it with: python3 -m venv venv && ./venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

# Run backend using relative venv python
exec "$SCRIPT_DIR/venv/bin/python" -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
