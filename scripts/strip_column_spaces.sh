#!/usr/bin/env bash
# strip_column_spaces.sh
# Activates the backend venv and strips whitespace from DuckDB column names.
# Run from Git Bash on Windows:  bash scripts/strip_column_spaces.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(dirname "$SCRIPT_DIR")"
DB_FILE=".data/local-20260220-0436.duckdb"

cd "$BACKEND_DIR"

# Activate venv — Git Bash uses Scripts/, Linux/Mac uses bin/
if [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "ERROR: Could not find venv/Scripts/activate or venv/bin/activate"
    exit 1
fi

echo "Python: $(which python)"
echo "Running strip on: $DB_FILE"
echo ""

python scripts/strip_column_spaces.py "$DB_FILE"
