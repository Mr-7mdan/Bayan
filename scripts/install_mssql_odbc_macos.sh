#!/usr/bin/env bash
set -euo pipefail
# Install Microsoft ODBC Driver 18 for SQL Server on macOS using Homebrew
# Also installs unixODBC which pyodbc needs.
# Usage: ./scripts/install_mssql_odbc_macos.sh

# Ensure Homebrew exists
if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew not found. Install from https://brew.sh first." >&2
  exit 1
fi

# Fix a common tap mismatch (remote URL case mismatch)
# If an old/mismatched tap exists, untap it so the correct one can be added.
if brew tap | grep -qi '^microsoft/mssql-release$'; then
  # Verify remote matches the official lowercase org
  REMOTE_URL=$(brew tap-info microsoft/mssql-release --json | grep -E '"remote":' | sed -E 's/.*"remote":\s*"([^"]+)".*/\1/') || true
  if [[ "$REMOTE_URL" != *"github.com/microsoft/homebrew-mssql-release"* ]]; then
    echo "Found mismatched tap remote ($REMOTE_URL). Untapping and re-tapping..."
    brew untap microsoft/mssql-release || true
  fi
fi

# Tap the official repo (lowercase org)
brew tap microsoft/mssql-release https://github.com/microsoft/homebrew-mssql-release || true

# Update, accept EULA, and install the driver
export ACCEPT_EULA=Y
brew update
brew install msodbcsql18 || true
brew install unixodbc || true

cat <<EOF

[OK] Microsoft ODBC Driver 18 for SQL Server installed.
Verify with:
  odbcinst -j  # shows ODBC config paths
  python -c "import pyodbc; print(pyodbc.drivers())"

If drivers() lists 'ODBC Driver 18 for SQL Server', you're set for mssql+pyodbc.
EOF
