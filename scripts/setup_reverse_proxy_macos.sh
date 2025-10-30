#!/usr/bin/env bash
set -euo pipefail

# Caddy reverse proxy setup for macOS (brew)
# - Proxies / -> Next.js (default 127.0.0.1:3000)
# - Proxies /api -> FastAPI (default 127.0.0.1:8000)
# - If a domain is provided, enables HTTPS via Caddy/ACME (ports 80/443)
# - If domain is blank, serves HTTP on :80 only

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This script is for macOS (Darwin) only." >&2
  exit 1
fi

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required. Install it from https://brew.sh and re-run this script." >&2
  exit 1
fi

read -rp "Domain (e.g. example.com). Leave blank for HTTP-only on :80: " DOMAIN || true
read -rp "Next.js upstream [127.0.0.1:3000]: " WEB_UP || true
WEB_UP=${WEB_UP:-127.0.0.1:3000}
read -rp "FastAPI upstream [127.0.0.1:8000]: " API_UP || true
API_UP=${API_UP:-127.0.0.1:8000}
EMAIL=""
if [[ -n "${DOMAIN}" ]]; then
  read -rp "Email for TLS notifications (optional, e.g. ops@example.com): " EMAIL || true
fi

# Install Caddy if missing
if ! command -v caddy >/dev/null 2>&1; then
  echo "Installing Caddy via Homebrew..."
  brew install caddy
fi

# Optionally install Redis
read -rp "Install and start Redis locally? [y/N]: " INSTALL_REDIS || true
if [[ "${INSTALL_REDIS,,}" == "y" || "${INSTALL_REDIS,,}" == "yes" ]]; then
  if ! command -v redis-server >/dev/null 2>&1; then
    echo "Installing Redis via Homebrew..."
    brew install redis
  fi
  echo "Starting Redis via brew services..."
  brew services start redis || true
fi

# Determine brew prefix & Caddyfile path
BREW_PREFIX="$(brew --prefix)"
CADDY_ETC_DIR="${BREW_PREFIX}/etc/caddy"
mkdir -p "${CADDY_ETC_DIR}"
CADDYFILE="${CADDY_ETC_DIR}/Caddyfile"

# Build Caddyfile
TMP_FILE="$(mktemp)"
{
  if [[ -n "${EMAIL}" ]]; then
    echo "{"
    echo "  email ${EMAIL}"
    echo "}"
    echo
  fi

  if [[ -n "${DOMAIN}" ]]; then
    echo "${DOMAIN} {"
  else
    echo ":80 {"
    echo "  auto_https off"
  fi
  echo "  encode zstd gzip"
  echo "  # Route API to FastAPI backend (preserve /api prefix)"
  echo "  handle /api* {"
  echo "    reverse_proxy http://${API_UP}"
  echo "  }"
  echo "  # Everything else -> Next.js"
  echo "  handle {"
  echo "    reverse_proxy http://${WEB_UP}"
  echo "  }"
  echo "}"
} >"${TMP_FILE}"

sudo tee "${CADDYFILE}" >/dev/null <"${TMP_FILE}"
rm -f "${TMP_FILE}"

echo "Validating Caddy config..."
"$(command -v caddy)" validate --config "${CADDYFILE}"

echo "Starting Caddy as a service (requires sudo for ports 80/443)..."
# On macOS, running on :80/:443 typically requires root; brew services with sudo uses LaunchDaemons
sudo brew services restart caddy || sudo brew services start caddy

cat <<EOF

Caddy is configured and started.
- Config: ${CADDYFILE}
- Site:   ${DOMAIN:-:80}
- Upstreams:
  * Next.js -> http://${WEB_UP}
  * FastAPI -> http://${API_UP}

Redis (optional):
- If installed and started, set in backend/.env:
  REDIS_URL=redis://127.0.0.1:6379/0
  REDIS_PREFIX=ratelimit
  RESULT_CACHE_TTL=5

If you provided a public domain, ensure inbound ports 80/443 are open and DNS A/AAAA records point to this host.
Use 'sudo brew services stop caddy' to stop, and 'sudo brew services restart caddy' after edits.
EOF
