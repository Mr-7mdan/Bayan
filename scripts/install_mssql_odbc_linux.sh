#!/usr/bin/env bash
set -euo pipefail
# Install Microsoft ODBC Driver 18 for SQL Server on Linux
# Supports Debian/Ubuntu (apt) and RHEL/CentOS/Amazon Linux (dnf/yum).
# Usage: sudo ./scripts/install_mssql_odbc_linux.sh

if [[ $EUID -ne 0 ]]; then
  echo "Please run as root (use sudo)." >&2
  exit 1
fi

# Parse OS info
source /etc/os-release || true
ID_LIKE=${ID_LIKE:-}
ID=${ID:-}
VERSION_ID=${VERSION_ID:-}

install_debian_like() {
  echo "Installing msodbcsql18 on Debian/Ubuntu..."
  apt-get update -y
  apt-get install -y curl gnupg apt-transport-https software-properties-common
  curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
  case "${VERSION_ID}" in
    24.04)
      echo "deb [arch=amd64,arm64] https://packages.microsoft.com/ubuntu/24.04/prod noble main" > /etc/apt/sources.list.d/mssql-release.list
      ;;
    22.04)
      echo "deb [arch=amd64,arm64] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" > /etc/apt/sources.list.d/mssql-release.list
      ;;
    20.04)
      echo "deb [arch=amd64,arm64] https://packages.microsoft.com/ubuntu/20.04/prod focal main" > /etc/apt/sources.list.d/mssql-release.list
      ;;
    *)
      # Fallback for Debian 12/11
      if [[ "${ID}" == "debian" ]]; then
        if [[ "${VERSION_ID}" == "12" ]]; then
          echo "deb [arch=amd64,arm64] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list
        else
          echo "deb [arch=amd64,arm64] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list
        fi
      else
        echo "Unsupported Ubuntu/Debian version (${VERSION_ID}), attempting Ubuntu 22.04 repo." >&2
        echo "deb [arch=amd64,arm64] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" > /etc/apt/sources.list.d/mssql-release.list
      fi
      ;;
  esac
  export ACCEPT_EULA=Y
  apt-get update -y
  apt-get install -y msodbcsql18 unixodbc-dev
}

install_rhel_like() {
  echo "Installing msodbcsql18 on RHEL/CentOS/Amazon..."
  if command -v dnf >/dev/null 2>&1; then
    PKG=dnf
  else
    PKG=yum
  fi
  ${PKG} install -y curl gnupg
  # Add Microsoft repo (adjusts for RHEL/centos/amzn)
  if [[ "${ID}" == "amzn" ]]; then
    rpm -Uvh https://packages.microsoft.com/config/amazon/2/prod.repo || true
  elif [[ "${VERSION_ID%%.*}" -ge 9 ]]; then
    rpm -Uvh https://packages.microsoft.com/config/rhel/9/prod.rpm || true
  else
    rpm -Uvh https://packages.microsoft.com/config/rhel/8/prod.rpm || true
  fi
  export ACCEPT_EULA=Y
  ${PKG} install -y msodbcsql18 unixODBC-devel
}

if [[ "${ID}" =~ (debian|ubuntu) || "${ID_LIKE}" =~ (debian|ubuntu) ]]; then
  install_debian_like
elif [[ "${ID}" =~ (rhel|centos|rocky|almalinux|amzn|fedora) || "${ID_LIKE}" =~ (rhel|fedora|centos) ]]; then
  install_rhel_like
else
  echo "Unsupported distro ($ID $VERSION_ID). Install manually from https://learn.microsoft.com/sql/connect/odbc/" >&2
  exit 2
fi

echo "[OK] Microsoft ODBC Driver 18 installed. Verify with:"
echo "  odbcinst -j"
echo "  python3 -c 'import pyodbc; print(pyodbc.drivers())'"
