# Install Microsoft ODBC Driver 18 for SQL Server on Windows
# Usage (PowerShell as Administrator):
#   Set-ExecutionPolicy Bypass -Scope Process -Force
#   .\scripts\install_mssql_odbc_windows.ps1

$ErrorActionPreference = 'Stop'

function Has-Command($name) {
  $cmd = Get-Command $name -ErrorAction SilentlyContinue
  return $null -ne $cmd
}

Write-Host "Installing Microsoft ODBC Driver 18 for SQL Server..." -ForegroundColor Cyan

$installed = $false

# Prefer winget
if (Has-Command 'winget') {
  try {
    winget install --id Microsoft.ODBCDriverForSQLServer.18 -e --accept-source-agreements --accept-package-agreements
    $installed = $true
  } catch {
    Write-Warning "winget install failed: $($_.Exception.Message)"
  }
}

# Fallback to Chocolatey
if (-not $installed -and (Has-Command 'choco')) {
  try {
    choco install microsoft-odbc-driver-18-sqlserver -y
    $installed = $true
  } catch {
    Write-Warning "choco install failed: $($_.Exception.Message)"
  }
}

if (-not $installed) {
  Write-Warning "winget/choco not available. Please download the MSI from Microsoft:"
  Write-Host "https://learn.microsoft.com/sql/connect/odbc/windows/release-notes-odbc-sql-server-windows" -ForegroundColor Yellow
  Write-Host "After installation, re-run verification below."
} else {
  Write-Host "[OK] ODBC driver install attempted. Verifying..." -ForegroundColor Green
}

try {
  # Try to show installed ODBC drivers via .NET API
  Add-Type -AssemblyName System.Data
  $table = [System.Data.Odbc.OdbcEnumerator]::GetEnumerator().GetRows()
  $drivers = $table | ForEach-Object { $_.Driver } | Sort-Object -Unique
  Write-Host "ODBC Drivers:" -ForegroundColor Cyan
  $drivers | ForEach-Object { Write-Host "  - $_" }
} catch {
  Write-Warning "Could not enumerate ODBC drivers via .NET: $($_.Exception.Message)"
}

Write-Host "If 'ODBC Driver 18 for SQL Server' appears, pyodbc can use it." -ForegroundColor Green
Write-Host "You can also verify via Python:" -ForegroundColor Cyan
Write-Host "  py -c \"import pyodbc; print(pyodbc.drivers())\"" -ForegroundColor Yellow
