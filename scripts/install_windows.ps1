#requires -RunAsAdministrator
param(
  [string]$RepoOwner = "Mr-7mdan",
  [string]$RepoName = "Bayan",
  [string]$InstallDir = "C:\Bayan",
  [string]$BackendHost = "127.0.0.1",
  [int]$BackendPort = 8000,
  [int]$FrontendPort = 3000,
  [string]$AdminEmail = "admin@example.com",
  [string]$AdminPassword = "StrongPass123!",
  [string]$AdminName = "Admin"
)

$ErrorActionPreference = 'Stop'
function Write-Heading($msg) { Write-Host "==== $msg ==== " -ForegroundColor Cyan }
function Test-Admin {
  $id = [Security.Principal.WindowsIdentity]::GetCurrent()
  $p = New-Object Security.Principal.WindowsPrincipal($id)
  return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}
if (-not (Test-Admin)) { Write-Error "Please run this script as Administrator."; exit 1 }

$env:Path = [System.Environment]::GetEnvironmentVariable('Path','Machine') + ';' + [System.Environment]::GetEnvironmentVariable('Path','User')

function Ensure-Dir($path){ if (-not (Test-Path $path)) { New-Item -ItemType Directory -Force -Path $path | Out-Null } }
function Ensure-Command($name){ return (Get-Command $name -ErrorAction SilentlyContinue) -ne $null }
function Install-WithWinget($id){
  if (-not (Ensure-Command winget)) {
    Write-Warning "winget not found. Please install App Installer from Microsoft Store or run scripts/install_winget.ps1"
    return $false
  }
  try {
    Write-Host "Installing $id via winget..."
    winget install --id $id -e --accept-package-agreements --accept-source-agreements --silent | Out-Null
    return $true
  } catch { return $false }
}
function Install-Git {
  if (Ensure-Command git) { return }
  if (-not (Install-WithWinget 'Git.Git')) { throw "Failed to install Git. Please install Git and rerun." }
}
function Install-Python {
  # Prefer Python 3.12
  $hasPy = $false
  try { $v = (& py -3.12 -V) 2>$null; if ($LASTEXITCODE -eq 0) { $hasPy = $true } } catch {}
  if (-not $hasPy) {
    if (-not (Install-WithWinget 'Python.Python.3.12')) { throw "Failed to install Python 3.12. Please install and rerun." }
  }
}
function Install-Node {
  if (Ensure-Command node) { return }
  if (-not (Install-WithWinget 'OpenJS.NodeJS.LTS')) { throw "Failed to install Node.js LTS. Please install and rerun." }
}
function Install-NSSM {
  $nssmTarget = "C:\Program Files\nssm\nssm.exe"
  if (Test-Path $nssmTarget) { return $nssmTarget }
  if (Ensure-Command choco) {
    try { choco install nssm -y | Out-Null } catch {}
    if (Test-Path $nssmTarget) { return $nssmTarget }
  }
  Write-Host "Downloading NSSM..."
  $tmp = Join-Path $env:TEMP ("nssm-" + [guid]::NewGuid() + ".zip")
  $url = 'https://nssm.cc/release/nssm-2.24.zip'
  Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile $tmp
  $tmpDir = Join-Path $env:TEMP ("nssm-" + [guid]::NewGuid())
  Expand-Archive -LiteralPath $tmp -DestinationPath $tmpDir -Force
  Ensure-Dir "C:\Program Files\nssm"
  $cand = Get-ChildItem -Path $tmpDir -Recurse -Filter nssm.exe | Where-Object { $_.DirectoryName -match 'win64' } | Select-Object -First 1
  if (-not $cand) { $cand = Get-ChildItem -Path $tmpDir -Recurse -Filter nssm.exe | Select-Object -First 1 }
  if (-not $cand) { throw "Failed to locate nssm.exe in archive." }
  Copy-Item $cand.FullName "C:\Program Files\nssm\nssm.exe" -Force
  Remove-Item $tmp -Force -ErrorAction SilentlyContinue
  Remove-Item $tmpDir -Force -Recurse -ErrorAction SilentlyContinue
  return $nssmTarget
}

function Random-Secret($length=32){ $b = New-Object byte[] $length; [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($b); [Convert]::ToBase64String($b).TrimEnd('=') }

Write-Heading "Installing prerequisites"
Install-Git
Install-Python
Install-Node
$nssm = Install-NSSM

Write-Heading "Cloning or updating repository"
if (-not (Test-Path $InstallDir)) { Ensure-Dir $InstallDir }
if (-not (Test-Path (Join-Path $InstallDir '.git'))) {
  git clone "https://github.com/$RepoOwner/$RepoName.git" "$InstallDir"
} else {
  git -C "$InstallDir" fetch --all --prune
  git -C "$InstallDir" checkout main
  git -C "$InstallDir" reset --hard origin/main
}

$backendDir = Join-Path $InstallDir 'backend'
$frontendDir = Join-Path $InstallDir 'frontend'
Ensure-Dir (Join-Path $InstallDir 'logs')

Write-Heading "Configuring backend (.env)"
$envFile = Join-Path $backendDir '.env'
$envExample = Join-Path $backendDir '.env.example'
if (-not (Test-Path $envFile)) {
  if (Test-Path $envExample) { Copy-Item $envExample $envFile -Force } else { New-Item -ItemType File -Path $envFile | Out-Null }
}
# Load and patch basic keys
$secret = Random-Secret 48
$content = Get-Content -Raw -LiteralPath $envFile
$existingSecret = $null
if ($content -match '(?m)^\s*SECRET_KEY\s*=\s*(.*)\s*$') { $existingSecret = $Matches[1].Trim() }
if (-not $existingSecret) {
  if ($content -match '(?m)^\s*SECRET_KEY\s*=') { $content = ($content -replace '(?m)^\s*SECRET_KEY\s*=.*$', "SECRET_KEY=$secret") }
  else { $content += "`nSECRET_KEY=$secret" }
}
# CORS origins
if ($content -notmatch '^CORS_ORIGINS=') { $content += "`nCORS_ORIGINS=http://localhost:${FrontendPort},http://127.0.0.1:${FrontendPort}" }
# Admin bootstrap
if ($content -notmatch '^ADMIN_EMAIL=') { $content += "`nADMIN_EMAIL=$AdminEmail" } else { $content = ($content -replace '^ADMIN_EMAIL=.*', "ADMIN_EMAIL=$AdminEmail") }
if ($content -notmatch '^ADMIN_PASSWORD=') { $content += "`nADMIN_PASSWORD=$AdminPassword" } else { $content = ($content -replace '^ADMIN_PASSWORD=.*', "ADMIN_PASSWORD=$AdminPassword") }
if ($content -notmatch '^ADMIN_NAME=') { $content += "`nADMIN_NAME=$AdminName" } else { $content = ($content -replace '^ADMIN_NAME=.*', "ADMIN_NAME=$AdminName") }
Set-Content -LiteralPath $envFile -Value $content -Encoding UTF8

Write-Heading "Setting up backend virtual environment"
Push-Location $backendDir
# Prefer Python launcher for 3.12
$venvPath = Join-Path $backendDir 'venv'
if (-not (Test-Path (Join-Path $venvPath 'Scripts\python.exe'))) {
  if (Ensure-Command py) { py -3.12 -m venv venv } else { python -m venv venv }
}
& "$venvPath\Scripts\python.exe" -m pip install --upgrade pip
& "$venvPath\Scripts\pip.exe" install -r (Join-Path $backendDir 'requirements.txt')
Pop-Location

Write-Heading "Setting up frontend (install and build)"
Push-Location $frontendDir
# Try npm ci first; fallback to npm install if lock file is out of sync
$npmOk = $false
if (-not (Test-Path (Join-Path $frontendDir 'node_modules'))) {
  try { npm ci 2>&1 | Out-Null; $npmOk = $true } catch {}
}
if (-not $npmOk) {
  Write-Host "Running npm install (lock file may be stale)..."
  npm install
}
$env:NODE_ENV = 'production'
# Use same-origin API behind the reverse proxy
$env:NEXT_PUBLIC_API_BASE_URL = "/api"
# Build Next.js app
npm run build
Pop-Location

Write-Heading "Configuring reverse proxy (Caddy)"
$proxyScript = Join-Path $InstallDir 'scripts\setup_reverse_proxy_windows.ps1'
if (Test-Path $proxyScript) {
  # Run the proxy setup (prompts for domain and upstreams). This may register a Caddy service which we'll replace with NSSM for consistency.
  & powershell -ExecutionPolicy Bypass -File $proxyScript
} else {
  Write-Host "Proxy script not found at $proxyScript. Skipping reverse proxy setup."
}

Write-Heading "Creating and starting services with NSSM"
$backendSvc = 'BayanAPIUvicorn'
$frontendSvc = 'BayanUI'
$cmdExe = 'C:\Windows\System32\cmd.exe'

# Backend service
$backendBat = Join-Path $backendDir 'run_prod_uvicorn_windows.bat'
if (-not (Test-Path $backendBat)) { throw "Missing $backendBat" }
$exists = (Start-Process -FilePath $nssm -ArgumentList @('status', $backendSvc) -PassThru -NoNewWindow -Wait -ErrorAction SilentlyContinue).ExitCode -eq 0
if (-not $exists) {
  & $nssm install $backendSvc $cmdExe '/c', $backendBat
}
& $nssm set $backendSvc AppDirectory $backendDir
& $nssm set $backendSvc Start SERVICE_AUTO_START
& $nssm set $backendSvc AppStdout (Join-Path $InstallDir 'logs\backend.out.log')
& $nssm set $backendSvc AppStderr (Join-Path $InstallDir 'logs\backend.err.log')
try { & $nssm stop $backendSvc | Out-Null } catch {}
& $nssm start $backendSvc | Out-Null

# Frontend service
$frontendBat = Join-Path $frontendDir 'run_frontend_prod.bat'
if (-not (Test-Path $frontendBat)) { throw "Missing $frontendBat" }
$exists2 = (Start-Process -FilePath $nssm -ArgumentList @('status', $frontendSvc) -PassThru -NoNewWindow -Wait -ErrorAction SilentlyContinue).ExitCode -eq 0
if (-not $exists2) {
  & $nssm install $frontendSvc $cmdExe '/c', $frontendBat
}
& $nssm set $frontendSvc AppDirectory $frontendDir
& $nssm set $frontendSvc Start SERVICE_AUTO_START
& $nssm set $frontendSvc AppStdout (Join-Path $InstallDir 'logs\frontend.out.log')
& $nssm set $frontendSvc AppStderr (Join-Path $InstallDir 'logs\frontend.err.log')
# Optionally set env overrides for the service (uncomment to customize)
# & $nssm set $frontendSvc AppEnvironmentExtra "PORT=${FrontendPort}`nHOST=0.0.0.0`nNODE_ENV=production`nNEXT_PUBLIC_API_BASE_URL=http://${BackendHost}:${BackendPort}/api"
try { & $nssm stop $frontendSvc | Out-Null } catch {}
& $nssm start $frontendSvc | Out-Null

# Reverse proxy (Caddy) service via NSSM
$proxySvc = 'BayanProxy'
$caddyExe = ($null)
try { $caddyExe = (Get-Command caddy -ErrorAction SilentlyContinue).Source } catch {}
if ($caddyExe) {
  # Remove Caddy's own Windows service to avoid conflicts (ignore errors if not present)
  try { caddy stop-service 2>$null | Out-Null } catch {}
  try { caddy remove-service 2>$null | Out-Null } catch {}

  $exists3 = (Start-Process -FilePath $nssm -ArgumentList @('status', $proxySvc) -PassThru -NoNewWindow -Wait -ErrorAction SilentlyContinue).ExitCode -eq 0
  if (-not $exists3) {
    & $nssm install $proxySvc $caddyExe 'run' '--config' 'C:\ProgramData\Caddy\Caddyfile' '--watch'
  }
  & $nssm set $proxySvc AppDirectory 'C:\ProgramData\Caddy'
  & $nssm set $proxySvc Start SERVICE_AUTO_START
  & $nssm set $proxySvc AppStdout (Join-Path $InstallDir 'logs\proxy.out.log')
  & $nssm set $proxySvc AppStderr (Join-Path $InstallDir 'logs\proxy.err.log')
  try { & $nssm stop $proxySvc | Out-Null } catch {}
  & $nssm start $proxySvc | Out-Null
  # Ensure firewall rules (idempotent)
  if (-not (Get-NetFirewallRule -DisplayName 'Caddy HTTP 80' -ErrorAction SilentlyContinue)) { New-NetFirewallRule -DisplayName 'Caddy HTTP 80' -Direction Inbound -LocalPort 80 -Protocol TCP -Action Allow | Out-Null }
  if (-not (Get-NetFirewallRule -DisplayName 'Caddy HTTPS 443' -ErrorAction SilentlyContinue)) { New-NetFirewallRule -DisplayName 'Caddy HTTPS 443' -Direction Inbound -LocalPort 443 -Protocol TCP -Action Allow | Out-Null }
} else {
  Write-Host 'caddy.exe not found in PATH; skipping proxy service registration. Run scripts/setup_reverse_proxy_windows.ps1 first.'
}

Write-Heading "Done"
Write-Host "Backend: http://${BackendHost}:${BackendPort}/api/healthz"
Write-Host "Frontend (Next.js upstream): http://127.0.0.1:${FrontendPort}"
Write-Host "Reverse proxy (Caddy): http(s)://<your-domain>  (serving / -> Next.js, /api -> FastAPI)"
