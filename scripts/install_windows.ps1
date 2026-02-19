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

# Ensure Chocolatey is installed (fallback when winget is unavailable)
function Ensure-Choco {
  if (Ensure-Command choco) { return $true }
  Write-Host "Chocolatey not found. Installing Chocolatey..."
  try {
    Set-ExecutionPolicy Bypass -Scope Process -Force | Out-Null
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
    Invoke-Expression ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
    # Refresh PATH for current session
    $env:Path = [System.Environment]::GetEnvironmentVariable('Path','Machine') + ';' + [System.Environment]::GetEnvironmentVariable('Path','User')
  } catch {
    Write-Warning "Failed to install Chocolatey automatically: $($_.Exception.Message)"
  }
  return (Ensure-Command choco)
}
function Install-Git {
  if (Ensure-Command git) { return }
  if (Install-WithWinget 'Git.Git') { return }
  if (Ensure-Choco) {
    try { choco install git -y | Out-Null; return } catch {}
  }
  throw "Failed to install Git via winget or Chocolatey. Please install Git manually and rerun."
}
function Install-Python {
  # Prefer Python 3.12
  $hasPy = $false
  try { $v = (& py -3.12 -V) 2>$null; if ($LASTEXITCODE -eq 0) { $hasPy = $true } } catch {}
  if (-not $hasPy) {
    if (Install-WithWinget 'Python.Python.3.12') { return }
    if (Ensure-Choco) {
      try { choco install python --version=3.12.7 -y | Out-Null; return } catch { try { choco install python -y | Out-Null; return } catch {} }
    }
    throw "Failed to install Python 3.12 via winget or Chocolatey. Please install Python 3.12 and rerun."
  }
}
function Install-Node {
  if (Ensure-Command node) { return }
  if (Install-WithWinget 'OpenJS.NodeJS.LTS') { return }
  if (Ensure-Choco) {
    try { choco install nodejs-lts -y | Out-Null; return } catch {}
  }
  throw "Failed to install Node.js LTS via winget or Chocolatey. Please install Node.js LTS and rerun."
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

Write-Heading "Stopping running services (if any)"
foreach ($svc in @('BayanAPIUvicorn','BayanUI','BayanProxy')) {
  try { & $nssm stop $svc | Out-Null } catch {}
}

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
$caddyConfigDir = 'C:\ProgramData\Caddy'
$caddyFilePath = Join-Path $caddyConfigDir 'Caddyfile'
$proxyScript = $null

if (Test-Path $caddyFilePath) {
  Write-Host "Existing Caddy configuration detected at $caddyFilePath. Skipping reverse proxy setup. To change domain or upstreams later, run scripts\setup_reverse_proxy_windows.ps1 manually."
} else {
  # Prefer ProgramData location to avoid spaces in path
  $caddyPreferred = 'C:\ProgramData\Caddy\caddy.exe'
  # Check if winget is available for Caddy installation
  if (-not (Ensure-Command winget)) {
    if (Test-Path $caddyPreferred) {
      Write-Host "Found Caddy at $caddyPreferred"
      if (-not (Get-Command caddy -ErrorAction SilentlyContinue)) { Set-Alias -Name caddy -Value $caddyPreferred -Scope Global }
      try { & $caddyPreferred version | Out-Null } catch {}
      $proxyScript = Join-Path $InstallDir 'scripts\setup_reverse_proxy_windows.ps1'
    } else {
      Write-Warning "winget not found. Caddy automatic install unavailable."
      $defaultCaddyDir = 'C:\ProgramData\Caddy'
      $defaultCaddyExe = Join-Path $defaultCaddyDir 'caddy.exe'
      Write-Host "Manual steps (recommended location):" -ForegroundColor Cyan
      Write-Host "  1) Download Caddy (Windows amd64) from: https://caddyserver.com/download"
      Write-Host "  2) Rename the downloaded file (e.g., caddy_windows_amd64.exe) to: caddy.exe"
      Write-Host "  3) Create folder: $defaultCaddyDir"
      Write-Host "  4) Move caddy.exe to: $defaultCaddyExe"
      Write-Host ""
      $cont = Read-Host "Press Enter when done to continue, or type 'S' to skip proxy setup"
      if ($cont -match '^(?i:s|skip)$') { Write-Host "Skipping proxy setup."; $proxyScript = $null }
      else {
        $caddyPathInput = Read-Host "If you used a different location, enter full path to caddy.exe (or press Enter to use default)"
        $caddyExePath = if ([string]::IsNullOrWhiteSpace($caddyPathInput)) { $defaultCaddyExe } else { $caddyPathInput }
        if (-not (Test-Path $caddyExePath)) {
          Write-Warning "caddy.exe not found at '$caddyExePath'. Skipping proxy setup."
          $proxyScript = $null
        } else {
          $caddyDir = Split-Path $caddyExePath -Parent
          # Add to current PATH
          if ($env:Path -notlike ("*" + $caddyDir + "*")) { $env:Path += ";" + $caddyDir }
          # Persist to machine PATH
          try {
            $machinePath = [Environment]::GetEnvironmentVariable('Path','Machine')
            if ($machinePath -notlike ("*" + $caddyDir + "*")) {
              [Environment]::SetEnvironmentVariable('Path', $machinePath + ';' + $caddyDir, 'Machine')
            }
          } catch {}
          # Ensure 'caddy' resolves in this session
          if (-not (Get-Command caddy -ErrorAction SilentlyContinue)) { Set-Alias -Name caddy -Value $caddyExePath -Scope Global }
          try { & $caddyExePath version | Out-Null } catch {}
          $proxyScript = Join-Path $InstallDir 'scripts\setup_reverse_proxy_windows.ps1'
        }
      }
    }
  } else {
    $proxyScript = Join-Path $InstallDir 'scripts\setup_reverse_proxy_windows.ps1'
  }

  if ($proxyScript) {
    if (Test-Path $proxyScript) {
      # Run the proxy setup (prompts for domain and upstreams). This may register a Caddy service which we'll replace with NSSM for consistency.
      & powershell -ExecutionPolicy Bypass -File $proxyScript
    } else {
      Write-Host "Proxy script not found at $proxyScript. Skipping reverse proxy setup."
    }
  }
}

# Ensure logs directory exists
$logsDir = Join-Path $InstallDir 'logs'
if (-not (Test-Path $logsDir)) { New-Item -ItemType Directory -Path $logsDir | Out-Null }

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
# Log rotation and restart throttling
& $nssm set $backendSvc AppRotateFiles 1
& $nssm set $backendSvc AppRotateOnline 1
& $nssm set $backendSvc AppRotateBytes 10485760        # 10 MB max per file
& $nssm set $backendSvc AppRotateSeconds 86400         # rotate at least daily
& $nssm set $backendSvc AppRestartDelay 60000          # wait 60s before restart
& $nssm set $backendSvc AppThrottle 15000              # throttle rapid restarts
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
# Log rotation and restart throttling
& $nssm set $frontendSvc AppRotateFiles 1
& $nssm set $frontendSvc AppRotateOnline 1
& $nssm set $frontendSvc AppRotateBytes 10485760       # 10 MB max per file
& $nssm set $frontendSvc AppRotateSeconds 86400        # rotate at least daily
& $nssm set $frontendSvc AppRestartDelay 60000         # wait 60s before restart
& $nssm set $frontendSvc AppThrottle 15000             # throttle rapid restarts
# Optionally set env overrides for the service (uncomment to customize)
# & $nssm set $frontendSvc AppEnvironmentExtra "PORT=${FrontendPort}`nHOST=0.0.0.0`nNODE_ENV=production`nNEXT_PUBLIC_API_BASE_URL=http://${BackendHost}:${BackendPort}/api"
try { & $nssm stop $frontendSvc | Out-Null } catch {}
& $nssm start $frontendSvc | Out-Null

# Reverse proxy (Caddy) service via NSSM
$proxySvc = 'BayanProxy'
$caddyExe = $null
$caddyPreferred = 'C:\ProgramData\Caddy\caddy.exe'
if (Test-Path $caddyPreferred) { $caddyExe = $caddyPreferred } else { try { $caddyExe = (Get-Command caddy -ErrorAction SilentlyContinue).Source } catch {} }
if ($caddyExe) {
  # Remove Caddy's own Windows service to avoid conflicts (ignore errors if not present)
  try { caddy stop-service 2>$null | Out-Null } catch {}
  try { caddy remove-service 2>$null | Out-Null } catch {}

  $exists3 = (Start-Process -FilePath $nssm -ArgumentList @('status', $proxySvc) -PassThru -NoNewWindow -Wait -ErrorAction SilentlyContinue).ExitCode -eq 0
  if (-not $exists3) {
    & $nssm install $proxySvc $caddyExe 'run' '--config' 'C:\ProgramData\Caddy\Caddyfile' '--watch'
  }
  # Always enforce ProgramData caddy.exe and parameters on existing/new service
  & $nssm set $proxySvc Application $caddyExe
  & $nssm set $proxySvc AppParameters 'run --config C:\\ProgramData\\Caddy\\Caddyfile --watch'
  & $nssm set $proxySvc AppDirectory 'C:\ProgramData\Caddy'
  & $nssm set $proxySvc Start SERVICE_AUTO_START
  & $nssm set $proxySvc AppStdout (Join-Path $InstallDir 'logs\proxy.out.log')
  & $nssm set $proxySvc AppStderr (Join-Path $InstallDir 'logs\proxy.err.log')
  # Log rotation and restart throttling
  & $nssm set $proxySvc AppRotateFiles 1
  & $nssm set $proxySvc AppRotateOnline 1
  & $nssm set $proxySvc AppRotateBytes 10485760        # 10 MB max per file
  & $nssm set $proxySvc AppRotateSeconds 86400         # rotate at least daily
  & $nssm set $proxySvc AppRestartDelay 60000          # wait 60s before restart
  & $nssm set $proxySvc AppThrottle 15000              # throttle rapid restarts
  try { & $nssm stop $proxySvc | Out-Null } catch {}
  & $nssm start $proxySvc | Out-Null
  # Ensure firewall rules (idempotent)
  if (-not (Get-NetFirewallRule -DisplayName 'Caddy HTTP 80' -ErrorAction SilentlyContinue)) { New-NetFirewallRule -DisplayName 'Caddy HTTP 80' -Direction Inbound -LocalPort 80 -Protocol TCP -Action Allow | Out-Null }
  if (-not (Get-NetFirewallRule -DisplayName 'Caddy HTTPS 443' -ErrorAction SilentlyContinue)) { New-NetFirewallRule -DisplayName 'Caddy HTTPS 443' -Direction Inbound -LocalPort 443 -Protocol TCP -Action Allow | Out-Null }
} else {
  Write-Host 'caddy.exe not found in PATH; skipping proxy service registration. Run scripts/setup_reverse_proxy_windows.ps1 first.'
}

Write-Heading "Post-install verification"
# Backend direct health
try {
  $r = Invoke-WebRequest -UseBasicParsing -Uri "http://${BackendHost}:${BackendPort}/api/healthz" -TimeoutSec 5
  Write-Host ("Backend health: {0}" -f $r.StatusCode)
} catch {
  Write-Warning ("Backend health check failed: " + $_.Exception.Message)
}
# Proxy health (HTTP via localhost)
try {
  $r2 = Invoke-WebRequest -UseBasicParsing -Uri "http://localhost/api/healthz" -TimeoutSec 5
  Write-Host ("Proxy health: {0}" -f $r2.StatusCode)
} catch {
  Write-Warning ("Proxy health check failed (if proxy not configured for :80, this may be expected): " + $_.Exception.Message)
}
# Frontend reachability
try {
  $r3 = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:${FrontendPort}" -TimeoutSec 5
  Write-Host ("Frontend check: {0}" -f $r3.StatusCode)
} catch {
  Write-Warning ("Frontend check failed: " + $_.Exception.Message)
}
# Listener summary
$ports = @(8000,3000,80,443)
foreach ($p in $ports) {
  try {
    $t = Test-NetConnection -ComputerName 127.0.0.1 -Port $p -WarningAction SilentlyContinue
    $status = if ($t.TcpTestSucceeded) { 'LISTENING' } else { 'not listening' }
    Write-Host ("Port {0}: {1}" -f $p, $status)
  } catch {
    Write-Host ("Port {0}: unknown" -f $p)
  }
}

Write-Heading "Done"
Write-Host "Backend: http://${BackendHost}:${BackendPort}/api/healthz"
Write-Host "Frontend (Next.js upstream): http://127.0.0.1:${FrontendPort}"
# Read the domain back from the Caddyfile written by the proxy setup script
$_caddySite = 'localhost'
try {
  $caddyfileSummary = 'C:\ProgramData\Caddy\Caddyfile'
  if (Test-Path $caddyfileSummary) {
    $caddyLines = Get-Content -LiteralPath $caddyfileSummary -ErrorAction SilentlyContinue
    foreach ($line in $caddyLines) {
      $trimmed = $line.Trim()
      # Skip blank lines, comments, and the global block (starts with "{")
      if ($trimmed -eq '' -or $trimmed.StartsWith('#') -or $trimmed -eq '{') { continue }
      # The first site-block header looks like ":80 {" or "example.com {" or "example.com {"
      $siteLine = $trimmed -replace '\s*\{.*$', '' | ForEach-Object { $_.Trim() }
      if ($siteLine -and $siteLine -ne '') { $_caddySite = $siteLine; break }
    }
  }
} catch {}
Write-Host "Reverse proxy (Caddy): http(s)://$_caddySite  (serving / -> Next.js, /api -> FastAPI)"
