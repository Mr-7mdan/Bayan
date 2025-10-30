#requires -version 5.1
<#!
Caddy reverse proxy setup for Windows (PowerShell)
- Proxies / -> Next.js (default 127.0.0.1:3000)
- Proxies /api -> FastAPI (default 127.0.0.1:8000)
- If a domain is provided, enables HTTPS via Caddy/ACME (ports 80/443)
- If domain is blank, serves HTTP on :80 only
- Installs Caddy via winget if missing and registers Caddy as a Windows service
#>

$ErrorActionPreference = 'Stop'

function Ensure-Admin {
  $currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
  $principal = New-Object Security.Principal.WindowsPrincipal($currentIdentity)
  if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Error 'This script must be run as Administrator.'
  }
}

function Ensure-Caddy {
  if (-not (Get-Command caddy -ErrorAction SilentlyContinue)) {
    Write-Host 'Caddy not found. Installing via winget...'
    # Winget may require agreement; ensure repo is available
    winget install --id=Light-CaddyServer.Caddy -e --source winget -h 2>$null | Out-Null
  }
  if (-not (Get-Command caddy -ErrorAction SilentlyContinue)) {
    Write-Error 'Caddy installation failed or not in PATH. Please install from https://caddyserver.com and re-run.'
  }
}

Ensure-Admin
Ensure-Caddy

$domain = Read-Host 'Domain (e.g. example.com). Leave blank for HTTP-only on :80'
$webUp = Read-Host 'Next.js upstream [127.0.0.1:3000]'
if ([string]::IsNullOrWhiteSpace($webUp)) { $webUp = '127.0.0.1:3000' }
$apiUp = Read-Host 'FastAPI upstream [127.0.0.1:8000]'
if ([string]::IsNullOrWhiteSpace($apiUp)) { $apiUp = '127.0.0.1:8000' }
$email = ''
if (-not [string]::IsNullOrWhiteSpace($domain)) {
  $email = Read-Host 'Email for TLS notifications (optional, e.g. ops@example.com)'
}

# Optional Redis install/start
$installRedis = Read-Host 'Install and start Redis locally? [y/N]'
if ($installRedis -match '^(?i:y|yes)$') {
  if (-not (Get-Command redis-server -ErrorAction SilentlyContinue)) {
    Write-Host 'Installing Redis via winget (or Memurai fallback)...'
    $ok = $false
    try { winget install -e --id=Redis.Redis -h | Out-Null; $ok = $true } catch {}
    if (-not $ok) {
      try { winget install -e --id=Memurai.MemuraiDeveloper -h | Out-Null; $ok = $true } catch {}
    }
    if (-not $ok) { Write-Warning 'Failed to install Redis via winget; please install manually or use Docker: docker run -p 6379:6379 redis' }
  }
  try {
    # Try to start Redis if installed as a service; ignore if running in console mode
    if (Get-Service -Name 'redis' -ErrorAction SilentlyContinue) { Start-Service redis -ErrorAction SilentlyContinue }
    elseif (Get-Service -Name 'Memurai' -ErrorAction SilentlyContinue) { Start-Service Memurai -ErrorAction SilentlyContinue }
  } catch {}
}

# Paths
$caddyRoot = Join-Path $env:ProgramData 'Caddy'
$caddyEtc = $caddyRoot # keep simple on Windows
$caddyfile = Join-Path $caddyEtc 'Caddyfile'
if (-not (Test-Path $caddyEtc)) { New-Item -ItemType Directory -Path $caddyEtc | Out-Null }

# Build Caddyfile content
$caddyGlobal = ''
if (-not [string]::IsNullOrWhiteSpace($email)) {
  $caddyGlobal = "{`n  email $email`n}`n`n"
}
$siteHeader = if ([string]::IsNullOrWhiteSpace($domain)) { ":80 {`n  auto_https off" } else { "$domain {" }
$caddyContent = @"
${caddyGlobal}${siteHeader}
  encode zstd gzip
  # Route API to FastAPI backend (preserve /api prefix)
  handle /api* {
    reverse_proxy http://$apiUp
  }
  # Everything else -> Next.js
  handle {
    reverse_proxy http://$webUp
  }
}
"@

$caddyContent | Out-File -FilePath $caddyfile -Encoding UTF8 -Force

# Validate config
caddy validate --config "$caddyfile"

# Open firewall for 80/443
Write-Host 'Ensuring Windows Firewall rules for ports 80 and 443...'
$rules = @(
  @{ Name = 'Caddy HTTP 80';  Port = 80;  Protocol = 'TCP' },
  @{ Name = 'Caddy HTTPS 443'; Port = 443; Protocol = 'TCP' }
)
foreach ($r in $rules) {
  if (-not (Get-NetFirewallRule -DisplayName $r.Name -ErrorAction SilentlyContinue)) {
    New-NetFirewallRule -DisplayName $r.Name -Direction Inbound -LocalPort $r.Port -Protocol $r.Protocol -Action Allow | Out-Null
  }
}

# Install/Restart Caddy as a service (runs under Local System)
# Service will run: caddy run --config Caddyfile --watch
Write-Host 'Installing or restarting Caddy Windows service...'
try {
  caddy stop-service 2>$null | Out-Null
} catch {}
try {
  caddy remove-service 2>$null | Out-Null
} catch {}

caddy add-service --config "$caddyfile" --watch
caddy start-service

Write-Host "`nCaddy is configured and running as a Windows service."
Write-Host "- Config: $caddyfile"
$siteDisplay = if ([string]::IsNullOrWhiteSpace($domain)) { ':80' } else { $domain }
Write-Host ("- Site:   {0}" -f $siteDisplay)
Write-Host "- Upstreams:"
Write-Host "  * Next.js -> http://$webUp"
Write-Host "  * FastAPI -> http://$apiUp"
Write-Host "`nIf you provided a public domain, ensure inbound ports 80/443 are open, and DNS records point to this host."
Write-Host "`nRedis (optional): Set in backend/.env if running locally:"
Write-Host "  REDIS_URL=redis://127.0.0.1:6379/0"
Write-Host "  REDIS_PREFIX=ratelimit"
Write-Host "  RESULT_CACHE_TTL=5"
