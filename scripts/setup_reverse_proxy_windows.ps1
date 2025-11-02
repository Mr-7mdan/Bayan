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
    winget install --id=Light-CaddyServer.Caddy -e --source winget -h 2>$null | Out-Null
  }
  if (-not (Get-Command caddy -ErrorAction SilentlyContinue)) {
    Write-Warning 'Caddy not found in PATH. Will still write Caddyfile; install Caddy and run it later.'
    return $false
  }
  return $true
}

# Ensure Chocolatey is available for package install fallbacks
function Ensure-Choco {
  if (Get-Command choco -ErrorAction SilentlyContinue) { return $true }
  Write-Host 'Chocolatey not found. Installing Chocolatey...'
  try {
    Set-ExecutionPolicy Bypass -Scope Process -Force | Out-Null
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
    Invoke-Expression ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
    $env:Path = [System.Environment]::GetEnvironmentVariable('Path','Machine') + ';' + [System.Environment]::GetEnvironmentVariable('Path','User')
  } catch {
    Write-Warning ("Failed to install Chocolatey automatically: {0}" -f $_.Exception.Message)
  }
  return (Get-Command choco -ErrorAction SilentlyContinue) -ne $null
}

# Quick helpers
function Test-PortInUse {
  param([int]$Port)
  try { return (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1) -ne $null } catch { return $false }
}

# Try to resolve a domain via public DNS; return $true if any A/AAAA exists
function Test-DnsReady {
  param([string]$Name)
  if ([string]::IsNullOrWhiteSpace($Name)) { return $false }
  $ok = $false
  try { $a = Resolve-DnsName -Server 8.8.8.8 -Name $Name -Type A -ErrorAction Stop; if ($a) { $ok = $true } } catch {}
  if (-not $ok) {
    try { $aaaa = Resolve-DnsName -Server 1.1.1.1 -Name $Name -Type AAAA -ErrorAction Stop; if ($aaaa) { $ok = $true } } catch {}
  }
  return $ok
}

Ensure-Admin
$hasCaddy = Ensure-Caddy

$domain = Read-Host 'Domain (e.g. example.com). Leave blank for HTTP-only on :80'
$webUp = Read-Host 'Next.js upstream [127.0.0.1:3000]'
if ([string]::IsNullOrWhiteSpace($webUp)) { $webUp = '127.0.0.1:3000' }
$apiUp = Read-Host 'FastAPI upstream [127.0.0.1:8000]'
if ([string]::IsNullOrWhiteSpace($apiUp)) { $apiUp = '127.0.0.1:8000' }
$email = ''
if (-not [string]::IsNullOrWhiteSpace($domain)) {
  $email = Read-Host 'Email for TLS notifications (optional, e.g. ops@example.com)'
}

# Ask whether to enable HTTPS when a domain is provided
$useHttps = 'n'
if (-not [string]::IsNullOrWhiteSpace($domain)) {
  $useHttps = Read-Host 'Enable HTTPS with automatic certificates (ACME)? [y/N]'
}

# Optional local certificate when ACME is disabled
$useLocalCert = 'n'
$certPath = ''
$keyPath = ''
if (-not [string]::IsNullOrWhiteSpace($domain) -and -not ($useHttps -match '^(?i:y|yes)$')) {
  $useLocalCert = Read-Host 'Use a local SSL certificate? [y/N]'
  if ($useLocalCert -match '^(?i:y|yes)$') {
    $defaultCertDir = Join-Path (Join-Path $env:ProgramData 'Caddy') 'certs'
    if (-not (Test-Path $defaultCertDir)) { New-Item -ItemType Directory -Path $defaultCertDir | Out-Null }
    Write-Host ("Place your certificate and key files in: {0}" -f $defaultCertDir)
    Write-Host 'Recommended names: fullchain.pem (cert) and privkey.pem (key).'
    $certPath = Read-Host ("Path to certificate [.pem/.crt] [default: {0}]" -f (Join-Path $defaultCertDir 'fullchain.pem'))
    if ([string]::IsNullOrWhiteSpace($certPath)) { $certPath = (Join-Path $defaultCertDir 'fullchain.pem') }
    $keyPath = Read-Host ("Path to private key [.pem/.key] [default: {0}]" -f (Join-Path $defaultCertDir 'privkey.pem'))
    if ([string]::IsNullOrWhiteSpace($keyPath)) { $keyPath = (Join-Path $defaultCertDir 'privkey.pem') }
  }
}

# Optional Redis install/start
$installRedis = Read-Host 'Install and start Redis locally? [y/N]'
if ($installRedis -match '^(?i:y|yes)$') {
  if (-not (Get-Command redis-server -ErrorAction SilentlyContinue) -and -not (Get-Service -Name 'Memurai' -ErrorAction SilentlyContinue)) {
    Write-Host 'Installing Redis/Memurai (winget -> winget fallback -> Chocolatey fallback)...'
    $ok = $false
    try { winget install -e --id=Redis.Redis -h | Out-Null; $ok = $true } catch {}
    if (-not $ok) { try { winget install -e --id=Memurai.MemuraiDeveloper -h | Out-Null; $ok = $true } catch {} }
    if (-not $ok) {
      if (Ensure-Choco) {
        try { choco install memurai-developer -y | Out-Null; $ok = $true } catch {}
        if (-not $ok) { try { choco install redis-64 -y | Out-Null; $ok = $true } catch {} }
      }
    }
    if (-not $ok) { Write-Warning 'Failed to install Redis via winget/choco; please install manually or use Docker: docker run -p 6379:6379 redis' }
  }
  try {
    # Try to start Redis if installed as a service; ignore errors if running in console mode
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
$siteHeader = if ([string]::IsNullOrWhiteSpace($domain)) { ":80 {`n  auto_https off" } elseif ($useHttps -match '^(?i:y|yes)$') { "$domain {" } else { "$domain {`n  auto_https off" }
$tlsLine = ''
if (-not [string]::IsNullOrWhiteSpace($domain) -and -not ($useHttps -match '^(?i:y|yes)$') -and ($useLocalCert -match '^(?i:y|yes)$')) {
  $tlsLine = "  tls \"$certPath\" \"$keyPath\""
}
$caddyContent = @"
${caddyGlobal}${siteHeader}
  encode zstd gzip
${tlsLine}
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

# Format for consistency
if ($hasCaddy) {
  try { caddy fmt --overwrite "$caddyfile" | Out-Null } catch {}
}

# Validate config
$canValidate = $hasCaddy
if ($hasCaddy -and (-not [string]::IsNullOrWhiteSpace($domain)) -and (-not ($useHttps -match '^(?i:y|yes)$')) -and ($useLocalCert -match '^(?i:y|yes)$')) {
  if (-not (Test-Path $certPath) -or -not (Test-Path $keyPath)) {
    Write-Warning "Skipping Caddy validation: certificate or key file not found."
    Write-Host "  Cert: $certPath"
    Write-Host "  Key : $keyPath"
    $canValidate = $false
  }
}
if ($canValidate) { caddy validate --config "$caddyfile" }

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

# Port conflict detection (IIS/HTTP.SYS common holders)
$portsInUse = @()
if (Test-PortInUse -Port 80) { $portsInUse += 80 }
if (Test-PortInUse -Port 443) { $portsInUse += 443 }
if ($portsInUse.Count -gt 0) {
  Write-Warning ("Ports in use: {0}. Caddy may fail to start." -f ($portsInUse -join ', '))
  $w3svc = Get-Service -Name 'W3SVC' -ErrorAction SilentlyContinue
  $was = Get-Service -Name 'WAS' -ErrorAction SilentlyContinue
  if (($w3svc -and $w3svc.Status -eq 'Running') -or ($was -and $was.Status -eq 'Running')) {
    $stopIIS = Read-Host 'IIS services detected (W3SVC/WAS). Stop them now? [y/N]'
    if ($stopIIS -match '^(?i:y|yes)$') {
      try { if ($w3svc) { Stop-Service W3SVC -Force -ErrorAction SilentlyContinue } } catch {}
      try { if ($was) { Stop-Service WAS -Force -ErrorAction SilentlyContinue } } catch {}
      Start-Sleep -Seconds 1
    }
  }
}

if ($hasCaddy) {
  # Install/Restart Caddy as a service (runs under Local System)
  # Service will run: caddy run --config Caddyfile --watch
  Write-Host 'Installing or restarting Caddy Windows service...'
  try { caddy stop-service 2>$null | Out-Null } catch {}
  try { caddy remove-service 2>$null | Out-Null } catch {}
  caddy add-service --config "$caddyfile" --watch
  if ($canValidate) {
    caddy start-service
    Write-Host "`nCaddy is configured and running as a Windows service."
  } else {
    Write-Host "`nCaddy service installed but NOT started due to missing certificate files."
    Write-Host "Place cert/key, then run:  caddy start-service"
  }
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
} else {
  Write-Host "`nCaddyfile written. Install Caddy and run:"
  Write-Host "  caddy run --config \"$caddyfile\" --watch"
  Write-Host "- Config: $caddyfile"
  $siteDisplay = if ([string]::IsNullOrWhiteSpace($domain)) { ':80' } else { $domain }
  Write-Host ("- Site:   {0}" -f $siteDisplay)
  Write-Host "- Upstreams:"
  Write-Host "  * Next.js -> http://$webUp"
  Write-Host "  * FastAPI -> http://$apiUp"
}
