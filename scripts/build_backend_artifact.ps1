# Build a backend (FastAPI) artifact and bump version by 0.1
# Outputs: scripts/out/backend-<version>.tar.gz and .sha256

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Resolve-Path (Join-Path $ScriptDir '..')
$OutDir = Join-Path $RootDir 'scripts/out'
$VerFile = Join-Path $RootDir 'scripts/VERSION_BACKEND'
$BackendDir = Join-Path $RootDir 'backend'

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Read current version and bump +0.1 (one decimal)
$Curr = '0.0'
if (Test-Path $VerFile) {
  $Curr = (Get-Content -Raw $VerFile).Trim()
}
try {
  $New = [math]::Round([double]$Curr + 0.1, 1).ToString('0.0')
} catch {
  $New = '0.1'
}
Set-Content -Path $VerFile -Value $New -NoNewline
Write-Host "[backend] Building version $New"

# Stage (no venv)
$DistDir = Join-Path $BackendDir 'dist'
$StageDir = Join-Path $DistDir ("backend-" + $New)
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $DistDir | Out-Null
New-Item -ItemType Directory -Force -Path $StageDir | Out-Null

Copy-Item -Recurse (Join-Path $BackendDir 'app') $StageDir
if (Test-Path (Join-Path $BackendDir 'requirements.txt')) { Copy-Item (Join-Path $BackendDir 'requirements.txt') $StageDir }
if (Test-Path (Join-Path $BackendDir 'wsgi.py')) { Copy-Item (Join-Path $BackendDir 'wsgi.py') $StageDir }
if (Test-Path (Join-Path $BackendDir 'asgi.py')) { Copy-Item (Join-Path $BackendDir 'asgi.py') $StageDir }
# Optional run scripts
$opt = @('run_prod_gunicorn.sh','run_prod_waitress.sh','run_prod_uvicorn_windows.bat','run_prod_waitress_windows.bat')
foreach ($f in $opt) { $p = Join-Path $BackendDir $f; if (Test-Path $p) { Copy-Item $p $StageDir } }

Set-Content -Path (Join-Path $StageDir 'VERSION') -Value $New -NoNewline

# Tar
$Artifact = Join-Path $OutDir ("backend-" + $New + ".tar.gz")
if (Test-Path $Artifact) { Remove-Item $Artifact -Force }
Push-Location $DistDir
try {
  tar -czf $Artifact ("backend-" + $New)
} finally {
  Pop-Location
}

# Hash
$Hash = (Get-FileHash -Algorithm SHA256 $Artifact).Hash
Set-Content -Path ($Artifact + '.sha256') -Value ("$Hash  " + (Split-Path -Leaf $Artifact)) -NoNewline
Write-Host "[backend] Built: $Artifact"
Write-Host "[backend] SHA256: $Hash"

$meta = @{ artifact = (Split-Path -Leaf $Artifact); version = $New; sha256 = $Hash } | ConvertTo-Json -Compress
Set-Content -Path (Join-Path $OutDir 'backend-latest.json') -Value $meta -NoNewline
