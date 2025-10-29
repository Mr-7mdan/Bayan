# Build a Next.js production artifact and bump version by 0.1
# Outputs: scripts/out/frontend-<version>.tar.gz and .sha256

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Resolve-Path (Join-Path $ScriptDir '..')
$OutDir = Join-Path $RootDir 'scripts/out'
$VerFile = Join-Path $RootDir 'scripts/VERSION_FRONTEND'

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Read version and bump +0.1 (one decimal)
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
Write-Host "[frontend] Building version $New"

$FrontendDir = Join-Path $RootDir 'frontend'
Set-Location $FrontendDir
if (-not (Test-Path (Join-Path $FrontendDir 'package.json'))) {
  throw "package.json not found in $FrontendDir"
}

# Clean & build
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue .next, dist | Out-Null
npm ci
npm run build
# keep only production deps
npm prune --omit=dev | Out-Null

# Stage (exclude build/artifact directories)
$StageDir = Join-Path $FrontendDir ("dist/frontend-" + $New)
New-Item -ItemType Directory -Force -Path $StageDir | Out-Null

$excludeDirs = @('.next','node_modules','dist','.git')
$excludeFiles = @('.DS_Store')

# Copy files at root excluding unwanted files
Get-ChildItem -Force -File | Where-Object { $excludeFiles -notcontains $_.Name } | ForEach-Object {
  Copy-Item $_.FullName -Destination (Join-Path $StageDir $_.Name)
}
# Copy directories at root excluding unwanted dirs
Get-ChildItem -Force -Directory | Where-Object { $excludeDirs -notcontains $_.Name } | ForEach-Object {
  Copy-Item -Recurse $_.FullName -Destination (Join-Path $StageDir $_.Name)
}

Set-Content -Path (Join-Path $StageDir 'VERSION') -Value $New -NoNewline

# Tar
$Artifact = Join-Path $OutDir ("frontend-" + $New + ".tar.gz")
if (Test-Path $Artifact) { Remove-Item $Artifact -Force }

# Windows has bsdtar as 'tar' in PATH
# We cd into dist so the folder name inside archive is frontend-<ver>
Push-Location (Join-Path $FrontendDir 'dist')
try {
  tar -czf $Artifact ("frontend-" + $New)
} finally {
  Pop-Location
}

# Hash
$Hash = (Get-FileHash -Algorithm SHA256 $Artifact).Hash
Set-Content -Path ($Artifact + '.sha256') -Value ("$Hash  " + (Split-Path -Leaf $Artifact)) -NoNewline
Write-Host "[frontend] Built: $Artifact"
Write-Host "[frontend] SHA256: $Hash"

$meta = @{ artifact = (Split-Path -Leaf $Artifact); version = $New; sha256 = $Hash } | ConvertTo-Json -Compress
Set-Content -Path (Join-Path $OutDir 'frontend-latest.json') -Value $meta -NoNewline
