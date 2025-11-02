param(
  [switch]$AllUsers
)

$ErrorActionPreference = "Stop"
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}

function Test-Admin { $id = [Security.Principal.WindowsIdentity]::GetCurrent(); (New-Object Security.Principal.WindowsPrincipal($id)).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator) }

# Expand .nupkg/.zip using .NET to avoid Expand-Archive's .zip-only restriction
function Expand-ZipCompat {
  param([string]$Path, [string]$Destination)
  try {
    if (Test-Path $Destination) { Remove-Item -Recurse -Force $Destination }
    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
  } catch {}
  try {
    Add-Type -AssemblyName System.IO.Compression.FileSystem -ErrorAction SilentlyContinue
  } catch {}
  try {
    [System.IO.Compression.ZipFile]::ExtractToDirectory($Path, $Destination)
  } catch {
    # Fallback: try Expand-Archive if available
    Expand-Archive -Path $Path -DestinationPath $Destination -Force
  }
}

if (Get-Command winget -ErrorAction SilentlyContinue) {
  try { $v = & winget -v; Write-Host "winget already installed: $v" } catch { Write-Host "winget already installed" }
  exit 0
}

$root = Join-Path $env:TEMP ("winget-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $root -Force | Out-Null

# Install Windows App Runtime (dependency for App Installer/winget)
function Install-WindowsAppRuntime {
  param([string]$DownloadDir, [string]$MajorMinor = '1.8')
  try {
    $releases = Invoke-RestMethod -UseBasicParsing -Uri "https://api.github.com/repos/microsoft/windowsappsdk/releases"
    $rel = $releases | Where-Object { $_.tag_name -like ("v" + $MajorMinor + ".*") } | ForEach-Object { $_ } |
      Sort-Object -Property { try { [version]($_.tag_name.TrimStart('v')) } catch { [version]'0.0.0.0' } } -Descending |
      Select-Object -First 1
    if (-not $rel) { return $false }
    $asset = $rel.assets | Where-Object { $_.name -match 'WindowsAppRuntimeInstall-x64\.exe$' } | Select-Object -First 1
    if (-not $asset) { return $false }
    $out = Join-Path $DownloadDir $asset.name
    Invoke-WebRequest -UseBasicParsing -Uri $asset.browser_download_url -OutFile $out
    Start-Process -FilePath $out -ArgumentList '/silent' -Wait -NoNewWindow
    return $true
  } catch { return $false }
}

try {
  $vclibs = Join-Path $root "Microsoft.VCLibs.x64.14.00.Desktop.appx"
  Invoke-WebRequest -UseBasicParsing -Uri "https://aka.ms/Microsoft.VCLibs.x64.14.00.Desktop.appx" -OutFile $vclibs

  $uixamlNupkg = Join-Path $root "Microsoft.UI.Xaml.2.8.6.nupkg"
  Invoke-WebRequest -UseBasicParsing -Uri "https://www.nuget.org/api/v2/package/Microsoft.UI.Xaml/2.8.6" -OutFile $uixamlNupkg
  $uixamlDir = Join-Path $root "uixaml"
  Expand-ZipCompat -Path $uixamlNupkg -Destination $uixamlDir
  $uixamlAppx = Get-ChildItem -Recurse -Path $uixamlDir -Filter *.appx | Where-Object { $_.FullName -match "\\x64\\" } | Select-Object -First 1
  if (-not $uixamlAppx) { throw "Microsoft.UI.Xaml appx not found" }

  $release = Invoke-RestMethod -UseBasicParsing -Uri "https://api.github.com/repos/microsoft/winget-cli/releases/latest"
  $asset = $release.assets | Where-Object { $_.name -like "*.msixbundle" } | Select-Object -First 1
  if (-not $asset) { throw "App Installer msixbundle not found" }
  $appinstaller = Join-Path $root $asset.name
  Invoke-WebRequest -UseBasicParsing -Uri $asset.browser_download_url -OutFile $appinstaller

  $deps = @($vclibs, $uixamlAppx.FullName)

  # Proactively install Windows App Runtime 1.8 (safe if already present)
  try { $null = Install-WindowsAppRuntime -DownloadDir $root -MajorMinor '1.8' } catch {}

  if ($AllUsers -and (Get-Command Add-AppxProvisionedPackage -ErrorAction SilentlyContinue)) {
    if (Test-Admin) {
      try { Add-AppxProvisionedPackage -Online -PackagePath $appinstaller -SkipLicense -DependencyPackagePath $deps | Out-Null } catch {}
    } else {
      Write-Warning "AllUsers requested but not elevated; installing for current user."
    }
  }

  try { Add-AppxPackage -Path $uixamlAppx.FullName | Out-Null } catch {}
  try { Add-AppxPackage -Path $vclibs | Out-Null } catch {}
  try {
    Add-AppxPackage -Path $appinstaller -DependencyPath $deps | Out-Null
  } catch {
    if ($_.Exception.Message -match 'Microsoft\.WindowsAppRuntime') {
      Write-Host 'Windows App Runtime missing; installing and retrying...'
      if (Install-WindowsAppRuntime -DownloadDir $root -MajorMinor '1.8') {
        Add-AppxPackage -Path $appinstaller -DependencyPath $deps | Out-Null
      } else { throw }
    } else { throw }
  }

  $ver = & winget -v
  Write-Host "winget installed: $ver"
  exit 0
}
catch {
  Write-Error ("Failed to install winget: " + $_.Exception.Message)
  exit 1
}
finally {
  try { Remove-Item -Recurse -Force $root } catch {}
}
