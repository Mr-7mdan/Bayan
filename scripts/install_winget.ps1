param(
  [switch]$AllUsers
)

$ErrorActionPreference = "Stop"
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}

function Test-Admin { $id = [Security.Principal.WindowsIdentity]::GetCurrent(); (New-Object Security.Principal.WindowsPrincipal($id)).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator) }

if (Get-Command winget -ErrorAction SilentlyContinue) {
  try { $v = & winget -v; Write-Host "winget already installed: $v" } catch { Write-Host "winget already installed" }
  exit 0
}

$root = Join-Path $env:TEMP ("winget-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $root -Force | Out-Null

try {
  $vclibs = Join-Path $root "Microsoft.VCLibs.x64.14.00.Desktop.appx"
  Invoke-WebRequest -UseBasicParsing -Uri "https://aka.ms/Microsoft.VCLibs.x64.14.00.Desktop.appx" -OutFile $vclibs

  $uixamlNupkg = Join-Path $root "Microsoft.UI.Xaml.2.8.6.nupkg"
  Invoke-WebRequest -UseBasicParsing -Uri "https://www.nuget.org/api/v2/package/Microsoft.UI.Xaml/2.8.6" -OutFile $uixamlNupkg
  $uixamlDir = Join-Path $root "uixaml"
  Expand-Archive -Path $uixamlNupkg -DestinationPath $uixamlDir -Force
  $uixamlAppx = Get-ChildItem -Recurse -Path $uixamlDir -Filter *.appx | Where-Object { $_.FullName -match "\\x64\\" } | Select-Object -First 1
  if (-not $uixamlAppx) { throw "Microsoft.UI.Xaml appx not found" }

  $release = Invoke-RestMethod -UseBasicParsing -Uri "https://api.github.com/repos/microsoft/winget-cli/releases/latest"
  $asset = $release.assets | Where-Object { $_.name -like "*.msixbundle" } | Select-Object -First 1
  if (-not $asset) { throw "App Installer msixbundle not found" }
  $appinstaller = Join-Path $root $asset.name
  Invoke-WebRequest -UseBasicParsing -Uri $asset.browser_download_url -OutFile $appinstaller

  $deps = @($vclibs, $uixamlAppx.FullName)

  if ($AllUsers -and (Get-Command Add-AppxProvisionedPackage -ErrorAction SilentlyContinue)) {
    if (Test-Admin) {
      try { Add-AppxProvisionedPackage -Online -PackagePath $appinstaller -SkipLicense -DependencyPackagePath $deps | Out-Null } catch {}
    } else {
      Write-Warning "AllUsers requested but not elevated; installing for current user."
    }
  }

  try { Add-AppxPackage -Path $uixamlAppx.FullName | Out-Null } catch {}
  try { Add-AppxPackage -Path $vclibs | Out-Null } catch {}
  Add-AppxPackage -Path $appinstaller -DependencyPath $deps | Out-Null

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
