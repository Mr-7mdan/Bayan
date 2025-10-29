@echo off
setlocal enabledelayedexpansion
if "%~3"=="" (
  echo Usage: %~nx0 OWNER REPO TAG
  exit /b 1
)
set OWNER=%~1
set REPO=%~2
set TAG=%~3

where gh >nul 2>&1
if %ERRORLEVEL%==0 (
  for /f "usebackq delims=" %%i in (`gh api repos/!OWNER!/!REPO!/releases/tags/!TAG! --jq ".id" 2^>nul`) do set RID=%%i
  if defined RID (
    gh api -X DELETE repos/!OWNER!/!REPO!/releases/!RID! >nul 2>&1
  )
  gh api -X DELETE repos/!OWNER!/!REPO!/git/refs/tags/!TAG! >nul 2>&1
  echo Rolled back !OWNER!/!REPO! !TAG!
  exit /b 0
)

set TOKEN=%GITHUB_TOKEN%
if not defined TOKEN set TOKEN=%GH_TOKEN%
if not defined TOKEN (
  echo Error: gh not found and GITHUB_TOKEN/GH_TOKEN not set
  exit /b 1
)

set HDR_AUTH=Authorization: Bearer %TOKEN%
set HDR_ACC=Accept: application/vnd.github+json
set HDR_VER=X-GitHub-Api-Version: 2022-11-28

for /f "usebackq delims=" %%i in (`curl -fsS https://api.github.com/repos/%OWNER%/%REPO%/releases/tags/%TAG% -H "%HDR_AUTH%" -H "%HDR_ACC%" -H "%HDR_VER%" ^| powershell -NoProfile -Command "$obj = Get-Content -Raw -Encoding UTF8 - ^| ConvertFrom-Json; if ($null -ne $obj.id) { Write-Output $obj.id }" 2^>nul`) do set RID=%%i
if defined RID (
  curl -fsS -X DELETE https://api.github.com/repos/%OWNER%/%REPO%/releases/%RID% -H "%HDR_AUTH%" -H "%HDR_ACC%" -H "%HDR_VER%" >nul 2>&1
)

curl -fsS -X DELETE https://api.github.com/repos/%OWNER%/%REPO%/git/refs/tags/%TAG% -H "%HDR_AUTH%" -H "%HDR_ACC%" -H "%HDR_VER%" >nul 2>&1

echo Rolled back %OWNER%/%REPO% %TAG%
