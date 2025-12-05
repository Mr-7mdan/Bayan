@echo off
setlocal enabledelayedexpansion

REM Determine paths
set SCRIPT_DIR=%~dp0
set ROOT_DIR=%SCRIPT_DIR%..
pushd "%ROOT_DIR%\frontend" >nul 2>&1
if errorlevel 1 (
  echo Failed to change directory to frontend
  exit /b 1
)

REM Defaults
if not defined PORT set PORT=3000
if not defined HOST set HOST=0.0.0.0
set NODE_ENV=production

REM Sanity checks
if not exist package.json (
  echo package.json not found in %ROOT_DIR%\frontend
  popd >nul 2>&1
  exit /b 1
)

REM Install deps if needed
if not exist node_modules (
  call npm ci
  if errorlevel 1 (
    echo npm ci failed
    popd >nul 2>&1
    exit /b 1
  )
)

REM Build (incremental - uses cache for speed)
call npm run build
if errorlevel 1 (
  echo npm run build failed
  popd >nul 2>&1
  exit /b 1
)

REM Start Next.js in production
call npm run start -- -p %PORT% -H %HOST%
set RC=%ERRORLEVEL%
popd >nul 2>&1
exit /b %RC%
