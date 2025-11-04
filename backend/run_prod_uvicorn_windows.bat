@echo off
setlocal
cd /d %~dp0
if not exist venv\Scripts\python.exe (
  echo venv missing. Create it and install requirements.
  exit /b 1
)
call venv\Scripts\activate
if "%HOST%"=="" set HOST=127.0.0.1
if "%PORT%"=="" set PORT=8000
if "%WORKERS%"=="" set WORKERS=1
if "%RUN_SCHEDULER%"=="" set RUN_SCHEDULER=1
if "%HOT_RELOAD%"=="1" (
  echo [watch] uvicorn --reload enabled
  venv\Scripts\python.exe -m uvicorn app.main:app --host %HOST% --port %PORT% --workers %WORKERS% --reload --log-level warning --no-access-log
) else (
  venv\Scripts\python.exe -m uvicorn app.main:app --host %HOST% --port %PORT% --workers %WORKERS% --log-level warning --no-access-log
)
