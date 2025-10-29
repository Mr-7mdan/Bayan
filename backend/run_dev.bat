@echo off
setlocal
REM Resolve script directory (handles spaces)
set "DIR=%~dp0"

REM Optionally activate venv for PATH and tools
if exist "%DIR%venv\Scripts\activate.bat" call "%DIR%venv\Scripts\activate.bat"

REM Ensure venv python exists
if not exist "%DIR%venv\Scripts\python.exe" (
  echo Error: venv python not found at "%DIR%venv\Scripts\python.exe"
  echo Tip: create it with: py -3 -m venv venv && venv\Scripts\pip install -r requirements.txt
  exit /b 1
)

REM Run backend using relative venv python
"%DIR%venv\Scripts\python.exe" -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
endlocal
