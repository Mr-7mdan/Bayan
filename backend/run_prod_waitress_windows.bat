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
if "%THREADS%"=="" set THREADS=8
if "%RUN_SCHEDULER%"=="" set RUN_SCHEDULER=1
if "%HOT_RELOAD%"=="1" (
  echo [watch] Using watchmedo auto-restart for waitress (if available)
  if exist venv\Scripts\watchmedo.exe (
    venv\Scripts\watchmedo.exe auto-restart --directory=app --patterns="*.py;*.html;*.css;*.js" --recursive -- venv\Scripts\waitress-serve.exe --listen=%HOST%:%PORT% --threads=%THREADS% --ident=reporting-api wsgi:application
  ) else (
    echo watchmedo not found; running without watcher
    venv\Scripts\waitress-serve.exe --listen=%HOST%:%PORT% --threads=%THREADS% --ident=reporting-api wsgi:application
  )
) else (
  venv\Scripts\waitress-serve.exe --listen=%HOST%:%PORT% --threads=%THREADS% --ident=reporting-api wsgi:application
)
