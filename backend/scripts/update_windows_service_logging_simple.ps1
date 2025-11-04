# Update NSSM service to disable access logs
# Run this after updating to v1.3 to apply the logging changes

$ServiceName = "BayanAPIUvicorn"
$BackendDir = "C:\bayan\backend"

Write-Host "Updating $ServiceName logging configuration..." -ForegroundColor Cyan

# Check if NSSM is available
$nssm = Get-Command nssm -ErrorAction SilentlyContinue
if (-not $nssm) {
    Write-Host "ERROR: NSSM not found. Please install NSSM first." -ForegroundColor Red
    exit 1
}

# Stop the service
Write-Host "Stopping service..." -ForegroundColor Yellow
nssm stop $ServiceName
Start-Sleep -Seconds 3

# Get current configuration
Write-Host "Current configuration:" -ForegroundColor Gray
nssm get $ServiceName Application
nssm get $ServiceName AppParameters

# Update the service with new parameters
$PythonExe = "$BackendDir\venv\Scripts\python.exe"
$AppParameters = "-m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1 --log-level warning --no-access-log"

Write-Host ""
Write-Host "Updating service parameters..." -ForegroundColor Cyan
nssm set $ServiceName Application $PythonExe
nssm set $ServiceName AppParameters $AppParameters
nssm set $ServiceName AppDirectory $BackendDir

# Set environment variables
Write-Host "Setting environment variables..." -ForegroundColor Cyan
nssm set $ServiceName AppEnvironmentExtra "RUN_SCHEDULER=1"

# Start the service
Write-Host "Starting service..." -ForegroundColor Green
nssm start $ServiceName
Start-Sleep -Seconds 5

# Check status
Write-Host ""
Write-Host "Service updated! Checking status..." -ForegroundColor Green
nssm status $ServiceName

Write-Host ""
Write-Host "New configuration:" -ForegroundColor Gray
nssm get $ServiceName AppParameters

Write-Host ""
Write-Host "Check logs in C:\bayan\logs\backend.out.log" -ForegroundColor Cyan
Write-Host "You should now see only WARNING and ERROR messages" -ForegroundColor Gray
