# Clean up old log files to prevent disk space issues (Windows)

$LogsDir = Join-Path $PSScriptRoot "..\logs"

Write-Host "🧹 Cleaning up old log files in $LogsDir" -ForegroundColor Cyan

if (-not (Test-Path $LogsDir)) {
    Write-Host "✓ No logs directory found" -ForegroundColor Green
    exit 0
}

# Count files before
$Before = (Get-ChildItem -Path $LogsDir -Filter "*.log" -Recurse -File).Count
$SizeBefore = (Get-ChildItem -Path $LogsDir -Recurse | Measure-Object -Property Length -Sum).Sum / 1MB

Write-Host "📊 Before: $Before log files, $([math]::Round($SizeBefore, 2)) MB total" -ForegroundColor Gray

# Delete logs older than 7 days
Get-ChildItem -Path $LogsDir -Filter "*.log" -Recurse -File | 
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-7) } | 
    Remove-Item -Force

# Truncate current logs if they're over 10MB
Get-ChildItem -Path $LogsDir -Filter "*.log" -Recurse -File | 
    Where-Object { $_.Length -gt 10MB } | 
    ForEach-Object { 
        Write-Host "  Truncating large log: $($_.Name)" -ForegroundColor Yellow
        Clear-Content $_.FullName 
    }

# Count files after
$After = (Get-ChildItem -Path $LogsDir -Filter "*.log" -Recurse -File).Count
$SizeAfter = (Get-ChildItem -Path $LogsDir -Recurse | Measure-Object -Property Length -Sum).Sum / 1MB

Write-Host "📊 After: $After log files, $([math]::Round($SizeAfter, 2)) MB total" -ForegroundColor Gray
Write-Host "✅ Done! Deleted $($Before - $After) old log files" -ForegroundColor Green
