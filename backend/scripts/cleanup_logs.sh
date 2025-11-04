#!/usr/bin/env bash
# Clean up old log files to prevent disk space issues

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS_DIR="${SCRIPT_DIR}/../logs"

echo "🧹 Cleaning up old log files in ${LOGS_DIR}"

if [ ! -d "${LOGS_DIR}" ]; then
    echo "✓ No logs directory found"
    exit 0
fi

# Count files before
BEFORE=$(find "${LOGS_DIR}" -type f -name "*.log" | wc -l)
SIZE_BEFORE=$(du -sh "${LOGS_DIR}" 2>/dev/null | cut -f1)

echo "📊 Before: ${BEFORE} log files, ${SIZE_BEFORE} total"

# Delete logs older than 7 days
find "${LOGS_DIR}" -type f -name "*.log" -mtime +7 -delete

# Truncate current logs if they're over 10MB
find "${LOGS_DIR}" -type f -name "*.log" -size +10M -exec truncate -s 0 {} \;

# Count files after
AFTER=$(find "${LOGS_DIR}" -type f -name "*.log" | wc -l)
SIZE_AFTER=$(du -sh "${LOGS_DIR}" 2>/dev/null | cut -f1)

echo "📊 After: ${AFTER} log files, ${SIZE_AFTER} total"
echo "✅ Done! Deleted $((BEFORE - AFTER)) old log files"
