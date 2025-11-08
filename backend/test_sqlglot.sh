#!/bin/bash
# Quick test script for SQLGlot

set -e

echo "🧪 Testing SQLGlot Implementation"
echo "=================================="
echo ""

# Activate venv if not already active
if [ -z "$VIRTUAL_ENV" ]; then
    echo "📦 Activating virtual environment..."
    source venv/bin/activate
fi

# Check SQLGlot installation
echo "✓ Checking SQLGlot installation..."
python -c "import sqlglot; print(f'  SQLGlot version: {sqlglot.__version__}')"

# Run tests
echo ""
echo "✓ Running unit tests..."
PYTHONPATH=. pytest tests/test_sqlglot_builder.py -v --tb=short

echo ""
echo "✅ All tests passed!"
echo ""
echo "📋 Next steps:"
echo "  1. Set ENABLE_SQLGLOT=true in .env"
echo "  2. Restart backend server"
echo "  3. Check logs for [SQLGlot] messages"
echo ""
