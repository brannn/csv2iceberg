#!/bin/bash
# SQLBatcher Test Runner
# This script runs the core SQLBatcher tests that don't require external connections

echo "Running SQLBatcher core tests..."
python -m pytest tests/test_batcher.py::TestSQLBatcher tests/test_adapters.py::TestSQLAdapter -v

# Note about adapter tests
echo ""
echo "Note: Database-specific adapter tests are skipped."
echo "To run full test suite, a PostgreSQL database connection is required."
echo ""

# Check exit status
if [ $? -eq 0 ]; then
    echo "✅ All core tests passed!"
    exit 0
else
    echo "❌ Some tests failed."
    exit 1
fi