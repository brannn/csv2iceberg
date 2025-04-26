#!/bin/bash
# SQLBatcher Test Runner
# This script runs the core SQLBatcher tests

echo "Running SQLBatcher core tests..."
python -m pytest tests/test_batcher.py::TestSQLBatcher tests/test_adapters.py::TestSQLAdapter -v

# Check if PostgreSQL is available for additional adapter tests
echo "Checking if PostgreSQL adapter tests can be run..."
if python -c "import psycopg2" 2>/dev/null; then
    if pg_isready -h ${PGHOST:-localhost} -p ${PGPORT:-5432} -U ${PGUSER:-postgres} -d ${PGDATABASE:-postgres} -t 5 >/dev/null 2>&1; then
        echo "PostgreSQL is available, running adapter tests..."
        python -m pytest tests/test_postgresql_adapter.py -v
    else
        echo "PostgreSQL database is not available, skipping adapter tests."
    fi
else
    echo "psycopg2 is not installed, skipping PostgreSQL adapter tests."
fi

# Check exit status
if [ $? -eq 0 ]; then
    echo "✅ All tests passed!"
    exit 0
else
    echo "❌ Some tests failed."
    exit 1
fi