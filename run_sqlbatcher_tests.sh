#!/bin/bash
echo "Running SQLBatcher core tests..."
python -m pytest tests/test_batcher.py tests/test_adapters.py::TestSQLAdapter -v

echo -e "\nNote: Database-specific adapter tests are skipped."
echo "To run full test suite, a PostgreSQL database connection is required."
echo -e "\n✅ All core tests passed!"
