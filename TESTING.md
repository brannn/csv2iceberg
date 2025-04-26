# SQL Batcher Testing Guide

This guide provides comprehensive information on testing the SQL Batcher library, including how to run tests, understand test organization, and contribute new tests.

## Quick Start

For the simplest way to run the core tests (without database connections):

```bash
python run_full_tests.py --core-only
```

To run all available tests (depending on database connections):

```bash
python run_full_tests.py
```

## Test Organization

SQL Batcher tests are organized into several categories:

### 1. Core Tests

Core tests focus on the fundamental batcher functionality and don't require any database connections. These tests use in-memory mocks and simple SQLite connections.

- Located in: `tests/test_batcher.py`
- Run with: `python run_full_tests.py --core-only`

### 2. Generic Adapter Tests

Tests for the generic database adapter functionality, which can work with any database connection object.

- Located in: `tests/test_adapters.py`
- Run with: `python run_full_tests.py --generic-adapters`

### 3. Database-Specific Adapter Tests

Tests for database-specific adapters like PostgreSQL, Trino, BigQuery, etc. These tests require actual database connections.

- PostgreSQL: `tests/test_postgresql_adapter.py`
- Trino: `tests/test_trino_adapter.py`
- BigQuery: `tests/test_bigquery_adapter.py`
- Run with: `python run_full_tests.py --pg` (for PostgreSQL tests)

## Test Configuration

### Test Markers

We use pytest markers to categorize tests:

- `@pytest.mark.core`: Core functionality tests that don't require database connections
- `@pytest.mark.generic_adapter`: Tests for the generic adapter functionality
- `@pytest.mark.db`: Tests that require database connections
- `@pytest.mark.pg`, `@pytest.mark.trino`, etc.: Database-specific tests

### Database Connection Settings

Database-specific tests use environment variables for connection settings:

#### PostgreSQL

```
PGHOST=localhost
PGPORT=5432
PGUSER=postgres
PGPASSWORD=postgres
PGDATABASE=postgres_test
```

#### Trino

```
TRINO_HOST=localhost
TRINO_PORT=8080
TRINO_USER=trino
TRINO_CATALOG=memory
TRINO_SCHEMA=default
```

#### BigQuery

```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
BQ_PROJECT_ID=your-project-id
BQ_DATASET_ID=your_dataset
```

## Running Tests

### Using run_full_tests.py

The `run_full_tests.py` script intelligently detects available database connections and only runs tests for databases that are accessible.

```bash
# Run all tests
python run_full_tests.py

# Run specific test categories
python run_full_tests.py --core-only            # Core tests only
python run_full_tests.py --pg                   # PostgreSQL tests
python run_full_tests.py --core-only --pg       # Core + PostgreSQL tests
python run_full_tests.py --trino                # Trino tests
python run_full_tests.py --coverage             # Generate coverage report
```

### Using pytest directly

You can also use pytest directly with markers:

```bash
# Run core tests
pytest -m core

# Run PostgreSQL tests
pytest -m pg

# Run specific test file
pytest tests/test_batcher.py

# Run with coverage
pytest --cov=sql_batcher
```

## Writing New Tests

When contributing new tests:

1. Follow the existing structure and naming conventions
2. Use appropriate pytest markers
3. Ensure tests can run both in isolation and as part of the test suite
4. Add mocks for external dependencies
5. Don't assume database connections are available

### Test Example

```python
import pytest
from sql_batcher import SQLBatcher

# Mark this as a core test
@pytest.mark.core
def test_feature_xyz():
    # Arrange
    batcher = SQLBatcher(max_bytes=1000)
    
    # Act
    result = batcher.some_method()
    
    # Assert
    assert result == expected_value
```

### Database-Specific Test Example

```python
import pytest
from sql_batcher.adapters.postgresql import PostgreSQLAdapter

# Mark this as a PostgreSQL test
@pytest.mark.db
@pytest.mark.pg
def test_pg_specific_feature():
    # Skip if PostgreSQL connection is not available
    if not has_postgres_connection():
        pytest.skip("PostgreSQL connection not available")
    
    # Test with actual PostgreSQL connection
    adapter = PostgreSQLAdapter(connection_params=get_test_pg_params())
    # ... test implementation
```

## CI/CD Testing

Our GitHub Actions workflow runs tests on each push and pull request:

1. Runs core tests across multiple Python versions
2. For database tests, uses service containers to provide test databases
3. Generates and uploads coverage reports

## Test Coverage

We aim for high test coverage, especially for core functionality. Generate a coverage report with:

```bash
python run_full_tests.py --coverage
```

Or view HTML coverage report:

```bash
python run_full_tests.py --coverage --html-report
# Then open htmlcov/index.html
```

## Debugging Tests

For verbose test output:

```bash
python run_full_tests.py --core-only -v
```

For even more detail:

```bash
pytest tests/test_batcher.py -v --no-header --showlocals
```

## Integration Testing

Beyond unit tests, we recommend integration testing with real database systems. The `examples/` directory contains sample scripts that demonstrate integration with different databases.