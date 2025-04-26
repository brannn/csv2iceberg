# SQL Batcher Testing Guide

SQL Batcher uses pytest as its primary testing framework. This document describes the testing approach, test categories, and how to run the tests.

## Test Categories

The tests are divided into several categories:

1. **Core Tests**: Tests that don't require any database connections
   - Test the SQLBatcher core functionality
   - Test the abstract adapter interface
   - Test the query collector functionality

2. **Generic Adapter Tests**: Tests that use mocked database connections
   - Test the basic functionality of the generic adapter
   - Don't require actual database connections

3. **Database-Specific Tests**: Tests that require actual database connections
   - PostgreSQL adapter tests
   - Spark adapter tests (if applicable)
   - Trino adapter tests (if applicable)

## Running Tests

### Basic Test Run

To run the core tests only (no database connections required):

```bash
python run_full_tests.py --core-only
```

### Running All Tests

To run all tests (requires database connections if available):

```bash
python run_full_tests.py --all
```

### Running Specific Database Tests

To run PostgreSQL-specific tests:

```bash
python run_full_tests.py --pg
```

### Test Coverage

To generate a test coverage report:

```bash
python run_full_tests.py --coverage
```

This will create an HTML coverage report in the `coverage_html` directory.

## Test Structure

The tests are organized as follows:

- `tests/test_batcher.py`: Tests for the core SQLBatcher class
- `tests/test_adapters.py`: Tests for the abstract adapter and generic adapter
- `tests/test_postgresql_adapter.py`: Tests for the PostgreSQL adapter
- `tests/conftest.py`: Pytest fixtures and test configuration

## Adding New Tests

When adding new tests:

1. Use the appropriate marker for the test category (core, db, postgres, etc.)
2. Keep database-specific tests in their own files
3. Use pytest fixtures for common setup
4. Mock external dependencies when possible

Example of adding a new test:

```python
@pytest.mark.core
def test_new_feature():
    batcher = SQLBatcher()
    # Test the new feature
    assert batcher.new_feature() == expected_result
```

## Continuous Integration

The repository is configured to run core tests on every commit. Database-specific tests run only when the necessary environment variables for database connections are available.

## Database Requirements

To run the database-specific tests:

### PostgreSQL

Set the following environment variables:
- `PGHOST`: PostgreSQL host (default: "localhost")
- `PGPORT`: PostgreSQL port (default: "5432")
- `PGUSER`: PostgreSQL username (default: "postgres")
- `PGDATABASE`: PostgreSQL database (default: "postgres")
- `PGPASSWORD`: PostgreSQL password (if needed)