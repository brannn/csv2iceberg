# Testing Guide for SQL Batcher

This document outlines the testing approach, organization, and methods used in the SQL Batcher project.

## Testing Framework

SQL Batcher uses pytest as its primary testing framework. Tests are organized to allow running at different levels:

1. **Core Tests**: Tests for the core functionality that don't require any database connections
2. **Adapter Tests**: Tests for specific database adapters, which may require actual database connections
3. **Integration Tests**: End-to-end tests that verify SQL Batcher works correctly with actual databases

## Test Organization

Tests are organized in the `tests` directory with the following structure:

- `test_batcher.py`: Core tests for the SQLBatcher class
- `test_adapters.py`: Tests for the adapter base class and the generic adapter
- `test_postgresql_adapter.py`: Tests for the PostgreSQL adapter
- `test_snowflake_adapter.py`: Tests for the Snowflake adapter
- `test_trino_adapter.py`: Tests for the Trino adapter
- `test_bigquery_adapter.py`: Tests for the BigQuery adapter
- `test_spark_adapter.py`: Tests for the Spark adapter

## Test Markers

We use pytest markers to categorize tests:

- `@pytest.mark.core`: Tests that don't require database connections
- `@pytest.mark.db`: Tests that require actual database connections
- `@pytest.mark.postgres`: Tests that require a PostgreSQL database
- `@pytest.mark.snowflake`: Tests that require a Snowflake connection
- `@pytest.mark.trino`: Tests that require a Trino connection
- `@pytest.mark.bigquery`: Tests that require a BigQuery connection
- `@pytest.mark.spark`: Tests that require a Spark connection

## Running Tests

The project includes a comprehensive test runner script that intelligently runs tests based on available connections.

### Run All Tests

```bash
python run_full_tests.py
```

This will:
1. Detect which database connections are available
2. Run core tests that don't require connections
3. Run adapter-specific tests for available databases
4. Generate a summary report

### Run Core Tests Only

```bash
python run_full_tests.py --core-only
```

### Run with Coverage

```bash
python run_full_tests.py --coverage
```

This will generate a coverage report in both terminal output and HTML format (in the `coverage_html` directory).

### Run Individual Test Files

You can also run individual test files directly with pytest:

```bash
python -m pytest tests/test_batcher.py -v
```

Or specific test cases:

```bash
python -m pytest tests/test_batcher.py::TestSQLBatcher::test_init_with_defaults -v
```

## Setting Up Database Connections for Testing

The test suite will automatically detect available database connections using environment variables:

### PostgreSQL

```
PGHOST=localhost
PGPORT=5432
PGUSER=postgres
PGPASSWORD=password
PGDATABASE=test
```

### Snowflake

```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse
```

### Trino

```
TRINO_HOST=localhost
TRINO_PORT=8080
TRINO_USER=trino
TRINO_PASSWORD=password
TRINO_CATALOG=catalog
TRINO_SCHEMA=schema
```

### BigQuery

BigQuery uses Application Default Credentials. Set up credentials with:

```
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

## Mock Testing

For adapter-specific tests that can't connect to a real database, most can still run using mock connections. This allows testing most of the adapter functionality without requiring actual database connections.

## Adding New Tests

When adding new tests:

1. Add test cases to the appropriate test file
2. Use the correct markers to categorize the test
3. If testing database-specific functionality, use mocks when possible
4. Run the tests to ensure they pass
5. Update the coverage report to check test coverage

## Continuous Integration

The test suite is designed to work well in CI/CD environments:

- Core tests will always run
- Database-specific tests will only run if connections are available
- Coverage reports are generated for visibility into test quality