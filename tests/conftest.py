"""
Pytest configuration and fixtures for SQL Batcher tests.
"""
import os
import pytest
from unittest.mock import MagicMock

# Define test markers
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "core: tests that don't require database connections"
    )
    config.addinivalue_line(
        "markers", "db: tests that require actual database connections"
    )
    config.addinivalue_line(
        "markers", "postgres: tests that require PostgreSQL database connections"
    )
    config.addinivalue_line(
        "markers", "snowflake: tests that require Snowflake database connections"
    )
    config.addinivalue_line(
        "markers", "trino: tests that require Trino database connections"
    )
    config.addinivalue_line(
        "markers", "bigquery: tests that require BigQuery database connections"
    )
    config.addinivalue_line(
        "markers", "spark: tests that require Spark database connections"
    )


# PostgreSQL connection check
def has_postgres_connection():
    """Check if PostgreSQL connection is available."""
    # Check for required environment variables
    required_vars = ["PGHOST", "PGPORT", "PGUSER", "PGDATABASE"]
    for var in required_vars:
        if not os.environ.get(var):
            return False
            
    # Try to connect to PostgreSQL
    try:
        import psycopg2
        conn_params = {
            "host": os.environ.get("PGHOST", "localhost"),
            "port": os.environ.get("PGPORT", "5432"),
            "user": os.environ.get("PGUSER", "postgres"),
            "dbname": os.environ.get("PGDATABASE", "postgres"),
            "password": os.environ.get("PGPASSWORD", ""),
            "connect_timeout": 5,
        }
        
        conn = psycopg2.connect(**conn_params)
        conn.close()
        return True
    except (ImportError, Exception):
        return False


# Connection check for other databases
def has_snowflake_connection():
    """Check if Snowflake connection is available."""
    # Check for required environment variables
    required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_DATABASE"]
    for var in required_vars:
        if not os.environ.get(var):
            return False
    
    # Check for snowflake-connector-python package
    try:
        import snowflake.connector
        return True
    except ImportError:
        return False


def has_trino_connection():
    """Check if Trino connection is available."""
    # Check for required environment variables
    required_vars = ["TRINO_HOST", "TRINO_USER"]
    for var in required_vars:
        if not os.environ.get(var):
            return False
    
    # Check for trino package
    try:
        import trino
        return True
    except ImportError:
        return False


def has_bigquery_connection():
    """Check if BigQuery connection is available."""
    # Check for required environment variables
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return False
    
    # Check for google-cloud-bigquery package
    try:
        from google.cloud import bigquery
        return True
    except ImportError:
        return False


def has_spark_connection():
    """Check if Spark connection is available."""
    # Check for pyspark package
    try:
        import pyspark
        return True
    except ImportError:
        return False


# Skip database tests if connection not available
def pytest_collection_modifyitems(config, items):
    """Skip tests based on markers and available connections."""
    skip_postgres = pytest.mark.skip(reason="PostgreSQL connection not available")
    skip_snowflake = pytest.mark.skip(reason="Snowflake connection not available")
    skip_trino = pytest.mark.skip(reason="Trino connection not available")
    skip_bigquery = pytest.mark.skip(reason="BigQuery connection not available")
    skip_spark = pytest.mark.skip(reason="Spark connection not available")
    
    for item in items:
        # Skip tests based on database connection availability
        if "postgres" in item.keywords and not has_postgres_connection():
            item.add_marker(skip_postgres)
        if "snowflake" in item.keywords and not has_snowflake_connection():
            item.add_marker(skip_snowflake)
        if "trino" in item.keywords and not has_trino_connection():
            item.add_marker(skip_trino)
        if "bigquery" in item.keywords and not has_bigquery_connection():
            item.add_marker(skip_bigquery)
        if "spark" in item.keywords and not has_spark_connection():
            item.add_marker(skip_spark)


# Generic database connection fixture
@pytest.fixture
def mock_db_connection():
    """Mock database connection for generic adapter tests."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    
    # Mock cursor.execute to return result sets for SELECT statements
    def side_effect(sql, *args, **kwargs):
        if sql.strip().upper().startswith("SELECT"):
            cursor.description = [("id",), ("name",)]
            cursor.rowcount = 1
            cursor.fetchall.return_value = [(1, "Test")]
        return cursor
    
    cursor.execute.side_effect = side_effect
    
    # Configure transaction methods
    conn.commit = MagicMock()
    conn.rollback = MagicMock()
    conn.close = MagicMock()
    
    return conn