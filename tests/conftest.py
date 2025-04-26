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


# Skip database tests if connection not available
def pytest_collection_modifyitems(config, items):
    """Skip tests based on markers and available connections."""
    skip_postgres = pytest.mark.skip(reason="PostgreSQL connection not available")
    
    for item in items:
        # Skip PostgreSQL tests if PostgreSQL is not available
        if "postgres" in item.keywords and not has_postgres_connection():
            item.add_marker(skip_postgres)


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