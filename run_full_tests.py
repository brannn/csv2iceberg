#!/usr/bin/env python
"""
Comprehensive test runner for SQL Batcher with intelligent adapter handling.

This script runs tests for the SQL Batcher package, with smart handling of
database-specific tests. It detects available database connections and only runs
tests for databases that are available.
"""
import argparse
import os
import subprocess
import sys
from typing import List, Optional


def run_core_tests(options: List[str] = None) -> int:
    """
    Run core SQL Batcher tests that don't require database connections.
    
    Args:
        options: Additional pytest options
        
    Returns:
        Exit code from pytest
    """
    cmd = ["pytest", "tests/test_batcher.py", "tests/test_adapters.py::TestSQLAdapter"]
    
    if options:
        cmd.extend(options)
    
    print(f"Running core tests: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def run_generic_adapter_tests(options: List[str] = None) -> int:
    """
    Run generic adapter tests with mocked connections.
    
    Args:
        options: Additional pytest options
        
    Returns:
        Exit code from pytest
    """
    cmd = ["pytest", "tests/test_adapters.py::TestGenericAdapter"]
    
    if options:
        cmd.extend(options)
        
    print(f"Running generic adapter tests: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def has_postgresql_connection() -> bool:
    """
    Check if PostgreSQL connection is available.
    
    Returns:
        True if PostgreSQL connection is available, False otherwise
    """
    # Check for required environment variables
    required_vars = ["PGHOST", "PGPORT", "PGUSER", "PGDATABASE"]
    for var in required_vars:
        if not os.environ.get(var):
            print(f"PostgreSQL environment variable {var} not set, skipping PostgreSQL tests")
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
    except (ImportError, Exception) as e:
        print(f"PostgreSQL connection failed: {str(e)}")
        return False


def run_postgresql_tests(options: List[str] = None) -> int:
    """
    Run PostgreSQL adapter tests.
    
    Args:
        options: Additional pytest options
    
    Returns:
        Exit code from pytest, or 0 if tests are skipped
    """
    if not has_postgresql_connection():
        print("Skipping PostgreSQL tests - no connection available")
        return 0
    
    cmd = ["pytest", "tests/test_postgresql_adapter.py"]
    
    if options:
        cmd.extend(options)
        
    print(f"Running PostgreSQL tests: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def has_snowflake_connection() -> bool:
    """
    Check if Snowflake connection is available.
    
    Returns:
        True if Snowflake connection is available, False otherwise
    """
    # Check for required environment variables
    required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_DATABASE"]
    for var in required_vars:
        if not os.environ.get(var):
            print(f"Snowflake environment variable {var} not set, skipping Snowflake tests")
            return False
    
    # Snowflake connection requires the snowflake-connector-python package
    try:
        import snowflake.connector
        return True
    except ImportError:
        print("snowflake-connector-python not installed, skipping Snowflake tests")
        return False


def has_trino_connection() -> bool:
    """
    Check if Trino connection is available.
    
    Returns:
        True if Trino connection is available, False otherwise
    """
    # Check for required environment variables
    required_vars = ["TRINO_HOST", "TRINO_USER"]
    for var in required_vars:
        if not os.environ.get(var):
            print(f"Trino environment variable {var} not set, skipping Trino tests")
            return False
    
    # Trino connection requires the trino package
    try:
        import trino
        return True
    except ImportError:
        print("trino package not installed, skipping Trino tests")
        return False


def has_bigquery_connection() -> bool:
    """
    Check if BigQuery connection is available.
    
    Returns:
        True if BigQuery connection is available, False otherwise
    """
    # Check for BigQuery credentials
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("GOOGLE_APPLICATION_CREDENTIALS not set, skipping BigQuery tests")
        return False
    
    # BigQuery connection requires the google-cloud-bigquery package
    try:
        from google.cloud import bigquery
        return True
    except ImportError:
        print("google-cloud-bigquery not installed, skipping BigQuery tests")
        return False


def parse_args():
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run SQL Batcher tests")
    parser.add_argument("--all", action="store_true", help="Run all tests (requires database connections)")
    parser.add_argument("--core-only", action="store_true", help="Run only core tests (no database connections required)")
    parser.add_argument("--pg", action="store_true", help="Run only PostgreSQL tests")
    parser.add_argument("--snowflake", action="store_true", help="Run only Snowflake tests")
    parser.add_argument("--trino", action="store_true", help="Run only Trino tests")
    parser.add_argument("--bigquery", action="store_true", help="Run only BigQuery tests")
    parser.add_argument("--coverage", action="store_true", help="Collect test coverage information")
    parser.add_argument("--xml-report", action="store_true", help="Generate JUnit XML report")
    parser.add_argument("--verbose", "-v", action="store_true", help="Run tests with verbose output")
    
    return parser.parse_args()


def run_tests_for_adapter(adapter_name: str, test_file: str, has_connection_fn, options: List[str] = None) -> int:
    """
    Run tests for a specific adapter.
    
    Args:
        adapter_name: The name of the adapter (for display)
        test_file: The test file to run
        has_connection_fn: Function to check if connection is available
        options: Additional pytest options
        
    Returns:
        Exit code from pytest, or 0 if tests are skipped
    """
    if not has_connection_fn():
        print(f"Skipping {adapter_name} tests - no connection available")
        return 0
    
    cmd = ["pytest", test_file]
    
    if options:
        cmd.extend(options)
        
    print(f"Running {adapter_name} tests: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def main():
    """
    Main entry point.
    
    This function runs the tests based on the provided arguments.
    """
    args = parse_args()
    exit_codes = []
    
    # Determine which specific database adapter was requested
    specific_adapter = None
    if args.pg:
        specific_adapter = "postgresql"
    elif args.snowflake:
        specific_adapter = "snowflake"
    elif args.trino:
        specific_adapter = "trino"
    elif args.bigquery:
        specific_adapter = "bigquery"
    
    # Configure pytest options
    pytest_options = []
    if args.coverage:
        pytest_options.extend(["--cov=src/sql_batcher", "--cov-report=term", "--cov-report=html:coverage_html"])
    if args.xml_report:
        pytest_options.append("--junit-xml=test-results.xml")
    if args.verbose:
        pytest_options.append("-v")
    
    # Run tests based on arguments
    if specific_adapter:
        # Run only the requested adapter's tests
        if specific_adapter == "postgresql":
            exit_codes.append(run_postgresql_tests(pytest_options))
        elif specific_adapter == "snowflake":
            exit_codes.append(run_tests_for_adapter(
                "Snowflake", "tests/test_snowflake_adapter.py", has_snowflake_connection, pytest_options
            ))
        elif specific_adapter == "trino":
            exit_codes.append(run_tests_for_adapter(
                "Trino", "tests/test_trino_adapter.py", has_trino_connection, pytest_options
            ))
        elif specific_adapter == "bigquery":
            exit_codes.append(run_tests_for_adapter(
                "BigQuery", "tests/test_bigquery_adapter.py", has_bigquery_connection, pytest_options
            ))
    elif args.core_only:
        # Run only core tests
        exit_codes.append(run_core_tests(pytest_options))
        exit_codes.append(run_generic_adapter_tests(pytest_options))
    else:
        # Run core tests plus others based on flags
        exit_codes.append(run_core_tests(pytest_options))
        exit_codes.append(run_generic_adapter_tests(pytest_options))
        
        if args.all:
            # Run database-specific tests for available databases
            exit_codes.append(run_postgresql_tests(pytest_options))
            exit_codes.append(run_tests_for_adapter(
                "Snowflake", "tests/test_snowflake_adapter.py", has_snowflake_connection, pytest_options
            ))
            exit_codes.append(run_tests_for_adapter(
                "Trino", "tests/test_trino_adapter.py", has_trino_connection, pytest_options
            ))
            exit_codes.append(run_tests_for_adapter(
                "BigQuery", "tests/test_bigquery_adapter.py", has_bigquery_connection, pytest_options
            ))
    
    # Return non-zero if any test failed
    if any(code != 0 for code in exit_codes):
        sys.exit(1)
    else:
        print("All selected tests passed!")


if __name__ == "__main__":
    main()