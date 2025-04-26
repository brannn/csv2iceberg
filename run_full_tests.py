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


def run_core_tests(coverage: bool = False) -> int:
    """
    Run core SQL Batcher tests that don't require database connections.
    
    Args:
        coverage: Whether to collect coverage information
        
    Returns:
        Exit code from pytest
    """
    cmd = ["pytest", "tests/test_batcher.py", "tests/test_adapters.py::TestSQLAdapter", "-v"]
    
    if coverage:
        cmd.extend(["--cov=src/sql_batcher", "--cov-report=term", "--cov-report=html:coverage_html"])
    
    print(f"Running core tests: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def run_generic_adapter_tests() -> int:
    """
    Run generic adapter tests with mocked connections.
    
    Returns:
        Exit code from pytest
    """
    cmd = ["pytest", "tests/test_adapters.py::TestGenericAdapter", "-v"]
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


def run_postgresql_tests() -> int:
    """
    Run PostgreSQL adapter tests.
    
    Returns:
        Exit code from pytest, or 0 if tests are skipped
    """
    if not has_postgresql_connection():
        print("Skipping PostgreSQL tests - no connection available")
        return 0
    
    cmd = ["pytest", "tests/test_postgresql_adapter.py", "-v"]
    print(f"Running PostgreSQL tests: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


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
    parser.add_argument("--coverage", action="store_true", help="Collect test coverage information")
    
    return parser.parse_args()


def main():
    """
    Main entry point.
    
    This function runs the tests based on the provided arguments.
    """
    args = parse_args()
    exit_codes = []
    
    if args.pg:
        # Run only PostgreSQL tests
        exit_codes.append(run_postgresql_tests())
    elif args.core_only:
        # Run only core tests
        exit_codes.append(run_core_tests(coverage=args.coverage))
        exit_codes.append(run_generic_adapter_tests())
    else:
        # Run all tests or tests that don't need connections
        exit_codes.append(run_core_tests(coverage=args.coverage))
        exit_codes.append(run_generic_adapter_tests())
        
        if args.all:
            # Run database-specific tests
            exit_codes.append(run_postgresql_tests())
    
    # Return non-zero if any test failed
    if any(code != 0 for code in exit_codes):
        sys.exit(1)
    else:
        print("All selected tests passed!")


if __name__ == "__main__":
    main()