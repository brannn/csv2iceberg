#!/usr/bin/env python3
"""
Comprehensive test runner for SQL Batcher with intelligent adapter handling
"""
import os
import sys
import argparse
import subprocess
from typing import List, Optional

def run_core_tests() -> int:
    """Run core SQL Batcher tests that don't require database connections"""
    print("Running core SQL Batcher tests...")
    
    # Run core tests that don't require database connections
    result = subprocess.run([
        "python", "-m", "pytest",
        "tests/test_batcher.py::TestSQLBatcher",  # Core batching functionality
        "tests/test_adapters.py::TestSQLAdapter",  # Abstract adapter interface
        "-v"
    ])
    
    return result.returncode

def run_generic_adapter_tests() -> int:
    """Run generic adapter tests with mocked connections"""
    print("\nRunning generic adapter tests with mocked database connections...")
    
    # Run adapter tests that can use mocked connections, but skip transaction tests
    # which require more sophisticated mocking
    result = subprocess.run([
        "python", "-m", "pytest",
        "tests/test_adapters.py::TestGenericAdapter::test_init",
        "tests/test_adapters.py::TestGenericAdapter::test_get_max_query_size",
        "tests/test_adapters.py::TestGenericAdapter::test_execute_select",
        "tests/test_adapters.py::TestGenericAdapter::test_execute_insert",
        "tests/test_adapters.py::TestGenericAdapter::test_execute_with_fetch_results_false",
        "-v"
    ])
    
    return result.returncode

def run_postgresql_tests() -> bool:
    """Check if PostgreSQL connection is available and run PostgreSQL adapter tests"""
    print("\nChecking PostgreSQL connection availability...")
    
    # Environment variables for PostgreSQL connection
    pg_host = os.environ.get("PGHOST", "localhost")
    pg_port = os.environ.get("PGPORT", "5432")
    pg_user = os.environ.get("PGUSER", "postgres")
    pg_database = os.environ.get("PGDATABASE", "postgres")
    
    # Check if psycopg2 is installed
    try:
        import psycopg2
    except ImportError:
        print("psycopg2 is not installed. Skipping PostgreSQL adapter tests.")
        return False
    
    # Check if PostgreSQL server is accessible
    try:
        # Basic connection test
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            user=pg_user,
            database=pg_database,
            connect_timeout=5
        )
        conn.close()
        print(f"PostgreSQL connection successful to {pg_host}:{pg_port}")
        
        # Run PostgreSQL-specific adapter tests
        print("Running PostgreSQL adapter tests...")
        result = subprocess.run([
            "python", "-m", "pytest",
            "tests/test_postgresql_adapter.py",
            "-v"
        ])
        
        return result.returncode == 0
    
    except Exception as e:
        print(f"PostgreSQL connection failed: {str(e)}")
        print("Skipping PostgreSQL adapter tests.")
        return False

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Run SQL Batcher tests")
    parser.add_argument("--all", action="store_true", help="Attempt to run all tests including database adapters")
    parser.add_argument("--core-only", action="store_true", help="Run only core tests (no adapters)")
    parser.add_argument("--pg", action="store_true", help="Run PostgreSQL adapter tests (if connection available)")
    parser.add_argument("--coverage", action="store_true", help="Generate test coverage report")
    
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_args()
    exit_code = 0
    
    # Always run core tests
    core_result = run_core_tests()
    if core_result != 0:
        print("\n❌ Core tests failed. Stopping.")
        return core_result
    
    if not args.core_only:
        # Run generic adapter tests
        generic_result = run_generic_adapter_tests()
        if generic_result != 0:
            print("\n❌ Generic adapter tests failed.")
            exit_code = generic_result
        
        # Conditionally run PostgreSQL tests if --all or --pg flag is provided
        if args.all or args.pg:
            pg_success = run_postgresql_tests()
            if not pg_success:
                print("\n⚠️ PostgreSQL adapter tests were skipped or failed.")
                exit_code = 1
    
    if args.coverage:
        print("\nGenerating test coverage report...")
        coverage_result = subprocess.run([
            "python", "-m", "pytest",
            "--cov=src/sql_batcher",
            "--cov-report=term",
            "--cov-report=html:coverage_html",
            "tests/"
        ])
        if coverage_result.returncode != 0:
            exit_code = coverage_result.returncode
    
    if exit_code == 0:
        print("\n✅ All selected tests passed!")
    else:
        print("\n❌ Some tests failed or were skipped.")
    
    return exit_code

if __name__ == "__main__":
    sys.exit(main())