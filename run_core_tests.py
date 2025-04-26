#!/usr/bin/env python3
"""
Script to run SQL Batcher core tests that don't require database connections
"""
import os
import sys
import subprocess

def run_core_tests():
    """Run only the core tests that don't require database connections"""
    # Use pytest with a marker to skip tests that require database connections
    command = [
        "python", "-m", "pytest",
        "tests/test_batcher.py::TestSQLBatcher",  # Core batching functionality
        "tests/test_adapters.py::TestSQLAdapter",  # Abstract adapter class tests
        "-v"  # Verbose output
    ]
    
    return subprocess.run(command)

if __name__ == "__main__":
    result = run_core_tests()
    sys.exit(result.returncode)