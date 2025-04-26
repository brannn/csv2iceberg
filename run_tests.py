#!/usr/bin/env python3
"""
Simple test runner for SQL Batcher without requiring full package installation
"""
import os
import sys
import unittest

# Add src directory to Python path
sys.path.insert(0, os.path.abspath('src'))

# Find and run tests that don't require database connections
def run_tests():
    """Run core SQL Batcher tests that don't require database connections"""
    # Create a test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add only tests that don't require database connections
    # Start with the batcher tests which are core functionality
    from tests.test_batcher import TestSQLBatcher
    suite.addTest(loader.loadTestsFromTestCase(TestSQLBatcher))
    
    # Add adapter tests but skip database connection tests
    try:
        from tests.test_adapters import TestSQLAdapter
        suite.addTest(loader.loadTestsFromTestCase(TestSQLAdapter))
    except (ImportError, AttributeError) as e:
        print(f"Unable to load adapter tests: {e}")
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    return runner.run(suite)

if __name__ == "__main__":
    result = run_tests()
    sys.exit(not result.wasSuccessful())