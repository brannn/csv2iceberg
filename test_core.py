#!/usr/bin/env python3
"""
Simple test runner for SQL Batcher core functionality
"""
import os
import sys
import unittest

# Add src directory to Python path
sys.path.insert(0, os.path.abspath('src'))

def run_core_tests():
    # Create a test suite
    test_loader = unittest.TestLoader()
    test_suite = unittest.TestSuite()
    
    # Import the test classes for the core functionality
    sys.path.insert(0, os.path.abspath('tests'))
    from test_batcher import TestSQLBatcher
    
    # Add core tests to suite
    test_suite.addTest(test_loader.loadTestsFromTestCase(TestSQLBatcher))
    
    # Try to add the SQLAdapter abstract class tests
    try:
        from test_adapters import TestSQLAdapter
        test_suite.addTest(test_loader.loadTestsFromTestCase(TestSQLAdapter))
    except (ImportError, AttributeError) as e:
        print(f"Skipping adapter tests: {e}")
    
    # Run tests
    test_runner = unittest.TextTestRunner(verbosity=2)
    return test_runner.run(test_suite)

if __name__ == "__main__":
    result = run_core_tests()
    sys.exit(not result.wasSuccessful())