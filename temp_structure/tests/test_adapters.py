"""
Unit tests for SQL Batcher adapters.
"""
import unittest
from unittest import mock
import sqlite3

from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.adapters.generic import GenericAdapter


class TestSQLAdapter(unittest.TestCase):
    """Test cases for abstract SQLAdapter class."""
    
    def test_abstract_methods(self):
        """Test that SQLAdapter requires implementing abstract methods."""
        # Should not be able to instantiate the abstract class
        with self.assertRaises(TypeError):
            SQLAdapter()
        
        # Create a minimal implementation
        class MinimalAdapter(SQLAdapter):
            def execute(self, sql):
                return []
            
            def get_max_query_size(self):
                return 1000
            
            def close(self):
                pass
        
        # Should be able to instantiate the minimal implementation
        adapter = MinimalAdapter()
        self.assertIsNotNone(adapter)
        
        # Default transaction methods should not raise exceptions
        adapter.begin_transaction()
        adapter.commit_transaction()
        adapter.rollback_transaction()


class TestGenericAdapter(unittest.TestCase):
    """Test cases for GenericAdapter."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create an in-memory SQLite database
        self.connection = sqlite3.connect(":memory:")
        
        # Create a test table
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        cursor.execute("INSERT INTO test VALUES (1, 'Test 1')")
        cursor.execute("INSERT INTO test VALUES (2, 'Test 2')")
        self.connection.commit()
        
        # Create the adapter
        self.adapter = GenericAdapter(
            connection=self.connection,
            max_query_size=1000
        )
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.adapter.close()
    
    def test_init(self):
        """Test initialization."""
        self.assertEqual(self.adapter.max_query_size, 1000)
        self.assertTrue(self.adapter.fetch_results)
    
    def test_get_max_query_size(self):
        """Test get_max_query_size method."""
        self.assertEqual(self.adapter.get_max_query_size(), 1000)
    
    def test_execute_select(self):
        """Test executing a SELECT statement."""
        results = self.adapter.execute("SELECT * FROM test ORDER BY id")
        
        # Should return results
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0][0], 1)
        self.assertEqual(results[0][1], "Test 1")
    
    def test_execute_insert(self):
        """Test executing an INSERT statement."""
        results = self.adapter.execute("INSERT INTO test VALUES (3, 'Test 3')")
        
        # Should not return results for INSERT
        self.assertEqual(len(results), 0)
        
        # But should have inserted the row
        results = self.adapter.execute("SELECT * FROM test WHERE id = 3")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], 3)
        self.assertEqual(results[0][1], "Test 3")
    
    def test_execute_with_fetch_results_false(self):
        """Test executing with fetch_results=False."""
        # Create an adapter with fetch_results=False
        adapter = GenericAdapter(
            connection=self.connection,
            fetch_results=False
        )
        
        # Execute a SELECT statement
        results = adapter.execute("SELECT * FROM test")
        
        # Should not return results
        self.assertEqual(len(results), 0)
    
    def test_transactions(self):
        """Test transaction methods."""
        # Begin a transaction
        self.adapter.begin_transaction()
        
        # Insert a row
        self.adapter.execute("INSERT INTO test VALUES (4, 'Test 4')")
        
        # Row should be visible within the transaction
        results = self.adapter.execute("SELECT * FROM test WHERE id = 4")
        self.assertEqual(len(results), 1)
        
        # Rollback the transaction
        self.adapter.rollback_transaction()
        
        # Row should not be visible after rollback
        results = self.adapter.execute("SELECT * FROM test WHERE id = 4")
        self.assertEqual(len(results), 0)
        
        # Begin another transaction
        self.adapter.begin_transaction()
        
        # Insert a row
        self.adapter.execute("INSERT INTO test VALUES (5, 'Test 5')")
        
        # Commit the transaction
        self.adapter.commit_transaction()
        
        # Row should still be visible after commit
        results = self.adapter.execute("SELECT * FROM test WHERE id = 5")
        self.assertEqual(len(results), 1)


if __name__ == "__main__":
    unittest.main()