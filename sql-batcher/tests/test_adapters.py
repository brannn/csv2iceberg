"""
Unit tests for SQLBatcher adapters.

This module contains tests for the database adapters used with SQLBatcher.
"""
import unittest
from unittest.mock import MagicMock, patch

from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.adapters.generic import GenericAdapter


class TestSQLAdapter(unittest.TestCase):
    """Test cases for the base SQLAdapter class."""
    
    def test_abstract_methods(self):
        """Test that SQLAdapter requires implementations of abstract methods."""
        with self.assertRaises(TypeError):
            # Should raise TypeError because it's an abstract class
            SQLAdapter()
    
    def test_subclass_requirements(self):
        """Test that subclasses must implement required methods."""
        # Create a minimal subclass that doesn't implement all methods
        class IncompleteAdapter(SQLAdapter):
            def execute(self, sql):
                pass
        
        # Should still raise TypeError because not all abstract methods are implemented
        with self.assertRaises(TypeError):
            IncompleteAdapter()
        
        # Create a complete minimal implementation
        class MinimalAdapter(SQLAdapter):
            def execute(self, sql):
                pass
            
            def get_max_query_size(self):
                return 1000
            
            def close(self):
                pass
        
        # This should not raise an error
        adapter = MinimalAdapter()
        self.assertIsNotNone(adapter)


class TestGenericAdapter(unittest.TestCase):
    """Test cases for the GenericAdapter class."""
    
    def setUp(self):
        """Set up test environment before each test."""
        self.mock_connection = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_connection.cursor.return_value = self.mock_cursor
        
        # Set up mock results for a query
        self.mock_cursor.description = [("id",), ("name",)]
        self.mock_cursor.fetchall.return_value = [(1, "test"), (2, "example")]
        
        self.adapter = GenericAdapter(connection=self.mock_connection)
    
    def test_initialization(self):
        """Test GenericAdapter initialization."""
        self.assertEqual(self.adapter.connection, self.mock_connection)
        self.assertEqual(self.adapter.max_query_size, 1000000)  # Default value
        
        # Test with custom max size
        adapter = GenericAdapter(connection=self.mock_connection, max_query_size=500000)
        self.assertEqual(adapter.max_query_size, 500000)
    
    def test_execute(self):
        """Test execute method."""
        # Execute a test query
        result = self.adapter.execute("SELECT * FROM test")
        
        # Verify the connection was used correctly
        self.mock_connection.cursor.assert_called_once()
        self.mock_cursor.execute.assert_called_once_with("SELECT * FROM test")
        self.mock_cursor.fetchall.assert_called_once()
        
        # Verify the results
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], (1, "test"))
        self.assertEqual(result[1], (2, "example"))
    
    def test_get_max_query_size(self):
        """Test get_max_query_size method."""
        size = self.adapter.get_max_query_size()
        self.assertEqual(size, 1000000)  # Default value
    
    def test_close(self):
        """Test close method."""
        self.adapter.close()
        self.mock_connection.close.assert_called_once()
    
    def test_transaction_methods(self):
        """Test transaction management methods."""
        # Begin transaction
        self.adapter.begin_transaction()
        self.mock_connection.begin.assert_called_once()
        
        # Commit transaction
        self.adapter.commit_transaction()
        self.mock_connection.commit.assert_called_once()
        
        # Rollback transaction
        self.adapter.rollback_transaction()
        self.mock_connection.rollback.assert_called_once()


if __name__ == '__main__':
    unittest.main()