"""
Unit tests for SQLBatcher core functionality.

This module contains tests for the core SQLBatcher class.
"""
import unittest
from unittest.mock import MagicMock

from sql_batcher import SQLBatcher


class TestSQLBatcher(unittest.TestCase):
    """Test cases for the SQLBatcher class."""
    
    def setUp(self):
        """Set up test environment before each test."""
        self.batcher = SQLBatcher(max_bytes=100)
        self.mock_callback = MagicMock()
    
    def test_initialization(self):
        """Test SQLBatcher initialization."""
        batcher = SQLBatcher(max_bytes=200, delimiter=";", dry_run=True)
        self.assertEqual(batcher.max_bytes, 200)
        self.assertEqual(batcher.delimiter, ";")
        self.assertTrue(batcher.dry_run)
        self.assertEqual(batcher.current_batch, [])
        self.assertEqual(batcher.current_size, 0)
        
    def test_add_statement(self):
        """Test adding statements to the batcher."""
        # First statement should be added without issue
        result = self.batcher.add_statement("SELECT * FROM table")
        self.assertFalse(result)
        self.assertEqual(len(self.batcher.current_batch), 1)
        
        # Add another statement that will fit
        result = self.batcher.add_statement("SELECT id FROM table")
        self.assertFalse(result)
        self.assertEqual(len(self.batcher.current_batch), 2)
        
        # Add a statement that would exceed batch size
        result = self.batcher.add_statement("SELECT * FROM table WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)")
        self.assertTrue(result)  # Indicates batch is full
        self.assertEqual(len(self.batcher.current_batch), 2)  # Batch wasn't changed
        
    def test_reset(self):
        """Test resetting the batcher."""
        self.batcher.add_statement("SELECT * FROM table")
        self.assertNotEqual(self.batcher.current_size, 0)
        self.assertNotEqual(len(self.batcher.current_batch), 0)
        
        self.batcher.reset()
        self.assertEqual(self.batcher.current_size, 0)
        self.assertEqual(len(self.batcher.current_batch), 0)
        
    def test_flush(self):
        """Test flushing the batch."""
        # Add some statements
        self.batcher.add_statement("SELECT 1")
        self.batcher.add_statement("SELECT 2")
        
        # Flush and verify callback was called
        self.batcher.flush(self.mock_callback)
        self.assertEqual(self.mock_callback.call_count, 2)
        self.mock_callback.assert_any_call("SELECT 1")
        self.mock_callback.assert_any_call("SELECT 2")
        
        # Batch should be reset after flush
        self.assertEqual(self.batcher.current_size, 0)
        self.assertEqual(len(self.batcher.current_batch), 0)
        
    def test_process_statements(self):
        """Test processing a list of statements."""
        statements = ["SELECT 1", "SELECT 2", "SELECT 3"]
        result = self.batcher.process_statements(statements, self.mock_callback)
        
        # Verify all statements were processed
        self.assertEqual(result, 3)
        self.assertEqual(self.mock_callback.call_count, 3)
        
    def test_oversized_statement(self):
        """Test handling of a statement that exceeds max size."""
        large_statement = "SELECT * FROM " + ("x" * 200)  # Exceed max_bytes
        
        # This should log a warning but still add the statement
        result = self.batcher.add_statement(large_statement)
        self.assertFalse(result)  # Returns False because the batch was empty
        self.assertEqual(len(self.batcher.current_batch), 1)
        
    def test_dry_run_mode(self):
        """Test dry run mode."""
        batcher = SQLBatcher(max_bytes=100, dry_run=True)
        mock_collector = MagicMock()
        mock_collector.add_query = MagicMock()
        
        statements = ["SELECT 1", "SELECT 2"]
        batcher.process_statements(
            statements, 
            self.mock_callback,
            query_collector=mock_collector,
            metadata={"type": "SELECT", "table_name": "test"}
        )
        
        # In dry run mode, the callback should not be called
        self.mock_callback.assert_not_called()
        
        # But the collector should be used
        self.assertEqual(mock_collector.add_query.call_count, 2)
        

if __name__ == '__main__':
    unittest.main()