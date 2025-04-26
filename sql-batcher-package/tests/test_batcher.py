"""
Unit tests for SQL Batcher core functionality.
"""
import unittest
from unittest import mock

from sql_batcher import SQLBatcher
from sql_batcher.query_collector import ListQueryCollector


class TestSQLBatcher(unittest.TestCase):
    """Test cases for SQLBatcher class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.batcher = SQLBatcher(max_bytes=100)
        self.statements = [
            "INSERT INTO test VALUES (1)",
            "INSERT INTO test VALUES (2)",
            "INSERT INTO test VALUES (3)"
        ]
    
    def test_init_with_defaults(self):
        """Test initialization with default values."""
        batcher = SQLBatcher()
        self.assertEqual(batcher.max_bytes, 1_000_000)
        self.assertEqual(batcher.delimiter, ";")
        self.assertFalse(batcher.dry_run)
    
    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        batcher = SQLBatcher(max_bytes=500, delimiter="|", dry_run=True)
        self.assertEqual(batcher.max_bytes, 500)
        self.assertEqual(batcher.delimiter, "|")
        self.assertTrue(batcher.dry_run)
    
    def test_add_statement(self):
        """Test adding a statement to the batch."""
        # Add a statement
        result = self.batcher.add_statement("INSERT INTO test VALUES (1)")
        
        # Should not need to flush yet
        self.assertFalse(result)
        self.assertEqual(len(self.batcher.current_batch), 1)
        
        # Add more statements until batch is full
        while not result:
            result = self.batcher.add_statement("INSERT INTO test VALUES (2)")
        
        # Now we should need to flush
        self.assertTrue(result)
    
    def test_reset(self):
        """Test resetting the batch."""
        # Add a statement
        self.batcher.add_statement("INSERT INTO test VALUES (1)")
        
        # Reset the batch
        self.batcher.reset()
        
        # Batch should be empty
        self.assertEqual(len(self.batcher.current_batch), 0)
        self.assertEqual(self.batcher.current_size, 0)
    
    def test_flush(self):
        """Test flushing the batch."""
        # Add a statement
        self.batcher.add_statement("INSERT INTO test VALUES (1)")
        
        # Mock the callback function
        mock_callback = mock.Mock()
        
        # Flush the batch
        count = self.batcher.flush(mock_callback)
        
        # Should have executed one statement
        self.assertEqual(count, 1)
        mock_callback.assert_called_once()
        
        # Batch should be empty
        self.assertEqual(len(self.batcher.current_batch), 0)
    
    def test_process_statements(self):
        """Test processing multiple statements."""
        # Mock the callback function
        mock_callback = mock.Mock()
        
        # Process statements
        count = self.batcher.process_statements(self.statements, mock_callback)
        
        # Should have processed all statements
        self.assertEqual(count, 3)
        
        # Should have called the callback at least once
        mock_callback.assert_called()
        
        # Batch should be empty
        self.assertEqual(len(self.batcher.current_batch), 0)
    
    def test_dry_run_mode(self):
        """Test dry run mode with query collector."""
        # Create a batcher in dry run mode
        batcher = SQLBatcher(max_bytes=100, dry_run=True)
        
        # Create a query collector
        collector = ListQueryCollector()
        
        # Mock the callback function
        mock_callback = mock.Mock()
        
        # Process statements
        count = batcher.process_statements(
            self.statements, 
            mock_callback,
            query_collector=collector,
            metadata={"test": True}
        )
        
        # Should have processed all statements
        self.assertEqual(count, 3)
        
        # Callback should not have been called in dry run mode
        mock_callback.assert_not_called()
        
        # Query collector should have collected queries
        self.assertGreater(len(collector.get_queries()), 0)
        
        # Check that metadata was included
        for query_info in collector.get_queries():
            self.assertTrue(query_info["metadata"]["test"])
    
    def test_oversized_statement(self):
        """Test handling of statements that exceed the maximum batch size."""
        # Create a batcher with a very small size limit
        batcher = SQLBatcher(max_bytes=10)
        
        # Create a statement that exceeds the limit
        oversized_statement = "INSERT INTO test VALUES (1, 'This is a very long value that exceeds the limit')"
        
        # Mock the callback function
        mock_callback = mock.Mock()
        
        # Process the oversized statement
        count = batcher.process_statements([oversized_statement], mock_callback)
        
        # Should have processed the statement
        self.assertEqual(count, 1)
        
        # Callback should have been called once
        mock_callback.assert_called_once()


if __name__ == "__main__":
    unittest.main()