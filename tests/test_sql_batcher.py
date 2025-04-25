"""
Unit tests for the SQL batching module.
"""
import unittest
from unittest.mock import Mock, call

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.sql_batcher import SQLBatcher

class TestSQLBatcher(unittest.TestCase):
    """Test cases for the SQLBatcher class."""
    
    def test_basic_batching(self):
        """Test basic batching functionality."""
        # Create a batcher with a small max size
        batcher = SQLBatcher(max_bytes=50)
        
        # Create a mock execute function
        mock_execute = Mock()
        
        # Create some test SQL statements
        statements = [
            "SELECT 1",
            "SELECT 2",
            "SELECT 3",
            "SELECT 4 FROM really_long_table_name"
        ]
        
        # Process the statements
        count = batcher.process_statements(statements, mock_execute)
        
        # Verify results
        self.assertEqual(count, 4)
        self.assertEqual(mock_execute.call_count, 2)  # Should have made 2 batches
    
    def test_empty_batch(self):
        """Test handling of empty batches."""
        batcher = SQLBatcher()
        mock_execute = Mock()
        
        # No statements should result in no executions
        count = batcher.process_statements([], mock_execute)
        self.assertEqual(count, 0)
        mock_execute.assert_not_called()
        
        # Explicitly flushing an empty batch should also result in no executions
        batcher.flush(mock_execute)
        mock_execute.assert_not_called()
    
    def test_large_statement(self):
        """Test handling of statements larger than the max size."""
        batcher = SQLBatcher(max_bytes=20)
        mock_execute = Mock()
        
        # This statement is larger than our max size
        large_statement = "SELECT * FROM very_very_long_table_name"
        
        # It should still be processed (with a warning)
        count = batcher.process_statements([large_statement], mock_execute)
        
        # Verify it was executed
        self.assertEqual(count, 1)
        mock_execute.assert_called_once()
    
    def test_dry_run_mode(self):
        """Test dry run mode."""
        batcher = SQLBatcher(dry_run=True)
        mock_execute = Mock()
        mock_collector = Mock()
        
        statements = ["SELECT 1", "SELECT 2"]
        metadata = {"type": "DML", "row_count": 2, "table_name": "test"}
        
        # Process in dry run mode
        count = batcher.process_statements(statements, mock_execute, mock_collector, metadata)
        
        # Execute should not be called
        self.assertEqual(count, 2)
        mock_execute.assert_not_called()
        
        # Query collector should be called
        mock_collector.add_query.assert_called_once()
    
    def test_custom_delimiter(self):
        """Test custom delimiter."""
        # Use a custom delimiter
        batcher = SQLBatcher(max_bytes=100, delimiter=" UNION ALL ")
        mock_execute = Mock()
        
        statements = ["SELECT 1", "SELECT 2"]
        
        # Process with custom delimiter
        batcher.process_statements(statements, mock_execute)
        
        # Verify the statements were combined with the custom delimiter
        mock_execute.assert_called_once_with("SELECT 1 UNION ALL SELECT 2")
    
    def test_exception_handling(self):
        """Test exception handling during execution."""
        batcher = SQLBatcher()
        mock_execute = Mock(side_effect=Exception("Test exception"))
        
        # Should re-raise exceptions
        with self.assertRaises(RuntimeError):
            batcher.process_statements(["SELECT 1"], mock_execute)

if __name__ == "__main__":
    unittest.main()