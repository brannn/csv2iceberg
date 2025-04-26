"""
Unit tests for SQL Batcher core functionality.
"""
import pytest
from unittest import mock

from sql_batcher.batcher import SQLBatcher
from sql_batcher.query_collector import ListQueryCollector


@pytest.mark.core
class TestSQLBatcher:
    """Test cases for SQLBatcher class."""
    
    @pytest.fixture(autouse=True)
    def setup_batcher(self):
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
        assert batcher.max_bytes == 1_000_000
        assert batcher.delimiter == ";"
        assert batcher.dry_run is False
    
    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        batcher = SQLBatcher(max_bytes=500, delimiter="|", dry_run=True)
        assert batcher.max_bytes == 500
        assert batcher.delimiter == "|"
        assert batcher.dry_run is True
    
    def test_add_statement(self):
        """Test adding a statement to the batch."""
        # Add a statement
        result = self.batcher.add_statement("INSERT INTO test VALUES (1)")
        
        # Should not need to flush yet
        assert result is False
        assert len(self.batcher.current_batch) == 1
        
        # Add more statements until batch is full
        while not result:
            result = self.batcher.add_statement("INSERT INTO test VALUES (2)")
        
        # Now we should need to flush
        assert result is True
    
    def test_reset(self):
        """Test resetting the batch."""
        # Add a statement
        self.batcher.add_statement("INSERT INTO test VALUES (1)")
        
        # Reset the batch
        self.batcher.reset()
        
        # Batch should be empty
        assert len(self.batcher.current_batch) == 0
        assert self.batcher.current_size == 0
    
    def test_flush(self):
        """Test flushing the batch."""
        # Add a statement
        self.batcher.add_statement("INSERT INTO test VALUES (1)")
        
        # Mock the callback function
        mock_callback = mock.Mock()
        
        # Flush the batch
        count = self.batcher.flush(mock_callback)
        
        # Should have executed one statement
        assert count == 1
        mock_callback.assert_called_once()
        
        # Batch should be empty
        assert len(self.batcher.current_batch) == 0
    
    def test_process_statements(self):
        """Test processing multiple statements."""
        # Mock the callback function
        mock_callback = mock.Mock()
        
        # Process statements
        count = self.batcher.process_statements(self.statements, mock_callback)
        
        # Should have processed all statements
        assert count == 3
        
        # Should have called the callback at least once
        mock_callback.assert_called()
        
        # Batch should be empty
        assert len(self.batcher.current_batch) == 0
    
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
        assert count == 3
        
        # Callback should not have been called in dry run mode
        mock_callback.assert_not_called()
        
        # Query collector should have collected queries
        assert len(collector.get_queries()) > 0
        
        # Check that metadata was included
        for query_info in collector.get_queries():
            assert query_info["metadata"]["test"] is True
    
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
        assert count == 1
        
        # Callback should have been called once
        mock_callback.assert_called_once()
    
    def test_column_detection(self):
        """Test column count detection in INSERT statements."""
        batcher = SQLBatcher()
        
        # Test with explicit column list
        result = batcher.detect_column_count(
            "INSERT INTO users (id, name, email, age) VALUES (1, 'John', 'john@example.com', 30)"
        )
        assert result == 4
        
        # Test with VALUES only
        result = batcher.detect_column_count(
            "INSERT INTO users VALUES (1, 'John', 'john@example.com', 30)"
        )
        assert result == 4
        
        # Test with complex nested values
        result = batcher.detect_column_count(
            "INSERT INTO data VALUES (1, ARRAY[1, 2, 3], '{\"key\": \"value\"}', 'text')"
        )
        assert result == 4
        
        # Test with non-INSERT statement
        result = batcher.detect_column_count("SELECT * FROM users")
        assert result is None
    
    def test_auto_adjust_for_columns(self):
        """Test automatic batch size adjustment based on column count."""
        # Create a batcher with auto-adjustment enabled and specific baseline
        batcher = SQLBatcher(
            max_bytes=1_000_000,
            auto_adjust_for_columns=True,
            reference_column_count=10
        )
        
        # Process statements with more columns than reference (should reduce batch size)
        wide_statements = [
            "INSERT INTO wide_table VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)"
        ] * 10
        
        mock_callback = mock.Mock()
        batcher.process_statements(wide_statements, mock_callback)
        
        # Column count should be detected
        assert batcher.column_count == 20
        
        # Adjustment factor should be less than 1.0 (smaller batches)
        assert batcher.adjustment_factor < 1.0
        
        # Adjusted max_bytes should be less than original
        assert batcher.get_adjusted_max_bytes() < batcher.max_bytes
        
        # Create a batcher for narrow table test
        batcher = SQLBatcher(
            max_bytes=1_000_000,
            auto_adjust_for_columns=True,
            reference_column_count=10
        )
        
        # Process statements with fewer columns than reference (should increase batch size)
        narrow_statements = [
            "INSERT INTO narrow_table VALUES (1, 2, 3)"
        ] * 10
        
        mock_callback = mock.Mock()
        batcher.process_statements(narrow_statements, mock_callback)
        
        # Column count should be detected
        assert batcher.column_count == 3
        
        # Adjustment factor should be greater than 1.0 (larger batches)
        assert batcher.adjustment_factor > 1.0
        
        # Adjusted max_bytes should be greater than original
        assert batcher.get_adjusted_max_bytes() > batcher.max_bytes
    
    def test_adjustment_factor_bounds(self):
        """Test that adjustment factor is properly bounded."""
        # Create a batcher with specific bounds
        batcher = SQLBatcher(
            auto_adjust_for_columns=True,
            reference_column_count=5,
            min_adjustment_factor=0.2,
            max_adjustment_factor=3.0
        )
        
        # Test very wide table (many columns) - should hit lower bound
        wide_statement = "INSERT INTO very_wide_table VALUES (" + ", ".join(["1"] * 50) + ")"
        batcher.update_adjustment_factor(wide_statement)
        
        # Should be clamped to min value
        assert batcher.adjustment_factor == 0.2
        
        # Reset and test very narrow table
        batcher = SQLBatcher(
            auto_adjust_for_columns=True,
            reference_column_count=5,
            min_adjustment_factor=0.2,
            max_adjustment_factor=3.0
        )
        
        # Test very narrow table (one column) - should hit upper bound
        narrow_statement = "INSERT INTO single_column_table VALUES (1)"
        batcher.update_adjustment_factor(narrow_statement)
        
        # Should be clamped to max value
        assert batcher.adjustment_factor == 3.0