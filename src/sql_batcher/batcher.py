"""
Core SQL Batcher implementation.

This module contains the SQLBatcher class, which is the main entry point for
batching SQL statements based on size limits.
"""
import re
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from sql_batcher.query_collector import QueryCollector


class SQLBatcher:
    """
    SQL Batcher for efficiently executing SQL statements in batches.
    
    SQL Batcher addresses a common challenge in database programming: efficiently
    executing many SQL statements while respecting query size limitations. It's
    especially valuable for systems like Trino, Spark SQL, and Snowflake that
    have query size or memory constraints.
    
    Attributes:
        max_bytes: Maximum batch size in bytes
        delimiter: SQL statement delimiter
        dry_run: Whether to operate in dry run mode (without executing)
        current_batch: Current batch of SQL statements
        current_size: Current size of the batch in bytes
        auto_adjust_for_columns: Whether to dynamically adjust batch size based on column count
        reference_column_count: The reference column count for auto-adjustment (baseline)
        min_adjustment_factor: Minimum adjustment factor for batch size
        max_adjustment_factor: Maximum adjustment factor for batch size
        column_count: Detected column count for INSERT statements
        adjustment_factor: Current adjustment factor based on column count
    """
    
    def __init__(
        self,
        max_bytes: int = 1_000_000,
        delimiter: str = ";",
        dry_run: bool = False,
        auto_adjust_for_columns: bool = True,
        reference_column_count: int = 5,
        min_adjustment_factor: float = 0.2,
        max_adjustment_factor: float = 5.0
    ):
        """
        Initialize SQL Batcher.
        
        Args:
            max_bytes: Maximum batch size in bytes
            delimiter: SQL statement delimiter
            dry_run: Whether to operate in dry run mode (without executing)
            auto_adjust_for_columns: Whether to dynamically adjust batch size based on column count
            reference_column_count: The reference column count for auto-adjustment (baseline)
            min_adjustment_factor: Minimum adjustment factor for batch size
            max_adjustment_factor: Maximum adjustment factor for batch size
        """
        self.max_bytes = max_bytes
        self.delimiter = delimiter
        self.dry_run = dry_run
        self.auto_adjust_for_columns = auto_adjust_for_columns
        self.reference_column_count = reference_column_count
        self.min_adjustment_factor = min_adjustment_factor
        self.max_adjustment_factor = max_adjustment_factor
        self.current_batch: List[str] = []
        self.current_size: int = 0
        
        # Column detection state
        self.column_count: Optional[int] = None
        self.adjustment_factor: float = 1.0
    
    def detect_column_count(self, statement: str) -> Optional[int]:
        """
        Detect the number of columns in an INSERT statement.
        
        Args:
            statement: SQL statement to analyze
            
        Returns:
            Number of columns detected, or None if not an INSERT statement or cannot be determined
        """
        # Only process INSERT statements
        if not re.search(r'^\s*INSERT\s+INTO', statement, re.IGNORECASE):
            return None
            
        # Try to find column count from VALUES clause
        values_pattern = r'VALUES\s*\(([^)]*)\)'
        match = re.search(values_pattern, statement, re.IGNORECASE)
        if match:
            # Count commas in the first VALUES group and add 1
            values_content = match.group(1)
            # Handle nested parentheses in complex expressions
            depth = 0
            comma_count = 0
            for char in values_content:
                if char == '(' or char == '[' or char == '{':
                    depth += 1
                elif char == ')' or char == ']' or char == '}':
                    depth -= 1
                elif char == ',' and depth == 0:
                    comma_count += 1
            return comma_count + 1
        
        # Try to find explicit column list
        columns_pattern = r'INSERT\s+INTO\s+\w+\s*\(([^)]*)\)'
        match = re.search(columns_pattern, statement, re.IGNORECASE)
        if match:
            columns_str = match.group(1)
            # Count commas in the column list and add 1
            comma_count = columns_str.count(',')
            return comma_count + 1
            
        return None
        
    def update_adjustment_factor(self, statement: str) -> None:
        """
        Update the adjustment factor based on the column count in the statement.
        
        Args:
            statement: SQL statement to analyze
        """
        if not self.auto_adjust_for_columns:
            return
            
        # Only detect columns if we haven't already
        if self.column_count is None:
            detected_count = self.detect_column_count(statement)
            if detected_count is not None:
                self.column_count = detected_count
                
                # Calculate adjustment factor
                # More columns -> smaller batches (lower adjusted max_bytes)
                # Fewer columns -> larger batches (higher adjusted max_bytes)
                raw_factor = self.reference_column_count / max(1, self.column_count)
                
                # Clamp to min/max bounds
                self.adjustment_factor = max(
                    self.min_adjustment_factor,
                    min(self.max_adjustment_factor, raw_factor)
                )
                
                # Logging for debugging
                import logging
                logging.debug(
                    f"Column-based adjustment: detected {self.column_count} columns, "
                    f"reference is {self.reference_column_count}, "
                    f"adjustment factor is {self.adjustment_factor:.2f}"
                )
    
    def get_adjusted_max_bytes(self) -> int:
        """
        Get the max_bytes value adjusted for column count.
        
        Returns:
            Adjusted max_bytes value
        """
        if not self.auto_adjust_for_columns or self.adjustment_factor == 1.0:
            return self.max_bytes
            
        return int(self.max_bytes * self.adjustment_factor)
        
    def add_statement(self, statement: str) -> bool:
        """
        Add a statement to the current batch.
        
        Args:
            statement: SQL statement to add
            
        Returns:
            True if the batch should be flushed, False otherwise
        """
        # Update adjustment factor if needed
        self.update_adjustment_factor(statement)
        
        # Ensure statement ends with delimiter
        if not statement.strip().endswith(self.delimiter):
            statement = statement.strip() + self.delimiter
        
        # Add statement to batch
        self.current_batch.append(statement)
        
        # Update size
        statement_size = len(statement.encode("utf-8"))
        self.current_size += statement_size
        
        # Get adjusted max_bytes for comparison
        adjusted_max_bytes = self.get_adjusted_max_bytes()
        
        # Check if batch should be flushed
        return self.current_size >= adjusted_max_bytes
    
    def reset(self) -> None:
        """Reset the current batch."""
        self.current_batch = []
        self.current_size = 0
    
    def flush(
        self,
        execute_callback: Callable[[str], Any],
        query_collector: Optional[QueryCollector] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Flush the current batch.
        
        Args:
            execute_callback: Callback for executing SQL (takes SQL string, returns any)
            query_collector: Optional query collector for collecting queries
            metadata: Optional metadata to associate with the batch
            
        Returns:
            Number of statements flushed
        """
        count = len(self.current_batch)
        
        if count == 0:
            return 0
        
        # Join statements
        batch_sql = "\n".join(self.current_batch)
        
        # If in dry run mode, just collect the queries
        if self.dry_run:
            if query_collector:
                query_collector.collect(batch_sql, metadata)
        else:
            # Execute the batch
            execute_callback(batch_sql)
            
            # Optionally collect the query
            if query_collector:
                query_collector.collect(batch_sql, metadata)
        
        # Reset the batch
        self.reset()
        
        return count
    
    def process_statements(
        self,
        statements: List[str],
        execute_callback: Callable[[str], Any],
        query_collector: Optional[QueryCollector] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Process a list of SQL statements.
        
        Args:
            statements: List of SQL statements to process
            execute_callback: Callback for executing SQL (takes SQL string, returns any)
            query_collector: Optional query collector for collecting queries
            metadata: Optional metadata to associate with the batches
            
        Returns:
            Total number of statements processed
        """
        total_processed = 0
        
        # Analyze first few statements to establish column count if enabled
        if self.auto_adjust_for_columns and len(statements) > 0:
            # Try to detect columns from the first 5 statements (or fewer if less available)
            for i in range(min(5, len(statements))):
                self.update_adjustment_factor(statements[i])
                if self.column_count is not None:
                    # We found a valid column count, no need to check more
                    break
                    
            if self.column_count is not None:
                import logging
                logging.info(
                    f"Column-based batch sizing active: {self.column_count} columns detected, "
                    f"adjustment factor: {self.adjustment_factor:.2f}x, "
                    f"effective max_bytes: {self.get_adjusted_max_bytes()} "
                    f"(base: {self.max_bytes})"
                )
        
        for statement in statements:
            # Update adjustment factor if needed (in case not already set)
            if self.auto_adjust_for_columns and self.column_count is None:
                self.update_adjustment_factor(statement)
                
            # Handle oversized statements
            statement_size = len(statement.encode("utf-8"))
            adjusted_max_bytes = self.get_adjusted_max_bytes()
            
            if statement_size > adjusted_max_bytes:
                # Flush the current batch first
                total_processed += self.flush(
                    execute_callback, query_collector, metadata
                )
                
                # Handle the oversized statement individually
                if not statement.strip().endswith(self.delimiter):
                    statement = statement.strip() + self.delimiter
                
                if self.dry_run:
                    if query_collector:
                        query_collector.collect(statement, metadata)
                else:
                    execute_callback(statement)
                    
                    if query_collector:
                        query_collector.collect(statement, metadata)
                
                total_processed += 1
                continue
            
            # Add to batch, flush if needed
            should_flush = self.add_statement(statement)
            if should_flush:
                total_processed += self.flush(
                    execute_callback, query_collector, metadata
                )
        
        # Flush any remaining statements
        total_processed += self.flush(execute_callback, query_collector, metadata)
        
        return total_processed