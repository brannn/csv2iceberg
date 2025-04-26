"""
SQL Batcher core module for efficiently batching SQL statements by size.

This module provides the core SQLBatcher class that handles batching SQL statements
based on size limits rather than simple counts, which is more appropriate for
database systems with query size limitations.
"""
import logging
from typing import Any, Callable, Dict, List, Optional, Union


logger = logging.getLogger(__name__)


class SQLBatcher:
    """
    Core class for batching SQL statements based on size limits.
    
    SQLBatcher efficiently processes multiple SQL statements while respecting
    size limits. It's particularly useful for database systems like Trino, Spark,
    and Snowflake that have limits on query string size.
    
    Attributes:
        max_bytes (int): Maximum batch size in bytes
        delimiter (str): SQL statement delimiter (typically ";")
        dry_run (bool): If True, don't actually execute statements
        current_batch (list): Current batch of SQL statements
        current_size (int): Current size of the batch in bytes
    
    Examples:
        Basic usage:
        
        >>> from sql_batcher import SQLBatcher
        >>> batcher = SQLBatcher(max_bytes=1000000)
        >>> statements = [
        ...     "INSERT INTO users VALUES (1, 'Alice')",
        ...     "INSERT INTO users VALUES (2, 'Bob')"
        ... ]
        >>> def execute_sql(sql):
        ...     print(f"Executing: {sql}")
        ...     # In a real scenario, this would execute the SQL
        >>> batcher.process_statements(statements, execute_sql)
    """

    def __init__(
        self,
        max_bytes: int = 1_000_000,
        delimiter: str = ";",
        dry_run: bool = False,
        size_func: Optional[Callable[[str], int]] = None,
    ) -> None:
        """
        Initialize a new SQLBatcher instance.
        
        Args:
            max_bytes: Maximum batch size in bytes (default: 1,000,000)
            delimiter: SQL statement delimiter (default: ";")
            dry_run: If True, don't actually execute statements (default: False)
            size_func: Custom function to calculate statement size (default: None)
                       If provided, should take a string and return an integer size
        """
        self.max_bytes = max_bytes
        self.delimiter = delimiter
        self.dry_run = dry_run
        self.current_batch: List[str] = []
        self.current_size = 0
        self._size_func = size_func or (lambda s: len(s.encode("utf-8")))
        
        logger.debug(
            f"Initialized SQLBatcher with max_bytes={max_bytes}, "
            f"delimiter='{delimiter}', dry_run={dry_run}"
        )

    def add_statement(self, statement: str) -> bool:
        """
        Add a SQL statement to the current batch.
        
        This method adds a SQL statement to the current batch if there's enough space.
        If the statement would exceed the batch size limit, it returns True to indicate
        that the batch needs to be flushed before adding the statement.
        
        If a single statement exceeds the maximum batch size, it will be added to an
        empty batch and a warning will be logged.
        
        Args:
            statement: SQL statement to add
            
        Returns:
            True if the batch is full and should be flushed, False otherwise
        """
        # Calculate statement size
        statement_size = self._size_func(statement)
        
        # If this statement alone exceeds max_bytes, log a warning but proceed
        if statement_size > self.max_bytes:
            logger.warning(
                f"Statement size ({statement_size} bytes) exceeds max_bytes ({self.max_bytes}). "
                f"This statement will be executed individually."
            )
            
            # If the batch is empty, add this oversized statement
            if not self.current_batch:
                self.current_batch.append(statement)
                self.current_size = statement_size
                return False
            else:
                # Batch is not empty, so indicate it should be flushed first
                return True
                
        # Check if adding this statement would exceed max_bytes
        if self.current_size + statement_size <= self.max_bytes:
            # Add statement to the current batch
            self.current_batch.append(statement)
            self.current_size += statement_size
            return False
        else:
            # Batch would be too large, indicate it should be flushed
            return True

    def reset(self) -> None:
        """
        Reset the current batch.
        
        This method clears the current batch and resets the batch size to zero.
        """
        self.current_batch = []
        self.current_size = 0

    def flush(
        self,
        callback: Callable[[str], Any],
        query_collector: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Execute all statements in the current batch.
        
        This method:
        1. Joins all statements with the delimiter
        2. Executes the resulting SQL using the provided callback function
        3. Resets the batch
        
        In dry run mode, it uses the query_collector instead of executing.
        
        Args:
            callback: Function to execute the SQL (takes a string parameter)
            query_collector: Object to collect queries in dry run mode (optional)
            metadata: Additional metadata to associate with the query (optional)
            
        Returns:
            Number of statements executed
        """
        if not self.current_batch:
            return 0
            
        count = len(self.current_batch)
        
        # Join statements with delimiter
        joined_sql = self.delimiter.join(self.current_batch)
        if not joined_sql.endswith(self.delimiter):
            joined_sql += self.delimiter
            
        logger.debug(f"Flushing batch with {count} statements ({self.current_size} bytes)")
        
        # Execute or collect based on mode
        if self.dry_run:
            if query_collector:
                logger.debug(f"Dry run: Collecting query ({self.current_size} bytes)")
                query_collector.add_query(joined_sql, metadata)
            else:
                logger.debug(f"Dry run: Would execute ({self.current_size} bytes)")
        else:
            logger.debug(f"Executing batch: {count} statements, {self.current_size} bytes")
            callback(joined_sql)
            
        # Reset the batch
        self.reset()
        
        return count

    def process_statements(
        self,
        statements: List[str],
        callback: Callable[[str], Any],
        query_collector: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Process multiple SQL statements, batching them as needed.
        
        This method iterates through the provided statements, adding each to the
        current batch. When the batch reaches the size limit, it is automatically
        flushed and execution continues.
        
        Args:
            statements: List of SQL statements to execute
            callback: Function to execute the SQL (takes a string parameter)
            query_collector: Object to collect queries in dry run mode (optional)
            metadata: Additional metadata to associate with the query (optional)
            
        Returns:
            Total number of statements processed
        """
        total_processed = 0
        
        logger.info(f"Processing {len(statements)} SQL statements")
        
        for statement in statements:
            # Check if adding this statement would make the batch too large
            if self.add_statement(statement):
                # Flush the current batch
                total_processed += self.flush(callback, query_collector, metadata)
                
                # Add the statement to the now-empty batch
                self.add_statement(statement)
                
        # Flush any remaining statements
        total_processed += self.flush(callback, query_collector, metadata)
        
        logger.info(f"Processed {total_processed} SQL statements")
        
        return total_processed