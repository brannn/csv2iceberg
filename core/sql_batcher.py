"""
SQL batching module for efficient execution of multiple SQL statements.

This module provides a reusable batching mechanism for SQL statements
to optimize execution while respecting query size limits.
"""
import logging
from typing import Callable, List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class SQLBatcher:
    """
    Batches SQL statements for efficient execution based on size limits.
    
    This class handles the complexities of batching SQL statements to prevent
    exceeding query size limits while optimizing for performance.
    """
    
    def __init__(self, max_bytes: int = 900_000, delimiter: str = ";\n", dry_run: bool = False):
        """
        Initialize a new SQL batcher.
        
        Args:
            max_bytes: Maximum size in bytes of the query string batch (default: 900,000)
            delimiter: Delimiter to use between SQL statements (default: ";\n")
            dry_run: If True, just logs the queries without executing (default: False)
        """
        self.max_bytes = max_bytes
        self.delimiter = delimiter
        self.dry_run = dry_run
        self.reset()
        logger.debug(f"Initialized SQLBatcher with max_bytes={max_bytes}, "
                    f"delimiter='{delimiter}', dry_run={dry_run}")
    
    def reset(self) -> None:
        """Reset the current batch."""
        self.current_batch = []
        self.current_size = 0
        self.total_statements_processed = 0
    
    def add_statement(self, sql: str) -> bool:
        """
        Add a SQL statement to the current batch.
        
        Args:
            sql: The SQL statement to add to the batch
            
        Returns:
            True if the batch is full after adding this statement, False otherwise
        """
        # Calculate the size in bytes
        encoded = sql.encode("utf-8")
        # Add delimiter size if this isn't the first statement
        delimiter_size = len(self.delimiter.encode("utf-8")) if self.current_batch else 0
        size = len(encoded) + delimiter_size
        
        # If this statement alone exceeds the max size, log a warning
        if size > self.max_bytes:
            logger.warning(f"Single SQL statement exceeds max size: {size} bytes > {self.max_bytes} bytes")
            
        # Check if adding this statement would exceed the batch size
        if self.current_size + size > self.max_bytes and self.current_batch:
            return True  # Caller should flush before adding
        
        # Add the statement to the batch
        self.current_batch.append(sql)
        self.current_size += size
        return False
    
    def flush(self, execute_callback: Callable[[str], Any], 
             query_collector: Optional[Any] = None,
             metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Flush the current batch using the provided callback.
        
        Args:
            execute_callback: Function to call with the batched SQL
            query_collector: Optional query collector for dry run mode
            metadata: Additional metadata for the query collector
        """
        if not self.current_batch:
            return
        
        query = self.delimiter.join(self.current_batch)
        statements_count = len(self.current_batch)
        self.total_statements_processed += statements_count
        
        logger.debug(f"Flushing SQL batch ({self.current_size} bytes, {statements_count} statements)")
        
        if self.dry_run:
            logger.info(f"[DRY RUN] SQL batch with {statements_count} statements ({self.current_size} bytes)")
            logger.debug(f"[DRY RUN] SQL batch content:\n{query}")
            
            # If we have a query collector, add the query to it
            if query_collector and metadata:
                query_collector.add_query(
                    query,
                    metadata.get("type", "DML"),
                    metadata.get("row_count", statements_count),
                    metadata.get("table_name", "unknown")
                )
        else:
            # Execute the query
            try:
                execute_callback(query)
                logger.info(f"Successfully executed SQL batch with {statements_count} statements")
            except Exception as e:
                logger.error(f"Error executing SQL batch: {str(e)}", exc_info=True)
                raise RuntimeError(f"Failed to execute SQL batch: {str(e)}") from e
        
        self.reset()
    
    def process_statements(self, sql_statements: List[str], 
                          execute_callback: Callable[[str], Any],
                          query_collector: Optional[Any] = None,
                          metadata: Optional[Dict[str, Any]] = None) -> int:
        """
        Batch-process a sequence of SQL statements.
        
        Args:
            sql_statements: List of SQL statements to process
            execute_callback: Function to call with each batch
            query_collector: Optional query collector for dry run mode
            metadata: Additional metadata for the query collector
            
        Returns:
            Total number of statements processed
        """
        for sql in sql_statements:
            if self.add_statement(sql):
                self.flush(execute_callback, query_collector, metadata)
                self.add_statement(sql)  # Add the current statement to the new batch
                
        # Flush any remaining statements
        self.flush(execute_callback, query_collector, metadata)
        
        return self.total_statements_processed