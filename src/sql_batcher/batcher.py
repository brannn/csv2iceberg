"""
Core SQL Batcher implementation.

This module contains the SQLBatcher class, which is the main entry point for
batching SQL statements based on size limits.
"""
from typing import Any, Callable, Dict, List, Optional, Union

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
    """
    
    def __init__(
        self,
        max_bytes: int = 1_000_000,
        delimiter: str = ";",
        dry_run: bool = False
    ):
        """
        Initialize SQL Batcher.
        
        Args:
            max_bytes: Maximum batch size in bytes
            delimiter: SQL statement delimiter
            dry_run: Whether to operate in dry run mode (without executing)
        """
        self.max_bytes = max_bytes
        self.delimiter = delimiter
        self.dry_run = dry_run
        self.current_batch: List[str] = []
        self.current_size: int = 0
    
    def add_statement(self, statement: str) -> bool:
        """
        Add a statement to the current batch.
        
        Args:
            statement: SQL statement to add
            
        Returns:
            True if the batch should be flushed, False otherwise
        """
        # Ensure statement ends with delimiter
        if not statement.strip().endswith(self.delimiter):
            statement = statement.strip() + self.delimiter
        
        # Add statement to batch
        self.current_batch.append(statement)
        
        # Update size
        statement_size = len(statement.encode("utf-8"))
        self.current_size += statement_size
        
        # Check if batch should be flushed
        return self.current_size >= self.max_bytes
    
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
        
        for statement in statements:
            # Handle oversized statements
            statement_size = len(statement.encode("utf-8"))
            if statement_size > self.max_bytes:
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