"""
Generic adapter for SQL Batcher.

This module provides a generic adapter for SQLBatcher that works with
any database that follows the Python DB-API 2.0 specification.
"""
import logging
from typing import Any, Optional, Dict, List, Callable

from sql_batcher.adapters.base import SQLAdapter

logger = logging.getLogger(__name__)


class GenericAdapter(SQLAdapter):
    """
    Generic adapter for connecting SQLBatcher to any DB-API compatible database.
    
    This adapter works with any database that follows the Python DB-API 2.0 
    specification, such as SQLite, PostgreSQL, MySQL, etc.
    
    Example:
        >>> import sqlite3
        >>> from sql_batcher import SQLBatcher
        >>> from sql_batcher.adapters.generic import GenericAdapter
        >>> 
        >>> # Create a SQLite connection
        >>> conn = sqlite3.connect(":memory:")
        >>> 
        >>> # Create a generic adapter
        >>> adapter = GenericAdapter(
        ...     connection=conn,
        ...     max_query_size=100_000
        ... )
        >>> 
        >>> # Create a batcher
        >>> batcher = SQLBatcher()
        >>> 
        >>> # Define some statements
        >>> statements = [
        ...     "CREATE TABLE users (id INTEGER, name TEXT)",
        ...     "INSERT INTO users VALUES (1, 'Alice')",
        ...     "INSERT INTO users VALUES (2, 'Bob')"
        ... ]
        >>> 
        >>> # Process statements using the adapter
        >>> batcher.process_statements(statements, adapter.execute)
    """
    
    def __init__(
        self,
        connection: Any,
        create_cursor_fn: Optional[Callable] = None,
        max_query_size: int = 500_000,
        auto_commit: bool = True
    ):
        """
        Initialize a generic DB-API adapter.
        
        Args:
            connection: A DB-API compatible connection object
            create_cursor_fn: Optional function to create a cursor (defaults to connection.cursor())
            max_query_size: Maximum query size in bytes
            auto_commit: Whether to auto-commit after each statement
        """
        self.connection = connection
        self.create_cursor_fn = create_cursor_fn or (lambda conn: conn.cursor())
        self._max_query_size = max_query_size
        self.auto_commit = auto_commit
        self._cursor = None
        
        logger.debug(f"Initialized GenericAdapter with max_query_size={max_query_size}")
    
    def _get_cursor(self) -> Any:
        """Get a cursor, creating it if necessary."""
        if self._cursor is None:
            self._cursor = self.create_cursor_fn(self.connection)
        return self._cursor
    
    def execute(self, sql: str) -> Any:
        """
        Execute a SQL statement using the DB-API connection.
        
        Args:
            sql: The SQL statement to execute
            
        Returns:
            Result of the SQL execution
        """
        cursor = self._get_cursor()
        
        try:
            logger.debug(f"Executing SQL: {sql}")
            cursor.execute(sql)
            
            # Auto-commit if enabled
            if self.auto_commit and hasattr(self.connection, 'commit'):
                self.connection.commit()
            
            # For queries that return results
            if cursor.description:
                return cursor.fetchall()
            
            # For DDL/DML queries
            return None
        except Exception as e:
            logger.error(f"Error executing SQL: {str(e)}", exc_info=True)
            
            # Attempt to rollback if auto_commit is enabled
            if self.auto_commit and hasattr(self.connection, 'rollback'):
                try:
                    self.connection.rollback()
                except Exception:
                    pass  # Ignore rollback errors
                
            raise RuntimeError(f"Failed to execute SQL: {str(e)}") from e
    
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.
        
        Returns:
            Maximum query size in bytes
        """
        return self._max_query_size
    
    def close(self) -> None:
        """Close the cursor."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
            
        logger.debug("Closed DB cursor")
    
    def begin_transaction(self) -> None:
        """Begin a database transaction."""
        if hasattr(self.connection, 'begin'):
            self.connection.begin()
        elif hasattr(self.connection, 'execute'):
            self.connection.execute("BEGIN TRANSACTION")
    
    def commit_transaction(self) -> None:
        """Commit the current database transaction."""
        if hasattr(self.connection, 'commit'):
            self.connection.commit()
    
    def rollback_transaction(self) -> None:
        """Rollback the current database transaction."""
        if hasattr(self.connection, 'rollback'):
            self.connection.rollback()