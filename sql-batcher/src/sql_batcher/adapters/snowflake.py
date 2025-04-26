"""
Snowflake adapter for SQL Batcher.

This module provides a Snowflake-specific adapter for SQLBatcher, optimized
for working with Snowflake's SQL engine.
"""
import logging
from typing import Any, Optional, Dict, List, Union

from sql_batcher.adapters.base import SQLAdapter

logger = logging.getLogger(__name__)

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    logger.warning("Snowflake package not available. Install with 'pip install snowflake-connector-python' to use the SnowflakeAdapter.")


class SnowflakeAdapter(SQLAdapter):
    """
    Adapter for connecting SQLBatcher to Snowflake.
    
    This adapter provides Snowflake-specific integration for SQLBatcher,
    handling connection management and query execution.
    
    Example:
        >>> import snowflake.connector
        >>> from sql_batcher import SQLBatcher
        >>> from sql_batcher.adapters.snowflake import SnowflakeAdapter
        >>> 
        >>> # Create a Snowflake connection
        >>> conn = snowflake.connector.connect(
        ...     user='username',
        ...     password='password',
        ...     account='account_id',
        ...     warehouse='compute_wh',
        ...     database='example_db',
        ...     schema='public'
        ... )
        >>> 
        >>> # Create a Snowflake adapter
        >>> adapter = SnowflakeAdapter(connection=conn)
        >>> 
        >>> # Create a batcher with Snowflake's larger limits
        >>> batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
        >>> 
        >>> # Define some statements
        >>> statements = [
        ...     "INSERT INTO users VALUES (1, 'Alice')",
        ...     "INSERT INTO users VALUES (2, 'Bob')"
        ... ]
        >>> 
        >>> # Process statements using the adapter
        >>> batcher.process_statements(statements, adapter.execute)
    """
    
    def __init__(
        self,
        connection: Optional[Any] = None,
        connection_params: Optional[Dict[str, Any]] = None,
        max_query_size: int = 10_000_000,  # Snowflake has large query limits
        auto_close: bool = False
    ):
        """
        Initialize a Snowflake adapter.
        
        Args:
            connection: An existing Snowflake connection (optional)
            connection_params: Parameters to create a new connection (if connection not provided)
            max_query_size: Maximum query size in bytes (default: 10MB)
            auto_close: Whether to close the connection when the adapter is closed
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError("The snowflake-connector-python package is required to use SnowflakeAdapter.")
        
        self._max_query_size = max_query_size
        self.auto_close = auto_close
        
        # Use existing connection or create a new one
        if connection is not None:
            self.connection = connection
            self.created_connection = False
        elif connection_params is not None:
            self.connection = snowflake.connector.connect(**connection_params)
            self.created_connection = True
        else:
            raise ValueError("Either connection or connection_params must be provided")
        
        self._cursor = None
        logger.debug(f"Initialized SnowflakeAdapter with max_query_size={max_query_size}")
    
    def _get_cursor(self) -> Any:
        """Get a cursor, creating it if necessary."""
        if self._cursor is None:
            self._cursor = self.connection.cursor()
        return self._cursor
    
    def execute(self, sql: str) -> Any:
        """
        Execute a SQL statement on Snowflake.
        
        Args:
            sql: The SQL statement to execute
            
        Returns:
            Result of the SQL execution
        """
        cursor = self._get_cursor()
        
        try:
            logger.debug(f"Executing Snowflake SQL: {sql}")
            cursor.execute(sql)
            
            # For queries that return results
            if cursor.description:
                return cursor.fetchall()
            
            # For DDL/DML queries
            return None
        except Exception as e:
            logger.error(f"Error executing Snowflake SQL: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to execute Snowflake SQL: {str(e)}") from e
    
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes for Snowflake.
        
        Returns:
            Maximum query size in bytes
        """
        return self._max_query_size
    
    def close(self) -> None:
        """Close the Snowflake cursor and optionally the connection."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
            
        if self.auto_close and self.created_connection and self.connection:
            self.connection.close()
            logger.debug("Closed Snowflake connection")
    
    def begin_transaction(self) -> None:
        """Begin a Snowflake transaction."""
        self.execute("BEGIN")
    
    def commit_transaction(self) -> None:
        """Commit the current Snowflake transaction."""
        self.execute("COMMIT")
    
    def rollback_transaction(self) -> None:
        """Rollback the current Snowflake transaction."""
        self.execute("ROLLBACK")
    
    def __del__(self):
        """Ensure connections are closed when the adapter is garbage collected."""
        self.close()