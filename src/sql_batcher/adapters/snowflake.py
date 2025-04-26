"""
Snowflake adapter for SQL Batcher.

This module provides an adapter for Snowflake databases,
handling the specific requirements and limitations of Snowflake.
"""
import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from sql_batcher.adapters.base import SQLAdapter

logger = logging.getLogger(__name__)


class SnowflakeAdapter(SQLAdapter):
    """
    Adapter for Snowflake database connections.
    
    This adapter provides optimized support for Snowflake databases,
    including transaction support and timeout handling.
    
    Attributes:
        connection_params: Connection parameters for Snowflake
        max_query_size: Maximum query size in bytes
        auto_close: Whether to automatically close the connection after each execution
        connection_timeout: Connection timeout in seconds
        fetch_limit: Maximum number of rows to fetch (optional)
    
    Examples:
        >>> from sql_batcher import SQLBatcher
        >>> from sql_batcher.adapters.snowflake import SnowflakeAdapter
        >>> 
        >>> connection_params = {
        ...     "account": "your_account",
        ...     "user": "your_username",
        ...     "password": "your_password",
        ...     "database": "your_database",
        ...     "schema": "your_schema",
        ...     "warehouse": "your_warehouse"
        ... }
        >>> 
        >>> adapter = SnowflakeAdapter(connection_params=connection_params)
        >>> batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
        >>> 
        >>> # Process statements
        >>> batcher.process_statements(statements, adapter.execute)
        >>> 
        >>> # Clean up
        >>> adapter.close()
    """
    
    # Default maximum query size for Snowflake (1MB)
    DEFAULT_MAX_QUERY_SIZE = 1 * 1024 * 1024
    
    def __init__(
        self,
        connection_params: Dict[str, Any],
        max_query_size: int = DEFAULT_MAX_QUERY_SIZE,
        auto_close: bool = False,
        connection_timeout: int = 60,
        fetch_limit: Optional[int] = None,
    ) -> None:
        """
        Initialize a new SnowflakeAdapter instance.
        
        Args:
            connection_params: Connection parameters for Snowflake
            max_query_size: Maximum query size in bytes (default: 1MB)
            auto_close: Whether to automatically close connection after execution (default: False)
            connection_timeout: Connection timeout in seconds (default: 60)
            fetch_limit: Maximum number of rows to fetch (default: None)
        """
        self.connection_params = connection_params
        self.max_query_size = max_query_size
        self.auto_close = auto_close
        self.connection_timeout = connection_timeout
        self.fetch_limit = fetch_limit
        self.connection = None
        self.cursor = None
        
        logger.debug(
            f"Initialized SnowflakeAdapter with "
            f"max_query_size={max_query_size}, "
            f"auto_close={auto_close}, "
            f"connection_timeout={connection_timeout}, "
            f"fetch_limit={fetch_limit}"
        )
    
    def _connect(self) -> None:
        """
        Establish a connection to Snowflake.
        
        This method creates a connection to the Snowflake database using the
        provided connection parameters.
        
        Raises:
            ImportError: If the snowflake-connector-python module is not installed
            ConnectionError: If the connection to Snowflake fails
        """
        try:
            import snowflake.connector
            
            logger.debug("Connecting to Snowflake...")
            
            # Set timeout parameter if not already in connection params
            conn_params = self.connection_params.copy()
            if "timeout" not in conn_params:
                conn_params["timeout"] = self.connection_timeout
            
            # Create connection
            start_time = time.time()
            self.connection = snowflake.connector.connect(**conn_params)
            elapsed = time.time() - start_time
            
            logger.info(
                f"Connected to Snowflake in {elapsed:.2f}s "
                f"(account={conn_params.get('account')})"
            )
            
        except ImportError:
            logger.error(
                "Snowflake connector not found. "
                "Install it with `pip install snowflake-connector-python`."
            )
            raise ImportError(
                "Snowflake adapter requires the snowflake-connector-python module. "
                "Install it with `pip install snowflake-connector-python`."
            )
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise ConnectionError(f"Failed to connect to Snowflake: {e}")
    
    def execute(self, sql: str) -> List[Tuple]:
        """
        Execute a SQL statement on Snowflake.
        
        This method executes the SQL statement using the Snowflake connection
        and returns any results.
        
        Args:
            sql: SQL statement to execute
            
        Returns:
            List of result rows as tuples
            
        Raises:
            ConnectionError: If the connection to Snowflake is not established
            Exception: Any Snowflake-specific exception that occurs during execution
        """
        if not self.connection:
            logger.debug("No active connection to Snowflake. Connecting...")
            self._connect()
        
        # Create cursor if needed
        if not self.cursor:
            self.cursor = self.connection.cursor()
        
        logger.debug(f"Executing SQL on Snowflake: {sql[:100]}...")
        
        try:
            # Execute SQL
            start_time = time.time()
            self.cursor.execute(sql)
            elapsed = time.time() - start_time
            
            logger.debug(f"SQL executed in {elapsed:.2f}s")
            
            # For SELECT queries, return results
            if self.cursor.description:
                if self.fetch_limit:
                    logger.debug(f"Fetching up to {self.fetch_limit} rows")
                    results = self.cursor.fetchmany(self.fetch_limit)
                else:
                    logger.debug("Fetching all rows")
                    results = self.cursor.fetchall()
                
                logger.debug(f"Query returned {len(results)} rows")
                
                # Auto-close if configured
                if self.auto_close:
                    self.close()
                
                return results
            
            # For non-SELECT queries, return empty list
            if self.auto_close:
                self.close()
            
            return []
            
        except Exception as e:
            logger.error(f"Failed to execute SQL on Snowflake: {e}")
            # Auto-close on error if configured
            if self.auto_close:
                self.close()
            raise
    
    def get_max_query_size(self) -> int:
        """
        Return the maximum query size in bytes.
        
        Returns:
            Maximum query size in bytes
        """
        return self.max_query_size
    
    def close(self) -> None:
        """
        Close the Snowflake connection.
        
        This method closes the cursor and connection to release resources.
        """
        if self.cursor:
            logger.debug("Closing Snowflake cursor")
            try:
                self.cursor.close()
            except Exception as e:
                logger.warning(f"Error closing Snowflake cursor: {e}")
            self.cursor = None
        
        if self.connection:
            logger.debug("Closing Snowflake connection")
            try:
                self.connection.close()
            except Exception as e:
                logger.warning(f"Error closing Snowflake connection: {e}")
            self.connection = None
    
    def begin_transaction(self) -> None:
        """
        Begin a Snowflake transaction.
        
        This method starts a new transaction in Snowflake.
        """
        logger.debug("Beginning Snowflake transaction")
        
        if not self.connection:
            self._connect()
        
        self.execute("BEGIN")
    
    def commit_transaction(self) -> None:
        """
        Commit the current Snowflake transaction.
        
        This method commits the current transaction in Snowflake.
        
        Raises:
            ConnectionError: If no active connection exists
        """
        logger.debug("Committing Snowflake transaction")
        
        if not self.connection:
            raise ConnectionError("No active connection to commit transaction")
        
        self.execute("COMMIT")
    
    def rollback_transaction(self) -> None:
        """
        Rollback the current Snowflake transaction.
        
        This method rolls back the current transaction in Snowflake.
        
        Raises:
            ConnectionError: If no active connection exists
        """
        logger.debug("Rolling back Snowflake transaction")
        
        if not self.connection:
            raise ConnectionError("No active connection to rollback transaction")
        
        self.execute("ROLLBACK")