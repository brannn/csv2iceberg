"""
Trino adapter for SQL Batcher.

This module provides a Trino-specific adapter for SQLBatcher, optimized
for working with Trino's SQL engine.
"""
import logging
from typing import Any, Optional, Dict, List, Union

from sql_batcher.adapters.base import SQLAdapter

logger = logging.getLogger(__name__)

try:
    import trino
    TRINO_AVAILABLE = True
except ImportError:
    TRINO_AVAILABLE = False
    logger.warning("Trino package not available. Install with 'pip install trino' to use the TrinoAdapter.")


class TrinoAdapter(SQLAdapter):
    """
    Adapter for connecting SQLBatcher to Trino.
    
    This adapter provides Trino-specific integration for SQLBatcher,
    handling connection management and query execution.
    
    Example:
        >>> from sql_batcher import SQLBatcher
        >>> from sql_batcher.adapters.trino import TrinoAdapter
        >>> 
        >>> # Create a Trino adapter
        >>> adapter = TrinoAdapter(
        ...     host="localhost",
        ...     port=8080,
        ...     user="admin",
        ...     catalog="hive",
        ...     schema="default"
        ... )
        >>> 
        >>> # Create a batcher with Trino's recommended limits
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
        host: str,
        port: int = 8080,
        user: str = "admin",
        password: Optional[str] = None,
        catalog: str = "hive",
        schema: str = "default",
        http_scheme: str = "https",
        auth: Optional[Any] = None,
        max_query_size: int = 1_000_000,  # Default 1MB for Trino
        verify: Union[bool, str] = True
    ):
        """
        Initialize a Trino adapter.
        
        Args:
            host: Trino server hostname or IP
            port: Trino server port
            user: Username for authentication
            password: Password for authentication (if required)
            catalog: Default catalog to use
            schema: Default schema to use
            http_scheme: HTTP scheme (http or https)
            auth: Authentication method (if not using password)
            max_query_size: Maximum query size in bytes (default: 1MB)
            verify: Whether to verify SSL certificates (True, False, or path to CA bundle)
        """
        if not TRINO_AVAILABLE:
            raise ImportError("The trino package is required to use TrinoAdapter. Install with 'pip install trino'.")
        
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.catalog = catalog
        self.schema = schema
        self.http_scheme = http_scheme
        self.auth = auth
        self._max_query_size = max_query_size
        self.verify = verify
        
        self._connection = None
        self._cursor = None
        self._connect()
        
        logger.debug(f"Initialized TrinoAdapter for {http_scheme}://{host}:{port}")
    
    def _connect(self) -> None:
        """Establish a connection to Trino."""
        try:
            conn_params = {
                "host": self.host,
                "port": self.port,
                "user": self.user,
                "catalog": self.catalog,
                "schema": self.schema,
                "http_scheme": self.http_scheme,
                "verify": self.verify
            }
            
            if self.password:
                conn_params["auth"] = trino.auth.BasicAuthentication(self.user, self.password)
            elif self.auth:
                conn_params["auth"] = self.auth
                
            self._connection = trino.dbapi.connect(**conn_params)
            self._cursor = self._connection.cursor()
            logger.info(f"Connected to Trino server at {self.http_scheme}://{self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Error connecting to Trino: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to connect to Trino: {str(e)}") from e
    
    def execute(self, sql: str) -> Any:
        """
        Execute a SQL statement on Trino.
        
        Args:
            sql: The SQL statement to execute
            
        Returns:
            Result of the SQL execution
        """
        if not self._cursor:
            self._connect()
            
        try:
            logger.debug(f"Executing Trino SQL: {sql}")
            self._cursor.execute(sql)
            
            # For queries that return results
            if self._cursor.description:
                return self._cursor.fetchall()
            
            # For DDL/DML queries
            return None
        except Exception as e:
            logger.error(f"Error executing Trino SQL: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to execute Trino SQL: {str(e)}") from e
    
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes for Trino.
        
        Returns:
            Maximum query size in bytes
        """
        return self._max_query_size
    
    def close(self) -> None:
        """Close the Trino connection."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
            
        if self._connection:
            self._connection.close()
            self._connection = None
            
        logger.debug("Closed Trino connection")
    
    def __del__(self):
        """Ensure connections are closed when the adapter is garbage collected."""
        self.close()