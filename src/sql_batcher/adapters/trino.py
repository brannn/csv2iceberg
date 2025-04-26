"""
Trino adapter for SQL Batcher.

This module provides an adapter for Trino (formerly PrestoSQL) databases,
handling the specific requirements and limitations of Trino.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from sql_batcher.adapters.base import SQLAdapter

logger = logging.getLogger(__name__)


class TrinoAdapter(SQLAdapter):
    """
    Adapter for Trino database connections.
    
    This adapter provides optimized support for Trino databases, including
    configuration for authentication, catalog/schema selection, and size limits.
    
    Attributes:
        host: Trino host
        port: Trino port
        user: Username for authentication
        catalog: Catalog name
        schema: Schema name
        max_query_size: Maximum query size in bytes
    
    Examples:
        >>> from sql_batcher import SQLBatcher
        >>> from sql_batcher.adapters.trino import TrinoAdapter
        >>> 
        >>> adapter = TrinoAdapter(
        ...     host="trino.example.com",
        ...     port=443,
        ...     user="admin",
        ...     catalog="hive",
        ...     schema="default"
        ... )
        >>> 
        >>> batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
        >>> 
        >>> # Process statements
        >>> batcher.process_statements(statements, adapter.execute)
        >>> 
        >>> # Clean up
        >>> adapter.close()
    """
    
    # Default maximum query size for Trino (16MB)
    DEFAULT_MAX_QUERY_SIZE = 16 * 1024 * 1024
    
    def __init__(
        self,
        host: str,
        port: int = 443,
        user: str = "admin",
        password: Optional[str] = None,
        catalog: str = "hive",
        schema: str = "default",
        http_scheme: str = "https",
        role: Optional[str] = None,
        auth: Optional[Any] = None,
        max_query_size: int = DEFAULT_MAX_QUERY_SIZE,
        headers: Optional[Dict[str, str]] = None,
        verify: bool = True,
        dry_run: bool = False,
    ) -> None:
        """
        Initialize a new TrinoAdapter instance.
        
        Args:
            host: Trino host name
            port: Trino port number (default: 443)
            user: Username for authentication (default: "admin")
            password: Password for authentication (default: None)
            catalog: Catalog name (default: "hive")
            schema: Schema name (default: "default")
            http_scheme: HTTP scheme - "http" or "https" (default: "https")
            role: Trino role (default: None)
            auth: Authentication object (default: None)
            max_query_size: Maximum query size in bytes (default: 16MB)
            headers: Additional HTTP headers (default: None)
            verify: Whether to verify SSL certificates (default: True)
            dry_run: Whether to simulate without actual connections (default: False)
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.catalog = catalog
        self.schema = schema
        self.http_scheme = http_scheme
        self.role = role
        self.auth = auth
        self.max_query_size = max_query_size
        self.headers = headers
        self.verify = verify
        self.dry_run = dry_run
        self.connection = None
        self.cursor = None
        
        if not dry_run:
            self._connect()
        
        logger.debug(
            f"Initialized TrinoAdapter for {user}@{host}:{port}/{catalog}/{schema} "
            f"with max_query_size={max_query_size}"
        )
    
    def _connect(self) -> None:
        """
        Establish a connection to Trino.
        
        This method creates a connection to the Trino server using the
        provided connection parameters.
        
        Raises:
            ImportError: If the trino module is not installed
            ConnectionError: If the connection to Trino fails
        """
        try:
            import trino
            
            # Determine auth to use
            auth_obj = self.auth
            if self.password and not auth_obj:
                auth_obj = trino.auth.BasicAuthentication(self.user, self.password)
            
            # Create connection
            self.connection = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema,
                http_scheme=self.http_scheme,
                auth=auth_obj,
                role=self.role,
                http_headers=self.headers,
                verify=self.verify,
            )
            
            logger.info(f"Connected to Trino at {self.host}:{self.port}")
            
        except ImportError:
            logger.error(
                "Trino module not found. Install it with `pip install trino`."
            )
            raise ImportError(
                "Trino adapter requires the trino module. "
                "Install it with `pip install trino`."
            )
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {e}")
            raise ConnectionError(f"Failed to connect to Trino: {e}")
    
    def execute(self, sql: str) -> List[Tuple]:
        """
        Execute a SQL statement on Trino.
        
        This method executes the SQL statement using the Trino connection
        and returns any results.
        
        Args:
            sql: SQL statement to execute
            
        Returns:
            List of result rows as tuples
            
        Raises:
            ConnectionError: If the connection to Trino is not established
            Exception: Any Trino-specific exception that occurs during execution
        """
        if self.dry_run:
            logger.debug(f"Dry run: would execute SQL on Trino: {sql[:100]}...")
            return []
        
        if not self.connection:
            logger.warning("No active connection to Trino. Reconnecting...")
            self._connect()
        
        # Create a cursor if needed
        if not self.cursor:
            self.cursor = self.connection.cursor()
        
        logger.debug(f"Executing SQL on Trino: {sql[:100]}...")
        
        try:
            self.cursor.execute(sql)
            
            # For SELECT queries, return results
            if self.cursor.description:
                results = self.cursor.fetchall()
                logger.debug(f"Query returned {len(results)} rows")
                return results
            
            # For non-SELECT queries, return empty list
            return []
            
        except Exception as e:
            logger.error(f"Failed to execute SQL on Trino: {e}")
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
        Close the Trino connection.
        
        This method closes the cursor and connection to release resources.
        """
        if self.dry_run:
            logger.debug("Dry run: would close Trino connection")
            return
        
        if self.cursor:
            logger.debug("Closing Trino cursor")
            self.cursor.close()
            self.cursor = None
        
        if self.connection:
            logger.debug("Closing Trino connection")
            self.connection.close()
            self.connection = None
    
    def begin_transaction(self) -> None:
        """
        Begin a Trino transaction.
        
        Note: Trino has limited transaction support, and this functionality
        may not be available in all Trino versions.
        
        Raises:
            NotImplementedError: If transactions are not supported by the Trino version
        """
        if self.dry_run:
            logger.debug("Dry run: would begin Trino transaction")
            return
        
        logger.debug("Beginning Trino transaction")
        
        try:
            self.execute("START TRANSACTION")
        except Exception as e:
            logger.error(f"Failed to begin transaction: {e}")
            raise NotImplementedError(
                f"Trino transactions not supported or failed: {e}"
            )
    
    def commit_transaction(self) -> None:
        """
        Commit the current Trino transaction.
        
        Raises:
            NotImplementedError: If transactions are not supported by the Trino version
        """
        if self.dry_run:
            logger.debug("Dry run: would commit Trino transaction")
            return
        
        logger.debug("Committing Trino transaction")
        
        try:
            self.execute("COMMIT")
        except Exception as e:
            logger.error(f"Failed to commit transaction: {e}")
            raise NotImplementedError(
                f"Trino transactions not supported or failed: {e}"
            )
    
    def rollback_transaction(self) -> None:
        """
        Rollback the current Trino transaction.
        
        Raises:
            NotImplementedError: If transactions are not supported by the Trino version
        """
        if self.dry_run:
            logger.debug("Dry run: would rollback Trino transaction")
            return
        
        logger.debug("Rolling back Trino transaction")
        
        try:
            self.execute("ROLLBACK")
        except Exception as e:
            logger.error(f"Failed to rollback transaction: {e}")
            raise NotImplementedError(
                f"Trino transactions not supported or failed: {e}"
            )