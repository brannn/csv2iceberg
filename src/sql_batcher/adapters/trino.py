"""
Trino adapter for SQL Batcher.

This module provides a Trino-specific adapter for SQL Batcher, optimized for
Trino's query limitations and capabilities.
"""
from typing import Any, Dict, List, Optional, Tuple, Union

from sql_batcher.adapters.base import SQLAdapter

try:
    import trino.dbapi
    TRINO_AVAILABLE = True
except ImportError:
    TRINO_AVAILABLE = False


class TrinoAdapter(SQLAdapter):
    """
    Adapter for Trino database connections.
    
    This adapter is optimized for Trino's specific limitations, including
    its ~1MB query size limit.
    
    Note: Trino does not support executing multiple statements in a single query.
    This adapter executes each statement individually, while SQLBatcher optimizes 
    by keeping the connection open and reusing it for sequential statement execution.
    
    Args:
        host: Trino server hostname
        port: Trino server port
        user: Username for authentication
        catalog: Trino catalog to use
        schema: Schema name within the catalog
        use_ssl: Whether to use SSL for connection
        verify_ssl: Whether to verify SSL certificate
        session_properties: Dictionary of session properties to set
        http_headers: Custom HTTP headers for the connection
        isolation_level: Transaction isolation level
        **kwargs: Additional arguments passed to trino.dbapi.connect
    """
    
    def __init__(
        self,
        host: str,
        port: int = 8080,
        user: str = "trino",
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        use_ssl: bool = False,
        verify_ssl: bool = True,
        session_properties: Optional[Dict[str, str]] = None,
        http_headers: Optional[Dict[str, str]] = None,
        isolation_level: Optional[str] = None,
        **kwargs
    ):
        """Initialize the Trino adapter."""
        if not TRINO_AVAILABLE:
            raise ImportError(
                "trino package is required for TrinoAdapter. "
                "Install it with: pip install trino"
            )
        
        self._session_properties = session_properties or {}
        
        # Prepare connection parameters
        conn_params = {
            "host": host,
            "port": port,
            "user": user,
            "http_scheme": "https" if use_ssl else "http",
            "verify": verify_ssl
        }
        
        # Add optional parameters if provided
        if catalog:
            conn_params["catalog"] = catalog
        if schema:
            conn_params["schema"] = schema
        if http_headers:
            conn_params["http_headers"] = http_headers
        if isolation_level:
            conn_params["isolation_level"] = isolation_level
            
        # Add any additional kwargs
        conn_params.update(kwargs)
        
        # Create connection
        self._connection = trino.dbapi.connect(**conn_params)
        self._cursor = self._connection.cursor()
        
        # Set session properties
        for prop_name, prop_value in self._session_properties.items():
            self.set_session_property(prop_name, prop_value)
    
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.
        
        Trino has a practical limit of around 1MB for query size.
        
        Returns:
            Maximum query size in bytes (1,000,000)
        """
        return 1_000_000  # 1MB default limit
    
    def execute(self, sql: str) -> List[Tuple]:
        """
        Execute a SQL statement and return results.
        
        Important: Trino does not support executing multiple SQL statements in a single
        query. If the input contains multiple statements (separated by semicolons),
        this adapter will raise an error. Each statement must be executed individually.
        
        Args:
            sql: SQL statement to execute
            
        Returns:
            List of result rows as tuples
        """
        # First apply any session properties
        self._apply_session_properties()
        
        # Check for multiple statements separated by semicolons
        # This is a safety check as Trino does not support multiple statements per query
        if ";" in sql.strip()[:-1]:  # Ignore trailing semicolon
            sql_parts = [s.strip() for s in sql.split(";") if s.strip()]
            if len(sql_parts) > 1:
                raise ValueError(
                    "Trino does not support multiple statements in a single query. "
                    "Use SQLBatcher to process statements individually."
                )
        
        # Execute the statement
        self._cursor.execute(sql)
        
        # For SELECT statements, return the results
        if self._cursor.description is not None:
            return self._cursor.fetchall()
        
        # For other statements (INSERT, CREATE, etc.), return empty list
        return []
    
    def _apply_session_properties(self) -> None:
        """Apply session properties to the current connection."""
        for name, value in self._session_properties.items():
            # Skip if already applied (optimization for batch operations)
            self._cursor.execute(f"SET SESSION {name} = '{value}'")
    
    def begin_transaction(self) -> None:
        """Begin a transaction."""
        self._cursor.execute("START TRANSACTION")
    
    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        self._cursor.execute("COMMIT")
    
    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        self._cursor.execute("ROLLBACK")
    
    def close(self) -> None:
        """Close the connection."""
        if hasattr(self, '_cursor') and self._cursor is not None:
            self._cursor.close()
        if hasattr(self, '_connection') and self._connection is not None:
            self._connection.close()
    
    def set_session_property(self, name: str, value: str) -> None:
        """
        Set a Trino session property.
        
        Args:
            name: Property name
            value: Property value
        """
        self._session_properties[name] = value
        self._cursor.execute(f"SET SESSION {name} = '{value}'")
    
    def get_catalogs(self) -> List[str]:
        """
        Get available catalogs.
        
        Returns:
            List of catalog names
        """
        result = self.execute("SHOW CATALOGS")
        return [row[0] for row in result]
    
    def get_schemas(self, catalog: str) -> List[str]:
        """
        Get available schemas in a catalog.
        
        Args:
            catalog: Catalog name
            
        Returns:
            List of schema names
        """
        result = self.execute(f"SHOW SCHEMAS FROM {catalog}")
        return [row[0] for row in result]
    
    def get_tables(self, catalog: str, schema: str) -> List[str]:
        """
        Get available tables in a schema.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            
        Returns:
            List of table names
        """
        result = self.execute(f"SHOW TABLES FROM {catalog}.{schema}")
        return [row[0] for row in result]
    
    def get_columns(self, table: str, catalog: str, schema: str) -> List[Dict[str, str]]:
        """
        Get columns for a table.
        
        Args:
            table: Table name
            catalog: Catalog name
            schema: Schema name
            
        Returns:
            List of column information dictionaries
        """
        result = self.execute(
            f"SELECT column_name, data_type FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
        )
        return [{"name": row[0], "type": row[1]} for row in result]