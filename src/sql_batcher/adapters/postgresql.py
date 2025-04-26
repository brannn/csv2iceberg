"""
PostgreSQL adapter for SQL Batcher.

This module provides a specialized adapter for PostgreSQL databases, taking advantage
of PostgreSQL-specific features and optimizations.
"""
from typing import Any, Dict, List, Optional, Tuple, Union
import logging
import re

from sql_batcher.adapters.base import SQLAdapter

# Optional imports to avoid hard dependency
try:
    import psycopg2
    import psycopg2.extras
    _has_psycopg2 = True
except ImportError:
    _has_psycopg2 = False


logger = logging.getLogger(__name__)


class PostgreSQLAdapter(SQLAdapter):
    """
    PostgreSQL adapter for SQL Batcher.
    
    This adapter provides a SQL Batcher interface specifically optimized for PostgreSQL,
    implementing features that take advantage of PostgreSQL's capabilities.
    
    Attributes:
        connection: PostgreSQL database connection
        cursor: Current cursor for executing queries
        max_query_size: Maximum query size in bytes (default 500MB for PostgreSQL)
        isolation_level: Transaction isolation level (0-3)
        cursor_factory: Factory for creating cursors
        fetch_results: Whether to fetch results by default
    """
    
    def __init__(
        self, 
        connection_params: Optional[Dict[str, Any]] = None,
        connection: Optional[Any] = None,
        max_query_size: int = 500_000_000,  # 500MB is a practical limit for PostgreSQL
        isolation_level: str = "read_committed",
        cursor_factory: Optional[Any] = None,
        fetch_results: bool = True,
        application_name: Optional[str] = "sql_batcher"
    ):
        """
        Initialize the PostgreSQL adapter.
        
        Args:
            connection_params: Dictionary of PostgreSQL connection parameters
            connection: Existing PostgreSQL connection to use (optional)
            max_query_size: Maximum query size in bytes
            isolation_level: Transaction isolation level
                (read_committed, repeatable_read, serializable)
            cursor_factory: Factory for creating cursors
                (e.g., psycopg2.extras.DictCursor, psycopg2.extras.NamedTupleCursor)
            fetch_results: Whether to fetch results by default
            application_name: Application name to set in PostgreSQL (for monitoring)
            
        Raises:
            ImportError: If psycopg2 is not installed
            ValueError: If both connection and connection_params are None
            RuntimeError: If connection to PostgreSQL fails
        """
        if not _has_psycopg2:
            raise ImportError(
                "psycopg2 is not installed. "
                "Install it with 'pip install \"sql-batcher[postgresql]\"'"
            )
        
        # Import isolation levels here to avoid issues
        from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
        from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ
        from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE
        
        # Set up isolation level mapping
        isolation_levels = {
            "read_committed": ISOLATION_LEVEL_READ_COMMITTED,
            "repeatable_read": ISOLATION_LEVEL_REPEATABLE_READ,
            "serializable": ISOLATION_LEVEL_SERIALIZABLE,
        }
        
        self.max_query_size = max_query_size
        self.fetch_results = fetch_results
        
        # Set up cursor factory
        self.cursor_factory = cursor_factory
        
        # Determine the isolation level
        if isolation_level not in isolation_levels:
            raise ValueError(
                f"Invalid isolation level: {isolation_level}. "
                f"Valid values are: {', '.join(isolation_levels.keys())}"
            )
        self.isolation_level = isolation_levels[isolation_level]
        
        # Set up connection
        if connection is None and connection_params is None:
            raise ValueError("Either connection or connection_params must be provided")
        
        if connection is None:
            # Add application name to connection parameters if not present
            if application_name and connection_params is not None and "application_name" not in connection_params:
                connection_params["application_name"] = application_name
            
            try:
                # Import psycopg2 again inside the function to ensure it's available
                # This avoids the "possibly unbound" LSP error
                import psycopg2 as pg2
                
                conn_params = connection_params or {}
                self.connection = pg2.connect(**conn_params)
                self.connection.set_isolation_level(self.isolation_level)
                self.connection.autocommit = False
            except Exception as e:
                raise RuntimeError(f"Failed to connect to PostgreSQL: {str(e)}")
        else:
            self.connection = connection
            
            # Set isolation level on existing connection if possible
            try:
                self.connection.set_isolation_level(self.isolation_level)
            except Exception as e:
                logger.warning(
                    f"Could not set isolation level on existing connection: {str(e)}"
                )
        
        # Create a cursor
        self.cursor = self.create_cursor()
        
        # Flag to track transaction state
        self._in_transaction = False
            
    def create_cursor(self):
        """
        Create a new cursor with the configured cursor factory.
        
        Returns:
            New cursor object
        """
        if self.cursor_factory:
            return self.connection.cursor(cursor_factory=self.cursor_factory)
        else:
            return self.connection.cursor()
    
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.
        
        Returns:
            Maximum query size in bytes
        """
        return self.max_query_size
    
    def execute(self, sql: str) -> List[Tuple]:
        """
        Execute a SQL query.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            List of result rows
            
        Raises:
            RuntimeError: If there's an error executing the query
        """
        # Check for COPY statements which need special handling
        if re.match(r'^\s*COPY\s+', sql, re.IGNORECASE):
            return self._execute_copy(sql)
        
        try:
            self.cursor.execute(sql)
            
            if self.fetch_results and self.cursor.description is not None:
                return self.cursor.fetchall()
            return []
        except Exception as e:
            logger.error(f"PostgreSQL error: {str(e)}")
            if self._in_transaction:
                logger.info("Rolling back transaction due to error")
                self.rollback_transaction()
            raise RuntimeError(f"Failed to execute PostgreSQL query: {str(e)}")
    
    def _execute_copy(self, sql: str) -> List[Tuple]:
        """
        Execute a COPY statement, which needs special handling in psycopg2.
        
        Args:
            sql: COPY SQL statement to execute
            
        Returns:
            Empty list (COPY doesn't return rows)
            
        Raises:
            RuntimeError: If there's an error executing the COPY statement
        """
        try:
            # For COPY, we need to use connection.commit() after execution
            # as COPY statements implicitly commit in PostgreSQL
            self.cursor.execute(sql)
            self.connection.commit()
            
            # COPY doesn't return rows, but returns a count of rows affected
            return []
        except Exception as e:
            logger.error(f"PostgreSQL COPY error: {str(e)}")
            if self._in_transaction:
                logger.info("Rolling back transaction due to error")
                self.rollback_transaction()
            raise RuntimeError(f"Failed to execute PostgreSQL COPY: {str(e)}")
    
    def execute_batch(self, statements: List[str]) -> List[Tuple]:
        """
        Execute a batch of statements.
        
        This method provides simplified batch execution. For complex batch operations
        or better performance with many similar queries, consider using the `executemany`
        functionality available in the psycopg2 driver directly.
        
        Args:
            statements: List of SQL statements to execute
            
        Returns:
            Combined results from all executed statements
        """
        if not statements:
            return []
        
        results = []
        for statement in statements:
            res = self.execute(statement)
            results.extend(res)
        return results
    
    def begin_transaction(self) -> None:
        """
        Begin a transaction.
        """
        if not self._in_transaction:
            # PostgreSQL is in transaction by default, but we can be explicit
            self.execute("BEGIN")
            self._in_transaction = True
    
    def commit_transaction(self) -> None:
        """
        Commit the current transaction.
        """
        if self._in_transaction:
            self.connection.commit()
            self._in_transaction = False
    
    def rollback_transaction(self) -> None:
        """
        Rollback the current transaction.
        """
        if self._in_transaction:
            self.connection.rollback()
            self._in_transaction = False
    
    def close(self) -> None:
        """
        Close the connection.
        """
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
    
    def explain_analyze(self, sql: str) -> List[Tuple]:
        """
        Run EXPLAIN ANALYZE on a query to get execution plan details.
        
        Args:
            sql: SQL query to analyze
            
        Returns:
            List of rows with execution plan information
        """
        explain_sql = f"EXPLAIN (ANALYZE, VERBOSE, BUFFERS) {sql}"
        return self.execute(explain_sql)
    
    def create_temp_table(self, table_name: str, as_select: Optional[str] = None) -> None:
        """
        Create a temporary table, optionally populating it with a SELECT query.
        
        Args:
            table_name: Name for the temporary table
            as_select: Optional SELECT query to populate the table
        """
        if as_select:
            self.execute(f"CREATE TEMP TABLE {table_name} AS {as_select}")
        else:
            self.execute(f"CREATE TEMP TABLE {table_name} (LIKE {table_name} INCLUDING ALL)")
    
    def get_server_version(self) -> Tuple[int, int, int]:
        """
        Get the PostgreSQL server version.
        
        Returns:
            Tuple of (major, minor, patch) version numbers
        """
        result = self.execute("SHOW server_version")
        version_str = result[0][0]
        
        # Parse version string (e.g., "14.2" or "14.2 (Debian 14.2-1.pgdg110+1)")
        match = re.match(r'(\d+)\.(\d+)(?:\.(\d+))?', version_str)
        if match:
            major = int(match.group(1))
            minor = int(match.group(2))
            patch = int(match.group(3)) if match.group(3) else 0
            return (major, minor, patch)
        else:
            # Return a default if we can't parse
            return (0, 0, 0)
            
    def use_copy_for_bulk_insert(self, table_name: str, column_names: List[str], 
                                 data: List[Tuple], temp_file: Optional[str] = None) -> int:
        """
        Use PostgreSQL's COPY command for efficient bulk data loading.
        
        This method provides a much faster alternative to INSERT statements for
        bulk data loading.
        
        Args:
            table_name: Name of the target table
            column_names: List of column names
            data: List of data tuples (must match column order)
            temp_file: Optional path to temp file for COPY FROM (if None, uses COPY FROM STDIN)
            
        Returns:
            Number of rows inserted
            
        Raises:
            RuntimeError: If there's an error during the COPY operation
        """
        columns_clause = f"({', '.join(column_names)})" if column_names else ""
        
        try:
            if temp_file:
                # Write data to temp file and use COPY FROM file
                with open(temp_file, 'w') as f:
                    for row in data:
                        f.write('\t'.join(str(x) for x in row) + '\n')
                
                self.execute(f"COPY {table_name} {columns_clause} FROM '{temp_file}'")
                return len(data)
            else:
                # Use COPY FROM STDIN for direct streaming
                try:
                    # First try using the modern context manager approach if available
                    if hasattr(self.cursor, 'copy'):
                        # psycopg2 >= 2.8
                        with self.cursor.copy(f"COPY {table_name} {columns_clause} FROM STDIN") as copy:
                            for row in data:
                                copy.write_row(row)
                    else:
                        # Fallback for older psycopg2 versions using copy_from
                        from io import StringIO
                        output = StringIO()
                        for row in data:
                            # Convert each value to string and escape tab characters
                            values = [
                                str(val).replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                                if val is not None else '\\N'
                                for val in row
                            ]
                            output.write('\t'.join(values) + '\n')
                        
                        output.seek(0)
                        self.cursor.copy_from(output, table_name, columns=column_names)
                except AttributeError:
                    # If neither copy nor copy_from is available, execute INSERT statements as fallback
                    logger.warning("COPY functionality not available in this psycopg2 version - falling back to INSERT")
                    for row in data:
                        placeholders = ", ".join(["%s"] * len(row))
                        insert_sql = f"INSERT INTO {table_name} {columns_clause} VALUES ({placeholders})"
                        self.cursor.execute(insert_sql, row)
                
                return len(data)
        except Exception as e:
            logger.error(f"PostgreSQL COPY error: {str(e)}")
            if self._in_transaction:
                self.rollback_transaction()
            raise RuntimeError(f"Failed to execute PostgreSQL bulk insert via COPY: {str(e)}")
            
    def create_indices(self, table_name: str, indices: List[Dict[str, Any]]) -> None:
        """
        Create multiple indices on a table with error handling.
        
        Args:
            table_name: Name of the table to create indices on
            indices: List of index specifications, each a dict with keys:
                - 'columns': List of column names
                - 'name': Optional index name
                - 'unique': Boolean indicating if this is a unique index
                - 'method': Optional index method (btree, hash, gin, etc.)
                - 'where': Optional WHERE clause for partial indices
                - 'concurrent': Whether to create the index concurrently
        """
        for idx in indices:
            columns = idx.get('columns', [])
            if not columns:
                continue
                
            name = idx.get('name', f"idx_{table_name}_{'_'.join(columns)}")
            unique = "UNIQUE " if idx.get('unique', False) else ""
            method = f"USING {idx['method']} " if idx.get('method') else ""
            where = f"WHERE {idx['where']}" if idx.get('where') else ""
            concurrent = "CONCURRENTLY " if idx.get('concurrent', False) else ""
            
            columns_str = ', '.join(columns)
            
            try:
                self.execute(
                    f"CREATE {unique}INDEX {concurrent}{name} ON {table_name} {method}({columns_str}) {where}"
                )
            except Exception as e:
                logger.error(f"Failed to create index {name}: {str(e)}")
                # Continue with other indices even if one fails
                continue