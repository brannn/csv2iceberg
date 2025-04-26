"""
PostgreSQL adapter for SQL Batcher.

This module provides a PostgreSQL-specific adapter for SQL Batcher with
optimizations for PostgreSQL features like COPY commands and transaction management.
"""
import csv
import io
import os
from typing import Any, Dict, List, Optional, Tuple, Union

from sql_batcher.adapters.base import SQLAdapter

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


class PostgreSQLAdapter(SQLAdapter):
    """
    Adapter for PostgreSQL database connections.
    
    This adapter is optimized for PostgreSQL's specific features, including:
    - Multiple statements per query (semicolon-separated)
    - COPY command for bulk data loading
    - Full ACID transaction support
    - JSONB, array, and other PostgreSQL-specific data types
    
    Args:
        connection_params: Dictionary of connection parameters
        isolation_level: Transaction isolation level (read_committed, etc.)
        cursor_factory: Optional cursor factory class
        application_name: Optional application name for monitoring
    """
    
    def __init__(
        self,
        connection_params: Dict[str, Any],
        isolation_level: Optional[str] = None,
        cursor_factory: Optional[Any] = None,
        application_name: Optional[str] = None
    ):
        """Initialize the PostgreSQL adapter."""
        if not PSYCOPG2_AVAILABLE:
            raise ImportError(
                "psycopg2-binary package is required for PostgreSQLAdapter. "
                "Install it with: pip install psycopg2-binary"
            )
        
        # Add application name if provided
        if application_name:
            connection_params = connection_params.copy()
            connection_params["application_name"] = application_name
        
        # Create connection
        self._connection = psycopg2.connect(**connection_params)
        
        # Set isolation level if provided
        if isolation_level:
            if isolation_level.lower() == "read_committed":
                self._connection.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
            elif isolation_level.lower() == "serializable":
                self._connection.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
            elif isolation_level.lower() == "repeatable_read":
                self._connection.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ
            elif isolation_level.lower() == "read_uncommitted":
                self._connection.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_UNCOMMITTED
        
        # Create cursor with factory if provided
        self._cursor_factory = cursor_factory
        if cursor_factory:
            self._cursor = self._connection.cursor(cursor_factory=cursor_factory)
        else:
            self._cursor = self._connection.cursor()
    
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.
        
        PostgreSQL has a practical limit around 500MB for query size,
        but we use a much more conservative default for better performance.
        
        Returns:
            Maximum query size in bytes
        """
        return 5_000_000  # 5MB default limit (conservative)
    
    def execute(self, sql: str) -> List[Any]:
        """
        Execute a SQL statement and return results.
        
        PostgreSQL supports executing multiple statements in a single query
        by separating them with semicolons. This allows SQL Batcher to combine
        multiple statements into a single execution for better performance.
        
        Args:
            sql: SQL statement(s) to execute
            
        Returns:
            List of result rows (for SELECT queries) or empty list for others
        """
        try:
            self._cursor.execute(sql)
            
            # For SELECT statements, return the results
            if self._cursor.description is not None:
                return self._cursor.fetchall()
            
            # For other statements, return empty list
            return []
        except Exception as e:
            # Add context to the error
            raise Exception(f"PostgreSQL Error executing SQL: {str(e)}") from e
    
    def begin_transaction(self) -> None:
        """Begin a transaction."""
        self._connection.autocommit = False
    
    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        self._connection.commit()
    
    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        self._connection.rollback()
    
    def close(self) -> None:
        """Close the connection."""
        if hasattr(self, '_cursor') and self._cursor is not None:
            self._cursor.close()
        if hasattr(self, '_connection') and self._connection is not None:
            self._connection.close()
    
    def explain_analyze(self, sql: str) -> List[Tuple]:
        """
        Run EXPLAIN ANALYZE on a query.
        
        Args:
            sql: SQL query to analyze
            
        Returns:
            Execution plan as a list of rows
        """
        explain_sql = f"EXPLAIN ANALYZE {sql}"
        return self.execute(explain_sql)
    
    def create_temp_table(self, table_name: str, column_defs: str) -> None:
        """
        Create a temporary table.
        
        Args:
            table_name: Name of the temporary table
            column_defs: Column definitions as a SQL string
        """
        sql = f"CREATE TEMPORARY TABLE {table_name} ({column_defs})"
        self.execute(sql)
    
    def get_server_version(self) -> Tuple[int, int, int]:
        """
        Get the PostgreSQL server version.
        
        Returns:
            Tuple of (major, minor, patch) version numbers
        """
        return self._connection.server_version // 10000, (self._connection.server_version // 100) % 100, self._connection.server_version % 100
    
    def execute_batch(self, statements: List[str]) -> int:
        """
        Execute multiple statements as a batch.
        
        This is optimized for PostgreSQL which can handle multiple statements
        in a single execution when separated by semicolons.
        
        Args:
            statements: List of SQL statements to execute
            
        Returns:
            Number of statements executed
        """
        if not statements:
            return 0
        
        # Combine statements with semicolons
        combined_sql = ";\n".join(statements) + ";"
        
        # Execute the combined SQL
        self.execute(combined_sql)
        
        return len(statements)
    
    def use_copy_for_bulk_insert(
        self,
        table_name: str,
        column_names: List[str],
        data: List[Tuple],
        delimiter: str = "\t"
    ) -> int:
        """
        Use PostgreSQL's COPY command for bulk data loading.
        
        This is much faster than individual INSERT statements for large datasets.
        
        Args:
            table_name: Target table name
            column_names: List of column names
            data: List of data tuples
            delimiter: Delimiter for COPY command (default: tab)
            
        Returns:
            Number of rows copied
        """
        if not data:
            return 0
        
        column_clause = ", ".join(column_names)
        
        # Create a file-like object in memory
        csv_data = io.StringIO()
        csv_writer = csv.writer(csv_data, delimiter=delimiter)
        
        # Write all data rows
        for row in data:
            csv_writer.writerow(row)
        
        # Reset position to start of the buffer
        csv_data.seek(0)
        
        # Execute the COPY FROM command
        with self._connection.cursor() as copy_cursor:
            copy_cursor.copy_expert(
                f"COPY {table_name} ({column_clause}) FROM STDIN WITH DELIMITER '{delimiter}'",
                csv_data
            )
        
        # Commit the copy operation
        self._connection.commit()
        
        return len(data)
    
    def create_indices(
        self,
        table_name: str,
        indices: List[Dict[str, Union[str, List[str]]]]
    ) -> List[str]:
        """
        Create indices on a table.
        
        Args:
            table_name: Target table name
            indices: List of index definitions
                Each index is a dict with:
                - columns: List of column names
                - name: Index name
                - unique: Whether the index is unique (optional)
                - method: Index method (btree, hash, gin, etc.) (optional)
                - where: WHERE clause (optional)
            
        Returns:
            List of created index names
        """
        created_indices = []
        
        for index_def in indices:
            columns = index_def.get("columns", [])
            if not columns:
                continue
                
            index_name = index_def.get("name", f"idx_{table_name}_{'_'.join(columns)}")
            unique = "UNIQUE " if index_def.get("unique", False) else ""
            method = index_def.get("method", "btree")
            where = f" WHERE {index_def['where']}" if "where" in index_def else ""
            
            column_clause = ", ".join(columns)
            
            sql = f"CREATE {unique}INDEX IF NOT EXISTS {index_name} ON {table_name} USING {method} ({column_clause}){where}"
            self.execute(sql)
            created_indices.append(index_name)
        
        return created_indices