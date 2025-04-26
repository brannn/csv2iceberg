"""
Generic database adapter for SQL Batcher.

This module provides a generic adapter that can work with any database connection
that follows the DB-API 2.0 specification (e.g., sqlite3, psycopg2, etc.).
"""
from typing import Any, Dict, List, Optional, Tuple, Union

from sql_batcher.adapters.base import SQLAdapter


class GenericAdapter(SQLAdapter):
    """
    Generic adapter for SQL Batcher that works with any standard DB-API 2.0 connection.
    
    This adapter is designed to work with any database connection that follows
    the Python Database API Specification v2.0 (PEP 249), such as sqlite3,
    psycopg2, mysql-connector-python, etc.
    
    Attributes:
        connection: Database connection object
        max_query_size: Maximum query size in bytes
        fetch_results: Whether to fetch and return results for SELECT queries
    """
    
    def __init__(
        self,
        connection: Any,
        max_query_size: int = 1_000_000,
        fetch_results: bool = True
    ):
        """
        Initialize the generic adapter.
        
        Args:
            connection: Database connection object following DB-API 2.0
            max_query_size: Maximum query size in bytes
            fetch_results: Whether to fetch and return results for SELECT queries
        """
        self.connection = connection
        self.max_query_size = max_query_size
        self.fetch_results = fetch_results
        self._cursor = None
    
    def execute(self, sql: str) -> List[Tuple]:
        """
        Execute a SQL query.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            List of result rows (empty list for non-SELECT queries)
        """
        if not self._cursor:
            self._cursor = self.connection.cursor()
        
        self._cursor.execute(sql)
        
        # For SELECT queries, fetch results if requested
        if self.fetch_results and sql.strip().upper().startswith("SELECT"):
            results = self._cursor.fetchall()
            return results
        
        # For non-SELECT queries, commit changes
        if not sql.strip().upper().startswith("SELECT"):
            self.connection.commit()
        
        return []
    
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.
        
        Returns:
            Maximum query size in bytes
        """
        return self.max_query_size
    
    def begin_transaction(self) -> None:
        """Begin a transaction."""
        # Most DB-API connections automatically start a transaction,
        # but we can explicitly start one for databases that support it
        try:
            if hasattr(self.connection, "begin"):
                self.connection.begin()
        except Exception:
            # Ignore if not supported
            pass
    
    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        self.connection.commit()
    
    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        self.connection.rollback()
    
    def close(self) -> None:
        """Close the connection."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        
        if self.connection:
            self.connection.close()