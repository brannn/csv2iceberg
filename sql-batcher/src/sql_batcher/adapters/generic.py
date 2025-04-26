"""
Generic DBAPI adapter for SQL Batcher.

This module provides a generic adapter that works with any database connection
that follows the Python Database API Specification (PEP 249).
"""
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from sql_batcher.adapters.base import SQLAdapter


logger = logging.getLogger(__name__)


class GenericAdapter(SQLAdapter):
    """
    Generic adapter for DBAPI 2.0 compliant database connections.
    
    This adapter works with any database connection that follows the
    Python Database API Specification (PEP 249), providing a simple way
    to use SQLBatcher with a wide range of database systems.
    
    Attributes:
        connection: DBAPI-compliant database connection
        max_query_size: Maximum query size in bytes
        fetch_results: Whether to fetch and return results for SELECT queries
    
    Examples:
        Using with SQLite:
        
        >>> import sqlite3
        >>> from sql_batcher import SQLBatcher
        >>> from sql_batcher.adapters.generic import GenericAdapter
        >>> 
        >>> connection = sqlite3.connect(":memory:")
        >>> adapter = GenericAdapter(connection=connection)
        >>> batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
        >>> 
        >>> # Execute some statements
        >>> adapter.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        >>> statements = [
        ...     "INSERT INTO users VALUES (1, 'Alice')",
        ...     "INSERT INTO users VALUES (2, 'Bob')"
        ... ]
        >>> batcher.process_statements(statements, adapter.execute)
        >>> 
        >>> # Query the data
        >>> results = adapter.execute("SELECT * FROM users")
        >>> for row in results:
        ...     print(row)
    """
    
    def __init__(
        self,
        connection: Any,
        max_query_size: int = 1_000_000,
        fetch_results: bool = True,
    ) -> None:
        """
        Initialize a new GenericAdapter instance.
        
        Args:
            connection: DBAPI-compliant database connection
            max_query_size: Maximum query size in bytes (default: 1,000,000)
            fetch_results: Whether to fetch and return results (default: True)
        """
        self.connection = connection
        self.max_query_size = max_query_size
        self.fetch_results = fetch_results
        
        logger.debug(
            f"Initialized GenericAdapter with max_query_size={max_query_size}, "
            f"fetch_results={fetch_results}"
        )
    
    def execute(self, sql: str) -> List[Tuple]:
        """
        Execute a SQL statement using the provided connection.
        
        This method executes the SQL statement using the connection's cursor.
        For SELECT queries, it fetches and returns the results as a list of tuples.
        For other queries, it returns an empty list.
        
        Args:
            sql: SQL statement to execute
            
        Returns:
            List of result rows as tuples
            
        Raises:
            Exception: Any database-specific exception that occurs during execution
        """
        logger.debug(f"Executing SQL: {sql[:100]}...")
        
        cursor = self.connection.cursor()
        cursor.execute(sql)
        
        results = []
        # If it's a SELECT query and fetch_results is True, fetch the results
        if self.fetch_results and hasattr(cursor, "description") and cursor.description:
            results = cursor.fetchall()
            logger.debug(f"Query returned {len(results)} rows")
        
        return results
    
    def get_max_query_size(self) -> int:
        """
        Return the maximum query size in bytes.
        
        Returns:
            Maximum query size in bytes
        """
        return self.max_query_size
    
    def close(self) -> None:
        """
        Close the database connection.
        
        This method closes the database connection to release resources.
        """
        if self.connection:
            logger.debug("Closing database connection")
            self.connection.close()
    
    def begin_transaction(self) -> None:
        """
        Begin a database transaction.
        
        This method starts a transaction using the connection's begin() method,
        if it exists, or by executing a BEGIN statement.
        
        Raises:
            NotImplementedError: If the database doesn't support transactions
        """
        logger.debug("Beginning transaction")
        
        # Try connection.begin() method first (some DBAPI drivers support this)
        if hasattr(self.connection, "begin"):
            self.connection.begin()
        else:
            # Otherwise, try a standard BEGIN statement
            cursor = self.connection.cursor()
            cursor.execute("BEGIN")
    
    def commit_transaction(self) -> None:
        """
        Commit the current transaction.
        
        This method commits the current transaction using the connection's
        commit() method.
        
        Raises:
            NotImplementedError: If the database doesn't support transactions
        """
        logger.debug("Committing transaction")
        
        if hasattr(self.connection, "commit"):
            self.connection.commit()
        else:
            raise NotImplementedError("This database connection does not support transactions")
    
    def rollback_transaction(self) -> None:
        """
        Rollback the current transaction.
        
        This method rolls back the current transaction using the connection's
        rollback() method.
        
        Raises:
            NotImplementedError: If the database doesn't support transactions
        """
        logger.debug("Rolling back transaction")
        
        if hasattr(self.connection, "rollback"):
            self.connection.rollback()
        else:
            raise NotImplementedError("This database connection does not support transactions")