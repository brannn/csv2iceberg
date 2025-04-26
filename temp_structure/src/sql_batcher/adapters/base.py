"""
Base adapter classes for SQL Batcher.

This module defines the abstract base class for database adapters,
which serve as the interface between SQLBatcher and specific database systems.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union


class SQLAdapter(ABC):
    """
    Abstract base class for SQL database adapters.
    
    This class defines the interface that all database adapters must implement
    to work with SQLBatcher. Adapters handle the details of connecting to and
    executing SQL against specific database systems.
    
    At a minimum, concrete adapters must implement:
    - execute(): Execute a SQL statement
    - get_max_query_size(): Return the maximum query size in bytes
    - close(): Clean up resources
    
    Optional methods (can be overridden if the database supports them):
    - begin_transaction(): Start a transaction
    - commit_transaction(): Commit a transaction
    - rollback_transaction(): Rollback a transaction
    
    Examples:
        Creating a custom adapter:
        
        >>> class MyAdapter(SQLAdapter):
        ...     def execute(self, sql):
        ...         # Execute SQL using database-specific API
        ...         return []
        ...     
        ...     def get_max_query_size(self):
        ...         return 1000000
        ...     
        ...     def close(self):
        ...         # Clean up resources
        ...         pass
    """
    
    @abstractmethod
    def execute(self, sql: str) -> List[Tuple]:
        """
        Execute a SQL statement and return results.
        
        This method is responsible for executing the provided SQL statement
        against the database and returning any results.
        
        Args:
            sql: SQL statement to execute
            
        Returns:
            List of result rows as tuples
            
        Raises:
            Exception: Any database-specific exception that occurs during execution
        """
        pass
    
    @abstractmethod
    def get_max_query_size(self) -> int:
        """
        Return the maximum recommended query size in bytes.
        
        This method returns the maximum query size in bytes that the
        database can handle efficiently. This value is used by SQLBatcher
        to determine when to flush a batch.
        
        Returns:
            Maximum query size in bytes
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Close the database connection and clean up resources.
        
        This method should perform any necessary cleanup, such as
        closing database connections, releasing locks, etc.
        """
        pass
    
    def begin_transaction(self) -> None:
        """
        Begin a database transaction.
        
        This method starts a transaction, if the database supports
        transactions. The default implementation does nothing.
        
        Raises:
            NotImplementedError: If the database doesn't support transactions
        """
        pass
    
    def commit_transaction(self) -> None:
        """
        Commit the current transaction.
        
        This method commits the current transaction, if the database
        supports transactions. The default implementation does nothing.
        
        Raises:
            NotImplementedError: If the database doesn't support transactions
        """
        pass
    
    def rollback_transaction(self) -> None:
        """
        Rollback the current transaction.
        
        This method rolls back the current transaction, if the database
        supports transactions. The default implementation does nothing.
        
        Raises:
            NotImplementedError: If the database doesn't support transactions
        """
        pass