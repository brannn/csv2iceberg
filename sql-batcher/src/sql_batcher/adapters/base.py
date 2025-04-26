"""
Base adapter interface for SQL Batcher.

This module defines the base adapter interface that all specific
database adapters should implement.
"""
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List


class SQLAdapter(ABC):
    """
    Base class for database-specific adapters.
    
    This abstract class defines the interface that all database adapters
    must implement. It provides a standard way for SQLBatcher to interact
    with different database systems.
    """
    
    @abstractmethod
    def execute(self, sql: str) -> Any:
        """
        Execute a SQL statement.
        
        Args:
            sql: The SQL statement to execute
            
        Returns:
            Result of the SQL execution (implementation-specific)
        """
        pass
    
    @abstractmethod
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes for this database.
        
        Returns:
            Maximum query size in bytes
        """
        pass
    
    def close(self) -> None:
        """
        Close any open database connections.
        
        Default implementation does nothing, override as needed.
        """
        pass
    
    def begin_transaction(self) -> None:
        """
        Begin a database transaction.
        
        Default implementation does nothing, override as needed.
        """
        pass
    
    def commit_transaction(self) -> None:
        """
        Commit the current database transaction.
        
        Default implementation does nothing, override as needed.
        """
        pass
    
    def rollback_transaction(self) -> None:
        """
        Rollback the current database transaction.
        
        Default implementation does nothing, override as needed.
        """
        pass