"""
Base adapter interface for SQL Batcher.

This module defines the abstract base class that all SQL Batcher adapters must implement.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union


class SQLAdapter(ABC):
    """
    Abstract base class for SQL Batcher adapters.
    
    This class defines the interface that all SQL Batcher adapters must implement.
    Adapters provide a consistent interface for executing SQL statements across
    different database systems.
    """
    
    @abstractmethod
    def execute(self, sql: str) -> List[Tuple]:
        """
        Execute a SQL query.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            List of result rows
        """
        pass
    
    @abstractmethod
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.
        
        Returns:
            Maximum query size in bytes
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the connection."""
        pass
    
    def begin_transaction(self) -> None:
        """
        Begin a transaction.
        
        Implementations may override this method if they support transactions.
        By default, this method does nothing.
        """
        pass
    
    def commit_transaction(self) -> None:
        """
        Commit the current transaction.
        
        Implementations may override this method if they support transactions.
        By default, this method does nothing.
        """
        pass
    
    def rollback_transaction(self) -> None:
        """
        Rollback the current transaction.
        
        Implementations may override this method if they support transactions.
        By default, this method does nothing.
        """
        pass