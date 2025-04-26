"""
Query collector interface for SQL Batcher.

This module defines the QueryCollector class and related interfaces for
collecting SQL queries during dry run mode.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class QueryCollector(ABC):
    """
    Abstract base class for query collectors.
    
    QueryCollector classes are used to store SQL queries during dry run mode,
    allowing applications to analyze, log, or otherwise process these queries
    instead of executing them directly.
    
    Examples:
        Custom implementation:
        
        >>> class MyQueryCollector(QueryCollector):
        ...     def __init__(self):
        ...         self.queries = []
        ...     
        ...     def add_query(self, query, metadata=None):
        ...         self.queries.append({"query": query, "metadata": metadata})
        ...     
        ...     def get_queries(self):
        ...         return self.queries
    """
    
    @abstractmethod
    def add_query(self, query: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Add a query to the collector.
        
        Args:
            query: SQL query string
            metadata: Additional metadata to associate with the query (optional)
        """
        pass
    
    @abstractmethod
    def get_queries(self) -> List[Dict[str, Any]]:
        """
        Get all collected queries.
        
        Returns:
            List of collected queries, typically as dictionaries
        """
        pass


class ListQueryCollector(QueryCollector):
    """
    Simple query collector that stores queries in a list.
    
    This implementation stores queries as dictionaries with 'query' and 'metadata'
    keys in a simple list.
    
    Examples:
        >>> from sql_batcher import SQLBatcher
        >>> from sql_batcher.query_collector import ListQueryCollector
        >>> 
        >>> collector = ListQueryCollector()
        >>> batcher = SQLBatcher(max_bytes=1000000, dry_run=True)
        >>> 
        >>> statements = [
        ...     "INSERT INTO users VALUES (1, 'Alice')",
        ...     "INSERT INTO users VALUES (2, 'Bob')"
        ... ]
        >>> 
        >>> # Process statements without executing them
        >>> batcher.process_statements(
        ...     statements,
        ...     lambda x: None,  # No-op execution function
        ...     query_collector=collector
        ... )
        >>> 
        >>> # Access the collected queries
        >>> for query_info in collector.get_queries():
        ...     print(f"Query: {query_info['query']}")
        ...     if query_info['metadata']:
        ...         print(f"Metadata: {query_info['metadata']}")
    """
    
    def __init__(self) -> None:
        """Initialize an empty list collector."""
        self.queries: List[Dict[str, Any]] = []
    
    def add_query(self, query: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Add a query to the collector.
        
        Args:
            query: SQL query string
            metadata: Additional metadata to associate with the query (optional)
        """
        self.queries.append({
            "query": query,
            "metadata": metadata,
        })
    
    def get_queries(self) -> List[Dict[str, Any]]:
        """
        Get all collected queries.
        
        Returns:
            List of dictionaries with 'query' and 'metadata' keys
        """
        return self.queries