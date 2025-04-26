"""
Query collector for SQL Batcher to store and analyze queries in dry run mode.

This module provides a QueryCollector class for storing and analyzing SQL queries
during dry run executions.
"""
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class QueryCollector:
    """
    Collects and stores SQL queries for analysis.
    
    This class provides a standardized way to collect SQL queries during dry run mode,
    allowing for analysis and validation without actual execution.
    
    Example:
        >>> from sql_batcher import SQLBatcher, QueryCollector
        >>> 
        >>> # Create a collector and batcher for dry run
        >>> collector = QueryCollector()
        >>> batcher = SQLBatcher(dry_run=True)
        >>> 
        >>> # Define some statements
        >>> statements = [
        ...     "INSERT INTO users VALUES (1, 'Alice')",
        ...     "INSERT INTO users VALUES (2, 'Bob')"
        ... ]
        >>> 
        >>> # Simple callback (not actually executed in dry run)
        >>> def execute_sql(sql):
        ...     pass
        >>> 
        >>> # Process statements, collecting info in the collector
        >>> batcher.process_statements(
        ...     statements, 
        ...     execute_sql, 
        ...     query_collector=collector,
        ...     metadata={"table_name": "users", "type": "INSERT"}
        ... )
        >>> 
        >>> # Analyze collected queries
        >>> print(f"Collected {len(collector.queries)} queries")
        >>> print(f"Estimated total rows: {collector.total_row_count}")
    """
    
    def __init__(self):
        """Initialize a new query collector."""
        self.queries: List[Dict[str, Any]] = []
        self.total_row_count = 0
        
    def add_query(self, query: str, query_type: str = "DML", 
                 row_count: int = 1, table_name: str = "unknown") -> None:
        """
        Add a query to the collector.
        
        Args:
            query: SQL query string
            query_type: Type of query (DDL, DML, etc.)
            row_count: Number of rows affected by this query (estimate)
            table_name: Target table name
        """
        self.queries.append({
            "query": query,
            "type": query_type,
            "row_count": row_count,
            "table_name": table_name
        })
        self.total_row_count += row_count
        logger.debug(f"Added query to collector: {query_type} on {table_name} ({row_count} rows)")
        
    def clear(self) -> None:
        """Clear all collected queries."""
        self.queries = []
        self.total_row_count = 0
        
    def get_queries_by_table(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get all queries for a specific table.
        
        Args:
            table_name: Table name to filter by
            
        Returns:
            List of query dictionaries for the specified table
        """
        return [q for q in self.queries if q["table_name"] == table_name]
    
    def get_queries_by_type(self, query_type: str) -> List[Dict[str, Any]]:
        """
        Get all queries of a specific type.
        
        Args:
            query_type: Query type to filter by (DDL, DML, etc.)
            
        Returns:
            List of query dictionaries for the specified type
        """
        return [q for q in self.queries if q["type"] == query_type]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the collected queries.
        
        Returns:
            Dictionary with statistics like query counts by type, table, etc.
        """
        tables = set(q["table_name"] for q in self.queries)
        types = set(q["type"] for q in self.queries)
        
        stats = {
            "total_queries": len(self.queries),
            "total_row_count": self.total_row_count,
            "tables": {
                table: len(self.get_queries_by_table(table))
                for table in tables
            },
            "query_types": {
                qtype: len(self.get_queries_by_type(qtype))
                for qtype in types
            }
        }
        
        return stats