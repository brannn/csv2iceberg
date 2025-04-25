"""
Query collector module for dry run mode in CSV to Iceberg conversion
"""
import logging
from typing import Dict, List, Any, Optional, Union

logger = logging.getLogger(__name__)

class QueryCollector:
    """
    Collects queries during dry run mode instead of executing them.
    
    This class provides a way to track what SQL statements would be executed
    during a conversion job without actually running them against the database.
    """
    
    def __init__(self):
        """Initialize a new query collector"""
        self.queries: List[Dict[str, Any]] = []
        self.ddl_statements: List[Dict[str, Any]] = []
        # Use Dict[str, Union[int, float]] to allow mixed types
        self.stats: Dict[str, Union[int, float]] = {
            "total_rows": 0,
            "batches": 0,
            "estimated_execution_time": 0.0,  # In seconds (using float to avoid int conversion issues)
            "tables_created": 0,
            "tables_modified": 0
        }
    
    def add_query(self, query: str, query_type: str = "DML", row_count: int = 0, table_name: Optional[str] = None):
        """
        Add a query to the collector
        
        Args:
            query: The SQL query string
            query_type: The type of query (DDL or DML)
            row_count: Number of rows affected (for DML queries)
            table_name: Name of the table being affected
        """
        if query_type.upper() == "DDL":
            self.ddl_statements.append({
                "query": query,
                "table_name": table_name
            })
            
            # Track table creation
            if "CREATE TABLE" in query.upper():
                self.stats["tables_created"] += 1
            elif "ALTER TABLE" in query.upper() or "TRUNCATE TABLE" in query.upper() or "DELETE FROM" in query.upper():
                self.stats["tables_modified"] += 1
        else:
            self.queries.append({
                "query": query,
                "row_count": row_count,
                "table_name": table_name
            })
            self.stats["total_rows"] += row_count
            self.stats["batches"] += 1
            
            # Estimate execution time (very rough estimate)
            rows_per_second = 5000  # Assumption: about 5000 rows/sec 
            self.stats["estimated_execution_time"] += row_count / rows_per_second
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of collected queries
        
        Returns:
            Dictionary with summary information
        """
        return {
            "ddl_count": len(self.ddl_statements),
            "dml_count": len(self.queries),
            "stats": self.stats,
            "sample_queries": {
                "ddl": self.ddl_statements[0]["query"] if self.ddl_statements else None,
                "dml": self.queries[0]["query"] if self.queries else None
            }
        }
    
    def add_ddl(self, statement: str, table_name: Optional[str] = None):
        """
        Add a DDL statement to the collector.
        
        This is a convenience method specifically for DDL operations.
        
        Args:
            statement: The DDL statement (e.g., CREATE TABLE)
            table_name: Name of the table being affected
        """
        self.add_query(statement, query_type="DDL", table_name=table_name)
    
    def add_dml(self, statement: str, row_count: int = 0, table_name: Optional[str] = None):
        """
        Add a DML statement to the collector.
        
        This is a convenience method specifically for DML operations.
        
        Args:
            statement: The DML statement (e.g., INSERT INTO)
            row_count: Number of rows affected
            table_name: Name of the table being affected
        """
        self.add_query(statement, query_type="DML", row_count=row_count, table_name=table_name)
    
    def get_ddl_statements(self) -> List[Dict[str, Any]]:
        """
        Get all collected DDL statements.
        
        Returns:
            List of DDL statement dictionaries
        """
        return self.ddl_statements
        
    def get_dml_statements(self) -> List[Dict[str, Any]]:
        """
        Get all collected DML statements.
        
        Returns:
            List of DML statement dictionaries
        """
        return self.queries
    
    def get_full_report(self) -> Dict[str, Any]:
        """
        Get a full report of all collected queries
        
        Returns:
            Dictionary with the complete data
        """
        return {
            "ddl_statements": self.ddl_statements,
            "dml_queries": self.queries,
            "stats": self.stats
        }
    
    def log_summary(self):
        """Log a summary of the collected queries"""
        logger.info("=== DRY RUN SUMMARY ===")
        logger.info(f"Total DDL statements: {len(self.ddl_statements)}")
        logger.info(f"Total DML statements: {len(self.queries)}")
        logger.info(f"Would process approximately {self.stats['total_rows']} rows in {self.stats['batches']} batches")
        logger.info(f"Estimated execution time: {self.stats['estimated_execution_time']:.2f} seconds")
        logger.info(f"Would create {self.stats['tables_created']} tables")
        logger.info(f"Would modify {self.stats['tables_modified']} tables")
        
        # Sample DDL
        if self.ddl_statements:
            logger.info("Sample DDL:")
            sample_ddl = self.ddl_statements[0]["query"]
            # Truncate if too long
            if len(sample_ddl) > 200:
                sample_ddl = sample_ddl[:200] + "..."
            logger.info(sample_ddl)
        
        # Sample DML
        if self.queries:
            logger.info("Sample DML:")
            sample_dml = self.queries[0]["query"]
            # Truncate if too long
            if len(sample_dml) > 200:
                sample_dml = sample_dml[:200] + "..."
            logger.info(sample_dml)
        
        logger.info("========================")