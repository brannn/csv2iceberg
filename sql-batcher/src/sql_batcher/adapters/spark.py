"""
Spark SQL adapter for SQL Batcher.

This module provides a Spark-specific adapter for SQLBatcher, optimized
for working with Spark SQL.
"""
import logging
from typing import Any, Optional, Dict, List, Union

from sql_batcher.adapters.base import SQLAdapter

logger = logging.getLogger(__name__)

try:
    import pyspark
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    logger.warning("PySpark package not available. Install with 'pip install pyspark' to use the SparkAdapter.")


class SparkAdapter(SQLAdapter):
    """
    Adapter for connecting SQLBatcher to Spark SQL.
    
    This adapter provides Spark-specific integration for SQLBatcher,
    handling session management and SQL execution.
    
    Example:
        >>> from pyspark.sql import SparkSession
        >>> from sql_batcher import SQLBatcher
        >>> from sql_batcher.adapters.spark import SparkAdapter
        >>> 
        >>> # Create or get a SparkSession
        >>> spark = SparkSession.builder.appName("SQLBatcherExample").getOrCreate()
        >>> 
        >>> # Create a Spark adapter
        >>> adapter = SparkAdapter(spark_session=spark)
        >>> 
        >>> # Create a batcher with appropriate limits
        >>> batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
        >>> 
        >>> # Define some statements
        >>> statements = [
        ...     "CREATE TABLE IF NOT EXISTS users (id INT, name STRING)",
        ...     "INSERT INTO users VALUES (1, 'Alice')",
        ...     "INSERT INTO users VALUES (2, 'Bob')"
        ... ]
        >>> 
        >>> # Process statements using the adapter
        >>> batcher.process_statements(statements, adapter.execute)
    """
    
    def __init__(
        self,
        spark_session: Any,
        max_query_size: int = 2_000_000,  # Higher default for Spark
        return_dataframes: bool = False
    ):
        """
        Initialize a Spark SQL adapter.
        
        Args:
            spark_session: An active SparkSession
            max_query_size: Maximum query size in bytes (default: 2MB)
            return_dataframes: If True, return DataFrames from queries instead of collecting results
        """
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark is required to use SparkAdapter. Install with 'pip install pyspark'.")
        
        if not isinstance(spark_session, pyspark.sql.SparkSession):
            raise TypeError("spark_session must be a SparkSession")
        
        self.spark = spark_session
        self._max_query_size = max_query_size
        self.return_dataframes = return_dataframes
        
        logger.debug(f"Initialized SparkAdapter with max_query_size={max_query_size}")
    
    def execute(self, sql: str) -> Any:
        """
        Execute a SQL statement using Spark SQL.
        
        Args:
            sql: The SQL statement to execute
            
        Returns:
            Result of the SQL execution (DataFrame or collected result)
        """
        try:
            logger.debug(f"Executing Spark SQL: {sql}")
            
            # Execute the SQL
            df = self.spark.sql(sql)
            
            # Return based on configuration
            if self.return_dataframes:
                return df
            else:
                # Only collect results for SELECT queries
                if sql.strip().upper().startswith(("SELECT", "SHOW", "DESCRIBE")):
                    return df.collect()
                return None
        except Exception as e:
            logger.error(f"Error executing Spark SQL: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to execute Spark SQL: {str(e)}") from e
    
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes for Spark SQL.
        
        Returns:
            Maximum query size in bytes
        """
        return self._max_query_size
    
    def close(self) -> None:
        """
        Close operation for Spark (no-op).
        
        Spark sessions should be managed externally, so this is a no-op.
        """
        pass  # Spark sessions should be managed externally