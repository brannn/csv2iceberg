"""
Spark adapter for SQL Batcher.

This module provides an adapter for Apache Spark SQL via PySpark,
allowing SQL Batcher to work with Spark's SQL engine.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from sql_batcher.adapters.base import SQLAdapter

logger = logging.getLogger(__name__)


class SparkAdapter(SQLAdapter):
    """
    Adapter for Spark SQL using PySpark.
    
    This adapter provides support for executing SQL statements through Spark SQL,
    using a PySpark SparkSession.
    
    Attributes:
        spark_session: PySpark SparkSession
        return_dataframe: Whether to return DataFrames instead of result lists
        max_query_size: Maximum query size in bytes
        fetch_limit: Maximum number of rows to fetch (optional)
    
    Examples:
        >>> from pyspark.sql import SparkSession
        >>> from sql_batcher import SQLBatcher
        >>> from sql_batcher.adapters.spark import SparkAdapter
        >>> 
        >>> # Create a Spark session
        >>> spark = SparkSession.builder.appName("SQLBatcherExample").getOrCreate()
        >>> 
        >>> # Create a Spark adapter
        >>> adapter = SparkAdapter(spark_session=spark)
        >>> 
        >>> # Create a batcher
        >>> batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
        >>> 
        >>> # Process statements
        >>> batcher.process_statements(statements, adapter.execute)
        >>> 
        >>> # Query data
        >>> results = adapter.execute("SELECT * FROM users")
        >>> for row in results:
        ...     print(row)
    """
    
    # Default maximum query size for Spark (1MB)
    DEFAULT_MAX_QUERY_SIZE = 1 * 1024 * 1024
    
    def __init__(
        self,
        spark_session: Any,
        return_dataframe: bool = False,
        max_query_size: int = DEFAULT_MAX_QUERY_SIZE,
        fetch_limit: Optional[int] = None,
    ) -> None:
        """
        Initialize a new SparkAdapter instance.
        
        Args:
            spark_session: PySpark SparkSession
            return_dataframe: Whether to return DataFrames instead of lists (default: False)
            max_query_size: Maximum query size in bytes (default: 1MB)
            fetch_limit: Maximum number of rows to fetch (default: None)
        """
        try:
            import pyspark
            from pyspark.sql import SparkSession
            
            if not isinstance(spark_session, SparkSession):
                raise TypeError(
                    "spark_session must be a pyspark.sql.SparkSession instance"
                )
            
        except ImportError:
            logger.error(
                "PySpark not found. Install it with `pip install pyspark`."
            )
            raise ImportError(
                "Spark adapter requires PySpark. "
                "Install it with `pip install pyspark`."
            )
        
        self.spark = spark_session
        self.return_dataframe = return_dataframe
        self.max_query_size = max_query_size
        self.fetch_limit = fetch_limit
        
        logger.debug(
            f"Initialized SparkAdapter with "
            f"return_dataframe={return_dataframe}, "
            f"max_query_size={max_query_size}, "
            f"fetch_limit={fetch_limit}"
        )
    
    def execute(self, sql: str) -> Union[List[Tuple], Any]:
        """
        Execute a SQL statement on Spark.
        
        This method executes the SQL statement using Spark SQL and returns
        the results either as a DataFrame (if return_dataframe=True) or as
        a list of tuples.
        
        Args:
            sql: SQL statement to execute
            
        Returns:
            List of result rows as tuples, or a Spark DataFrame if return_dataframe=True
            
        Raises:
            Exception: Any Spark-specific exception that occurs during execution
        """
        logger.debug(f"Executing SQL on Spark: {sql[:100]}...")
        
        try:
            # Execute SQL and get DataFrame
            df = self.spark.sql(sql)
            
            # Return DataFrame if requested
            if self.return_dataframe:
                logger.debug("Returning Spark DataFrame")
                return df
            
            # Otherwise convert to list of tuples
            if self.fetch_limit:
                logger.debug(f"Fetching up to {self.fetch_limit} rows")
                rows = df.limit(self.fetch_limit).collect()
            else:
                logger.debug("Fetching all rows")
                rows = df.collect()
            
            # Convert rows to tuples
            results = [tuple(row) for row in rows]
            logger.debug(f"Query returned {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"Failed to execute SQL on Spark: {e}")
            raise
    
    def get_max_query_size(self) -> int:
        """
        Return the maximum query size in bytes.
        
        Returns:
            Maximum query size in bytes
        """
        return self.max_query_size
    
    def close(self) -> None:
        """
        Clean up any resources.
        
        This method doesn't actually close the Spark session, as that's typically
        managed externally, but it can be used to clean up any adapter-specific
        resources.
        """
        logger.debug("Cleaning up Spark adapter resources")
        # No actual cleanup needed for Spark adapter
        pass
    
    def begin_transaction(self) -> None:
        """
        Begin a Spark transaction.
        
        Note: Spark has limited transaction support, and this functionality
        depends on the specific Spark configuration and catalog implementation.
        
        Raises:
            NotImplementedError: If transactions are not supported by the Spark configuration
        """
        logger.debug("Beginning Spark transaction")
        
        try:
            self.execute("START TRANSACTION")
        except Exception as e:
            logger.error(f"Failed to begin transaction: {e}")
            raise NotImplementedError(
                f"Spark transactions not supported or failed: {e}"
            )
    
    def commit_transaction(self) -> None:
        """
        Commit the current Spark transaction.
        
        Raises:
            NotImplementedError: If transactions are not supported by the Spark configuration
        """
        logger.debug("Committing Spark transaction")
        
        try:
            self.execute("COMMIT")
        except Exception as e:
            logger.error(f"Failed to commit transaction: {e}")
            raise NotImplementedError(
                f"Spark transactions not supported or failed: {e}"
            )
    
    def rollback_transaction(self) -> None:
        """
        Rollback the current Spark transaction.
        
        Raises:
            NotImplementedError: If transactions are not supported by the Spark configuration
        """
        logger.debug("Rolling back Spark transaction")
        
        try:
            self.execute("ROLLBACK")
        except Exception as e:
            logger.error(f"Failed to rollback transaction: {e}")
            raise NotImplementedError(
                f"Spark transactions not supported or failed: {e}"
            )