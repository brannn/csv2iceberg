"""
BigQuery adapter for SQL Batcher.

This module provides an adapter for Google BigQuery, Google's serverless,
highly scalable, enterprise data warehouse solution.
"""
from typing import Any, Dict, List, Optional, Tuple, Union
import logging

from sql_batcher.adapters.base import SQLAdapter

# Optional imports to avoid hard dependency on Google Cloud libraries
try:
    from google.cloud import bigquery
    from google.cloud.exceptions import GoogleCloudError
    from google.api_core.exceptions import ClientError
    _has_bigquery = True
except ImportError:
    _has_bigquery = False


logger = logging.getLogger(__name__)


class BigQueryAdapter(SQLAdapter):
    """
    BigQuery adapter for SQL Batcher.
    
    This adapter provides a SQL Batcher interface for Google BigQuery, handling
    its specific connection, authentication, and query requirements.
    
    Attributes:
        client: BigQuery client instance
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        location: BigQuery dataset location
        job_config: Default job configuration for queries
        max_query_size: Maximum query size in bytes (1MB for interactive, 20MB for batch)
        use_batch_mode: Whether to use batch mode instead of interactive mode
    """
    
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        location: Optional[str] = None,
        credentials: Optional[Any] = None,
        use_batch_mode: bool = False,
        max_interactive_size: int = 1_000_000,  # 1MB for interactive queries
        max_batch_size: int = 20_000_000,  # 20MB for batch queries
        default_query_parameters: Optional[Dict[str, Any]] = None,
        timeout_ms: Optional[int] = None,
        client: Optional["bigquery.Client"] = None
    ):
        """
        Initialize the BigQuery adapter.
        
        Args:
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID to use
            location: BigQuery dataset location (e.g., "US", "EU")
            credentials: Google Cloud credentials object (optional)
            use_batch_mode: Whether to use batch mode for queries
            max_interactive_size: Maximum query size for interactive queries
            max_batch_size: Maximum query size for batch queries
            default_query_parameters: Default query parameters to use
            timeout_ms: Query timeout in milliseconds
            client: Existing BigQuery client (optional)
            
        Raises:
            ImportError: If the Google Cloud BigQuery library is not installed
        """
        if not _has_bigquery:
            raise ImportError(
                "Google Cloud BigQuery library is not installed. "
                "Install it with 'pip install \"sql-batcher[bigquery]\"'"
            )
        
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location
        self.use_batch_mode = use_batch_mode
        self.max_interactive_size = max_interactive_size
        self.max_batch_size = max_batch_size
        
        # Use provided client or create a new one
        if client:
            self.client = client
        else:
            self.client = bigquery.Client(
                project=project_id,
                credentials=credentials,
                location=location
            )
        
        # Set up default job configuration
        self.job_config = bigquery.QueryJobConfig()
        
        if default_query_parameters:
            self.job_config.query_parameters = [
                bigquery.ScalarQueryParameter(k, self._get_param_type(v), v)
                for k, v in default_query_parameters.items()
            ]
            
        if timeout_ms:
            self.job_config.timeout_ms = timeout_ms
        
        # Set default dataset reference
        self.job_config.default_dataset = f"{project_id}.{dataset_id}"
        
        # Use legacy SQL? (typically False for modern BigQuery usage)
        self.job_config.use_legacy_sql = False
        
        # Store the current transaction state (BigQuery supports multi-statement transactions)
        self._in_transaction = False
        
    def _get_param_type(self, value: Any) -> str:
        """
        Get the BigQuery parameter type based on the Python value.
        
        Args:
            value: Python value
            
        Returns:
            String representation of the BigQuery type
        """
        if isinstance(value, bool):
            return 'BOOL'
        elif isinstance(value, int):
            return 'INT64'
        elif isinstance(value, float):
            return 'FLOAT64'
        elif isinstance(value, str):
            return 'STRING'
        elif isinstance(value, bytes):
            return 'BYTES'
        elif hasattr(value, 'strftime'):  # datetime-like
            return 'TIMESTAMP'
        else:
            return 'STRING'  # Default to string for unknown types
        
    def execute(self, sql: str) -> List[Tuple]:
        """
        Execute a SQL query in BigQuery.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            List of result rows
            
        Raises:
            RuntimeError: If there's an error executing the query
        """
        try:
            # Clone the job config to avoid modifying the original
            job_config = self.job_config._copy()
            
            # Adjust batch mode and priority based on the use_batch_mode setting
            if self.use_batch_mode:
                job_config.priority = bigquery.QueryPriority.BATCH
            else:
                job_config.priority = bigquery.QueryPriority.INTERACTIVE
                
            # Execute the query
            query_job = self.client.query(sql, job_config=job_config)
            
            # Wait for the query to complete
            results = query_job.result()
            
            # Convert to a list of tuples
            rows = [tuple(row.values()) for row in results]
            return rows
            
        except (GoogleCloudError, ClientError) as e:
            logger.error(f"BigQuery error: {str(e)}")
            raise RuntimeError(f"Failed to execute BigQuery query: {str(e)}")
            
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes based on the configured mode.
        
        Returns:
            Maximum query size in bytes
        """
        if self.use_batch_mode:
            return self.max_batch_size
        else:
            return self.max_interactive_size
            
    def begin_transaction(self) -> None:
        """
        Begin a BigQuery multi-statement transaction.
        
        Note: BigQuery supports multi-statement transactions in certain contexts.
        This method sets up the transaction context.
        """
        if not self._in_transaction:
            self.execute("BEGIN TRANSACTION")
            self._in_transaction = True
            
    def commit_transaction(self) -> None:
        """
        Commit the current BigQuery transaction.
        """
        if self._in_transaction:
            self.execute("COMMIT TRANSACTION")
            self._in_transaction = False
            
    def rollback_transaction(self) -> None:
        """
        Rollback the current BigQuery transaction.
        """
        if self._in_transaction:
            self.execute("ROLLBACK TRANSACTION")
            self._in_transaction = False
            
    def close(self) -> None:
        """
        Close the BigQuery client connection.
        
        This method ensures any open resources are properly cleaned up.
        """
        if hasattr(self, 'client') and self.client:
            self.client.close()