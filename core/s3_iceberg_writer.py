"""
S3 Tables Iceberg writer module for CSV to Iceberg conversion using PyIceberg REST API
"""
import os
import logging
import time
import csv
from typing import Dict, List, Any, Optional, Callable, Tuple

# Use Polars for data processing
import polars as pl

# Import PyIceberg schema
from pyiceberg.schema import Schema

# Use S3 REST client
from connectors.s3_rest_client import S3RestClient
from core.schema_inferrer import infer_schema_from_df
from core.query_collector import QueryCollector

# Configure logger
logger = logging.getLogger(__name__)

class S3IcebergWriter:
    """Class for writing data to Iceberg tables in AWS S3 Tables"""
    
    def __init__(
        self,
        s3_rest_client: S3RestClient,
        namespace: str,
        table_name: str,
    ):
        """
        Initialize S3 Iceberg writer.
        
        Args:
            s3_rest_client: S3RestClient instance
            namespace: Schema/namespace name
            table_name: Table name
        """
        self.s3_rest_client = s3_rest_client
        self.namespace = namespace
        self.table_name = table_name
        self.performance_metrics = {
            "total_rows": 0,
            "total_processing_time": 0,
            "processing_rate": 0,
            "total_batches": 0,
            "avg_batch_size": 0,
            "avg_batch_time": 0,
            "batch_times": [],
            "batch_sizes": []
        }
        
    def create_table_with_schema(
        self,
        schema: Schema,
        mode: str = "append",
        location: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        dry_run: bool = False,
        query_collector: Optional[QueryCollector] = None
    ) -> bool:
        """
        Create an Iceberg table with the provided schema in AWS S3 Tables.
        
        Args:
            schema: PyIceberg Schema object
            mode: Write mode (append or overwrite)
            location: Optional custom location for the table
            properties: Additional table properties
            dry_run: If True, collect operations without executing them
            query_collector: QueryCollector instance for storing operations in dry run mode
            
        Returns:
            True if the operation was successful
        """
        # Check for dry run mode
        if dry_run and query_collector:
            query_collector.add_ddl(
                f"CREATE TABLE {self.namespace}.{self.table_name} (schema object) with properties: {properties}"
            )
            return True
            
        # Check if table exists
        table_exists = self.s3_rest_client.table_exists(self.namespace, self.table_name)
        
        # Handle table creation based on mode
        if table_exists:
            if mode == "overwrite":
                logger.info(f"Table {self.namespace}.{self.table_name} exists, dropping for overwrite mode")
                if not self.s3_rest_client.drop_table(self.namespace, self.table_name):
                    logger.error(f"Failed to drop table {self.namespace}.{self.table_name} for overwrite")
                    return False
                
                # Create new table after dropping
                return self._create_new_table(schema, location, properties)
            else:
                # Append mode - table already exists, so no need to create
                logger.info(f"Table {self.namespace}.{self.table_name} exists, using for append mode")
                return True
        else:
            # Table doesn't exist, create it
            return self._create_new_table(schema, location, properties)
    
    def _create_new_table(
        self, 
        schema: Schema, 
        location: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Create a new Iceberg table with the provided schema.
        
        Args:
            schema: PyIceberg Schema object
            location: Optional custom location for the table
            properties: Additional table properties
            
        Returns:
            True if the operation was successful
        """
        # Ensure namespace exists
        if not self.s3_rest_client.namespace_exists(self.namespace):
            logger.info(f"Creating namespace {self.namespace} as it doesn't exist")
            if not self.s3_rest_client.create_namespace(self.namespace):
                logger.error(f"Failed to create namespace {self.namespace}")
                return False
        
        # Create table with schema
        table_props = properties or {}
        
        # Add format version property
        if "format-version" not in table_props:
            table_props["format-version"] = "2"
            
        table = self.s3_rest_client.create_table(
            namespace=self.namespace,
            table_name=self.table_name,
            schema=schema,
            location=location,
            properties=table_props
        )
        
        return table is not None
    
    def write_dataframe(
        self,
        df: pl.DataFrame,
        mode: str = "append",
        batch_size: int = 20000,
        progress_callback: Optional[Callable[[int], None]] = None,
        dry_run: bool = False,
        query_collector: Optional[QueryCollector] = None
    ) -> bool:
        """
        Write a Polars DataFrame to an Iceberg table in S3 Tables.
        
        Args:
            df: Polars DataFrame
            mode: Write mode (append or overwrite)
            batch_size: Number of rows to process in each batch
            progress_callback: Optional callback function for progress updates
            dry_run: If True, collect operations without executing them
            query_collector: QueryCollector instance for storing operations in dry run mode
            
        Returns:
            True if the operation was successful
        """
        start_time = time.time()
        
        if dry_run and query_collector:
            total_rows = len(df)
            query_collector.add_dml(
                f"Write {total_rows} rows to table {self.namespace}.{self.table_name} in mode: {mode}"
            )
            
            # Track performance metrics for dry run
            self._update_performance_metrics(
                total_rows=total_rows,
                total_time=0.1,  # Mock time for dry run
                batch_count=1,
                batch_sizes=[total_rows],
                batch_times=[0.1]  # Mock time for dry run
            )
            return True
        
        # Ensure table exists with the proper schema
        if not self.s3_rest_client.table_exists(self.namespace, self.table_name):
            logger.error(f"Table {self.namespace}.{self.table_name} doesn't exist, create it first")
            return False
        
        # Process the dataframe in batches
        total_rows = len(df)
        processed_rows = 0
        batch_times = []
        batch_sizes = []
        
        # Split dataframe into batches
        for i in range(0, total_rows, batch_size):
            batch_start_time = time.time()
            
            # Get the current batch
            end_idx = min(i + batch_size, total_rows)
            current_batch = df.slice(i, end_idx - i)
            batch_size_actual = len(current_batch)
            
            # Write the batch
            logger.info(f"Writing batch of {batch_size_actual} rows to {self.namespace}.{self.table_name}")
            
            # Determine write mode for this batch
            # First batch in overwrite mode should overwrite, subsequent batches should append
            current_mode = mode if i == 0 else "append"
            
            # Write the batch to the table
            success = self.s3_rest_client.write_dataframe_to_table(
                namespace=self.namespace,
                table_name=self.table_name,
                df=current_batch,
                mode=current_mode
            )
            
            if not success:
                logger.error(f"Failed to write batch to {self.namespace}.{self.table_name}")
                return False
            
            # Update processed rows and report progress
            processed_rows += batch_size_actual
            
            # Calculate batch metrics
            batch_end_time = time.time()
            batch_time = batch_end_time - batch_start_time
            batch_times.append(batch_time)
            batch_sizes.append(batch_size_actual)
            
            # Report progress if callback provided
            if progress_callback:
                progress_percent = min(int((processed_rows / total_rows) * 100), 100)
                progress_callback(progress_percent)
                
            logger.info(f"Progress: {processed_rows}/{total_rows} rows ({(processed_rows/total_rows)*100:.1f}%)")
        
        # Calculate and store performance metrics
        total_time = time.time() - start_time
        self._update_performance_metrics(
            total_rows=total_rows,
            total_time=total_time,
            batch_count=len(batch_times),
            batch_sizes=batch_sizes,
            batch_times=batch_times
        )
        
        return True
    
    def _update_performance_metrics(
        self,
        total_rows: int,
        total_time: float,
        batch_count: int,
        batch_sizes: List[int],
        batch_times: List[float]
    ) -> None:
        """
        Update performance metrics for the writer.
        
        Args:
            total_rows: Total number of rows processed
            total_time: Total processing time in seconds
            batch_count: Number of batches processed
            batch_sizes: List of batch sizes
            batch_times: List of batch processing times
        """
        if total_time > 0:
            processing_rate = total_rows / total_time
        else:
            processing_rate = 0
            
        avg_batch_size = sum(batch_sizes) / batch_count if batch_count > 0 else 0
        avg_batch_time = sum(batch_times) / batch_count if batch_count > 0 else 0
        
        self.performance_metrics = {
            "total_rows": total_rows,
            "total_processing_time": total_time,
            "processing_rate": processing_rate,
            "total_batches": batch_count,
            "avg_batch_size": avg_batch_size,
            "avg_batch_time": avg_batch_time,
            "batch_times": batch_times,
            "batch_sizes": batch_sizes
        }
        
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get the performance metrics for the writer.
        
        Returns:
            Dictionary of performance metrics
        """
        return self.performance_metrics
    
    def close(self) -> None:
        """Close resources"""
        if self.s3_rest_client:
            self.s3_rest_client.close()
"""