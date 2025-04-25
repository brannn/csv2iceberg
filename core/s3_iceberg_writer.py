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
from core.schema_inferrer import infer_schema_from_df, infer_schema_from_csv
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
        
    def write_csv_to_iceberg(
        self,
        csv_file: str,
        mode: str = "append",
        delimiter: str = ",",
        has_header: bool = True,
        quote_char: str = '"',
        batch_size: int = 20000,
        sample_size: int = 1000,
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[int], None]] = None,
        dry_run: bool = False,
        max_query_size: int = 700000  # Not used for S3 Tables but kept for API compatibility
    ) -> int:
        """
        Write a CSV file to an Iceberg table in AWS S3 Tables.
        
        Args:
            csv_file: Path to the CSV file
            mode: Write mode (append or overwrite)
            delimiter: CSV delimiter character
            has_header: Whether the CSV has a header row
            quote_char: CSV quote character
            batch_size: Number of rows to process in each batch
            sample_size: Number of rows to sample for schema inference
            include_columns: List of column names to include
            exclude_columns: List of column names to exclude
            progress_callback: Optional callback function for progress updates
            dry_run: If True, collect operations without executing them
            max_query_size: Not used for S3 Tables, kept for API compatibility
            
        Returns:
            Number of rows written
        """
        # Keep track of dry run operations if needed
        query_collector = None
        if dry_run:
            from core.query_collector import QueryCollector
            query_collector = QueryCollector()
            
        # Store reference to performance metrics for easy access
        self.processing_stats = self.performance_metrics
        self.dry_run_results = {}
        
        # First, infer schema from the CSV file
        logger.info(f"Inferring schema from CSV file: {csv_file}")
        inferred_schema_polars, inferred_schema_iceberg = infer_schema_from_csv(
            csv_file=csv_file,
            delimiter=delimiter,
            has_header=has_header,
            quote_char=quote_char,
            max_rows=sample_size,
            include_columns=include_columns,
            exclude_columns=exclude_columns
        )
        
        if not inferred_schema_iceberg:
            logger.error("Failed to infer schema from CSV file")
            return 0
            
        column_names = inferred_schema_polars.columns
        logger.info(f"Inferred schema with {len(column_names)} columns: {column_names}")
        
        # Create the table with the inferred schema
        logger.info(f"Creating table {self.namespace}.{self.table_name} with inferred schema")
        if not self.create_table_with_schema(
            schema=inferred_schema_iceberg,
            mode=mode,
            dry_run=dry_run,
            query_collector=query_collector
        ):
            logger.error(f"Failed to create table with inferred schema")
            return 0
            
        # Read the CSV file into a Polars DataFrame
        logger.info(f"Reading CSV file into DataFrame")
        df_options = {
            "has_header": has_header,
            "separator": delimiter,
            "quote_char": quote_char,
            "ignore_errors": True,
            "truncate_ragged_lines": True  # Handle CSV files with inconsistent number of fields
        }
        
        try:
            if dry_run:
                # For dry run, only read the first few rows to simulate the process
                df = pl.read_csv(csv_file, n_rows=min(sample_size, 100), **df_options)
                logger.info(f"Dry run: Read {len(df)} sample rows from CSV file")
            else:
                # In production, read the entire file
                df = pl.read_csv(csv_file, **df_options)
                logger.info(f"Read {len(df)} rows from CSV file")
                
            # Apply column filters if specified
            if include_columns:
                # Keep only the specified columns
                included_cols = [col for col in include_columns if col in df.columns]
                df = df.select(included_cols)
                logger.info(f"Applied include filter, kept columns: {included_cols}")
                
            elif exclude_columns:
                # Remove the specified columns
                excluded_cols = [col for col in exclude_columns if col in df.columns]
                kept_cols = [col for col in df.columns if col not in excluded_cols]
                df = df.select(kept_cols)
                logger.info(f"Applied exclude filter, removed columns: {excluded_cols}")
                
            # Write the DataFrame to the Iceberg table
            logger.info(f"Writing DataFrame to Iceberg table {self.namespace}.{self.table_name}")
            success = self.write_dataframe(
                df=df,
                mode=mode,
                batch_size=batch_size,
                progress_callback=progress_callback,
                dry_run=dry_run,
                query_collector=query_collector
            )
            
            if not success:
                logger.error(f"Failed to write DataFrame to Iceberg table")
                return 0
                
            # Collect dry run results if applicable
            if dry_run and query_collector:
                self.dry_run_results = {
                    "ddl_statements": query_collector.get_ddl_statements(),
                    "dml_statements": query_collector.get_dml_statements()
                }
                
            # Return the number of rows processed
            return len(df)
            
        except Exception as e:
            logger.error(f"Error processing CSV file: {str(e)}", exc_info=True)
            return 0
    
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