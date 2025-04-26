"""
Iceberg writer module for CSV to Iceberg conversion using Polars and optimized SQL INSERTs
"""
import os
import logging
import time
import csv
from typing import Dict, List, Any, Optional, Callable, Tuple
import datetime

# Use Polars for data processing
import polars as pl

# Import PyIceberg schema
from pyiceberg.schema import Schema

# Use flat structure imports
from connectors.trino_client import TrinoClient
from core.schema_inferrer import infer_schema_from_df
from connectors.hive_client import HiveMetastoreClient
from core.query_collector import QueryCollector
from core.sql_batcher import SQLBatcher

# Configure logger
logger = logging.getLogger(__name__)

class IcebergWriter:
    """Class for writing data to Iceberg tables"""
    
    def __init__(
        self,
        trino_client: TrinoClient,
        catalog: str,
        schema: str,
        table: str,
        hive_client: Optional[HiveMetastoreClient] = None,
    ):
        """
        Initialize Iceberg writer.
        
        Args:
            trino_client: TrinoClient instance
            hive_client: HiveMetastoreClient instance
            catalog: Catalog name
            schema: Schema name
            table: Table name
        """
        self.trino_client = trino_client
        self.hive_client = hive_client
        self.catalog = catalog
        self.schema = schema
        self.table = table
        
        # Cache for target table schema to avoid repeated queries
        self._cached_target_schema = None
        self._cached_column_types_dict = None
        
        # Performance metrics and dry run results
        self.processing_stats = {}
        self.dry_run_results = None
        
    def invalidate_schema_cache(self):
        """
        Invalidate the schema cache.
        
        This method should be called when the table schema might have changed,
        such as after a DDL operation or when switching to a different table.
        """
        logger.info(f"Invalidating schema cache for {self.catalog}.{self.schema}.{self.table}")
        self._cached_target_schema = None
        self._cached_column_types_dict = None
        
    def write_csv_to_iceberg(
        self,
        csv_file: str,
        mode: str = 'append',
        delimiter: str = ',',
        has_header: bool = True,
        quote_char: str = '"',
        batch_size: int = 20000,  # Increased batch size for better performance
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[int], None]] = None,
        dry_run: bool = False,
        max_query_size: int = 700000  # Default 700KB (70% of Trino's 1MB limit)
    ) -> int:
        """
        Write CSV data to an Iceberg table using Polars.
        
        Args:
            csv_file: Path to the CSV file
            mode: Write mode (append or overwrite)
            delimiter: CSV delimiter character
            has_header: Whether the CSV has a header row
            quote_char: CSV quote character
            batch_size: Number of rows to process in each batch
            include_columns: List of column names to include (if None, include all except excluded)
            exclude_columns: List of column names to exclude (if None, no exclusions)
            progress_callback: Callback function to report progress
            dry_run: If True, collect and log queries that would be executed without actually running them
            max_query_size: Maximum SQL query size in bytes (default: 700000, 70% of Trino's 1MB limit)
        """
        try:
            # Initialize query collector for dry run mode
            query_collector = None
            if dry_run:
                query_collector = QueryCollector()
                logger.info("Running in DRY RUN mode - queries will be collected but not executed")
                
            # Get total row count for progress reporting
            total_rows = count_csv_rows(csv_file, delimiter, quote_char, has_header)
            logger.info(f"CSV file has {total_rows} rows")
            
            # Process the CSV in batches
            logger.info(f"Processing CSV file in batch mode (batch size: {batch_size})")
            logger.info(f"Write mode: {mode}")
            
            # Log column filtering parameters if provided
            if include_columns:
                logger.info(f"Including only these columns: {include_columns}")
            if exclude_columns:
                logger.info(f"Excluding these columns: {exclude_columns}")
            
            # Use Polars lazy reader with enhanced configuration for better memory efficiency and performance
            lazy_reader = pl.scan_csv(
                csv_file,
                separator=delimiter,
                has_header=has_header,
                quote_char=quote_char,
                null_values=["", "NULL", "null", "NA", "N/A", "na", "n/a", "None", "none"],
                try_parse_dates=True,
                low_memory=True,
                ignore_errors=True,
                truncate_ragged_lines=True,  # Handle CSV files with inconsistent numbers of fields
                n_rows=None,  # Process all rows
                rechunk=True,  # Rechunk for better memory layout
                row_count_name=None,  # No need for row count column
                row_count_offset=0
            )
            
            # Get column names - using slice and collect for compatibility with older Polars versions
            all_columns = lazy_reader.slice(0, 10).collect().columns
            
            # Apply column filtering
            columns_to_keep = all_columns  # Default to keeping all columns
            
            if has_header and (include_columns or exclude_columns):
                # Determine which columns to keep
                if include_columns:
                    columns_to_keep = [col for col in all_columns if col in include_columns]
                    logger.info(f"After include filtering: keeping {len(columns_to_keep)} of {len(all_columns)} columns")
                elif exclude_columns:
                    columns_to_keep = [col for col in all_columns if col not in exclude_columns]
                    logger.info(f"After exclude filtering: keeping {len(columns_to_keep)} of {len(all_columns)} columns")
                
                if len(columns_to_keep) == 0:
                    # Safety check to avoid empty schema
                    logger.error("Column filtering resulted in empty column set - using all columns instead")
                    columns_to_keep = all_columns
                
                # Apply column selection to the lazy reader
                if len(columns_to_keep) < len(all_columns):
                    lazy_reader = lazy_reader.select(columns_to_keep)
                    logger.info(f"Selected columns: {columns_to_keep}")
            
            # Update column names based on filtering
            column_names = columns_to_keep
            
            # Track progress and performance metrics
            processed_rows = 0
            last_progress = 0
            first_batch = True
            
            # Initialize batch statistics
            batch_stats = {
                "total_batches": 0,
                "total_processing_time": 0,
                "batch_sizes": [],
                "batch_times": [],
                "start_time": time.time()
            }
            
            # Process in batches using streaming approach
            for batch_start in range(0, total_rows, batch_size):
                # Define batch bounds
                batch_end = min(batch_start + batch_size, total_rows)
                batch_size_actual = batch_end - batch_start
                
                # Track batch processing time
                batch_start_time = time.time()
                
                # Get the current batch using Polars lazy evaluation
                batch = lazy_reader.slice(batch_start, batch_size_actual).collect()
                
                # Record batch read time
                batch_read_time = time.time() - batch_start_time
                
                # For first batch in overwrite mode, we need to use a different approach
                current_mode = mode if not first_batch or mode != 'overwrite' else 'overwrite'
                if first_batch:
                    first_batch = False
                
                # Write the batch to the Iceberg table directly using Polars DataFrame
                write_start_time = time.time()
                self._write_batch_to_iceberg(batch, current_mode, dry_run, query_collector, max_query_size)
                write_time = time.time() - write_start_time
                
                # Calculate total batch processing time
                batch_total_time = time.time() - batch_start_time
                
                # Update batch statistics
                batch_stats["total_batches"] += 1
                batch_stats["batch_sizes"].append(len(batch))
                batch_stats["batch_times"].append(batch_total_time)
                batch_stats["total_processing_time"] += batch_total_time
                
                # Log performance metrics for this batch
                logger.info(f"Batch {batch_stats['total_batches']}: {len(batch)} rows in {batch_total_time:.2f}s " +
                           f"(Read: {batch_read_time:.2f}s, Write: {write_time:.2f}s)")
                
                # Update progress
                processed_rows += len(batch)
                current_progress = min(100, int(processed_rows / total_rows * 100))
                
                if current_progress > last_progress:
                    last_progress = current_progress
                    if progress_callback:
                        progress_callback(current_progress)
                    logger.info(f"Progress: {current_progress}%")
            
            # Final update if needed
            if last_progress < 100 and progress_callback:
                progress_callback(100)
                logger.info("Progress: 100%")
            
            # Calculate final performance metrics
            total_elapsed_time = time.time() - batch_stats["start_time"]
            avg_batch_size = sum(batch_stats["batch_sizes"]) / batch_stats["total_batches"] if batch_stats["total_batches"] > 0 else 0
            avg_batch_time = sum(batch_stats["batch_times"]) / batch_stats["total_batches"] if batch_stats["total_batches"] > 0 else 0
            processing_rate = processed_rows / total_elapsed_time if total_elapsed_time > 0 else 0
            
            # Store processing statistics for later access
            self.processing_stats = {
                "total_rows": processed_rows,
                "total_batches": batch_stats["total_batches"],
                "total_processing_time": total_elapsed_time,
                "avg_batch_size": avg_batch_size,
                "avg_batch_time": avg_batch_time,
                "processing_rate": processing_rate,  # rows per second
                "batch_sizes": batch_stats["batch_sizes"],
                "batch_times": batch_stats["batch_times"]
            }
            
            # Log performance summary
            logger.info(f"Performance summary: {processed_rows} rows in {total_elapsed_time:.2f}s " +
                       f"({processing_rate:.2f} rows/sec)")
            logger.info(f"Batch statistics: {batch_stats['total_batches']} batches, " +
                       f"avg size: {avg_batch_size:.1f} rows, avg time: {avg_batch_time:.3f}s")
            
            # For dry run mode, store the query_collector results and log a summary
            if dry_run and query_collector:
                query_collector.log_summary()
                # Store the query collector for later access
                self.dry_run_results = query_collector.get_full_report()
                # In dry run mode, return the total number of rows that would have been processed
                logger.info(f"DRY RUN completed - would have processed {processed_rows} rows")
                return processed_rows
            else:
                logger.info(f"Successfully wrote {processed_rows} rows to {self.catalog}.{self.schema}.{self.table}")
                return processed_rows
            
        except Exception as e:
            logger.error(f"Error writing CSV to Iceberg: {str(e)}", exc_info=True)
            # Need to raise to maintain the same behavior, but ensure all paths return an int
            # This will never be reached because of the raise, but it keeps the type checker happy
            raise RuntimeError(f"Failed to write CSV to Iceberg: {str(e)}")
            return 0  # Will never reach here due to the raise
    
    def _write_batch_to_iceberg(self, batch_data, mode: str, dry_run: bool = False, query_collector = None, max_query_size: int = 700000) -> None:
        """
        Write a batch of data to an Iceberg table using optimized SQL INSERT statements.
        
        Args:
            batch_data: Batch of data to write (Polars DataFrame or any compatible DataFrame with columns attribute)
            mode: Write mode (append or overwrite)
            dry_run: If True, collect queries without executing them
            query_collector: QueryCollector instance for storing queries in dry run mode
            max_query_size: Maximum SQL query size in bytes (default: 700000, 70% of Trino's 1MB limit)
        """
        start_time = time.time()
        try:
            batch_size = len(batch_data) if hasattr(batch_data, '__len__') else 'unknown'
            logger.info(f"Writing batch of {batch_size} rows to {self.catalog}.{self.schema}.{self.table} in {mode} mode")
        except Exception as e:
            # This should never happen but makes logging more robust
            logger.warning(f"Could not determine batch size: {str(e)}")
            logger.info(f"Writing batch to {self.catalog}.{self.schema}.{self.table} in {mode} mode")
        
        try:
            # Ensure we're working with a Polars DataFrame for consistency
            if not isinstance(batch_data, pl.DataFrame):
                try:
                    # Try to convert to Polars if it's another DataFrame type
                    if hasattr(batch_data, 'to_dict') and callable(batch_data.to_dict):
                        # Convert from Pandas
                        logger.info("Converting Pandas DataFrame to Polars")
                        batch_data = pl.DataFrame(batch_data.to_dict('list'))
                    else:
                        logger.warning("Unknown DataFrame type, attempting to convert to Polars")
                        # Generic fallback
                        batch_data = pl.DataFrame(batch_data)
                except Exception as e:
                    logger.error(f"Failed to convert to Polars DataFrame: {str(e)}")
                    raise RuntimeError(f"Unsupported DataFrame type: {type(batch_data)}")
            
            # Get column names from the dataframe
            columns = batch_data.columns
            
            # Import clean_column_name function from utils for consistent cleaning
            from utils import clean_column_name
            
            # Clean column names for SQL compatibility
            cleaned_columns = []
            for col in columns:
                # Apply consistent column name cleaning
                clean_col = clean_column_name(col)
                cleaned_columns.append(clean_col)
            
            # Update the batch DataFrame with cleaned column names if needed
            if any(orig != cleaned for orig, cleaned in zip(columns, cleaned_columns)):
                # Create a mapping from original to cleaned column names
                column_mapping = {orig: cleaned for orig, cleaned in zip(columns, cleaned_columns)}
                batch_data = batch_data.rename(column_mapping)
                logger.debug(f"Renamed DataFrame columns for SQL compatibility: {column_mapping}")
                
            # Update column references
            columns = cleaned_columns
            quoted_columns = [f'"{col}"' for col in columns]
            column_names_str = ", ".join(quoted_columns)
            
            # Check if table exists for both append and overwrite modes
            table_exists = self.trino_client.table_exists(self.catalog, self.schema, self.table)
            logger.info(f"Table {self.catalog}.{self.schema}.{self.table} exists: {table_exists}")
            
            # Special handling for overwrite mode when table exists
            if table_exists and mode == 'overwrite' and len(batch_data) > 0:
                try:
                    # If table exists and we're in overwrite mode, truncate it
                    truncate_sql = f"DELETE FROM {self.catalog}.{self.schema}.{self.table}"
                    
                    if dry_run and query_collector:
                        # In dry run mode, just collect the query
                        query_collector.add_query(truncate_sql, "DDL", 0, f"{self.catalog}.{self.schema}.{self.table}")
                        logger.debug(f"[DRY RUN] Would execute: {truncate_sql}")
                    else:
                        # Normal execution
                        self.trino_client.execute_query(truncate_sql)
                        logger.debug(f"Truncated target table {self.table} for overwrite mode")
                    
                    # Invalidate schema cache after table truncation in case of schema changes
                    self.invalidate_schema_cache()
                    logger.debug("Schema cache invalidated after table truncation")
                except Exception as e:
                    # If truncate fails (e.g., due to table permissions), log the error
                    # but continue with the process - we'll attempt to write data anyway
                    logger.warning(f"Failed to truncate table for overwrite mode: {str(e)}")
                    logger.warning("Will continue with insert operation")
            
            # Handle table creation if it doesn't exist
            if not table_exists:
                logger.info(f"Table {self.catalog}.{self.schema}.{self.table} doesn't exist yet, creating it from batch data")
                
                # Infer schema from batch data
                iceberg_schema = infer_schema_from_df(batch_data)
                
                # Create the table (or collect the DDL in dry run mode)
                if dry_run and query_collector:
                    # Get the create table SQL without executing it
                    create_table_sql = self.trino_client.get_create_table_sql(
                        self.catalog, self.schema, self.table, iceberg_schema
                    )
                    query_collector.add_query(create_table_sql, "DDL", 0, f"{self.catalog}.{self.schema}.{self.table}")
                    logger.info(f"[DRY RUN] Would create table: {self.catalog}.{self.schema}.{self.table}")
                    logger.debug(f"[DRY RUN] Would execute: {create_table_sql}")
                else:
                    # Normal execution
                    self.trino_client.create_iceberg_table(self.catalog, self.schema, self.table, iceberg_schema)
                    logger.info(f"Created table {self.catalog}.{self.schema}.{self.table}")
                
                # Set empty schema to force dynamic inference for the first batch
                self._cached_target_schema = []
                self._cached_column_types_dict = {}
            
            # Use optimized SQL INSERT method
            logger.info("Using optimized SQL INSERT method for data loading")
            
            # Get the target table schema for improved data type handling
            try:
                # Check if schema is already cached
                if self._cached_target_schema is None:
                    logger.info(f"Fetching and caching schema for {self.catalog}.{self.schema}.{self.table}")
                    self._cached_target_schema = self.trino_client.get_table_schema(self.catalog, self.schema, self.table)
                    
                    # Create dictionary only if we got valid schema results
                    if self._cached_target_schema is not None and len(self._cached_target_schema) > 0:
                        self._cached_column_types_dict = {col_name: col_type for col_name, col_type in self._cached_target_schema}
                        logger.debug(f"Cached schema with types: {self._cached_column_types_dict}")
                    else:
                        # If schema retrieval returned empty result, initialize empty dict
                        self._cached_column_types_dict = {}
                        logger.warning(f"Retrieved empty schema for {self.catalog}.{self.schema}.{self.table}")
                else:
                    logger.debug(f"Using cached schema for {self.catalog}.{self.schema}.{self.table}")
            except Exception as e:
                logger.warning(f"Failed to retrieve schema: {str(e)}")
                self._cached_column_types_dict = {}
            
            # Process in optimized batches
            self._write_batch_to_iceberg_sql(batch_data, mode, dry_run, query_collector, max_query_size)
            
            # Log execution time for this batch
            elapsed_time = time.time() - start_time
            if dry_run:
                logger.info(f"SQL INSERT batch processing (dry run) completed in {elapsed_time:.2f} seconds")
            else:
                logger.info(f"SQL INSERT batch processing completed in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error in _write_batch_to_iceberg: {str(e)}", exc_info=True)
            
            # Check if this is overwrite mode, and we should invalidate the schema cache
            if mode == 'overwrite':
                logger.info("Invalidating schema cache due to error in overwrite mode")
                self.invalidate_schema_cache()
            
            raise RuntimeError(f"Failed to write batch to Iceberg: {str(e)}")
            
    def _write_batch_to_iceberg_sql(self, batch_data, mode: str, dry_run: bool = False, query_collector = None, max_query_size: int = 700000) -> None:
        """
        High-performance method to write batch data using optimized SQL INSERT statements with SQLBatcher.
        
        Args:
            batch_data: Batch of data to write (Polars DataFrame)
            mode: Write mode (append or overwrite)
            dry_run: If True, collect queries without executing them
            query_collector: QueryCollector instance for storing queries in dry run mode
            max_query_size: Maximum SQL query size in bytes (default: 700000, 70% of Trino's 1MB limit)
        """
        logger.info(f"Using optimized SQL INSERT method with SQLBatcher for batch of {len(batch_data)} rows")
        
        try:
            # Get column names from the dataframe
            columns = batch_data.columns
            quoted_columns = [f'"{col}"' for col in columns]
            column_names_str = ", ".join(quoted_columns)
            
            # Define maximum SQL query size (in characters) - Trino has a limit of 1,000,000
            # Use the passed max_query_size parameter instead of hardcoded value
            MAX_QUERY_LENGTH = max_query_size  # Default is 700KB (70% of Trino's limit)
            
            # Initialize row processing counter
            rows_processed = 0
            
            # Base SQL part
            base_sql = f"INSERT INTO {self.catalog}.{self.schema}.{self.table} ({column_names_str}) VALUES "
            
            # Create a SQL batcher instance with a safer limit
            sql_batcher = SQLBatcher(max_bytes=MAX_QUERY_LENGTH, dry_run=dry_run)
            
            # Define a callback function for the SQL batcher to execute queries
            def execute_callback(query_sql):
                self.trino_client.execute_query(query_sql)
            
            # Format all rows in the batch using a simpler approach with SQLBatcher
            formatted_rows = []
            for row in batch_data.rows(named=True):
                # Format row values
                row_values = []
                
                for col in columns:
                    val = row[col]
                    if val is None:
                        row_values.append("NULL")
                    elif isinstance(val, bool):
                        row_values.append("TRUE" if val else "FALSE")
                    elif isinstance(val, (int, float)):
                        row_values.append(str(val))
                    elif isinstance(val, datetime.datetime):
                        # Format timestamp properly for Trino
                        row_values.append(f"TIMESTAMP '{val}'")
                    elif isinstance(val, datetime.date):
                        row_values.append(f"DATE '{val}'")
                    else:
                        # Handle string values with proper escaping
                        str_val = str(val).replace("'", "''")
                        row_values.append(f"'{str_val}'")
                
                formatted_rows.append(f"({', '.join(row_values)})")
            
            # Prepare SQL INSERT statements with a reduced max size
            # Calculate average row size to determine batch size
            MAX_ROWS_PER_INSERT = 500  # Start with a safe limit
            
            if formatted_rows:
                # Calculate average row size
                avg_row_size = sum(len(row.encode('utf-8')) for row in formatted_rows) / len(formatted_rows)
                # Use the provided max_query_size
                max_safe_query_size = max_query_size
                # Base SQL part size 
                base_sql_size = len(base_sql.encode('utf-8'))
                # Calculate how many rows we can safely fit
                max_safe_rows = int((max_safe_query_size - base_sql_size) / (avg_row_size + 2))  # +2 for comma and space
                # Use the smaller of our calculated value or default
                batch_size = min(max_safe_rows, MAX_ROWS_PER_INSERT)
                batch_size = max(batch_size, 1)  # Ensure at least 1 row per batch
                
                logger.info(f"Calculated batch size: {batch_size} rows per INSERT (avg row size: {avg_row_size:.0f} bytes)")
            else:
                batch_size = MAX_ROWS_PER_INSERT
            
            # Create multiple INSERT statements with smaller row batches
            insert_statements = []
            for i in range(0, len(formatted_rows), batch_size):
                batch = formatted_rows[i:i + batch_size]
                values_sql = ", ".join(batch)
                insert_sql = f"{base_sql}{values_sql}"
                insert_statements.append(insert_sql)
            
            # Define metadata for the query collector
            metadata = {
                "type": "DML",
                "row_count": len(formatted_rows),
                "table_name": f"{self.catalog}.{self.schema}.{self.table}"
            }
            
            # Process all statements using the SQLBatcher for optimal batching
            rows_processed = 0
            if dry_run and query_collector:
                # In dry run mode
                rows_processed = sql_batcher.process_statements(
                    insert_statements,
                    execute_callback,
                    query_collector,
                    metadata
                )
                logger.info(f"[DRY RUN] Would insert {len(batch_data)} rows to {self.catalog}.{self.schema}.{self.table}")
            else:
                # Normal execution
                try:
                    rows_processed = sql_batcher.process_statements(
                        insert_statements, 
                        execute_callback
                    )
                    logger.info(f"Successfully inserted {rows_processed} rows to {self.catalog}.{self.schema}.{self.table} using SQL batcher")
                except Exception as e:
                    logger.error(f"Error during SQL INSERT: {str(e)}", exc_info=True)
                    raise RuntimeError(f"Failed to write data to Iceberg table: {str(e)}")
            
            logger.info(f"Successfully processed {rows_processed} rows using SQLBatcher")
        except Exception as e:
            logger.error(f"Error in SQL INSERT method: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to write data using SQL INSERT: {str(e)}")

def count_csv_rows(
    csv_file: str, 
    delimiter: str = ',', 
    quote_char: str = '"',
    has_header: bool = True
) -> int:
    """
    Count the number of rows in a CSV file using optimized methods.
    
    Args:
        csv_file: Path to the CSV file
        delimiter: CSV delimiter character
        quote_char: CSV quote character
        has_header: Whether the CSV has a header row
        
    Returns:
        Number of rows in the CSV file
    """
    try:
        # Try fast method first - file size based estimation with sampling
        file_size = os.path.getsize(csv_file)
        
        # For small files (< 10MB), just use the full scan approach
        if file_size < 10 * 1024 * 1024:  # 10MB
            # Use Polars scan_csv with optimized settings for row counting
            row_count = pl.scan_csv(
                csv_file,
                separator=delimiter,
                has_header=has_header,
                quote_char=quote_char,
                null_values=["", "NULL", "null", "NA", "N/A", "na", "n/a", "None", "none"],
                low_memory=True,
                ignore_errors=True,
                truncate_ragged_lines=True,  # Handle CSV files with inconsistent numbers of fields
                rechunk=True
            ).select(pl.count()).collect().item()
            
            logger.info(f"Counted {row_count} rows in CSV file using full scan")
            return row_count
        
        # For larger files, use a line-based approach for better performance
        logger.info(f"Using optimized row counting for large file ({file_size/1024/1024:.1f} MB)")
        
        # Open the file in binary mode for better performance
        with open(csv_file, 'rb') as f:
            # Count newlines
            line_count = sum(1 for _ in f.readlines())
            
        # Adjust for header if needed
        row_count = line_count - 1 if has_header else line_count
        logger.info(f"Counted {row_count} rows in CSV file using optimized method")
        return row_count
        
    except Exception as e:
        # Fallback to simple line counting if faster approaches fail
        logger.warning(f"Error in optimized row counting: {str(e)}")
        try:
            with open(csv_file, 'r') as f:
                line_count = sum(1 for _ in f)
                result = line_count - 1 if has_header else line_count
                logger.info(f"Counted {result} rows using fallback method")
                return result
        except Exception as e2:
            logger.error(f"Error counting CSV rows: {str(e2)}", exc_info=True)
            raise RuntimeError(f"Failed to count CSV rows: {str(e2)}")
