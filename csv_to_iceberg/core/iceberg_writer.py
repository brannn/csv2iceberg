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

# Use package relative imports
from csv_to_iceberg.connectors.trino_client import TrinoClient
from csv_to_iceberg.core.schema_inferrer import infer_schema_from_df
from csv_to_iceberg.connectors.hive_client import HiveMetastoreClient

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
        progress_callback: Optional[Callable[[int], None]] = None
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
        """
        try:
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
            
            # Track progress
            processed_rows = 0
            last_progress = 0
            first_batch = True
            
            # Process in batches using streaming approach
            for batch_start in range(0, total_rows, batch_size):
                # Define batch bounds
                batch_end = min(batch_start + batch_size, total_rows)
                batch_size_actual = batch_end - batch_start
                
                # Get the current batch using Polars lazy evaluation
                batch = lazy_reader.slice(batch_start, batch_size_actual).collect()
                
                # For first batch in overwrite mode, we need to use a different approach
                current_mode = mode if not first_batch or mode != 'overwrite' else 'overwrite'
                if first_batch:
                    first_batch = False
                
                # Write the batch to the Iceberg table directly using Polars DataFrame
                self._write_batch_to_iceberg(batch, current_mode)
                
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
                
            logger.info(f"Successfully wrote {processed_rows} rows to {self.catalog}.{self.schema}.{self.table}")
            return processed_rows
            
        except Exception as e:
            logger.error(f"Error writing CSV to Iceberg: {str(e)}", exc_info=True)
            # Need to raise to maintain the same behavior, but ensure all paths return an int
            # This will never be reached because of the raise, but it keeps the type checker happy
            raise RuntimeError(f"Failed to write CSV to Iceberg: {str(e)}")
            return 0  # Will never reach here due to the raise
    
    def _write_batch_to_iceberg(self, batch_data, mode: str) -> None:
        """
        Write a batch of data to an Iceberg table using optimized SQL INSERT statements.
        
        Args:
            batch_data: Batch of data to write (Polars DataFrame or any compatible DataFrame with columns attribute)
            mode: Write mode (append or overwrite)
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
            
            # Clean column names to replace spaces with underscores
            cleaned_columns = []
            for col in columns:
                # Replace spaces with underscores for SQL compatibility
                clean_col = col.replace(' ', '_') if isinstance(col, str) else str(col).replace(' ', '_')
                cleaned_columns.append(clean_col)
            
            # Update the batch DataFrame with cleaned column names if needed
            if any(orig != cleaned for orig, cleaned in zip(columns, cleaned_columns)):
                # Create a mapping from original to cleaned column names
                column_mapping = {orig: cleaned for orig, cleaned in zip(columns, cleaned_columns)}
                batch_data = batch_data.rename(column_mapping)
                logger.debug(f"Renamed DataFrame columns for SQL compatibility")
                
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
                
                # Create the table
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
            self._write_batch_to_iceberg_sql(batch_data, mode)
            
            # Log execution time for this batch
            elapsed_time = time.time() - start_time
            logger.info(f"SQL INSERT batch processing completed in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error in _write_batch_to_iceberg: {str(e)}", exc_info=True)
            
            # Check if this is overwrite mode, and we should invalidate the schema cache
            if mode == 'overwrite':
                logger.info("Invalidating schema cache due to error in overwrite mode")
                self.invalidate_schema_cache()
            
            raise RuntimeError(f"Failed to write batch to Iceberg: {str(e)}")
            
    def _write_batch_to_iceberg_sql(self, batch_data, mode: str) -> None:
        """
        High-performance method to write batch data using optimized SQL INSERT statements.
        
        Args:
            batch_data: Batch of data to write (Polars DataFrame)
            mode: Write mode (append or overwrite)
        """
        logger.info(f"Using optimized SQL INSERT method for batch of {len(batch_data)} rows")
        
        try:
            # Get column names from the dataframe
            columns = batch_data.columns
            quoted_columns = [f'"{col}"' for col in columns]
            column_names_str = ", ".join(quoted_columns)
            
            # Define parameters based on column count to optimize performance
            column_count = len(columns)
            
            # Define maximum SQL query size (in characters) - Trino has a limit of 1,000,000
            MAX_QUERY_LENGTH = 900000  # Setting a bit below the limit for safety
            
            # Define maximum rows per INSERT statement based on column count
            if column_count > 80:
                MAX_ROWS_PER_INSERT = 1000
            elif column_count > 50:
                MAX_ROWS_PER_INSERT = 2000
            elif column_count > 30:
                MAX_ROWS_PER_INSERT = 3000
            else:
                # For tables with fewer columns, we can process more rows per statement
                MAX_ROWS_PER_INSERT = 5000
            
            logger.info(f"Using maximum of {MAX_ROWS_PER_INSERT} rows per INSERT statement")
            
            # Process batch in chunks
            rows_to_process = list(batch_data.rows(named=True))
            current_chunk = []
            current_query_length = 0
            rows_processed = 0
            
            # Base SQL part
            base_sql = f"INSERT INTO {self.catalog}.{self.schema}.{self.table} ({column_names_str}) VALUES "
            base_length = len(base_sql)
            
            # Process rows
            for row in rows_to_process:
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
                
                row_sql = f"({', '.join(row_values)})"
                row_length = len(row_sql) + (2 if current_chunk else 0)  # +2 for comma and space
                
                # Check if adding this row would exceed query limits
                new_query_length = current_query_length + row_length
                
                # Execute current batch if needed
                if ((new_query_length + base_length > MAX_QUERY_LENGTH or 
                     len(current_chunk) >= MAX_ROWS_PER_INSERT) and current_chunk):
                    
                    # Build and execute the current chunk
                    insert_sql = f"{base_sql}{', '.join(current_chunk)}"
                    
                    try:
                        logger.debug(f"Executing SQL INSERT with {len(current_chunk)} rows (query length: {len(insert_sql)})")
                        self.trino_client.execute_query(insert_sql)
                        rows_processed += len(current_chunk)
                        logger.info(f"Successfully inserted chunk of {len(current_chunk)} rows")
                    except Exception as e:
                        logger.error(f"Error during SQL INSERT: {str(e)}", exc_info=True)
                        raise RuntimeError(f"Failed to write data to Iceberg table: {str(e)}")
                    
                    # Reset for next batch
                    current_chunk = []
                    current_query_length = 0
                
                # Add the current row to the chunk
                current_chunk.append(row_sql)
                current_query_length = new_query_length
            
            # Process any remaining rows in the final chunk
            if current_chunk:
                insert_sql = f"{base_sql}{', '.join(current_chunk)}"
                
                try:
                    logger.debug(f"Executing final SQL INSERT with {len(current_chunk)} rows (query length: {len(insert_sql)})")
                    self.trino_client.execute_query(insert_sql)
                    rows_processed += len(current_chunk)
                    logger.info(f"Successfully inserted final chunk of {len(current_chunk)} rows")
                except Exception as e:
                    logger.error(f"Error during SQL INSERT: {str(e)}", exc_info=True)
                    raise RuntimeError(f"Failed to write data to Iceberg table: {str(e)}")
            
            logger.info(f"Successfully wrote {rows_processed} rows to {self.catalog}.{self.schema}.{self.table} using SQL INSERT method")
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
