"""
Iceberg writer module for CSV to Iceberg conversion using Polars
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
        batch_size: int = 20000,  # Increased default batch size from 10000 to 20000
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
            
            # Use Polars lazy reader for better memory efficiency
            lazy_reader = pl.scan_csv(
                csv_file,
                separator=delimiter,
                has_header=has_header,
                quote_char=quote_char,
                null_values=["", "NULL", "null", "NA", "N/A", "na", "n/a", "None", "none"],
                try_parse_dates=True,
                low_memory=True,
                ignore_errors=True
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
        Write a batch of data to an Iceberg table using direct SQL INSERT statements.
        
        Args:
            batch_data: Batch of data to write (Polars DataFrame or any compatible DataFrame with columns attribute)
            mode: Write mode (append or overwrite)
        """
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
            
            # For append mode on a non-existent table, we'll create the table below
            # For overwrite mode on a non-existent table, we skip the truncate step
            
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
            
            # For both append and overwrite modes with non-existent tables
            if not table_exists:
                logger.info(f"Table {self.catalog}.{self.schema}.{self.table} doesn't exist, will be created automatically")
            
            # Get the target table schema (using cache if available)
            try:
                # Check if table exists first to avoid errors when trying to get schema
                table_exists = self.trino_client.table_exists(self.catalog, self.schema, self.table)
                
                if not table_exists:
                    logger.info(f"Table {self.catalog}.{self.schema}.{self.table} doesn't exist yet, creating it from batch data")
                    
                    # Create the table using inferred schema from batch data
                    # Use batch data to create a PyIceberg schema
                    # Using the infer_schema_from_df function imported at the top of the file
                    
                    # Infer schema from batch data
                    iceberg_schema = infer_schema_from_df(batch_data)
                    
                    # Create the table
                    self.trino_client.create_iceberg_table(self.catalog, self.schema, self.table, iceberg_schema)
                    logger.info(f"Created table {self.catalog}.{self.schema}.{self.table}")
                    
                    # Set empty schema to force dynamic inference for the first batch
                    self._cached_target_schema = []
                    self._cached_column_types_dict = {}
                    target_schema = []
                    column_types_dict = {}
                else:
                    # Table exists, get its schema
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
                    
                    # Use the cached schema information, ensuring they're never None
                    target_schema = self._cached_target_schema if self._cached_target_schema is not None else []
                    column_types_dict = self._cached_column_types_dict if self._cached_column_types_dict is not None else {}
            except Exception as e:
                logger.error(f"Failed to retrieve or create target schema: {str(e)}", exc_info=True)
                
                # Initialize empty schema info to allow fallback to dynamic type inference
                target_schema = []
                column_types_dict = {}
                logger.warning(f"Will use dynamic schema inference due to schema retrieval error")
            
            # Define maximum rows per batch to avoid memory issues
            # The maximum batch size depends on the column count
            column_count = len(columns)
            max_rows_per_batch = 10000  # Default batch size
            
            # Adjust batch size for wide tables
            if column_count > 50:
                max_rows_per_batch = 2500
            elif column_count > 30:
                max_rows_per_batch = 5000
            
            logger.info(f"Using batch size of {max_rows_per_batch} rows for table with {column_count} columns")
            
            # Process data in batches to optimize memory usage
            total_rows = len(batch_data)
            for i in range(0, total_rows, max_rows_per_batch):
                # Create batch slice
                batch_slice = batch_data.slice(i, min(max_rows_per_batch, total_rows - i))
                
                # Create SQL VALUES clause for direct insertion
                values_list = []
                
                # Iterate through the rows of the batch to build VALUES lists
                for row_idx, row in enumerate(batch_slice.rows(named=True)):
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
                    
                    values_list.append(f"({', '.join(row_values)})")
                
                # Build the full INSERT statement
                insert_sql = f"""
                INSERT INTO {self.catalog}.{self.schema}.{self.table} ({column_names_str})
                VALUES {', '.join(values_list)}
                """
                
                # Execute the SQL statement
                try:
                    logger.debug(f"Executing SQL INSERT with {len(values_list)} rows of data")
                    self.trino_client.execute_query(insert_sql)
                    logger.info(f"Successfully inserted batch of {len(batch_slice)} rows")
                except Exception as e:
                    logger.error(f"Error during SQL INSERT: {str(e)}", exc_info=True)
                    
                    # Provide more detailed error information
                    error_msg = str(e).lower()
                    if "type mismatch" in error_msg:
                        logger.error("Type mismatch detected - column types may not match target table schema")
                    elif "table not found" in error_msg:
                        logger.error("Table not found - the target table may have been dropped during the operation")
                    
                    raise RuntimeError(f"Failed to write data to Iceberg table: {str(e)}")
            
            logger.info(f"Successfully wrote {len(batch_data)} rows to {self.catalog}.{self.schema}.{self.table}")
            
        except Exception as e:
            logger.error(f"Error in _write_batch_to_iceberg: {str(e)}", exc_info=True)
            
            # Check if this is overwrite mode, and we should invalidate the schema cache
            if mode == 'overwrite':
                logger.info("Invalidating schema cache due to error in overwrite mode")
                self.invalidate_schema_cache()
            
            raise RuntimeError(f"Failed to write batch to Iceberg: {str(e)}")

def count_csv_rows(
    csv_file: str, 
    delimiter: str = ',', 
    quote_char: str = '"',
    has_header: bool = True
) -> int:
    """
    Count the number of rows in a CSV file using Polars.
    
    Args:
        csv_file: Path to the CSV file
        delimiter: CSV delimiter character
        quote_char: CSV quote character
        has_header: Whether the CSV has a header row
        
    Returns:
        Number of rows in the CSV file
    """
    try:
        # Use Polars scan_csv for efficient row counting without loading the entire file
        row_count = pl.scan_csv(
            csv_file,
            separator=delimiter,
            has_header=has_header,
            quote_char=quote_char,
            null_values=["", "NULL", "null", "NA", "N/A", "na", "n/a", "None", "none"],
            low_memory=True,
            ignore_errors=True
        ).select(pl.count()).collect().item()
        
        # Return the count (if has_header is True, the header row is already excluded)
        return row_count
    except Exception as e:
        # Fallback to simple line counting if Polars approach fails
        logger.warning(f"Error using Polars to count CSV rows: {str(e)}")
        try:
            with open(csv_file, 'r') as f:
                line_count = sum(1 for _ in f)
                return line_count - 1 if has_header else line_count
        except Exception as e2:
            logger.error(f"Error counting CSV rows: {str(e2)}", exc_info=True)
            raise RuntimeError(f"Failed to count CSV rows: {str(e2)}")
