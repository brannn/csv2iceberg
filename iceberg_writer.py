"""
Iceberg writer module for CSV to Iceberg conversion using Polars
"""
import os
import logging
import time
import csv
from typing import Dict, List, Any, Optional, Callable, Tuple
import tempfile
import datetime

# Use Polars for data processing
import polars as pl
import pyarrow as pa

# Import PyIceberg schema
from pyiceberg.schema import Schema

from trino_client import TrinoClient
from hive_client import HiveMetastoreClient

logger = logging.getLogger(__name__)

class IcebergWriter:
    """Class for writing data to Iceberg tables"""
    
    def __init__(
        self,
        trino_client: TrinoClient,
        hive_client: HiveMetastoreClient,
        catalog: str,
        schema: str,
        table: str,
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
        
    def write_csv_to_iceberg(
        self,
        csv_file: str,
        mode: str = 'append',
        delimiter: str = ',',
        has_header: bool = True,
        quote_char: str = '"',
        batch_size: int = 10000,
        progress_callback: Optional[Callable[[int], None]] = None
    ) -> None:
        """
        Write CSV data to an Iceberg table using Polars.
        
        Args:
            csv_file: Path to the CSV file
            mode: Write mode (append or overwrite)
            delimiter: CSV delimiter character
            has_header: Whether the CSV has a header row
            quote_char: CSV quote character
            batch_size: Number of rows to process in each batch
            progress_callback: Callback function to report progress
        """
        try:
            # Get total row count for progress reporting
            total_rows = count_csv_rows(csv_file, delimiter, quote_char, has_header)
            logger.info(f"CSV file has {total_rows} rows")
            
            # Process the CSV in batches
            logger.info(f"Processing CSV file in batch mode (batch size: {batch_size})")
            logger.info(f"Write mode: {mode}")
            
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
            column_names = lazy_reader.slice(0, 10).collect().columns
            
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
            
        except Exception as e:
            logger.error(f"Error writing CSV to Iceberg: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to write CSV to Iceberg: {str(e)}")
    
    def _write_batch_to_iceberg(self, batch_data, mode: str) -> None:
        """
        Write a batch of data to an Iceberg table using Trino.
        
        Args:
            batch_data: Batch of data to write (Polars DataFrame or any compatible DataFrame with columns attribute)
            mode: Write mode (append or overwrite)
        """
        logger.info(f"Writing batch to {self.catalog}.{self.schema}.{self.table} in {mode} mode")
        
        try:
            # Get column names from the dataframe
            # Works with both Polars and Pandas DataFrames
            columns = batch_data.columns.tolist() if hasattr(batch_data.columns, 'tolist') else list(batch_data.columns)
            quoted_columns = [f'"{col}"' for col in columns]
            column_names_str = ", ".join(quoted_columns)
            
            # If running in overwrite mode, truncate the target table first
            if mode == 'overwrite' and len(batch_data) > 0:
                truncate_sql = f"DELETE FROM {self.catalog}.{self.schema}.{self.table}"
                self.trino_client.execute_query(truncate_sql)
                logger.debug(f"Truncated target table {self.table} for overwrite mode")
            
            # Get the target table schema
            try:
                target_schema = self.trino_client.get_table_schema(self.catalog, self.schema, self.table)
                column_types_dict = {col_name: col_type for col_name, col_type in target_schema}
                logger.debug(f"Retrieved target schema with types: {column_types_dict}")
            except Exception as e:
                logger.error(f"Failed to retrieve target schema: {str(e)}")
                raise RuntimeError(f"Cannot write to table without schema information: {str(e)}")
            
            # Keep batches small to avoid query size limits
            max_rows_per_insert = 50  # Reduce to max 50 rows per INSERT
            
            # Process in smaller batches
            for i in range(0, len(batch_data), max_rows_per_insert):
                # Handle both Pandas and Polars dataframes
                if hasattr(batch_data, 'iloc'):
                    # Pandas DataFrame
                    batch_slice = batch_data.iloc[i:i+max_rows_per_insert]
                else:
                    # Polars DataFrame
                    batch_slice = batch_data.slice(i, min(max_rows_per_insert, len(batch_data) - i))
                
                # Determine column types by position
                column_types_by_position = []
                
                # First, try to get column types by position from the schema metadata
                if len(column_types_dict) == len(columns):
                    # Great - we can use the dictionary directly
                    column_types_by_position = [column_types_dict.get(col, "VARCHAR") for col in columns]
                    logger.debug(f"Using column types from schema: {column_types_by_position}")
                else:
                    # Use the data itself to infer column types dynamically
                    # This is a fallback approach for when metadata is incomplete
                    logger.info("Target schema metadata unavailable - inferring types from data")
                    
                    # Sample first row to dynamically infer types
                    sample_row = None
                    
                    # Handle getting first row from different DataFrame types
                    if len(batch_slice) > 0:
                        if hasattr(batch_slice, 'iloc'):
                            # Pandas DataFrame
                            sample_row = batch_slice.iloc[0]
                        elif hasattr(batch_slice, 'row'):
                            # Polars DataFrame
                            sample_row = batch_slice.row(0)
                        elif hasattr(batch_slice, 'to_dicts'):
                            # Polars DataFrame - get first row as dict
                            sample_row = batch_slice.to_dicts()[0] if len(batch_slice.to_dicts()) > 0 else None
                    
                    for j, col in enumerate(columns):
                        # Get the value from the sample row for this column
                        try:
                            if sample_row is not None:
                                if hasattr(sample_row, '__getitem__'):
                                    # Dictionary-like row (Polars to_dicts)
                                    sample_val = sample_row[col]
                                else:
                                    # Pandas Series
                                    sample_val = sample_row[col]
                            else:
                                sample_val = None
                        except (KeyError, IndexError):
                            sample_val = None
                        
                        # Try to infer type from the value
                        if sample_val is None:
                            # Default to VARCHAR for null values
                            column_types_by_position.append("VARCHAR")
                        elif isinstance(sample_val, bool):
                            column_types_by_position.append("BOOLEAN")
                        elif isinstance(sample_val, int):
                            column_types_by_position.append("BIGINT")
                        elif isinstance(sample_val, float):
                            column_types_by_position.append("DOUBLE")
                        elif isinstance(sample_val, datetime.datetime):
                            column_types_by_position.append("TIMESTAMP(6)")
                        elif isinstance(sample_val, datetime.date):
                            column_types_by_position.append("DATE")
                        else:
                            # Try to infer type from string data
                            val_str = str(sample_val)
                            
                            # Check if it's a number
                            try:
                                if '.' in val_str:
                                    float(val_str)
                                    column_types_by_position.append("DOUBLE")
                                else:
                                    int(val_str)
                                    column_types_by_position.append("BIGINT")
                            except ValueError:
                                # Check for date formats
                                try:
                                    datetime.datetime.strptime(val_str, "%Y-%m-%d")
                                    column_types_by_position.append("DATE")
                                except ValueError:
                                    try:
                                        datetime.datetime.strptime(val_str, "%Y-%m-%d %H:%M:%S")
                                        column_types_by_position.append("TIMESTAMP(6)")
                                    except ValueError:
                                        # Default to VARCHAR
                                        column_types_by_position.append("VARCHAR")
                                        
                    logger.debug(f"Dynamically inferred column types: {column_types_by_position}")
                
                values_list = []
                
                # Handle iterating over rows based on DataFrame type
                if hasattr(batch_slice, 'iterrows'):
                    # Pandas DataFrame
                    row_iterator = batch_slice.iterrows()
                else:
                    # Polars DataFrame - convert to dictionary records
                    row_iterator = [(i, row) for i, row in enumerate(batch_slice.to_dicts())]
                
                for _, row in row_iterator:
                    row_values = []
                    for idx, col in enumerate(columns):
                        # Get value based on DataFrame type
                        if hasattr(row, '__getitem__'):
                            # Dictionary-like row (Polars to_dicts)
                            val = row[col]
                        else:
                            # Pandas Series
                            val = row[col]
                            
                        # Get type from our position-based mapping
                        target_type = column_types_by_position[idx] if idx < len(column_types_by_position) else "VARCHAR"
                        
                        if val is None:
                            row_values.append("NULL")
                        elif isinstance(val, (int, float)):
                            # Handle numeric values based on target type
                            if "DOUBLE" in target_type or "REAL" in target_type or "FLOAT" in target_type:
                                # For double columns, directly output the number without quotes
                                row_values.append(str(val))
                            elif "TIMESTAMP" in target_type:
                                # Special handling for timestamp columns
                                try:
                                    # Try to interpret number as timestamp
                                    ts = datetime.datetime.fromtimestamp(int(val))
                                    row_values.append(f"TIMESTAMP '{ts}'")
                                except (ValueError, OverflowError):
                                    # Fallback to casting as string
                                    row_values.append(f"CAST('{val}' AS {target_type})")
                            elif "INTEGER" in target_type or "BIGINT" in target_type:
                                # For integer columns, convert to int and output directly
                                row_values.append(str(int(val)))
                            else:
                                # For VARCHAR columns, wrap in quotes and cast
                                row_values.append(f"CAST('{val}' AS {target_type})")
                        elif isinstance(val, bool):
                            # Handle boolean values
                            bool_val = 'TRUE' if val else 'FALSE'
                            if target_type.startswith("VARCHAR"):
                                row_values.append(f"CAST('{bool_val}' AS {target_type})")
                            else:
                                row_values.append(f"CAST({bool_val} AS {target_type})")
                        elif isinstance(val, datetime.date) and not isinstance(val, datetime.datetime):
                            # Handle date values
                            val_str = str(val).replace("'", "''")
                            if "TIMESTAMP" in target_type:
                                # Convert date to timestamp for timestamp columns
                                row_values.append(f"TIMESTAMP '{val_str} 00:00:00'")
                            elif "DATE" in target_type:
                                row_values.append(f"DATE '{val_str}'")
                            else:
                                # Default to casting as string for other types
                                row_values.append(f"CAST('{val_str}' AS {target_type})")
                        elif isinstance(val, datetime.datetime):
                            # Handle datetime values
                            val_str = str(val).replace("'", "''")
                            if "TIMESTAMP" in target_type:
                                # Directly use TIMESTAMP literals for timestamp columns
                                row_values.append(f"TIMESTAMP '{val_str}'")
                            else:
                                # Cast to target type for other columns
                                row_values.append(f"CAST('{val_str}' AS {target_type})")
                        else:
                            # Handle string and other values
                            val_str = str(val).replace("'", "''")
                            if "VARCHAR" in target_type or target_type == "":
                                # For VARCHAR, just use string literal
                                row_values.append(f"'{val_str}'")
                            elif "BIGINT" in target_type or "INTEGER" in target_type:
                                # For integer types, try to parse as integer first
                                try:
                                    # For numeric types, try to convert and use directly
                                    val_int = int(float(val_str))
                                    row_values.append(str(val_int))
                                except (ValueError, TypeError):
                                    # If not a valid number, use explicit cast
                                    # This will let Trino handle type conversion errors
                                    row_values.append(f"CAST('{val_str}' AS {target_type})")
                            else:
                                # Otherwise explicitly cast to the target type
                                row_values.append(f"CAST('{val_str}' AS {target_type})")
                    
                    values_list.append(f"({', '.join(row_values)})")
                
                # Direct insertion with VALUES clause and explicit casts
                direct_insert_sql = f"""
                INSERT INTO {self.catalog}.{self.schema}.{self.table} ({column_names_str})
                VALUES {', '.join(values_list)}
                """
                
                self.trino_client.execute_query(direct_insert_sql)
                logger.debug(f"Inserted batch of {len(values_list)} rows with explicit casts")
            
            logger.info(f"Successfully wrote {len(batch_data)} rows to {self.catalog}.{self.schema}.{self.table}")
            
        except Exception as e:
            logger.error(f"Error in _write_batch_to_iceberg: {str(e)}", exc_info=True)
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
