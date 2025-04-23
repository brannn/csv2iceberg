"""
Iceberg writer module for CSV to Iceberg conversion
"""
import os
import logging
import time
import csv
from typing import Dict, List, Any, Optional, Callable, Tuple
import tempfile
import datetime
import pandas as pd

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
        Write CSV data to an Iceberg table.
        
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
            
            # Read the CSV in batches with error handling for inconsistent field counts
            # Note: Empty fields are properly handled and converted to NULL values
            chunk_iter = pd.read_csv(
                csv_file, 
                delimiter=delimiter, 
                quotechar=quote_char,
                header=0 if has_header else None,
                chunksize=batch_size,
                on_bad_lines='skip',   # Skip only lines with inconsistent number of fields
                keep_default_na=True,  # Convert empty fields to NaN (which become NULL)
                na_values=['', 'NULL', 'null', 'NA', 'N/A', 'na', 'n/a', 'None', 'none']  # Additional values to treat as NULL
            )
            
            # Track progress
            processed_rows = 0
            last_progress = 0
            first_batch = True
            
            # Process each batch
            for chunk in chunk_iter:
                # For first batch in overwrite mode, we need to use a different approach
                current_mode = mode if not first_batch or mode != 'overwrite' else 'overwrite'
                if first_batch:
                    first_batch = False
                
                # Write the batch to the Iceberg table
                self._write_batch_to_iceberg(chunk, current_mode)
                
                # Update progress
                processed_rows += len(chunk)
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
            batch_data: Batch of data to write
            mode: Write mode (append or overwrite)
        """
        logger.info(f"Writing batch to {self.catalog}.{self.schema}.{self.table} in {mode} mode")
        
        try:
            # Get column names from the dataframe
            columns = batch_data.columns.tolist()
            quoted_columns = [f'"{col}"' for col in columns]
            column_names_str = ", ".join(quoted_columns)
            
            # If running in overwrite mode, truncate the target table first
            if mode == 'overwrite' and batch_data.shape[0] > 0:
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
            
            # Skip using temporary tables and PyArrow - directly craft INSERT with explicit CAST
            max_rows_per_insert = 100  # Keep batches small to avoid query size limits
            
            # Make the batch size even smaller to avoid query size limits
            max_rows_per_insert = min(max_rows_per_insert, 50)  # Reduce to max 50 rows per INSERT
            
            # Process in smaller batches
            for i in range(0, len(batch_data), max_rows_per_insert):
                batch_slice = batch_data.iloc[i:i+max_rows_per_insert]
                
                # Looking at the error message, we need exact column-by-column type matching
                # Create a static column index mapping based on the error message
                # Target schema: [varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, double, double, timestamp(6), timestamp(6), varchar, varchar, double, double, varchar, varchar, varchar, varchar, double, varchar, varchar, varchar]
                
                column_types_by_position = []
                
                # First, try to get column types by position from the schema metadata
                if len(column_types_dict) == len(columns):
                    # Great - we can use the dictionary directly
                    column_types_by_position = [column_types_dict.get(col, "VARCHAR") for col in columns]
                    logger.debug(f"Using column types from schema: {column_types_by_position}")
                else:
                    # We need to manually map types based on what we know from the error message
                    # This is a fallback approach for when metadata is incomplete
                    for i, col in enumerate(columns):
                        if i >= 0 and i <= 7:
                            column_types_by_position.append("VARCHAR")
                        elif i == 8 or i == 9 or i == 14 or i == 15 or i == 20:
                            column_types_by_position.append("DOUBLE")
                        elif i == 10 or i == 11:
                            column_types_by_position.append("TIMESTAMP(6)")
                        else:
                            column_types_by_position.append("VARCHAR")
                    
                    logger.debug(f"Using fallback column types mapping: {column_types_by_position}")
                
                values_list = []
                for _, row in batch_slice.iterrows():
                    row_values = []
                    for idx, col in enumerate(columns):
                        val = row[col]
                        # Get type from our position-based mapping
                        target_type = column_types_by_position[idx] if idx < len(column_types_by_position) else "VARCHAR"
                        
                        if pd.isna(val):
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
    Count the number of rows in a CSV file.
    
    Args:
        csv_file: Path to the CSV file
        delimiter: CSV delimiter character
        quote_char: CSV quote character
        has_header: Whether the CSV has a header row
        
    Returns:
        Number of rows in the CSV file
    """
    try:
        # For CSV files with inconsistent field counts, we need a more robust approach
        # Rather than counting raw lines, we'll use pandas to determine valid rows
        df_chunks = pd.read_csv(
            csv_file,
            delimiter=delimiter,
            quotechar=quote_char,
            header=0 if has_header else None,
            chunksize=10000,  # Use a reasonable chunk size
            on_bad_lines='skip',   # Skip only lines with inconsistent number of fields
            keep_default_na=True,  # Convert empty fields to NaN (which become NULL)
            na_values=['', 'NULL', 'null', 'NA', 'N/A', 'na', 'n/a', 'None', 'none']  # Additional values to treat as NULL
        )
        
        # Count rows across all chunks
        total_rows = sum(len(chunk) for chunk in df_chunks)
        
        # If there are rows and we're not counting the header, return as is
        # If counting with header, we already skipped it by using header=0
        return total_rows
    except Exception as e:
        # Fallback to simple line counting if pandas approach fails
        logger.warning(f"Error using pandas to count CSV rows: {str(e)}")
        try:
            with open(csv_file, 'r') as f:
                line_count = sum(1 for _ in f)
                return line_count - 1 if has_header else line_count
        except Exception as e2:
            logger.error(f"Error counting CSV rows: {str(e2)}", exc_info=True)
            raise RuntimeError(f"Failed to count CSV rows: {str(e2)}")
