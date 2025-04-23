"""
Iceberg writer module for CSV to Iceberg conversion
"""
import os
import logging
import time
import csv
from typing import Dict, List, Any, Optional, Callable, Tuple
import tempfile
from datetime import datetime
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
            column_names_str = ", ".join([f'"{col}"' for col in columns])
            
            # Create temporary table name with timestamp to avoid conflicts
            temp_table = f"temp_{self.table}_{int(time.time())}"
            temp_csv = f"temp_{self.table}_{int(time.time())}.csv"
            
            # Save this batch to a temporary CSV file
            temp_csv_path = os.path.join(os.getcwd(), temp_csv)
            batch_data.to_csv(temp_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
            logger.debug(f"Saved batch to temporary CSV: {temp_csv_path}")
            
            # Create a temporary external table directly from the CSV file
            # This approach bypasses the type conversion issues by letting Trino do the type inference
            schema_definitions = []
            for col in columns:
                schema_definitions.append(f'"{col}" varchar')
                
            schema_def_str = ", ".join(schema_definitions)
            
            # Create external table definition for the CSV file
            create_external_table_sql = f"""
            CREATE TABLE {self.catalog}.{self.schema}.{temp_table} (
                {schema_def_str}
            )
            WITH (
                external_location = '{temp_csv_path}',
                format = 'CSV',
                skip_header_line_count = 1
            )
            """
            
            # Try to create the external table - if this fails, fall back to direct INSERT
            try:
                self.trino_client.execute_query(create_external_table_sql)
                logger.debug(f"Created external table for CSV: {temp_table}")
                
                if mode == 'overwrite' and batch_data.shape[0] > 0:
                    # If in overwrite mode, truncate the target table first
                    truncate_sql = f"DELETE FROM {self.catalog}.{self.schema}.{self.table}"
                    self.trino_client.execute_query(truncate_sql)
                    logger.debug(f"Truncated target table {self.table} for overwrite mode")
                
                # Use CAST to ensure proper type conversion when inserting from temp table to target
                cast_columns = []
                try:
                    target_schema = self.trino_client.get_table_schema(self.catalog, self.schema, self.table)
                    column_types_dict = {col_name: col_type for col_name, col_type in target_schema}
                    
                    for col in columns:
                        if col in column_types_dict:
                            # Cast to the target table column type
                            cast_columns.append(f'CAST("{col}" AS {column_types_dict[col]}) AS "{col}"')
                        else:
                            # If no type info available, just use the column as is
                            cast_columns.append(f'"{col}"')
                except Exception:
                    # If we can't get schema info, just use columns as is
                    cast_columns = [f'"{col}"' for col in columns]
                
                cast_columns_str = ", ".join(cast_columns)
                
                # Insert from temp table to target using CAST
                insert_final_sql = f"""
                INSERT INTO {self.catalog}.{self.schema}.{self.table} ({column_names_str})
                SELECT {cast_columns_str} FROM {self.catalog}.{self.schema}.{temp_table}
                """
                self.trino_client.execute_query(insert_final_sql)
                logger.debug(f"Inserted data from external table to {self.table}")
                
                # Clean up
                drop_temp_sql = f"DROP TABLE {self.catalog}.{self.schema}.{temp_table}"
                self.trino_client.execute_query(drop_temp_sql)
                logger.debug(f"Dropped temporary table {temp_table}")
                
                # Remove temporary CSV
                if os.path.exists(temp_csv_path):
                    os.remove(temp_csv_path)
                    logger.debug(f"Removed temporary CSV: {temp_csv_path}")
                
            except Exception as e:
                logger.warning(f"External table approach failed: {str(e)}, falling back to direct insertion")
                
                # Fallback to direct insertion if external table doesn't work
                # First check if we need to handle overwrite mode
                if mode == 'overwrite':
                    truncate_sql = f"DELETE FROM {self.catalog}.{self.schema}.{self.table}"
                    self.trino_client.execute_query(truncate_sql)
                    logger.debug(f"Truncated target table {self.table} for overwrite mode")
                
                # Get target table schema
                try:
                    target_schema = self.trino_client.get_table_schema(self.catalog, self.schema, self.table)
                    column_types_dict = {col_name: col_type for col_name, col_type in target_schema}
                except Exception:
                    column_types_dict = {}  # Empty dict if we can't get the schema
                
                # Generate VALUES clause with proper CAST statements
                values = []
                for _, row in batch_data.iterrows():
                    row_values = []
                    for col_name, val in zip(batch_data.columns, row):
                        if pd.isna(val):
                            row_values.append("NULL")
                        else:
                            # Format the value based on its Python type
                            if isinstance(val, (int, float, bool)):
                                row_values.append(str(val))
                            else:
                                # Escape single quotes in string values
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"'{val_str}'")
                    
                    values.append(f"({', '.join(row_values)})")
                
                # Prepare target column list with explicit CASTs if needed
                cast_clauses = []
                for i, col in enumerate(columns):
                    if col in column_types_dict:
                        cast_clauses.append(f'CAST(v.column{i+1} AS {column_types_dict[col]})')
                    else:
                        cast_clauses.append(f'v.column{i+1}')
                
                # Split inserts into manageable chunks to avoid query size limits
                MAX_VALUES_PER_QUERY = 500
                for i in range(0, len(values), MAX_VALUES_PER_QUERY):
                    chunk_values = values[i:i + MAX_VALUES_PER_QUERY]
                    
                    # Create a query that uses VALUES clause with explicit column references
                    insert_sql = f"""
                    INSERT INTO {self.catalog}.{self.schema}.{self.table} ({column_names_str})
                    WITH value_table(column1{', column2' * (len(columns)-1)}) AS (
                      VALUES {', '.join(chunk_values)}
                    )
                    SELECT {', '.join(cast_clauses)}
                    FROM value_table v
                    """
                    
                    self.trino_client.execute_query(insert_sql)
                
                logger.debug(f"Inserted {len(values)} rows directly into target table")
                
                # Remove temporary CSV if it exists
                if os.path.exists(temp_csv_path):
                    os.remove(temp_csv_path)
                    logger.debug(f"Removed temporary CSV: {temp_csv_path}")
            
        except Exception as e:
            logger.error(f"Error in _write_batch_to_iceberg: {str(e)}", exc_info=True)
            # Clean up temporary files if they exist
            temp_csv_path = os.path.join(os.getcwd(), f"temp_{self.table}_{int(time.time())}.csv")
            if os.path.exists(temp_csv_path):
                try:
                    os.remove(temp_csv_path)
                except Exception:
                    pass
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
