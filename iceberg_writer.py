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
            column_names_str = ", ".join([f'"{col}"' for col in columns])
            
            # Create temporary table name with timestamp to avoid conflicts
            temp_table = f"temp_{self.table}_{int(time.time())}"
            
            # Convert pandas DataFrame to PyArrow Table for better type handling
            import pyarrow as pa
            
            try:
                # First get target table schema to ensure we match types correctly
                target_schema = self.trino_client.get_table_schema(self.catalog, self.schema, self.table)
                column_types_dict = {col_name: col_type for col_name, col_type in target_schema}
                
                # Map Trino types to PyArrow types
                arrow_types = {}
                for col in columns:
                    if col in column_types_dict:
                        trino_type = column_types_dict[col].lower()
                        # Convert Trino type to PyArrow type
                        if 'integer' in trino_type:
                            arrow_types[col] = pa.int32()
                        elif 'bigint' in trino_type:
                            arrow_types[col] = pa.int64()
                        elif 'smallint' in trino_type:
                            arrow_types[col] = pa.int16()
                        elif 'tinyint' in trino_type:
                            arrow_types[col] = pa.int8()
                        elif 'decimal' in trino_type:
                            # Extract precision and scale from decimal type
                            precision = 38  # Default
                            scale = 0  # Default
                            if '(' in trino_type:
                                try:
                                    parts = trino_type.split('(')[1].split(')')[0].split(',')
                                    precision = int(parts[0])
                                    if len(parts) > 1:
                                        scale = int(parts[1])
                                except Exception:
                                    pass
                            arrow_types[col] = pa.decimal128(precision, scale)
                        elif 'double' in trino_type or 'float' in trino_type or 'real' in trino_type:
                            arrow_types[col] = pa.float64()
                        elif 'boolean' in trino_type:
                            arrow_types[col] = pa.bool_()
                        elif 'timestamp' in trino_type:
                            arrow_types[col] = pa.timestamp('us')
                        elif 'date' in trino_type:
                            arrow_types[col] = pa.date32()
                        else:
                            # Default to string for other types
                            arrow_types[col] = pa.string()
                
                # Create a schema for the PyArrow table
                arrow_schema = pa.schema([pa.field(col, arrow_types.get(col, pa.string())) for col in columns])
                
                # Convert each column to the appropriate type
                arrays = []
                for col in columns:
                    # Handle NaN values by replacing them with None
                    data = batch_data[col].tolist()
                    data = [None if pd.isna(x) else x for x in data]
                    
                    if col in arrow_types:
                        try:
                            # Try to convert to the target type
                            arrays.append(pa.array(data, type=arrow_types[col]))
                        except pa.ArrowInvalid:
                            # Fallback to string if conversion fails
                            logger.warning(f"Could not convert column '{col}' to {arrow_types[col]}, falling back to string")
                            arrays.append(pa.array([str(x) if x is not None else None for x in data]))
                    else:
                        # Default to string for any columns not in the target schema
                        arrays.append(pa.array([str(x) if x is not None else None for x in data]))
                
                # Create the PyArrow table with schema
                arrow_table = pa.Table.from_arrays(arrays, names=columns)
                
            except Exception as e:
                logger.warning(f"Error creating PyArrow table with target schema: {str(e)}, using default conversion")
                # Fallback to simple conversion
                arrow_table = pa.Table.from_pandas(batch_data)
            
            # Now use PyIceberg to write directly to the target table
            try:
                # If running in overwrite mode, truncate the target table first
                if mode == 'overwrite' and batch_data.shape[0] > 0:
                    # If in overwrite mode, truncate the target table first
                    truncate_sql = f"DELETE FROM {self.catalog}.{self.schema}.{self.table}"
                    self.trino_client.execute_query(truncate_sql)
                    logger.debug(f"Truncated target table {self.table} for overwrite mode")
                
                # Create a temporary table with appropriate types from PyArrow schema
                pa_schema = arrow_table.schema
                column_defs = []
                
                for i, field in enumerate(pa_schema):
                    col_name = field.name
                    pa_type = field.type
                    
                    # Map PyArrow types to Trino types
                    if pa.types.is_integer(pa_type):
                        if pa.types.is_int64(pa_type):
                            col_type = "BIGINT"
                        elif pa.types.is_int32(pa_type):
                            col_type = "INTEGER"
                        elif pa.types.is_int16(pa_type):
                            col_type = "SMALLINT"
                        elif pa.types.is_int8(pa_type):
                            col_type = "TINYINT"
                        else:
                            col_type = "BIGINT"
                    elif pa.types.is_floating(pa_type):
                        col_type = "DOUBLE"
                    elif pa.types.is_boolean(pa_type):
                        col_type = "BOOLEAN"
                    elif pa.types.is_timestamp(pa_type):
                        col_type = "TIMESTAMP(3)"
                    elif pa.types.is_date(pa_type):
                        col_type = "DATE"
                    elif pa.types.is_decimal(pa_type):
                        col_type = f"DECIMAL({pa_type.precision}, {pa_type.scale})"
                    else:
                        # Default to varchar for other types
                        col_type = "VARCHAR"
                    
                    column_defs.append(f'"{col_name}" {col_type}')
                
                # Create a temporary table with the schema
                create_temp_table_sql = f"""
                CREATE TABLE {self.catalog}.{self.schema}.{temp_table} (
                    {", ".join(column_defs)}
                )
                """
                self.trino_client.execute_query(create_temp_table_sql)
                logger.debug(f"Created temporary table {temp_table} with correct schema")
                
                # Use COPY statement or INSERT for each batch of rows
                batch_size = 1000
                total_rows = len(arrow_table)
                
                for start_idx in range(0, total_rows, batch_size):
                    end_idx = min(start_idx + batch_size, total_rows)
                    batch = arrow_table.slice(start_idx, end_idx - start_idx)
                    
                    # Generate VALUES clause from PyArrow table
                    values = []
                    for row_idx in range(batch.num_rows):
                        row_values = []
                        for col_idx, col in enumerate(columns):
                            val = batch.column(col_idx)[row_idx].as_py()
                            if val is None:
                                row_values.append("NULL")
                            elif isinstance(val, (int, float)):
                                row_values.append(str(val))
                            elif isinstance(val, bool):
                                row_values.append('TRUE' if val else 'FALSE')
                            elif isinstance(val, datetime.date):
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"DATE '{val_str}'")
                            elif isinstance(val, datetime.datetime):
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"TIMESTAMP '{val_str}'")
                            else:
                                # Escape single quotes in string values
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"'{val_str}'")
                        
                        values.append(f"({', '.join(row_values)})")
                    
                    # Insert the batch into temp table
                    insert_temp_sql = f"""
                    INSERT INTO {self.catalog}.{self.schema}.{temp_table} ({column_names_str})
                    VALUES {", ".join(values)}
                    """
                    self.trino_client.execute_query(insert_temp_sql)
                
                logger.debug(f"Inserted {total_rows} rows into temporary table")
                
                # Now insert from temp table to target table
                # This approach ensures type compatibility
                insert_final_sql = f"""
                INSERT INTO {self.catalog}.{self.schema}.{self.table} ({column_names_str})
                SELECT {column_names_str} FROM {self.catalog}.{self.schema}.{temp_table}
                """
                self.trino_client.execute_query(insert_final_sql)
                logger.debug(f"Inserted data from temp table to {self.table}")
                
                # Drop the temporary table
                drop_temp_sql = f"DROP TABLE {self.catalog}.{self.schema}.{temp_table}"
                self.trino_client.execute_query(drop_temp_sql)
                logger.debug(f"Dropped temporary table {temp_table}")
                
            except Exception as e:
                logger.warning(f"PyArrow/PyIceberg direct write failed: {str(e)}, falling back to SQL INSERT")
                
                # Fallback to standard SQL INSERT if needed
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
                
                # Prepare target column list with explicit CASTs if needed
                cast_clauses = []
                for i, col in enumerate(columns):
                    if col in column_types_dict:
                        cast_clauses.append(f'CAST(v.column{i+1} AS {column_types_dict[col]})')
                    else:
                        cast_clauses.append(f'v.column{i+1}')
                
                # Generate VALUES clause from PyArrow table or fallback to pandas
                values = []
                try:
                    # Try to get values from PyArrow table
                    for row_idx in range(arrow_table.num_rows):
                        row_values = []
                        for col_idx, col in enumerate(columns):
                            val = arrow_table.column(col_idx)[row_idx].as_py()
                            if val is None:
                                row_values.append("NULL")
                            elif isinstance(val, (int, float)):
                                row_values.append(str(val))
                            elif isinstance(val, bool):
                                row_values.append('TRUE' if val else 'FALSE')
                            elif isinstance(val, datetime.date):
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"DATE '{val_str}'")
                            elif isinstance(val, datetime.datetime):
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"TIMESTAMP '{val_str}'")
                            else:
                                # Escape single quotes in string values
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"'{val_str}'")
                        
                        values.append(f"({', '.join(row_values)})")
                except Exception:
                    # Fallback to pandas if PyArrow access fails
                    for _, row in batch_data.iterrows():
                        row_values = []
                        for val in row:
                            if pd.isna(val):
                                row_values.append("NULL")
                            elif isinstance(val, (int, float)):
                                row_values.append(str(val))
                            elif isinstance(val, bool):
                                row_values.append('TRUE' if val else 'FALSE')
                            else:
                                # Escape single quotes in string values
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"'{val_str}'")
                        
                        values.append(f"({', '.join(row_values)})")
                
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
