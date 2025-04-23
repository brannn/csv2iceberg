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
            
            # Process in smaller batches
            for i in range(0, len(batch_data), max_rows_per_insert):
                batch_slice = batch_data.iloc[i:i+max_rows_per_insert]
                
                # Generate the CAST expressions for each column
                cast_expressions = []
                for col in columns:
                    if col in column_types_dict:
                        target_type = column_types_dict[col]
                        cast_expressions.append(f'CAST(? AS {target_type}) AS "{col}"')
                    else:
                        cast_expressions.append(f'CAST(? AS VARCHAR) AS "{col}"')
                
                # Create the INSERT statement with explicit casts
                insert_sql = f"""
                INSERT INTO {self.catalog}.{self.schema}.{self.table} ({column_names_str})
                SELECT {', '.join(cast_expressions)}
                """
                
                # Generate parameters for the query
                params = []
                for _, row in batch_slice.iterrows():
                    for col in columns:
                        val = row[col]
                        if pd.isna(val):
                            params.append(None)
                        else:
                            params.append(val)
                
                # Execute the parameterized query
                try:
                    # Create a cursor with parameterized query support
                    cursor = self.trino_client.connection.cursor()
                    cursor.execute(insert_sql, params)
                    self.trino_client.connection.commit()
                    logger.debug(f"Inserted batch of {len(batch_slice)} rows with parameterized query")
                except Exception as e:
                    logger.warning(f"Parameterized query failed: {str(e)}, trying alternative approach")
                    
                    # Try direct INSERT instead
                    values_list = []
                    for _, row in batch_slice.iterrows():
                        row_values = []
                        for col in columns:
                            val = row[col]
                            target_type = column_types_dict.get(col, "VARCHAR").upper()
                            
                            if pd.isna(val):
                                row_values.append("NULL")
                            elif isinstance(val, (int, float)):
                                if 'DOUBLE' in target_type or 'REAL' in target_type or 'FLOAT' in target_type:
                                    row_values.append(f"CAST({val} AS {target_type})")
                                elif 'DECIMAL' in target_type:
                                    row_values.append(f"CAST({val} AS {target_type})")
                                elif 'INT' in target_type or 'SMALL' in target_type or 'TINY' in target_type or 'BIG' in target_type:
                                    row_values.append(f"CAST({int(val)} AS {target_type})")
                                else:
                                    row_values.append(f"CAST({val} AS {target_type})")
                            elif isinstance(val, bool):
                                bool_val = 'TRUE' if val else 'FALSE'
                                row_values.append(f"CAST({bool_val} AS {target_type})")
                            elif isinstance(val, datetime.date) and not isinstance(val, datetime.datetime):
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"CAST(DATE '{val_str}' AS {target_type})")
                            elif isinstance(val, datetime.datetime):
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"CAST(TIMESTAMP '{val_str}' AS {target_type})")
                            else:
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"CAST('{val_str}' AS {target_type})")
                        
                        values_list.append(f"({', '.join(row_values)})")
                    
                    # Direct insertion with VALUES clause and explicit casts
                    direct_insert_sql = f"""
                    INSERT INTO {self.catalog}.{self.schema}.{self.table} ({column_names_str})
                    VALUES {', '.join(values_list)}
                    """
                    
                    logger.debug(f"Trying direct INSERT with {len(values_list)} rows")
                    self.trino_client.execute_query(direct_insert_sql)
            
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
