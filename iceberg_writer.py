"""
Iceberg writer module for CSV to Iceberg conversion
"""
import os
import logging
import time
from typing import Dict, List, Any, Optional, Callable, Tuple
import tempfile
from datetime import datetime
import pandas as pd

# Import schema components
from schema_inferrer import Schema

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
            
            # Read the CSV in batches
            chunk_iter = pd.read_csv(
                csv_file, 
                delimiter=delimiter, 
                quotechar=quote_char,
                header=0 if has_header else None,
                chunksize=batch_size
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
            
            # Create a temporary table to hold the batch data
            create_temp_table_sql = f"""
            CREATE TABLE {self.catalog}.{self.schema}.{temp_table} (
                {", ".join([f'"{col}" VARCHAR' for col in columns])}
            )
            """
            self.trino_client.execute_query(create_temp_table_sql)
            logger.debug(f"Created temporary table {temp_table}")
            
            # Insert data into the temporary table
            # Convert dataframe to list of tuples for insertion
            values = []
            for _, row in batch_data.iterrows():
                # Properly escape and quote string values
                row_values = []
                for val in row:
                    if pd.isna(val):
                        row_values.append("NULL")
                    elif isinstance(val, (int, float)):
                        row_values.append(str(val))
                    else:
                        # Escape single quotes in string values
                        val_str = str(val).replace("'", "''")
                        row_values.append(f"'{val_str}'")
                
                values.append(f"({', '.join(row_values)})")
            
            # Split inserts into smaller batches if needed to avoid query size limits
            MAX_VALUES_PER_QUERY = 1000
            for i in range(0, len(values), MAX_VALUES_PER_QUERY):
                batch_values = values[i:i + MAX_VALUES_PER_QUERY]
                insert_temp_sql = f"""
                INSERT INTO {self.catalog}.{self.schema}.{temp_table} ({column_names_str})
                VALUES {", ".join(batch_values)}
                """
                self.trino_client.execute_query(insert_temp_sql)
            
            logger.debug(f"Inserted {len(values)} rows into temporary table")
            
            # Now, insert from temporary table to the Iceberg table
            if mode == 'overwrite' and batch_data.shape[0] > 0:
                # If in overwrite mode, truncate the target table first
                truncate_sql = f"DELETE FROM {self.catalog}.{self.schema}.{self.table}"
                self.trino_client.execute_query(truncate_sql)
                logger.debug(f"Truncated target table {self.table} for overwrite mode")
            
            # Insert from temp table to the Iceberg table
            insert_final_sql = f"""
            INSERT INTO {self.catalog}.{self.schema}.{self.table} ({column_names_str})
            SELECT {column_names_str} FROM {self.catalog}.{self.schema}.{temp_table}
            """
            self.trino_client.execute_query(insert_final_sql)
            logger.debug(f"Inserted data from temporary table to {self.table}")
            
            # Drop the temporary table
            drop_temp_sql = f"DROP TABLE {self.catalog}.{self.schema}.{temp_table}"
            self.trino_client.execute_query(drop_temp_sql)
            logger.debug(f"Dropped temporary table {temp_table}")
            
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
        # Count lines in the file
        with open(csv_file, 'r') as f:
            line_count = sum(1 for _ in f)
            return line_count - 1 if has_header else line_count
    except Exception as e:
        logger.error(f"Error counting CSV rows: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to count CSV rows: {str(e)}")
