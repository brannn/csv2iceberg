"""
Core service for CSV to Iceberg conversion operations
This module centralizes the conversion logic to be used by both CLI and web interfaces
"""
import os
import json
import logging
import time
from typing import Dict, List, Any, Optional, Callable, Tuple, Union
import traceback

from core.iceberg_writer import IcebergWriter
from core.schema_inferrer import infer_schema_from_csv
from connectors.trino_client import TrinoClient
from connectors.hive_client import HiveMetastoreClient
from utils import clean_column_name

# Configure logging
logger = logging.getLogger(__name__)

def convert_csv_to_iceberg(
    # Required parameters
    csv_file: str,
    table_name: str,
    
    # Connection parameters
    trino_host: str,
    trino_port: int,
    trino_user: str,
    trino_password: Optional[str] = None,
    http_scheme: str = 'https',
    trino_role: str = 'sysadmin',
    trino_catalog: str = 'iceberg',
    trino_schema: str = 'default',
    use_hive_metastore: bool = False,
    hive_metastore_uri: str = 'localhost:9083',
    
    # CSV handling parameters
    delimiter: str = ',',
    quote_char: str = '"',
    has_header: bool = True,
    batch_size: int = 20000,
    
    # Schema/data parameters
    mode: str = 'append',
    sample_size: int = 1000,
    include_columns: Optional[List[str]] = None,
    exclude_columns: Optional[List[str]] = None,
    custom_schema: Optional[str] = None,
    
    # Dry run mode
    dry_run: bool = False,
    
    # Callback function
    progress_callback: Optional[Callable[[int], None]] = None
) -> Dict[str, Any]:
    """
    Convert a CSV file to an Iceberg table.
    
    This is the core conversion function used by both CLI and web interfaces.
    It encapsulates all the logic needed for the conversion process.
    
    Args:
        csv_file: Path to the CSV file
        table_name: Target table name (can be fully qualified as catalog.schema.table)
        trino_host: Trino host
        trino_port: Trino port
        trino_user: Trino user
        trino_password: Trino password (if authentication is enabled)
        http_scheme: HTTP scheme (http or https)
        trino_role: Trino role for authorization
        trino_catalog: Default Trino catalog (used if not specified in table_name)
        trino_schema: Default Trino schema (used if not specified in table_name)
        use_hive_metastore: Whether to use direct Hive Metastore connection
        hive_metastore_uri: Hive metastore Thrift URI
        delimiter: CSV delimiter character
        quote_char: CSV quote character
        has_header: Whether the CSV has a header row
        batch_size: Number of rows to process in each batch
        mode: Write mode (append or overwrite)
        sample_size: Number of rows to sample for schema inference
        include_columns: List of column names to include
        exclude_columns: List of column names to exclude
        custom_schema: JSON string containing a custom schema definition
        progress_callback: Callback function to report progress
        
    Returns:
        Dictionary with conversion results and statistics
    """
    start_time = time.time()
    result = {
        'success': False,
        'table_name': table_name,
        'rows_processed': 0,
        'error': None,
        'stdout': '',
        'stderr': '',
        'duration': 0
    }
    
    # Create a string buffer for stdout logging
    stdout_buffer = []
    
    # Helper function to add log messages
    def add_log(message):
        logger.info(message)
        stdout_buffer.append(message)
        result['stdout'] = '\n'.join(stdout_buffer)
    
    try:
        # Create Trino client
        if dry_run:
            add_log(f"Running in DRY RUN mode - simulating operations without connecting to Trino")
            add_log(f"Would connect to: {http_scheme}://{trino_host}:{trino_port}")
            add_log(f"Would use Trino user: {trino_user} with role: {trino_role}")
        else:
            add_log(f"Connecting to Trino server at {http_scheme}://{trino_host}:{trino_port}")
            add_log(f"Using Trino user: {trino_user} with role: {trino_role}")
            
        trino_client = TrinoClient(
            host=trino_host,
            port=trino_port,
            user=trino_user,
            password=trino_password,
            http_scheme=http_scheme,
            role=trino_role,
            dry_run=dry_run
        )
        
        if dry_run:
            add_log("Trino client created in dry run mode - no actual connection will be made")
        else:
            add_log("Trino client created successfully")
        
        # Create Hive client if needed
        hive_client = None
        if use_hive_metastore:
            if dry_run:
                add_log(f"Dry run mode: Would connect to Hive metastore at {hive_metastore_uri}")
                add_log("Skipping actual Hive metastore connection in dry run mode")
            else:
                add_log(f"Connecting to Hive metastore at {hive_metastore_uri}")
                hive_client = HiveMetastoreClient(hive_metastore_uri)
                add_log("Hive metastore client created successfully")
        else:
            add_log("Direct Hive metastore connection disabled, using Trino metadata APIs only")
        
        # Parse table name parts
        catalog_part, schema_part, table_part = parse_table_name(table_name)
        
        # Use specified parts or defaults
        catalog = catalog_part or trino_catalog
        schema = schema_part or trino_schema
        table = table_part or table_name  # Fallback to full table_name if parsing failed
        
        add_log(f"Target Iceberg table: {catalog}.{schema}.{table}")
        add_log(f"Using write mode: {mode}")
        
        # Create Iceberg writer
        writer = IcebergWriter(
            trino_client=trino_client,
            catalog=catalog,
            schema=schema,
            table=table,
            hive_client=hive_client
        )
        
        # Add information about CSV file
        add_log(f"Processing CSV file: {csv_file}")
        add_log(f"CSV delimiter: '{delimiter}', quote character: '{quote_char}', has header: {has_header}")
        add_log(f"Using batch size: {batch_size}")
        
        if include_columns:
            add_log(f"Including only these columns: {include_columns}")
        if exclude_columns:
            add_log(f"Excluding these columns: {exclude_columns}")
            
        # Write CSV to Iceberg
        add_log("Starting CSV to Iceberg conversion...")
        rows_written = writer.write_csv_to_iceberg(
            csv_file=csv_file,
            mode=mode,
            delimiter=delimiter,
            has_header=has_header,
            quote_char=quote_char,
            batch_size=batch_size,
            include_columns=include_columns,
            exclude_columns=exclude_columns,
            progress_callback=progress_callback
        )
        add_log(f"Conversion completed successfully! Processed {rows_written} rows.")
        
        # Record success
        result['success'] = True
        result['rows_processed'] = rows_written
        
    except Exception as e:
        import traceback
        error_msg = str(e)
        tb = traceback.format_exc()
        logger.error(f"Error during conversion: {error_msg}", exc_info=True)
        result['success'] = False
        result['error'] = error_msg
        result['traceback'] = tb
        
        # Also log the first few lines of the error to make debugging easier
        error_lines = tb.split('\n')
        if len(error_lines) > 5:
            logger.error(f"Error summary: {error_lines[-5:]}")
    finally:
        # Calculate duration
        end_time = time.time()
        duration = end_time - start_time
        result['duration'] = duration
        
    return result

def validate_csv_file(csv_file: str, delimiter: str, quote_char: str) -> bool:
    """
    Validate that the CSV file exists and is accessible.
    
    Args:
        csv_file: Path to the CSV file
        delimiter: CSV delimiter character
        quote_char: CSV quote character
        
    Returns:
        True if valid, False otherwise
    """
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        return False
        
    if not os.path.isfile(csv_file):
        logger.error(f"Not a file: {csv_file}")
        return False
        
    if not os.access(csv_file, os.R_OK):
        logger.error(f"CSV file not readable: {csv_file}")
        return False
        
    return True

def parse_table_name(table_name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse a table name in the format catalog.schema.table.
    
    Args:
        table_name: Table name string
        
    Returns:
        Tuple of (catalog, schema, table) components
    """
    parts = table_name.split('.')
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return None, parts[0], parts[1]
    elif len(parts) == 1:
        return None, None, parts[0]
    else:
        return None, None, None

def validate_connection_params(
    trino_host: str, 
    trino_port: int, 
    hive_metastore_uri: Optional[str] = None, 
    use_hive_metastore: bool = False
) -> bool:
    """
    Validate connection parameters.
    
    Args:
        trino_host: Trino host
        trino_port: Trino port
        hive_metastore_uri: Hive metastore Thrift URI
        use_hive_metastore: Whether to use direct Hive Metastore connection
        
    Returns:
        True if valid, False otherwise
    """
    if not trino_host:
        logger.error("Trino host cannot be empty")
        return False
        
    if not isinstance(trino_port, int) or trino_port <= 0:
        logger.error(f"Invalid Trino port: {trino_port}")
        return False
        
    if use_hive_metastore and not hive_metastore_uri:
        logger.error("Hive metastore URI is required when use_hive_metastore is True")
        return False
        
    return True