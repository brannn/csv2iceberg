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
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from core.iceberg_writer import IcebergWriter
from core.schema_inferrer import infer_schema_from_csv
from connectors.trino_client import TrinoClient
from connectors.hive_client import HiveMetastoreClient
from connectors.s3tables_client import S3TablesClient
from connectors.s3tables_utils import (
    timing_decorator, get_optimal_batch_size, optimize_parquet_writing,
    create_temp_parquet_file, cleanup_temp_files, validate_s3_path,
    format_size, log_operation_stats
)
from utils import clean_column_name

# Configure logging
logger = logging.getLogger(__name__)

@timing_decorator
def convert_csv_to_iceberg(
    # Required parameters
    csv_file: str,
    table_name: str,
    
    # Connection parameters
    profile_type: str = 'trino',  # 'trino' or 's3tables'
    
    # Trino connection parameters
    trino_host: Optional[str] = None,
    trino_port: Optional[int] = None,
    trino_user: Optional[str] = None,
    trino_password: Optional[str] = None,
    http_scheme: str = 'https',
    trino_role: str = 'sysadmin',
    trino_catalog: str = 'iceberg',
    trino_schema: str = 'default',
    use_hive_metastore: bool = False,
    hive_metastore_uri: str = 'localhost:9083',
    
    # S3 Tables connection parameters
    region: Optional[str] = None,
    table_bucket_arn: Optional[str] = None,
    namespace: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    
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
    
    # SQL batcher options
    max_query_size: int = 700000,
    
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
        profile_type: Type of connection profile ('trino' or 's3tables')
        
        # Trino connection parameters
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
        
        # S3 Tables connection parameters
        region: AWS region
        table_bucket_arn: S3 bucket ARN for tables
        namespace: S3 Tables namespace
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        
        # CSV handling parameters
        delimiter: CSV delimiter character
        quote_char: CSV quote character
        has_header: Whether the CSV has a header row
        batch_size: Number of rows to process in each batch
        mode: Write mode (append or overwrite)
        sample_size: Number of rows to sample for schema inference
        include_columns: List of column names to include
        exclude_columns: List of column names to exclude
        custom_schema: JSON string containing a custom schema definition
        dry_run: Run in dry-run mode without actually modifying data
        max_query_size: Maximum SQL query size in bytes (default: 700000, 70% of Trino's 1MB limit)
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
        if profile_type == 's3tables':
            # Validate S3 Tables parameters
            if not all([region, table_bucket_arn, namespace]):
                raise ValueError("Missing required S3 Tables parameters: region, table_bucket_arn, namespace")
            
            # Create S3 Tables client
            if dry_run:
                add_log(f"Running in DRY RUN mode - simulating S3 Tables operations")
                add_log(f"Would connect to AWS region: {region}")
                add_log(f"Would use bucket: {table_bucket_arn}")
                add_log(f"Would use namespace: {namespace}")
            else:
                add_log(f"Connecting to AWS region: {region}")
                add_log(f"Using bucket: {table_bucket_arn}")
                add_log(f"Using namespace: {namespace}")
            
            s3tables_client = S3TablesClient(
                endpoint=f"https://{region}.amazonaws.com",
                region=region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
            
            if dry_run:
                add_log("S3 Tables client created in dry run mode - no actual connection will be made")
            else:
                add_log("S3 Tables client created successfully")
            
            # Parse table name
            table = table_name  # For S3 Tables, we use the simple table name
            
            add_log(f"Target S3 Tables table: {namespace}.{table}")
            add_log(f"Using write mode: {mode}")
            
            # Add information about CSV file
            add_log(f"Processing CSV file: {csv_file}")
            add_log(f"CSV delimiter: '{delimiter}', quote character: '{quote_char}', has header: {has_header}")
            add_log(f"Using batch size: {batch_size}")
            
            if include_columns:
                add_log(f"Including only these columns: {include_columns}")
            if exclude_columns:
                add_log(f"Excluding these columns: {exclude_columns}")
            
            # Write CSV to S3 Tables
            add_log("Starting CSV to S3 Tables conversion...")
            
            # Read CSV file
            add_log("Reading CSV file...")
            df = pd.read_csv(
                csv_file,
                delimiter=delimiter,
                quotechar=quote_char,
                header=0 if has_header else None,
                names=None if has_header else [f"col{i}" for i in range(100)],  # Default column names
                dtype=str  # Read all columns as strings initially
            )
            
            # Clean column names
            df.columns = [clean_column_name(col) for col in df.columns]
            
            # Filter columns if needed
            if include_columns:
                df = df[include_columns]
            if exclude_columns:
                df = df.drop(columns=exclude_columns)
            
            # Convert to PyArrow table
            table = pa.Table.from_pandas(df)
            
            # Infer schema
            add_log("Inferring schema...")
            schema = infer_schema_from_csv(
                csv_file=csv_file,
                delimiter=delimiter,
                quote_char=quote_char,
                has_header=has_header,
                sample_size=sample_size
            )
            
            # Create table if it doesn't exist
            if mode == 'overwrite' or not s3tables_client.table_exists(namespace, table):
                add_log(f"Creating table {namespace}.{table}...")
                s3tables_client.create_table(
                    namespace=namespace,
                    table_name=table,
                    schema=schema
                )
                add_log("Table created successfully")
            
            # Calculate optimal batch size
            optimal_batch_size = get_optimal_batch_size(len(df) * 1024)  # Rough estimate of data size
            batch_size = min(batch_size, optimal_batch_size)
            add_log(f"Using batch size: {batch_size} rows")
            
            # Process data in batches
            total_rows = len(df)
            rows_processed = 0
            temp_files = []
            
            try:
                for i in range(0, total_rows, batch_size):
                    # Get batch
                    batch = df.iloc[i:i + batch_size]
                    batch_table = pa.Table.from_pandas(batch)
                    
                    # Optimize batch for Parquet writing
                    batch_table = optimize_parquet_writing(batch_table)
                    
                    # Create temporary Parquet file
                    temp_path, file_size = create_temp_parquet_file(batch_table)
                    temp_files.append(temp_path)
                    
                    # Insert batch
                    if not dry_run:
                        s3tables_client.insert_data(
                            namespace=namespace,
                            table_name=table,
                            data=batch_table
                        )
                    
                    # Update progress
                    rows_processed += len(batch)
                    if progress_callback:
                        progress_callback(int(rows_processed * 100 / total_rows))
                    
                    add_log(f"Processed {rows_processed}/{total_rows} rows ({(rows_processed/total_rows)*100:.1f}%)")
                
                # Record success
                result['success'] = True
                result['rows_processed'] = rows_processed
                
                # Log operation stats
                end_time = time.time()
                log_operation_stats(
                    operation="CSV to S3 Tables conversion",
                    start_time=start_time,
                    end_time=end_time,
                    rows_processed=rows_processed,
                    bytes_processed=sum(os.path.getsize(f) for f in temp_files)
                )
                
            finally:
                # Clean up temporary files
                cleanup_temp_files(temp_files)
            
        else:
            # Original Trino-based conversion logic
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
            add_log(f"Using max query size: {max_query_size} bytes ({max_query_size/1000:.0f}KB)")
            
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
                progress_callback=progress_callback,
                dry_run=dry_run,
                max_query_size=max_query_size
            )
            add_log(f"Conversion completed successfully! Processed {rows_written} rows.")
            
            # Record success
            result['success'] = True
            result['rows_processed'] = rows_written
            
            # Capture performance metrics
            if hasattr(writer, 'processing_stats') and writer.processing_stats:
                result['performance_metrics'] = writer.processing_stats
                add_log(f"Processing rate: {writer.processing_stats.get('processing_rate', 0):.2f} rows/sec")
                add_log(f"Batches: {writer.processing_stats.get('total_batches', 0)}, " +
                       f"avg size: {writer.processing_stats.get('avg_batch_size', 0):.1f} rows")
                add_log(f"Total processing time: {writer.processing_stats.get('total_processing_time', 0):.2f}s")
                
            # Capture dry run results if available
            if dry_run and hasattr(writer, 'dry_run_results') and writer.dry_run_results:
                result['dry_run_results'] = writer.dry_run_results
        
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