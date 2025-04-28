"""
S3 Tables utilities for common operations and performance optimization
"""
import os
import time
import logging
import tempfile
from typing import Dict, Any, List, Optional, Tuple, Callable
import pyarrow as pa
import pyarrow.parquet as pq
from functools import wraps

logger = logging.getLogger(__name__)

def timing_decorator(func: Callable) -> Callable:
    """Decorator to measure function execution time.
    
    Args:
        func: Function to measure
        
    Returns:
        Wrapped function with timing
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.debug(f"{func.__name__} took {execution_time:.2f} seconds to execute")
        return result
    return wrapper

def get_optimal_batch_size(data_size: int, memory_limit: int = 1024 * 1024 * 1024) -> int:
    """Calculate optimal batch size based on available memory.
    
    Args:
        data_size: Total size of data in bytes
        memory_limit: Memory limit in bytes (default: 1GB)
        
    Returns:
        Optimal batch size in rows
    """
    # Estimate memory per row (conservative estimate)
    memory_per_row = 1024  # 1KB per row
    
    # Calculate batch size
    batch_size = memory_limit // memory_per_row
    
    # Ensure batch size is reasonable
    batch_size = max(1000, min(batch_size, 1000000))
    
    return batch_size

def optimize_parquet_writing(df: pa.Table, compression: str = 'snappy',
                           row_group_size: int = 100000) -> pa.Table:
    """Optimize PyArrow table for Parquet writing.
    
    Args:
        df: PyArrow table
        compression: Parquet compression codec
        row_group_size: Row group size
        
    Returns:
        Optimized PyArrow table
    """
    # Set Parquet writer properties
    pq_writer_properties = pq.ParquetWriterProperties(
        compression=compression,
        row_group_size=row_group_size,
        write_batch_size=row_group_size,
        dictionary_pagesize_limit=2097152,  # 2MB
        write_page_index=True,
        write_statistics=True
    )
    
    return df

def create_temp_parquet_file(df: pa.Table, compression: str = 'snappy') -> Tuple[str, int]:
    """Create a temporary Parquet file.
    
    Args:
        df: PyArrow table
        compression: Parquet compression codec
        
    Returns:
        Tuple of (file path, file size)
    """
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
        temp_path = temp_file.name
        
        # Write the table to the temporary file
        pq.write_table(df, temp_path, compression=compression)
        
        # Get file size
        file_size = os.path.getsize(temp_path)
        
        return temp_path, file_size

def cleanup_temp_files(file_paths: List[str]) -> None:
    """Clean up temporary files.
    
    Args:
        file_paths: List of file paths to clean up
    """
    for path in file_paths:
        try:
            if os.path.exists(path):
                os.unlink(path)
        except OSError as e:
            logger.warning(f"Could not remove temporary file {path}: {str(e)}")

def validate_s3_path(path: str) -> Tuple[str, str]:
    """Validate and parse S3 path.
    
    Args:
        path: S3 path (s3://bucket/key)
        
    Returns:
        Tuple of (bucket, key)
        
    Raises:
        ValueError: If path is invalid
    """
    if not path.startswith('s3://'):
        raise ValueError("Path must start with 's3://'")
    
    parts = path[5:].split('/', 1)
    if len(parts) != 2:
        raise ValueError("Path must be in format 's3://bucket/key'")
    
    bucket, key = parts
    if not bucket or not key:
        raise ValueError("Bucket and key must not be empty")
    
    return bucket, key

def format_size(size_bytes: int) -> str:
    """Format size in bytes to human-readable string.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"

def log_operation_stats(operation: str, start_time: float, end_time: float,
                       rows_processed: int, bytes_processed: int) -> None:
    """Log operation statistics.
    
    Args:
        operation: Operation name
        start_time: Start time
        end_time: End time
        rows_processed: Number of rows processed
        bytes_processed: Number of bytes processed
    """
    duration = end_time - start_time
    rows_per_second = rows_processed / duration if duration > 0 else 0
    bytes_per_second = bytes_processed / duration if duration > 0 else 0
    
    logger.info(
        f"{operation} completed:"
        f"\n  Duration: {duration:.2f} seconds"
        f"\n  Rows processed: {rows_processed:,}"
        f"\n  Bytes processed: {format_size(bytes_processed)}"
        f"\n  Throughput: {rows_per_second:.2f} rows/second"
        f"\n  Bandwidth: {format_size(bytes_per_second)}/second"
    ) 