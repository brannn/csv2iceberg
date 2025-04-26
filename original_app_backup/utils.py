"""
Utility functions for CSV to Iceberg conversion
"""
import os
import sys
import logging
import socket
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

def setup_logging() -> logging.Logger:
    """
    Set up logging configuration.
    
    Returns:
        Logger instance
    """
    logger = logging.getLogger("csv_to_iceberg")
    
    # Create handlers
    console_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler("csv_to_iceberg.log")
    
    # Set levels
    logger.setLevel(logging.INFO)
    console_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.DEBUG)
    
    # Create formatters
    console_format = logging.Formatter('%(levelname)s: %(message)s')
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Set formatters
    console_handler.setFormatter(console_format)
    file_handler.setFormatter(file_format)
    
    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def validate_csv_file(file_path: str, delimiter: str, quote_char: str) -> bool:
    """
    Validate a CSV file.
    
    Args:
        file_path: Path to the CSV file
        delimiter: CSV delimiter character
        quote_char: CSV quote character
        
    Returns:
        True if the file is valid
    """
    logger = logging.getLogger("csv_to_iceberg")
    
    # Check if file exists
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False
    
    # Check if file is readable
    if not os.access(file_path, os.R_OK):
        logger.error(f"File is not readable: {file_path}")
        return False
    
    # Check file size
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        logger.error(f"File is empty: {file_path}")
        return False
    
    # Check file extension
    if not file_path.lower().endswith('.csv'):
        logger.warning(f"File does not have .csv extension: {file_path}")
    
    # Try to read first few lines to validate format
    try:
        with open(file_path, 'r') as file:
            # Read first line
            first_line = file.readline().strip()
            if not first_line:
                logger.error(f"File is empty or first line is blank: {file_path}")
                return False
            
            # Check if delimiter is present in the first line
            if delimiter not in first_line:
                logger.warning(
                    f"Delimiter '{delimiter}' not found in the first line. "
                    f"File might not be using the specified delimiter."
                )
            
            # Read more lines to check consistency (extend to 20 lines for better detection)
            line_count = 1
            field_count = first_line.count(delimiter) + 1
            
            # Track field counts to identify the most common one
            field_counts = {field_count: 1}  # Start with first line's count
            inconsistent_lines = []
            
            for _ in range(20):  # Check up to 20 more lines
                line = file.readline().strip()
                if not line:
                    break
                
                line_count += 1
                current_field_count = line.count(delimiter) + 1
                
                # Track this field count
                if current_field_count not in field_counts:
                    field_counts[current_field_count] = 0
                field_counts[current_field_count] += 1
                
                if current_field_count != field_count:
                    inconsistent_lines.append(line_count)
                    logger.warning(
                        f"Inconsistent field count: line 1 has {field_count} fields, "
                        f"line {line_count} has {current_field_count} fields"
                    )
            
            # If we found inconsistent field counts, log a summary
            if len(field_counts) > 1:
                # Find the most common field count
                most_common_count = max(field_counts.items(), key=lambda x: x[1])[0]
                
                field_count_summary = ", ".join(
                    [f"{count} fields: {occurrences} rows" for count, occurrences in field_counts.items()]
                )
                
                logger.warning(f"CSV has inconsistent field counts: {field_count_summary}")
                logger.info(f"Most common field count is {most_common_count} (found in {field_counts[most_common_count]} rows)")
                logger.info("The application will handle this by skipping problematic rows")
        
        return True
    except Exception as e:
        logger.error(f"Error validating CSV file: {str(e)}")
        return False

def validate_connection_params(host: str, port: int, uri: Optional[str] = "", use_hive: bool = True) -> bool:
    """
    Validate connection parameters.
    
    Args:
        host: Host name or IP address
        port: Port number
        uri: Hive metastore URI string (can be None if use_hive is False)
        use_hive: Whether Hive metastore is being used
        
    Returns:
        True if parameters are valid
    """
    logger = logging.getLogger("csv_to_iceberg")
    
    # Validate host
    if not host:
        logger.error("Host cannot be empty")
        return False
    
    # Validate port
    if port <= 0 or port > 65535:
        logger.error(f"Invalid port number: {port}")
        return False
    
    # Validate URI if Hive metastore is being used
    if use_hive:
        if not uri:
            logger.error("Hive metastore URI cannot be empty when using Hive metastore")
            return False
        
        if ':' not in uri and not uri.isalpha():
            logger.error(f"Invalid Hive metastore URI format: {uri}")
            return False
    
    return True

def get_table_location(catalog: str, schema: str, table: str, base_location: str = "/user/hive/warehouse") -> str:
    """
    Get the default location for an Iceberg table.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        base_location: Base warehouse location
        
    Returns:
        Table location path
    """
    return f"{base_location}/{catalog}/{schema}.db/{table}"

def check_port_availability(host: str, port: int, timeout: float = 1.0) -> bool:
    """
    Check if a port is available on a host.
    
    Args:
        host: Host name or IP address
        port: Port number
        timeout: Connection timeout in seconds
        
    Returns:
        True if the port is available/open
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    
    try:
        sock.connect((host, port))
        sock.close()
        return True
    except (socket.timeout, ConnectionRefusedError):
        return False
    except Exception:
        return False

def get_file_size(file_path: str) -> int:
    """
    Get the size of a file in bytes.
    
    Args:
        file_path: Path to the file
        
    Returns:
        File size in bytes
    """
    try:
        return os.path.getsize(file_path)
    except (FileNotFoundError, OSError):
        return 0

def format_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: File size in bytes
        
    Returns:
        Formatted file size string
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

def get_trino_role_header(role: str) -> Dict[str, str]:
    """
    Get Trino role header for system role.
    
    Args:
        role: Role name
        
    Returns:
        Dictionary with X-Trino-Role header
    """
    if not role:
        return {}
    
    return {"X-Trino-Role": f"system={role}"}

def is_test_job_id(job_id: str) -> bool:
    """
    Check if a job ID is for a test job.
    
    Args:
        job_id: Job ID
        
    Returns:
        True if it's a test job ID
    """
    return job_id.startswith("test_")

def format_duration(duration=None, start_time=None, end_time=None) -> str:
    """
    Calculate and format the duration between two timestamps or from a duration value.
    
    This function can be called in multiple ways:
    - format_duration(duration): With just a duration in seconds
    - format_duration(start_time=start, end_time=end): With named start and end times
    - format_duration(start_time, end_time): With positional start and end times
    
    Args:
        duration: Duration in seconds (optional)
        start_time: Start timestamp (datetime object, optional)
        end_time: End timestamp (datetime object, optional)
        
    Returns:
        Formatted duration string (e.g., "00:05:30" or "5 minutes 30 seconds")
    """
    logger = logging.getLogger("csv_to_iceberg")
    
    # Handle if we're given datetime objects directly
    if isinstance(duration, datetime):
        if end_time is None:
            end_time = datetime.now()
        if isinstance(end_time, datetime):
            try:
                duration = (end_time - duration).total_seconds()
            except Exception as e:
                logger.error(f"Error calculating duration from datetime: {e}")
                return "00:00:00"
        else:
            logger.error(f"If duration is a datetime, end_time must also be a datetime, not {type(end_time)}")
            return "00:00:00"
    
    # Calculate duration if not provided but we have start and end times
    if duration is None and start_time and end_time:
        try:
            if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
                logger.warning(f"Invalid datetime objects: start={type(start_time)}, end={type(end_time)}")
                return "00:00:00"
            
            # Ensure end_time is always >= start_time to avoid negative duration
            if end_time >= start_time:
                duration = (end_time - start_time).total_seconds()
            else:
                logger.warning(f"End time {end_time} is before start time {start_time}")
                return "00:00:00"
        except Exception as e:
            logger.error(f"Error calculating duration: {e}")
            return "00:00:00"
    
    if duration is None:
        return "N/A"
    
    try:
        # Ensure duration is a number
        if isinstance(duration, str):
            duration = float(duration)
        elif isinstance(duration, (int, float)):
            duration = float(duration)
        else:
            logger.error(f"Unexpected duration type: {type(duration)}")
            return "00:00:00"
            
        # Ensure duration is positive
        duration = max(0, duration)
        
        # Format as HH:MM:SS for UI display
        hours = int(duration // 3600)
        remaining = duration % 3600
        minutes = int(remaining // 60)
        seconds = int(remaining % 60)
        
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except Exception as e:
        logger.error(f"Error formatting duration: {e}")
        return "00:00:00"

def format_datetime(dt) -> str:
    """
    Format a datetime object or ISO datetime string for display.
    
    Args:
        dt: Datetime object or ISO format string
        
    Returns:
        Formatted datetime string
    """
    if not dt:
        return "N/A"
    
    # Handle ISO format strings
    if isinstance(dt, str):
        try:
            import datetime
            dt = datetime.datetime.fromisoformat(dt.replace('Z', '+00:00'))
        except (ValueError, TypeError) as e:
            import logging
            logging.warning(f"Failed to parse datetime string: {dt}, error: {str(e)}")
            return dt
    
    # Now format the datetime object
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def format_status(status: str) -> str:
    """
    Format job status for display.
    
    Args:
        status: Status string
        
    Returns:
        Formatted status string
    """
    if not status:
        return "Unknown"
        
    status_map = {
        "pending": "Pending",
        "running": "Running",
        "completed": "Completed",
        "failed": "Failed"
    }
    
    return status_map.get(status.lower(), status.capitalize())

def clean_column_name(name: str) -> str:
    """
    Clean a column name for use in SQL statements.
    
    Args:
        name: Column name
        
    Returns:
        Cleaned column name
    """
    if not name:
        return "unnamed_column"
        
    # Replace all special characters (including parentheses) with underscores
    cleaned = "".join(c if c.isalnum() or c == '_' else '_' for c in str(name).strip())
    
    # Ensure name starts with a letter or underscore
    if cleaned and not (cleaned[0].isalpha() or cleaned[0] == '_'):
        cleaned = 'col_' + cleaned
    
    # Handle special cases (SQL reserved words)
    if not cleaned or cleaned.lower() in ('table', 'column', 'select', 'where', 'from', 'order', 'group', 'by'):
        cleaned = 'col_' + cleaned
    
    # If the name is empty after cleaning, use a default
    if not cleaned:
        return "unnamed_column"
        
    return cleaned