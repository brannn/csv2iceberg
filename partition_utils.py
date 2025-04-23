"""
Partition utilities for CSV to Iceberg conversion
"""
from typing import List, Tuple, Optional, Dict, Any
import re
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Valid transform functions and their compatible data types
VALID_TRANSFORMS = {
    'year': ['date', 'timestamp'],
    'month': ['date', 'timestamp'],
    'day': ['date', 'timestamp'],
    'hour': ['timestamp'],
    'bucket': ['int', 'bigint', 'varchar', 'string', 'uuid'],
    'truncate': ['int', 'bigint', 'varchar', 'string']
}

def parse_partition_spec(spec: str) -> Tuple[Optional[str], str, Optional[int]]:
    """
    Parse a partition specification into transform, column, and parameter.
    
    Args:
        spec: Partition specification string (e.g., "year(date)", "bucket(id, 16)", "column_name")
        
    Returns:
        Tuple of (transform, column_name, transform_param)
    """
    # Check for transform functions
    transform_match = re.match(r'(\w+)\(([^,)]+)(?:,\s*(\d+))?\)', spec)
    
    if transform_match:
        transform = transform_match.group(1)
        column = transform_match.group(2)
        param = transform_match.group(3)
        param = int(param) if param else None
        return transform, column, param
    else:
        # No transform, just identity partitioning
        return None, spec, None

def validate_partition_spec(
    spec: str, 
    column_names: List[str], 
    column_types: List[str]
) -> Tuple[bool, str]:
    """
    Validate a partition specification against the available columns and types.
    
    Args:
        spec: Partition specification string
        column_names: List of available column names
        column_types: List of column types matching column_names
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    transform, column, param = parse_partition_spec(spec)
    
    # Check if column exists
    if column not in column_names:
        return False, f"Column '{column}' not found in the schema"
    
    # If no transform, identity partitioning is always valid
    if not transform:
        return True, ""
    
    # Check if transform is valid
    if transform not in VALID_TRANSFORMS:
        return False, f"Invalid transform function: {transform}"
    
    # Check if column type is compatible with transform
    col_idx = column_names.index(column)
    col_type = column_types[col_idx]
    
    # Strip length information from column type
    base_type = col_type.split('(')[0].lower()
    
    if base_type not in VALID_TRANSFORMS[transform]:
        return False, f"Transform '{transform}' is not compatible with column type '{col_type}'"
    
    # Validate transform parameters
    if transform == 'bucket' and (not param or param <= 0):
        return False, "Bucket transform requires a positive integer parameter"
    
    if transform == 'truncate' and (not param or param <= 0):
        return False, "Truncate transform requires a positive integer parameter"
    
    return True, ""

def format_partition_spec(transform: Optional[str], column: str, param: Optional[int] = None) -> str:
    """
    Format a partition specification for use in Trino SQL.
    
    Args:
        transform: Transform function name or None for identity
        column: Column name
        param: Optional transform parameter
        
    Returns:
        Formatted partition specification string
    """
    if not transform:
        return column
    
    if param is not None:
        return f"{transform}({column}, {param})"
    else:
        return f"{transform}({column})"

def suggest_partition_columns(
    column_names: List[str], 
    column_types: List[str],
    sample_data: Optional[List[List[Any]]] = None
) -> List[Tuple[str, Optional[str], Optional[int]]]:
    """
    Suggest potential partition columns based on column types and sample data.
    
    Args:
        column_names: List of column names
        column_types: List of column types
        sample_data: Optional sample data for analysis
        
    Returns:
        List of (column_name, suggested_transform, suggested_param) tuples
    """
    suggestions = []
    
    for i, (col_name, col_type) in enumerate(zip(column_names, column_types)):
        base_type = col_type.split('(')[0].lower()
        
        # Date/timestamp columns are good candidates for time-based partitioning
        if base_type == 'date':
            suggestions.append((col_name, 'year', None))
        elif base_type == 'timestamp':
            suggestions.append((col_name, 'month', None))
        
        # String columns might be candidates for identity partitioning if low cardinality
        elif base_type in ('varchar', 'string'):
            # If we have sample data, check cardinality
            if sample_data and len(sample_data) > 0:
                unique_values = set(row[i] for row in sample_data if i < len(row) and row[i] is not None)
                # Suggest identity partitioning only if cardinality is reasonable
                if 2 <= len(unique_values) <= 100:
                    suggestions.append((col_name, None, None))  # Identity partitioning
                elif len(unique_values) > 100:
                    suggestions.append((col_name, 'truncate', 10))  # Truncate for high cardinality
        
        # Integer keys might be candidates for bucketing
        elif base_type in ('int', 'bigint'):
            suggestions.append((col_name, 'bucket', 16))
    
    return suggestions

def get_transform_description(transform: Optional[str]) -> str:
    """
    Get a human-readable description of a partition transform.
    
    Args:
        transform: Transform name or None for identity partitioning
        
    Returns:
        Description of the transform
    """
    descriptions = {
        'year': 'Partition by year (extracts year from a date/timestamp)',
        'month': 'Partition by month (extracts month from a date/timestamp)',
        'day': 'Partition by day (extracts day from a date/timestamp)',
        'hour': 'Partition by hour (extracts hour from a timestamp)',
        'bucket': 'Hash partition into buckets (distributes data evenly)',
        'truncate': 'Truncate values to specified length',
        None: 'Identity partitioning (partition by exact values)'
    }
    
    return descriptions.get(transform, 'Unknown transform')