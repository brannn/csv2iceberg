"""
Schema inference module for CSV to Iceberg conversion
"""
import os
import sys
import logging
import time
import datetime
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple

# Import PyIceberg Schema and type classes
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DateType,
    TimestampType,
    StringType,
    DecimalType,
    StructType,
    NestedField
)

# Create Field alias for backward compatibility
Field = NestedField

logger = logging.getLogger(__name__)

def infer_schema_from_csv(
    csv_file: str, 
    delimiter: str = ',', 
    has_header: bool = True, 
    quote_char: str = '"',
    sample_size: int = 1000
) -> Schema:
    """
    Infer an Iceberg schema from a CSV file.
    
    Args:
        csv_file: Path to the CSV file
        delimiter: CSV delimiter character
        has_header: Whether the CSV has a header row
        quote_char: CSV quote character
        sample_size: Number of rows to sample for schema inference
        
    Returns:
        PyIceberg Schema object
    """
    logger.info(f"Inferring schema from {csv_file} (sampling {sample_size} rows)")
    
    try:
        # First check if the file exists and is accessible
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"CSV file not found: {csv_file}")
        
        # Use pandas to read a sample of the CSV file
        header = 0 if has_header else None
        try:
            # Try to read with pandas to get column names and infer types
            # Note: on_bad_lines='skip' only skips rows with inconsistent field counts
            # Empty fields are still properly handled and converted to NULL values
            df = pd.read_csv(
                csv_file, 
                delimiter=delimiter,
                quotechar=quote_char,
                header=header,
                nrows=sample_size,
                low_memory=False,      # Disable low memory warnings
                on_bad_lines='skip',   # Skip only lines with inconsistent number of fields
                warn_bad_lines=True,   # Warn about skipped lines
                keep_default_na=True,  # Convert empty fields to NaN (which become NULL)
                na_values=['', 'NULL', 'null', 'NA', 'N/A', 'na', 'n/a', 'None', 'none']  # Additional values to treat as NULL
            )
        except Exception as e:
            logger.warning(f"Error reading CSV with pandas: {str(e)}")
            # Fallback to simple file reading for column names
            with open(csv_file, 'r') as f:
                first_line = f.readline().strip()
                
            if has_header:
                column_names = [col.strip() for col in first_line.split(delimiter)]
            else:
                # Generate default column names based on delimiter count
                col_count = first_line.count(delimiter) + 1
                column_names = [f"col_{i}" for i in range(col_count)]
                
            # Create fields with string type as fallback
            fields = []
            for i, name in enumerate(column_names):
                # Try to create a field with the appropriate parameters
                try:
                    # Using the real PyIceberg NestedField which requires named parameters
                    fields.append(NestedField(field_id=i+1, required=True, name=name, field_type=StringType()))
                except (TypeError, ValueError):
                    # Fallback to using positional arguments
                    fields.append(Field(i+1, name, StringType()))
                
            schema = Schema(*fields)
            logger.info(f"Created fallback schema with {len(fields)} string columns")
            return schema
        
        # Get column names - use pandas column names
        column_names = df.columns.tolist()
        
        # Map pandas dtypes to Iceberg types
        fields = []
        for i, (name, dtype) in enumerate(zip(column_names, df.dtypes)):
            field_id = i + 1
            iceberg_type = _pandas_dtype_to_iceberg_type(dtype, df[name])
            
            # Try to create a field with the appropriate parameters
            try:
                # Using the real PyIceberg NestedField with named parameters
                fields.append(NestedField(field_id=field_id, required=True, name=name, field_type=iceberg_type))
            except (TypeError, ValueError):
                # Fallback to using positional arguments or custom Field
                try:
                    fields.append(Field(field_id, name, iceberg_type))
                except (TypeError, ValueError):
                    # Last resort fallback
                    fields.append(Field(field_id, name, StringType()))
        
        schema = Schema(*fields)
        logger.info(f"Inferred schema with {len(fields)} columns")
        
        return schema
        
    except Exception as e:
        logger.error(f"Error inferring schema: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to infer schema: {str(e)}")
        
def _pandas_dtype_to_iceberg_type(dtype, series) -> Any:
    """
    Convert a pandas dtype to an Iceberg type.
    
    Args:
        dtype: pandas dtype
        series: The pandas Series with this dtype
        
    Returns:
        Iceberg type
    """
    dtype_name = str(dtype)
    
    # Check for numeric types
    if 'int' in dtype_name:
        if dtype_name in ['int8', 'int16', 'int32']:
            return IntegerType()
        else:
            return LongType()
    elif 'float' in dtype_name:
        return DoubleType()
    elif 'bool' in dtype_name:
        return BooleanType()
    elif 'datetime' in dtype_name:
        return TimestampType()
    elif 'date' in dtype_name:
        return DateType()
    else:
        # For object/string types, check if they're actually dates/timestamps
        if series.dtype == 'object':
            # Try to parse as date/timestamp
            try:
                # If more than 80% of non-null values can be parsed as timestamps
                non_null = series.dropna()
                if len(non_null) > 0:
                    success = 0
                    for val in non_null.head(min(100, len(non_null))):
                        try:
                            pd.to_datetime(val)
                            success += 1
                        except (ValueError, TypeError):
                            pass
                    
                    if success / len(non_null.head(min(100, len(non_null)))) > 0.8:
                        return TimestampType()
            except Exception:
                pass
    
    # Default to string for anything else
    return StringType()

def _infer_schema_from_large_csv(
    csv_file: str, 
    delimiter: str,
    has_header: bool,
    quote_char: str,
    sample_size: int
) -> Schema:
    """
    Infer schema from a large CSV file by sampling rows.
    
    Args:
        csv_file: Path to the CSV file
        delimiter: CSV delimiter character
        has_header: Whether the CSV has a header row
        quote_char: CSV quote character
        sample_size: Number of rows to sample
        
    Returns:
        PyIceberg Schema object
    """
    # For the demo, we'll use the same implementation as the regular method
    return infer_schema_from_csv(csv_file, delimiter, has_header, quote_char, sample_size)

def validate_iceberg_schema(schema: Schema) -> bool:
    """
    Validate an Iceberg schema for common issues.
    
    Args:
        schema: PyIceberg Schema
        
    Returns:
        True if the schema is valid
    """
    # Validate that field IDs are unique
    field_ids = set()
    for field in schema.fields:
        if field.field_id in field_ids:
            logger.error(f"Duplicate field ID: {field.field_id}")
            return False
        field_ids.add(field.field_id)
    
    # Validate that field names are valid
    for field in schema.fields:
        if not field.name or not isinstance(field.name, str):
            logger.error(f"Invalid field name: {field.name}")
            return False
    
    return True
