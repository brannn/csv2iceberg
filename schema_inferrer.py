"""
Schema inference module for CSV to Iceberg conversion using Polars
"""
import os
import logging
import datetime
from typing import Dict, List, Any, Optional, Tuple

import polars as pl
import pyarrow as pa

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

logger = logging.getLogger(__name__)

def infer_schema_from_csv(
    csv_file: str, 
    delimiter: str = ',', 
    has_header: bool = True, 
    quote_char: str = '"',
    sample_size: int = 1000
) -> Schema:
    """
    Infer an Iceberg schema from a CSV file using Polars.
    
    Args:
        csv_file: Path to the CSV file
        delimiter: CSV delimiter character
        has_header: Whether the CSV has a header row
        quote_char: CSV quote character
        sample_size: Number of rows to sample for schema inference
        
    Returns:
        PyIceberg Schema object
    """
    logger.info(f"Inferring schema from CSV file using Polars: {csv_file}")
    
    try:
        # First check if the file exists and is accessible
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"CSV file not found: {csv_file}")
        
        # For large CSV files, use sampling to avoid loading the entire file
        file_size = os.path.getsize(csv_file)
        if file_size > 10 * 1024 * 1024:  # 10 MB
            logger.info(f"CSV file size is {file_size/1024/1024:.2f} MB, using efficient sampling")
            return _infer_schema_from_large_csv(csv_file, delimiter, has_header, quote_char, sample_size)
        
        try:
            # Read the CSV file with Polars
            df = pl.read_csv(
                csv_file,
                separator=delimiter,
                has_header=has_header,
                quote_char=quote_char,
                n_rows=sample_size,
                null_values=["", "NULL", "null", "NA", "N/A", "na", "n/a", "None", "none"],
                infer_schema_length=sample_size,  # Use all sampled rows for schema inference
                try_parse_dates=True,  # Try to parse date/datetime columns
                low_memory=True,
                ignore_errors=True  # Skip rows with parsing errors
            )
        except Exception as e:
            logger.warning(f"Error reading CSV with Polars: {str(e)}")
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
                fields.append(NestedField(field_id=i+1, name=name, field_type=StringType(), required=False))
                
            schema = Schema(*fields)
            logger.info(f"Created fallback schema with {len(fields)} string columns")
            return schema
        
        # If no header was provided, generate column names
        if not has_header:
            df.columns = [f"col_{i}" for i in range(len(df.columns))]
        
        # Convert to PyArrow table to leverage better type system for Iceberg
        arrow_table = df.to_arrow()
        arrow_schema = arrow_table.schema
        
        # Create a schema fields list for PyIceberg
        fields = []
        
        for i, field in enumerate(arrow_schema):
            col_name = field.name
            # Clean column name - remove special characters, spaces, etc.
            clean_col_name = str(col_name).strip()
            
            # Convert PyArrow type to Iceberg type
            iceberg_type = _pyarrow_type_to_iceberg_type(field.type)
            
            # Add field to schema (all are optional by default)
            fields.append(NestedField(field_id=i+1, name=clean_col_name, field_type=iceberg_type, required=False))
        
        # Create the PyIceberg Schema
        schema = Schema(*fields)
        
        logger.info(f"Successfully inferred schema with {len(fields)} fields using Polars")
        return schema
        
    except Exception as e:
        logger.error(f"Error inferring schema from CSV with Polars: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to infer schema from CSV: {str(e)}")

def _pyarrow_type_to_iceberg_type(pa_type: pa.DataType) -> Any:
    """
    Convert a PyArrow type to an Iceberg type.
    
    Args:
        pa_type: PyArrow data type
        
    Returns:
        Iceberg type
    """
    # Boolean type
    if pa.types.is_boolean(pa_type):
        return BooleanType()
    
    # Integer types
    elif pa.types.is_int8(pa_type) or pa.types.is_int16(pa_type) or pa.types.is_int32(pa_type):
        return IntegerType()
    elif pa.types.is_int64(pa_type) or pa.types.is_uint32(pa_type) or pa.types.is_uint64(pa_type):
        return LongType()
    
    # Floating point types
    elif pa.types.is_float32(pa_type):
        return FloatType()
    elif pa.types.is_float64(pa_type):
        return DoubleType()
    
    # Temporal types
    elif pa.types.is_date(pa_type):
        return DateType()
    elif pa.types.is_timestamp(pa_type):
        return TimestampType()
    
    # Decimal type
    elif pa.types.is_decimal(pa_type):
        precision = pa_type.precision
        scale = pa_type.scale
        return DecimalType(precision=precision, scale=scale)
    
    # List type - convert to string for compatibility
    elif pa.types.is_list(pa_type):
        logger.warning(f"Converting list type to string in schema inference")
        return StringType()
    
    # Struct type - convert to string for compatibility
    elif pa.types.is_struct(pa_type):
        logger.warning(f"Converting struct type to string in schema inference")
        return StringType()
    
    # Binary type - convert to string for compatibility
    elif pa.types.is_binary(pa_type):
        logger.warning(f"Converting binary type to string in schema inference")
        return StringType()
    
    # Default case: string type
    else:
        return StringType()

def _infer_schema_from_large_csv(
    csv_file: str, 
    delimiter: str,
    has_header: bool,
    quote_char: str,
    sample_size: int
) -> Schema:
    """
    Infer schema from a large CSV file by sampling rows using Polars.
    
    Args:
        csv_file: Path to the CSV file
        delimiter: CSV delimiter character
        has_header: Whether the CSV has a header row
        quote_char: CSV quote character
        sample_size: Number of rows to sample
        
    Returns:
        PyIceberg Schema object
    """
    try:
        # Get the total number of rows in the CSV file
        # Use Polars streaming capabilities to count rows efficiently
        row_count = pl.scan_csv(
            csv_file,
            separator=delimiter,
            has_header=has_header,
            quote_char=quote_char,
            low_memory=True,
            ignore_errors=True
        ).select(pl.count()).collect().item()
        
        if has_header and row_count > 0:
            row_count -= 1  # Exclude header row from count
        
        if row_count <= sample_size:
            # For small files, use all rows for schema inference
            return infer_schema_from_csv(csv_file, delimiter, has_header, quote_char, None)
        
        # First read the header to get column names
        df_header = pl.read_csv(
            csv_file,
            separator=delimiter,
            has_header=has_header,
            quote_char=quote_char,
            n_rows=1
        )
        
        column_names = df_header.columns
        
        # Calculate a reasonable sample factor
        sample_factor = max(0.01, min(1.0, sample_size / row_count))
        
        # Use Polars lazy API to sample the data efficiently
        df_sampled = pl.scan_csv(
            csv_file,
            separator=delimiter,
            has_header=has_header,
            quote_char=quote_char,
            null_values=["", "NULL", "null", "NA", "N/A", "na", "n/a", "None", "none"],
            infer_schema_length=sample_size,
            try_parse_dates=True,
            low_memory=True,
            ignore_errors=True
        ).sample(fraction=sample_factor, seed=42)
        
        # Collect the sample
        df = df_sampled.collect(streaming=True)
        
        # If no header was provided, generate column names
        if not has_header:
            df.columns = [f"col_{i}" for i in range(len(df.columns))]
        
        # Convert to PyArrow table for schema inference
        arrow_table = df.to_arrow()
        arrow_schema = arrow_table.schema
        
        # Create schema fields
        fields = []
        for i, field in enumerate(arrow_schema):
            col_name = field.name if i < len(column_names) else f"col_{i}"
            clean_col_name = str(col_name).strip()
            
            # Convert PyArrow type to Iceberg type
            iceberg_type = _pyarrow_type_to_iceberg_type(field.type)
            
            # Add field to schema
            fields.append(NestedField(field_id=i+1, name=clean_col_name, field_type=iceberg_type, required=False))
        
        # Create the PyIceberg Schema
        schema = Schema(*fields)
        
        logger.info(f"Successfully inferred schema with {len(fields)} fields from sampled data using Polars")
        return schema
            
    except Exception as e:
        logger.error(f"Error inferring schema from large CSV with Polars: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to infer schema from large CSV: {str(e)}")

def validate_iceberg_schema(schema: Schema) -> bool:
    """
    Validate an Iceberg schema for common issues.
    
    Args:
        schema: PyIceberg Schema
        
    Returns:
        True if the schema is valid
    """
    if not schema or not schema.fields:
        logger.error("Schema is empty or has no fields")
        return False
        
    # Check for duplicate column names
    col_names = [field.name for field in schema.fields]
    if len(col_names) != len(set(col_names)):
        duplicates = [name for name in col_names if col_names.count(name) > 1]
        logger.error(f"Schema contains duplicate column names: {duplicates}")
        return False
    
    # Check for duplicate field IDs
    field_ids = [field.field_id for field in schema.fields]
    if len(field_ids) != len(set(field_ids)):
        duplicates = [id for id in field_ids if field_ids.count(id) > 1]
        logger.error(f"Schema contains duplicate field IDs: {duplicates}")
        return False
    
    # Check for unsupported types or structures
    for field in schema.fields:
        if isinstance(field.field_type, StructType):
            logger.warning(f"Column '{field.name}' has struct type which may not be fully supported")
        
    return True
