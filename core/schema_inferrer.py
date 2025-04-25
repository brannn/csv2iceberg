"""
Schema inference module for CSV to Iceberg conversion using Polars
"""
import os
import logging
import datetime
import re
from typing import Dict, List, Any, Optional, Tuple, Set

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

def clean_column_name(col_name: str) -> str:
    """
    Clean a column name to be compatible with Iceberg.
    
    Args:
        col_name: Original column name
        
    Returns:
        Cleaned column name
    """
    if not col_name:
        return "column"
        
    # Replace invalid characters with underscores
    clean_name = "".join(c if c.isalnum() or c == '_' else '_' for c in str(col_name).strip())
    
    # Ensure name starts with a letter or underscore
    if clean_name and not (clean_name[0].isalpha() or clean_name[0] == '_'):
        clean_name = 'col_' + clean_name
    
    # Handle special cases
    if not clean_name or clean_name.lower() in ('table', 'column', 'select', 'where', 'from', 'order', 'group', 'by'):
        clean_name = 'col_' + clean_name
    
    return clean_name

def infer_schema_from_csv(
    csv_file: str, 
    delimiter: str = ',', 
    has_header: bool = True, 
    quote_char: str = '"',
    sample_size: Optional[int] = 1000,
    include_columns: Optional[List[str]] = None,
    exclude_columns: Optional[List[str]] = None
) -> Schema:
    """
    Infer an Iceberg schema from a CSV file using Polars.
    
    Args:
        csv_file: Path to the CSV file
        delimiter: CSV delimiter character
        has_header: Whether the CSV has a header row
        quote_char: CSV quote character
        sample_size: Number of rows to sample for schema inference
        include_columns: List of column names to include (if None, include all except excluded)
        exclude_columns: List of column names to exclude (if None, no exclusions)
        
    Returns:
        PyIceberg Schema object
    """
    logger.info(f"Inferring schema from CSV file using Polars: {csv_file}")
    
    try:
        # First check if the file exists and is accessible
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"CSV file not found: {csv_file}")
        
        # Log column filtering parameters if provided
        if include_columns:
            logger.info(f"Including only these columns: {include_columns}")
        if exclude_columns:
            logger.info(f"Excluding these columns: {exclude_columns}")
        
        # For large CSV files, use sampling to avoid loading the entire file
        file_size = os.path.getsize(csv_file)
        if file_size > 10 * 1024 * 1024:  # 10 MB
            logger.info(f"CSV file size is {file_size/1024/1024:.2f} MB, using efficient sampling")
            # Use default sample size (1000) if none provided
            actual_sample_size = sample_size if sample_size is not None else 1000
            return _infer_schema_from_large_csv(
                csv_file, 
                delimiter, 
                has_header, 
                quote_char, 
                actual_sample_size,
                include_columns,
                exclude_columns
            )
        
        try:
            # Read the CSV file with Polars
            read_args = {
                "separator": delimiter,
                "has_header": has_header,
                "quote_char": quote_char,
                "null_values": ["", "NULL", "null", "NA", "N/A", "na", "n/a", "None", "none"],
                "try_parse_dates": True,  # Try to parse date/datetime columns
                "low_memory": True,
                "ignore_errors": True  # Skip rows with parsing errors
            }
            
            # Add n_rows and infer_schema_length only if sample_size is specified
            if sample_size is not None:
                read_args["n_rows"] = sample_size
                read_args["infer_schema_length"] = sample_size
            
            df = pl.read_csv(csv_file, **read_args)
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
                # Ensure required is explicitly a boolean value to avoid validation errors
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
        field_id = 1
        
        for i, field in enumerate(arrow_schema):
            col_name = field.name
            # Clean column name - remove special characters, spaces, etc.
            clean_col_name = str(col_name).strip()
            
            # Apply column filtering
            if include_columns is not None and clean_col_name not in include_columns:
                logger.debug(f"Skipping column '{clean_col_name}' (not in include list)")
                continue
                
            if exclude_columns is not None and clean_col_name in exclude_columns:
                logger.debug(f"Skipping column '{clean_col_name}' (in exclude list)")
                continue
            
            # Convert PyArrow type to Iceberg type
            iceberg_type = _pyarrow_type_to_iceberg_type(field.type)
            
            # Add field to schema with explicit boolean for required parameter
            fields.append(NestedField(field_id=field_id, name=clean_col_name, field_type=iceberg_type, required=False))
            field_id += 1
        
        # Create the PyIceberg Schema
        schema = Schema(*fields)
        
        logger.info(f"Successfully inferred schema with {len(fields)} fields using Polars")
        return schema
        
    except Exception as e:
        logger.error(f"Error inferring schema from CSV with Polars: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to infer schema from CSV: {str(e)}")

def infer_schema_from_df(df) -> Schema:
    """
    Infer an Iceberg schema from a Pandas or Polars DataFrame.
    
    Args:
        df: DataFrame (pandas or polars)
        
    Returns:
        PyIceberg Schema object
    """
    try:
        logger.info(f"Inferring schema from DataFrame with {len(df.columns)} columns")
        
        # Clean and normalize column names
        clean_columns = [clean_column_name(col) for col in df.columns]
        
        # If it's a pandas DataFrame, convert to PyArrow table
        if hasattr(df, 'to_arrow'):
            # It's a pandas DataFrame
            arrow_table = df.to_arrow()
        elif hasattr(df, 'to_arrow_table'):
            # It's a polars DataFrame
            arrow_table = df.to_arrow_table()
        else:
            # Unknown DataFrame type
            raise ValueError(f"Unsupported DataFrame type: {type(df)}")
        
        # Process the schema
        fields = []
        field_id = 1  # Start field IDs from 1 (0 is reserved in Iceberg)
        
        # Process each column
        for i, field in enumerate(arrow_table.schema):
            # Get clean name
            clean_col_name = clean_columns[i]
            
            # Convert PyArrow type to Iceberg type
            iceberg_type = _pyarrow_type_to_iceberg_type(field.type)
            
            # Add field to schema with explicit boolean for required parameter
            fields.append(NestedField(field_id=field_id, name=clean_col_name, field_type=iceberg_type, required=False))
            field_id += 1
        
        # Create the PyIceberg Schema
        schema = Schema(*fields)
        
        logger.info(f"Successfully inferred schema with {len(fields)} fields from DataFrame")
        return schema
        
    except Exception as e:
        logger.error(f"Error inferring schema from DataFrame: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to infer schema from DataFrame: {str(e)}")

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
    sample_size: Optional[int],
    include_columns: Optional[List[str]] = None,
    exclude_columns: Optional[List[str]] = None
) -> Schema:
    """
    Infer schema from a large CSV file by sampling rows using Polars.
    
    Args:
        csv_file: Path to the CSV file
        delimiter: CSV delimiter character
        has_header: Whether the CSV has a header row
        quote_char: CSV quote character
        sample_size: Number of rows to sample
        include_columns: List of column names to include (if None, include all except excluded)
        exclude_columns: List of column names to exclude (if None, no exclusions)
        
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
        
        if sample_size is None or row_count <= sample_size:
            # For small files or when no sample size is specified, use all rows for schema inference
            return infer_schema_from_csv(csv_file, delimiter, has_header, quote_char, None, include_columns, exclude_columns)
        
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
        # Make sure we have a valid sample_size (should be handled by the check above, but just to be safe)
        if sample_size is None:
            sample_size = 1000
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
        field_id = 1
        
        for i, field in enumerate(arrow_schema):
            col_name = field.name if i < len(column_names) else f"col_{i}"
            clean_col_name = str(col_name).strip()
            
            # Apply column filtering
            if include_columns is not None and clean_col_name not in include_columns:
                logger.debug(f"Skipping column '{clean_col_name}' (not in include list)")
                continue
                
            if exclude_columns is not None and clean_col_name in exclude_columns:
                logger.debug(f"Skipping column '{clean_col_name}' (in exclude list)")
                continue
            
            # Convert PyArrow type to Iceberg type
            iceberg_type = _pyarrow_type_to_iceberg_type(field.type)
            
            # Add field to schema with explicit boolean for required parameter
            fields.append(NestedField(field_id=field_id, name=clean_col_name, field_type=iceberg_type, required=False))
            field_id += 1
        
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


def analyze_column_cardinality(
    csv_file: str,
    delimiter: str = ',',
    has_header: bool = True,
    quote_char: str = '"',
    sample_size: int = 10000,
    schema: Optional[Schema] = None
) -> List[Dict[str, Any]]:
    """
    Analyze column cardinality from a CSV file and recommend partitioning strategies.
    
    Args:
        csv_file: Path to the CSV file
        delimiter: CSV delimiter character
        has_header: Whether the CSV has a header row
        quote_char: CSV quote character
        sample_size: Number of rows to sample for analysis
        schema: Optional pre-inferred schema
        
    Returns:
        List of dictionaries with column recommendations
    """
    logger.info(f"Analyzing column cardinality for partitioning recommendations")
    
    try:
        # Read the CSV file with a limited sample size for analysis
        df = pl.read_csv(
            csv_file,
            separator=delimiter,
            has_header=has_header,
            quote_char=quote_char,
            null_values=["", "NULL", "null", "NA", "N/A", "na", "n/a", "None", "none"],
            try_parse_dates=True,
            n_rows=sample_size,
            low_memory=True,
            ignore_errors=True
        )
        
        # Use the provided schema if available, otherwise get column metadata from DataFrame
        column_types = {}
        if schema:
            for field in schema.fields:
                column_types[field.name] = type(field.field_type).__name__.replace('Type', '').lower()
        
        # Analyze cardinality for each column
        results = []
        for col_name in df.columns:
            try:
                # Count unique values
                unique_values = len(df[col_name].unique())
                total_values = len(df[col_name].drop_nulls())
                null_count = len(df) - total_values
                
                # Skip columns with no data
                if total_values == 0:
                    continue
                    
                # Calculate cardinality ratio
                cardinality_ratio = unique_values / total_values if total_values > 0 else 0
                
                # Get column data type (from schema or inferred)
                if schema and col_name in column_types:
                    col_type = column_types[col_name]
                else:
                    col_type = str(df[col_name].dtype).lower()
                
                # Check date/time patterns in string columns
                date_match = False
                time_patterns = [
                    # ISO date/datetime patterns
                    r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                    r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
                    r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
                    r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
                ]
                
                if 'string' in col_type or 'str' in col_type:
                    # Check for date patterns in sample values
                    sample_values = df[col_name].drop_nulls().head(100)
                    for val in sample_values:
                        if any(re.search(pattern, str(val)) for pattern in time_patterns):
                            date_match = True
                            break
                
                # Recommend partitioning strategy based on data type and cardinality
                recommendation = {
                    'column': col_name,
                    'type': col_type,
                    'unique_values': unique_values,
                    'total_values': total_values,
                    'null_count': null_count,
                    'cardinality_ratio': cardinality_ratio,
                    'suitability_score': 0,  # Will be calculated below
                    'recommendations': []
                }
                
                # Calculate suitability score (0-100)
                # Ideal partitioning columns have moderate cardinality - not too high or too low
                suitability_score = 0
                
                # Timestamp/Date types are ideal for partitioning
                is_datetime = ('date' in col_type or 'timestamp' in col_type or 
                             'time' in col_type or date_match)
                
                if is_datetime:
                    # Date/time columns are generally excellent for partitioning,
                    # but we still need to consider their cardinality
                    
                    # Calculate recommendations based on cardinality
                    if cardinality_ratio >= 0.9:
                        # Extremely high cardinality - likely timestamps with precision
                        # Not ideal for direct partitioning, too many partitions
                        suitability_score = 70
                        recommendation['recommendations'].append({
                            'transform': 'year',
                            'description': 'Partition by year (high cardinality column)',
                            'example': f"PARTITION BY year({col_name})"
                        })
                    elif cardinality_ratio >= 0.3:
                        # High cardinality - good for year/month partitioning
                        suitability_score = 90
                        recommendation['recommendations'].append({
                            'transform': 'year',
                            'description': 'Partition by year',
                            'example': f"PARTITION BY year({col_name})"
                        })
                        recommendation['recommendations'].append({
                            'transform': 'month',
                            'description': 'Partition by month',
                            'example': f"PARTITION BY month({col_name})"
                        })
                    elif cardinality_ratio >= 0.05:
                        # Moderate cardinality - perfect for year/month/day partitioning
                        suitability_score = 95
                        recommendation['recommendations'].append({
                            'transform': 'year',
                            'description': 'Partition by year',
                            'example': f"PARTITION BY year({col_name})"
                        })
                        recommendation['recommendations'].append({
                            'transform': 'month',
                            'description': 'Partition by month',
                            'example': f"PARTITION BY month({col_name})"
                        })
                        recommendation['recommendations'].append({
                            'transform': 'day',
                            'description': 'Partition by day',
                            'example': f"PARTITION BY day({col_name})"
                        })
                    else:
                        # Low cardinality dates - could be sparse dates or low-res dates
                        # Still good but check the exact cardinality
                        if unique_values >= 10:
                            suitability_score = 85
                            recommendation['recommendations'].append({
                                'transform': 'identity',
                                'description': 'Partition directly by date (low cardinality)',
                                'example': f"PARTITION BY {col_name}"
                            })
                        elif unique_values >= 3:
                            suitability_score = 75
                            recommendation['recommendations'].append({
                                'transform': 'identity',
                                'description': 'Partition directly by date (very low cardinality)',
                                'example': f"PARTITION BY {col_name}"
                            })
                        else:
                            # Too few unique dates to be useful
                            suitability_score = 60
                            recommendation['recommendations'].append({
                                'transform': 'identity',
                                'description': 'Limited partitioning value (extremely low cardinality)',
                                'example': f"PARTITION BY {col_name} -- Warning: Very few unique values"
                            })
                
                # Integer/numeric types with moderate cardinality
                elif ('int' in col_type or 'long' in col_type or 'float' in col_type or 
                      'double' in col_type or 'decimal' in col_type):
                    
                    # For numeric columns, we want a moderate number of unique values that's not too low or too high
                    # Ideal range: 0.05-0.5 (5%-50% unique values)
                    # This balances having enough distinct values to make partitioning useful
                    # but not so many that we create too many small partitions
                    
                    # Perfect cardinality is between 0.05 and 0.15 (5% to 15% unique values)
                    if 0.05 <= cardinality_ratio <= 0.15:
                        suitability_score = 85
                    # Very good cardinality is between 0.15 and 0.3 (15% to 30% unique values)
                    elif 0.15 < cardinality_ratio <= 0.3:
                        suitability_score = 80
                    # Good cardinality is between 0.3 and 0.5 (30% to 50% unique values)
                    elif 0.3 < cardinality_ratio <= 0.5:
                        suitability_score = 75
                    # Acceptable cardinality is between 0.01 and 0.05 (1% to 5% unique values)
                    elif 0.01 <= cardinality_ratio < 0.05:
                        suitability_score = 60
                    # Poor cardinality is > 0.5 (too many unique values) or < 0.01 (too few)
                    else:
                        suitability_score = 30
                    
                    # Recommend bucketing for numeric columns with moderate to high cardinality
                    if cardinality_ratio > 0.01:
                        bucket_size = 10 if unique_values < 1000 else 100
                        recommendation['recommendations'].append({
                            'transform': 'bucket',
                            'description': f'Hash into {bucket_size} buckets',
                            'example': f"PARTITION BY bucket({bucket_size}, {col_name})"
                        })
                    # Direct partitioning for lower cardinality
                    else:
                        recommendation['recommendations'].append({
                            'transform': 'identity',
                            'description': 'Partition directly by this column',
                            'example': f"PARTITION BY {col_name}"
                        })
                
                # String types with appropriate cardinality can be good candidates
                elif 'string' in col_type or 'str' in col_type:
                    # Check for value distribution to identify skewed distributions
                    value_distribution = df[col_name].value_counts().to_dict()
                    most_common_count = max(value_distribution.values()) if value_distribution else 0
                    distribution_ratio = most_common_count / total_values if total_values > 0 else 0
                    
                    # Calculate an evenness score (0-1) where 1 means perfectly even distribution
                    # Low score means one value dominates (like "California" in state field)
                    evenness_score = 1.0 - distribution_ratio
                    
                    # For strings, the ideal is to have enough unique values (5-20%) 
                    # that are evenly distributed to create useful partitions
                    
                    # Moderate cardinality strings (5-20% unique) with good distribution
                    if 0.05 <= cardinality_ratio <= 0.2:
                        # Adjust score based on distribution evenness
                        if evenness_score >= 0.7:  # Very even distribution
                            suitability_score = 85  # Perfect candidate
                            recommendation['recommendations'].append({
                                'transform': 'identity',
                                'description': 'Partition directly by this column (excellent distribution)',
                                'example': f"PARTITION BY {col_name}"
                            })
                        elif evenness_score >= 0.5:  # Good distribution
                            suitability_score = 80  # Very good candidate
                            recommendation['recommendations'].append({
                                'transform': 'identity',
                                'description': 'Partition directly by this column (good distribution)',
                                'example': f"PARTITION BY {col_name}"
                            })
                        else:  # Uneven distribution
                            suitability_score = 50  # Less suitable due to skew
                            recommendation['recommendations'].append({
                                'transform': 'bucket',
                                'description': 'Hash into 10 buckets to counter uneven distribution',
                                'example': f"PARTITION BY bucket(10, {col_name})"
                            })
                    
                    # Low cardinality strings (2-5% unique values)
                    # Can be good if values are evenly distributed
                    elif 0.02 <= cardinality_ratio < 0.05 and unique_values < 200:
                        if evenness_score >= 0.6:  # Even distribution
                            suitability_score = 75  # Good candidate
                            recommendation['recommendations'].append({
                                'transform': 'identity',
                                'description': 'Partition directly by this column',
                                'example': f"PARTITION BY {col_name}"
                            })
                        else:  # Uneven distribution
                            suitability_score = 40  # Poor due to skew
                            recommendation['recommendations'].append({
                                'transform': 'identity',
                                'description': 'Not recommended due to uneven distribution',
                                'example': f"PARTITION BY {col_name} -- Warning: Uneven distribution"
                            })
                    
                    # Very low cardinality strings (<2% unique)
                    elif cardinality_ratio < 0.02:
                        # Very low cardinality is generally poor for partitioning
                        # unless exceptionally well-distributed
                        if evenness_score >= 0.8 and unique_values >= 5:
                            suitability_score = 60  # Acceptable only with perfect distribution
                            recommendation['recommendations'].append({
                                'transform': 'identity',
                                'description': 'Partition directly by this column',
                                'example': f"PARTITION BY {col_name}"
                            })
                        else:
                            suitability_score = 20  # Generally poor
                            recommendation['recommendations'].append({
                                'transform': 'identity',
                                'description': 'Not recommended: too few unique values',
                                'example': f"PARTITION BY {col_name} -- Warning: Too few unique values"
                            })
                    
                    # Higher cardinality strings (20-40% unique)
                    elif 0.2 < cardinality_ratio <= 0.4:
                        suitability_score = 70
                        recommendation['recommendations'].append({
                            'transform': 'truncate',
                            'description': 'Truncate to first 5 characters',
                            'example': f"PARTITION BY truncate({col_name}, 5)"
                        })
                        recommendation['recommendations'].append({
                            'transform': 'bucket',
                            'description': 'Hash into 20 buckets',
                            'example': f"PARTITION BY bucket(20, {col_name})"
                        })
                    
                    # Very high cardinality strings (>40% unique)
                    else:
                        suitability_score = 30  # Not ideal for direct partitioning
                        recommendation['recommendations'].append({
                            'transform': 'bucket',
                            'description': 'Hash into 50 buckets',
                            'example': f"PARTITION BY bucket(50, {col_name})"
                        })
                
                # Boolean columns are typically not good for partitioning
                elif 'bool' in col_type:
                    suitability_score = 30 if null_count > 0 else 20
                    # Only recommend if there are nulls (making it a 3-value column)
                    if null_count > 0:
                        recommendation['recommendations'].append({
                            'transform': 'identity',
                            'description': 'Partition by boolean value (only with nulls)',
                            'example': f"PARTITION BY {col_name}"
                        })
                
                # Update the suitability score
                recommendation['suitability_score'] = suitability_score
                
                # Add to results if we have high-quality recommendations (score >= 75)
                if recommendation['recommendations'] and recommendation['suitability_score'] >= 75:
                    results.append(recommendation)
                    
            except Exception as e:
                logger.warning(f"Error analyzing column {col_name}: {str(e)}")
                continue
        
        # Sort by suitability score (descending)
        results.sort(key=lambda x: x['suitability_score'], reverse=True)
        
        # Return top 5 recommendations
        return results[:5]
        
    except Exception as e:
        logger.error(f"Error analyzing column cardinality: {str(e)}", exc_info=True)
        return []