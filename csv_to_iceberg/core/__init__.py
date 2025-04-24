"""
Core modules for CSV to Iceberg conversion
"""
from csv_to_iceberg.utils import (
    get_trino_role_header, get_file_size, is_test_job_id,
    format_duration, format_datetime, format_size, format_status,
    clean_column_name
)
from csv_to_iceberg.core.schema_inferrer import infer_schema_from_csv as infer_schema
from csv_to_iceberg.core.iceberg_writer import IcebergWriter, count_csv_rows