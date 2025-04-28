#!/usr/bin/env python3
"""
CSV to Iceberg - CLI tool for converting CSV files to Iceberg tables using Trino and Hive metastore.
"""
import os
import sys
import logging
from typing import Optional, Tuple

import click
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn

# Use the new package imports
from csv_to_iceberg.core.schema_inferrer import infer_schema_from_csv
from csv_to_iceberg.connectors.trino_client import TrinoClient
from csv_to_iceberg.connectors.hive_client import HiveMetastoreClient
from csv_to_iceberg.connectors.s3tables_client import S3TablesClient
from csv_to_iceberg.core.iceberg_writer import IcebergWriter
from csv_to_iceberg.utils import setup_logging, validate_csv_file, validate_connection_params

# Initialize console for rich output
console = Console()

# Set up logging
logger = setup_logging()

@click.group()
@click.version_option(version="1.0.0")
def cli():
    """
    CSV to Iceberg - CLI tool for converting CSV files to Iceberg tables.
    
    This tool allows converting CSV files to Iceberg tables using Trino and Hive metastore,
    or directly to S3 Tables.
    """
    pass

@cli.command()
@click.option('--csv-file', '-f', required=True, help='Path to the CSV file')
@click.option('--delimiter', '-d', default=',', help='CSV delimiter (default: comma)')
@click.option('--has-header', '-h', is_flag=True, default=True, help='CSV has header row (default: True)')
@click.option('--quote-char', '-q', default='"', help='CSV quote character (default: double quote)')
@click.option('--batch-size', '-b', default=10000, type=int, help='Batch size for processing (default: 10000)')
@click.option('--table-name', '-t', required=True, help='Target Iceberg table name (format: catalog.schema.table)')
@click.option('--profile-type', type=click.Choice(['trino', 's3tables']), default='trino', 
              help='Type of connection profile (default: trino)')
@click.option('--trino-host', help='Trino host (required for trino profile)')
@click.option('--trino-port', default=443, help='Trino port (default: 443)')
@click.option('--trino-user', default=os.getenv('USER', 'admin'), help='Trino user')
@click.option('--trino-password', help='Trino password (if authentication is enabled)')
@click.option('--http-scheme', type=click.Choice(['http', 'https']), default='https', 
              help='HTTP scheme for Trino connection (http or https, default: https)')
@click.option('--trino-role', default='sysadmin', help='Trino role for authorization (default: sysadmin)')
@click.option('--trino-catalog', help='Trino catalog (required for trino profile)')
@click.option('--trino-schema', help='Trino schema (required for trino profile)')
@click.option('--hive-metastore-uri', default="localhost:9083", help='Hive metastore Thrift URI')
@click.option('--use-hive-metastore/--no-hive-metastore', default=True, help='Use direct Hive Metastore connection')
@click.option('--region', help='AWS region (required for s3tables profile)')
@click.option('--table-bucket-arn', help='S3 bucket ARN for tables (required for s3tables profile)')
@click.option('--namespace', help='S3 Tables namespace (required for s3tables profile)')
@click.option('--aws-access-key-id', help='AWS access key ID (optional)')
@click.option('--aws-secret-access-key', help='AWS secret access key (optional)')
@click.option('--mode', '-m', type=click.Choice(['append', 'overwrite']), default='append', 
              help='Write mode (append or overwrite, default: append)')
@click.option('--sample-size', default=1000, help='Number of rows to sample for schema inference (default: 1000)')
@click.option('--custom-schema', help='Path to a JSON file containing a custom schema definition')
@click.option('--include-columns', help='Comma-separated list of column names to include (overrides exclude-columns)')
@click.option('--exclude-columns', help='Comma-separated list of column names to exclude')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def convert(csv_file: str, delimiter: str, has_header: bool, quote_char: str, batch_size: int,
            table_name: str, profile_type: str, trino_host: Optional[str], trino_port: int, 
            trino_user: str, trino_password: Optional[str], http_scheme: str, trino_role: str, 
            trino_catalog: Optional[str], trino_schema: Optional[str], hive_metastore_uri: str,
            use_hive_metastore: bool, region: Optional[str], table_bucket_arn: Optional[str],
            namespace: Optional[str], aws_access_key_id: Optional[str], 
            aws_secret_access_key: Optional[str], mode: str, sample_size: int, 
            custom_schema: Optional[str], include_columns: Optional[str], 
            exclude_columns: Optional[str], verbose: bool):
    """
    Convert a CSV file to an Iceberg table.
    
    This command reads a CSV file, infers its schema, creates an Iceberg table,
    and loads the data into the table using either Trino and Hive metastore,
    or directly to S3 Tables.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Validate CSV file
        if not validate_csv_file(csv_file, delimiter, quote_char):
            console.print(f"[bold red]Error:[/bold red] Invalid CSV file: {csv_file}")
            sys.exit(1)
        
        # Validate profile-specific parameters
        if profile_type == 'trino':
            if not all([trino_host, trino_catalog, trino_schema]):
                console.print("[bold red]Error:[/bold red] Missing required Trino parameters")
                sys.exit(1)
            if not validate_connection_params(trino_host, trino_port, hive_metastore_uri, use_hive_metastore):
                console.print("[bold red]Error:[/bold red] Invalid Trino connection parameters")
                sys.exit(1)
        else:  # s3tables
            if not all([region, table_bucket_arn, namespace]):
                console.print("[bold red]Error:[/bold red] Missing required S3 Tables parameters")
                sys.exit(1)
            
        # Parse table name components
        if profile_type == 'trino':
            catalog, schema, table = parse_table_name(table_name)
            if not catalog or not schema or not table:
                console.print("[bold red]Error:[/bold red] Invalid table name format. Use: catalog.schema.table")
                sys.exit(1)
        else:  # s3tables
            table = table_name  # For S3 Tables, we use the simple table name
        
        # Get schema (either from custom schema file or by inference)
        if custom_schema:
            with console.status("[bold blue]Loading custom schema...[/bold blue]") as status:
                try:
                    logger.info(f"Loading custom schema from file: {custom_schema}")
                    import json
                    from pyiceberg.schema import Schema
                    from pyiceberg.types import (
                        BooleanType, IntegerType, LongType, FloatType, DoubleType,
                        DateType, TimestampType, StringType, DecimalType, NestedField
                    )
                    
                    # Map string type names to actual PyIceberg types
                    type_mapping = {
                        'Boolean': BooleanType(),
                        'Integer': IntegerType(),
                        'Long': LongType(),
                        'Float': FloatType(),
                        'Double': DoubleType(),
                        'Date': DateType(),
                        'Timestamp': TimestampType(),
                        'String': StringType(),
                        'Decimal': DecimalType(38, 10)  # Default precision and scale
                    }
                    
                    with open(custom_schema, 'r') as f:
                        schema_data = json.load(f)
                    
                    # Create schema fields from the custom schema definition
                    fields = []
                    for field_def in schema_data:
                        field_id = field_def.get('id', 0)
                        field_name = field_def.get('name', '')
                        field_type_name = field_def.get('type', 'String')
                        field_required = field_def.get('required', False)
                        
                        # Get the actual PyIceberg type from the mapping
                        field_type = type_mapping.get(field_type_name, StringType())
                        
                        # Add field to schema
                        fields.append(NestedField(
                            field_id=field_id, 
                            name=field_name, 
                            field_type=field_type, 
                            required=field_required
                        ))
                    
                    # Create the schema
                    iceberg_schema = Schema(*fields)
                    logger.debug(f"Loaded custom schema: {iceberg_schema}")
                except Exception as e:
                    logger.error(f"Error loading custom schema: {str(e)}", exc_info=True)
                    console.print(f"[bold red]Error:[/bold red] Failed to load custom schema: {str(e)}")
                    sys.exit(1)
            console.print(f"[bold green]✓[/bold green] Custom schema loaded successfully")
        else:
            # 1. Infer schema from CSV
            # Parse include/exclude columns lists if provided
            include_cols = None
            exclude_cols = None
            
            if include_columns:
                include_cols = [col.strip() for col in include_columns.split(',')]
                logger.info(f"Including only these columns: {include_cols}")
                
            if exclude_columns and not include_cols:  # Include columns takes precedence
                exclude_cols = [col.strip() for col in exclude_columns.split(',')]
                logger.info(f"Excluding these columns: {exclude_cols}")
                
            with console.status("[bold blue]Inferring schema from CSV...[/bold blue]") as status:
                logger.info(f"Inferring schema from CSV file: {csv_file}")
                iceberg_schema = infer_schema_from_csv(
                    csv_file=csv_file,
                    delimiter=delimiter, 
                    has_header=has_header,
                    quote_char=quote_char,
                    sample_size=sample_size,
                    include_columns=include_cols,
                    exclude_columns=exclude_cols
                )
                logger.debug(f"Inferred schema: {iceberg_schema}")
            console.print(f"[bold green]✓[/bold green] Schema inferred successfully")
        
        if profile_type == 'trino':
            # 2. Connect to Trino
            with console.status("[bold blue]Connecting to Trino...[/bold blue]") as status:
                logger.info(f"Connecting to Trino at {trino_host}:{trino_port}")
                trino_client = TrinoClient(
                    host=trino_host,
                    port=trino_port,
                    user=trino_user,
                    password=trino_password,
                    catalog=trino_catalog,
                    schema=trino_schema,
                    http_scheme=http_scheme,
                    role=trino_role
                )
            console.print(f"[bold green]✓[/bold green] Connected to Trino")
            
            # 3. Connect to Hive metastore (if enabled)
            hive_client = None
            if use_hive_metastore:
                with console.status("[bold blue]Connecting to Hive metastore...[/bold blue]") as status:
                    logger.info(f"Connecting to Hive metastore at {hive_metastore_uri}")
                    try:
                        hive_client = HiveMetastoreClient(hive_metastore_uri)
                        console.print(f"[bold green]✓[/bold green] Connected to Hive metastore")
                    except Exception as e:
                        logger.warning(f"Failed to connect to Hive metastore: {str(e)}")
                        console.print(f"[bold yellow]![/bold yellow] Could not connect to Hive metastore: {str(e)}")
                        console.print(f"[bold yellow]![/bold yellow] Continuing without direct Hive metastore connection")
            else:
                logger.info("Hive metastore connection disabled via --no-hive-metastore flag")
                console.print("[bold blue]i[/bold blue] Hive metastore connection disabled, using Trino for all operations")
            
            # 4. Create Iceberg table or verify it exists
            with console.status(f"[bold blue]Creating/verifying Iceberg table {table_name}...[/bold blue]") as status:
                table_exists = trino_client.table_exists(catalog, schema, table)
                
                if table_exists and mode == 'overwrite':
                    logger.info(f"Table {table_name} exists and mode is overwrite, dropping table")
                    trino_client.drop_table(catalog, schema, table)
                    table_exists = False
                
                if not table_exists:
                    logger.info(f"Creating table {table_name}")
                    trino_client.create_iceberg_table(catalog, schema, table, iceberg_schema)
                else:
                    logger.info(f"Table {table_name} already exists, verifying schema compatibility")
                    if not trino_client.validate_table_schema(catalog, schema, table, iceberg_schema):
                        console.print("[bold red]Error:[/bold red] Existing table schema is incompatible with inferred schema")
                        sys.exit(1)
            console.print(f"[bold green]✓[/bold green] Iceberg table ready")
            
            # 5. Write data to Iceberg table
            logger.info(f"Writing data from {csv_file} to {table_name} in {mode} mode")
            writer = IcebergWriter(
                trino_client=trino_client,
                hive_client=hive_client,
                catalog=catalog,
                schema=schema,
                table=table
            )
            
            # Simple progress tracking without Rich Progress
            def progress_update(percent):
                console.print(f"[bold blue]Writing data: {percent}% complete[/bold blue]")
                
            writer.write_csv_to_iceberg(
                csv_file=csv_file,
                mode=mode,
                delimiter=delimiter,
                has_header=has_header,
                quote_char=quote_char,
                batch_size=batch_size,
                include_columns=include_cols,
                exclude_columns=exclude_cols,
                progress_callback=progress_update
            )
            
        else:  # s3tables
            # 2. Create S3 Tables client
            with console.status("[bold blue]Connecting to S3 Tables...[/bold blue]") as status:
                logger.info(f"Connecting to AWS region: {region}")
                s3tables_client = S3TablesClient(
                    endpoint=f"https://{region}.amazonaws.com",
                    region=region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )
            console.print(f"[bold green]✓[/bold green] Connected to S3 Tables")
            
            # 3. Create table if it doesn't exist
            with console.status(f"[bold blue]Creating/verifying S3 Tables table {namespace}.{table}...[/bold blue]") as status:
                table_exists = s3tables_client.table_exists(namespace, table)
                
                if table_exists and mode == 'overwrite':
                    logger.info(f"Table {namespace}.{table} exists and mode is overwrite, dropping table")
                    s3tables_client.delete_table(namespace, table)
                    table_exists = False
                
                if not table_exists:
                    logger.info(f"Creating table {namespace}.{table}")
                    s3tables_client.create_table(
                        namespace=namespace,
                        table_name=table,
                        schema=iceberg_schema
                    )
            console.print(f"[bold green]✓[/bold green] S3 Tables table ready")
            
            # 4. Write data to S3 Tables
            logger.info(f"Writing data from {csv_file} to {namespace}.{table} in {mode} mode")
            
            # Simple progress tracking without Rich Progress
            def progress_update(percent):
                console.print(f"[bold blue]Writing data: {percent}% complete[/bold blue]")
            
            # Use the conversion service for S3 Tables
            from csv_to_iceberg.core.conversion_service import convert_csv_to_iceberg
            
            result = convert_csv_to_iceberg(
                csv_file=csv_file,
                table_name=table,
                profile_type='s3tables',
                region=region,
                table_bucket_arn=table_bucket_arn,
                namespace=namespace,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                delimiter=delimiter,
                has_header=has_header,
                quote_char=quote_char,
                batch_size=batch_size,
                mode=mode,
                sample_size=sample_size,
                include_columns=include_cols,
                exclude_columns=exclude_cols,
                progress_callback=progress_update
            )
            
            if not result['success']:
                console.print(f"[bold red]Error:[/bold red] {result['error']}")
                sys.exit(1)
            
            console.print(f"[bold green]✓[/bold green] Successfully wrote {result['rows_processed']} rows to {namespace}.{table}")
            
    except Exception as e:
        logger.error(f"Error during conversion: {str(e)}", exc_info=True)
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)

def parse_table_name(table_name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse a table name in the format catalog.schema.table."""
    parts = table_name.split('.')
    if len(parts) != 3:
        return None, None, None
    return parts[0], parts[1], parts[2]

# Export the CLI function as main for easy importing
main = cli

if __name__ == '__main__':
    cli()
