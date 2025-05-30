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

# Use the flat structure imports
from core.schema_inferrer import infer_schema_from_csv
from connectors.trino_client import TrinoClient
from connectors.hive_client import HiveMetastoreClient
from core.iceberg_writer import IcebergWriter
from utils import setup_logging, validate_csv_file, validate_connection_params

# Initialize console for rich output
console = Console()

# Set up logging
logger = setup_logging()

@click.group()
@click.version_option(version="1.0.0")
def cli():
    """
    CSV to Iceberg - CLI tool for converting CSV files to Iceberg tables.
    
    This tool allows converting CSV files to Iceberg tables using Trino and Hive metastore.
    """
    pass

@cli.command()
@click.option('--csv-file', '-f', required=True, help='Path to the CSV file')
@click.option('--delimiter', '-d', default=',', help='CSV delimiter (default: comma)')
@click.option('--has-header', '-h', is_flag=True, default=True, help='CSV has header row (default: True)')
@click.option('--quote-char', '-q', default='"', help='CSV quote character (default: double quote)')
@click.option('--batch-size', '-b', default=10000, type=int, help='Batch size for processing (default: 10000)')
@click.option('--table-name', '-t', required=True, help='Target Iceberg table name (format: catalog.schema.table)')
@click.option('--trino-host', required=True, help='Trino host')
@click.option('--trino-port', default=443, help='Trino port (default: 443)')
@click.option('--trino-user', default=os.getenv('USER', 'admin'), help='Trino user')
@click.option('--trino-password', help='Trino password (if authentication is enabled)')
@click.option('--http-scheme', type=click.Choice(['http', 'https']), default='https', 
              help='HTTP scheme for Trino connection (http or https, default: https)')
@click.option('--trino-role', default='sysadmin', help='Trino role for authorization (default: sysadmin)')
@click.option('--trino-catalog', required=True, help='Trino catalog')
@click.option('--trino-schema', required=True, help='Trino schema')
@click.option('--hive-metastore-uri', default="localhost:9083", help='Hive metastore Thrift URI')
@click.option('--use-hive-metastore/--no-hive-metastore', default=True, help='Use direct Hive Metastore connection')
@click.option('--mode', '-m', type=click.Choice(['append', 'overwrite']), default='append', 
              help='Write mode (append or overwrite, default: append)')
@click.option('--sample-size', default=1000, help='Number of rows to sample for schema inference (default: 1000)')
@click.option('--custom-schema', help='Path to a JSON file containing a custom schema definition')
@click.option('--include-columns', help='Comma-separated list of column names to include (overrides exclude-columns)')
@click.option('--exclude-columns', help='Comma-separated list of column names to exclude')
@click.option('--max-query-size', default=700000, type=int, help='Maximum SQL query size in bytes (default: 700000, 70% of Trino\'s 1MB limit)')
@click.option('--dry-run', is_flag=True, help='Run in dry run mode - report queries without executing them')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def convert(csv_file: str, delimiter: str, has_header: bool, quote_char: str, batch_size: int,
            table_name: str, trino_host: str, trino_port: int, trino_user: str, trino_password: Optional[str],
            http_scheme: str, trino_role: str, trino_catalog: str, trino_schema: str, hive_metastore_uri: str,
            use_hive_metastore: bool, mode: str, sample_size: int, custom_schema: Optional[str], 
            include_columns: Optional[str], exclude_columns: Optional[str], max_query_size: int,
            dry_run: bool, verbose: bool):
    """
    Convert a CSV file to an Iceberg table.
    
    This command reads a CSV file, infers its schema, creates an Iceberg table,
    and loads the data into the table using Trino and Hive metastore.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Validate CSV file
        if not validate_csv_file(csv_file, delimiter, quote_char):
            console.print(f"[bold red]Error:[/bold red] Invalid CSV file: {csv_file}")
            sys.exit(1)
        
        # Validate connection parameters
        if not validate_connection_params(trino_host, trino_port, hive_metastore_uri, use_hive_metastore):
            console.print("[bold red]Error:[/bold red] Invalid connection parameters")
            sys.exit(1)
            
        # Parse table name components
        catalog, schema, table = parse_table_name(table_name)
        if not catalog or not schema or not table:
            console.print("[bold red]Error:[/bold red] Invalid table name format. Use: catalog.schema.table")
            sys.exit(1)
        
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
                role=trino_role,
                dry_run=dry_run
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
            
        if dry_run:
            console.print("[bold blue]Running in DRY RUN mode...[/bold blue]")
            console.print("Queries will be collected but not executed against the database.")
            
        writer.write_csv_to_iceberg(
            csv_file=csv_file,
            mode=mode,
            delimiter=delimiter,
            has_header=has_header,
            quote_char=quote_char,
            batch_size=batch_size,
            include_columns=include_cols,
            exclude_columns=exclude_cols,
            progress_callback=progress_update,
            dry_run=dry_run,
            max_query_size=max_query_size
        )
        
        if dry_run:
            console.print("[bold green]✓[/bold green] Dry run completed successfully")
            console.print("[bold blue]Summary of operations that would be performed:[/bold blue]")
            if hasattr(writer, 'dry_run_results'):
                stats = writer.dry_run_results.get('stats', {})
                console.print(f"  - Total rows that would be processed: {stats.get('total_rows', 0)}")
                console.print(f"  - Number of batches: {stats.get('batches', 0)}")
                console.print(f"  - Tables that would be created: {stats.get('tables_created', 0)}")
                console.print(f"  - Tables that would be modified: {stats.get('tables_modified', 0)}")
                console.print(f"  - Estimated execution time: {stats.get('estimated_execution_time', 0):.2f} seconds")
            return
        
        console.print(f"[bold green]✓[/bold green] Data written successfully to {table_name}")
        
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
