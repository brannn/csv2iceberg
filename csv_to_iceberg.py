#!/usr/bin/env python3
"""
CSV to Iceberg - CLI tool for converting CSV files to Iceberg tables using Trino and Hive metastore.
"""
import os
import sys
import logging
from typing import Optional, Tuple, List

import click
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn

from schema_inferrer import infer_schema_from_csv
from trino_client import TrinoClient, iceberg_type_to_trino_type
from hive_client import HiveMetastoreClient
from iceberg_writer import IcebergWriter
from utils import setup_logging, validate_csv_file, validate_connection_params
from partition_utils import (
    parse_partition_spec, validate_partition_spec, format_partition_spec,
    suggest_partition_columns, get_transform_description, VALID_TRANSFORMS
)

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
@click.option('--trino-port', default=8080, help='Trino port (default: 8080)')
@click.option('--trino-user', default=os.getenv('USER', 'admin'), help='Trino user')
@click.option('--trino-password', help='Trino password (if authentication is enabled)')
@click.option('--trino-catalog', required=True, help='Trino catalog')
@click.option('--trino-schema', required=True, help='Trino schema')
@click.option('--hive-metastore-uri', required=True, help='Hive metastore Thrift URI')
@click.option('--mode', '-m', type=click.Choice(['append', 'overwrite']), default='append', 
              help='Write mode (append or overwrite, default: append)')
@click.option('--sample-size', default=1000, help='Number of rows to sample for schema inference (default: 1000)')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.option(
    '--partition-by', 
    multiple=True, 
    help='Optional: Column to partition by, with optional transform. ' +
         'Format: transform(column_name) or column_name. ' +
         'Examples: year(date), month(timestamp), id. ' +
         'Can be specified multiple times for multi-level partitioning.'
)
@click.option(
    '--list-partition-transforms', 
    is_flag=True, 
    help='List available partition transforms and exit.'
)
@click.option(
    '--suggest-partitioning', 
    is_flag=True, 
    help='Analyze CSV data and suggest appropriate partition strategies.'
)
def convert(csv_file: str, delimiter: str, has_header: bool, quote_char: str, batch_size: int,
            table_name: str, trino_host: str, trino_port: int, trino_user: str, trino_password: Optional[str],
            trino_catalog: str, trino_schema: str, hive_metastore_uri: str,
            mode: str, sample_size: int, verbose: bool, partition_by: Tuple[str], 
            list_partition_transforms: bool, suggest_partitioning: bool):
    """
    Convert a CSV file to an Iceberg table.
    
    This command reads a CSV file, infers its schema, creates an Iceberg table,
    and loads the data into the table using Trino and Hive metastore.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Display partition transforms if requested
    if list_partition_transforms:
        console.print("[bold]Available Partition Transforms:[/bold]")
        for transform, compatible_types in VALID_TRANSFORMS.items():
            compatible_str = ", ".join(compatible_types)
            description = get_transform_description(transform)
            console.print(f"  [cyan]{transform}(...)[/cyan] - {description}")
            console.print(f"    Compatible with column types: {compatible_str}")
        
        # Show identity partitioning
        console.print("  [cyan]column_name[/cyan] - Identity partitioning (partition by exact values)")
        console.print("    Compatible with all column types")
        
        # Examples
        console.print("\n[bold]Examples:[/bold]")
        console.print("  --partition-by year(date_column)")
        console.print("  --partition-by month(timestamp_column)")
        console.print("  --partition-by bucket(id, 16)")
        console.print("  --partition-by truncate(name, 10)")
        console.print("  --partition-by country")
        return 0
    
    try:
        # Validate CSV file
        if not validate_csv_file(csv_file, delimiter, quote_char):
            console.print(f"[bold red]Error:[/bold red] Invalid CSV file: {csv_file}")
            sys.exit(1)
        
        # Validate connection parameters
        if not validate_connection_params(trino_host, trino_port, hive_metastore_uri):
            console.print("[bold red]Error:[/bold red] Invalid connection parameters")
            sys.exit(1)
            
        # Parse table name components
        catalog, schema, table = parse_table_name(table_name)
        if not catalog or not schema or not table:
            console.print("[bold red]Error:[/bold red] Invalid table name format. Use: catalog.schema.table")
            sys.exit(1)
            
        # 1. Infer schema from CSV
        with console.status("[bold blue]Inferring schema from CSV...[/bold blue]") as status:
            logger.info(f"Inferring schema from CSV file: {csv_file}")
            iceberg_schema = infer_schema_from_csv(
                csv_file=csv_file,
                delimiter=delimiter, 
                has_header=has_header,
                quote_char=quote_char,
                sample_size=sample_size
            )
            logger.debug(f"Inferred schema: {iceberg_schema}")
        console.print(f"[bold green]✓[/bold green] Schema inferred successfully")
        
        # Get column names and types from the schema
        column_names = [field.name for field in iceberg_schema.fields]
        column_types = [iceberg_type_to_trino_type(field.field_type) for field in iceberg_schema.fields]
        
        # Handle partitioning suggestion if requested
        if suggest_partitioning:
            # Sample some data to help with suggestions
            with console.status("[bold blue]Analyzing data for partition recommendations...[/bold blue]") as status:
                # In a real implementation, we would sample data here
                # For this demo, we'll just use the schema information
                partition_suggestions = suggest_partition_columns(column_names, column_types)
            
            console.print("[bold]Recommended Partition Strategies:[/bold]")
            if partition_suggestions:
                for col_name, transform, param in partition_suggestions:
                    if transform:
                        if param:
                            spec = f"{transform}({col_name}, {param})"
                        else:
                            spec = f"{transform}({col_name})"
                    else:
                        spec = col_name
                    
                    description = get_transform_description(transform)
                    console.print(f"  [cyan]--partition-by {spec}[/cyan] - {description}")
            else:
                console.print("  No suitable partitioning columns found in this dataset")
            
            console.print("\n[bold yellow]Note:[/bold yellow] These are suggestions only. Choose based on your query patterns.")
            console.print("For time-series data, consider using year(), month(), or day() transforms.")
            console.print("For high-cardinality columns, consider using bucket() transform.")
            console.print("Avoid over-partitioning - 1-3 partition levels are usually sufficient.")
            
            # Ask if user wants to continue with these suggestions
            return 0
        
        # Process partition specifications if provided
        partition_specs = []
        if partition_by and len(partition_by) > 0:
            with console.status("[bold blue]Validating partition specifications...[/bold blue]") as status:
                for spec in partition_by:
                    # Validate the partition specification
                    is_valid, error = validate_partition_spec(spec, column_names, column_types)
                    if not is_valid:
                        console.print(f"[bold red]Error:[/bold red] Invalid partition specification: {spec}")
                        console.print(f"  {error}")
                        sys.exit(1)
                    
                    # Parse and format the specification for SQL
                    transform, column, param = parse_partition_spec(spec)
                    formatted_spec = format_partition_spec(transform, column, param)
                    partition_specs.append(formatted_spec)
                    
                    # Log the partition info
                    if transform:
                        if param:
                            logger.info(f"Using partition specification: {transform}({column}, {param})")
                        else:
                            logger.info(f"Using partition specification: {transform}({column})")
                    else:
                        logger.info(f"Using identity partition on column: {column}")
            
            if partition_specs:
                console.print(f"[bold green]✓[/bold green] Partition specifications validated")
                for spec in partition_specs:
                    console.print(f"  - {spec}")
        
        # 2. Connect to Trino
        with console.status("[bold blue]Connecting to Trino...[/bold blue]") as status:
            logger.info(f"Connecting to Trino at {trino_host}:{trino_port}")
            trino_client = TrinoClient(
                host=trino_host,
                port=trino_port,
                user=trino_user,
                password=trino_password,
                catalog=trino_catalog,
                schema=trino_schema
            )
        console.print(f"[bold green]✓[/bold green] Connected to Trino")
        
        # 3. Connect to Hive metastore
        with console.status("[bold blue]Connecting to Hive metastore...[/bold blue]") as status:
            logger.info(f"Connecting to Hive metastore at {hive_metastore_uri}")
            hive_client = HiveMetastoreClient(hive_metastore_uri)
        console.print(f"[bold green]✓[/bold green] Connected to Hive metastore")
        
        # 4. Create Iceberg table or verify it exists
        with console.status(f"[bold blue]Creating/verifying Iceberg table {table_name}...[/bold blue]") as status:
            table_exists = trino_client.table_exists(catalog, schema, table)
            
            if table_exists and mode == 'overwrite':
                logger.info(f"Table {table_name} exists and mode is overwrite, dropping table")
                trino_client.drop_table(catalog, schema, table)
                table_exists = False
            
            if not table_exists:
                logger.info(f"Creating table {table_name}")
                if partition_specs:
                    logger.info(f"With partitioning: {partition_specs}")
                trino_client.create_iceberg_table(
                    catalog=catalog, 
                    schema=schema, 
                    table=table, 
                    iceberg_schema=iceberg_schema,
                    partition_spec=partition_specs
                )
            else:
                logger.info(f"Table {table_name} already exists, verifying schema compatibility")
                if not trino_client.validate_table_schema(catalog, schema, table, iceberg_schema):
                    console.print("[bold red]Error:[/bold red] Existing table schema is incompatible with inferred schema")
                    sys.exit(1)
                # Note: We can't change partitioning of an existing table
                if partition_specs:
                    console.print("[bold yellow]Warning:[/bold yellow] Partitioning specifications are ignored for existing tables")
        
        # Display partition info if provided
        if partition_specs:
            console.print(f"[bold green]✓[/bold green] Iceberg table created with partitioning: {', '.join(partition_specs)}")
        else:
            console.print(f"[bold green]✓[/bold green] Iceberg table ready (no partitioning)")
        
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
            progress_callback=progress_update
        )
        
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

if __name__ == '__main__':
    cli()
