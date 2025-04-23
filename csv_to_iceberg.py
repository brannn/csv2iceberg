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

from schema_inferrer import infer_schema_from_csv
from trino_client import TrinoClient
from hive_client import HiveMetastoreClient
from iceberg_writer import IcebergWriter
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
@click.option('--trino-port', default=8080, help='Trino port (default: 8080)')
@click.option('--trino-user', default=os.getenv('USER', 'admin'), help='Trino user')
@click.option('--trino-password', help='Trino password (if authentication is enabled)')
@click.option('--http-scheme', type=click.Choice(['http', 'https']), default='http', 
              help='HTTP scheme for Trino connection (http or https, default: http)')
@click.option('--trino-role', default='sysadmin', help='Trino role for authorization (default: sysadmin)')
@click.option('--trino-catalog', required=True, help='Trino catalog')
@click.option('--trino-schema', required=True, help='Trino schema')
@click.option('--hive-metastore-uri', required=True, help='Hive metastore Thrift URI')
@click.option('--mode', '-m', type=click.Choice(['append', 'overwrite']), default='append', 
              help='Write mode (append or overwrite, default: append)')
@click.option('--sample-size', default=1000, help='Number of rows to sample for schema inference (default: 1000)')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def convert(csv_file: str, delimiter: str, has_header: bool, quote_char: str, batch_size: int,
            table_name: str, trino_host: str, trino_port: int, trino_user: str, trino_password: Optional[str],
            http_scheme: str, trino_role: str, trino_catalog: str, trino_schema: str, hive_metastore_uri: str,
            mode: str, sample_size: int, verbose: bool):
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
