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
@click.option('--config-file', '-c', help='Path to configuration file')
def config(config_file):
    """
    Manage configuration profiles.
    
    This command launches the configuration management utility.
    """
    # Import here to avoid circular imports
    import config_cli
    import sys
    
    # Prepare arguments
    args = ['--config-file', config_file] if config_file else []
    
    # Launch config CLI with the arguments
    sys.argv = [sys.argv[0]] + args
    config_cli.cli()

@cli.command()
@click.option('--csv-file', '-f', required=True, help='Path to the CSV file')
@click.option('--delimiter', '-d', default=None, help='CSV delimiter (default: comma)')
@click.option('--has-header', '-h', is_flag=True, default=None, help='CSV has header row (default: True)')
@click.option('--quote-char', '-q', default=None, help='CSV quote character (default: double quote)')
@click.option('--batch-size', '-b', default=None, type=int, help='Batch size for processing (default: 10000)')
@click.option('--table-name', '-t', required=True, help='Target Iceberg table name (format: catalog.schema.table)')
@click.option('--trino-host', help='Trino host')
@click.option('--trino-port', default=None, type=int, help='Trino port (default: 8080)')
@click.option('--trino-user', default=None, help='Trino user')
@click.option('--trino-password', help='Trino password (if authentication is enabled)')
@click.option('--trino-catalog', help='Trino catalog')
@click.option('--trino-schema', help='Trino schema')
@click.option('--hive-metastore-uri', help='Hive metastore Thrift URI')
@click.option('--mode', '-m', type=click.Choice(['append', 'overwrite']), default=None, 
              help='Write mode (append or overwrite, default: append)')
@click.option('--sample-size', default=None, type=int, help='Number of rows to sample for schema inference (default: 1000)')
@click.option('--verbose', '-v', is_flag=True, default=None, help='Enable verbose logging')
@click.option(
    '--config-file', '-c',
    help='Path to configuration file'
)
@click.option(
    '--profile', '-p',
    help='Use a specific configuration profile'
)
def convert(csv_file: str, delimiter: str, has_header: bool, quote_char: str, batch_size: int,
            table_name: str, trino_host: str, trino_port: int, trino_user: str, trino_password: Optional[str],
            trino_catalog: str, trino_schema: str, hive_metastore_uri: str,
            mode: str, sample_size: int, verbose: bool,
            config_file: Optional[str] = None, profile: Optional[str] = None):
    """
    Convert a CSV file to an Iceberg table.
    
    This command reads a CSV file, infers its schema, creates an Iceberg table,
    and loads the data into the table using Trino and Hive metastore.
    """
    # Import config manager only when needed to avoid circular imports
    from config_manager import ConfigManager
    
    # Load configuration if a profile is specified
    config_values = {}
    if profile:
        config_manager = ConfigManager(config_file)
        
        if not config_manager.profile_exists(profile):
            console.print(f"[bold red]Error:[/bold red] Profile '{profile}' not found.")
            sys.exit(1)
            
        profile_data = config_manager.get_profile(profile)
        logger.info(f"Loaded configuration from profile: {profile}")
        
        # Extract connection settings
        if 'connection' in profile_data:
            conn = profile_data['connection']
            config_values.update({
                'trino_host': conn.get('trino_host'),
                'trino_port': conn.get('trino_port'),
                'trino_user': conn.get('trino_user'),
                'trino_password': conn.get('trino_password'),
                'trino_catalog': conn.get('trino_catalog'),
                'trino_schema': conn.get('trino_schema'),
                'hive_metastore_uri': conn.get('hive_metastore_uri')
            })
        
        # Extract default settings
        if 'defaults' in profile_data:
            defaults = profile_data['defaults']
            config_values.update({
                'delimiter': defaults.get('delimiter'),
                'has_header': defaults.get('has_header'),
                'quote_char': defaults.get('quote_char'),
                'batch_size': defaults.get('batch_size'),
                'mode': defaults.get('mode'),
                'sample_size': defaults.get('sample_size'),
                'verbose': defaults.get('verbose')
            })
        
        # Partitioning support has been removed
        # No partitioning configuration is needed
    
    # Command line arguments take precedence over profile settings
    # Apply configuration values where CLI arguments are None
    trino_host = trino_host or config_values.get('trino_host', 'localhost')
    trino_port = trino_port or config_values.get('trino_port', 8080)
    trino_user = trino_user or config_values.get('trino_user', os.getenv('USER', 'admin'))
    # Only use config password if no password was provided on command line
    if trino_password is None and 'trino_password' in config_values:
        trino_password = config_values['trino_password']
    trino_catalog = trino_catalog or config_values.get('trino_catalog')
    trino_schema = trino_schema or config_values.get('trino_schema')
    hive_metastore_uri = hive_metastore_uri or config_values.get('hive_metastore_uri')
    delimiter = delimiter or config_values.get('delimiter', ',')
    has_header = has_header if has_header is not None else config_values.get('has_header', True)
    quote_char = quote_char or config_values.get('quote_char', '"')
    batch_size = batch_size or config_values.get('batch_size', 10000)
    mode = mode or config_values.get('mode', 'append')
    sample_size = sample_size or config_values.get('sample_size', 1000)
    # Verbose flag should be True if either CLI flag or config is True
    if verbose is None:
        verbose = config_values.get('verbose', False)
    
    # Configure logging based on verbose setting
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
        
        # Get column names and types from the schema
        column_names = [field.name for field in iceberg_schema.fields]
        column_types = [iceberg_type_to_trino_type(field.field_type) for field in iceberg_schema.fields]
        
        # No partitioning support
        partition_specs = []
        
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
                trino_client.create_iceberg_table(
                    catalog=catalog, 
                    schema=schema, 
                    table=table, 
                    iceberg_schema=iceberg_schema,
                    partition_spec=[]
                )
            else:
                logger.info(f"Table {table_name} already exists, verifying schema compatibility")
                if not trino_client.validate_table_schema(catalog, schema, table, iceberg_schema):
                    console.print("[bold red]Error:[/bold red] Existing table schema is incompatible with inferred schema")
                    sys.exit(1)
        
        # Display success message
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
