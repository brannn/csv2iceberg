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

from csv_to_iceberg.core.conversion_service import (
    convert_csv_to_iceberg, 
    validate_csv_file, 
    validate_connection_params,
    parse_table_name
)
from csv_to_iceberg.utils import setup_logging

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
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def convert(csv_file: str, delimiter: str, has_header: bool, quote_char: str, batch_size: int,
            table_name: str, trino_host: str, trino_port: int, trino_user: str, trino_password: Optional[str],
            http_scheme: str, trino_role: str, trino_catalog: str, trino_schema: str, hive_metastore_uri: str,
            use_hive_metastore: bool, mode: str, sample_size: int, custom_schema: Optional[str], 
            include_columns: Optional[str], exclude_columns: Optional[str], verbose: bool):
    """
    Convert a CSV file to an Iceberg table.
    
    This command reads a CSV file, infers its schema, creates an Iceberg table,
    and loads the data into the table using Trino and Hive metastore.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Process include/exclude columns
        include_cols = None
        exclude_cols = None
        
        if include_columns:
            include_cols = [col.strip() for col in include_columns.split(',')]
            logger.info(f"Including only these columns: {include_cols}")
            
        if exclude_columns and not include_cols:  # Include columns takes precedence
            exclude_cols = [col.strip() for col in exclude_columns.split(',')]
            logger.info(f"Excluding these columns: {exclude_cols}")
            
        # Convert custom schema file to JSON string if provided
        custom_schema_content = None
        if custom_schema:
            with console.status("[bold blue]Loading custom schema...[/bold blue]"):
                try:
                    logger.info(f"Loading custom schema from file: {custom_schema}")
                    with open(custom_schema, 'r') as f:
                        custom_schema_content = f.read()
                    console.print(f"[bold green]✓[/bold green] Custom schema loaded successfully")
                except Exception as e:
                    logger.error(f"Error loading custom schema: {str(e)}", exc_info=True)
                    console.print(f"[bold red]Error:[/bold red] Failed to load custom schema: {str(e)}")
                    sys.exit(1)
                
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
            
        # Create progress bar
        progress = Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeRemainingColumn(),
            console=console
        )
        
        # Progress callback function
        def progress_update(percent):
            nonlocal task_id
            progress.update(task_id, completed=percent)
        
        # Run the conversion with progress tracking
        with progress:
            task_id = progress.add_task("[green]Converting CSV to Iceberg...", total=100)
            
            console.print(f"[bold blue]Converting CSV to Iceberg in {mode} mode...[/bold blue]")
            result = convert_csv_to_iceberg(
                # File parameters
                csv_file=csv_file,
                table_name=table_name,
                
                # Connection parameters
                trino_host=trino_host,
                trino_port=trino_port,
                trino_user=trino_user,
                trino_password=trino_password,
                http_scheme=http_scheme,
                trino_role=trino_role,
                trino_catalog=trino_catalog,
                trino_schema=trino_schema,
                use_hive_metastore=use_hive_metastore,
                hive_metastore_uri=hive_metastore_uri,
                
                # CSV handling parameters
                delimiter=delimiter,
                quote_char=quote_char,
                has_header=has_header,
                batch_size=batch_size,
                
                # Schema/data parameters
                mode=mode,
                sample_size=sample_size,
                include_columns=include_cols,
                exclude_columns=exclude_cols,
                custom_schema=custom_schema_content,
                
                # Progress callback
                progress_callback=progress_update
            )
        
        # Handle the result
        if result['success']:
            console.print(f"[bold green]✓[/bold green] Successfully converted CSV to Iceberg table {table_name}")
            console.print(f"[bold green]✓[/bold green] Processed {result['rows_processed']} rows in {result['duration']:.2f} seconds")
        else:
            console.print(f"[bold red]Error during conversion:[/bold red] {result['error']}")
            if verbose and 'traceback' in result:
                console.print(result['traceback'])
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error during conversion: {str(e)}", exc_info=True)
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)

# parse_table_name is now imported from conversion_service.py

if __name__ == '__main__':
    cli()
