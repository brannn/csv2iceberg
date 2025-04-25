#!/usr/bin/env python3
"""
CSV to Iceberg - CLI tool for converting CSV files to Iceberg tables.

This tool supports both Trino/Starburst and Amazon S3 Tables using Iceberg REST API.
"""
import os
import sys
import logging
from typing import Optional, Tuple, Literal

import click
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn

# Use the flat structure imports
from core.schema_inferrer import infer_schema_from_csv
from core.conversion_service import convert_csv_to_iceberg
from connectors.trino_client import TrinoClient
from connectors.hive_client import HiveMetastoreClient
from connectors.s3_rest_client import S3RestClient
from core.iceberg_writer import IcebergWriter
from core.s3_iceberg_writer import S3IcebergWriter
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
    
    This tool allows converting CSV files to Iceberg tables using either:
    1. Trino/Starburst with optional Hive metastore connection
    2. Amazon S3 Tables using the Iceberg REST API (not AWS Glue)
    
    Use the 'convert' command with '--connection-type' option to specify which connection type to use.
    """
    pass

@cli.command()
@click.option('--csv-file', '-f', required=True, help='Path to the CSV file')
@click.option('--delimiter', '-d', default=',', help='CSV delimiter (default: comma)')
@click.option('--has-header', '-h', is_flag=True, default=True, help='CSV has header row (default: True)')
@click.option('--quote-char', '-q', default='"', help='CSV quote character (default: double quote)')
@click.option('--batch-size', '-b', default=20000, type=int, help='Batch size for processing (default: 20000)')
@click.option('--table-name', '-t', required=True, 
              help='Target Iceberg table name. For Trino: catalog.schema.table. For S3: can be just table name')
# Connection type selection
@click.option('--connection-type', type=click.Choice(['trino', 's3_rest']), default='trino',
              help='Connection type: either Trino/Starburst or Amazon S3 Tables REST API')

# Trino specific options
@click.option('--trino-host', help='Trino host')
@click.option('--trino-port', default=443, type=int, help='Trino port (default: 443)')
@click.option('--trino-user', default=os.getenv('USER', 'admin'), help='Trino user')
@click.option('--trino-password', help='Trino password (if authentication is enabled)')
@click.option('--http-scheme', type=click.Choice(['http', 'https']), default='https', 
              help='HTTP scheme for Trino connection (http or https, default: https)')
@click.option('--trino-role', default='sysadmin', help='Trino role for authorization (default: sysadmin)')
@click.option('--trino-catalog', default='iceberg', help='Trino catalog (default: iceberg)')
@click.option('--trino-schema', default='default', help='Trino schema (default: default)')
@click.option('--hive-metastore-uri', default="localhost:9083", help='Hive metastore Thrift URI')
@click.option('--use-hive-metastore/--no-hive-metastore', default=False, help='Use direct Hive Metastore connection')

# S3 REST API specific options
@click.option('--s3-rest-uri', help='S3 Tables REST API endpoint URI')
@click.option('--s3-warehouse-location', help='S3 warehouse location (e.g., s3://bucket/warehouse)')
@click.option('--s3-namespace', default='default', help='S3 Tables namespace (schema) (default: default)')
@click.option('--s3-client-id', help='OAuth client ID for S3 Tables REST API')
@click.option('--s3-client-secret', help='OAuth client secret for S3 Tables REST API')
@click.option('--s3-token', help='Bearer token for S3 Tables REST API')
@click.option('--aws-access-key-id', help='AWS access key ID for S3 Tables')
@click.option('--aws-secret-access-key', help='AWS secret access key for S3 Tables')
@click.option('--aws-session-token', help='AWS session token for S3 Tables')
@click.option('--aws-region', default='us-east-1', help='AWS region for S3 Tables (default: us-east-1)')

# Common options
@click.option('--mode', '-m', type=click.Choice(['append', 'overwrite']), default='append', 
              help='Write mode (append or overwrite, default: append)')
@click.option('--sample-size', default=1000, type=int, help='Number of rows to sample for schema inference (default: 1000)')
@click.option('--custom-schema', help='Path to a JSON file containing a custom schema definition')
@click.option('--include-columns', help='Comma-separated list of column names to include (overrides exclude-columns)')
@click.option('--exclude-columns', help='Comma-separated list of column names to exclude')
@click.option('--max-query-size', default=700000, type=int, help='Maximum SQL query size in bytes (default: 700000, 70% of Trino\'s 1MB limit)')
@click.option('--dry-run', is_flag=True, help='Run in dry run mode - report operations without executing them')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def convert(
        csv_file: str, delimiter: str, has_header: bool, quote_char: str, batch_size: int,
        table_name: str, connection_type: str,
        # Trino params
        trino_host: Optional[str], trino_port: int, trino_user: Optional[str], trino_password: Optional[str],
        http_scheme: str, trino_role: str, trino_catalog: str, trino_schema: str, 
        hive_metastore_uri: str, use_hive_metastore: bool,
        # S3 REST params
        s3_rest_uri: Optional[str], s3_warehouse_location: Optional[str], s3_namespace: str,
        s3_client_id: Optional[str], s3_client_secret: Optional[str], s3_token: Optional[str],
        aws_access_key_id: Optional[str], aws_secret_access_key: Optional[str], 
        aws_session_token: Optional[str], aws_region: str,
        # Common params
        mode: str, sample_size: int, custom_schema: Optional[str],
        include_columns: Optional[str], exclude_columns: Optional[str], max_query_size: int,
        dry_run: bool, verbose: bool):
    """
    Convert a CSV file to an Iceberg table.
    
    This command reads a CSV file, infers its schema, creates an Iceberg table,
    and loads the data into the table using either:
    1. Trino/Starburst with optional Hive metastore connection, or
    2. Amazon S3 Tables using the Iceberg REST API.
    
    For Trino connections, specify the Trino host, port, user, and other Trino-specific options.
    For S3 Tables, specify the S3 REST API endpoint URI and warehouse location.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Parse include/exclude columns lists if provided
        include_cols = None
        exclude_cols = None
        
        if include_columns:
            include_cols = [col.strip() for col in include_columns.split(',')]
            logger.info(f"Including only these columns: {include_cols}")
            
        if exclude_columns and not include_cols:  # Include columns takes precedence
            exclude_cols = [col.strip() for col in exclude_columns.split(',')]
            logger.info(f"Excluding these columns: {exclude_cols}")
        
        # Load custom schema data if provided
        custom_schema_str = None
        if custom_schema:
            with console.status("[bold blue]Loading custom schema...[/bold blue]") as status:
                try:
                    logger.info(f"Loading custom schema from file: {custom_schema}")
                    with open(custom_schema, 'r') as f:
                        custom_schema_str = f.read()
                except Exception as e:
                    logger.error(f"Error loading custom schema: {str(e)}", exc_info=True)
                    console.print(f"[bold red]Error:[/bold red] Failed to load custom schema: {str(e)}")
                    sys.exit(1)
            console.print(f"[bold green]✓[/bold green] Custom schema loaded successfully")
        
        # Create a progress tracking callback
        def progress_update(percent):
            console.print(f"[bold blue]Writing data: {percent}% complete[/bold blue]")
            
        # Validate the required parameters for the selected connection type
        if connection_type == "trino":
            if not trino_host or not trino_user:
                console.print("[bold red]Error:[/bold red] Trino connection requires host and user")
                sys.exit(1)
                
            console.print(f"[bold]Using Trino connection to {trino_host}:{trino_port} with user {trino_user}[/bold]")
            
        elif connection_type == "s3_rest":
            if not s3_rest_uri or not s3_warehouse_location:
                console.print("[bold red]Error:[/bold red] S3 REST connection requires rest_uri and warehouse_location")
                sys.exit(1)
                
            console.print(f"[bold]Using AWS S3 Tables connection to {s3_rest_uri}[/bold]")
            console.print(f"[bold]With warehouse location {s3_warehouse_location}[/bold]")
        else:
            console.print(f"[bold red]Error:[/bold red] Unknown connection type: {connection_type}")
            sys.exit(1)
        
        # Log the operation being performed
        if dry_run:
            console.print("[bold blue]Running in DRY RUN mode - simulating operations without making changes[/bold blue]")
        else:
            console.print("[bold]Starting CSV to Iceberg conversion...[/bold]")
        
        # Use the unified conversion service
        with console.status("[bold blue]Converting CSV to Iceberg...[/bold blue]") as status:
            result = convert_csv_to_iceberg(
                # Required parameters
                csv_file=csv_file,
                table_name=table_name,
                
                # Connection type
                connection_type=connection_type,
                
                # Trino connection parameters
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
                
                # S3 REST API connection parameters
                s3_rest_uri=s3_rest_uri,
                s3_warehouse_location=s3_warehouse_location,
                s3_namespace=s3_namespace,
                s3_client_id=s3_client_id,
                s3_client_secret=s3_client_secret,
                s3_token=s3_token,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                aws_region=aws_region,
                
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
                custom_schema=custom_schema_str,
                
                # Dry run mode
                dry_run=dry_run,
                
                # SQL batcher options
                max_query_size=max_query_size,
                
                # Callback function
                progress_callback=progress_update
            )
        
        # Check the result
        if not result['success']:
            console.print(f"[bold red]Error during conversion:[/bold red] {result['error']}")
            sys.exit(1)
            
        # Display success message and statistics
        console.print(f"[bold green]✓[/bold green] Conversion completed successfully!")
        console.print(f"[bold]Processed {result['rows_processed']} rows[/bold]")
        
        # Display performance metrics if available
        if 'performance_metrics' in result:
            metrics = result['performance_metrics']
            console.print(f"[bold]Performance metrics:[/bold]")
            console.print(f"  - Processing rate: {metrics.get('processing_rate', 0):.2f} rows/sec")
            console.print(f"  - Total time: {metrics.get('total_processing_time', 0):.2f} seconds")
            console.print(f"  - Batches: {metrics.get('total_batches', 0)}")
            console.print(f"  - Avg batch size: {metrics.get('avg_batch_size', 0):.1f} rows")
            
        # Display dry run results if available
        if dry_run and 'dry_run_results' in result:
            dry_run_results = result['dry_run_results']
            console.print("[bold blue]Dry run summary:[/bold blue]")
            
            # Print DDL statements
            if 'ddl_statements' in dry_run_results:
                ddl_statements = dry_run_results['ddl_statements']
                console.print(f"[bold]DDL Statements ({len(ddl_statements)}):[/bold]")
                for i, stmt in enumerate(ddl_statements[:3]):  # Show first 3 statements
                    console.print(f"  {i+1}. {stmt.get('query', '')}...")
                if len(ddl_statements) > 3:
                    console.print(f"  ... and {len(ddl_statements) - 3} more statements")
            
            # Print DML statements
            if 'dml_statements' in dry_run_results:
                dml_statements = dry_run_results['dml_statements']
                console.print(f"[bold]DML Statements ({len(dml_statements)}):[/bold]")
                for i, stmt in enumerate(dml_statements[:2]):  # Show first 2 statements
                    rows = stmt.get('row_count', 0)
                    query = stmt.get('query', '')
                    if len(query) > 60:
                        query = query[:60] + "..."
                    console.print(f"  {i+1}. {query} ({rows} rows)")
                if len(dml_statements) > 2:
                    console.print(f"  ... and {len(dml_statements) - 2} more statements")
        
    except Exception as e:
        logger.error(f"Error during conversion: {str(e)}", exc_info=True)
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)

def parse_table_name(table_name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse a table name in the format catalog.schema.table or schema.table or just table.
    
    This function supports both Trino's catalog.schema.table format and
    simplified formats for S3 Tables which might just use schema.table or table.
    
    Args:
        table_name: The table name to parse
        
    Returns:
        A tuple of (catalog, schema, table) - any component might be None
    """
    parts = table_name.split('.')
    
    if len(parts) == 3:
        # Full format: catalog.schema.table
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        # Medium format: schema.table
        return None, parts[0], parts[1]
    elif len(parts) == 1:
        # Simple format: just table
        return None, None, parts[0]
    else:
        # Invalid format
        return None, None, None

# Export the CLI function as main for easy importing
main = cli

if __name__ == '__main__':
    cli()
