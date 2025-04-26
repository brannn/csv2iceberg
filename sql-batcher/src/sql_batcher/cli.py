#!/usr/bin/env python
"""
Command-line interface for SQLBatcher.

This module provides a simple command-line interface for SQLBatcher.
"""
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import List, Optional


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("sql_batcher")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="SQLBatcher - Batch SQL statements by size for efficient execution"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Process SQL file command
    process_parser = subparsers.add_parser(
        "process", help="Process SQL statements from a file"
    )
    process_parser.add_argument(
        "input_file", type=str, help="SQL file to process (one statement per line)"
    )
    process_parser.add_argument(
        "--max-size", type=int, default=1000000, help="Maximum batch size in bytes"
    )
    process_parser.add_argument(
        "--delimiter", type=str, default=";", help="SQL statement delimiter"
    )
    process_parser.add_argument(
        "--dry-run", action="store_true", help="Don't execute, just print batches"
    )
    process_parser.add_argument(
        "--output", type=str, help="Output file for batched SQL (for dry-run mode)"
    )
    process_parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging"
    )
    
    # Show adapters command
    adapters_parser = subparsers.add_parser(
        "adapters", help="List available database adapters"
    )
    adapters_parser.add_argument(
        "--verbose", action="store_true", help="Show adapter details"
    )
    
    # Version command
    subparsers.add_parser("version", help="Show version information")
    
    return parser.parse_args()


def read_sql_statements(file_path: str, delimiter: str = ";") -> List[str]:
    """
    Read SQL statements from a file.
    
    Args:
        file_path: Path to the SQL file
        delimiter: Statement delimiter
        
    Returns:
        List of SQL statements
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    
    with open(path, "r") as f:
        content = f.read()
    
    # Split by delimiter but ignore delimiters in comments or quotes
    # This is a simplified approach - a proper SQL parser would be better
    statements = []
    current = ""
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
            
        current += line + " "
        if line.endswith(delimiter):
            statements.append(current.strip())
            current = ""
    
    # Add any remaining statement
    if current.strip():
        statements.append(current.strip())
    
    return statements


def process_sql_file(args: argparse.Namespace) -> None:
    """Process SQL statements from a file."""
    from sql_batcher import SQLBatcher
    
    logger = setup_logging(args.verbose)
    logger.info(f"Processing SQL file: {args.input_file}")
    
    try:
        # Read statements from file
        statements = read_sql_statements(args.input_file, args.delimiter)
        logger.info(f"Read {len(statements)} SQL statements")
        
        # Create batcher
        batcher = SQLBatcher(
            max_bytes=args.max_size,
            delimiter=args.delimiter,
            dry_run=args.dry_run
        )
        
        # Define execution callback based on mode
        if args.dry_run:
            batches = []
            
            def execute_callback(sql: str) -> None:
                batches.append(sql)
                logger.info(f"Would execute batch with {sql.count(';')} statements")
            
            # Process statements
            total = batcher.process_statements(statements, execute_callback)
            logger.info(f"Processed {total} statements in {len(batches)} batches")
            
            # Save batches if requested
            if args.output:
                with open(args.output, "w") as f:
                    for i, batch in enumerate(batches):
                        f.write(f"-- Batch {i+1}\n")
                        f.write(batch)
                        f.write("\n\n")
                logger.info(f"Saved batched SQL to: {args.output}")
        else:
            logger.error("Execution mode not implemented. Use --dry-run for now.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error processing SQL file: {str(e)}")
        sys.exit(1)


def list_adapters(args: argparse.Namespace) -> None:
    """List available database adapters."""
    logger = setup_logging(args.verbose)
    
    # Import adapter-related modules
    from sql_batcher.adapters import __all__ as adapter_names
    from sql_batcher.adapters.base import SQLAdapter
    
    # Get adapter classes
    adapters = {}
    for name in adapter_names:
        if name != "SQLAdapter":  # Skip the base class
            try:
                module = __import__(f"sql_batcher.adapters", fromlist=[name])
                adapter_class = getattr(module, name)
                adapters[name] = adapter_class
            except (ImportError, AttributeError):
                continue
    
    if args.verbose:
        adapter_info = {}
        for name, cls in adapters.items():
            adapter_info[name] = {
                "description": cls.__doc__.splitlines()[0] if cls.__doc__ else "No description",
                "dependencies": getattr(cls, "dependencies", ["None"]),
                "default_max_size": getattr(cls, "DEFAULT_MAX_QUERY_SIZE", 1000000),
            }
        print(json.dumps(adapter_info, indent=2))
    else:
        print("Available adapters:")
        for name in adapters.keys():
            print(f"- {name}")


def show_version() -> None:
    """Show version information."""
    try:
        from importlib.metadata import version
        ver = version("sql-batcher")
    except:
        ver = "unknown"
    
    print(f"SQLBatcher version: {ver}")
    print("A tool for batching and efficiently executing SQL statements")
    print("https://github.com/example/sql-batcher")


def main() -> None:
    """Main entry point for the CLI."""
    args = parse_args()
    
    if args.command == "process":
        process_sql_file(args)
    elif args.command == "adapters":
        list_adapters(args)
    elif args.command == "version":
        show_version()
    else:
        print("No command specified. Use --help for available commands.")
        sys.exit(1)


if __name__ == "__main__":
    main()