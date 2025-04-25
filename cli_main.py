"""
Entry point for the CSV to Iceberg CLI application.

This file serves as a clean entry point to the CLI functionality.
"""
from cli.commands import cli

if __name__ == "__main__":
    cli()