#!/usr/bin/env python
"""
Basic usage example for SQLBatcher

This example demonstrates the core functionality of SQLBatcher
without any specific database adapter.
"""
import logging

from sql_batcher import SQLBatcher


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Run the basic SQLBatcher example."""
    # Create a simple execution callback
    def execute_sql(sql):
        logger.info(f"Would execute: {sql}")
        # In a real scenario, this would execute using a database connection
    
    # Create a batcher with a smaller limit for demonstration
    batcher = SQLBatcher(max_bytes=200, dry_run=True)
    
    # Create some example SQL statements
    statements = [
        "INSERT INTO example_table VALUES (1, 'First value', '2023-01-01')",
        "INSERT INTO example_table VALUES (2, 'Second value', '2023-01-02')",
        "INSERT INTO example_table VALUES (3, 'Third value with longer text', '2023-01-03')",
        "INSERT INTO example_table VALUES (4, 'Fourth value', '2023-01-04')",
        "INSERT INTO example_table VALUES (5, 'Fifth value', '2023-01-05')",
    ]
    
    # Process the statements
    logger.info("Processing SQL statements with size-based batching...")
    total = batcher.process_statements(statements, execute_sql)
    
    logger.info(f"Processed {total} statements in total")


if __name__ == "__main__":
    main()