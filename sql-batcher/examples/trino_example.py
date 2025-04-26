#!/usr/bin/env python
"""
Trino usage example for SQLBatcher

This example demonstrates how to use SQLBatcher with Trino
for efficient execution of many SQL statements.
"""
import logging
import os
from typing import List

from sql_batcher import SQLBatcher
from sql_batcher.adapters.trino import TrinoAdapter


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Run the Trino SQLBatcher example."""
    # Create a Trino adapter
    # Note: In a real scenario, you would need to provide actual connection details
    try:
        adapter = TrinoAdapter(
            host=os.environ.get("TRINO_HOST", "localhost"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
            user=os.environ.get("TRINO_USER", "admin"),
            catalog=os.environ.get("TRINO_CATALOG", "hive"),
            schema=os.environ.get("TRINO_SCHEMA", "default"),
            http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "https")
        )
    except ImportError:
        logger.error("This example requires the trino package. Install with: pip install trino")
        return
    
    # Create the batcher with Trino's size limits
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    try:
        # Create a test table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS example_table (
            id INTEGER,
            name VARCHAR,
            created_date DATE
        )
        WITH (
            format = 'PARQUET'
        )
        """
        adapter.execute(create_table_sql)
        
        # Generate a lot of INSERT statements
        statements = generate_insert_statements(1000)
        
        # Process the statements using the adapter
        logger.info(f"Processing {len(statements)} SQL statements with Trino...")
        total = batcher.process_statements(statements, adapter.execute)
        
        logger.info(f"Successfully processed {total} statements with Trino")
        
        # Query the data to verify
        results = adapter.execute("SELECT COUNT(*) FROM example_table")
        logger.info(f"Total rows in table: {results[0][0]}")
        
    except Exception as e:
        logger.error(f"Error in Trino example: {str(e)}")
    finally:
        # Clean up
        adapter.close()


def generate_insert_statements(count: int) -> List[str]:
    """Generate a specified number of INSERT statements for testing."""
    statements = []
    for i in range(1, count + 1):
        statements.append(
            f"INSERT INTO example_table VALUES ({i}, 'User {i}', DATE '2023-01-{(i % 28) + 1:02d}')"
        )
    return statements


if __name__ == "__main__":
    main()