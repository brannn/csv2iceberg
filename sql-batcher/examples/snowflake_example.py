#!/usr/bin/env python
"""
Snowflake usage example for SQLBatcher

This example demonstrates how to use SQLBatcher with Snowflake
for efficient execution of many SQL statements.
"""
import logging
import os
from typing import List

from sql_batcher import SQLBatcher


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Run the Snowflake SQLBatcher example."""
    try:
        import snowflake.connector
        from sql_batcher.adapters.snowflake import SnowflakeAdapter
    except ImportError:
        logger.error("This example requires snowflake-connector-python. Install with: pip install snowflake-connector-python")
        return
    
    # Create a Snowflake connection
    # Note: In a real scenario, you would need to provide actual connection details
    logger.info("Connecting to Snowflake...")
    
    connection_params = {
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
    }
    
    # Verify we have the necessary credentials
    missing_params = [k for k, v in connection_params.items() if v is None]
    if missing_params:
        logger.error(f"Missing required Snowflake connection parameters: {', '.join(missing_params)}")
        logger.error("Please set the corresponding environment variables.")
        return
    
    try:
        # Create a Snowflake adapter
        adapter = SnowflakeAdapter(connection_params=connection_params, auto_close=True)
        
        # Create the batcher with Snowflake's size limits
        batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
        
        # Begin a transaction
        adapter.begin_transaction()
        
        # Create a test table
        logger.info("Creating example table...")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS example_table (
            id INTEGER,
            name VARCHAR,
            created_date DATE
        )
        """
        adapter.execute(create_table_sql)
        
        # Generate a lot of INSERT statements
        statements = generate_insert_statements(1000)
        
        # Process the statements using the adapter
        logger.info(f"Processing {len(statements)} SQL statements with Snowflake...")
        total = batcher.process_statements(statements, adapter.execute)
        
        # Commit the transaction
        adapter.commit_transaction()
        
        logger.info(f"Successfully processed {total} statements with Snowflake")
        
        # Query the data to verify
        results = adapter.execute("SELECT COUNT(*) FROM example_table")
        count = results[0][0]
        logger.info(f"Total rows in table: {count}")
        
        # Show some sample data
        logger.info("Sample data:")
        sample_data = adapter.execute("SELECT * FROM example_table LIMIT 5")
        for row in sample_data:
            logger.info(row)
        
    except Exception as e:
        logger.error(f"Error in Snowflake example: {str(e)}")
        # Rollback on error
        try:
            adapter.rollback_transaction()
        except Exception:
            pass
    finally:
        # Clean up
        adapter.close()


def generate_insert_statements(count: int) -> List[str]:
    """Generate a specified number of INSERT statements for testing."""
    statements = []
    for i in range(1, count + 1):
        statements.append(
            f"INSERT INTO example_table VALUES ({i}, 'User {i}', '2023-01-{(i % 28) + 1:02d}')"
        )
    return statements


if __name__ == "__main__":
    main()