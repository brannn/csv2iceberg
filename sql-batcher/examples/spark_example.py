#!/usr/bin/env python
"""
Spark SQL usage example for SQLBatcher

This example demonstrates how to use SQLBatcher with Spark SQL
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
    """Run the Spark SQLBatcher example."""
    try:
        from pyspark.sql import SparkSession
        from sql_batcher.adapters.spark import SparkAdapter
    except ImportError:
        logger.error("This example requires PySpark. Install with: pip install pyspark")
        return
    
    # Create a SparkSession
    logger.info("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("SQLBatcherSparkExample") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Create a Spark adapter
        adapter = SparkAdapter(spark_session=spark)
        
        # Create the batcher with Spark's size limits
        batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
        
        # Create a test table
        logger.info("Creating example table...")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS example_table (
            id INT,
            name STRING,
            created_date DATE
        ) USING PARQUET
        """
        adapter.execute(create_table_sql)
        
        # Generate a lot of INSERT statements
        statements = generate_insert_statements(1000)
        
        # Process the statements using the adapter
        logger.info(f"Processing {len(statements)} SQL statements with Spark...")
        total = batcher.process_statements(statements, adapter.execute)
        
        logger.info(f"Successfully processed {total} statements with Spark")
        
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
        logger.error(f"Error in Spark example: {str(e)}")
    finally:
        # Stop the SparkSession
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


def generate_insert_statements(count: int) -> List[str]:
    """Generate a specified number of INSERT statements for testing."""
    statements = []
    for i in range(1, count + 1):
        statements.append(
            f"INSERT INTO example_table VALUES ({i}, 'User {i}', date '2023-01-{(i % 28) + 1:02d}')"
        )
    return statements


if __name__ == "__main__":
    main()