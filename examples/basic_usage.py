"""
Basic usage example for SQL Batcher.

This example demonstrates how to use SQL Batcher with a simple in-memory SQLite database.
"""
import sqlite3
import logging

from sql_batcher import SQLBatcher
from sql_batcher.adapters.generic import GenericAdapter


# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def main():
    """Run the basic usage example."""
    # Create an in-memory SQLite database
    connection = sqlite3.connect(":memory:")
    
    # Create the adapter
    adapter = GenericAdapter(connection=connection)
    
    # Create a batcher with a 100KB size limit
    batcher = SQLBatcher(max_bytes=100_000)
    
    # Create a table
    adapter.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    print("Created users table")
    
    # Generate many insert statements
    statements = []
    for i in range(1, 1001):
        statements.append(f"INSERT INTO users VALUES ({i}, 'User {i}')")
    
    print(f"Generated {len(statements)} INSERT statements")
    
    # Process all statements
    total_processed = batcher.process_statements(statements, adapter.execute)
    print(f"Processed {total_processed} statements")
    
    # Verify the data
    results = adapter.execute("SELECT COUNT(*) FROM users")
    print(f"Total users in database: {results[0][0]}")
    
    # Query some users
    results = adapter.execute("SELECT * FROM users WHERE id <= 5 ORDER BY id")
    print("\nFirst 5 users:")
    for row in results:
        print(f"  User ID: {row[0]}, Name: {row[1]}")
    
    # Clean up
    adapter.close()
    print("\nClosed database connection")


if __name__ == "__main__":
    main()