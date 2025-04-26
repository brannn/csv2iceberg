"""
Basic usage example for SQL Batcher.

This example demonstrates how to use SQL Batcher with a simple in-memory SQLite database.
"""
import sqlite3
from sql_batcher import SQLBatcher
from sql_batcher.adapters.generic import GenericAdapter


def main():
    """Run the basic usage example."""
    print("SQL Batcher - Basic Example")
    print("===========================")
    
    # Create a simple SQLite in-memory database
    print("\nCreating SQLite in-memory database...")
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    
    # Create a table
    print("Creating table...")
    cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER
    )
    """)
    
    # Create a GenericAdapter to work with any database
    adapter = GenericAdapter(
        connection=conn,
        max_query_size=100_000  # 100KB (overkill for SQLite, but demonstrates the concept)
    )
    
    # Generate a batch of INSERT statements
    print("\nGenerating 1,000 INSERT statements...")
    statements = []
    for i in range(1, 1001):
        name = f"User {i}"
        email = f"user{i}@example.com"
        age = 20 + (i % 50)  # Ages between 20 and 69
        
        statements.append(
            f"INSERT INTO users (id, name, email, age) VALUES ({i}, '{name}', '{email}', {age})"
        )
    
    print(f"Generated {len(statements)} statements")
    
    # Create a SQLBatcher with a modest max_bytes setting
    # For this example, we'll make it small to force multiple batches
    batcher = SQLBatcher(max_bytes=10_000)  # 10KB per batch
    
    # Process all statements
    print("\nProcessing statements with SQL Batcher...")
    total_processed = batcher.process_statements(statements, adapter.execute)
    
    print(f"Processed {total_processed} statements")
    
    # Verify the results
    cursor.execute("SELECT COUNT(*) FROM users")
    result = cursor.fetchone()
    print(f"Total users in database: {result[0]}")
    
    # Query some data to verify it was inserted correctly
    print("\nSample data:")
    cursor.execute("SELECT id, name, email, age FROM users LIMIT 5")
    for row in cursor.fetchall():
        print(f"  User {row[0]}: {row[1]}, {row[2]}, Age: {row[3]}")
    
    # Run a query with aggregation
    print("\nAge statistics:")
    cursor.execute("""
    SELECT 
        MIN(age) as min_age,
        MAX(age) as max_age,
        AVG(age) as avg_age,
        COUNT(*) as total_users
    FROM users
    """)
    stats = cursor.fetchone()
    print(f"  Users: {stats[3]}")
    print(f"  Age range: {stats[0]} - {stats[1]}")
    print(f"  Average age: {stats[2]:.1f}")
    
    # Clean up
    conn.close()
    print("\nDatabase connection closed")
    print("\nExample completed successfully!")


if __name__ == "__main__":
    main()