############
Usage Examples
############

This section provides practical examples of SQL Batcher in various scenarios to help you understand how to use it effectively.

Basic Usage
=========

.. code-block:: python

    from sql_batcher import SQLBatcher

    # Create a batcher with a 100KB size limit
    batcher = SQLBatcher(max_bytes=100_000)

    # Define SQL statements
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "INSERT INTO users VALUES (3, 'Charlie')"
    ]

    # Define an execution function
    def execute_sql(sql):
        print(f"Executing SQL batch ({len(sql)} bytes):")
        print(sql)
        # In a real scenario, this would execute the SQL against your database

    # Process all statements
    batcher.process_statements(statements, execute_sql)

Working with Database Adapters
============================

Using the Generic Adapter with SQLite:

.. code-block:: python

    import sqlite3
    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.generic import GenericAdapter

    # Create an in-memory SQLite database
    connection = sqlite3.connect(":memory:")
    
    # Create the adapter
    adapter = GenericAdapter(connection=connection)
    
    # Create a batcher using the adapter's size limit
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # Create a table
    adapter.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    
    # Generate many insert statements
    statements = []
    for i in range(1, 1001):
        statements.append(f"INSERT INTO users VALUES ({i}, 'User {i}')")
    
    # Process all statements efficiently
    batcher.process_statements(statements, adapter.execute)
    
    # Verify the data
    results = adapter.execute("SELECT COUNT(*) FROM users")
    print(f"Total users inserted: {results[0][0]}")
    
    # Clean up
    adapter.close()

Advanced Usage with Dry Run Mode
==============================

Sometimes you want to see what SQL would be executed without actually running it. The dry run mode allows this:

.. code-block:: python

    from sql_batcher import SQLBatcher
    from sql_batcher.query_collector import ListQueryCollector

    # Create a query collector
    collector = ListQueryCollector()
    
    # Create a batcher in dry run mode
    batcher = SQLBatcher(max_bytes=50_000, dry_run=True)
    
    # Define a large number of statements
    statements = []
    for i in range(1, 1001):
        statements.append(f"INSERT INTO logs VALUES ({i}, 'Log message {i}', CURRENT_TIMESTAMP)")
    
    # Process statements without executing them
    batcher.process_statements(
        statements,
        lambda x: None,  # This won't be called in dry run mode
        query_collector=collector
    )
    
    # Review the batches that would have been executed
    for i, query_info in enumerate(collector.get_queries()):
        print(f"Batch {i+1} - {len(query_info['query'])} bytes")
        print(f"First statement: {query_info['query'].split(';')[0]}")
        print(f"Number of statements: {query_info['query'].count(';')}")
        print()

Using with Transactions
=====================

Many databases support transactions, which can be used to ensure all statements succeed or fail together:

.. code-block:: python

    import psycopg2
    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.generic import GenericAdapter

    # Connect to PostgreSQL
    connection = psycopg2.connect(
        host="localhost",
        database="mydb",
        user="postgres",
        password="password"
    )
    
    # Create the adapter
    adapter = GenericAdapter(connection=connection)
    
    # Begin a transaction
    adapter.begin_transaction()
    
    try:
        # Create a table
        adapter.execute("CREATE TABLE items (id INTEGER, name TEXT, price NUMERIC)")
        
        # Generate insert statements
        statements = []
        for i in range(1, 101):
            statements.append(f"INSERT INTO items VALUES ({i}, 'Item {i}', {i * 9.99})")
        
        # Create a batcher
        batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
        
        # Process all statements within the transaction
        batcher.process_statements(statements, adapter.execute)
        
        # Commit the transaction
        adapter.commit_transaction()
        print("Transaction committed successfully")
        
    except Exception as e:
        # Rollback on error
        adapter.rollback_transaction()
        print(f"Transaction rolled back due to error: {e}")
        
    finally:
        # Clean up
        adapter.close()

Handling Large Statements
=======================

SQL Batcher automatically handles statements that exceed the maximum batch size:

.. code-block:: python

    from sql_batcher import SQLBatcher
    import logging

    # Set up logging to see what's happening
    logging.basicConfig(level=logging.DEBUG)
    
    # Create a batcher with a very small size limit
    batcher = SQLBatcher(max_bytes=100)  # Only 100 bytes!
    
    # Define statements, including one that exceeds the limit
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        # This one is much larger than our 100 byte limit
        "INSERT INTO users VALUES (3, 'Charlie') /* " + ("X" * 200) + " */"
    ]
    
    # Define an execution function that logs what it receives
    def execute_sql(sql):
        print(f"Executing SQL batch ({len(sql)} bytes):")
        print(sql)
    
    # Process statements - watch how the oversized one is handled
    batcher.process_statements(statements, execute_sql)

Custom Adapter Implementation
==========================

Creating a custom adapter for a database not covered by the built-in adapters:

.. code-block:: python

    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.base import SQLAdapter
    
    class MyCustomAdapter(SQLAdapter):
        """Custom adapter for a hypothetical database."""
        
        def __init__(self, connection_string, max_query_size=1_000_000):
            self.connection_string = connection_string
            self.max_query_size = max_query_size
            self.connection = None
            self._connect()
        
        def _connect(self):
            print(f"Connecting to database with: {self.connection_string}")
            # In a real implementation, this would establish a connection
            self.connection = True
        
        def execute(self, sql):
            if not self.connection:
                self._connect()
            
            print(f"Executing SQL ({len(sql)} bytes)")
            # In a real implementation, this would execute the SQL
            
            # If it's a SELECT query, return some mock results
            if sql.strip().upper().startswith("SELECT"):
                return [("result1",), ("result2",)]
            
            # For non-SELECT queries, return an empty list
            return []
        
        def get_max_query_size(self):
            return self.max_query_size
        
        def close(self):
            print("Closing database connection")
            self.connection = None
    
    # Use the custom adapter
    adapter = MyCustomAdapter("mydb://localhost:1234")
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # Define some statements
    statements = [
        "CREATE TABLE test (id INTEGER, value TEXT)",
        "INSERT INTO test VALUES (1, 'Test 1')",
        "INSERT INTO test VALUES (2, 'Test 2')"
    ]
    
    # Process statements using the custom adapter
    batcher.process_statements(statements, adapter.execute)
    
    # Query the data
    results = adapter.execute("SELECT * FROM test")
    print("Query results:", results)
    
    # Clean up
    adapter.close()

Performance Optimization
=====================

Some tips for optimizing performance with SQL Batcher:

1. **Adjust the batch size**: Choose a batch size that balances performance and memory usage.

   .. code-block:: python
   
       # For most systems, 1-5MB is a good starting point
       batcher = SQLBatcher(max_bytes=5_000_000)
   
       # For databases with large query limits, you can go higher
       batcher = SQLBatcher(max_bytes=16_000_000)  # 16MB for Trino

2. **Use transactions**: Wrap batches in transactions for better performance.

   .. code-block:: python
   
       adapter.begin_transaction()
       batcher.process_statements(statements, adapter.execute)
       adapter.commit_transaction()

3. **Disable result fetching for inserts**: When you don't need results, set fetch_results=False.

   .. code-block:: python
   
       # Don't fetch results for INSERT statements
       adapter = GenericAdapter(connection=connection, fetch_results=False)

4. **Use custom size calculation**: For specialized use cases, you can provide a custom function to calculate statement sizes.

   .. code-block:: python
   
       # Example: Custom size function that accounts for Unicode characters
       def get_unicode_size(sql):
           return len(sql.encode('utf-8'))
       
       batcher = SQLBatcher(max_bytes=1_000_000, size_func=get_unicode_size)