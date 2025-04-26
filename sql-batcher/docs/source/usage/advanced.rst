###############
Advanced Usage
###############

This section covers advanced use cases and features of SQL Batcher.

Dry Run Mode
===========

SQL Batcher provides a "dry run" mode that allows you to test your batching logic without executing any SQL statements. This is useful for:

* Testing batch sizes
* Validating SQL statement formatting
* Debugging batching logic

Example:

.. code-block:: python

    from sql_batcher import SQLBatcher

    # Create a batcher in dry run mode
    batcher = SQLBatcher(max_bytes=100_000, dry_run=True)

    # Define statements
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        # ... many more statements
    ]

    # In dry run mode, this function won't be called
    def execute_sql(sql):
        print(f"Would execute: {sql}")

    # Process statements (nothing will be executed)
    batcher.process_statements(statements, execute_sql)

Using Query Collectors
=====================

Query collectors allow you to capture what SQL would be executed in dry run mode. This is especially useful for debugging and testing.

.. code-block:: python

    from sql_batcher import SQLBatcher
    from sql_batcher.query_collector import SimpleQueryCollector

    # Create a batcher in dry run mode
    batcher = SQLBatcher(max_bytes=100_000, dry_run=True)

    # Create a query collector
    collector = SimpleQueryCollector()

    # Define statements
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        # ... more statements
    ]

    # Process statements with the collector
    batcher.process_statements(
        statements, 
        lambda sql: None,  # No-op execution function
        query_collector=collector,
        metadata={"operation": "insert", "table": "users"}
    )

    # Examine collected queries
    for i, query in enumerate(collector.queries):
        print(f"Batch {i+1}:")
        print(f"SQL: {query.sql}")
        print(f"Metadata: {query.metadata}")
        print(f"Timestamp: {query.timestamp}")

Custom Adapters
==============

If you need to support a database system that doesn't have a built-in adapter, you can create your own by subclassing ``SQLAdapter``:

.. code-block:: python

    from sql_batcher.adapters.base import SQLAdapter

    class MyCustomAdapter(SQLAdapter):
        """Adapter for my custom database system."""
        
        # Default query size limit for this database
        DEFAULT_MAX_QUERY_SIZE = 1_000_000
        
        def __init__(self, connection_params, max_query_size=None):
            """Initialize the adapter with connection parameters."""
            self.connection_params = connection_params
            self.connection = None
            self.max_query_size = max_query_size or self.DEFAULT_MAX_QUERY_SIZE
            self._connect()
        
        def _connect(self):
            """Establish a connection to the database."""
            # Implementation depends on your database's Python driver
            self.connection = MyDatabaseLibrary.connect(**self.connection_params)
        
        def execute(self, sql):
            """Execute a SQL statement."""
            cursor = self.connection.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
        
        def get_max_query_size(self):
            """Return the maximum query size in bytes."""
            return self.max_query_size
        
        def close(self):
            """Close the database connection."""
            if self.connection:
                self.connection.close()

Error Handling
=============

SQL Batcher provides several ways to handle errors during batch execution:

Handling Individual Statement Failures
-------------------------------------

.. code-block:: python

    from sql_batcher import SQLBatcher

    # Create a batcher
    batcher = SQLBatcher()

    # Define an execution function with error handling
    def execute_with_error_handling(sql):
        try:
            # Attempt to execute the SQL
            connection.execute(sql)
            return True
        except Exception as e:
            print(f"Error executing SQL: {str(e)}")
            # Log the error, but don't stop processing
            return False

    # Process statements with error handling
    batcher.process_statements(statements, execute_with_error_handling)

Custom Sizing Function
=====================

If you need more control over how the batch size is calculated, you can provide a custom sizing function:

.. code-block:: python

    from sql_batcher import SQLBatcher

    # Define a custom sizing function
    def complex_size_calculator(sql):
        # Basic size in bytes
        basic_size = len(sql.encode('utf-8'))
        
        # Add weight for specific operations
        if "JOIN" in sql.upper():
            basic_size *= 1.5  # JOIN operations are more expensive
        
        if "ORDER BY" in sql.upper():
            basic_size *= 1.2  # ORDER BY adds overhead
            
        return int(basic_size)

    # Create a batcher with the custom sizing function
    batcher = SQLBatcher(max_bytes=500_000, size_func=complex_size_calculator)

    # Use the batcher as normal
    batcher.process_statements(statements, execute_sql)