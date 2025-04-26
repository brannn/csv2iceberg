###########
Basic Usage
###########

This section covers the fundamental concepts and basic usage of SQL Batcher.

Core Concepts
============

SQL Batcher works by:

1. Collecting SQL statements until a batch reaches a specified size limit
2. Executing each batch when it's full
3. Providing detailed feedback on the batching process

Key Parameters
-------------

When creating a ``SQLBatcher`` instance, you can configure these parameters:

* ``max_bytes``: Maximum batch size in bytes (default: 1,000,000 bytes / ~1MB)
* ``delimiter``: SQL statement delimiter for batching (default: ";")
* ``dry_run``: If True, don't actually execute statements (default: False)

Simple Example
=============

Here's a basic example showing how to use SQL Batcher:

.. code-block:: python

    from sql_batcher import SQLBatcher

    # Create a batcher with default settings
    batcher = SQLBatcher()

    # Define SQL statements to execute
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "INSERT INTO users VALUES (3, 'Charlie')"
    ]

    # Define an execution function
    def execute_sql(sql):
        print(f"Executing SQL: {sql}")
        # In a real scenario, this would execute the SQL using your DB connection
        # For example: connection.cursor().execute(sql)

    # Process all statements
    batcher.process_statements(statements, execute_sql)

Batch Size Considerations
========================

SQL Batcher uses byte size rather than statement count to determine batches. This is especially important for databases like Trino, Spark, and Snowflake that have limits on the query string size.

When a batch reaches the ``max_bytes`` limit:

1. The current batch is executed
2. The batcher is reset
3. The new statement is added to start a fresh batch

Oversized Statements
-------------------

If a single statement exceeds the ``max_bytes`` limit, SQL Batcher will:

1. Log a warning
2. Execute the oversized statement individually
3. Continue with the next statement in a new batch

This ensures that all statements are processed even if some exceed the size limit.

Working with Transaction Blocks
==============================

For databases that support transactions, you can use SQL Batcher like this:

.. code-block:: python

    from sql_batcher import SQLBatcher

    # Create a batcher
    batcher = SQLBatcher(max_bytes=500_000)

    # Start a transaction
    connection.execute("BEGIN TRANSACTION")

    try:
        # Process statements within the transaction
        batcher.process_statements(statements, lambda sql: connection.execute(sql))
        
        # Commit if all statements succeed
        connection.execute("COMMIT")
    except Exception as e:
        # Rollback on error
        connection.execute("ROLLBACK")
        raise e