################
Getting Started
################

This guide will help you start using SQL Batcher quickly with practical examples.

Installation
===========

First, install SQL Batcher using pip:

.. code-block:: bash

    pip install sql-batcher

For specialized database support, install with the appropriate extra:

.. code-block:: bash

    # For Trino support
    pip install sql-batcher[trino]
    
    # For Spark support
    pip install sql-batcher[spark]
    
    # For Snowflake support
    pip install sql-batcher[snowflake]
    
    # For all database adapters
    pip install sql-batcher[all]

Basic Usage
=========

The core component of SQL Batcher is the ``SQLBatcher`` class, which handles batching SQL statements based on size limits.

Here's a simple example:

.. code-block:: python

    from sql_batcher import SQLBatcher
    
    # Create a batcher with a 1MB size limit
    batcher = SQLBatcher(max_bytes=1_000_000)
    
    # Define SQL statements to execute
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "INSERT INTO users VALUES (3, 'Charlie')"
    ]
    
    # Define a function to execute the batched SQL
    def execute_sql(sql):
        print(f"Executing SQL batch: {sql}")
        # In a real scenario, this would execute the SQL using your database connection
    
    # Process all statements, automatically batching as needed
    batcher.process_statements(statements, execute_sql)

Using with Database Adapters
=========================

SQL Batcher includes adapters for various database systems, which simplify the process of executing SQL.

Generic Database Adapter
---------------------

The ``GenericAdapter`` works with any database connection that follows the Python Database API Specification (PEP 249):

.. code-block:: python

    import sqlite3
    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.generic import GenericAdapter
    
    # Create a SQLite in-memory database
    connection = sqlite3.connect(":memory:")
    
    # Create the adapter
    adapter = GenericAdapter(connection=connection)
    
    # Create a batcher
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # Create a table
    adapter.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    
    # Define insert statements
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "INSERT INTO users VALUES (3, 'Charlie')"
    ]
    
    # Process statements using the adapter as the execution callback
    batcher.process_statements(statements, adapter.execute)
    
    # Query the data
    results = adapter.execute("SELECT * FROM users ORDER BY id")
    for row in results:
        print(row)
    
    # Clean up
    adapter.close()

Trino Adapter
-----------

If you've installed with Trino support, you can use the ``TrinoAdapter``:

.. code-block:: python

    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.trino import TrinoAdapter
    
    # Create a Trino adapter
    adapter = TrinoAdapter(
        host="trino.example.com",
        port=443,
        user="admin",
        catalog="hive",
        schema="default",
        http_scheme="https"
    )
    
    # Create a batcher
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # Process statements
    batcher.process_statements(statements, adapter.execute)
    
    # Clean up
    adapter.close()

Spark Adapter
-----------

If you've installed with Spark support, you can use the ``SparkAdapter``:

.. code-block:: python

    from pyspark.sql import SparkSession
    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.spark import SparkAdapter
    
    # Create a Spark session
    spark = SparkSession.builder.appName("SQLBatcherExample").getOrCreate()
    
    # Create a Spark adapter
    adapter = SparkAdapter(spark_session=spark)
    
    # Create a batcher
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # Process statements
    batcher.process_statements(statements, adapter.execute)
    
    # Clean up
    adapter.close()

Snowflake Adapter
--------------

If you've installed with Snowflake support, you can use the ``SnowflakeAdapter``:

.. code-block:: python

    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.snowflake import SnowflakeAdapter
    
    # Connection parameters for Snowflake
    connection_params = {
        "account": "your_account",
        "user": "your_username",
        "password": "your_password",
        "database": "your_database",
        "schema": "your_schema",
        "warehouse": "your_warehouse"
    }
    
    # Create a Snowflake adapter
    adapter = SnowflakeAdapter(connection_params=connection_params)
    
    # Create a batcher
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # Process statements
    batcher.process_statements(statements, adapter.execute)
    
    # Clean up
    adapter.close()

Advanced Features
==============

Dry Run Mode
----------

You can use dry run mode to see what would be executed without actually running the SQL:

.. code-block:: python

    from sql_batcher import SQLBatcher
    from sql_batcher.query_collector import ListQueryCollector
    
    # Create a query collector to store the SQL
    collector = ListQueryCollector()
    
    # Create a batcher in dry run mode
    batcher = SQLBatcher(max_bytes=100_000, dry_run=True)
    
    # Define statements
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        # ... many more statements
    ]
    
    # Process statements without executing them
    batcher.process_statements(
        statements,
        lambda x: None,  # This won't be called in dry run mode
        query_collector=collector
    )
    
    # Review the batches that would have been executed
    for i, query_info in enumerate(collector.get_queries()):
        print(f"Batch {i+1}:")
        print(query_info["query"])
        print()

Custom Size Calculation
--------------------

You can provide a custom function to calculate statement sizes:

.. code-block:: python

    from sql_batcher import SQLBatcher
    
    # Define a custom size function
    def my_size_function(sql):
        # Example: Count only non-whitespace characters
        return len(''.join(sql.split()))
    
    # Create a batcher with the custom size function
    batcher = SQLBatcher(max_bytes=100_000, size_func=my_size_function)
    
    # Use as normal
    batcher.process_statements(statements, execute_sql)

Transaction Support
----------------

Many adapters support transactions:

.. code-block:: python

    from sql_batcher import SQLBatcher
    
    # Using an adapter that supports transactions
    adapter.begin_transaction()
    
    try:
        # Process statements within a transaction
        batcher.process_statements(statements, adapter.execute)
        
        # Commit if all statements succeed
        adapter.commit_transaction()
        
    except Exception as e:
        # Rollback if any statement fails
        adapter.rollback_transaction()
        raise e
    
    finally:
        # Always clean up
        adapter.close()

What's Next?
==========

Now that you've learned the basics of SQL Batcher, you can:

- Explore more detailed :doc:`examples`
- Check out the :doc:`api/modules` for a complete API reference
- Learn about creating :doc:`adapters/custom` for your specific database
- Read the complete :doc:`usage` guide for advanced usage patterns