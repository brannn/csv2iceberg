################
Generic Adapter
################

The Generic Adapter is a versatile adapter that works with any database connection that follows the Python Database API Specification (PEP 249), such as SQLite, PostgreSQL, MySQL, and many others.

Basic Usage
=========

.. code-block:: python

    import sqlite3
    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.generic import GenericAdapter
    
    # Create a database connection
    connection = sqlite3.connect(":memory:")
    
    # Create the adapter
    adapter = GenericAdapter(connection=connection)
    
    # Create a batcher using the adapter's size limit
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # Create a table
    adapter.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    
    # Define some insert statements
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "INSERT INTO users VALUES (3, 'Charlie')"
    ]
    
    # Process the statements using the adapter as the callback
    batcher.process_statements(statements, adapter.execute)
    
    # Query the data
    results = adapter.execute("SELECT * FROM users ORDER BY id")
    for row in results:
        print(row)
    
    # Clean up
    adapter.close()

Using with Other Databases
========================

PostgreSQL Example:

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
    adapter = GenericAdapter(
        connection=connection,
        max_query_size=5_000_000  # 5MB limit
    )
    
    # Use the adapter with SQLBatcher
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    batcher.process_statements(statements, adapter.execute)
    
    # Use transactions if needed
    adapter.begin_transaction()
    try:
        batcher.process_statements(statements, adapter.execute)
        adapter.commit_transaction()
    except Exception as e:
        adapter.rollback_transaction()
        raise e
    finally:
        adapter.close()

MySQL Example:

.. code-block:: python

    import mysql.connector
    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.generic import GenericAdapter
    
    # Connect to MySQL
    connection = mysql.connector.connect(
        host="localhost",
        database="mydb",
        user="root",
        password="password"
    )
    
    # Create the adapter
    adapter = GenericAdapter(connection=connection)
    
    # Use the adapter with SQLBatcher
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    batcher.process_statements(statements, adapter.execute)
    
    # Clean up
    adapter.close()

Configuration Options
===================

The ``GenericAdapter`` accepts the following parameters:

connection
---------
**Required**. A DBAPI 2.0 compliant database connection. This can be any connection object that follows the Python Database API Specification (PEP 249).

max_query_size
------------
**Optional**. The maximum query size in bytes. Default: 1,000,000 (1MB).

fetch_results
-----------
**Optional**. Whether to fetch and return results for SELECT queries. Default: True.

If set to False, execute() will always return an empty list, even for SELECT queries. This can be useful for performance when you don't need the results.

Handling Transactions
===================

The ``GenericAdapter`` provides methods for transaction management:

.. code-block:: python

    # Begin a transaction
    adapter.begin_transaction()
    
    try:
        # Execute some statements
        batcher.process_statements(statements, adapter.execute)
        
        # Commit the transaction
        adapter.commit_transaction()
    except Exception as e:
        # Rollback on error
        adapter.rollback_transaction()
        raise e

Not all databases support transactions in the same way. The adapter tries to use the connection's native transaction methods if available, or falls back to sending BEGIN/COMMIT/ROLLBACK statements directly.

Technical Notes
============

- The adapter automatically detects if a statement is a SELECT query by examining the cursor's description attribute after execution. If description is not None, it assumes the statement returns results and fetches them.

- For large result sets, consider using a server-side cursor or setting fetch_results=False and handling the fetching manually to avoid loading all results into memory at once.

- The adapter doesn't provide query parameter binding - it expects fully formed SQL statements. For dynamic queries, format your statements before passing them to the batcher.