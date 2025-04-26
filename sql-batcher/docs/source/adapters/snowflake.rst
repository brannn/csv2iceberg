##################
Snowflake Adapter
##################

The Snowflake Adapter provides integration with Snowflake, allowing SQL Batcher to efficiently execute batched SQL statements against Snowflake databases.

Features
========

- Native Snowflake integration via the ``snowflake-connector-python`` package
- Support for various authentication methods (password, key pair, SSO)
- Transaction management
- Parameter binding for improved security
- Connection pooling support

Installation
===========

The Snowflake Adapter requires the ``snowflake-connector-python`` package:

.. code-block:: bash

    pip install sql-batcher[snowflake]

Or install the dependency directly:

.. code-block:: bash

    pip install snowflake-connector-python>=2.7

Basic Usage
==========

.. code-block:: python

    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.snowflake import SnowflakeAdapter
    
    # Connection parameters
    connection_params = {
        "user": "username",
        "password": "password",
        "account": "your_account",
        "warehouse": "compute_wh",
        "database": "your_database",
        "schema": "public"
    }
    
    # Create a Snowflake adapter
    adapter = SnowflakeAdapter(connection_params=connection_params)
    
    # Create a batcher with Snowflake's limits
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # Create a test table
    adapter.execute("""
        CREATE TABLE IF NOT EXISTS example_table (
            id INTEGER,
            name VARCHAR
        )
    """)
    
    # Define SQL statements
    statements = [
        "INSERT INTO example_table VALUES (1, 'First')",
        "INSERT INTO example_table VALUES (2, 'Second')",
        # ... more statements
    ]
    
    # Process statements
    batcher.process_statements(statements, adapter.execute)
    
    # Query the data
    results = adapter.execute("SELECT * FROM example_table")
    for row in results:
        print(row)
    
    # Clean up
    adapter.close()

Transaction Support
=================

Snowflake Adapter provides explicit transaction control:

.. code-block:: python

    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.snowflake import SnowflakeAdapter
    
    # Create adapter
    adapter = SnowflakeAdapter(connection_params=connection_params)
    batcher = SQLBatcher(max_bytes=1_000_000)
    
    # Use explicit transaction management
    adapter.begin_transaction()
    
    try:
        # Process statements within the transaction
        batcher.process_statements(statements, adapter.execute)
        
        # Commit if all statements succeed
        adapter.commit_transaction()
        print("Transaction committed successfully!")
    except Exception as e:
        # Rollback on error
        adapter.rollback_transaction()
        print(f"Transaction rolled back due to error: {str(e)}")
    finally:
        # Always close the connection
        adapter.close()

Authentication Options
=====================

Password Authentication (Basic)
-----------------------------

.. code-block:: python

    connection_params = {
        "user": "username",
        "password": "password",
        "account": "your_account",
        "warehouse": "compute_wh",
        "database": "your_database",
        "schema": "public"
    }

Key Pair Authentication
---------------------

.. code-block:: python

    connection_params = {
        "user": "username",
        "account": "your_account",
        "private_key_path": "/path/to/private_key.p8",
        "private_key_passphrase": "passphrase",  # Optional
        "warehouse": "compute_wh",
        "database": "your_database",
        "schema": "public"
    }

SSO Authentication
----------------

.. code-block:: python

    connection_params = {
        "user": "username",
        "account": "your_account",
        "authenticator": "externalbrowser",
        "warehouse": "compute_wh",
        "database": "your_database",
        "schema": "public"
    }

Parameters
=========

The ``SnowflakeAdapter`` constructor accepts the following parameters:

- ``connection_params``: Dictionary of connection parameters (required)
- ``max_query_size``: Maximum query size in bytes (default: 1,048,576)
- ``auto_close``: Whether to automatically close the connection when adapter is garbage collected (default: False)
- ``connection_timeout``: Connection timeout in seconds (default: 60)
- ``fetch_limit``: Maximum number of rows to fetch for SELECT queries (default: None)

Connection Parameters
===================

The ``connection_params`` dictionary can include:

- ``user``: Snowflake username (required)
- ``password``: Password for authentication
- ``account``: Snowflake account identifier (required)
- ``warehouse``: Default warehouse (required)
- ``database``: Default database (required)
- ``schema``: Default schema (default: PUBLIC)
- ``role``: Role to use for the session
- ``authenticator``: Authentication method (default is password)
- ``private_key_path``: Path to private key file for key pair authentication
- ``private_key_passphrase``: Passphrase for private key
- ``autocommit``: Whether to enable autocommit (default: False)
- ``client_session_keep_alive``: Keep the session alive (default: False)
- ``client_prefetch_threads``: Number of prefetch threads (default: 4)

Size Limits in Snowflake
======================

Snowflake has a default SQL statement size limit of 1MB (1,048,576 bytes). The ``SnowflakeAdapter`` uses this as the default ``max_query_size`` value. For larger statements, you may need to use the COPY command or other bulk loading methods.