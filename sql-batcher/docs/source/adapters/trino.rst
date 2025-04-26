##############
Trino Adapter
##############

The Trino Adapter provides specialized integration with Trino (formerly PrestoSQL) database servers.

Features
========

- Native Trino integration via the ``trino`` Python package
- Support for authentication methods (basic, Kerberos, JWT)
- Custom headers for roles and additional parameters
- Detailed error handling with Trino-specific information
- Query size limit awareness for Trino

Installation
===========

The Trino Adapter requires the ``trino`` Python package:

.. code-block:: bash

    pip install sql-batcher[trino]

Or install the dependency directly:

.. code-block:: bash

    pip install trino>=0.305

Basic Usage
==========

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
    
    # Create a batcher with Trino's size limits
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # Define SQL statements
    statements = [
        "INSERT INTO example_table VALUES (1, 'First row')",
        "INSERT INTO example_table VALUES (2, 'Second row')",
        # ... more statements
    ]
    
    # Process statements
    batcher.process_statements(statements, adapter.execute)
    
    # Clean up
    adapter.close()

Authentication Options
=====================

Basic Authentication
------------------

.. code-block:: python

    adapter = TrinoAdapter(
        host="trino.example.com",
        user="admin",
        password="password",  # For basic auth
        catalog="hive",
        schema="default"
    )

Using a Role
----------

.. code-block:: python

    adapter = TrinoAdapter(
        host="trino.example.com",
        user="admin",
        catalog="hive",
        schema="default",
        role="sysadmin"  # Use a specific Trino role
    )

Custom Authentication
-------------------

For more complex authentication scenarios, you can pass custom auth classes:

.. code-block:: python

    from trino.auth import KerberosAuthentication
    
    adapter = TrinoAdapter(
        host="trino.example.com",
        user="admin",
        catalog="hive",
        schema="default",
        auth=KerberosAuthentication()
    )

Parameters
=========

The ``TrinoAdapter`` constructor accepts the following parameters:

- ``host``: Trino server hostname or IP (required)
- ``port``: Trino server port (default: 443)
- ``user``: Username for authentication (default: "admin")
- ``password``: Password for basic authentication (optional)
- ``catalog``: Default catalog name (default: "hive")
- ``schema``: Default schema name (default: "default")
- ``http_scheme``: HTTP scheme (default: "https")
- ``role``: Trino role to use (optional)
- ``auth``: Custom authentication instance (optional)
- ``max_query_size``: Maximum query size in bytes (default: 16,777,216)
- ``headers``: Additional HTTP headers to send (optional)
- ``verify``: SSL certificate verification (default: True)
- ``dry_run``: If True, don't actually connect to Trino (default: False)

Size Limits in Trino
===================

Trino has a default HTTP request size limit of 16MB. The ``TrinoAdapter`` uses this as the default ``max_query_size`` value. You may need to adjust this based on your specific Trino server configuration.

Error Handling
=============

The ``TrinoAdapter`` provides detailed error information for Trino-specific exceptions:

.. code-block:: python

    try:
        adapter.execute("SELECT * FROM nonexistent_table")
    except Exception as e:
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        # With Trino errors, you may get additional details like:
        # - Error name (e.g., "TABLE_NOT_FOUND")
        # - Error type (e.g., "SEMANTIC_ERROR")
        # - Line/position information
        # - Query ID for troubleshooting