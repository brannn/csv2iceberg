###############
Generic Adapter
###############

The Generic Adapter provides a simple way to integrate SQL Batcher with any database system that follows the Python DBAPI 2.0 specification (PEP 249).

Features
========

- Works with any DBAPI-compliant database connection
- No dependencies beyond Python standard library
- Transaction support if the underlying connection supports it
- Simple and lightweight implementation

Usage
=====

Basic example:

.. code-block:: python

    import sqlite3
    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.generic import GenericAdapter
    
    # Create a database connection
    connection = sqlite3.connect(":memory:")
    
    # Create a generic adapter
    adapter = GenericAdapter(connection=connection)
    
    # Create a batcher
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # Create a test table
    adapter.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    """)
    
    # Define some INSERT statements
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "INSERT INTO users VALUES (3, 'Charlie')"
    ]
    
    # Process statements
    batcher.process_statements(statements, adapter.execute)
    
    # Query the data
    results = adapter.execute("SELECT * FROM users")
    for row in results:
        print(row)
    
    # Clean up
    adapter.close()

Working with Transactions
========================

The Generic Adapter supports transaction management if the underlying connection provides it:

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
    
    # Create a generic adapter
    adapter = GenericAdapter(connection=connection)
    
    # Create a batcher
    batcher = SQLBatcher(max_bytes=500000)
    
    # Define statements
    statements = [...]
    
    # Use transaction management
    adapter.begin_transaction()
    try:
        batcher.process_statements(statements, adapter.execute)
        adapter.commit_transaction()
    except Exception as e:
        adapter.rollback_transaction()
        raise e
    finally:
        adapter.close()

Parameters
=========

The ``GenericAdapter`` constructor accepts the following parameters:

- ``connection``: A DBAPI-compliant database connection
- ``max_query_size``: Maximum query size in bytes (default: 1,000,000)
- ``fetch_results``: Whether to fetch and return results (default: True)

Compatible Database Drivers
=========================

GenericAdapter works with many Python database drivers, including:

- sqlite3 (standard library)
- psycopg2 (PostgreSQL)
- mysql-connector-python (MySQL)
- cx_Oracle (Oracle)
- pymssql (Microsoft SQL Server)
- ibm_db (IBM DB2)

And any other driver that implements the Python DBAPI 2.0 specification.