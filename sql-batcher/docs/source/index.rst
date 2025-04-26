SQL Batcher Documentation
========================

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   
   introduction
   installation
   getting_started
   usage
   api/modules
   adapters/index
   examples
   contributing
   changelog

SQL Batcher - Efficiently Batch SQL Statements
============================================

**SQL Batcher** is a Python package that helps you efficiently execute many SQL statements while respecting query size limitations. It's especially useful for database systems like Trino, Spark SQL, and Snowflake that have limits on query string size.

Key Features
-----------

- **Adaptive Batching**: Batch SQL statements based on actual byte size rather than simple counts
- **Database Agnostic**: Use with any database through the generic adapter
- **Specialized Adapters**: Optimized adapters for Trino, Spark SQL, and Snowflake
- **Modular Design**: Create custom adapters for other database systems
- **Dry Run Support**: Simulate execution without actually running SQL
- **Transaction Support**: Optional transaction handling for compatible databases

Installation
-----------

Install from PyPI:

.. code-block:: bash

   pip install sql-batcher

Or with optional dependencies:

.. code-block:: bash

   pip install sql-batcher[trino,spark,snowflake]

Quick Start
----------

.. code-block:: python

   from sql_batcher import SQLBatcher
   
   # Create a batcher
   batcher = SQLBatcher(max_bytes=1000000)
   
   # Define SQL statements
   statements = [
       "INSERT INTO users VALUES (1, 'Alice')",
       "INSERT INTO users VALUES (2, 'Bob')",
       # ... many more statements
   ]
   
   # Define a function to execute SQL
   def execute_sql(sql):
       print(f"Executing: {sql}")
       # In a real scenario, this would execute using your database connection
   
   # Process all statements, automatically batching as needed
   batcher.process_statements(statements, execute_sql)

With a database adapter:

.. code-block:: python

   import sqlite3
   from sql_batcher import SQLBatcher
   from sql_batcher.adapters.generic import GenericAdapter
   
   # Create a connection and adapter
   connection = sqlite3.connect(":memory:")
   adapter = GenericAdapter(connection=connection)
   
   # Create a batcher
   batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
   
   # Use the adapter directly as the execution callback
   batcher.process_statements(statements, adapter.execute)

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`