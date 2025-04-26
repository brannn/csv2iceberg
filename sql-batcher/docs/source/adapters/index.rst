########
Adapters
########

.. toctree::
   :maxdepth: 2
   
   overview
   generic
   trino
   spark
   snowflake
   custom

SQL Batcher Adapters
==================

Adapters are the bridge between SQLBatcher and specific database systems. They handle the details of connecting to and executing SQL against different types of databases.

All adapters implement a common interface defined by the ``SQLAdapter`` abstract base class, making it easy to switch between different database backends or create your own custom adapters.

Available Adapters
================

Generic Adapter
-------------

The ``GenericAdapter`` works with any database connection that follows the Python Database API Specification (PEP 249), providing a simple way to use SQLBatcher with a wide range of database systems.

.. code-block:: python

   import sqlite3
   from sql_batcher import SQLBatcher
   from sql_batcher.adapters.generic import GenericAdapter
   
   # Create a connection and adapter
   connection = sqlite3.connect(":memory:")
   adapter = GenericAdapter(connection=connection)
   
   # Use the adapter with SQLBatcher
   batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
   batcher.process_statements(statements, adapter.execute)

Specialized Adapters
-----------------

SQL Batcher includes specialized adapters for:

- **Trino/Starburst**: Optimized for Trino with size limits and authentication options
- **Spark SQL**: Works with PySpark's SparkSession for Spark SQL operations
- **Snowflake**: Connects to Snowflake with transaction support and timeout handling

Custom Adapters
------------

You can create your own adapters for other database systems by implementing the ``SQLAdapter`` interface. See the :doc:`custom` page for details.