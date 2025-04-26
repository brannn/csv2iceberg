########
Adapters
########

SQL Batcher comes with several database adapters that simplify integration with popular database systems. Each adapter handles the specifics of connecting to and executing SQL against its respective database.

Available Adapters
=================

.. toctree::
   :maxdepth: 1
   
   generic
   trino
   spark
   snowflake
   custom

Adapter Selection Guide
=====================

The following table can help you select the right adapter for your database system:

+------------+-------------------+-------------------------+------------------------+
| Database   | Adapter           | Strengths               | Limitations            |
+============+===================+=========================+========================+
| Trino      | TrinoAdapter      | - Native integration    | - Requires trino      |
|            |                   | - Query size handling   |   Python package      |
|            |                   | - Role-based auth       |                        |
+------------+-------------------+-------------------------+------------------------+
| Spark SQL  | SparkAdapter      | - Works with PySpark    | - Requires PySpark    |
|            |                   | - Supports Spark SQL    |   package             |
|            |                   | - DataFrame integration |                        |
+------------+-------------------+-------------------------+------------------------+
| Snowflake  | SnowflakeAdapter  | - Transaction support   | - Requires snowflake- |
|            |                   | - Connection pooling    |   connector-python    |
|            |                   | - Security integrations |                        |
+------------+-------------------+-------------------------+------------------------+
| Generic    | GenericAdapter    | - Works with any DBAPI  | - Minimal integration |
|            |                   | - No dependencies       | - Basic feature set   |
|            |                   | - Simple to use         |                        |
+------------+-------------------+-------------------------+------------------------+

Common Adapter Features
=====================

All adapters implement a common interface that includes:

- ``execute(sql)``: Execute a SQL statement
- ``get_max_query_size()``: Get the maximum recommended batch size
- ``close()``: Close database connections
- Transaction management methods (if supported by the database)

Dependency Management
===================

Each adapter has specific dependencies:

- **TrinoAdapter**: Requires ``trino`` package
- **SparkAdapter**: Requires ``pyspark`` package
- **SnowflakeAdapter**: Requires ``snowflake-connector-python`` package
- **GenericAdapter**: No additional dependencies

When an adapter's dependencies are not installed, SQL Batcher will raise an ImportError when you try to use it.

Common Usage Pattern
==================

All adapters follow this common usage pattern:

.. code-block:: python

    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.xyz import XYZAdapter
    
    # 1. Create the adapter with connection details
    adapter = XYZAdapter(connection_params)
    
    # 2. Create a batcher with the adapter's recommended size limit
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # 3. Define SQL statements to execute
    statements = [...]
    
    # 4. Process statements using the adapter's execute method
    batcher.process_statements(statements, adapter.execute)
    
    # 5. Clean up
    adapter.close()