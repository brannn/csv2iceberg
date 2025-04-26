################
Adapter Overview
################

SQL Batcher uses a modular adapter system to connect with different database systems. This overview explains the adapter architecture and how to choose the right adapter for your needs.

What Are Adapters?
===============

Adapters are the bridge between SQLBatcher and specific database systems. They handle the details of:

- Connecting to the database
- Executing SQL statements
- Retrieving results
- Managing transactions
- Determining maximum query sizes

Each adapter implements a common interface, making it easy to switch between different database backends.

Adapter Architecture
=================

All adapters in SQL Batcher implement the ``SQLAdapter`` abstract base class, which defines these key methods:

.. code-block:: python

    class SQLAdapter(ABC):
        @abstractmethod
        def execute(self, sql):
            """Execute a SQL statement and return results."""
            pass
        
        @abstractmethod
        def get_max_query_size(self):
            """Return the maximum query size in bytes."""
            pass
        
        @abstractmethod
        def close(self):
            """Close the database connection."""
            pass
        
        def begin_transaction(self):
            """Begin a transaction (if supported)."""
            pass
        
        def commit_transaction(self):
            """Commit a transaction (if supported)."""
            pass
        
        def rollback_transaction(self):
            """Rollback a transaction (if supported)."""
            pass

This unified interface ensures that all adapters work consistently with SQLBatcher.

Available Adapters
===============

SQL Batcher includes these adapters:

1. **GenericAdapter**: Works with any DBAPI 2.0 compliant database connection
2. **TrinoAdapter**: Optimized for Trino/Starburst databases
3. **SparkAdapter**: Works with PySpark's SparkSession for Spark SQL
4. **SnowflakeAdapter**: Connects to Snowflake databases

Additionally, you can create custom adapters for other database systems by implementing the ``SQLAdapter`` interface.

Choosing the Right Adapter
=======================

The best adapter for your needs depends on your database system and requirements:

Generic Adapter
-------------

Use the **GenericAdapter** when:

- Working with standard DBAPI-compliant database drivers
- Using common databases like SQLite, PostgreSQL, MySQL, etc.
- You need a simple, versatile adapter
- You already have a database connection object

.. code-block:: python

    import sqlite3
    from sql_batcher.adapters.generic import GenericAdapter
    
    connection = sqlite3.connect(":memory:")
    adapter = GenericAdapter(connection=connection)

Trino Adapter
-----------

Use the **TrinoAdapter** when:

- Working with Trino or Starburst databases
- You need optimized handling of Trino's size limitations
- You want built-in authentication options
- You need access to Trino-specific settings

.. code-block:: python

    from sql_batcher.adapters.trino import TrinoAdapter
    
    adapter = TrinoAdapter(
        host="trino.example.com",
        port=443,
        user="admin",
        catalog="hive",
        schema="default"
    )

Spark Adapter
-----------

Use the **SparkAdapter** when:

- Working with Spark SQL via PySpark
- You need to execute SQL within a Spark context
- You want to work with Spark DataFrames

.. code-block:: python

    from pyspark.sql import SparkSession
    from sql_batcher.adapters.spark import SparkAdapter
    
    spark = SparkSession.builder.appName("Example").getOrCreate()
    adapter = SparkAdapter(spark_session=spark)

Snowflake Adapter
--------------

Use the **SnowflakeAdapter** when:

- Working with Snowflake databases
- You need optimized handling of Snowflake's query limitations
- You need transaction support for Snowflake

.. code-block:: python

    from sql_batcher.adapters.snowflake import SnowflakeAdapter
    
    adapter = SnowflakeAdapter(
        connection_params={
            "account": "your_account",
            "user": "your_username",
            "password": "your_password",
            "database": "your_database"
        }
    )

Custom Adapters
------------

Create a custom adapter when:

- Working with a database system that doesn't have a built-in adapter
- You need specialized handling for a particular database
- You want to add custom features or optimizations

See the :doc:`custom` page for details on creating custom adapters.

Adapter Features Comparison
========================

+----------------+----------------+----------------+----------------+----------------+
| Feature        | GenericAdapter | TrinoAdapter   | SparkAdapter   | SnowflakeAdapter |
+================+================+================+================+================+
| DBAPI Support  | ✅            | ✅            | ❌            | ✅            |
+----------------+----------------+----------------+----------------+----------------+
| Authentication | Basic          | Multiple Options | Spark Context  | Multiple Options |
+----------------+----------------+----------------+----------------+----------------+
| Transactions   | If supported   | Limited        | Limited        | ✅            |
+----------------+----------------+----------------+----------------+----------------+
| DataFrame      | ❌            | ❌            | ✅            | ❌            |
| Support        |                |                |                |                |
+----------------+----------------+----------------+----------------+----------------+
| Default Size   | 1 MB           | 16 MB          | 1 MB           | 1 MB           |
| Limit          |                |                |                |                |
+----------------+----------------+----------------+----------------+----------------+
| Lazy           | ❌            | ✅            | ✅            | ✅            |
| Connections    |                |                |                |                |
+----------------+----------------+----------------+----------------+----------------+

Adapter Initialization
===================

Each adapter has its own initialization parameters, but all share a consistent pattern:

1. Create the adapter with connection parameters
2. Use the adapter's ``execute`` method directly or with a batcher
3. Close the adapter when finished

Example:

.. code-block:: python

    # Step 1: Create the adapter
    adapter = SomeAdapter(connection_params)
    
    # Step 2: Use the adapter
    results = adapter.execute("SELECT * FROM some_table")
    
    # Or use with a batcher
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    batcher.process_statements(statements, adapter.execute)
    
    # Step 3: Close the adapter
    adapter.close()

For adapter-specific initialization details, see the individual adapter documentation pages.