#############
Spark Adapter
#############

The Spark Adapter provides integration with Apache Spark SQL for executing batched SQL statements within a Spark environment.

Features
========

- Works with PySpark sessions
- Supports all Spark SQL statements
- Efficient handling of large SQL batches
- DataFrame result conversion

Installation
===========

The Spark Adapter requires the ``pyspark`` Python package:

.. code-block:: bash

    pip install sql-batcher[spark]

Or install the dependency directly:

.. code-block:: bash

    pip install pyspark>=3.0

Basic Usage
==========

.. code-block:: python

    from pyspark.sql import SparkSession
    from sql_batcher import SQLBatcher
    from sql_batcher.adapters.spark import SparkAdapter
    
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("SQLBatcherExample") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create a Spark adapter
    adapter = SparkAdapter(spark_session=spark)
    
    # Create a batcher with Spark's recommended size limits
    batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())
    
    # Create a test table
    adapter.execute("""
        CREATE TABLE IF NOT EXISTS example_table (
            id INT,
            name STRING
        ) USING PARQUET
    """)
    
    # Define SQL statements
    statements = [
        "INSERT INTO example_table VALUES (1, 'First')",
        "INSERT INTO example_table VALUES (2, 'Second')",
        # ... more statements
    ]
    
    # Process statements
    batcher.process_statements(statements, adapter.execute)
    
    # Query the data as a list of tuples
    results = adapter.execute("SELECT * FROM example_table")
    for row in results:
        print(row)
    
    # Clean up
    spark.stop()

Working with DataFrames
======================

The Spark Adapter can return results as DataFrames instead of lists of tuples:

.. code-block:: python

    # Create adapter with DataFrame result mode
    adapter = SparkAdapter(
        spark_session=spark, 
        return_dataframe=True
    )
    
    # Now execute returns a DataFrame
    df = adapter.execute("SELECT * FROM example_table")
    
    # Use DataFrame operations
    df.show()
    df.printSchema()
    
    # Transform the data
    filtered_df = df.filter(df.id > 1)
    filtered_df.show()

Configuration Options
====================

Here's how to configure the Spark adapter with various options:

.. code-block:: python

    from pyspark.sql import SparkSession
    from sql_batcher.adapters.spark import SparkAdapter
    
    # Create a configured Spark session
    spark = SparkSession.builder \
        .appName("SQLBatcherConfig") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create a configured adapter
    adapter = SparkAdapter(
        spark_session=spark,
        return_dataframe=True,
        max_query_size=2_000_000,
        fetch_limit=1000
    )

Parameters
=========

The ``SparkAdapter`` constructor accepts the following parameters:

- ``spark_session``: A PySpark SparkSession instance (required)
- ``return_dataframe``: If True, return PySpark DataFrames rather than lists of tuples (default: False)
- ``max_query_size``: Maximum query size in bytes (default: 1,000,000)
- ``fetch_limit``: Maximum number of rows to fetch for SELECT queries (default: None, meaning all rows)

Size Limits in Spark
==================

Unlike some databases, Spark SQL doesn't have a hard limit on SQL query size. However, very large queries can cause memory issues. The ``SparkAdapter`` uses a default limit of 1MB, but this can be adjusted based on your Spark cluster's capabilities.

Error Handling
=============

The ``SparkAdapter`` provides Spark-specific error information:

.. code-block:: python

    try:
        adapter.execute("SELECT * FROM nonexistent_table")
    except Exception as e:
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        
        # PySpark exceptions typically include:
        # - Error analysis information
        # - Stack traces with executor information
        # - Location of the error in the SQL statement