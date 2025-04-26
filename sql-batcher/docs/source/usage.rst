################
Detailed Usage
################

This guide provides detailed information on using SQL Batcher effectively in various scenarios.

Core Concepts
===========

The SQL Batcher library revolves around a few key concepts:

1. **SQLBatcher**: The core class that handles batching statements based on size
2. **SQL Adapters**: Classes that connect SQLBatcher to specific database systems
3. **Query Collectors**: Optional components for collecting queries during dry runs

Understanding these components and how they interact will help you use SQL Batcher effectively.

SQLBatcher Class
=============

The ``SQLBatcher`` class is the primary component you'll interact with. It takes care of:

- Calculating the size of SQL statements
- Grouping statements into appropriately sized batches
- Executing batches via a callback function
- Handling oversized statements

Core Methods
---------

.. code-block:: python

    from sql_batcher import SQLBatcher
    
    # Create a batcher
    batcher = SQLBatcher(max_bytes=1_000_000)
    
    # Add a single statement
    needs_flush = batcher.add_statement("INSERT INTO users VALUES (1, 'Alice')")
    if needs_flush:
        # The batch is full and needs to be flushed
        batcher.flush(execute_function)
    
    # Process multiple statements at once
    batcher.process_statements(statements, execute_function)
    
    # Manually reset the batch
    batcher.reset()
    
    # Manually flush the current batch
    statements_executed = batcher.flush(execute_function)

Configuration Options
------------------

The ``SQLBatcher`` constructor accepts several parameters:

- **max_bytes**: Maximum batch size in bytes (default: 1,000,000)
- **delimiter**: SQL statement delimiter (default: ";")
- **dry_run**: Whether to simulate execution without running SQL (default: False)
- **size_func**: Custom function to calculate statement size (default: None)

Examples:

.. code-block:: python

    # Default configuration
    batcher1 = SQLBatcher()  # 1MB limit, ";" delimiter
    
    # Custom batch size
    batcher2 = SQLBatcher(max_bytes=5_000_000)  # 5MB limit
    
    # Custom delimiter
    batcher3 = SQLBatcher(delimiter="|")  # Use | as the delimiter
    
    # Dry run mode
    batcher4 = SQLBatcher(dry_run=True)  # Don't actually execute SQL
    
    # Custom size function
    def my_size_function(sql):
        # Custom logic to calculate statement size
        return len(sql.encode('utf-8'))
    
    batcher5 = SQLBatcher(size_func=my_size_function)

Understanding Batch Flushing
=========================

The term "flushing" refers to the process of executing the current batch of SQL statements and then clearing the batch. Flushing can happen:

1. **Automatically**: When ``process_statements()`` detects that adding the next statement would exceed ``max_bytes``
2. **Manually**: When you explicitly call ``flush()``
3. **Implicitly**: At the end of ``process_statements()`` to execute any remaining statements

Here's what happens during a flush:

1. SQL statements in the current batch are joined using the delimiter
2. The joined SQL is passed to the execution callback function
3. The batch is cleared (reset)
4. The number of statements executed is returned

Working with SQL Adapters
======================

SQL Adapters implement the database-specific logic for executing SQL statements and managing connections.

Generic Adapter
------------

The ``GenericAdapter`` works with any DBAPI-compliant database connection:

.. code-block:: python

    import sqlite3
    from sql_batcher.adapters.generic import GenericAdapter
    
    # Create a database connection
    connection = sqlite3.connect(":memory:")
    
    # Create the adapter
    adapter = GenericAdapter(
        connection=connection,
        max_query_size=1_000_000,
        fetch_results=True
    )
    
    # Execute SQL
    adapter.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    
    # Process statements through a batcher
    batcher.process_statements(statements, adapter.execute)
    
    # Query data
    results = adapter.execute("SELECT * FROM test")
    
    # Clean up
    adapter.close()

Specialized Adapters
-----------------

SQL Batcher includes specialized adapters for specific database systems:

.. code-block:: python

    # Trino Adapter
    from sql_batcher.adapters.trino import TrinoAdapter
    
    trino_adapter = TrinoAdapter(
        host="trino.example.com",
        port=443,
        user="admin",
        catalog="hive",
        schema="default"
    )
    
    # Spark Adapter
    from pyspark.sql import SparkSession
    from sql_batcher.adapters.spark import SparkAdapter
    
    spark = SparkSession.builder.appName("Example").getOrCreate()
    spark_adapter = SparkAdapter(spark_session=spark)
    
    # Snowflake Adapter
    from sql_batcher.adapters.snowflake import SnowflakeAdapter
    
    snowflake_adapter = SnowflakeAdapter(
        connection_params={
            "account": "your_account",
            "user": "your_username",
            "password": "your_password",
            "database": "your_database"
        }
    )

Each adapter is designed to handle the specific requirements and limitations of its target database system.

Adapter Interface
-------------

All adapters implement the ``SQLAdapter`` interface, which defines these key methods:

.. code-block:: python

    # Execute SQL
    results = adapter.execute("SELECT * FROM test")
    
    # Get maximum query size
    max_size = adapter.get_max_query_size()
    
    # Close connection
    adapter.close()
    
    # Transaction support (if available)
    adapter.begin_transaction()
    adapter.commit_transaction()
    adapter.rollback_transaction()

Working with Transactions
======================

Many database systems support transactions, which allow you to ensure that a group of statements either all succeed or all fail together.

Basic Transaction Usage
--------------------

.. code-block:: python

    # Begin a transaction
    adapter.begin_transaction()
    
    try:
        # Execute statements within the transaction
        batcher.process_statements(statements, adapter.execute)
        
        # Commit the transaction if everything succeeded
        adapter.commit_transaction()
        
    except Exception as e:
        # Rollback the transaction if anything failed
        adapter.rollback_transaction()
        raise e
    
    finally:
        # Clean up
        adapter.close()

Transaction Support by Adapter
---------------------------

- **GenericAdapter**: Supports transactions if the underlying connection does
- **TrinoAdapter**: Limited transaction support (depends on Trino version)
- **SparkAdapter**: Limited transaction support (depends on Spark configuration)
- **SnowflakeAdapter**: Full transaction support

Dry Run Mode
==========

Dry run mode allows you to see what would be executed without actually running the SQL. This is useful for:

- Debugging
- Testing
- Generating SQL scripts
- Validating batching logic

Basic Dry Run
----------

.. code-block:: python

    from sql_batcher import SQLBatcher
    
    # Create a batcher in dry run mode
    batcher = SQLBatcher(max_bytes=1_000_000, dry_run=True)
    
    # This won't actually execute anything
    batcher.process_statements(statements, execute_function)

Using Query Collectors
-------------------

Query collectors store the SQL that would have been executed:

.. code-block:: python

    from sql_batcher import SQLBatcher
    from sql_batcher.query_collector import ListQueryCollector
    
    # Create a collector
    collector = ListQueryCollector()
    
    # Create a batcher in dry run mode
    batcher = SQLBatcher(max_bytes=1_000_000, dry_run=True)
    
    # Process statements and collect the SQL
    batcher.process_statements(
        statements,
        lambda x: None,  # This won't be called
        query_collector=collector
    )
    
    # Access the collected queries
    for query_info in collector.get_queries():
        sql = query_info["query"]
        metadata = query_info["metadata"]
        print(f"SQL: {sql}")
        if metadata:
            print(f"Metadata: {metadata}")

Custom Query Collectors
--------------------

You can create custom query collectors by implementing the ``QueryCollector`` interface:

.. code-block:: python

    from sql_batcher.query_collector import QueryCollector
    
    class FileQueryCollector(QueryCollector):
        def __init__(self, filename):
            self.filename = filename
            self.queries = []
            
            # Create/clear the file
            with open(self.filename, 'w') as f:
                f.write("-- SQL Batcher Dry Run Output\n\n")
        
        def add_query(self, query, metadata=None):
            # Write query to file
            with open(self.filename, 'a') as f:
                f.write(f"-- Batch {len(self.queries) + 1}\n")
                if metadata:
                    f.write(f"-- Metadata: {metadata}\n")
                f.write(f"{query}\n\n")
            
            # Also store in memory
            self.queries.append({"query": query, "metadata": metadata})
        
        def get_queries(self):
            return self.queries

Using with Oversized Statements
============================

SQL Batcher handles statements that exceed the maximum batch size by:

1. Logging a warning
2. Executing the oversized statement individually
3. Continuing with the remaining statements

This ensures that oversized statements don't cause the process to fail:

.. code-block:: python

    import logging
    from sql_batcher import SQLBatcher
    
    # Set up logging to see warnings
    logging.basicConfig(level=logging.WARNING)
    
    # Create a batcher with a small size limit
    batcher = SQLBatcher(max_bytes=100)
    
    # Define statements, including one that exceeds the limit
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",  # Small
        "INSERT INTO users VALUES (2, 'Bob')",    # Small
        "INSERT INTO users VALUES (3, 'Charlie') /* " + ("X" * 200) + " */"  # Oversized
    ]
    
    # Process statements - the oversized one will be handled appropriately
    batcher.process_statements(statements, execute_function)

Performance Considerations
=======================

To get the best performance from SQL Batcher, consider these tips:

Batch Size Optimization
--------------------

Choosing the right batch size involves balancing:

- **Database limits**: Stay under database query size limits
- **Memory usage**: Larger batches use more memory
- **Network overhead**: More batches mean more network round-trips
- **Database performance**: Some databases process large batches more efficiently

A good starting point is 1-5MB for most systems, but you may need to adjust based on your specific database and workload.

Statement Preparation
------------------

Prepare your statements efficiently:

- **Generate statements in batches**: Avoid generating all statements at once if you have millions
- **Use consistent formatting**: Consistent statement sizes make batching more predictable
- **Consider statement compression**: Remove unnecessary whitespace to reduce size

Adapter Selection
--------------

Use the most appropriate adapter for your database:

- **Specialized adapters** are optimized for specific database systems
- **Generic adapter** works with any DBAPI-compliant database
- **Custom adapters** can provide even better performance for specific use cases

Error Handling
-----------

Implement appropriate error handling:

.. code-block:: python

    try:
        batcher.process_statements(statements, execute_function)
    except Exception as e:
        print(f"Error processing statements: {e}")
        # Implement retry logic or fallback as needed