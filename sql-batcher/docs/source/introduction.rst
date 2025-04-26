############
Introduction
############

What is SQL Batcher?
=================

SQL Batcher is a Python library designed to solve a common problem in database programming: efficiently executing many SQL statements while respecting database query size limitations.

When working with databases like Trino, Spark SQL, or Snowflake, you often need to insert or update large amounts of data using multiple SQL statements. However, these database systems typically have limits on the size of queries they can process efficiently. Traditional batching approaches based on simple statement counts don't account for the varying sizes of SQL statements, which can lead to failed queries or suboptimal performance.

SQL Batcher addresses this challenge by batching statements based on their actual byte size rather than a fixed count, ensuring that each batch stays within the database's size constraints.

Key Features
==========

- **Size-based Batching**: Group SQL statements based on actual byte size, not just count
- **Modular Design**: Use with any database through the adapter interface
- **Specialized Adapters**: Optimized adapters for Trino, Spark SQL, and Snowflake
- **Generic DBAPI Support**: Works with any database that follows the Python Database API Specification
- **Transaction Support**: Optional transaction handling for compatible databases
- **Dry Run Mode**: Simulate execution without actually running SQL
- **Customizable**: Extend with custom adapters and size calculation functions

Why Use SQL Batcher?
=================

Database Size Limitations
----------------------

Many database systems have limits on the size of queries they can process:

- **Trino/Starburst** has a default HTTP header size limit (typically 16MB)
- **Spark SQL** has JVM memory constraints when processing large queries
- **Snowflake** has limits on the size of SQL statements it can process
- **Other databases** often have similar constraints

SQL Batcher helps you stay within these limits by batching statements based on their actual size.

Performance Optimization
---------------------

Even when databases don't have explicit size limits, large batches of SQL statements can:

- Use excessive memory
- Cause timeouts
- Lead to network bottlenecks
- Result in slower overall performance

SQL Batcher optimizes batching for best performance while avoiding these issues.

Code Simplicity
------------

Without SQL Batcher, you would need to implement complex batching logic yourself:

.. code-block:: python

    # Without SQL Batcher - complex manual batching
    statements = [...]  # Many SQL statements
    
    # Manual batching by size
    current_batch = []
    current_size = 0
    max_size = 1000000
    
    for statement in statements:
        statement_size = len(statement.encode('utf-8'))
        
        if current_size + statement_size > max_size:
            # Execute current batch
            execute_batch(current_batch)
            current_batch = []
            current_size = 0
        
        current_batch.append(statement)
        current_size += statement_size
    
    # Don't forget the last batch
    if current_batch:
        execute_batch(current_batch)

With SQL Batcher, this simplifies to:

.. code-block:: python

    # With SQL Batcher - clean, simple batching
    from sql_batcher import SQLBatcher
    
    statements = [...]  # Many SQL statements
    batcher = SQLBatcher(max_bytes=1000000)
    batcher.process_statements(statements, execute_batch)

Common Use Cases
=============

SQL Batcher is particularly useful for:

- **ETL Processes**: Efficiently loading large datasets into a database
- **Data Migration**: Moving data between different database systems
- **Bulk Updates**: Making many changes to a database at once
- **Log Processing**: Inserting many log entries into a database
- **Test Data Generation**: Creating large volumes of test data

Design Philosophy
==============

SQL Batcher was designed with the following principles in mind:

1. **Simplicity**: Provide a clean, simple interface for a common task
2. **Flexibility**: Work with a wide range of database systems
3. **Extensibility**: Allow for custom adapters and configurations
4. **Efficiency**: Optimize for performance and resource usage
5. **Reliability**: Handle edge cases like oversized statements gracefully

Next Steps
========

- See :doc:`installation` for installation instructions
- Check out the :doc:`getting_started` guide for basic usage
- Explore :doc:`examples` for practical use cases
- Dive into the :doc:`api/modules` for detailed API documentation