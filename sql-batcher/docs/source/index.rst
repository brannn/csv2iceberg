====================================
SQL Batcher Documentation
====================================

.. image:: https://badge.fury.io/py/sql-batcher.svg
    :target: https://badge.fury.io/py/sql-batcher
    :alt: PyPI version

.. image:: https://img.shields.io/pypi/pyversions/sql-batcher.svg
    :target: https://pypi.org/project/sql-batcher/
    :alt: Python Versions

.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
    :target: https://opensource.org/licenses/MIT
    :alt: License: MIT

Overview
========

SQL Batcher addresses a common challenge in database programming: efficiently executing many SQL statements while respecting query size limitations. It's especially valuable for systems like Trino, Spark SQL, and Snowflake that have query size or memory constraints.

Key features:

* **Size-based batching**: Group statements based on byte size, not just count
* **Flexible adapters**: Built-in support for Trino, Spark, Snowflake, and generic DBAPI
* **Dry run support**: Test your batching logic without executing actual queries
* **Query collection**: Examine what would be executed in dry run mode
* **Comprehensive logging**: Detailed insight into batching operations
* **Error resilience**: Robust error handling with detailed reporting

Getting Started
==============

Installation
-----------

Install SQL Batcher with pip:

.. code-block:: bash

    pip install sql-batcher

For specific database support, install the relevant driver:

.. code-block:: bash

    # For Trino support
    pip install sql-batcher[trino]

    # For Spark support
    pip install sql-batcher[spark]

    # For Snowflake support
    pip install sql-batcher[snowflake]

    # For all supported databases
    pip install sql-batcher[all]

Quick Start
----------

Basic usage:

.. code-block:: python

    from sql_batcher import SQLBatcher

    # Create a batcher with a 500KB size limit
    batcher = SQLBatcher(max_bytes=500_000)

    # Define statements to execute
    statements = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        # ... many more statements
    ]

    # Define an execution callback
    def execute_sql(sql):
        print(f"Executing: {sql}")
        # In a real scenario, this would execute the SQL using your DB connection

    # Process all statements with automatic batching
    batcher.process_statements(statements, execute_sql)

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   usage/basic
   usage/advanced
   usage/cli
   adapters/index
   api/modules
   examples
   development/contributing
   development/testing

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`