SQL Batcher Documentation
========================

SQL Batcher is a Python library for batching SQL statements to optimize database operations. It helps you manage large volumes of SQL statements by sending them to the database in optimized batches, improving performance and reducing server load.

.. image:: https://img.shields.io/pypi/pyversions/sql-batcher.svg
   :target: https://pypi.org/project/sql-batcher

.. image:: https://img.shields.io/pypi/v/sql-batcher.svg
   :target: https://pypi.org/project/sql-batcher

.. image:: https://img.shields.io/pypi/l/sql-batcher.svg
   :target: https://github.com/yourusername/sql-batcher/blob/main/LICENSE

Features
--------

- 🚀 **High Performance**: Optimize database operations by batching multiple SQL statements
- 🧩 **Modularity**: Easily swap between different database adapters (Trino, Snowflake, Spark, etc.)
- 🔍 **Transparency**: Dry run mode to inspect generated SQL without execution
- 📊 **Monitoring**: Collect and analyze batched queries
- 🔗 **Extensibility**: Create custom adapters for any database system
- 🛡️ **Type Safety**: Full type annotations for better IDE support

Installation
-----------

.. code-block:: bash

   pip install sql-batcher

Optional Dependencies
^^^^^^^^^^^^^^^^^^^^

Install with specific database adapters:

.. code-block:: bash

   pip install "sql-batcher[trino]"     # For Trino support
   pip install "sql-batcher[snowflake]"  # For Snowflake support
   pip install "sql-batcher[spark]"      # For PySpark support
   pip install "sql-batcher[all]"        # All adapters

Contents
-------

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   guides/installation
   guides/quickstart
   guides/configuration

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   guides/usage
   guides/adapters
   guides/dry_run
   guides/transactions
   guides/performance

.. toctree::
   :maxdepth: 2
   :caption: Examples

   examples/basic
   examples/trino
   examples/snowflake
   examples/spark
   examples/postgresql

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/batcher
   api/query_collector
   api/adapters

.. toctree::
   :maxdepth: 1
   :caption: Development

   guides/contributing
   guides/testing
   guides/releasing

Indices and tables
-----------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`