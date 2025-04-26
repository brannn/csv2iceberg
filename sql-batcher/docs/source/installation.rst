############
Installation
############

SQL Batcher can be installed in several ways, depending on your requirements and database systems.

Basic Installation
===============

To install SQL Batcher with the core functionality:

.. code-block:: bash

    pip install sql-batcher

This installs the core package without any database-specific dependencies, which is suitable if you're using the generic adapter with DBAPI-compliant database drivers that you've already installed.

Installation with Database-specific Dependencies
============================================

SQL Batcher supports specific database systems through optional dependencies. You can install these dependencies based on your needs:

Trino Support
-----------

To install SQL Batcher with Trino support:

.. code-block:: bash

    pip install sql-batcher[trino]

This installs the Trino Python client alongside SQL Batcher.

Spark Support
-----------

To install SQL Batcher with Spark support:

.. code-block:: bash

    pip install sql-batcher[spark]

Note that most PySpark environments already have PySpark installed, so this dependency might be redundant.

Snowflake Support
--------------

To install SQL Batcher with Snowflake support:

.. code-block:: bash

    pip install sql-batcher[snowflake]

This installs the Snowflake connector for Python.

All Database Adapters
------------------

To install SQL Batcher with support for all specialized database adapters:

.. code-block:: bash

    pip install sql-batcher[all]

This installs dependencies for Trino, Spark, and Snowflake.

Development Installation
=====================

For development purposes, you might want to install SQL Batcher with additional tools for testing, linting, and documentation:

.. code-block:: bash

    pip install sql-batcher[dev]

This includes development tools like pytest, black, flake8, and Sphinx.

Installing from Source
===================

You can also install SQL Batcher directly from the GitHub repository:

.. code-block:: bash

    git clone https://github.com/yourusername/sql-batcher.git
    cd sql-batcher
    pip install -e .

For development with all dependencies:

.. code-block:: bash

    pip install -e ".[dev,all]"

System Requirements
================

SQL Batcher has minimal system requirements:

- Python 3.7 or higher
- Database drivers for your specific database systems

Optional requirements based on adapter:

- **Trino**: Requires the Trino Python client (version 0.305.0 or higher)
- **Spark**: Requires PySpark (version 3.0.0 or higher)
- **Snowflake**: Requires the Snowflake connector for Python (version 2.4.0 or higher)

Verifying Installation
===================

After installation, you can verify that SQL Batcher is installed correctly by importing it in Python:

.. code-block:: python

    >>> from sql_batcher import SQLBatcher
    >>> print(SQLBatcher.__doc__)
    'Core class for batching SQL statements based on size limits.'

If you've installed database-specific adapters, you can verify them as well:

.. code-block:: python

    >>> from sql_batcher.adapters import GenericAdapter  # Always available
    >>> 
    >>> # If installed with [trino]
    >>> from sql_batcher.adapters import TrinoAdapter
    >>> 
    >>> # If installed with [spark]
    >>> from sql_batcher.adapters import SparkAdapter
    >>> 
    >>> # If installed with [snowflake]
    >>> from sql_batcher.adapters import SnowflakeAdapter