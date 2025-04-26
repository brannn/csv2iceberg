######################
Command-Line Interface
######################

SQL Batcher includes a command-line interface (CLI) that allows you to use its functionality without writing Python code.

Basic Commands
=============

After installing the package, you can use the ``sql-batcher`` command:

.. code-block:: bash

    # Show help
    sql-batcher --help
    
    # Show version
    sql-batcher version
    
    # List available adapters
    sql-batcher adapters
    
    # Process a SQL file
    sql-batcher process my_statements.sql --max-size 500000 --dry-run

Available Commands
=================

SQL Batcher supports the following commands:

``version``
-----------

Shows the installed version of SQL Batcher:

.. code-block:: bash

    sql-batcher version

``adapters``
-----------

Lists all available database adapters:

.. code-block:: bash

    # Basic list
    sql-batcher adapters
    
    # Detailed information
    sql-batcher adapters --verbose

``process``
-----------

Processes SQL statements from a file:

.. code-block:: bash

    sql-batcher process my_statements.sql

This command supports the following options:

* ``--max-size``: Maximum batch size in bytes (default: 1000000)
* ``--delimiter``: SQL statement delimiter (default: ";")
* ``--dry-run``: Don't execute, just simulate batching
* ``--output``: Output file for batches (when using --dry-run)
* ``--verbose``: Enable verbose logging

Example SQL File Format
======================

The input SQL file should contain one SQL statement per line or separated by delimiters:

.. code-block:: sql

    -- File: my_statements.sql
    INSERT INTO users VALUES (1, 'Alice');
    INSERT INTO users VALUES (2, 'Bob');
    INSERT INTO users VALUES (3, 'Charlie');
    -- Comments are ignored
    INSERT INTO users VALUES (4, 'Dave');

Output File Format
=================

When using ``--dry-run`` with ``--output``, the command generates a file with batched SQL statements:

.. code-block:: sql

    -- Batch 1
    INSERT INTO users VALUES (1, 'Alice');
    INSERT INTO users VALUES (2, 'Bob');
    
    -- Batch 2
    INSERT INTO users VALUES (3, 'Charlie');
    INSERT INTO users VALUES (4, 'Dave');

Environment Variables
====================

The CLI respects the following environment variables:

* ``SQL_BATCHER_MAX_SIZE``: Default maximum batch size in bytes
* ``SQL_BATCHER_DELIMITER``: Default SQL statement delimiter
* ``SQL_BATCHER_VERBOSE``: Set to "1" to enable verbose logging by default

Example CLI Workflows
====================

Testing Batch Sizes
------------------

.. code-block:: bash

    # Generate batches with a small batch size and save to output file
    sql-batcher process large_statements.sql --max-size 1000 --dry-run --output batched.sql
    
    # Check how many batches were created
    grep -c "-- Batch" batched.sql

Processing Multiple Files
------------------------

.. code-block:: bash

    # Process multiple SQL files in sequence
    for file in *.sql; do
        echo "Processing $file..."
        sql-batcher process "$file" --verbose
    done