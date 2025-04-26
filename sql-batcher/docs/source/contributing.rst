############
Contributing
############

Thank you for your interest in contributing to SQL Batcher! This guide will help you get started with the development process.

Setting Up for Development
========================

Clone the Repository
-----------------

First, clone the repository:

.. code-block:: bash

    git clone https://github.com/yourusername/sql-batcher.git
    cd sql-batcher

Install Development Dependencies
----------------------------

Install the package in development mode with all dependencies:

.. code-block:: bash

    pip install -e ".[dev,all]"

This installs SQL Batcher in editable mode, along with all development tools and database adapters.

Development Workflow
=================

Code Style
--------

SQL Batcher follows these style conventions:

- **PEP 8** for general Python style
- **Black** for automatic code formatting
- **isort** for import sorting
- **Type hints** for all function signatures

You can check your code with:

.. code-block:: bash

    # Format code
    black src/sql_batcher tests
    
    # Sort imports
    isort src/sql_batcher tests
    
    # Run static type checking
    mypy src/sql_batcher

Running Tests
-----------

SQL Batcher uses pytest for testing:

.. code-block:: bash

    # Run all tests
    pytest
    
    # Run with coverage report
    pytest --cov=sql_batcher
    
    # Run specific test file
    pytest tests/test_batcher.py

Building Documentation
-------------------

To build the documentation:

.. code-block:: bash

    cd docs
    make html

The documentation will be available in ``docs/build/html``.

Contribution Guidelines
=====================

Pull Request Process
-----------------

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-adapter`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Update documentation as needed
7. Submit a pull request

When creating a pull request, please:

- Use a clear, descriptive title
- Describe what the change does and why it's needed
- Include any relevant issue numbers (e.g., "Fixes #42")

Code Requirements
--------------

All new code should:

- Have appropriate test coverage
- Include type hints
- Follow the project's style conventions
- Be well-documented with docstrings
- Handle errors gracefully

Creating New Adapters
------------------

If you're creating a new database adapter:

1. Subclass ``SQLAdapter`` from ``sql_batcher.adapters.base``
2. Implement the required methods (``execute``, ``get_max_query_size``, ``close``)
3. Add transaction support if applicable
4. Write thorough tests for your adapter
5. Document the adapter with usage examples
6. Update the adapter index in ``docs/source/adapters/index.rst``

Example skeleton for a new adapter:

.. code-block:: python

    from sql_batcher.adapters.base import SQLAdapter
    
    class MyDatabaseAdapter(SQLAdapter):
        """
        Adapter for MyDatabase.
        
        Args:
            connection_params: Connection parameters for MyDatabase
            max_query_size: Maximum query size in bytes (default: 1,000,000)
        """
        
        def __init__(self, connection_params, max_query_size=1_000_000):
            self.connection_params = connection_params
            self.max_query_size = max_query_size
            self.connection = None
            self._connect()
        
        def _connect(self):
            """Establish a connection to the database."""
            # Implementation details
        
        def execute(self, sql):
            """Execute a SQL statement on MyDatabase."""
            # Implementation details
        
        def get_max_query_size(self):
            """Return the maximum query size for MyDatabase."""
            return self.max_query_size
        
        def close(self):
            """Close the connection to MyDatabase."""
            # Implementation details
        
        def begin_transaction(self):
            """Begin a transaction (if supported)."""
            # Implementation details
        
        def commit_transaction(self):
            """Commit the current transaction (if supported)."""
            # Implementation details
        
        def rollback_transaction(self):
            """Rollback the current transaction (if supported)."""
            # Implementation details

Reporting Issues
=============

If you find a bug or have a feature request, please report it by creating an issue. When reporting bugs, please include:

- A clear, descriptive title
- A detailed description of the issue
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Your environment (Python version, database system, etc.)
- Any relevant error messages or logs

Code of Conduct
============

Please be respectful and considerate of others when contributing. We strive to maintain a welcoming and inclusive environment for all contributors.