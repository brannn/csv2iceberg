#################
Custom Adapters
#################

This section explains how to create your own custom adapters for SQL Batcher to integrate with database systems that don't have built-in adapter support.

Adapter Interface
===============

All adapters in SQL Batcher implement the abstract ``SQLAdapter`` base class, which defines the following interface:

.. code-block:: python

    from abc import ABC, abstractmethod
    from typing import Any, Dict, List, Optional, Tuple, Union

    class SQLAdapter(ABC):
        """Abstract base class for SQL database adapters."""
        
        @abstractmethod
        def execute(self, sql: str) -> List[Tuple]:
            """Execute SQL statement and return results."""
            pass
        
        @abstractmethod
        def get_max_query_size(self) -> int:
            """Return maximum SQL query size in bytes."""
            pass
        
        @abstractmethod
        def close(self) -> None:
            """Close database connection."""
            pass
        
        def begin_transaction(self) -> None:
            """Begin a transaction (if supported)."""
            pass
        
        def commit_transaction(self) -> None:
            """Commit the current transaction (if supported)."""
            pass
        
        def rollback_transaction(self) -> None:
            """Rollback the current transaction (if supported)."""
            pass

Creating a Custom Adapter
=======================

To create a custom adapter, subclass ``SQLAdapter`` and implement at least the required methods:

1. ``execute(sql)`` - Execute a SQL statement and return results
2. ``get_max_query_size()`` - Return the maximum batch size in bytes
3. ``close()`` - Clean up resources

The transaction methods (``begin_transaction()``, ``commit_transaction()``, and ``rollback_transaction()``) are optional, but should be implemented if your database supports transactions.

Example Implementation
====================

Here's an example of a custom adapter for a hypothetical database:

.. code-block:: python

    from sql_batcher.adapters.base import SQLAdapter
    import my_database_library

    class MyCustomAdapter(SQLAdapter):
        """Adapter for MyDatabase system."""
        
        # Default query size limit for this database
        DEFAULT_MAX_QUERY_SIZE = 2_000_000
        
        def __init__(self, host, port, username, password, database, max_query_size=None):
            """Initialize the adapter with connection parameters."""
            self.connection_params = {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "database": database
            }
            self.max_query_size = max_query_size or self.DEFAULT_MAX_QUERY_SIZE
            self.connection = None
            self._connect()
        
        def _connect(self):
            """Establish a connection to the database."""
            self.connection = my_database_library.connect(**self.connection_params)
        
        def execute(self, sql):
            """Execute a SQL statement."""
            if not self.connection:
                self._connect()
            
            cursor = self.connection.cursor()
            cursor.execute(sql)
            
            # For SELECT statements, return results
            if sql.strip().upper().startswith("SELECT"):
                results = cursor.fetchall()
                return results
            
            # For non-SELECT statements, return empty list
            return []
        
        def get_max_query_size(self):
            """Return the maximum query size in bytes."""
            return self.max_query_size
        
        def close(self):
            """Close the database connection."""
            if self.connection:
                self.connection.close()
                self.connection = None
        
        def begin_transaction(self):
            """Begin a transaction."""
            if not self.connection:
                self._connect()
            self.connection.begin()
        
        def commit_transaction(self):
            """Commit the current transaction."""
            if self.connection:
                self.connection.commit()
        
        def rollback_transaction(self):
            """Rollback the current transaction."""
            if self.connection:
                self.connection.rollback()

Best Practices
=============

When implementing a custom adapter, consider these best practices:

1. **Connection Management**
   - Create connections lazily (only when needed)
   - Handle reconnection if the connection is lost
   - Properly close connections to avoid resource leaks

2. **Error Handling**
   - Provide clear error messages with database-specific details
   - Handle connection errors gracefully
   - Consider implementing retry logic for transient errors

3. **Type Handling**
   - Ensure proper conversion between database types and Python types
   - Handle NULL values appropriately

4. **Security**
   - Use parameter binding when your database supports it
   - Don't log sensitive information like passwords
   - Consider using connection pooling for better security

5. **Performance**
   - Set appropriate timeouts
   - Consider implementing batching at the adapter level if the database supports it
   - Optimize result fetching for large datasets

Registering Your Adapter
=======================

To make your adapter available through the normal import mechanism, you can create a package and register it in your project:

.. code-block:: python

    # In your package's __init__.py
    from sql_batcher.adapters.base import SQLAdapter
    from .my_custom_adapter import MyCustomAdapter
    
    __all__ = ["MyCustomAdapter"]