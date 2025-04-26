"""
Database adapters for SQL Batcher.

This package contains adapters for various database systems, allowing
SQLBatcher to work with different database backends.

Available adapters:
- GenericAdapter: Works with any DBAPI 2.0 compliant database connection
- TrinoAdapter: Optimized for Trino/Starburst databases
- SparkAdapter: For Spark SQL via PySpark
- SnowflakeAdapter: For Snowflake databases

Each adapter implements the SQLAdapter interface defined in adapters.base.
"""

# Core interfaces
from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.adapters.generic import GenericAdapter

# Optional adapters that are imported lazily
__all__ = ["SQLAdapter", "GenericAdapter"]

# Make specific database adapters available but import them lazily
# when actually used to avoid unnecessary dependencies
try:
    from sql_batcher.adapters.trino import TrinoAdapter
    __all__.append("TrinoAdapter")
except ImportError:
    pass

try:
    from sql_batcher.adapters.spark import SparkAdapter
    __all__.append("SparkAdapter")
except ImportError:
    pass

try:
    from sql_batcher.adapters.snowflake import SnowflakeAdapter
    __all__.append("SnowflakeAdapter")
except ImportError:
    pass