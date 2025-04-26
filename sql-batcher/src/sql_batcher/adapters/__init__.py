"""
SQL Batcher adapters for specific database engines.

This package provides adapters for connecting SQLBatcher with various database
engines and APIs.
"""

from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.adapters.generic import GenericAdapter
from sql_batcher.adapters.trino import TrinoAdapter

# These are lazily imported to avoid hard dependencies
try:
    from sql_batcher.adapters.spark import SparkAdapter
    has_spark = True
except ImportError:
    has_spark = False

try:
    from sql_batcher.adapters.snowflake import SnowflakeAdapter
    has_snowflake = True
except ImportError:
    has_snowflake = False

__all__ = ["SQLAdapter", "GenericAdapter", "TrinoAdapter"]

if has_spark:
    __all__.append("SparkAdapter")
    
if has_snowflake:
    __all__.append("SnowflakeAdapter")