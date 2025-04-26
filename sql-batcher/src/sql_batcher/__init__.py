"""
SQL Batcher - A tool for efficiently batching SQL statements

This package provides a reusable SQLBatcher class for optimizing SQL statement execution
while respecting query size limits. It's particularly useful for systems like Trino,
Spark SQL, and Snowflake that have query size limitations.
"""

from sql_batcher.batcher import SQLBatcher
from sql_batcher.query_collector import QueryCollector

__version__ = "0.1.0"
__all__ = ["SQLBatcher", "QueryCollector"]