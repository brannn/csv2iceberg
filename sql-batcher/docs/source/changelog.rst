############
Changelog
############

This page documents the changes in each version of SQL Batcher.

Version 0.1.0 (Initial Release)
=============================

Released: 2025-04-26

Initial release of SQL Batcher with the following features:

Core Features
-----------

- ``SQLBatcher`` class for batching SQL statements by size
- Size-based batching with configurable maximum size
- Dry run mode for simulating execution without running SQL
- Support for custom size calculation functions
- Proper handling of oversized statements

Adapters
-------

- ``SQLAdapter`` abstract base class for database adapters
- ``GenericAdapter`` for DBAPI 2.0 compliant database connections
- ``TrinoAdapter`` for Trino/Starburst databases
- ``SparkAdapter`` for Spark SQL via PySpark
- ``SnowflakeAdapter`` for Snowflake databases

Query Collection
--------------

- ``QueryCollector`` interface for collecting queries in dry run mode
- ``ListQueryCollector`` implementation for storing queries in a list

Documentation
-----------

- Comprehensive API documentation
- Detailed usage guides and examples
- Adapter-specific documentation