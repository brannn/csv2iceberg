# SQL Batcher

A Python library for efficiently batching SQL statements based on size limits, optimized for Trino, Spark, Snowflake, and other database engines.

[![PyPI version](https://badge.fury.io/py/sql-batcher.svg)](https://badge.fury.io/py/sql-batcher)
[![Python Versions](https://img.shields.io/pypi/pyversions/sql-batcher.svg)](https://pypi.org/project/sql-batcher/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

SQL Batcher addresses a common challenge in database programming: efficiently executing many SQL statements while respecting query size limitations. It's especially valuable for systems like Trino, Spark SQL, and Snowflake that have query size or memory constraints.

Key features:
- **Size-based batching**: Group statements based on byte size, not just count
- **Flexible adapters**: Built-in support for Trino, Spark, Snowflake, and generic DBAPI
- **Dry run support**: Test your batching logic without executing actual queries
- **Query collection**: Examine what would be executed in dry run mode
- **Comprehensive logging**: Detailed insight into batching operations
- **Error resilience**: Robust error handling with detailed reporting

## Installation

```bash
pip install sql-batcher
```

For specific database support, install the relevant driver:

```bash
# For Trino support
pip install sql-batcher[trino]

# For Spark support
pip install sql-batcher[spark]

# For Snowflake support
pip install sql-batcher[snowflake]

# For all supported databases
pip install sql-batcher[all]
```

## Quick Start

### Basic Usage

```python
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
```

### Using With Trino

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters.trino import TrinoAdapter

# Create a Trino adapter
adapter = TrinoAdapter(
    host="localhost",
    port=8080,
    user="admin",
    catalog="hive",
    schema="default"
)

# Create a batcher with Trino's recommended limits
batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())

# Define some statements
statements = [
    "INSERT INTO users VALUES (1, 'Alice')",
    "INSERT INTO users VALUES (2, 'Bob')"
]

# Process statements using the adapter
batcher.process_statements(statements, adapter.execute)
```

### Using With Spark

```python
from pyspark.sql import SparkSession
from sql_batcher import SQLBatcher
from sql_batcher.adapters.spark import SparkAdapter

# Initialize Spark
spark = SparkSession.builder.appName("SQLBatcherExample").getOrCreate()

# Create a Spark adapter
adapter = SparkAdapter(spark_session=spark)

# Create a batcher with appropriate limits
batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())

# Process statements
statements = [
    "INSERT INTO users VALUES (1, 'Alice')",
    "INSERT INTO users VALUES (2, 'Bob')"
]
batcher.process_statements(statements, adapter.execute)
```

## Documentation

For full documentation, examples, and API reference, see [the documentation site](https://sql-batcher.readthedocs.io).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.