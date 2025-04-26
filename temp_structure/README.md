# SQL Batcher

[![Python Version](https://img.shields.io/pypi/pyversions/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![PyPI Version](https://img.shields.io/pypi/v/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![License](https://img.shields.io/pypi/l/sql-batcher.svg)](https://github.com/yourusername/sql-batcher/blob/main/LICENSE)

SQL Batcher is a Python library for batching SQL statements to optimize database operations. It helps you manage large volumes of SQL statements by sending them to the database in optimized batches, improving performance and reducing server load.

## Features

- 🚀 **High Performance**: Optimize database operations by batching multiple SQL statements
- 🧩 **Modularity**: Easily swap between different database adapters (Trino, Snowflake, Spark, etc.)
- 🔍 **Transparency**: Dry run mode to inspect generated SQL without execution
- 📊 **Monitoring**: Collect and analyze batched queries
- 🔗 **Extensibility**: Create custom adapters for any database system
- 🛡️ **Type Safety**: Full type annotations for better IDE support

## Installation

```bash
pip install sql-batcher
```

### Optional Dependencies

Install with specific database adapters:

```bash
pip install "sql-batcher[trino]"     # For Trino support
pip install "sql-batcher[snowflake]"  # For Snowflake support
pip install "sql-batcher[spark]"      # For PySpark support
pip install "sql-batcher[all]"        # All adapters
```

## Quick Start

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters.generic import GenericAdapter
import sqlite3

# Create a connection
connection = sqlite3.connect(":memory:")
cursor = connection.cursor()

# Create an adapter
adapter = GenericAdapter(connection=connection)

# Create a table
adapter.execute("CREATE TABLE users (id INTEGER, name TEXT)")

# Generate many INSERT statements
statements = [f"INSERT INTO users VALUES ({i}, 'User {i}')" for i in range(1, 1001)]

# Create a batcher with a 100KB size limit
batcher = SQLBatcher(max_bytes=100_000)

# Process all statements
total_processed = batcher.process_statements(statements, adapter.execute)
print(f"Processed {total_processed} statements")

# Verify the data
results = adapter.execute("SELECT COUNT(*) FROM users")
print(f"Total users in database: {results[0][0]}")

# Close the connection
adapter.close()
```

## Advanced Usage

### Dry Run Mode

```python
from sql_batcher import SQLBatcher
from sql_batcher.query_collector import ListQueryCollector

# Create a query collector
collector = ListQueryCollector()

# Create a batcher in dry run mode
batcher = SQLBatcher(max_bytes=50_000, dry_run=True)

# Process statements without executing them
batcher.process_statements(
    statements, 
    lambda x: None,  # This won't be called in dry run mode
    query_collector=collector
)

# Get the collected queries
for query_info in collector.get_queries():
    print(f"Batch query: {query_info['query']}")
```

### Using Trino Adapter

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters.trino import TrinoAdapter

# Create a Trino adapter
adapter = TrinoAdapter(
    host="trino.example.com",
    port=443,
    user="admin",
    catalog="hive",
    schema="default"
)

# Create a batcher
batcher = SQLBatcher(max_bytes=1_000_000)

# Process statements
batcher.process_statements(statements, adapter.execute)

# Close the connection
adapter.close()
```

## Documentation

For complete documentation, visit [the docs site](https://github.com/yourusername/sql-batcher).

## Adapters

SQL Batcher comes with several built-in adapters:

- `GenericAdapter`: For generic database connections (SQLite, PostgreSQL, etc.)
- `TrinoAdapter`: For Trino/Presto databases
- `SnowflakeAdapter`: For Snowflake databases
- `SparkAdapter`: For PySpark SQL

You can also create custom adapters by extending the `SQLAdapter` base class.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.