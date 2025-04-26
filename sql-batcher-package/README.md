# SQL Batcher

SQL Batcher is a Python library designed to efficiently execute many SQL statements while respecting database query size limitations. It's especially useful for database systems like Trino, Spark SQL, and Snowflake that have limits on query string size.

## Key Features

- **Adaptive Batching**: Batch SQL statements based on actual byte size rather than simple counts
- **Database Agnostic**: Use with any database through the generic adapter
- **Specialized Adapters**: Optimized adapters for Trino, Spark SQL, and Snowflake
- **Modular Design**: Create custom adapters for other database systems
- **Dry Run Support**: Simulate execution without actually running SQL
- **Transaction Support**: Optional transaction handling for compatible databases

## Installation

Install from PyPI:

```bash
pip install sql-batcher
```

With specialized database support:

```bash
# For Trino support
pip install sql-batcher[trino]

# For Spark support
pip install sql-batcher[spark]

# For Snowflake support
pip install sql-batcher[snowflake]

# For all adapters
pip install sql-batcher[all]
```

## Quick Start

```python
from sql_batcher import SQLBatcher

# Create a batcher
batcher = SQLBatcher(max_bytes=1000000)

# Define SQL statements
statements = [
    "INSERT INTO users VALUES (1, 'Alice')",
    "INSERT INTO users VALUES (2, 'Bob')",
    # ... many more statements
]

# Define a function to execute SQL
def execute_sql(sql):
    print(f"Executing: {sql}")
    # In a real scenario, this would execute using your database connection

# Process all statements, automatically batching as needed
batcher.process_statements(statements, execute_sql)
```

With a database adapter:

```python
import sqlite3
from sql_batcher import SQLBatcher
from sql_batcher.adapters.generic import GenericAdapter

# Create a connection and adapter
connection = sqlite3.connect(":memory:")
adapter = GenericAdapter(connection=connection)

# Create a batcher
batcher = SQLBatcher(max_bytes=adapter.get_max_query_size())

# Use the adapter directly as the execution callback
batcher.process_statements(statements, adapter.execute)
```

## Documentation

For full documentation, visit [sql-batcher.readthedocs.io](https://sql-batcher.readthedocs.io/).

## License

This project is licensed under the MIT License - see the LICENSE file for details.