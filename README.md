# SQL Batcher

[![Python Version](https://img.shields.io/pypi/pyversions/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![PyPI Version](https://img.shields.io/pypi/v/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![License](https://img.shields.io/pypi/l/sql-batcher.svg)](https://github.com/sql-batcher/sql-batcher/blob/main/LICENSE)

SQL Batcher is a Python library that helps manage large volumes of SQL statements when working with databases that have query size and memory constraints. It groups statements into appropriately sized batches and offers database-specific adapters to improve performance across different database systems.

## Why Use SQL Batcher?

**For Data and Analytics Engineers:**
- **Overcome Database Limitations:** Automatically manages query size constraints in systems like Trino (~1MB), BigQuery (1MB interactive/20MB batch), and Snowflake (1-8MB depending on edition)
- **10x Performance Improvements:** Dramatically reduces execution time for large-scale operations by finding the optimal balance between batch size and round-trip overhead
- **Smart Resource Management:** Prevents memory overflow errors and server rejections by sizing batches based on table structure (columns)

**For Application Development:**
- **Simplified Architecture:** Eliminates the need to build custom batching logic for each database system in your stack
- **Production Reliability:** Handles transaction management, connection pooling, and error recovery through database-specific adapters
- **Transparent Operations:** Includes dry-run capability and monitoring tools to inspect how statements are batched before execution

**For Database-Agnostic Code:**
- **Uniform Interface:** Maintain a single codebase while supporting diverse database systems through consistent adapter interfaces
- **Future-Proof Design:** Add support for new databases by implementing simple adapters without changing application logic
- **Performance Portability:** Code optimized for one database system will automatically adapt to the performance characteristics of others

## Features

- 🚀 **High Performance**: Optimize database operations by batching multiple SQL statements
- 🧩 **Modularity**: Easily swap between different database adapters (Trino, Snowflake, Spark, etc.)
- 📏 **Smart Sizing**: Automatic batch size adjustment based on column count 
- 🔍 **Transparency**: Dry run mode to inspect generated SQL without execution
- 📊 **Monitoring**: Collect and analyze batched queries
- 🔗 **Extensibility**: Create custom adapters for any database system
- 🛡️ **Type Safety**: Full type annotations for better IDE support

## Which Databases Benefit from SQL Batcher?

SQL Batcher is especially valuable for database systems with query size limitations:

| Database/Engine | Query Size Limitations | Benefits from SQL Batcher |
|-----------------|------------------------|---------------------------|
| **Trino/Presto** ![Adapter](https://img.shields.io/badge/Dedicated%20Adapter-5cb85c) | ~1MB query size limit | Essential for bulk operations |
| **Snowflake** ![Adapter](https://img.shields.io/badge/Dedicated%20Adapter-5cb85c) | 1MB-8MB statement size limits depending on edition | Significant for large data sets |
| **BigQuery** ![Adapter](https://img.shields.io/badge/Dedicated%20Adapter-5cb85c) | 1MB for interactive, 20MB for batch | Critical for complex operations |
| **Redshift** | 16MB maximum query size | Important for ETL processes |
| **MySQL/MariaDB** | 4MB default `max_allowed_packet` | Important for large INSERT operations |
| **PostgreSQL** ![Adapter](https://img.shields.io/badge/Dedicated%20Adapter-5cb85c) | 1GB limit, but practical performance issues with large queries | Helpful for bulk operations |
| **Hive** | Configuration-dependent | Essential for data warehouse operations |
| **Oracle** | ~4GB theoretical, much lower in practice | Useful for enterprise applications |
| **SQL Server** | 2GB batch size, 4MB network packet size | Important for large-scale operations |
| **DB2** | 2MB statement size by default | Significant for bulk processing |
| **Spark SQL** ![Adapter](https://img.shields.io/badge/Dedicated%20Adapter-5cb85c) | Depends on driver memory | Important for big data processing |

## Installation

```bash
pip install sql-batcher
```

### Optional Dependencies

Install with specific database adapters:

```bash
pip install "sql-batcher[postgresql]" # For PostgreSQL optimized adapter
pip install "sql-batcher[trino]"      # For Trino support
pip install "sql-batcher[snowflake]"  # For Snowflake support
pip install "sql-batcher[spark]"      # For PySpark support
pip install "sql-batcher[bigquery]"   # For Google BigQuery support
pip install "sql-batcher[all]"        # All adapters
```

## Quick Start

### Minimal Example

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters.generic import GenericAdapter
import sqlite3  # Any database driver

# Create a simple in-memory SQLite database
conn = sqlite3.connect(":memory:")
cursor = conn.cursor()
cursor.execute("CREATE TABLE users (id INTEGER, name TEXT)")

# Create a generic adapter that can work with any database
adapter = GenericAdapter(
    connection=conn,
    execute_func=lambda sql: cursor.execute(sql),
    close_func=lambda: conn.close()
)

# Create a batcher with default settings
batcher = SQLBatcher(max_bytes=100_000)

# Generate some simple INSERT statements
statements = [
    f"INSERT INTO users VALUES ({i}, 'User {i}')"
    for i in range(1, 1001)
]

# Process all statements in optimized batches
total_processed = batcher.process_statements(statements, adapter.execute)
print(f"Processed {total_processed} INSERT statements")

# Verify the results
cursor.execute("SELECT COUNT(*) FROM users")
print(f"Total users: {cursor.fetchone()[0]}")

# Clean up
adapter.close()
```

### Comprehensive PostgreSQL Example

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters.postgresql import PostgreSQLAdapter
import psycopg2  # PostgreSQL adapter

# Connection parameters for PostgreSQL
connection_params = {
    "host": "localhost",
    "database": "mydb",
    "user": "postgres",
    "password": "password"
}

# Create a PostgreSQL-specific adapter with optimized features
adapter = PostgreSQLAdapter(
    connection_params=connection_params, 
    isolation_level="read_committed",
    cursor_factory=psycopg2.extras.RealDictCursor,  # Return results as dictionaries
    application_name="sql-batcher-demo"  # Helps identify the connection in pg_stat_activity
)

# Create a table with appropriate PostgreSQL data types
adapter.execute("""
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# Create an index to optimize queries
adapter.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)")

print("Preparing batch of INSERT statements...")

# Generate many INSERT statements with more complex data
statements = []
for i in range(1, 1001):
    email = f"user{i}@example.com"
    name = f"User {i}"
    metadata = f'{{"source": "batch_insert", "group": "{i % 10}", "priority": {i % 3}}}'
    
    statements.append(
        f"INSERT INTO users (id, name, email, metadata) VALUES "
        f"({i}, '{name}', '{email}', '{metadata}'::jsonb)"
    )

print(f"Generated {len(statements)} INSERT statements")

# Create a batcher with dynamic column-based batch sizing
# When tables have many columns, smaller batches will be used
# When tables have few columns, larger batches will be used
batcher = SQLBatcher(
    max_bytes=500_000,             # Base maximum batch size
    auto_adjust_for_columns=True,  # Enable dynamic column-based adjustment
    reference_column_count=5,      # Reference point for column count (baseline)
    min_adjustment_factor=0.2,     # Don't go below 20% of max_bytes
    max_adjustment_factor=3.0      # Don't go above 300% of max_bytes
)

# Begin a transaction for atomicity
adapter.begin_transaction()

try:
    # Process all statements
    total_processed = batcher.process_statements(statements, adapter.execute)
    print(f"Processed {total_processed} statements in batches")
    
    # Commit the transaction
    adapter.commit_transaction()
    print("Transaction committed successfully")
except Exception as e:
    # Rollback on error
    adapter.rollback_transaction()
    print(f"Error: {e}")
    raise

# Verify the data with a query that leverages PostgreSQL features
results = adapter.execute("""
SELECT 
    metadata->>'group' AS user_group, 
    COUNT(*) as user_count,
    MIN(created_at) as first_created,
    MAX(created_at) as last_created
FROM users 
GROUP BY metadata->>'group'
ORDER BY user_group
""")

print("\nUser Groups Summary:")
for row in results:
    if isinstance(row, dict):  # When using RealDictCursor
        print(f"  Group {row['user_group']}: {row['user_count']} users")
    else:  # Default tuple result
        print(f"  Group {row[0]}: {row[1]} users")

# Demonstrate PostgreSQL-specific batch operations
print("\nDemonstrating optimized COPY operations for bulk data:")

# Prepare some additional users to insert with COPY
additional_users = []
for i in range(1001, 1501):
    additional_users.append((
        i,
        f"Bulk User {i}",
        f"bulk_user{i}@example.com",
        f'{{"source": "copy_insert", "group": "{i % 5}", "priority": {i % 2}}}'
    ))

# Use PostgreSQL's COPY command for maximum insert performance  
copied_count = adapter.use_copy_for_bulk_insert(
    table_name="users",
    column_names=["id", "name", "email", "metadata"],
    data=additional_users
)
print(f"Added {copied_count} more users via COPY bulk insert")

# Verify total count
total_results = adapter.execute("SELECT COUNT(*) FROM users")
print(f"Total users in database: {total_results[0][0] if isinstance(total_results[0], tuple) else total_results[0]['count']}")

# Run EXPLAIN ANALYZE to check query performance
explain_results = adapter.explain_analyze(
    "SELECT * FROM users WHERE metadata->>'priority' = '0'"
)
print("\nQuery Execution Plan Preview:")
print(f"  {explain_results[0][0]}")
print("  ...")

# Get PostgreSQL server version
version = adapter.get_server_version()
print(f"\nPostgreSQL Server Version: {version[0]}.{version[1]}.{version[2]}")

# Close the connection
adapter.close()
```

## Advanced Usage

### How SQL Batcher Works

SQL Batcher uses a client-side batching approach to optimize database operations:

1. **Statement Collection**: You provide a list of SQL statements (typically INSERT statements).
2. **Size Analysis**: SQL Batcher analyzes each statement's size in bytes.
3. **Optimal Batching**: Statements are combined into batches up to the configured `max_bytes` limit.
4. **Database-Specific Execution**:
   - For PostgreSQL: Semicolon-separated statements can be executed in a single query
   - For Trino/Presto: Each statement is executed individually within the same adapter
   - For Snowflake: Statements are grouped within transaction blocks
   - For BigQuery: Statements use either interactive or batch mode depending on size

SQL Batcher doesn't attempt to modify the database's underlying query execution mechanism. Instead, it optimizes how multiple statements are grouped and submitted to the database, working within each system's constraints and capabilities.

### Dynamic Column-Based Batch Sizing

SQL Batcher now includes dynamic batch sizing based on the number of columns in your INSERT statements. This feature automatically adjusts the batch size to optimize performance based on table width.

```python
from sql_batcher import SQLBatcher

# Create a batcher with dynamic column-based batch sizing
batcher = SQLBatcher(
    max_bytes=1_000_000,           # Base maximum batch size
    auto_adjust_for_columns=True,  # Enable column-based adjustment (default: True)
    reference_column_count=5,      # Baseline column count (default: 5)
    min_adjustment_factor=0.2,     # Minimum adjustment multiplier (default: 0.2)
    max_adjustment_factor=5.0      # Maximum adjustment multiplier (default: 5.0)
)

# How it works:
# - The batcher automatically detects the number of columns in your INSERT statements
# - For wide tables (many columns), it reduces the batch size to prevent oversized batches
# - For narrow tables (few columns), it increases the batch size for better throughput
# - Adjustment is proportional: tables with 2x more columns than reference get ~0.5x batch size

# Example adjustment scenarios:
# - 15-column table (with reference=5): uses ~0.33x max_bytes (smaller batches)
# - 2-column table (with reference=5): uses ~2.5x max_bytes (larger batches)
```

This feature is especially valuable when working with tables of varying widths, as it balances the number of statements per batch based on the actual column count.

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
import pandas as pd

# Create a Trino adapter with SSL and additional authentication options
adapter = TrinoAdapter(
    host="trino.example.com",
    port=443,
    user="admin",
    catalog="hive",
    schema="default",
    # Optional parameters for enhanced connectivity
    use_ssl=True,
    verify_ssl=True,
    session_properties={"query_max_run_time": "2h", "distributed_join": "true"},
    http_headers={"X-Trino-Role": "system=admin"}
)

# Create a table for our data
create_table_sql = """
CREATE TABLE IF NOT EXISTS customer_metrics (
    customer_id VARCHAR,
    metric_date DATE,
    revenue DECIMAL(12,2),
    transactions INTEGER,
    region VARCHAR
)
"""
adapter.execute(create_table_sql)

# Read data from a Pandas DataFrame (example scenario)
df = pd.DataFrame({
    'customer_id': ['C001', 'C002', 'C003', 'C004', 'C005'],
    'metric_date': ['2025-01-15', '2025-01-15', '2025-01-16', '2025-01-16', '2025-01-17'],
    'revenue': [125.50, 89.99, 45.25, 210.75, 55.50],
    'transactions': [3, 2, 1, 4, 1],
    'region': ['North', 'South', 'East', 'North', 'West']
})

# Generate INSERT statements from the DataFrame
statements = []
for _, row in df.iterrows():
    # Format each value appropriately based on its type
    customer_id = f"'{row['customer_id']}'"
    metric_date = f"DATE '{row['metric_date']}'"
    revenue = str(row['revenue'])
    transactions = str(row['transactions'])
    region = f"'{row['region']}'"
    
    # Create INSERT statement
    insert_sql = f"INSERT INTO customer_metrics VALUES ({customer_id}, {metric_date}, {revenue}, {transactions}, {region})"
    statements.append(insert_sql)

# Create a batcher with a 900KB size limit (safe for Trino's ~1MB query limit)
batcher = SQLBatcher(max_bytes=900_000)

# Process all INSERT statements in optimized batches
total_processed = batcher.process_statements(statements, adapter.execute)
print(f"Processed {total_processed} INSERT statements")

# Verify the data with a simple query
results = adapter.execute("SELECT region, SUM(revenue) as total_revenue FROM customer_metrics GROUP BY region ORDER BY total_revenue DESC")
print("\nRevenue by Region:")
for row in results:
    print(f"  {row[0]}: ${row[1]:.2f}")

# Close the connection
adapter.close()
```

### Multi-statement Trino Example with Transactions

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters.trino import TrinoAdapter
from sql_batcher.query_collector import ListQueryCollector

# Create a Trino adapter
adapter = TrinoAdapter(
    host="trino.example.com",
    port=443,
    user="admin",
    catalog="hive",
    schema="default"
)

# Create tables for a data warehouse ETL scenario
setup_statements = [
    """CREATE TABLE IF NOT EXISTS staging_sales (
        sale_id VARCHAR,
        product_id VARCHAR,
        sale_date DATE,
        quantity INTEGER,
        unit_price DECIMAL(10,2),
        customer_id VARCHAR
    )""",
    """CREATE TABLE IF NOT EXISTS sales_facts (
        sale_id VARCHAR,
        product_id VARCHAR,
        date_key INTEGER,
        quantity INTEGER,
        revenue DECIMAL(12,2),
        customer_id VARCHAR
    )""",
    "TRUNCATE TABLE staging_sales"
]

# Execute setup statements
for statement in setup_statements:
    adapter.execute(statement)

# Generate INSERT statements for staging data
staging_inserts = []
for i in range(1, 1001):
    sale_id = f"S{i:05d}"
    product_id = f"P{(i % 100) + 1:03d}"
    date_str = f"2025-{((i % 12) + 1):02d}-{((i % 28) + 1):02d}"
    quantity = (i % 10) + 1
    unit_price = (i % 50) + 10.99
    customer_id = f"C{(i % 200) + 1:04d}"
    
    staging_inserts.append(
        f"INSERT INTO staging_sales VALUES ('{sale_id}', '{product_id}', DATE '{date_str}', {quantity}, {unit_price}, '{customer_id}')"
    )

# Create a query collector to analyze the batches
collector = ListQueryCollector()

# Create a batcher with a 900KB limit
batcher = SQLBatcher(max_bytes=900_000)

# Process all staging inserts
print("Loading staging data...")
total_staged = batcher.process_statements(
    staging_inserts, 
    adapter.execute,
    query_collector=collector
)
print(f"Loaded {total_staged} rows into staging table")

# Get batch statistics
batches = collector.get_queries()
print(f"Required {len(batches)} batches to process all statements")
avg_batch_size = sum(len(b['query'].encode('utf-8')) for b in batches) / len(batches)
print(f"Average batch size: {avg_batch_size / 1024:.2f} KB")

# Now transform the data into the fact table with a single statement
transform_sql = """
INSERT INTO sales_facts
SELECT 
    sale_id,
    product_id,
    (YEAR(sale_date) * 10000) + (MONTH(sale_date) * 100) + DAY(sale_date) as date_key,
    quantity,
    quantity * unit_price as revenue,
    customer_id
FROM staging_sales
"""

print("\nTransforming data to fact table...")
adapter.execute(transform_sql)

# Verify the results
count_result = adapter.execute("SELECT COUNT(*) FROM sales_facts")
print(f"Fact table now contains {count_result[0][0]} rows")

# Close the connection
adapter.close()
```

### Using BigQuery Adapter

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters.bigquery import BigQueryAdapter
import datetime
import os

# Create a BigQuery adapter with authentication options
# Note: This example uses Application Default Credentials
# For other auth methods, see: https://cloud.google.com/docs/authentication
adapter = BigQueryAdapter(
    project_id="your-project-id",
    dataset_id="analytics_data",
    location="US",
    # Optional: Use batch mode for large operations (increases max query size to 20MB)
    use_batch_mode=True
)

# Create a table for event analytics data
create_table_sql = """
CREATE TABLE IF NOT EXISTS event_analytics (
    event_id STRING,
    event_timestamp TIMESTAMP,
    user_id STRING,
    session_id STRING,
    event_name STRING,
    platform STRING,
    country STRING,
    device_type STRING,
    properties JSON
)
"""
adapter.execute(create_table_sql)
print("Created event_analytics table")

# Generate INSERT statements for batch processing
insert_statements = []

# Generate event data for the last 3 days
platforms = ["web", "ios", "android"]
countries = ["US", "UK", "CA", "DE", "FR", "JP", "AU", "BR", "IN"]
devices = ["mobile", "tablet", "desktop"]
event_types = ["page_view", "click", "scroll", "form_submit", "purchase", "login", "signup"]

# Generate sample event data
base_time = datetime.datetime.now() - datetime.timedelta(days=3)
for i in range(1, 1001):
    # Create realistic but varied event data
    event_id = f"evt_{i:06d}"
    event_time = base_time + datetime.timedelta(
        hours=i % 72,
        minutes=i % 60,
        seconds=i % 60
    )
    timestamp_str = event_time.strftime("%Y-%m-%d %H:%M:%S")
    
    user_id = f"user_{(i % 100) + 1:03d}"
    session_id = f"session_{(i % 50) + 1:02d}_{(i % 10) + 1}"
    event_name = event_types[i % len(event_types)]
    platform = platforms[i % len(platforms)]
    country = countries[i % len(countries)]
    device = devices[i % len(devices)]
    
    # Create JSON properties with varying complexity
    if event_name == "page_view":
        props = f'{{"page_url": "/page/{i % 20}", "referrer": "google.com", "load_time": {(i % 5) + 0.5}}}'
    elif event_name == "purchase":
        props = f'{{"transaction_id": "TX{i:04d}", "amount": {(i % 100) + 9.99}, "currency": "USD", "items": [{{"id": "item1", "price": {(i % 50) + 4.99}}}]}}'
    else:
        props = f'{{"element_id": "btn_{i % 10}", "position": {{"x": {i % 100}, "y": {i % 200}}}}}'
    
    # Create the INSERT statement with proper formatting for BigQuery
    insert_sql = f"""
    INSERT INTO event_analytics (
        event_id, event_timestamp, user_id, session_id, 
        event_name, platform, country, device_type, properties
    ) VALUES (
        '{event_id}',
        TIMESTAMP '{timestamp_str}',
        '{user_id}',
        '{session_id}',
        '{event_name}',
        '{platform}',
        '{country}',
        '{device}',
        '{props}'
    )
    """
    insert_statements.append(insert_sql)

# For BigQuery, a larger batch size is effective in batch mode
batcher = SQLBatcher(max_bytes=15_000_000)  # 15MB for batch mode operations

print(f"Inserting {len(insert_statements)} events...")

# Use a transaction for batch operations (BigQuery supports multi-statement transactions)
try:
    # Begin transaction
    adapter.begin_transaction()
    
    # Process all INSERT statements
    total_processed = batcher.process_statements(insert_statements, adapter.execute)
    print(f"Processed {total_processed} INSERT statements")
    
    # Commit the transaction
    adapter.commit_transaction()
    print("Transaction committed successfully")
except Exception as e:
    # Rollback on error
    adapter.rollback_transaction()
    print(f"Error: {e}")
    raise

# Run some analytical queries to demonstrate BigQuery's strengths
print("\nRunning analytical queries...")

# Query 1: Events by platform and country
platform_query = """
SELECT
  platform,
  country,
  COUNT(*) as event_count
FROM 
  event_analytics
GROUP BY 
  platform, country
ORDER BY 
  event_count DESC
LIMIT 10
"""
platform_results = adapter.execute(platform_query)
print("\nTop Platform-Country Combinations:")
for row in platform_results:
    print(f"  {row[0]} / {row[1]}: {row[2]} events")

# Query 2: Event counts by hour of day
hourly_query = """
SELECT
  EXTRACT(HOUR FROM event_timestamp) as hour_of_day,
  COUNT(*) as event_count
FROM 
  event_analytics
GROUP BY 
  hour_of_day
ORDER BY 
  hour_of_day
"""
hourly_results = adapter.execute(hourly_query)
print("\nEvents by Hour of Day:")
for row in hourly_results:
    hour = int(row[0])
    count = row[1]
    print(f"  {hour:02d}:00 - {hour:02d}:59: {count} events {'|' * (count // 10)}")

# Query 3: Complex JSON property extraction and analysis
if event_name == "purchase":
    purchase_query = """
    SELECT
      JSON_VALUE(properties, '$.transaction_id') as transaction_id,
      CAST(JSON_VALUE(properties, '$.amount') AS FLOAT64) as amount,
      user_id,
      event_timestamp
    FROM 
      event_analytics
    WHERE 
      event_name = 'purchase'
    ORDER BY 
      amount DESC
    LIMIT 5
    """
    purchase_results = adapter.execute(purchase_query)
    print("\nTop 5 Purchases:")
    for row in purchase_results:
        print(f"  Transaction: {row[0]}, Amount: ${row[1]:.2f}, User: {row[2]}, Time: {row[3]}")

# Close the connection
adapter.close()
```

### Using PostgreSQL Adapter with Advanced Features

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters.postgresql import PostgreSQLAdapter
import psycopg2
import psycopg2.extras
import json
import time

# Create a PostgreSQL adapter with advanced features
adapter = PostgreSQLAdapter(
    connection_params={
        "host": "localhost",
        "database": "analytics",
        "user": "postgres",
        "password": "your_password"
    },
    isolation_level="read_committed",
    cursor_factory=psycopg2.extras.RealDictCursor,  # Return dictionaries instead of tuples
    application_name="data-pipeline"  # Helps with monitoring in pg_stat_activity
)

# Create a table with PostgreSQL-specific features like JSONB and arrays
adapter.execute("""
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    properties JSONB DEFAULT '{}'::jsonb,
    tags TEXT[] DEFAULT '{}'
)
""")

# Create some indices for better performance
indices = [
    {
        "columns": ["user_id", "event_type"],
        "name": "idx_events_user_event",
        "method": "btree"
    },
    {
        "columns": ["event_time"],
        "name": "idx_events_time",
        "method": "brin"  # BRIN indices are great for time-series data
    },
    {
        "columns": ["properties"],
        "name": "idx_events_properties",
        "method": "gin"  # GIN indices for JSONB queries
    },
    {
        "columns": ["tags"],
        "name": "idx_events_tags",
        "method": "gin"  # GIN for array queries
    }
]

# Create all indices in a single operation
adapter.create_indices("events", indices)

# Generate event data
events = []
for i in range(1, 1001):
    user_id = i % 100 + 1
    event_type = ["pageview", "click", "signup", "purchase", "login"][i % 5]
    
    # Complex JSON properties
    properties = {
        "page": f"/page/{i % 20}",
        "referrer": ["google", "facebook", "twitter", "direct"][i % 4],
        "device": {
            "type": ["mobile", "desktop", "tablet"][i % 3],
            "browser": ["chrome", "firefox", "safari", "edge"][i % 4],
            "os": ["windows", "macos", "linux", "ios", "android"][i % 5]
        },
        "metrics": {
            "load_time": round(i % 10 * 0.1 + 0.5, 2),
            "engagement": i % 5 + 1
        }
    }
    
    # Array of tags
    tags = ["web"]
    if i % 3 == 0:
        tags.append("promotion")
    if i % 5 == 0:
        tags.append("new-user")
    
    events.append((user_id, event_type, json.dumps(properties), tags))

# Use the COPY command for ultra-fast bulk loading
start_time = time.time()
row_count = adapter.use_copy_for_bulk_insert(
    table_name="events",
    column_names=["user_id", "event_type", "properties", "tags"],
    data=events
)
elapsed = time.time() - start_time
print(f"Inserted {row_count} events in {elapsed:.2f} seconds ({row_count/elapsed:.1f} rows/sec)")

# Run a complex analytical query using PostgreSQL's JSONB operators
results = adapter.execute("""
SELECT 
    e.event_type,
    COUNT(*) as event_count,
    e.properties->'device'->'type' as device_type,
    COUNT(DISTINCT e.user_id) as unique_users,
    ARRAY_AGG(DISTINCT t) as all_tags
FROM 
    events e,
    UNNEST(e.tags) t
WHERE 
    e.properties->>'referrer' = 'google' AND
    e.properties->'metrics'->>'engagement' >= '3'
GROUP BY 
    e.event_type, e.properties->'device'->'type'
ORDER BY 
    event_count DESC
""")

# Print results as a dictionary when using RealDictCursor
for row in results:
    print(f"Event: {row['event_type']}, Device: {row['device_type']}")
    print(f"  Count: {row['event_count']}, Unique Users: {row['unique_users']}")
    print(f"  Tags: {row['all_tags']}")

# Close the connection
adapter.close()
```

### Using Snowflake Adapter

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters.snowflake import SnowflakeAdapter
import datetime
import csv
import os

# Create a Snowflake adapter with authentication options
adapter = SnowflakeAdapter(
    account="your_account.snowflakecomputing.com",
    user="your_username",
    password="your_password",  # Or use key_pair_path, sso_token, etc.
    warehouse="compute_wh",
    database="analytics",
    schema="marketing",
    role="analyst",
    session_parameters={
        "TIMEZONE": "America/New_York",
        "QUERY_TAG": "sql_batcher_example"
    }
)

# Create a stage and table for our marketing campaign data
setup_statements = [
    """CREATE OR REPLACE TABLE marketing_campaigns (
        campaign_id VARCHAR(16),
        campaign_name VARCHAR(100),
        start_date DATE,
        end_date DATE,
        channel VARCHAR(50),
        budget DECIMAL(12,2),
        target_audience VARCHAR(50),
        status VARCHAR(20)
    )""",
    """CREATE OR REPLACE STAGE temp_campaign_stage
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 
                       FIELD_OPTIONALLY_ENCLOSED_BY = '"')"""
]

# Execute setup statements
for stmt in setup_statements:
    adapter.execute(stmt)

# Create a temporary CSV file with campaign data
campaigns = [
    ["CMP001", "Summer Sale 2025", "2025-06-01", "2025-06-30", "Email", 15000.00, "Existing Customers", "Scheduled"],
    ["CMP002", "New Product Launch", "2025-07-15", "2025-08-15", "Social Media", 25000.00, "New Prospects", "Draft"],
    ["CMP003", "Holiday Special", "2025-12-01", "2025-12-25", "Multiple", 50000.00, "All Customers", "Planning"],
    ["CMP004", "Spring Promo", "2025-03-01", "2025-03-31", "Display Ads", 18500.00, "Lapsed Customers", "Scheduled"],
    ["CMP005", "Brand Campaign", "2025-08-01", "2025-10-31", "TV", 75000.00, "General Public", "Approved"]
]

# Write to a temporary CSV file
temp_csv_path = "campaigns.csv"
with open(temp_csv_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["campaign_id", "campaign_name", "start_date", "end_date", 
                    "channel", "budget", "target_audience", "status"])
    writer.writerows(campaigns)

# Upload the CSV to Snowflake stage
print("Uploading CSV to Snowflake stage...")
adapter.execute(f"PUT file://{temp_csv_path} @temp_campaign_stage OVERWRITE=TRUE")

# Copy data from stage to table
copy_cmd = """
COPY INTO marketing_campaigns
FROM @temp_campaign_stage/campaigns.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"')
"""
adapter.execute(copy_cmd)
print("Loaded CSV data into marketing_campaigns table")

# Now let's generate additional campaigns programmatically
insert_statements = []

# Generate 100 additional campaign records
for i in range(6, 106):
    campaign_id = f"CMP{i:03d}"
    campaign_name = f"Campaign {i} " + ("Q1" if i % 4 == 0 else "Q2" if i % 4 == 1 else "Q3" if i % 4 == 2 else "Q4")
    
    # Alternate channels
    channels = ["Email", "Social Media", "Display Ads", "Search", "TV", "Radio", "Print", "Direct Mail"]
    channel = channels[i % len(channels)]
    
    # Alternate target audiences
    audiences = ["New Customers", "Existing Customers", "Lapsed Customers", "High Value", "Low Activity", "Regional"]
    audience = audiences[i % len(audiences)]
    
    # Vary budget based on channel
    base_budget = 5000 + (i * 100)
    if channel in ["TV", "Radio"]:
        budget = base_budget * 3
    elif channel in ["Print", "Direct Mail"]:
        budget = base_budget * 2
    else:
        budget = base_budget
    
    # Vary status
    statuses = ["Draft", "Planning", "Scheduled", "Active", "Completed", "On Hold"]
    status = statuses[i % len(statuses)]
    
    # Create date range in different quarters
    year = 2025
    quarter = (i % 4) + 1
    month_start = (quarter - 1) * 3 + 1
    month_end = month_start + 2
    
    # Generate an INSERT statement with proper Snowflake date formatting
    insert_sql = f"""
    INSERT INTO marketing_campaigns (
        campaign_id, campaign_name, start_date, end_date, 
        channel, budget, target_audience, status
    ) VALUES (
        '{campaign_id}', 
        '{campaign_name}', 
        TO_DATE('{year}-{month_start:02d}-01'), 
        TO_DATE('{year}-{month_end:02d}-{28 if month_end == 2 else 30}'), 
        '{channel}', 
        {budget}, 
        '{audience}', 
        '{status}'
    )
    """
    insert_statements.append(insert_sql)

# Create a batcher with an 8MB batch size (Snowflake can handle large batches)
batcher = SQLBatcher(max_bytes=8_000_000)

# Process batch inserts with transaction support
try:
    # Begin a transaction
    adapter.begin_transaction()
    
    # Process all insert statements
    print("Inserting additional campaign records...")
    total_inserted = batcher.process_statements(insert_statements, adapter.execute)
    print(f"Inserted {total_inserted} additional campaign records")
    
    # Commit the transaction
    adapter.commit_transaction()
except Exception as e:
    # Rollback on error
    adapter.rollback_transaction()
    print(f"Error: {e}")
    raise

# Run analytics queries on the campaign data
print("\nRunning analytics queries...")

# Query 1: Campaign count by channel
channel_query = """
SELECT channel, COUNT(*) as campaign_count, SUM(budget) as total_budget
FROM marketing_campaigns
GROUP BY channel
ORDER BY total_budget DESC
"""
channel_results = adapter.execute(channel_query)
print("\nCampaign Distribution by Channel:")
for row in channel_results:
    print(f"  {row[0]}: {row[1]} campaigns, ${row[2]:,.2f} total budget")

# Query 2: Campaign by quarter
quarter_query = """
SELECT 
    DATEADD(QUARTER, DATEDIFF(QUARTER, '1970-01-01', start_date), '1970-01-01') as quarter_start,
    COUNT(*) as campaign_count
FROM marketing_campaigns
GROUP BY quarter_start
ORDER BY quarter_start
"""
quarter_results = adapter.execute(quarter_query)
print("\nCampaign Count by Quarter:")
for row in quarter_results:
    print(f"  Q{row[0].month//3 + 1} {row[0].year}: {row[1]} campaigns")

# Clean up
adapter.execute("DROP STAGE IF EXISTS temp_campaign_stage")
os.remove(temp_csv_path)

# Close the connection
adapter.close()
```

## Documentation

SQL Batcher's documentation is available in several formats:

- **API Reference**: Visit [sql-batcher.readthedocs.io](https://sql-batcher.readthedocs.io/) for the full API documentation
- **Examples**: See the [`examples/`](https://github.com/yourusername/sql-batcher/tree/main/examples) directory for practical usage examples
- **Source Code**: The [GitHub repository](https://github.com/yourusername/sql-batcher) contains the latest code and issue tracker
- **In-Code Documentation**: All classes and methods include detailed docstrings that can be accessed via Python's `help()` function

### Building Documentation Locally

```bash
# Clone the repository
git clone https://github.com/yourusername/sql-batcher.git
cd sql-batcher

# Install documentation dependencies
pip install -e ".[docs]"

# Build the documentation
cd docs
make html

# View documentation
# Open _build/html/index.html in your browser
```

## Adapters

SQL Batcher comes with several built-in adapters:

- `GenericAdapter`: For generic database connections (MySQL, SQLite, etc.) that follow the DB-API 2.0 specification
- `PostgreSQLAdapter`: For PostgreSQL databases with optimized features like COPY and specialized transaction management
- `TrinoAdapter`: For Trino/Presto databases with specific size constraints
- `SnowflakeAdapter`: For Snowflake databases with transaction support
- `BigQueryAdapter`: For Google BigQuery with support for interactive and batch query modes
- `SparkAdapter`: For PySpark SQL operations

You can also create custom adapters by extending the `SQLAdapter` base class for other database systems.

## Database-Specific Features

Different databases have unique feature sets and limitations:

### PostgreSQL
- **Query Size Limit**: Practical limit ~500MB
- **Transaction Support**: Full ACID compliance
- **Batch Strategy**: Client-side batching with semicolon-separated statements
- **COPY Command**: Ultra-fast bulk data loading
- **Advanced Features**: JSONB, Array types, GIN/GiST indices
- **Ideal Batch Size**: 500KB-5MB

### Trino (formerly PrestoSQL)
- **Query Size Limit**: ~1MB
- **Transaction Support**: Limited, depends on catalog (Hive ACID, etc.)
- **Batch Strategy**: Client-side statement batching (one statement per execution)
- **Ideal Batch Size**: 500-900KB

### Snowflake
- **Query Size Limit**: 1MB for UI, 50MB for JDBC/ODBC
- **Transaction Support**: Yes (multi-statement)
- **Batch Strategy**: Client-side batching with transaction blocks
- **Ideal Batch Size**: 1-10MB

### BigQuery
- **Query Size Limit**: 1MB for interactive, 20MB for batch
- **Transaction Support**: Yes (since 2022)
- **Batch Strategy**: Client-side batching with Jobs API for large operations
- **Ideal Batch Size**: 10-15MB for batch mode

### Spark SQL
- **Query Size Limit**: Depends on driver memory
- **Transaction Support**: Limited (Delta Lake adds ACID)
- **Batch Strategy**: Sequential statement execution via SparkSession.sql()
- **Ideal Batch Size**: 10-50MB

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.