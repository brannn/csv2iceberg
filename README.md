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