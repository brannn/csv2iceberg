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

## Which Databases Benefit from SQL Batcher?

SQL Batcher is especially valuable for database systems with query size limitations:

| Database/Engine | Query Size Limitations | Benefits from SQL Batcher |
|-----------------|------------------------|---------------------------|
| **Trino/Presto** | ~1MB query size limit | Essential for bulk operations |
| **Snowflake** | 1MB-8MB statement size limits depending on edition | Significant for large data sets |
| **BigQuery** | 1MB for interactive, 20MB for batch | Critical for complex operations |
| **Redshift** | 16MB maximum query size | Important for ETL processes |
| **MySQL/MariaDB** | 4MB default `max_allowed_packet` | Important for large INSERT operations |
| **PostgreSQL** | 1GB limit, but practical performance issues with large queries | Helpful for bulk operations |
| **Hive** | Configuration-dependent | Essential for data warehouse operations |
| **Oracle** | ~4GB theoretical, much lower in practice | Useful for enterprise applications |
| **SQL Server** | 2GB batch size, 4MB network packet size | Important for large-scale operations |
| **DB2** | 2MB statement size by default | Significant for bulk processing |

## Installation

```bash
pip install sql-batcher
```

### Optional Dependencies

Install with specific database adapters:

```bash
pip install "sql-batcher[trino]"      # For Trino support
pip install "sql-batcher[snowflake]"  # For Snowflake support
pip install "sql-batcher[spark]"      # For PySpark support
pip install "sql-batcher[bigquery]"   # For Google BigQuery support
pip install "sql-batcher[all]"        # All adapters
```

## Quick Start

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters.generic import GenericAdapter
import psycopg2  # PostgreSQL adapter

# Create a connection to PostgreSQL
connection = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="postgres",
    password="password"
)

# Create an adapter
adapter = GenericAdapter(connection=connection)

# Create a table 
adapter.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# Generate many INSERT statements with more complex data
statements = []
for i in range(1, 1001):
    email = f"user{i}@example.com"
    name = f"User {i}"
    statements.append(
        f"INSERT INTO users (id, name, email) VALUES ({i}, '{name}', '{email}')"
    )

# Create a batcher with a 500KB size limit (practical for PostgreSQL)
batcher = SQLBatcher(max_bytes=500_000)

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

- `GenericAdapter`: For generic database connections (PostgreSQL, MySQL, etc.) that follow the DB-API 2.0 specification
- `TrinoAdapter`: For Trino/Presto databases with specific size constraints
- `SnowflakeAdapter`: For Snowflake databases with transaction support
- `BigQueryAdapter`: For Google BigQuery with support for interactive and batch query modes
- `SparkAdapter`: For PySpark SQL operations

You can also create custom adapters by extending the `SQLAdapter` base class for other database systems.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.