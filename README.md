# CSV to Iceberg Conversion Tool

A command-line tool for converting CSV files to Apache Iceberg tables.

## Overview

This tool allows data engineers to easily convert CSV data files to Apache Iceberg tables with automatic schema inference, batch processing, and progress tracking. It connects to a Trino server for SQL operations and a Hive metastore for metadata management.

## Features

- Automatic schema inference from CSV files
- Support for both append and overwrite modes
- Batch processing for large CSV files
- Progress tracking with percentage complete
- Schema validation and compatibility checks
- Configurable CSV parsing options (delimiter, quote character, headers)
- Data Dictionary Integration with column comments and table properties
- Column filtering to include/exclude specific columns

## Requirements

- Python 3.8+
- Trino server
- Hive metastore
- Required Python packages:
  - click
  - pandas
  - pyarrow
  - pyiceberg
  - pyhive
  - thriftpy2
  - rich (for progress display)

## Installation

```bash
# Install the required dependencies
pip install -r requirements.txt
```

## Web Interface

You can use the web interface to convert CSV files to Iceberg tables through a user-friendly browser UI. This provides an interactive experience with:

- CSV file uploading and preview
- Schema customization with type editing and column comments
- Table property management
- Column filtering with include/exclude options
- Job monitoring with real-time progress updates

To start the web interface:

```bash
# Start with Flask's development server
python main.py

# Or use Gunicorn for production deployment
gunicorn --bind 0.0.0.0:5000 --reuse-port --reload main:app
```

Then open your browser to http://localhost:5000 to access the web interface.

## CLI Usage

### Basic Usage

Convert a CSV file to an Iceberg table:

```bash
python csv_to_iceberg.py convert \
  --csv-file sample_data.csv \
  --table-name catalog.schema.table \
  --trino-host localhost \
  --trino-port 8080 \
  --trino-user admin \
  --trino-password your_password \
  --trino-catalog hive \
  --trino-schema default \
  --hive-metastore-uri localhost:9083
```

### Available Options

```
Usage: csv_to_iceberg.py convert [OPTIONS]

  Convert a CSV file to an Iceberg table.

Options:
  -f, --csv-file TEXT            Path to the CSV file  [required]
  -d, --delimiter TEXT           CSV delimiter (default: comma)
  -h, --has-header               CSV has header row (default: True)
  -q, --quote-char TEXT          CSV quote character (default: double quote)
  -b, --batch-size INTEGER       Batch size for processing (default: 10000)
  -t, --table-name TEXT          Target Iceberg table name (format:
                                 catalog.schema.table)  [required]
  --trino-host TEXT              Trino host  [required]
  --trino-port INTEGER           Trino port (default: 8080)
  --trino-user TEXT              Trino user
  --trino-password TEXT          Trino password (if authentication is enabled)
  --http-scheme [http|https]     HTTP scheme for Trino connection (default: http)
  --trino-role TEXT              Trino role for authorization (default: sysadmin)
  --trino-catalog TEXT           Trino catalog  [required]
  --trino-schema TEXT            Trino schema  [required]
  --hive-metastore-uri TEXT      Hive metastore Thrift URI  [required]
  -m, --mode [append|overwrite]  Write mode (append or overwrite, default:
                                 append)
  --sample-size INTEGER          Number of rows to sample for schema inference
                                 (default: 1000)
  --include-columns TEXT         Comma-separated list of column names to include
                                 (all others will be excluded)
  --exclude-columns TEXT         Comma-separated list of column names to exclude
                                 (ignored if --include-columns is specified)
  --custom-schema TEXT           Path to a JSON file with custom schema definition
                                 (allows type overrides and column comments)
  --table-properties TEXT        Path to a JSON file with table properties
                                 to add to the Iceberg table
  --use-hive-metastore/--no-hive-metastore
                                 Whether to use Hive metastore for table operations
                                 (default: use-hive-metastore)
  -v, --verbose                  Enable verbose logging
  --help                         Show this message and exit.
```

## Examples

### Convert a CSV with custom delimiter and quote character

```bash
python csv_to_iceberg.py convert \
  --csv-file data.csv \
  --delimiter ";" \
  --quote-char "'" \
  --table-name hive.default.my_table \
  --trino-host localhost \
  --trino-port 8080 \
  --trino-user admin \
  --trino-password your_password \
  --trino-catalog hive \
  --trino-schema default \
  --hive-metastore-uri localhost:9083
```

### Overwrite an existing table

```bash
python csv_to_iceberg.py convert \
  --csv-file updated_data.csv \
  --table-name hive.default.existing_table \
  --trino-host localhost \
  --trino-port 8080 \
  --trino-user admin \
  --trino-password your_password \
  --trino-catalog hive \
  --trino-schema default \
  --hive-metastore-uri localhost:9083 \
  --mode overwrite
```

### Process a large CSV with custom batch size

```bash
python csv_to_iceberg.py convert \
  --csv-file large_data.csv \
  --batch-size 50000 \
  --table-name hive.default.large_table \
  --trino-host localhost \
  --trino-port 8080 \
  --trino-user admin \
  --trino-password your_password \
  --trino-catalog hive \
  --trino-schema default \
  --hive-metastore-uri localhost:9083
```

### Use custom schema with column comments

First, create a JSON file with your custom schema (e.g., `schema.json`):

```json
[
  {
    "id": 1,
    "name": "id",
    "type": "int",
    "required": true,
    "comment": "Primary key for the table"
  },
  {
    "id": 2,
    "name": "customer_name",
    "type": "string",
    "required": false,
    "comment": "Full name of the customer"
  },
  {
    "id": 3,
    "name": "order_date",
    "type": "timestamp",
    "required": true,
    "comment": "Date when the order was placed"
  }
]
```

Then run the conversion with this schema:

```bash
python csv_to_iceberg.py convert \
  --csv-file orders.csv \
  --table-name iceberg.analytics.orders \
  --trino-host trino-server.example.com \
  --trino-port 443 \
  --trino-user admin \
  --trino-password your_password \
  --http-scheme https \
  --trino-catalog iceberg \
  --trino-schema analytics \
  --hive-metastore-uri metastore.example.com:9083 \
  --custom-schema schema.json
```

### Add table properties and filter columns

Create a JSON file with table properties (e.g., `properties.json`):

```json
{
  "comment": "Orders data from retail system",
  "owner": "data_team",
  "retention.watermark": "2023-01-01",
  "write.format.default": "parquet",
  "write.metadata.compression-codec": "gzip"
}
```

Then run the conversion with column filtering and table properties:

```bash
python csv_to_iceberg.py convert \
  --csv-file orders_export.csv \
  --table-name iceberg.analytics.filtered_orders \
  --trino-host trino-server.example.com \
  --trino-port 443 \
  --trino-user admin \
  --trino-password your_password \
  --http-scheme https \
  --trino-catalog iceberg \
  --trino-schema analytics \
  --hive-metastore-uri metastore.example.com:9083 \
  --table-properties properties.json \
  --include-columns "id,customer_name,order_date,total_amount" \
  --mode overwrite
```

## Troubleshooting

If you encounter issues:

1. Use the `--verbose` flag to enable detailed logging
2. Ensure Trino server and Hive metastore are running
3. Verify connection parameters (host, port, URI)
4. Check CSV file format and encoding
5. For authentication errors, verify Trino username and password are correct

### Authentication Issues

If you receive a "Cannot use authentication with HTTP" error:

1. Use HTTPS instead of HTTP when connecting to a Trino server with authentication:
   ```bash
   python csv_to_iceberg.py convert \
     --csv-file data.csv \
     --table-name hive.default.my_table \
     --trino-host trino-server.example.com \
     --trino-port 8443 \
     --trino-user admin \
     --trino-password your_password \
     --http-scheme https \   # Use HTTPS for authentication
     --trino-catalog hive \
     --trino-schema default \
     --hive-metastore-uri metastore.example.com:9083
   ```

2. If you cannot use HTTPS:
   - Consider using a Trino server configuration without authentication
   - Configure Trino to accept HTTP authentication (not recommended for production)

Note: For security reasons, passwords should only be transmitted over HTTPS connections.

### Data Dictionary Integration

This tool supports enhancing your Iceberg tables with data dictionary metadata through:

1. **Column Comments**: Add descriptive documentation to columns using the custom schema definition:
   - Helps document the meaning and purpose of each column
   - Makes tables self-documenting for other users
   - Preserved in Iceberg table metadata and visible in SQL clients

2. **Table Properties**: Add table-level metadata using a properties JSON file:
   - Set table comments, ownership information, and other metadata
   - Configure Iceberg-specific table behavior properties
   - Add custom key-value metadata relevant to your organization

These features together provide a comprehensive data dictionary capability directly integrated with your Iceberg tables, making them more maintainable and easier to understand.

### Role-Based Authorization

This tool supports Trino's role-based authorization system using the `--trino-role` parameter. The role is passed to Trino in the `X-Trino-Role` header in the format `system=ROLE{roleName}`.

```bash
python csv_to_iceberg.py convert \
  --csv-file data.csv \
  --table-name hive.default.my_table \
  --trino-host trino-server.example.com \
  --trino-user admin \
  --trino-password your_password \
  --http-scheme https \
  --trino-role my_role \   # Specifies a custom role
  --trino-catalog hive \
  --trino-schema default \
  --hive-metastore-uri metastore.example.com:9083
```

By default, the tool uses the `sysadmin` role if no role is specified. You may need to adjust this based on your Trino server's security configuration:

1. **Common role options**:
   - `sysadmin`: Administrative role with high privileges (default)
   - `user`: Standard user role with more restricted permissions
   - Custom roles configured in your Trino deployment

2. **Using different roles**:
   - For table creation and modification: A role with DDL permissions is required
   - For read-only operations: A role with SELECT permissions may be sufficient

Contact your Trino administrator if you're unsure which role to use for your specific scenario.