# CSV to Iceberg Conversion Tool

A command-line tool for converting CSV files to Apache Iceberg tables.

## Overview

This tool allows data engineers to easily convert CSV data files to Apache Iceberg tables with automatic schema inference, batch processing, and progress tracking. It connects to a Trino server for SQL operations and includes placeholder support for future Hive metastore integration, which is currently not enabled.

## Features

- Automatic schema inference from CSV files
- Support for both append and overwrite modes
- Batch processing for large CSV files
- Progress tracking with percentage complete
- Schema validation and compatibility checks
- Configurable CSV parsing options (delimiter, quote character, headers)

## Requirements

- Python 3.8+
- Trino server with Iceberg catalog
- Required Python packages:
  - click
  - polars
  - pyarrow
  - pyiceberg
  - pyhive (for optional Hive metastore support)
  - thrift
  - trino
  - rich (for progress display)
  - lmdb (for storing configuration and job data)

Optional Features (Future Support):
- Direct Hive metastore connection (currently disabled but code structure maintained for future implementation)

## Installation

```bash
# Install the required dependencies
pip install -r requirements.txt
```

## Usage

### Basic Usage

Convert a CSV file to an Iceberg table:

```bash
python csv_to_iceberg.py convert \
  --csv-file sample_data.csv \
  --table-name catalog.schema.table \
  --trino-host localhost \
  --trino-port 443 \
  --trino-user admin \
  --trino-password your_password \
  --http-scheme https \
  --trino-role sysadmin \
  --trino-catalog iceberg \
  --trino-schema default
```

Note: Direct Hive metastore connection is currently disabled but may be supported in future versions. The following parameters exist in the codebase but are not active:

```bash
# Future support - not currently implemented
python csv_to_iceberg.py convert \
  # ... standard parameters as above ...
  --hive-metastore-uri localhost:9083 \
  --use-hive-metastore
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
  --table-name iceberg.default.my_table \
  --trino-host localhost \
  --trino-port 443 \
  --trino-user admin \
  --trino-password your_password \
  --http-scheme https \
  --trino-catalog iceberg \
  --trino-schema default
```

### Overwrite an existing table

```bash
python csv_to_iceberg.py convert \
  --csv-file updated_data.csv \
  --table-name iceberg.default.existing_table \
  --trino-host localhost \
  --trino-port 443 \
  --trino-user admin \
  --trino-password your_password \
  --http-scheme https \
  --trino-catalog iceberg \
  --trino-schema default \
  --mode overwrite
```

### Process a large CSV with custom batch size

```bash
python csv_to_iceberg.py convert \
  --csv-file large_data.csv \
  --batch-size 50000 \
  --table-name iceberg.default.large_table \
  --trino-host localhost \
  --trino-port 443 \
  --trino-user admin \
  --trino-password your_password \
  --http-scheme https \
  --trino-catalog iceberg \
  --trino-schema default
```

## Troubleshooting

If you encounter issues:

1. Use the `--verbose` flag to enable detailed logging
2. Ensure Trino server is running and accessible 
3. Verify connection parameters (host, port)
4. Check CSV file format and encoding
5. For authentication errors, verify Trino username and password are correct

Note: Direct Hive metastore connection is not currently active in this version but the code structure is maintained for future implementation.

### Authentication Issues

If you receive a "Cannot use authentication with HTTP" error:

1. Use HTTPS instead of HTTP when connecting to a Trino server with authentication:
   ```bash
   python csv_to_iceberg.py convert \
     --csv-file data.csv \
     --table-name iceberg.default.my_table \
     --trino-host trino-server.example.com \
     --trino-port 8443 \
     --trino-user admin \
     --trino-password your_password \
     --http-scheme https \   # Use HTTPS for authentication
     --trino-catalog iceberg \
     --trino-schema default
   ```

2. If you cannot use HTTPS:
   - Consider using a Trino server configuration without authentication
   - Configure Trino to accept HTTP authentication (not recommended for production)

Note: For security reasons, passwords should only be transmitted over HTTPS connections.

### Role-Based Authorization

This tool supports Trino's role-based authorization system using the `--trino-role` parameter. The role is passed to Trino in the `X-Trino-Role` header in the format `system=ROLE{roleName}`.

```bash
python csv_to_iceberg.py convert \
  --csv-file data.csv \
  --table-name iceberg.default.my_table \
  --trino-host trino-server.example.com \
  --trino-user admin \
  --trino-password your_password \
  --http-scheme https \
  --trino-role my_role \   # Specifies a custom role
  --trino-catalog iceberg \
  --trino-schema default
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