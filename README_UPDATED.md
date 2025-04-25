# Milton: CSV to Iceberg Conversion Tool

A web application and command-line tool for converting CSV files to Apache Iceberg tables.

## Overview

Milton (named after the iconic red stapler from "Office Space") allows data engineers to easily convert CSV data files to Apache Iceberg tables with automatic schema inference, smart batch processing, and real-time progress tracking. It connects to a Trino server for SQL operations and includes a user-friendly web interface for interactive use as well as a companion CLI for automation needs.

## Features

- Automatic schema inference from CSV files
- Support for both append and overwrite modes
- Progress tracking with percentage complete
- Schema validation and compatibility checks
- Configurable CSV parsing options (delimiter, quote character, headers)
- Iceberg partitioning recommendations based on column cardinality analysis
- Schema customization interface with data preview
- Connection profile management for reusable configurations
- Persistent job history with detailed logs and statistics

## Requirements

- Python 3.8+
- Trino server with Iceberg catalog
- Required Python packages:
  - click
  - email-validator
  - flask
  - flask-sqlalchemy
  - gunicorn
  - lmdb
  - numpy
  - polars
  - psycopg2-binary
  - pyarrow
  - pyhive
  - pyiceberg
  - requests
  - rich
  - sqlalchemy
  - thrift
  - trino
  - urllib3
  - werkzeug

Optional Features (Future Support):
- Direct Hive metastore connection (currently disabled but code structure maintained for future implementation)

## Installation

```bash
# Install the required dependencies
pip install -r requirements-equivalent.txt

# Start the web application
gunicorn --bind 0.0.0.0:5000 --reuse-port --reload main:app

# Or run directly with Python
python main.py
```

## Usage

### Web Interface

The web interface provides an intuitive way to convert CSV files to Iceberg tables:

1. Start the application:
   ```bash
   gunicorn --bind 0.0.0.0:5000 --reuse-port --reload main:app
   ```

2. Open your browser and navigate to http://localhost:5000

3. From the web interface, you can:
   - Manage connection profiles for different Trino clusters
   - Upload and convert CSV files
   - Customize schemas with the visual editor
   - View partitioning recommendations
   - Monitor job progress
   - Review job history and logs

### Command-Line Interface (CLI)

For automation and scripting, you can use the command-line interface:

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

### CLI Options

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
  --trino-port INTEGER           Trino port (default: 443)
  --trino-user TEXT              Trino user
  --trino-password TEXT          Trino password (if authentication is enabled)
  --http-scheme [http|https]     HTTP scheme for Trino connection (default: https)
  --trino-role TEXT              Trino role for authorization (default: sysadmin)
  --trino-catalog TEXT           Trino catalog  [required]
  --trino-schema TEXT            Trino schema  [required]
  --hive-metastore-uri TEXT      Hive metastore Thrift URI
  --use-hive-metastore           Use direct Hive Metastore connection
  -m, --mode [append|overwrite]  Write mode (append or overwrite, default: append)
  --sample-size INTEGER          Number of rows to sample for schema inference
                                 (default: 1000)
  --custom-schema TEXT           Path to a JSON file containing a custom schema
  --include-columns TEXT         Comma-separated list of column names to include
  --exclude-columns TEXT         Comma-separated list of column names to exclude
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

### Include or exclude specific columns

```bash
python csv_to_iceberg.py convert \
  --csv-file data.csv \
  --table-name iceberg.default.filtered_table \
  --trino-host localhost \
  --trino-port 443 \
  --trino-user admin \
  --trino-password your_password \
  --http-scheme https \
  --trino-catalog iceberg \
  --trino-schema default \
  --include-columns "id,name,date,value"
```

### Using a custom schema file

You can define a custom schema in a JSON file and use it instead of the auto-inferred schema:

```bash
python csv_to_iceberg.py convert \
  --csv-file data.csv \
  --table-name iceberg.default.custom_schema_table \
  --trino-host localhost \
  --trino-port 443 \
  --trino-user admin \
  --trino-password your_password \
  --http-scheme https \
  --trino-catalog iceberg \
  --trino-schema default \
  --custom-schema schema.json
```

## Web Interface Features

### Connection Profiles

The application stores connection profiles in LMDB for secure, persistent storage. Profiles include:
- Trino connection details (host, port, catalog, schema)
- Authentication credentials
- HTTP/HTTPS scheme settings
- Default role

### Schema Customization

The schema editor allows you to:
- View inferred column types
- Modify column names and types
- Mark columns as required or optional
- Preview sample data
- See data type distribution

### Partitioning Recommendations

Based on column cardinality analysis, the application provides intelligent partitioning suggestions:
- Identifies columns ideal for partitioning
- Calculates suitability scores
- Recommends appropriate transforms (identity, bucket, truncate)
- Provides ready-to-use SQL examples

### Job Management

Job tracking features include:
- Real-time progress indicators
- Detailed logs
- Error reporting
- Job history with search and filtering
- Duration tracking
- Row count statistics

## Storage and Performance

- Jobs are persisted in LMDB (Lightning Memory-Mapped Database)
- Completed jobs are retained for 30 days or a maximum of 100 jobs
- Batch processing adapts to column count and row size
- SQL INSERTs are optimized to handle large CSV files efficiently

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