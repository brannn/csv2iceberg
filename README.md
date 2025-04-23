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
- Optional table partitioning with various transforms (year, month, day, hour, bucket, truncate)

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

## Usage

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
  --trino-catalog TEXT           Trino catalog  [required]
  --trino-schema TEXT            Trino schema  [required]
  --hive-metastore-uri TEXT      Hive metastore Thrift URI  [required]
  -m, --mode [append|overwrite]  Write mode (append or overwrite, default:
                                 append)
  --sample-size INTEGER          Number of rows to sample for schema inference
                                 (default: 1000)
  -v, --verbose                  Enable verbose logging
  -p, --partition-by TEXT        Partition specification (can be used multiple times)
                                 Examples: "year(date)", "month(ts)", "bucket(id, 16)"
  --list-partition-transforms    List available partition transforms and exit
  --suggest-partitioning         Suggest potential partitioning strategies based on schema
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

### Create a partitioned table

```bash
python csv_to_iceberg.py convert \
  --csv-file events.csv \
  --table-name hive.default.events_table \
  --trino-host localhost \
  --trino-port 8080 \
  --trino-user admin \
  --trino-password your_password \
  --trino-catalog hive \
  --trino-schema default \
  --hive-metastore-uri localhost:9083 \
  --partition-by "year(event_date)" \
  --partition-by "month(event_date)" \
  --partition-by "bucket(user_id, 16)"
```

### Get partition transform suggestions

```bash
python csv_to_iceberg.py convert \
  --csv-file data.csv \
  --table-name hive.default.my_table \
  --trino-host localhost \
  --trino-port 8080 \
  --trino-catalog hive \
  --trino-schema default \
  --hive-metastore-uri localhost:9083 \
  --suggest-partitioning
```

## Troubleshooting

If you encounter issues:

1. Use the `--verbose` flag to enable detailed logging
2. Ensure Trino server and Hive metastore are running
3. Verify connection parameters (host, port, URI)
4. Check CSV file format and encoding
5. For authentication errors, verify Trino username and password are correct