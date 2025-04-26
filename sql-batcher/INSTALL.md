# Installation Guide

This document provides instructions for installing the `sql-batcher` package.

## Requirements

- Python 3.7 or later

## Installing from PyPI

The recommended way to install `sql-batcher` is from PyPI:

```bash
pip install sql-batcher
```

To install with support for specific database backends:

```bash
# Trino support
pip install sql-batcher[trino]

# Spark support
pip install sql-batcher[spark]

# Snowflake support
pip install sql-batcher[snowflake]

# All supported databases
pip install sql-batcher[all]
```

## Installing from Source

To install from source:

1. Clone the repository
   ```bash
   git clone https://github.com/example/sql-batcher.git
   cd sql-batcher
   ```

2. Install the package
   ```bash
   pip install -e .
   ```

   For development installation with testing tools:
   ```bash
   pip install -e ".[dev]"
   ```

## Verifying Installation

After installation, you can verify that it's working correctly:

```bash
# Check the installed version
sql-batcher version

# List available adapters
sql-batcher adapters
```

## Development Setup

For development, it's recommended to use a virtual environment:

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate
# On Unix or MacOS:
source venv/bin/activate

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest
```