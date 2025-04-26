"""
Configuration settings for the CSV to Iceberg application.

This module contains default settings and configuration variables used
throughout the application.
"""
import os
import logging

# Default Trino connection parameters
DEFAULT_TRINO_HOST = "sep.sdp-dev.pd.switchnet.nv"
DEFAULT_TRINO_PORT = 443
DEFAULT_TRINO_USER = "admin"
DEFAULT_HTTP_SCHEME = "https"
DEFAULT_TRINO_ROLE = "sysadmin"
DEFAULT_TRINO_CATALOG = "iceberg"
DEFAULT_TRINO_SCHEMA = "default"

# Default Hive connection parameters
DEFAULT_HIVE_METASTORE_URI = ""
DEFAULT_USE_HIVE_METASTORE = False

# Default CSV conversion settings
DEFAULT_DELIMITER = ","
DEFAULT_QUOTE_CHAR = '"'
DEFAULT_BATCH_SIZE = 1000
DEFAULT_SAMPLE_SIZE = 1000
DEFAULT_MODE = "overwrite"  # 'overwrite' or 'append'

# Database paths
APP_DATA_DIR = os.path.expanduser("~/.csv_to_iceberg")
LMDB_JOBS_PATH = os.path.join(APP_DATA_DIR, "lmdb_jobs")
LMDB_CONFIG_PATH = os.path.join(APP_DATA_DIR, "lmdb_config")

# Upload settings
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), "uploads")
MAX_UPLOAD_SIZE = 500 * 1024 * 1024  # 500MB

# Ensure necessary directories exist
os.makedirs(APP_DATA_DIR, exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Log file location
LOG_FILE = os.path.join(os.path.dirname(__file__), "csv_to_iceberg.log")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Job retention settings
JOB_RETENTION_DAYS = 30
MAX_JOBS = 100