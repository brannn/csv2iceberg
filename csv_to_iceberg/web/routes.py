"""
Routes for the CSV to Iceberg web application
"""
from flask import (
    Blueprint, render_template, request, redirect, url_for, 
    flash, jsonify, session, send_from_directory
)
from werkzeug.utils import secure_filename
import os
import json
import uuid
import datetime
import tempfile
import logging
import threading
import sys
import traceback
from typing import Dict, Any, List, Optional, Tuple

# These imports use the new package structure
from csv_to_iceberg.storage.job_manager import JobManager
from csv_to_iceberg.storage.config_manager import ConfigManager
from csv_to_iceberg.storage.lmdb_config_manager import LMDBConfigManager
from csv_to_iceberg.connectors.trino_client import TrinoClient
from csv_to_iceberg.core.schema_inferrer import infer_schema_from_csv
from csv_to_iceberg.utils import (
    clean_column_name, get_trino_role_header, get_file_size, is_test_job_id, 
    format_duration, format_datetime, format_size, format_status
)

# Blueprint for routes
routes = Blueprint('routes', __name__)

# Global instances
job_manager = JobManager()
config_manager = LMDBConfigManager()

# Set up logging
logger = logging.getLogger(__name__)

# Helper functions
def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    ALLOWED_EXTENSIONS = {'csv', 'txt'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Route handlers
@routes.route('/')
def index():
    """Render the home page."""
    logger.debug("Index route called")
    try:
        return render_template('index.html')
    except Exception as e:
        logger.error(f"Error rendering index.html: {str(e)}", exc_info=True)
        return "Error rendering template. Check logs."

@routes.route('/convert', methods=['GET', 'POST'])
def convert():
    """Render the conversion page."""
    logger.debug("Convert route called")
    return render_template('convert.html')

@routes.route('/jobs')
def jobs():
    """Render the jobs list page."""
    logger.debug("Jobs route called")
    return render_template('jobs.html')

@routes.route('/profiles')
def profiles():
    """Render the profiles page."""
    logger.debug("Profiles route called")
    return render_template('profiles.html')