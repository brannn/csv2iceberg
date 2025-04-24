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
    profiles_list = config_manager.get_profiles()
    last_used = None
    last_used_profile = config_manager.get_last_used_profile()
    if last_used_profile:
        last_used = last_used_profile.get('name')
    return render_template('profiles.html', profiles=profiles_list, last_used=last_used)

@routes.route('/profiles/add', methods=['GET', 'POST'])
def profile_add():
    """Add a new profile."""
    logger.debug("Profile add route called")
    if request.method == 'POST':
        # Process the form submission
        profile = {
            'name': request.form.get('name'),
            'description': request.form.get('description'),
            'trino_host': request.form.get('trino_host'),
            'trino_port': int(request.form.get('trino_port', 443)),
            'trino_user': request.form.get('trino_user'),
            'trino_password': request.form.get('trino_password', ''),
            'http_scheme': request.form.get('http_scheme', 'https'),
            'trino_role': request.form.get('trino_role', 'sysadmin'),
            'trino_catalog': request.form.get('trino_catalog', 'iceberg'),
            'trino_schema': request.form.get('trino_schema', 'default'),
            'use_hive_metastore': request.form.get('use_hive_metastore') == 'true',
            'hive_metastore_uri': request.form.get('hive_metastore_uri', 'localhost:9083')
        }
        
        success = config_manager.add_profile(profile)
        if success:
            flash('Profile added successfully', 'success')
            return redirect(url_for('routes.profiles'))
        else:
            flash('Failed to add profile', 'error')
    
    # For GET requests, show the form
    return render_template('profile_form.html', profile=None, mode='add')

@routes.route('/profiles/edit/<name>', methods=['GET', 'POST'])
def profile_edit(name):
    """Edit an existing profile."""
    logger.debug(f"Profile edit route called for {name}")
    profile = config_manager.get_profile(name)
    
    if not profile:
        flash(f'Profile {name} not found', 'error')
        return redirect(url_for('routes.profiles'))
    
    if request.method == 'POST':
        # Process the form submission
        updated_profile = {
            'name': request.form.get('name'),
            'description': request.form.get('description'),
            'trino_host': request.form.get('trino_host'),
            'trino_port': int(request.form.get('trino_port', 443)),
            'trino_user': request.form.get('trino_user'),
            'trino_password': request.form.get('trino_password', ''),
            'http_scheme': request.form.get('http_scheme', 'https'),
            'trino_role': request.form.get('trino_role', 'sysadmin'),
            'trino_catalog': request.form.get('trino_catalog', 'iceberg'),
            'trino_schema': request.form.get('trino_schema', 'default'),
            'use_hive_metastore': request.form.get('use_hive_metastore') == 'true',
            'hive_metastore_uri': request.form.get('hive_metastore_uri', 'localhost:9083')
        }
        
        success = config_manager.update_profile(name, updated_profile)
        if success:
            flash('Profile updated successfully', 'success')
            return redirect(url_for('routes.profiles'))
        else:
            flash('Failed to update profile', 'error')
    
    # For GET requests, show the form with existing data
    return render_template('profile_form.html', profile=profile, mode='edit')

@routes.route('/profiles/delete/<name>')
def profile_delete(name):
    """Delete a profile."""
    logger.debug(f"Profile delete route called for {name}")
    success = config_manager.delete_profile(name)
    if success:
        flash(f'Profile {name} deleted successfully', 'success')
    else:
        flash(f'Failed to delete profile {name}', 'error')
    return redirect(url_for('routes.profiles'))

@routes.route('/profiles/use/<name>')
def profile_use(name):
    """Set a profile as the last used profile."""
    logger.debug(f"Setting {name} as last used profile")
    success = config_manager.set_last_used_profile(name)
    if success:
        flash(f'Now using profile: {name}', 'success')
    else:
        flash(f'Failed to set {name} as the active profile', 'error')
    return redirect(url_for('routes.profiles'))