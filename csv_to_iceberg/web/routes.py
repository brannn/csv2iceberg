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
from datetime import datetime, timedelta
import tempfile
import logging
import threading

# Utility functions for templates
def format_datetime(timestamp):
    """Format a timestamp for display."""
    if not timestamp:
        return "N/A"
    if isinstance(timestamp, (int, float)):
        dt = datetime.fromtimestamp(timestamp)
    else:
        dt = timestamp
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def format_duration(duration=None, start_time=None, end_time=None):
    """Format a duration or calculate it from start and end times.
    
    This function can be called in multiple ways:
    - format_duration(duration): With just a duration in seconds
    - format_duration(start_time=start, end_time=end): With named start and end times
    - format_duration(start, end): With positional start and end times
    """
    # Handle the case when called with positional arguments
    if duration is not None and start_time is not None and end_time is None:
        # This means it was called as format_duration(start_time, end_time)
        start_time, end_time = duration, start_time
        duration = None
    
    if duration is not None:
        # If duration is directly provided
        seconds = duration
    elif start_time and end_time:
        # Calculate duration from start and end times
        if isinstance(start_time, (int, float)) and isinstance(end_time, (int, float)):
            seconds = end_time - start_time
        elif isinstance(start_time, datetime) and isinstance(end_time, datetime):
            seconds = (end_time - start_time).total_seconds()
        else:
            return "N/A"
    else:
        return "N/A"
    
    # Convert seconds to a human-readable format
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hours"

def format_status(status):
    """Format a job status for display."""
    if status == "completed":
        return "Completed"
    elif status == "failed":
        return "Failed"
    elif status == "running":
        return "Running"
    else:
        return status.capitalize() if status else "Unknown"

def format_size(size_bytes):
    """Format file size in a human-readable format."""
    if not size_bytes:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(size_bytes)
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.2f} {units[unit_index]}"
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

def get_git_info():
    """Get git information (branch and commit SHA)."""
    import subprocess
    
    try:
        # Get branch name
        branch = subprocess.check_output(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            stderr=subprocess.DEVNULL
        ).decode('utf-8').strip()
        
        # Get commit SHA
        commit_sha = subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            stderr=subprocess.DEVNULL
        ).decode('utf-8').strip()
        
        if branch and commit_sha:
            return f"{branch}/{commit_sha}"
        elif commit_sha:
            return commit_sha
        else:
            return "unknown"
    except (subprocess.SubprocessError, FileNotFoundError):
        return "unknown"

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
    
@routes.route('/analyze-csv', methods=['POST'])
def analyze_csv():
    """Analyze a CSV file and return its schema."""
    logger.debug("Analyze CSV route called")
    try:
        if 'csv_file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
            
        csv_file = request.files['csv_file']
        if not csv_file or csv_file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
            
        # Get parameters
        delimiter = request.form.get('delimiter', ',')
        quote_char = request.form.get('quote_char', '"')
        has_header = request.form.get('has_header') == 'true'
        sample_size = int(request.form.get('sample_size', 1000))
        
        # Save the file temporarily
        temp_dir = os.path.join(os.getcwd(), 'uploads')
        os.makedirs(temp_dir, exist_ok=True)
        
        file_id = str(uuid.uuid4())
        file_path = os.path.join(temp_dir, f'{file_id}_{secure_filename(csv_file.filename)}')
        
        csv_file.save(file_path)
        
        # Infer schema
        schema = infer_schema_from_csv(
            csv_file=file_path,
            delimiter=delimiter,
            quote_char=quote_char,
            has_header=has_header,
            sample_size=sample_size
        )
        
        # Clean up the file
        try:
            os.remove(file_path)
        except OSError:
            logger.warning(f"Could not remove temporary file: {file_path}")
            
        # Convert the PyIceberg schema to a list of columns
        columns = []
        for field in schema.fields:
            columns.append({
                'name': field.name,
                'type': str(field.field_type),
                'required': field.required,
            })
            
        return jsonify({'schema': columns}), 200
        
    except Exception as e:
        logger.error(f"Error analyzing CSV: {str(e)}", exc_info=True)
        return jsonify({'error': f'Error analyzing CSV: {str(e)}'}), 500

@routes.route('/convert', methods=['GET', 'POST'])
def convert():
    """Render the conversion page and handle conversion requests."""
    logger.debug("Convert route called")
    profiles_list = config_manager.get_profiles()
    last_used_profile = None
    last_used = config_manager.get_last_used_profile()
    if last_used:
        last_used_profile = last_used.get('name')
    
    if request.method == 'POST':
        try:
            # Get the uploaded file
            csv_file = request.files.get('csv_file')
            if not csv_file:
                flash('No file selected', 'error')
                return render_template('convert.html', profiles=profiles_list, last_used_profile=last_used_profile)
            
            # Get form parameters
            profile_name = request.form.get('profile')
            table_name = request.form.get('table_name')
            delimiter = request.form.get('delimiter', ',')
            has_header = request.form.get('has_header') == 'true'
            quote_char = request.form.get('quote_char', '"')
            write_mode = request.form.get('write_mode', 'append')
            batch_size = int(request.form.get('batch_size', 10000))
            sample_size = int(request.form.get('sample_size', 1000))
            include_columns = request.form.get('include_columns', '')
            exclude_columns = request.form.get('exclude_columns', '')
            use_custom_schema = request.form.get('use_custom_schema') == 'true'
            custom_schema = request.form.get('custom_schema', '') if use_custom_schema else ''
            
            # Parse column lists
            include_cols_list = [col.strip() for col in include_columns.split(',')] if include_columns.strip() else None
            exclude_cols_list = [col.strip() for col in exclude_columns.split(',')] if exclude_columns.strip() else None
            
            # Validate required fields
            if not profile_name:
                flash('Profile is required', 'error')
                return render_template('convert.html', profiles=profiles_list, last_used_profile=last_used_profile)
            
            if not table_name:
                flash('Table name is required', 'error')
                return render_template('convert.html', profiles=profiles_list, last_used_profile=last_used_profile)
            
            # Get the profile
            profile = config_manager.get_profile(profile_name)
            if not profile:
                flash(f'Profile {profile_name} not found', 'error')
                return render_template('convert.html', profiles=profiles_list, last_used_profile=last_used_profile)
            
            # Set last used profile
            config_manager.set_last_used_profile(profile_name)
            
            # Save the file to a temporary location
            temp_dir = os.path.join(os.getcwd(), 'uploads')
            os.makedirs(temp_dir, exist_ok=True)
            
            # Create a unique filename
            file_id = str(uuid.uuid4())
            file_path = os.path.join(temp_dir, f'{file_id}_{secure_filename(csv_file.filename)}')
            
            # Save the file
            csv_file.save(file_path)
            
            # Create a job ID
            job_id = str(uuid.uuid4())
            
            # Get file size
            file_size = os.path.getsize(file_path)
            
            # Get catalog and schema from the profile
            catalog = profile.get('trino_catalog')
            schema = profile.get('trino_schema')
            
            # Construct the fully qualified table name if needed
            # If the user entered a bare table name (no dots), prepend catalog and schema
            if table_name and '.' not in table_name:
                full_table_name = f"{catalog}.{schema}.{table_name}"
                logger.info(f"Constructing full table name: {full_table_name} from catalog: {catalog}, schema: {schema}, table: {table_name}")
            else:
                # User provided a qualified name already, use as-is
                full_table_name = table_name
                logger.info(f"Using user-provided qualified table name: {full_table_name}")
            
            # Create job parameters
            job_params = {
                'csv_file': file_path,
                'delimiter': delimiter,
                'has_header': has_header,
                'quote_char': quote_char,
                'batch_size': batch_size,
                'table_name': full_table_name,
                'trino_host': profile.get('trino_host'),
                'trino_port': profile.get('trino_port'),
                'trino_user': profile.get('trino_user'),
                'trino_password': profile.get('trino_password'),
                'http_scheme': profile.get('http_scheme'),
                'trino_role': profile.get('trino_role'),
                'trino_catalog': profile.get('trino_catalog'),
                'trino_schema': profile.get('trino_schema'),
                'hive_metastore_uri': profile.get('hive_metastore_uri'),
                'use_hive_metastore': profile.get('use_hive_metastore'),
                'mode': write_mode,
                'sample_size': sample_size,
                'file_size': file_size,
                'original_filename': secure_filename(csv_file.filename),
                'include_columns': include_cols_list,
                'exclude_columns': exclude_cols_list,
                'custom_schema': custom_schema if custom_schema.strip() else None
            }
            
            # Create the job
            job = job_manager.create_job(job_id, job_params)
            
            # Run the job in a background thread
            def run_conversion_job():
                try:
                    # Import the centralized conversion service
                    from csv_to_iceberg.core.conversion_service import convert_csv_to_iceberg
                    
                    # Update job status to running
                    job_manager.update_job(job_id, {
                        'status': 'running',
                        'started_at': datetime.now(),
                    })
                    job_manager.update_job_progress(job_id, 0)
                    
                    # Progress callback function
                    def update_progress(percent):
                        job_manager.update_job_progress(job_id, percent)
                    
                    # Run the conversion using our centralized service
                    result = convert_csv_to_iceberg(
                        # File parameters
                        csv_file=job_params['csv_file'],
                        table_name=job_params['table_name'],
                        
                        # Connection parameters
                        trino_host=job_params['trino_host'],
                        trino_port=job_params['trino_port'],
                        trino_user=job_params['trino_user'],
                        trino_password=job_params['trino_password'],
                        http_scheme=job_params['http_scheme'],
                        trino_role=job_params['trino_role'],
                        trino_catalog=job_params['trino_catalog'],
                        trino_schema=job_params['trino_schema'],
                        use_hive_metastore=job_params['use_hive_metastore'],
                        hive_metastore_uri=job_params['hive_metastore_uri'],
                        
                        # CSV handling parameters
                        delimiter=job_params['delimiter'],
                        quote_char=job_params['quote_char'],
                        has_header=job_params['has_header'],
                        batch_size=job_params['batch_size'],
                        
                        # Schema/data parameters
                        mode=job_params['mode'],
                        sample_size=job_params['sample_size'],
                        include_columns=job_params['include_columns'],
                        exclude_columns=job_params['exclude_columns'],
                        custom_schema=job_params['custom_schema'],
                        
                        # Progress callback
                        progress_callback=update_progress
                    )
                    
                    # Handle the result
                    if result['success']:
                        # Get the row count 
                        rows_processed = result.get('rows_processed', 0)
                        stdout = result.get('stdout', '')
                        
                        # Update job with row count and stdout
                        updates = {
                            'rows_processed': rows_processed,
                            'stdout': stdout
                        }
                        job_manager.update_job(job_id, updates)
                        
                        # Mark job as completed with success, stdout, and ensure rows_processed stays
                        job_manager.mark_job_completed(
                            job_id, 
                            success=True,
                            stdout=stdout
                        )
                        
                        # Make a final update to ensure rows_processed is preserved
                        job_manager.update_job(job_id, {'rows_processed': rows_processed})
                    else:
                        # Mark job as failed with error and traceback if available
                        error = result['error']
                        traceback_info = result.get('traceback', '')
                        stdout = result.get('stdout', '')
                        
                        # Create a detailed error message with traceback
                        stderr_info = f"Error: {error}\n\n"
                        if traceback_info:
                            stderr_info += f"Traceback:\n{traceback_info}"
                            
                        # First update the job with any collected stdout/logs
                        if stdout:
                            job_manager.update_job(job_id, {'stdout': stdout})
                            
                        # Then mark the job as failed with complete error details    
                        job_manager.mark_job_completed(
                            job_id, 
                            success=False, 
                            error=error,
                            stderr=stderr_info,
                            stdout=stdout
                        )
                    
                    # Clean up temporary file in either case
                    try:
                        os.remove(job_params['csv_file'])
                    except OSError:
                        logger.warning(f"Could not remove temporary file: {job_params['csv_file']}")
                        
                except Exception as e:
                    import traceback
                    error_msg = str(e)
                    # Get the full traceback
                    tb = traceback.format_exc()
                    logger.error(f"Error in conversion job {job_id}: {error_msg}", exc_info=True)
                    
                    # Create a detailed error message with traceback
                    detailed_error = f"Error: {error_msg}\n\nTraceback:\n{tb}"
                    
                    # Store both the summary error and detailed stderr output
                    job_manager.mark_job_completed(
                        job_id, 
                        success=False, 
                        error=error_msg, 
                        stderr=detailed_error
                    )
                    
                    # Clean up temporary file on error
                    try:
                        os.remove(job_params['csv_file'])
                    except OSError:
                        logger.warning(f"Could not remove temporary file: {job_params['csv_file']}")
            
            # Start the job in a background thread
            threading.Thread(target=run_conversion_job, daemon=True).start()
            
            flash(f'Conversion started with job ID: {job_id}', 'success')
            # Redirect to the job detail page instead of the jobs list
            return redirect(url_for('routes.job_detail', job_id=job_id))
            
        except Exception as e:
            logger.error(f"Error starting conversion: {str(e)}", exc_info=True)
            flash(f'Error starting conversion: {str(e)}', 'error')
    
    return render_template('convert.html', profiles=profiles_list, last_used_profile=last_used_profile)

@routes.route('/jobs')
def jobs():
    """Render the jobs list page."""
    logger.debug("Jobs route called")
    all_jobs = job_manager.get_all_jobs(limit=50, include_test_jobs=False)
    
    # Sort jobs by creation time, newest first
    jobs_sorted = sorted(all_jobs, key=lambda j: j.get('created_at', 0), reverse=True)
    
    # Format job data for display
    for job in jobs_sorted:
        # Format duration
        duration = job.get('duration')
        job['duration_formatted'] = format_duration(duration) if duration else 'N/A'
        
        # Format timestamps
        created_at = job.get('created_at')
        completed_at = job.get('completed_at')
        job['created_at_formatted'] = format_datetime(created_at) if created_at else 'N/A'
        job['completed_at_formatted'] = format_datetime(completed_at) if completed_at else 'N/A'
        
        # Format status
        status = job.get('status')
        job['status_formatted'] = format_status(status) if status is not None else 'Unknown'
        
        # Format file size
        file_size = job.get('file_size', 0)
        job['file_size_formatted'] = format_size(file_size) if file_size else 'N/A'
    
    # Pass utility functions to the template
    return render_template(
        'jobs.html', 
        jobs=jobs_sorted,
        format_duration=format_duration,
        format_datetime=format_datetime,
        format_status=format_status,
        format_size=format_size,
        now=datetime.now()
    )

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

@routes.route('/profiles/get/<name>')
def profile_get(name):
    """Get a profile by name."""
    logger.debug(f"Profile get route called for {name}")
    profile = config_manager.get_profile(name)
    if profile:
        return jsonify({'profile': profile}), 200
    else:
        return jsonify({'error': f'Profile {name} not found'}), 404

@routes.route('/job/<job_id>')
def job_detail(job_id):
    """Display job details."""
    logger.debug(f"Job detail route called for {job_id}")
    job = job_manager.get_job(job_id)
    
    if not job:
        flash(f'Job {job_id} not found', 'error')
        return redirect(url_for('routes.jobs'))
    
    # Mark job as active to prevent cleanup
    job_manager.mark_job_as_active(job_id)
    
    # Format job data for display
    # Format duration
    duration = job.get('duration')
    job['duration_formatted'] = format_duration(duration) if duration else 'N/A'
    
    # Format timestamps
    created_at = job.get('created_at')
    completed_at = job.get('completed_at')
    job['created_at_formatted'] = format_datetime(created_at) if created_at else 'N/A'
    job['completed_at_formatted'] = format_datetime(completed_at) if completed_at else 'N/A'
    
    # Add ISO format for JavaScript timer with timezone info for better compatibility
    if created_at and isinstance(created_at, datetime):
        # Format with explicit timezone info to ensure proper client-side parsing
        # Use UTC format with Z suffix for better JavaScript compatibility
        job['created_at_iso'] = created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Format status
    status = job.get('status')
    job['status_formatted'] = format_status(status) if status is not None else 'Unknown'
    
    # Format file size
    file_size = job.get('file_size', 0)
    job['file_size_formatted'] = format_size(file_size) if file_size else 'N/A'
    
    # Ensure core job fields are present (even if empty)
    if not job.get('table_name'):
        job['table_name'] = job.get('params', {}).get('table_name', '')
    
    if not job.get('original_filename'):
        job['original_filename'] = job.get('params', {}).get('original_filename', '')
    
    if not job.get('mode'):
        job['mode'] = job.get('params', {}).get('mode', '')
    
    # Format parameters for the connection details table
    params = {}
    for k, v in job.items():
        if k not in ('created_at', 'completed_at', 'duration', 'status', 'progress', 
                     'error', 'stdout', 'stderr', 'table_name', 'file_size', 
                     'original_filename', 'mode'):
            if isinstance(v, (str, int, float, bool)) or v is None:
                params[k] = v
    
    # Include important connection parameters from the params dictionary
    job_params = job.get('params', {})
    params.update({
        'trino_host': job_params.get('trino_host', ''),
        'trino_port': job_params.get('trino_port', ''),
        'trino_user': job_params.get('trino_user', ''),
        'trino_role': job_params.get('trino_role', ''),
        'trino_catalog': job_params.get('trino_catalog', ''),
        'trino_schema': job_params.get('trino_schema', ''),
        'use_hive_metastore': job_params.get('use_hive_metastore', False),
        'hive_metastore_uri': job_params.get('hive_metastore_uri', '')
    })
    
    return render_template(
        'job_detail.html', 
        job=job, 
        params=params,
        format_duration=format_duration,
        format_datetime=format_datetime,
        format_status=format_status,
        format_size=format_size,
        now=datetime.now()
    )
    
@routes.route('/api/job/<job_id>/status')
def job_status(job_id):
    """Get job status as JSON."""
    job = job_manager.get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    # Mark job as active to prevent cleanup
    job_manager.mark_job_as_active(job_id)
    
    # Extract relevant status information
    status_data = {
        'id': job_id,
        'status': job.get('status'),
        'progress': job.get('progress', 0),
        'created_at': job.get('created_at'),
        'completed_at': job.get('completed_at'),
        'duration': job.get('duration'),
        'rows_processed': job.get('rows_processed'),
        'error': job.get('error')
    }
    
    return jsonify(status_data)