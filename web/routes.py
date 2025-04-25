import uuid
import os
import logging
import traceback
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_from_directory, abort
from werkzeug.utils import secure_filename
import datetime
import json
import tempfile
import time
import io
import re

from storage.job_manager import JobManager
from storage.lmdb_config_manager import LMDBConfigManager
from core.conversion_service import convert_csv_to_iceberg

# Define default batch size
DEFAULT_BATCH_SIZE = 20000
from core.query_collector import QueryCollector

# Create a blueprint for all routes
routes = Blueprint('routes', __name__)

# Initialize the job manager and config manager
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
        ).decode().strip()
        
        # Get commit SHA
        commit_sha = subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            stderr=subprocess.DEVNULL
        ).decode().strip()
        
        return {
            'branch': branch,
            'commit': commit_sha
        }
    except (subprocess.SubprocessError, FileNotFoundError):
        # Return placeholders if git commands fail
        return {
            'branch': 'unknown',
            'commit': 'unknown'
        }
        
# Define a formatting filter for datetimes
def format_datetime_filter(timestamp):
    """Convert ISO datetime string to a formatted datetime string."""
    if not timestamp:
        return "N/A"
        
    # If it's already a datetime object, use it directly
    if isinstance(timestamp, datetime.datetime):
        dt = timestamp
    else:
        # Otherwise, parse the ISO string
        try:
            dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return "Invalid date"
            
    # Format the datetime for display
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# Main routes
@routes.route('/')
def index():
    """Render the main page."""
    return render_template('index.html')

@routes.route('/profiles')
def profiles():
    """Show the profiles page."""
    profiles = config_manager.get_profiles()
    last_used = config_manager.get_last_used_profile()
    return render_template('profiles.html', profiles=profiles, last_used=last_used)

@routes.route('/profiles/add', methods=['GET', 'POST'])
def profile_add():
    """Add a new profile."""
    if request.method == 'POST':
        # Process the form submission
        connection_type = request.form.get('connection_type', 'trino')
        
        # Start with common fields
        profile = {
            'name': request.form.get('name'),
            'description': request.form.get('description'),
            'connection_type': connection_type
        }
        
        # Add connection-specific fields based on connection type
        if connection_type == 'trino':
            # Trino connection fields
            profile.update({
                'trino_host': request.form.get('trino_host'),
                'trino_port': int(request.form.get('trino_port', 443)),
                'trino_user': request.form.get('trino_user'),
                'trino_password': request.form.get('trino_password', ''),
                'http_scheme': request.form.get('http_scheme', 'https'),
                'trino_role': request.form.get('trino_role', 'sysadmin'),
                'trino_catalog': request.form.get('trino_catalog', 'iceberg'),
                'trino_schema': request.form.get('trino_schema', 'default'),
                'use_hive_metastore': request.form.get('use_hive_metastore') == 'true',
                'hive_metastore_uri': request.form.get('hive_metastore_uri', ''),
                'max_query_size': int(request.form.get('max_query_size', 700000))
            })
        elif connection_type == 's3_rest':
            # S3 REST API connection fields
            profile.update({
                's3_rest_uri': request.form.get('s3_rest_uri'),
                's3_warehouse_location': request.form.get('s3_warehouse_location'),
                's3_namespace': request.form.get('s3_namespace', 'default'),
                's3_client_id': request.form.get('s3_client_id', ''),
                's3_client_secret': request.form.get('s3_client_secret', ''),
                's3_token': request.form.get('s3_token', ''),
                'aws_region': request.form.get('aws_region', 'us-east-1'),
                'aws_access_key_id': request.form.get('aws_access_key_id', ''),
                'aws_secret_access_key': request.form.get('aws_secret_access_key', ''),
                'aws_session_token': request.form.get('aws_session_token', ''),
                'max_query_size': int(request.form.get('max_query_size', 700000))
            })
        
        # Add the new profile
        if config_manager.add_profile(profile):
            flash(f'Profile {profile["name"]} added successfully', 'success')
            return redirect(url_for('routes.profiles'))
        else:
            flash('Failed to add profile', 'error')
    
    # For GET requests, show an empty form
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
        connection_type = request.form.get('connection_type', 'trino')
        
        # Start with common fields
        updated_profile = {
            'name': request.form.get('name'),
            'description': request.form.get('description'),
            'connection_type': connection_type
        }
        
        # Add connection-specific fields based on connection type
        if connection_type == 'trino':
            # Trino connection fields
            updated_profile.update({
                'trino_host': request.form.get('trino_host'),
                'trino_port': int(request.form.get('trino_port', 443)),
                'trino_user': request.form.get('trino_user'),
                'trino_password': request.form.get('trino_password', ''),
                'http_scheme': request.form.get('http_scheme', 'https'),
                'trino_role': request.form.get('trino_role', 'sysadmin'),
                'trino_catalog': request.form.get('trino_catalog', 'iceberg'),
                'trino_schema': request.form.get('trino_schema', 'default'),
                'use_hive_metastore': request.form.get('use_hive_metastore') == 'true',
                'hive_metastore_uri': request.form.get('hive_metastore_uri', ''),
                'max_query_size': int(request.form.get('max_query_size', 700000))
            })
        elif connection_type == 's3_rest':
            # S3 REST API connection fields
            updated_profile.update({
                's3_rest_uri': request.form.get('s3_rest_uri'),
                's3_warehouse_location': request.form.get('s3_warehouse_location'),
                's3_namespace': request.form.get('s3_namespace', 'default'),
                's3_client_id': request.form.get('s3_client_id', ''),
                's3_client_secret': request.form.get('s3_client_secret', ''),
                's3_token': request.form.get('s3_token', ''),
                'aws_region': request.form.get('aws_region', 'us-east-1'),
                'aws_access_key_id': request.form.get('aws_access_key_id', ''),
                'aws_secret_access_key': request.form.get('aws_secret_access_key', ''),
                'aws_session_token': request.form.get('aws_session_token', ''),
                'max_query_size': int(request.form.get('max_query_size', 700000))
            })
        
        # Update the profile
        if config_manager.update_profile(name, updated_profile):
            flash(f'Profile {updated_profile["name"]} updated successfully', 'success')
            return redirect(url_for('routes.profiles'))
        else:
            flash('Failed to update profile', 'error')
    
    # For GET requests, show the form with existing data
    return render_template('profile_form.html', profile=profile, mode='edit')

@routes.route('/profiles/delete/<name>')
def profile_delete(name):
    """Delete a profile."""
    if config_manager.delete_profile(name):
        flash(f'Profile {name} deleted successfully', 'success')
    else:
        flash(f'Failed to delete profile {name}', 'error')
    return redirect(url_for('routes.profiles'))

@routes.route('/convert', methods=['GET', 'POST'])
def convert():
    """Convert page and form handler."""
    profiles = config_manager.get_profiles()
    
    # Get the last used profile for default selection
    last_profile = config_manager.get_last_used_profile()
    
    if request.method == 'POST':
        # Process file upload and conversion
        profile_name = request.form.get('profile')
        profile = config_manager.get_profile(profile_name)
        
        if not profile:
            flash('Invalid profile selected', 'error')
            return redirect(url_for('routes.convert'))
            
        # Set this as the last used profile
        config_manager.set_last_used_profile(profile_name)
        
        # Get other form inputs
        table_name = request.form.get('table_name', '').strip()
        mode = request.form.get('mode', 'append')
        
        # Validate table name
        if not table_name:
            flash('Table name is required', 'error')
            return redirect(url_for('routes.convert'))
        
        # Check if file was uploaded
        if 'csv_file' not in request.files:
            flash('No file part', 'error')
            return redirect(url_for('routes.convert'))
            
        csv_file = request.files['csv_file']
        
        # Check if a file was selected
        if csv_file.filename == '':
            flash('No file selected', 'error')
            return redirect(url_for('routes.convert'))
            
        # Validate file type
        if not allowed_file(csv_file.filename):
            flash('Invalid file type. Please upload a .csv or .txt file.', 'error')
            return redirect(url_for('routes.convert'))
            
        # Create uploads directory if it doesn't exist
        os.makedirs('uploads', exist_ok=True)
        
        # Generate a unique filename for the uploaded file
        filename = secure_filename(csv_file.filename)
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        unique_filename = f"{timestamp}_{filename}"
        file_path = os.path.join('uploads', unique_filename)
        
        try:
            # Save the file
            csv_file.save(file_path)
            
            # Create a job ID
            job_id = str(uuid.uuid4())
            
            # Get file size
            file_size = os.path.getsize(file_path)
            
            # Get the connection type from the profile
            connection_type = profile.get('connection_type', 'trino')
            
            # Create a unique table name based on the connection type
            if connection_type == 'trino':
                # For Trino, use catalog.schema.table format
                catalog = profile.get('trino_catalog')
                schema = profile.get('trino_schema')
                
                # Construct the fully qualified table name if needed
                # If the user entered a bare table name (no dots), prepend catalog and schema
                if table_name and '.' not in table_name:
                    full_table_name = f"{catalog}.{schema}.{table_name}"
                    logger.info(f"Constructing full Trino table name: {full_table_name} from catalog: {catalog}, schema: {schema}, table: {table_name}")
                else:
                    # User provided a qualified name already, use as-is
                    full_table_name = table_name
                    logger.info(f"Using user-provided qualified table name: {full_table_name}")
            elif connection_type == 's3_rest':
                # For S3 Tables REST API, use namespace.table format or just table name
                namespace = profile.get('s3_namespace', 'default')
                
                # Construct the table name with namespace if needed
                if table_name and '.' not in table_name:
                    full_table_name = f"{namespace}.{table_name}"
                    logger.info(f"Constructing S3 table name: {full_table_name} from namespace: {namespace}, table: {table_name}")
                else:
                    # User provided a qualified name (namespace.table) already, use as-is
                    full_table_name = table_name
                    logger.info(f"Using user-provided qualified S3 table name: {full_table_name}")
            else:
                # Fallback, just use the table name as-is
                full_table_name = table_name
                logger.warning(f"Unknown connection type '{connection_type}', using table name as-is: {table_name}")
            
            # Get CSV file parameters
            delimiter = request.form.get('delimiter', ',')
            if delimiter == 'tab':
                delimiter = '\t'
            elif delimiter == 'pipe':
                delimiter = '|'
            elif delimiter == 'semicolon':
                delimiter = ';'
                
            quote_char = request.form.get('quote_char', '"')
            has_header = request.form.get('has_header') == 'true'
            
            # Get batch size
            try:
                batch_size = int(request.form.get('batch_size', DEFAULT_BATCH_SIZE))
            except ValueError:
                batch_size = DEFAULT_BATCH_SIZE
                
            # Create a conversion job
            job = {
                'id': job_id,
                'status': 'queued',
                'created': datetime.datetime.now().isoformat(),
                'updated': datetime.datetime.now().isoformat(),
                'file_path': file_path,
                'file_name': filename,
                'file_size': file_size,
                'table_name': full_table_name,
                'profile_name': profile_name,
                'delimiter': delimiter,
                'quote_char': quote_char,
                'has_header': has_header,
                'batch_size': batch_size,
                'mode': mode,
                'connection_type': connection_type
            }
            
            # Save the job
            job_manager.add_job(job)
            
            # Redirect to the job status page
            flash(f'Conversion job created successfully. Job ID: {job_id}', 'success')
            return redirect(url_for('routes.job_detail', job_id=job_id))
            
        except Exception as e:
            traceback.print_exc()
            flash(f'Error creating conversion job: {str(e)}', 'error')
            return redirect(url_for('routes.convert'))
    
    # For GET requests, show the conversion form
    return render_template('convert.html', profiles=profiles, last_profile=last_profile)

@routes.route('/job/<job_id>')
def job_detail(job_id):
    """Show job details."""
    job = job_manager.get_job(job_id)
    
    if not job:
        flash('Job not found', 'error')
        return redirect(url_for('routes.jobs'))
        
    # For test jobs, we don't have an actual CSV file
    is_test_job = job.get('is_test', False)
    
    # Get the profile associated with the job
    profile_name = job.get('profile_name')
    profile = config_manager.get_profile(profile_name) if profile_name else None
    
    # Get any generated SQL for the job
    job_id = job.get('id')
    sql_queries = job.get('sql_queries', [])
    
    # Get the job status
    status = job.get('status', 'unknown')
    
    # Check if the job is completed or failed
    is_completed = status in ['completed', 'failed']
    
    # Template variables
    template_vars = {
        'job': job,
        'profile': profile,
        'sql_queries': sql_queries,
        'is_completed': is_completed,
        'is_test_job': is_test_job
    }
    
    # If the job is in 'queued' status, start processing it
    if status == 'queued' and not is_test_job:
        # Update the job status to 'processing'
        job_manager.update_job_status(job_id, 'processing')
        
        # Background thread to process the job
        def process_job():
            try:
                # Get the job parameters
                file_path = job.get('file_path')
                table_name = job.get('table_name')
                delimiter = job.get('delimiter', ',')
                quote_char = job.get('quote_char', '"')
                has_header = job.get('has_header', True)
                batch_size = job.get('batch_size', DEFAULT_BATCH_SIZE)
                mode = job.get('mode', 'append')
                
                # Get the profile for this job
                profile = config_manager.get_profile(job.get('profile_name'))
                
                if not profile:
                    raise ValueError(f"Profile {job.get('profile_name')} not found")
                    
                # Log job start
                logger.info(f"Starting job {job_id} - Converting {file_path} to {table_name}")
                
                # Create a query collector for storing SQL statements
                query_collector = QueryCollector()
                
                # Process the CSV file
                convert_csv_to_iceberg(
                    file_path=file_path,
                    table_name=table_name,
                    connection_config=profile,
                    delimiter=delimiter,
                    quote_char=quote_char,
                    has_header=has_header,
                    batch_size=batch_size,
                    mode=mode,
                    query_collector=query_collector
                )
                
                # Update the job with the collected queries
                job_manager.update_job_field(job_id, 'sql_queries', query_collector.get_queries())
                
                # Mark the job as completed
                job_manager.update_job_status(job_id, 'completed')
                logger.info(f"Job {job_id} completed successfully")
                
            except Exception as e:
                # Log the error
                logger.error(f"Error processing job {job_id}: {str(e)}")
                traceback.print_exc()
                
                # Update the job with the error message
                job_manager.update_job_field(job_id, 'error', str(e))
                
                # Mark the job as failed
                job_manager.update_job_status(job_id, 'failed')
        
        # Start the background thread
        import threading
        thread = threading.Thread(target=process_job)
        thread.daemon = True
        thread.start()
        
        # Show the job details page while the job is running
        return render_template('job_detail.html', **template_vars)
    
    # For completed or failed jobs, show the details
    return render_template('job_detail.html', **template_vars)

@routes.route('/jobs')
def jobs():
    """Show all jobs."""
    # Retrieve all jobs
    all_jobs = job_manager.get_all_jobs()
    
    # Group the jobs by date
    grouped_jobs = {}
    
    # Process each job
    for job in all_jobs:
        # Get the job creation date
        created = job.get('created')
        
        # Skip jobs without a creation date
        if not created:
            continue
            
        # Convert to a datetime object if it's a string
        if isinstance(created, str):
            try:
                created_dt = datetime.datetime.fromisoformat(created.replace('Z', '+00:00'))
            except ValueError:
                # Skip jobs with invalid dates
                continue
        else:
            # Already a datetime
            created_dt = created
            
        # Format the date as YYYY-MM-DD for grouping
        date_str = created_dt.strftime('%Y-%m-%d')
        
        # Add to the group
        if date_str not in grouped_jobs:
            grouped_jobs[date_str] = []
            
        grouped_jobs[date_str].append(job)
        
    # Sort each group by creation date, newest first
    for date_str in grouped_jobs:
        grouped_jobs[date_str] = sorted(
            grouped_jobs[date_str],
            key=lambda j: j.get('created', ''),
            reverse=True
        )
        
    # Sort the dates, newest first
    sorted_dates = sorted(grouped_jobs.keys(), reverse=True)
    
    # Render the jobs list template
    return render_template('jobs.html', grouped_jobs=grouped_jobs, sorted_dates=sorted_dates)

@routes.route('/job/<job_id>/delete')
def job_delete(job_id):
    """Delete a job."""
    # Delete the job
    if job_manager.delete_job(job_id):
        flash(f'Job {job_id} deleted successfully', 'success')
    else:
        flash(f'Failed to delete job {job_id}', 'error')
        
    # Redirect to the jobs list
    return redirect(url_for('routes.jobs'))

@routes.route('/storage')
def storage_stats():
    """Show storage statistics."""
    # Get stats from all LMDB databases
    lmdb_stats = {}
    
    # Add job store stats
    lmdb_stats['jobs'] = job_manager.get_stats()
    
    # Add config store stats
    lmdb_stats['config'] = config_manager.get_stats()
    
    # Calculate total size
    total_size = sum(stats.get('size', 0) for stats in lmdb_stats.values())
    
    # Render the storage stats template
    return render_template('storage_stats.html', 
                          lmdb_stats=lmdb_stats, 
                          total_size=total_size)

@routes.route('/uploads/<filename>')
def download_file(filename):
    """Download a file from the uploads directory."""
    return send_from_directory('uploads', filename)

@routes.route('/uploads/<path:path>')
def static_file(path):
    """Serve static files from the uploads directory."""
    return send_from_directory('uploads', path)

@routes.route('/api/check-table', methods=['POST'])
def check_table():
    """API endpoint to check if a table exists."""
    # Get the JSON data from the request
    data = request.json
    
    if not data:
        return jsonify({
            'error': 'No data provided'
        }), 400
        
    # Get the table name and profile name
    table_name = data.get('table_name')
    profile_name = data.get('profile_name')
    
    if not table_name or not profile_name:
        return jsonify({
            'error': 'Table name and profile name are required'
        }), 400
        
    # Get the profile
    profile = config_manager.get_profile(profile_name)
    
    if not profile:
        return jsonify({
            'error': 'Profile not found'
        }), 404
        
    # Check if the table exists
    # This is just a placeholder - in a real app, you would check the table
    # This would require connecting to the database, which is beyond this example
    table_exists = False
    
    # Return the result
    return jsonify({
        'exists': table_exists,
        'table_name': table_name
    })
