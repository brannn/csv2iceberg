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
    return render_template('profiles.html', profiles=profiles)

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
        flash(f'Job {job_id} not found', 'error')
        return redirect(url_for('routes.jobs'))
        
    # Get the associated profile
    profile = config_manager.get_profile(job.get('profile_name'))
    
    # Use the full path as path in the template
    job_path = job.get('file_path')
    
    # Get job error log if any
    error_log = job.get('error_log', '')
    
    return render_template('job_detail.html', 
                           job=job, 
                           profile=profile, 
                           job_path=job_path, 
                           error_log=error_log)

@routes.route('/jobs')
def jobs():
    """Show all jobs."""
    all_jobs = job_manager.get_jobs()
    
    # Sort jobs by creation date (descending)
    sorted_jobs = sorted(all_jobs, key=lambda j: j.get('created', ''), reverse=True)
    
    return render_template('jobs.html', jobs=sorted_jobs)

@routes.route('/jobs/delete/<job_id>')
def job_delete(job_id):
    """Delete a job."""
    if job_manager.delete_job(job_id):
        flash(f'Job {job_id} deleted successfully', 'success')
    else:
        flash(f'Failed to delete job {job_id}', 'error')
    return redirect(url_for('routes.jobs'))

@routes.route('/run_job/<job_id>')
def run_job(job_id):
    """Run a conversion job."""
    job = job_manager.get_job(job_id)
    
    if not job:
        flash(f'Job {job_id} not found', 'error')
        return redirect(url_for('routes.jobs'))
        
    # Check if the job is already running or completed
    current_status = job.get('status')
    if current_status in ['running', 'completed']:
        flash(f'Job is already {current_status}', 'warning')
        return redirect(url_for('routes.job_detail', job_id=job_id))
        
    # Get the profile
    profile_name = job.get('profile_name')
    profile = config_manager.get_profile(profile_name)
    
    if not profile:
        flash(f'Profile {profile_name} not found', 'error')
        return redirect(url_for('routes.job_detail', job_id=job_id))
        
    try:
        # Update job status to running
        job['status'] = 'running'
        job['started'] = datetime.datetime.now().isoformat()
        job['updated'] = datetime.datetime.now().isoformat()
        job_manager.update_job(job_id, job)
        
        # Run the conversion
        result = convert_csv_to_iceberg(
            csv_file=job.get('file_path'),
            table_name=job.get('table_name'),
            connection_type=job.get('connection_type', 'trino'),
            profile=profile,
            delimiter=job.get('delimiter', ','),
            quote_char=job.get('quote_char', '"'),
            has_header=job.get('has_header', True),
            batch_size=job.get('batch_size', DEFAULT_BATCH_SIZE),
            mode=job.get('mode', 'append')
        )
        
        # Update job status based on the result
        if result.get('success'):
            job['status'] = 'completed'
            
            # Copy the statistics and queries into the job
            job['stats'] = result.get('stats', {})
            job['queries'] = result.get('queries', [])
            
            flash('Conversion completed successfully', 'success')
        else:
            job['status'] = 'failed'
            job['error'] = result.get('error', 'Unknown error')
            job['error_log'] = result.get('error_log', '')
            
            flash(f'Conversion failed: {result.get("error", "Unknown error")}', 'error')
            
        # Set completed timestamp
        job['completed'] = datetime.datetime.now().isoformat()
        job['updated'] = datetime.datetime.now().isoformat()
        
        # Save the updated job
        job_manager.update_job(job_id, job)
        
    except Exception as e:
        # Handle any unexpected errors
        traceback.print_exc()
        error_details = traceback.format_exc()
        
        job['status'] = 'failed'
        job['error'] = str(e)
        job['error_log'] = error_details
        job['updated'] = datetime.datetime.now().isoformat()
        job_manager.update_job(job_id, job)
        
        flash(f'Error running conversion job: {str(e)}', 'error')
    
    return redirect(url_for('routes.job_detail', job_id=job_id))

@routes.route('/api/infer_schema', methods=['POST'])
def infer_schema():
    """API endpoint to infer schema from a CSV file."""
    try:
        # Check if file was uploaded
        if 'csv_file' not in request.files:
            return jsonify({'error': 'No file part'}), 400
            
        csv_file = request.files['csv_file']
        
        # Check if a file was selected
        if csv_file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
            
        # Validate file type
        if not allowed_file(csv_file.filename):
            return jsonify({'error': 'Invalid file type'}), 400
            
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
        
        # Process the file
        from core.schema_inferrer import infer_schema_from_csv
        
        # Save file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            csv_file.save(temp.name)
            temp_path = temp.name
        
        try:
            # Infer schema from the temporary file
            columns, partition_recommendations = infer_schema_from_csv(
                temp_path, 
                delimiter=delimiter, 
                quote_char=quote_char, 
                has_header=has_header
            )
            
            logger.debug(f"Inferred schema with {len(columns)} columns: {columns}")
            
            # Format the partition recommendations for the UI
            formatted_recommendations = []
            for rec in partition_recommendations:
                # Create a simplified version for the UI
                formatted_rec = {
                    'column': rec['column'],
                    'type': rec['type'],
                    'suitability_score': rec['suitability_score'],
                    'cardinality_ratio': round(rec['cardinality_ratio'] * 100, 2),  # Convert to percentage
                    'unique_values': rec['unique_values'],
                    'total_values': rec['total_values'],
                    'transforms': []
                }
                
                # Add the transform recommendations
                for transform in rec['recommendations']:
                    formatted_rec['transforms'].append({
                        'name': transform['transform'],
                        'description': transform['description'],
                        'example': transform['example']
                    })
                
                formatted_recommendations.append(formatted_rec)
                
            # Return both the schema and partition recommendations
            return jsonify({
                'schema': columns,
                'partition_recommendations': formatted_recommendations
            })
            
        finally:
            # Clean up the temporary file
            try:
                os.unlink(temp_path)
            except:
                pass
                
    except Exception as e:
        logger.error(f"Error inferring schema: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@routes.route('/api/profile/<name>')
def get_profile_api(name):
    """API endpoint to get a profile by name."""
    profile = config_manager.get_profile(name)
    
    if not profile:
        return jsonify({'error': 'Profile not found'}), 404
        
    return jsonify(profile)

@routes.route('/api/job/<job_id>')
def get_job_api(job_id):
    """API endpoint to get a job by ID."""
    job = job_manager.get_job(job_id)
    
    if not job:
        return jsonify({'error': 'Job not found'}), 404
        
    return jsonify(job)

@routes.route('/api/job/<job_id>/logs')
def get_job_logs(job_id):
    """API endpoint to get job logs."""
    job = job_manager.get_job(job_id)
    
    if not job:
        return jsonify({'error': 'Job not found'}), 404
        
    # Return any available logs or error messages
    logs = {
        'status': job.get('status', 'unknown'),
        'error': job.get('error', ''),
        'error_log': job.get('error_log', ''),
        'queries': job.get('queries', []),
        'stats': job.get('stats', {})
    }
    
    return jsonify(logs)

@routes.route('/uploads/<filename>')
def uploaded_file(filename):
    """Serve uploaded files."""
    return send_from_directory('uploads', filename)

@routes.route('/dry_run/<job_id>')
def dry_run_job(job_id):
    """Perform a dry run of a conversion job without writing to the database."""
    job = job_manager.get_job(job_id)
    
    if not job:
        flash(f'Job {job_id} not found', 'error')
        return redirect(url_for('routes.jobs'))
        
    # Get the profile
    profile_name = job.get('profile_name')
    profile = config_manager.get_profile(profile_name)
    
    if not profile:
        flash(f'Profile {profile_name} not found', 'error')
        return redirect(url_for('routes.job_detail', job_id=job_id))
        
    try:
        # Create a query collector to store SQL statements
        query_collector = QueryCollector()
        
        # Run the dry run
        result = convert_csv_to_iceberg(
            csv_file=job.get('file_path'),
            table_name=job.get('table_name'),
            connection_type=job.get('connection_type', 'trino'),
            profile=profile,
            delimiter=job.get('delimiter', ','),
            quote_char=job.get('quote_char', '"'),
            has_header=job.get('has_header', True),
            batch_size=job.get('batch_size', DEFAULT_BATCH_SIZE),
            mode=job.get('mode', 'append'),
            dry_run=True,
            query_collector=query_collector
        )
        
        # Return the queries that would be executed
        return render_template('dry_run.html', 
                              job=job, 
                              profile=profile, 
                              queries=query_collector.get_queries(),
                              stats=result.get('stats', {}))
        
    except Exception as e:
        # Handle any unexpected errors
        flash(f'Error performing dry run: {str(e)}', 'error')
        return redirect(url_for('routes.job_detail', job_id=job_id))

@routes.route('/storage_stats')
def storage_stats():
    """Show LMDB storage statistics."""
    from storage.lmdb_stats import get_all_storage_stats
    
    try:
        stats = get_all_storage_stats()
        return render_template('storage_stats.html', stats=stats)
    except Exception as e:
        flash(f'Error retrieving storage statistics: {str(e)}', 'error')
        return redirect(url_for('routes.index'))

@routes.route('/about')
def about():
    """Show information about the application."""
    git_info = get_git_info()
    
    return render_template('about.html', git_info=git_info)

# Additional API endpoints for asynchronous operations
@routes.route('/api/job/<job_id>/status')
def job_status_api(job_id):
    """API endpoint to get job status."""
    job = job_manager.get_job(job_id)
    
    if not job:
        return jsonify({'error': 'Job not found'}), 404
        
    # Return basic status information
    status_info = {
        'id': job.get('id'),
        'status': job.get('status', 'unknown'),
        'created': job.get('created'),
        'started': job.get('started'),
        'completed': job.get('completed'),
        'error': job.get('error', '')
    }
    
    return jsonify(status_info)

# Register the datetime filter with the blueprint
routes.add_app_template_filter(format_datetime_filter, 'format_datetime')
