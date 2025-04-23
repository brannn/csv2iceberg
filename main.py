import os
import logging
import datetime
import time
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
import tempfile
import subprocess
import threading
from werkzeug.utils import secure_filename
from collections import OrderedDict

# Import helper modules
from utils import setup_logging

# Set up logging
logger = setup_logging()
# Enable more verbose Flask logging
logging.basicConfig(level=logging.DEBUG)

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "development-secret-key")

# Configure upload folder
UPLOAD_FOLDER = tempfile.mkdtemp()
ALLOWED_EXTENSIONS = {'csv', 'txt'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB limit

# Constants for job management
MAX_JOBS_TO_KEEP = 50      # Maximum number of jobs to keep in memory
COMPLETED_JOB_TTL = 1800   # Seconds to keep completed jobs in memory (30 minutes)
TEST_JOB_TTL = 3600        # Seconds to keep test jobs in memory (1 hour)

# Dictionary to track which jobs are being actively viewed
# This prevents cleanup of jobs that are currently being viewed
active_job_views = {}

# Utility function for job duration
def format_duration(start_time, end_time):
    """
    Calculate and format the duration between two timestamps.
    
    Args:
        start_time: Start timestamp (datetime object)
        end_time: End timestamp (datetime object)
        
    Returns:
        Formatted duration string (e.g., "5 minutes 30 seconds")
    """
    if not start_time or not end_time:
        return "N/A"
        
    # Calculate duration in seconds
    duration_seconds = (end_time - start_time).total_seconds()
    
    # Format duration
    if duration_seconds < 60:
        return f"{int(duration_seconds)} seconds"
    elif duration_seconds < 3600:
        minutes = int(duration_seconds // 60)
        seconds = int(duration_seconds % 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} {seconds} second{'s' if seconds != 1 else ''}"
    else:
        hours = int(duration_seconds // 3600)
        remaining = duration_seconds % 3600
        minutes = int(remaining // 60)
        seconds = int(remaining % 60)
        return f"{hours} hour{'s' if hours != 1 else ''} {minutes} minute{'s' if minutes != 1 else ''} {seconds} second{'s' if seconds != 1 else ''}"

# Store conversion jobs using OrderedDict to maintain insertion order
# This helps manage job retention policies by time
conversion_jobs = OrderedDict()

def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def run_conversion(job_id, file_path, params):
    """Run the CSV to Iceberg conversion as a background task."""
    try:
        # Get server URL from current environment
        server_host = os.environ.get('HOST', 'localhost')
        server_port = os.environ.get('PORT', '5000')
        server_url = f"http://{server_host}:{server_port}"
        
        # Handle schema customization if present
        schema_temp_file = None
        if 'schema_customization' in params:
            try:
                # Create a temporary file for the custom schema
                import json
                import tempfile
                
                schema_temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
                json.dump(params['schema_customization'], schema_temp_file)
                schema_temp_file.close()
                
                logger.info(f"Created temporary schema customization file: {schema_temp_file.name}")
                
                # This will be passed to the CLI tool
                custom_schema_file = schema_temp_file.name
            except Exception as schema_err:
                logger.error(f"Error creating schema customization file: {str(schema_err)}", exc_info=True)
                if schema_temp_file:
                    try:
                        os.unlink(schema_temp_file.name)
                    except:
                        pass
                custom_schema_file = None
        else:
            custom_schema_file = None
        
        # Build conversion command
        conversion_cmd = [
            "python", "csv_to_iceberg.py", "convert",
            "--csv-file", file_path,
            "--table-name", params['table_name'],
            "--trino-host", params['trino_host'],
            "--trino-port", params['trino_port'],
            "--trino-catalog", params['trino_catalog'],
            "--trino-schema", params['trino_schema'],
            "--hive-metastore-uri", params['hive_metastore_uri']
        ]
        
        # Add Trino authentication if provided
        if params.get('trino_user'):
            conversion_cmd.extend(["--trino-user", params['trino_user']])
        if params.get('trino_password'):
            conversion_cmd.extend(["--trino-password", params['trino_password']])
        if params.get('http_scheme'):
            conversion_cmd.extend(["--http-scheme", params['http_scheme']])
        if params.get('trino_role'):
            conversion_cmd.extend(["--trino-role", params['trino_role']])
        
        # Add optional parameters
        if params.get('delimiter'):
            conversion_cmd.extend(["--delimiter", params['delimiter']])
        if params.get('has_header') == 'false':
            conversion_cmd.append('--no-has-header')
        if params.get('quote_char'):
            conversion_cmd.extend(["--quote-char", params['quote_char']])
        if params.get('batch_size'):
            conversion_cmd.extend(["--batch-size", params['batch_size']])
        if params.get('mode'):
            conversion_cmd.extend(["--mode", params['mode']])
        if params.get('sample_size'):
            conversion_cmd.extend(["--sample-size", params['sample_size']])
        if params.get('verbose') == 'true':
            conversion_cmd.append('--verbose')
            
        # Add custom schema if available
        if custom_schema_file:
            conversion_cmd.extend(["--custom-schema", custom_schema_file])
            
        # Build process monitor command (which will run the conversion command)
        cmd = [
            "python", "process_monitor.py",
            "--job-id", job_id,
            "--api-url", server_url,
            "--"  # Separator for the conversion command
        ] + conversion_cmd
            
        # Run the process monitor
        logger.info(f"Starting conversion job {job_id} with command: {' '.join(cmd)}")
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        # Store results
        conversion_jobs[job_id]['status'] = 'completed' if process.returncode == 0 else 'failed'
        conversion_jobs[job_id]['stdout'] = process.stdout
        conversion_jobs[job_id]['stderr'] = process.stderr
        conversion_jobs[job_id]['returncode'] = process.returncode
        # Add completion timestamp
        conversion_jobs[job_id]['completed_at'] = datetime.datetime.now()
        # Set progress to 100% when completed
        if conversion_jobs[job_id]['status'] == 'completed':
            conversion_jobs[job_id]['progress'] = 100
        
        logger.info(f"Completed conversion job {job_id} with status: {conversion_jobs[job_id]['status']}")
        
        # Clean up temporary files
        if custom_schema_file:
            try:
                os.unlink(custom_schema_file)
                logger.info(f"Removed temporary schema file: {custom_schema_file}")
            except Exception as clean_err:
                logger.warning(f"Failed to clean up schema file {custom_schema_file}: {str(clean_err)}")
        
    except Exception as e:
        logger.error(f"Error in conversion job {job_id}: {str(e)}", exc_info=True)
        conversion_jobs[job_id]['status'] = 'failed'
        conversion_jobs[job_id]['error'] = str(e)
        # Add completion timestamp even for failed jobs
        conversion_jobs[job_id]['completed_at'] = datetime.datetime.now()
        
        # Clean up temporary files in case of error
        if schema_temp_file:
            try:
                os.unlink(schema_temp_file.name)
            except:
                pass

@app.route('/')
def index():
    """Render the home page."""
    logger.debug("Index route called")
    try:
        rendered = render_template('index.html')
        logger.debug("Successfully rendered index.html")
        return rendered
    except Exception as e:
        logger.error(f"Error rendering index.html: {str(e)}", exc_info=True)
        return "Error rendering template. Check logs."

@app.route('/convert', methods=['GET', 'POST'])
def convert():
    """Handle CSV to Iceberg conversion."""
    logger.debug(f"Convert route called with method: {request.method}")
    
    if request.method == 'POST':
        logger.debug(f"POST request data: {request.form}")
        logger.debug(f"POST request files: {request.files.keys()}")
        
        # Check if this is the analyze button (for schema preview)
        if 'analyze_schema' in request.form:
            return handle_schema_analyze()
            
        # Check if a file was uploaded
        if 'csv_file' not in request.files:
            logger.error("No file part in the request")
            flash('No file part', 'error')
            return redirect(request.url)
            
        file = request.files['csv_file']
        logger.debug(f"File received: {file.filename}")
        
        # Check if the file is empty
        if file.filename == '':
            logger.error("No selected file")
            flash('No selected file', 'error')
            return redirect(request.url)
            
        # Check if the file is allowed
        if file and allowed_file(file.filename):
            # Save the file
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            logger.debug(f"Saving file to: {file_path}")
            file.save(file_path)
            
            # Extract CSV parameters
            csv_params = {
                'delimiter': request.form.get('delimiter', ','),
                'has_header': request.form.get('has_header', 'true') == 'true',
                'quote_char': request.form.get('quote_char', '"'),
                'sample_size': int(request.form.get('sample_size', '1000'))
            }
            
            # Check if schema preview is requested
            if 'preview_schema' in request.form and request.form['preview_schema'] == 'true':
                # Create a session for schema preview
                session['schema_preview'] = {
                    'file_path': file_path, 
                    'filename': filename,
                    'csv_params': csv_params,
                    'timestamp': datetime.datetime.now().isoformat()
                }
                logger.debug(f"Created schema preview session: {session['schema_preview']}")
                return redirect(url_for('schema_preview'))
            
            # Regular conversion flow
            # Create job ID
            job_id = os.urandom(8).hex()
            logger.debug(f"Created job ID: {job_id}")
            
            # Collect parameters
            params = {
                'table_name': request.form.get('table_name'),
                'trino_host': request.form.get('trino_host'),
                'trino_port': request.form.get('trino_port'),
                'trino_user': request.form.get('trino_user'),
                'trino_password': request.form.get('trino_password'),
                'http_scheme': request.form.get('http_scheme', 'http'),
                'trino_role': request.form.get('trino_role', 'sysadmin'),
                'trino_catalog': request.form.get('trino_catalog'),
                'trino_schema': request.form.get('trino_schema'),
                'hive_metastore_uri': request.form.get('hive_metastore_uri'),
                'delimiter': csv_params['delimiter'],
                'has_header': 'true' if csv_params['has_header'] else 'false',
                'quote_char': csv_params['quote_char'],
                'batch_size': request.form.get('batch_size'),
                'mode': request.form.get('mode', 'append'),
                'sample_size': str(csv_params['sample_size']),
                'verbose': request.form.get('verbose', 'false')
            }
            logger.debug(f"Collected parameters: {params}")
            
            # Apply schema customizations if they exist in the session
            if 'schema_customization' in session:
                logger.debug("Found schema customization in session")
                # This will be implemented in the run_conversion function
                params['schema_customization'] = session['schema_customization']
                # Clean up the session
                session.pop('schema_customization', None)
            
            # Create job
            conversion_jobs[job_id] = {
                'file_path': file_path,
                'filename': filename,
                'params': params,
                'status': 'running',
                'stdout': '',
                'stderr': '',
                'error': None,
                'returncode': None,
                'started_at': datetime.datetime.now(),
                'progress': 0  # Initialize progress to 0
            }
            logger.debug(f"Created job entry: {conversion_jobs[job_id]}")
            
            # Start conversion thread
            thread = threading.Thread(
                target=run_conversion,
                args=(job_id, file_path, params)
            )
            thread.daemon = True
            thread.start()
            logger.debug(f"Started conversion thread for job {job_id}")
            
            # Redirect to job status page
            logger.debug(f"Redirecting to job status page for job {job_id}")
            return redirect(url_for('job_status', job_id=job_id))
            
        else:
            logger.error(f"File type not allowed: {file.filename}")
            flash('File type not allowed', 'error')
            return redirect(request.url)
            
    # GET request - show conversion form
    logger.debug("Rendering convert.html template")
    return render_template('convert.html')

def handle_schema_analyze():
    """Handle schema analysis when the analyze button is clicked."""
    logger.debug("Handling schema analysis request")
    
    if 'csv_file' not in request.files:
        flash('No file part', 'error')
        return redirect(url_for('convert'))
        
    file = request.files['csv_file']
    if file.filename == '':
        flash('No selected file', 'error')
        return redirect(url_for('convert'))
        
    if file and allowed_file(file.filename):
        # Save the file
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        # Extract CSV parameters
        csv_params = {
            'delimiter': request.form.get('delimiter', ','),
            'has_header': request.form.get('has_header', 'true') == 'true',
            'quote_char': request.form.get('quote_char', '"'),
            'sample_size': int(request.form.get('sample_size', '1000'))
        }
        
        # Store in session for the schema preview page
        session['schema_preview'] = {
            'file_path': file_path,
            'filename': filename,
            'csv_params': csv_params,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        return redirect(url_for('schema_preview'))
    else:
        flash('File type not allowed', 'error')
        return redirect(url_for('convert'))

@app.route('/schema/preview', methods=['GET'])
def schema_preview():
    """Preview and edit the inferred schema from a CSV file."""
    logger.debug("Schema preview route called")
    
    # Check if we have a schema preview session
    if 'schema_preview' not in session:
        flash('No CSV file analyzed. Please upload a file first.', 'error')
        return redirect(url_for('convert'))
    
    preview_data = session['schema_preview']
    file_path = preview_data['file_path']
    filename = preview_data['filename']
    csv_params = preview_data['csv_params']
    
    # Check if file still exists
    if not os.path.exists(file_path):
        flash('CSV file no longer available. Please upload again.', 'error')
        session.pop('schema_preview', None)
        return redirect(url_for('convert'))
    
    try:
        # Infer schema from the CSV
        from schema_inferrer import infer_schema_from_csv
        schema = infer_schema_from_csv(
            file_path,
            delimiter=csv_params['delimiter'],
            has_header=csv_params['has_header'],
            quote_char=csv_params['quote_char'],
            sample_size=csv_params['sample_size']
        )
        
        # Get sample data for preview
        import polars as pl
        read_args = {
            "separator": csv_params['delimiter'],
            "has_header": csv_params['has_header'],
            "quote_char": csv_params['quote_char'],
            "infer_schema_length": 100,
            "n_rows": 10  # Just get a few rows for preview
        }
        
        try:
            df = pl.read_csv(file_path, **read_args)
            # Convert to dict for template rendering
            sample_data = df.to_dicts()
            column_names = df.columns
        except Exception as e:
            logger.warning(f"Error reading CSV sample data: {str(e)}")
            sample_data = []
            column_names = [field.name for field in schema.fields]
        
        # Extract schema information in a template-friendly format
        schema_fields = []
        for field in schema.fields:
            type_info = {
                'original_type': type(field.field_type).__name__.replace('Type', ''),
                'options': ['Boolean', 'Integer', 'Long', 'Float', 'Double', 'Date', 'Timestamp', 'String', 'Decimal']
            }
            
            schema_fields.append({
                'id': field.field_id,
                'name': field.name,
                'type': type_info,
                'required': field.required
            })
        
        # Render the schema preview template
        return render_template(
            'schema_preview.html',
            filename=filename,
            schema_fields=schema_fields,
            sample_data=sample_data,
            column_names=column_names,
            csv_params=csv_params
        )
        
    except Exception as e:
        logger.error(f"Error generating schema preview: {str(e)}", exc_info=True)
        flash(f'Error analyzing CSV schema: {str(e)}', 'error')
        return redirect(url_for('convert'))

@app.route('/schema/apply', methods=['POST'])
def schema_apply():
    """Apply customized schema and proceed with conversion."""
    logger.debug("Schema apply route called")
    
    # Check if we have a schema preview session
    if 'schema_preview' not in session:
        flash('Session expired. Please upload your file again.', 'error')
        return redirect(url_for('convert'))
    
    try:
        # Get the customized schema from the form
        customized_schema = []
        
        # Format of field IDs in form: field_1, field_2, etc.
        field_ids = [key.split('_')[1] for key in request.form.keys() if key.startswith('field_')]
        
        for field_id in field_ids:
            name = request.form.get(f'name_{field_id}')
            type_name = request.form.get(f'type_{field_id}')
            required = request.form.get(f'required_{field_id}', 'false') == 'true'
            
            customized_schema.append({
                'id': int(field_id),
                'name': name,
                'type': type_name,
                'required': required
            })
        
        # Store the customized schema in the session
        session['schema_customization'] = customized_schema
        
        # Redirect to the final conversion step
        flash('Schema customization applied. Proceed with conversion.', 'success')
        
        # Get parameters from the schema preview session
        preview_data = session['schema_preview']
        
        # Pass the parameters to the template
        return render_template(
            'convert_final.html',
            filename=preview_data['filename'],
            file_path=preview_data['file_path'],
            csv_params=preview_data['csv_params'],
            schema_preview=True  # Flag to indicate we came from schema preview
        )
        
    except Exception as e:
        logger.error(f"Error applying schema customization: {str(e)}", exc_info=True)
        flash(f'Error applying schema customization: {str(e)}', 'error')
        return redirect(url_for('schema_preview'))

@app.route('/job/<job_id>')
def job_status(job_id):
    """Show the status of a conversion job."""
    # Clean up old jobs first
    cleanup_old_jobs()
    
    if job_id not in conversion_jobs:
        flash('Job not found or has been archived', 'info')
        return redirect(url_for('jobs'))
    
    # Mark this job as being actively viewed (to prevent premature cleanup)
    mark_job_as_active(job_id)
    
    now = datetime.datetime.now()
    return render_template('job_status.html', 
                          job=conversion_jobs[job_id], 
                          job_id=job_id,
                          now=now,
                          format_duration=format_duration,
                          job_ttl=COMPLETED_JOB_TTL if not (job_id.startswith('test_') or job_id.startswith('running_test_')) else TEST_JOB_TTL)

@app.route('/test/progress/<job_id>')
def test_progress(job_id):
    """Test page for monitoring job progress."""
    # Clean up old jobs first
    cleanup_old_jobs()
    
    if job_id not in conversion_jobs:
        flash('Job not found or has been archived', 'info')
        return redirect(url_for('jobs'))
    
    # Mark this job as being actively viewed (to prevent premature cleanup)
    mark_job_as_active(job_id)
    
    # Determine appropriate TTL based on job type
    ttl = TEST_JOB_TTL if job_id.startswith('test_') or job_id.startswith('running_test_') else COMPLETED_JOB_TTL
    
    return render_template('test_progress.html',
                          job=conversion_jobs[job_id],
                          job_id=job_id,
                          job_ttl=ttl)

@app.route('/jobs')
def jobs():
    """List all conversion jobs."""
    # Clean up old jobs first
    cleanup_old_jobs()
    
    # When the jobs list is viewed, all jobs being viewed are considered "active"
    # This prevents cleanup of jobs shown in the list while user is viewing them
    for job_id in conversion_jobs.keys():
        mark_job_as_active(job_id)
        
    now = datetime.datetime.now()
    return render_template('jobs.html', 
                          jobs=conversion_jobs, 
                          now=now, 
                          format_duration=format_duration,
                          regular_ttl=COMPLETED_JOB_TTL,
                          test_ttl=TEST_JOB_TTL)

@app.route('/job/<job_id>/progress/<int:percent>', methods=['POST'])
def update_job_progress(job_id, percent):
    """Update the progress of a conversion job."""
    logger.debug(f"Progress update for job {job_id}: {percent}%")
    if job_id in conversion_jobs:
        conversion_jobs[job_id]['progress'] = percent
        return jsonify({"status": "ok"})
    else:
        return jsonify({"status": "error", "message": "Job not found"}), 404

# Function to mark a job as being actively viewed
def mark_job_as_active(job_id):
    """
    Mark a job as being actively viewed to prevent cleanup.
    
    Args:
        job_id: The ID of the job to mark as active
    """
    if job_id in conversion_jobs:
        active_job_views[job_id] = datetime.datetime.now()
        logger.debug(f"Marked job {job_id} as active (being viewed)")

# Function to clean up old jobs (both completed/failed and to prevent memory leaks)
def cleanup_old_jobs():
    """
    Remove old completed jobs from memory to prevent memory leaks.
    
    This function:
    1. Keeps a maximum number of jobs in memory
    2. Removes completed/failed jobs after their TTL expires
    3. Respects active job views to prevent removing jobs that are being viewed
    4. Uses a longer TTL for test jobs
    """
    try:
        current_time = datetime.datetime.now()
        jobs_to_remove = []
        
        # First, clean up the active job views list (remove old entries)
        active_view_ids = list(active_job_views.keys())
        for job_id in active_view_ids:
            # Remove active view entry if older than 5 minutes
            if (current_time - active_job_views[job_id]).total_seconds() > 300:  # 5 minutes
                del active_job_views[job_id]
                logger.debug(f"Removed stale active view for job {job_id}")
        
        # Mark jobs that have been completed and exceeded their TTL
        for job_id, job in conversion_jobs.items():
            # Skip jobs that are being actively viewed
            if job_id in active_job_views:
                logger.debug(f"Skipping cleanup for job {job_id} (actively viewed)")
                continue
                
            if job['status'] in ['completed', 'failed']:
                if 'completed_at' in job and job['completed_at']:
                    # Use a longer TTL for test jobs
                    ttl = TEST_JOB_TTL if job_id.startswith('test_') or job_id.startswith('running_test_') else COMPLETED_JOB_TTL
                    time_since_completion = (current_time - job['completed_at']).total_seconds()
                    
                    if time_since_completion > ttl:
                        jobs_to_remove.append(job_id)
        
        # Remove marked jobs
        for job_id in jobs_to_remove:
            if job_id in active_job_views:
                logger.debug(f"Not removing job {job_id} despite TTL expiry (actively viewed)")
                continue
                
            logger.debug(f"Removing completed job {job_id} (TTL expired)")
            if job_id in conversion_jobs:
                del conversion_jobs[job_id]
        
        # If still too many jobs, remove oldest completed ones first, then oldest of any type
        if len(conversion_jobs) > MAX_JOBS_TO_KEEP:
            # Sort completed jobs by completion time, but exclude actively viewed jobs
            completed_jobs = [(job_id, job) for job_id, job in conversion_jobs.items() 
                             if job['status'] in ['completed', 'failed'] 
                             and 'completed_at' in job
                             and job_id not in active_job_views]
            completed_jobs.sort(key=lambda x: x[1]['completed_at'])
            
            # Remove oldest completed jobs first
            for job_id, _ in completed_jobs:
                if len(conversion_jobs) <= MAX_JOBS_TO_KEEP:
                    break
                logger.debug(f"Removing oldest completed job {job_id} (max jobs limit)")
                if job_id in conversion_jobs and job_id not in active_job_views:
                    del conversion_jobs[job_id]
            
            # If still too many, remove oldest by start time (excluding active ones)
            if len(conversion_jobs) > MAX_JOBS_TO_KEEP:
                # Get oldest jobs by start time (excluding active ones)
                all_jobs = [(job_id, job) for job_id, job in conversion_jobs.items()
                           if job_id not in active_job_views]
                all_jobs.sort(key=lambda x: x[1]['started_at'])
                
                # Remove oldest jobs
                for job_id, _ in all_jobs:
                    if len(conversion_jobs) <= MAX_JOBS_TO_KEEP:
                        break
                    logger.debug(f"Removing oldest job {job_id} (max jobs limit)")
                    if job_id in conversion_jobs and job_id not in active_job_views:
                        del conversion_jobs[job_id]
    
    except Exception as e:
        logger.error(f"Error during job cleanup: {str(e)}", exc_info=True)

@app.route('/job/<job_id>/progress', methods=['GET'])
def get_job_progress(job_id):
    """Get the current progress of a conversion job."""
    # Clean up old jobs first (except the one being viewed)
    cleanup_old_jobs()
    
    if job_id in conversion_jobs:
        # Mark this job as being actively viewed during progress polling
        mark_job_as_active(job_id)
        
        return jsonify({
            "job_id": job_id, 
            "progress": conversion_jobs[job_id].get('progress', 0),
            "status": conversion_jobs[job_id]['status'],
            # Include extra info for UI improvements
            "phase": _get_progress_phase(conversion_jobs[job_id].get('progress', 0)),
            "test_job": job_id.startswith('test_') or job_id.startswith('running_test_')
        })
    else:
        # Include an archive flag to help frontend understand why the job is missing
        was_archived = any(job_id.startswith(job_id[:10]) for job_id in conversion_jobs.keys())
        return jsonify({
            "status": "error", 
            "message": "Job not found",
            "possible_reason": "The job was completed and archived" if was_archived else "Unknown job ID"
        }), 404
        
def _get_progress_phase(progress):
    """Get a descriptive phase label for a progress percentage."""
    if progress < 5:
        return "Initializing"
    elif progress < 20:
        return "Connecting to Services"
    elif progress < 40:
        return "Analyzing Data"
    elif progress < 50:
        return "Creating Table"
    elif progress < 90:
        return "Transferring Data"
    elif progress < 100:
        return "Finalizing"
    else:
        return "Completed"

@app.route('/diagnostics')
def diagnostics():
    """Diagnostic endpoint to verify server operation."""
    logger.info("Diagnostics endpoint called")
    try:
        # Count jobs by status
        status_counts = {}
        for job in conversion_jobs.values():
            status = job.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
            
        # Count test jobs
        test_job_count = sum(1 for job_id in conversion_jobs if 
                            job_id.startswith('test_') or job_id.startswith('running_test_'))
            
        # Get active views info
        active_views_info = {
            'count': len(active_job_views),
            'jobs': [{'job_id': job_id, 'last_viewed': active_job_views[job_id].isoformat()} 
                    for job_id in active_job_views]
        }
        
        data = {
            "status": "ok",
            "jobs_count": len(conversion_jobs),
            "jobs_by_status": status_counts,
            "test_job_count": test_job_count,
            "active_views": active_views_info,
            "ttl_settings": {
                "regular_job_ttl_minutes": COMPLETED_JOB_TTL / 60,
                "test_job_ttl_minutes": TEST_JOB_TTL / 60,
                "max_jobs_to_keep": MAX_JOBS_TO_KEEP
            },
            "timestamp": datetime.datetime.now().isoformat(),
            "upload_folder": UPLOAD_FOLDER,
            "template_folder": app.template_folder
        }
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error in diagnostics: {str(e)}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)})

@app.route('/add_test_job')
def add_test_job():
    """Add a test job for development purposes."""
    job_id = "test_job_" + os.urandom(4).hex()
    
    # Create a mock job with completed status and timestamps
    now = datetime.datetime.now()
    ten_minutes_ago = now - datetime.timedelta(minutes=10)
    
    conversion_jobs[job_id] = {
        'file_path': '/tmp/test.csv',
        'filename': 'test.csv',
        'params': {
            'table_name': 'iceberg.test.mock_table',
            'trino_host': 'sep.sdp-dev.pd.switchnet.nv',
            'trino_port': '443',
            'trino_user': 'test_user',
            'trino_password': None,
            'http_scheme': 'https',
            'trino_role': 'sysadmin',
            'trino_catalog': 'iceberg',
            'trino_schema': 'test',
            'hive_metastore_uri': 'localhost:9083',
            'delimiter': ',',
            'has_header': 'true',
            'quote_char': '"',
            'batch_size': '100',
            'mode': 'append',
            'sample_size': '1000',
            'verbose': 'false'
        },
        'status': 'completed',
        'stdout': 'Mock conversion completed successfully.',
        'stderr': '',
        'error': None,
        'returncode': 0,
        'started_at': ten_minutes_ago,
        'completed_at': now,
        'progress': 100  # Completed jobs have 100% progress
    }
    
    flash(f'Test job created with ID: {job_id}', 'success')
    return redirect(url_for('jobs'))

@app.route('/add_running_test_job')
def add_running_test_job():
    """Add a test job with running status for testing progress updates."""
    job_id = "running_test_" + os.urandom(4).hex()
    
    # Create a mock job with running status and timestamps
    now = datetime.datetime.now()
    
    conversion_jobs[job_id] = {
        'file_path': '/tmp/test.csv',
        'filename': 'test.csv',
        'params': {
            'table_name': 'iceberg.test.running_test',
            'trino_host': 'sep.sdp-dev.pd.switchnet.nv',
            'trino_port': '443',
            'trino_user': 'test_user',
            'trino_password': None,
            'http_scheme': 'https',
            'trino_role': 'sysadmin',
            'trino_catalog': 'iceberg',
            'trino_schema': 'test',
            'hive_metastore_uri': 'localhost:9083',
            'delimiter': ',',
            'has_header': 'true',
            'quote_char': '"',
            'batch_size': '100',
            'mode': 'append',
            'sample_size': '1000',
            'verbose': 'false'
        },
        'status': 'running',
        'stdout': 'Mock conversion in progress...',
        'stderr': '',
        'error': None,
        'returncode': None,
        'started_at': now,
        'progress': 0  # Start with 0% progress
    }
    
    # Start a thread to simulate progress updates with improved reliability
    def simulate_progress():
        try:
            # Log initial state
            logger.info(f"Starting progress simulation for job {job_id}")
            logger.info(f"Initial job state: {conversion_jobs[job_id]}")
            
            # Progress simulation phases
            phases = [
                (0, "Initializing..."),
                (5, "Connecting to Trino..."),
                (10, "Connected to Trino."),
                (15, "Connecting to Hive Metastore..."),
                (20, "Connected to Hive Metastore."),
                (25, "Analyzing CSV file..."),
                (30, "Inferring schema..."),
                (40, "Schema inferred."),
                (45, "Creating table..."),
                (50, "Table created."),
                (55, "Starting data transfer..."),
                (60, "Processing batch 1..."),
                (70, "Processing batch 2..."),
                (80, "Processing batch 3..."),
                (90, "Finalizing..."),
                (95, "Validation checks..."),
                (100, "Completed.")
            ]
            
            # Simulate each phase of the progress
            for progress, message in phases:
                if job_id not in conversion_jobs:
                    logger.warning(f"Job {job_id} no longer exists, stopping simulation")
                    return
                    
                # Update the progress and add a message to stdout
                conversion_jobs[job_id]['progress'] = progress
                conversion_jobs[job_id]['stdout'] += f"\n{message} ({progress}%)"
                
                # Log progress update (use info level for better visibility)
                logger.info(f"Updated progress for job {job_id} to {progress}% - {message}")
                
                # Sleep for a bit to simulate processing time (more realistic timing)
                time.sleep(1.5)  # Slow down a bit for better visibility
            
            # Mark as completed when done
            if job_id in conversion_jobs:
                conversion_jobs[job_id]['status'] = 'completed'
                conversion_jobs[job_id]['completed_at'] = datetime.datetime.now()
                conversion_jobs[job_id]['progress'] = 100
                conversion_jobs[job_id]['stdout'] += "\nCSV to Iceberg conversion completed successfully."
                logger.info(f"Completed job {job_id}, final state: {conversion_jobs[job_id]}")
            
        except Exception as e:
            logger.error(f"Error in progress simulation: {str(e)}", exc_info=True)
            if job_id in conversion_jobs:
                conversion_jobs[job_id]['status'] = 'failed'
                conversion_jobs[job_id]['error'] = str(e)
                conversion_jobs[job_id]['completed_at'] = datetime.datetime.now()
    
    # Start the simulation thread
    thread = threading.Thread(target=simulate_progress, daemon=True)
    thread.start()
    logger.info(f"Started progress simulation thread for job {job_id}")
    
    flash(f'Running test job created with ID: {job_id}', 'success')
    return redirect(url_for('test_progress', job_id=job_id))

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Run the app
    app.run(host='0.0.0.0', port=5000, debug=True)