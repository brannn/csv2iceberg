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
from csv_to_iceberg.utils import setup_logging
from csv_to_iceberg.storage.config_manager import ConfigManager

# LMDB is our permanent storage solution
USE_LMDB = True
LMDB_IMPORTED = False

try:
    from csv_to_iceberg.storage.lmdb_config_manager import LMDBConfigManager
    LMDB_IMPORTED = True
    logger = logging.getLogger("csv_to_iceberg")
    logger.info("Using LMDB for configuration storage")
except ImportError:
    USE_LMDB = False
    logger = logging.getLogger("csv_to_iceberg")
    logger.warning("Failed to import LMDB modules, falling back to JSON storage as a last resort")

# Set up logging
logger = setup_logging()
# Enable more verbose Flask logging
logging.basicConfig(level=logging.DEBUG)

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "development-secret-key")

# Initialize config manager
if USE_LMDB:
    config_manager = LMDBConfigManager()
else:
    config_manager = ConfigManager()

# Create a before_request handler to set session variables
@app.before_request
def set_session_defaults():
    """Set default session variables."""
    # Always set USE_LMDB to True as it's our permanent storage solution
    session['USE_LMDB'] = True

# Configure upload folder
UPLOAD_FOLDER = tempfile.mkdtemp()
ALLOWED_EXTENSIONS = {'csv', 'txt'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB limit

# Import job manager for job storage with LMDB support
from csv_to_iceberg.storage.job_manager import job_manager

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

# We use the job_manager for job storage instead of in-memory OrderedDict

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
            "--trino-schema", params['trino_schema']
        ]
        
        # Add Hive Metastore parameters
        if params.get('use_hive_metastore'):
            # If Hive is enabled, add the URI and enable flag
            conversion_cmd.append("--use-hive-metastore")
            if params.get('hive_metastore_uri'):
                conversion_cmd.extend(["--hive-metastore-uri", params['hive_metastore_uri']])
        else:
            # If Hive is disabled, add the no-hive-metastore flag
            conversion_cmd.append("--no-hive-metastore")
        
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
            
        # Add column filtering parameters if provided
        if params.get('include_columns'):
            # Sanitize and format: split, strip, and join with commas
            include_cols = ','.join([col.strip() for col in params['include_columns'].split(',') if col.strip()])
            if include_cols:
                conversion_cmd.extend(["--include-columns", include_cols])
                logger.info(f"Adding include columns filter: {include_cols}")
                
        # Exclude columns are only processed if include columns are not specified
        elif params.get('exclude_columns'):
            # Sanitize and format: split, strip, and join with commas
            exclude_cols = ','.join([col.strip() for col in params['exclude_columns'].split(',') if col.strip()])
            if exclude_cols:
                conversion_cmd.extend(["--exclude-columns", exclude_cols])
                logger.info(f"Adding exclude columns filter: {exclude_cols}")
                
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
        
        # Store results using job manager
        status = 'completed' if process.returncode == 0 else 'failed'
        
        # Mark job as completed
        job_manager.mark_job_completed(
            job_id, 
            success=(status == 'completed'),
            stdout=process.stdout,
            stderr=process.stderr,
            returncode=process.returncode
        )
        
        logger.info(f"Completed conversion job {job_id} with status: {status}")
        
        # Clean up temporary files
        if custom_schema_file:
            try:
                os.unlink(custom_schema_file)
                logger.info(f"Removed temporary schema file: {custom_schema_file}")
            except Exception as clean_err:
                logger.warning(f"Failed to clean up schema file {custom_schema_file}: {str(clean_err)}")
        
    except Exception as e:
        logger.error(f"Error in conversion job {job_id}: {str(e)}", exc_info=True)
        # Mark job as failed using job manager
        job_manager.mark_job_completed(job_id, success=False, error=str(e))
        
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
        
        # Define csv_params at this scope
        csv_params = {}
        file_path = None
        filename = None

        # Check if we're coming from the schema preview page
        if 'file_path' in request.form and os.path.exists(request.form['file_path']):
            # File already exists (from schema preview flow)
            file_path = request.form['file_path']
            filename = os.path.basename(file_path)
            logger.debug(f"Using existing file from schema preview: {file_path}")
            
            # Extract CSV parameters from the form
            csv_params = {
                'delimiter': request.form.get('delimiter', ','),
                'has_header': request.form.get('has_header', 'true') == 'true',
                'quote_char': request.form.get('quote_char', '"'),
                'sample_size': int(request.form.get('sample_size', '1000'))
            }
        else:
            # Normal file upload flow
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
            if not (file and allowed_file(file.filename)):
                logger.error(f"File type not allowed: {file.filename}")
                flash('File type not allowed', 'error')
                return redirect(request.url)
                
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
                    'timestamp': datetime.datetime.now().isoformat(),
                    'include_columns': request.form.get('include_columns', ''),
                    'exclude_columns': request.form.get('exclude_columns', '')
                }
                logger.debug(f"Created schema preview session: {session['schema_preview']}")
                return redirect(url_for('schema_preview'))
        
        # At this point, we have a valid file_path and filename, plus csv_params
        # Proceed with the conversion flow
        
        # Create job ID
        job_id = os.urandom(8).hex()
        logger.debug(f"Created job ID: {job_id}")
        
        # Check if a profile was selected and use its values
        profile_name = request.form.get('connection_profile')
        connection_params = {}
        
        if profile_name:
            # Get the profile
            profile = config_manager.get_profile(profile_name)
            if profile:
                logger.debug(f"Using connection profile: {profile_name}")
                # Set the profile as last used
                config_manager.set_last_used_profile(profile_name)
                
                # Use the profile values for connection settings
                connection_params = {
                    'trino_host': profile.get('trino_host'),
                    'trino_port': str(profile.get('trino_port')),
                    'trino_user': profile.get('trino_user'),
                    'trino_password': profile.get('trino_password'),
                    'http_scheme': profile.get('http_scheme'),
                    'trino_role': profile.get('trino_role'),
                    'trino_catalog': profile.get('trino_catalog'),
                    'trino_schema': profile.get('trino_schema'),
                    'hive_metastore_uri': profile.get('hive_metastore_uri'),
                    'use_hive_metastore': profile.get('use_hive_metastore', False)
                }
                logger.debug(f"Applied connection parameters from profile: {profile_name}")
            else:
                logger.warning(f"Selected profile '{profile_name}' not found, using form values instead")
        
        # Use form values as defaults or fallbacks if no profile is selected
        params = {
            'table_name': request.form.get('table_name'),
            'trino_host': connection_params.get('trino_host') or request.form.get('trino_host'),
            'trino_port': connection_params.get('trino_port') or request.form.get('trino_port'),
            'trino_user': connection_params.get('trino_user') or request.form.get('trino_user'),
            'trino_password': connection_params.get('trino_password') or request.form.get('trino_password'),
            'http_scheme': connection_params.get('http_scheme') or request.form.get('http_scheme', 'https'),
            'trino_role': connection_params.get('trino_role') or request.form.get('trino_role', 'sysadmin'),
            'trino_catalog': connection_params.get('trino_catalog') or request.form.get('trino_catalog'),
            'trino_schema': connection_params.get('trino_schema') or request.form.get('trino_schema'),
            'hive_metastore_uri': connection_params.get('hive_metastore_uri') or request.form.get('hive_metastore_uri'),
            'use_hive_metastore': connection_params.get('use_hive_metastore') if 'use_hive_metastore' in connection_params else (request.form.get('use_hive_metastore', 'false') == 'true'),
            'delimiter': csv_params.get('delimiter', ','),
            'has_header': 'true' if csv_params.get('has_header', True) else 'false',
            'quote_char': csv_params.get('quote_char', '"'),
            'batch_size': request.form.get('batch_size'),
            'mode': request.form.get('mode', 'append'),
            'sample_size': str(csv_params.get('sample_size', 1000)),
            'verbose': request.form.get('verbose', 'false'),
            'include_columns': request.form.get('include_columns', ''),
            'exclude_columns': request.form.get('exclude_columns', '')
        }
        logger.debug(f"Collected parameters: {params}")
        
        # Apply schema customizations if they exist in the session
        if 'schema_customization' in session:
            logger.debug("Found schema customization in session")
            # This will be implemented in the run_conversion function
            params['schema_customization'] = session['schema_customization']
            # Clean up the session
            session.pop('schema_customization', None)
        
        # Create job using job manager
        job_data = job_manager.create_job(job_id, {
            'file_path': file_path,
            'filename': filename,
            'params': params,
            'is_test': False  # This is a real job, not a test one
        })
        
        # Update the job to mark it as running
        job_manager.update_job(job_id, {
            'status': 'running',
            'started_at': datetime.datetime.now(),
        })
        
        logger.debug(f"Created job entry: {job_data}")
        
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
            
    # GET request - show conversion form
    logger.debug("Rendering convert.html template")
    
    # Get available profiles and last used profile
    profiles = config_manager.get_profiles()
    last_used_profile = None
    last_used = config_manager.get_last_used_profile()
    if last_used:
        last_used_profile = last_used.get('name')
    
    return render_template(
        'convert.html',
        profiles=profiles,
        last_used_profile=last_used_profile
    )

# The handle_schema_analyze function has been removed as it's no longer needed

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
        
        # Process column filtering parameters if they exist in the session
        include_cols = None
        exclude_cols = None
        
        if 'include_columns' in session.get('schema_preview', {}) and session['schema_preview']['include_columns']:
            include_cols = [col.strip() for col in session['schema_preview']['include_columns'].split(',') if col.strip()]
            logger.info(f"Using include columns filter from session: {include_cols}")
            
        if not include_cols and 'exclude_columns' in session.get('schema_preview', {}) and session['schema_preview']['exclude_columns']:
            exclude_cols = [col.strip() for col in session['schema_preview']['exclude_columns'].split(',') if col.strip()]
            logger.info(f"Using exclude columns filter from session: {exclude_cols}")
        
        schema = infer_schema_from_csv(
            file_path,
            delimiter=csv_params['delimiter'],
            has_header=csv_params['has_header'],
            quote_char=csv_params['quote_char'],
            sample_size=csv_params['sample_size'],
            include_columns=include_cols,
            exclude_columns=exclude_cols
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
        
        # Get profiles for the template
        profiles = config_manager.get_profiles()
        last_used_profile = None
        last_used = config_manager.get_last_used_profile()
        if last_used:
            last_used_profile = last_used.get('name')
            
        # Pass the parameters to the template
        return render_template(
            'convert_final.html',
            filename=preview_data['filename'],
            file_path=preview_data['file_path'],
            csv_params=preview_data['csv_params'],
            schema_preview=True,  # Flag to indicate we came from schema preview
            profiles=profiles,
            last_used_profile=last_used_profile
        )
        
    except Exception as e:
        logger.error(f"Error applying schema customization: {str(e)}", exc_info=True)
        flash(f'Error applying schema customization: {str(e)}', 'error')
        return redirect(url_for('schema_preview'))

@app.route('/job/<job_id>')
def job_status(job_id):
    """Show the status of a conversion job."""
    logger.info(f"Job status requested for job ID: {job_id}")
    
    # Clean up old jobs from memory
    job_manager.cleanup_old_jobs()
    
    # Get job from job manager
    job = job_manager.get_job(job_id)
    logger.info(f"Job retrieval result: {'Found' if job else 'Not found'}")
    
    if not job:
        # Get list of all available job IDs for debugging
        all_jobs = job_manager.get_all_jobs(include_test_jobs=True)
        job_ids = [j.get('id', '') for j in all_jobs]
        logger.info(f"Available job IDs: {job_ids}")
        
        flash('Job not found or has been archived', 'info')
        return redirect(url_for('jobs'))
    
    # Mark this job as being actively viewed (to prevent premature cleanup)
    job_manager.mark_job_as_active(job_id)
    
    now = datetime.datetime.now()
    return render_template('job_status.html', 
                          job=job, 
                          job_id=job_id,
                          now=now,
                          format_duration=format_duration)

@app.route('/test/progress/<job_id>')
def test_progress(job_id):
    """Test page for monitoring job progress."""
    # Clean up old jobs first
    job_manager.cleanup_old_jobs()
    
    # Get job from job manager
    job = job_manager.get_job(job_id)
    if not job:
        flash('Job not found or has been archived', 'info')
        return redirect(url_for('jobs'))
    
    # Mark this job as being actively viewed
    job_manager.mark_job_as_active(job_id)
    
    return render_template('test_progress.html',
                          job=job,
                          job_id=job_id)

@app.route('/jobs')
def jobs():
    """List all conversion jobs."""
    # Clean up old jobs first
    job_manager.cleanup_old_jobs()
    
    # Get all jobs from job manager
    all_jobs = job_manager.get_all_jobs(include_test_jobs=True)
    
    # Mark all jobs as active when viewing the list
    for job in all_jobs:
        job_manager.mark_job_as_active(job.get('id'))
        
    now = datetime.datetime.now()
    return render_template('jobs.html', 
                          jobs=all_jobs, 
                          now=now, 
                          format_duration=format_duration)

@app.route('/job/<job_id>/progress/<int:percent>', methods=['POST'])
def update_job_progress(job_id, percent):
    """Update the progress of a conversion job."""
    logger.debug(f"Progress update for job {job_id}: {percent}%")
    if job_manager.update_job_progress(job_id, percent):
        return jsonify({"status": "ok"})
    else:
        return jsonify({"status": "error", "message": "Job not found"}), 404

# DEPRECATED: The functions mark_job_as_active and cleanup_old_jobs have been 
# replaced by job_manager methods. They are kept for reference but are no longer used.

@app.route('/job/<job_id>/progress', methods=['GET'])
def get_job_progress(job_id):
    """Get the current progress of a conversion job."""
    # Clean up old jobs first (except the one being viewed)
    job_manager.cleanup_old_jobs()
    
    # Get job from job manager
    job = job_manager.get_job(job_id)
    
    if job:
        # Mark this job as being actively viewed during progress polling
        job_manager.mark_job_as_active(job_id)
        
        # Get current progress value and log it
        current_progress = job.get('progress', 0)
        current_status = job.get('status', 'unknown')
        current_phase = _get_progress_phase(current_progress)
        
        logger.debug(f"Progress for job {job_id}: {current_progress}%, status: {current_status}, phase: {current_phase}")
        
        return jsonify({
            "job_id": job_id, 
            "progress": current_progress,
            "status": current_status,
            # Include extra info for UI improvements
            "phase": current_phase,
            "test_job": job_id.startswith('test_') or job_id.startswith('running_test_')
        })
    else:
        # Check if there are any jobs with similar ID prefix
        all_jobs = job_manager.get_all_jobs()
        job_ids = [j.get('id', '') for j in all_jobs]
        was_archived = any(job_id.startswith(jid[:10]) or jid.startswith(job_id[:10]) for jid in job_ids)
        
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
        # Get all jobs from job manager
        all_jobs = job_manager.get_all_jobs(include_test_jobs=True)
        
        # Count jobs by status
        status_counts = {}
        for job in all_jobs:
            status = job.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
            
        # Count test jobs
        test_job_count = sum(1 for job in all_jobs if 
                          job.get('id', '').startswith('test_') or 
                          job.get('id', '').startswith('running_test_'))
        
        # Get storage information
        from job_manager import USE_LMDB_JOBS
        storage_type = "LMDB" if USE_LMDB_JOBS else "Memory"
        
        data = {
            "status": "ok",
            "jobs_count": len(all_jobs),
            "jobs_by_status": status_counts,
            "test_job_count": test_job_count,
            "job_storage": {
                "type": storage_type,
                "job_manager_class": job_manager.__class__.__name__
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
    
    # Create job using job manager
    job_data = job_manager.create_job(job_id, {
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
        'is_test': True
    })
    
    # Set job as completed
    job_manager.mark_job_completed(
        job_id,
        success=True,
        stdout='Mock conversion completed successfully.',
        stderr='',
        returncode=0
    )
    
    # Update job timestamps for testing
    job_manager.update_job(job_id, {
        'started_at': ten_minutes_ago,
        'completed_at': now,
        'progress': 100
    })
    
    flash(f'Test job created with ID: {job_id}', 'success')
    return redirect(url_for('jobs'))

@app.route('/api_test')
def api_test():
    """Simple API testing page to verify progress updates."""
    test_job_id = "test_api_" + os.urandom(4).hex()
    
    # Create a simple test job using job manager
    job_data = job_manager.create_job(test_job_id, {
        'filename': 'test.csv',
        'params': {'table_name': 'test_table'},
        'is_test': True
    })
    
    # Update the job to set progress and status
    job_manager.update_job(test_job_id, {
        'status': 'running',
        'progress': 50,  # Set to 50% for testing
        'started_at': datetime.datetime.now()
    })
    
    # Create HTML using normal string formatting instead of f-string
    html = '''
    <html>
    <head><title>API Test</title></head>
    <body>
        <h1>API Test Page</h1>
        <p>Testing job ID: {job_id}</p>
        <p>Progress: 50%</p>
        
        <button onclick="testProgressAPI()">Test Progress API</button>
        <button onclick="testProgressUpdate()">Test Progress Update</button>
        
        <pre id="result">Click a button to test...</pre>
        
        <script>
        async function testProgressAPI() {
            try {
                const response = await fetch('/job/{job_id}/progress');
                const data = await response.json();
                document.getElementById('result').textContent = 
                    'Progress API Response:\n' + JSON.stringify(data, null, 2);
            } catch (error) {
                document.getElementById('result').textContent = 
                    'Error: ' + error.message;
            }
        }
        
        async function testProgressUpdate() {
            try {
                const newProgress = 75;
                const response = await fetch('/job/{job_id}/progress/' + newProgress, {
                    method: 'POST'
                });
                const data = await response.json();
                document.getElementById('result').textContent = 
                    'Update API Response:\n' + JSON.stringify(data, null, 2) + 
                    '\n\nNow checking current progress...';
                    
                // Now check if it was updated
                const checkResponse = await fetch('/job/{job_id}/progress');
                const checkData = await checkResponse.json();
                document.getElementById('result').textContent += 
                    '\n\nCurrent Progress:\n' + JSON.stringify(checkData, null, 2);
            } catch (error) {
                document.getElementById('result').textContent = 
                    'Error: ' + error.message;
            }
        }
        </script>
    </body>
    </html>
    '''.format(job_id=test_job_id)
    
    return html

@app.route('/add_running_test_job')
def add_running_test_job():
    """Add a test job with running status for testing progress updates."""
    job_id = "running_test_" + os.urandom(4).hex()
    
    # Create a mock job with running status and timestamps
    now = datetime.datetime.now()
    
    # Create job using job manager
    job_data = job_manager.create_job(job_id, {
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
        'is_test': True
    })
    
    # Update the job to mark it as running
    job_manager.update_job(job_id, {
        'status': 'running',
        'started_at': now,
        'stdout': 'Mock conversion in progress...',
        'stderr': '',
        'error': None,
        'returncode': None,
        'progress': 0  # Start with 0% progress
    })
    
    # Start a thread to simulate progress updates with improved reliability
    def simulate_progress():
        try:
            # Log initial state
            logger.info(f"Starting progress simulation for job {job_id}")
            
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
                # Get the current job to check if it still exists
                job = job_manager.get_job(job_id)
                if not job:
                    logger.warning(f"Job {job_id} no longer exists, stopping simulation")
                    return
                
                # Update job progress
                job_manager.update_job_progress(job_id, progress)
                
                # Append message to stdout
                current_stdout = job.get('stdout', '')
                job_manager.update_job(job_id, {
                    'stdout': current_stdout + f"\n{message} ({progress}%)"
                })
                
                # Log progress update
                logger.info(f"Updated progress for job {job_id} to {progress}% - {message}")
                
                # Sleep for a bit to simulate processing time
                time.sleep(1.5)
            
            # Mark as completed when done
            job = job_manager.get_job(job_id)
            if job:
                current_stdout = job.get('stdout', '')
                job_manager.mark_job_completed(
                    job_id,
                    success=True,
                    stdout=current_stdout + "\nCSV to Iceberg conversion completed successfully.",
                    stderr='',
                    returncode=0
                )
                logger.info(f"Completed job {job_id}")
            
        except Exception as e:
            logger.error(f"Error in progress simulation: {str(e)}", exc_info=True)
            job = job_manager.get_job(job_id)
            if job:
                job_manager.mark_job_completed(
                    job_id,
                    success=False,
                    error=str(e)
                )
    
    # Start the simulation thread
    thread = threading.Thread(target=simulate_progress, daemon=True)
    thread.start()
    logger.info(f"Started progress simulation thread for job {job_id}")
    
    flash(f'Running test job created with ID: {job_id}', 'success')
    return redirect(url_for('test_progress', job_id=job_id))

@app.route('/demo/schema_preview')
def demo_schema_preview():
    """Demo endpoint for schema preview feature."""
    # Set up a session with sample data for the schema preview
    sample_csv_path = '/tmp/csv_test/sample_data.csv'
    if not os.path.exists(sample_csv_path):
        flash('Sample data file not found. Please run the setup script first.', 'error')
        return redirect(url_for('convert'))
    
    # Extract CSV parameters
    csv_params = {
        'delimiter': ',',
        'has_header': True,
        'quote_char': '"',
        'sample_size': 1000
    }
    
    # Store in session for the schema preview page
    session['schema_preview'] = {
        'file_path': sample_csv_path,
        'filename': 'sample_data.csv',
        'csv_params': csv_params,
        'timestamp': datetime.datetime.now().isoformat()
    }
    
    # Redirect to schema preview
    return redirect(url_for('schema_preview'))

# Connection Profiles Management Routes
@app.route('/profiles')
def profiles():
    """List all connection profiles."""
    profiles_list = config_manager.get_profiles()
    last_used = config_manager.get_last_used_profile()
    last_used_name = last_used.get('name') if last_used else None
    
    return render_template('profiles.html', profiles=profiles_list, last_used=last_used_name)

@app.route('/profiles/add', methods=['GET', 'POST'])
def profile_add():
    """Add a new connection profile."""
    if request.method == 'POST':
        profile = {
            "name": request.form.get('name'),
            "description": request.form.get('description', ''),
            "trino_host": request.form.get('trino_host'),
            "trino_port": int(request.form.get('trino_port', 443)),
            "trino_user": request.form.get('trino_user', ''),
            "trino_password": request.form.get('trino_password', ''),
            "http_scheme": request.form.get('http_scheme', 'https'),
            "trino_role": request.form.get('trino_role', 'sysadmin'),
            "trino_catalog": request.form.get('trino_catalog'),
            "trino_schema": request.form.get('trino_schema'),
            "use_hive_metastore": request.form.get('use_hive_metastore') == 'true',
            "hive_metastore_uri": request.form.get('hive_metastore_uri', '')
        }
        
        if config_manager.add_profile(profile):
            flash(f"Profile '{profile['name']}' created successfully.", 'success')
            return redirect(url_for('profiles'))
        else:
            flash(f"Failed to create profile '{profile['name']}'.", 'error')
    
    return render_template('profile_form.html', profile=None)

@app.route('/profiles/edit/<name>', methods=['GET', 'POST'])
def profile_edit(name):
    """Edit an existing connection profile."""
    profile = config_manager.get_profile(name)
    
    if not profile:
        flash(f"Profile '{name}' not found.", 'error')
        return redirect(url_for('profiles'))
    
    if request.method == 'POST':
        updated_profile = {
            "name": name,  # Keep the original name
            "description": request.form.get('description', ''),
            "trino_host": request.form.get('trino_host'),
            "trino_port": int(request.form.get('trino_port', 443)),
            "trino_user": request.form.get('trino_user', ''),
            "trino_password": request.form.get('trino_password') or profile.get('trino_password', ''),  # Keep existing password if not provided
            "http_scheme": request.form.get('http_scheme', 'https'),
            "trino_role": request.form.get('trino_role', 'sysadmin'),
            "trino_catalog": request.form.get('trino_catalog'),
            "trino_schema": request.form.get('trino_schema'),
            "use_hive_metastore": request.form.get('use_hive_metastore') == 'true',
            "hive_metastore_uri": request.form.get('hive_metastore_uri', '')
        }
        
        if config_manager.update_profile(name, updated_profile):
            flash(f"Profile '{name}' updated successfully.", 'success')
            return redirect(url_for('profiles'))
        else:
            flash(f"Failed to update profile '{name}'.", 'error')
    
    return render_template('profile_form.html', profile=profile)

@app.route('/profiles/delete/<name>')
def profile_delete(name):
    """Delete a connection profile."""
    if config_manager.delete_profile(name):
        flash(f"Profile '{name}' deleted successfully.", 'success')
    else:
        flash(f"Failed to delete profile '{name}'.", 'error')
    
    return redirect(url_for('profiles'))

@app.route('/profiles/use/<name>')
def profile_use(name):
    """Set a profile as the active profile."""
    if config_manager.set_last_used_profile(name):
        flash(f"Profile '{name}' is now the active profile.", 'success')
    else:
        flash(f"Failed to set profile '{name}' as active.", 'error')
    
    return redirect(url_for('profiles'))

@app.route('/admin/storage/status')
def storage_status():
    """Show the current storage status."""
    # Get profile names
    profile_names = [p['name'] for p in config_manager.get_profiles()]
    
    # Get information about temp and cache directories
    temp_dir = tempfile.gettempdir()
    home_dir = os.path.expanduser("~")
    
    # Get path constants
    from lmdb_config_manager import DEFAULT_LMDB_PATH
    
    status = {
        "storage_type": "LMDB",
        "session_storage_type": "LMDB",
        "lmdb_available": True,
        "current_profiles": profile_names,
        "current_profiles_count": len(profile_names),
        "other_storage_profiles": [],
        "environment_variable": "true",
        "temp_directory": temp_dir,
        "home_directory": home_dir,
        "lmdb_config_path": os.path.expanduser(DEFAULT_LMDB_PATH),
        "json_config_path": "Not used"
    }
    
    return render_template('storage_status.html', status=status)

# Storage toggle route has been removed as LMDB is now the permanent storage solution

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Run the app
    app.run(host='0.0.0.0', port=5000, debug=True)