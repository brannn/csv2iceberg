import os
import logging
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
import tempfile
import subprocess
import threading
from werkzeug.utils import secure_filename

# Import helper modules
from utils import setup_logging
from config_manager import ConfigManager

# Set up logging
logger = setup_logging()

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "development-secret-key")

# Initialize config manager
config_manager = ConfigManager()

# Configure upload folder
UPLOAD_FOLDER = tempfile.mkdtemp()
ALLOWED_EXTENSIONS = {'csv', 'txt'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB limit

# Store conversion jobs
conversion_jobs = {}

def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def run_conversion(job_id, file_path, params):
    """Run the CSV to Iceberg conversion as a background task."""
    try:
        # Build command from parameters
        cmd = [
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
            cmd.extend(["--trino-user", params['trino_user']])
        if params.get('trino_password'):
            cmd.extend(["--trino-password", params['trino_password']])
        
        # Add optional parameters
        if params.get('delimiter'):
            cmd.extend(["--delimiter", params['delimiter']])
        if params.get('has_header') == 'false':
            cmd.append('--no-has-header')
        if params.get('quote_char'):
            cmd.extend(["--quote-char", params['quote_char']])
        if params.get('batch_size'):
            cmd.extend(["--batch-size", params['batch_size']])
        if params.get('mode'):
            cmd.extend(["--mode", params['mode']])
        if params.get('sample_size'):
            cmd.extend(["--sample-size", params['sample_size']])
        if params.get('verbose') == 'true':
            cmd.append('--verbose')
        
        # Add partition specifications if provided
        if params.get('partition_spec') and len(params['partition_spec']) > 0:
            for spec in params['partition_spec']:
                cmd.extend(["--partition-by", spec])
            
        # Run the conversion process
        logger.info(f"Starting conversion job {job_id} with command: {' '.join(cmd)}")
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        # Store results
        conversion_jobs[job_id]['status'] = 'completed' if process.returncode == 0 else 'failed'
        conversion_jobs[job_id]['stdout'] = process.stdout
        conversion_jobs[job_id]['stderr'] = process.stderr
        conversion_jobs[job_id]['returncode'] = process.returncode
        
        logger.info(f"Completed conversion job {job_id} with status: {conversion_jobs[job_id]['status']}")
        
    except Exception as e:
        logger.error(f"Error in conversion job {job_id}: {str(e)}", exc_info=True)
        conversion_jobs[job_id]['status'] = 'failed'
        conversion_jobs[job_id]['error'] = str(e)

@app.route('/')
def index():
    """Render the home page."""
    return render_template('index.html')

@app.route('/convert', methods=['GET', 'POST'])
def convert():
    """Handle CSV to Iceberg conversion."""
    logger.info(f"Convert route called with method: {request.method}")
    if request.method == 'GET':
        # Get profile name from query parameter if provided
        profile_name = request.args.get('profile')
        profile_data = {}
        
        # If a profile is specified, load its data
        if profile_name and config_manager.profile_exists(profile_name):
            profile_data = config_manager.get_profile(profile_name)
            conn_settings = profile_data.get('connection', {})
            default_settings = profile_data.get('defaults', {})
            partition_settings = profile_data.get('partitioning', {})
        else:
            conn_settings = {}
            default_settings = {}
            partition_settings = {'enabled': False, 'specs': []}
        
        # Pass config_manager and profile data to the template
        return render_template('convert.html', 
                               config_manager=config_manager,
                               profile_name=profile_name,
                               conn_settings=conn_settings,
                               default_settings=default_settings,
                               partition_settings=partition_settings)
    
    if request.method == 'POST':
        logger.info("POST request received in convert route")
        logger.info(f"Form data keys: {list(request.form.keys())}")
        logger.info(f"Files keys: {list(request.files.keys())}")
        logger.info(f"Request content type: {request.content_type}")
        logger.info(f"Request content length: {request.content_length}")
        
        # Check if a file was uploaded
        if 'csv_file' not in request.files:
            logger.error("No file part in request")
            flash('No file part', 'error')
            return redirect(request.url)
            
        file = request.files['csv_file']
        
        # Check if the file is empty
        if file.filename == '':
            flash('No selected file', 'error')
            return redirect(request.url)
            
        # Check if the file is allowed
        if file and allowed_file(file.filename):
            # Save the file
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            # Create job ID
            job_id = os.urandom(8).hex()
            logger.info(f"Created job ID: {job_id}")
            
            # Check if using a profile
            profile_name = request.form.get('profile')
            profile_data = {}
            
            if profile_name:
                if config_manager.profile_exists(profile_name):
                    profile_data = config_manager.get_profile(profile_name)
                    logger.info(f"Using configuration profile: {profile_name}")
                else:
                    flash(f'Profile "{profile_name}" not found', 'error')
                    return redirect(request.url)
            
            # Get form parameters, falling back to profile settings where needed
            conn_settings = profile_data.get('connection', {}) if profile_data else {}
            default_settings = profile_data.get('defaults', {}) if profile_data else {}
            
            # Collect parameters from form or fall back to profile settings
            params = {
                # Table name always comes from the form
                'table_name': request.form.get('table_name'),
                
                # Connection parameters - fall back to profile settings
                'trino_host': request.form.get('trino_host') or conn_settings.get('trino_host'),
                'trino_port': request.form.get('trino_port') or str(conn_settings.get('trino_port', '')),
                'trino_user': request.form.get('trino_user') or conn_settings.get('trino_user'),
                'trino_password': request.form.get('trino_password') or conn_settings.get('trino_password'),
                'trino_catalog': request.form.get('trino_catalog') or conn_settings.get('trino_catalog'),
                'trino_schema': request.form.get('trino_schema') or conn_settings.get('trino_schema'),
                'hive_metastore_uri': request.form.get('hive_metastore_uri') or conn_settings.get('hive_metastore_uri'),
                
                # CSV settings - fall back to profile settings
                'delimiter': request.form.get('delimiter') or default_settings.get('delimiter', ','),
                'has_header': request.form.get('has_header') or str(default_settings.get('has_header', True)).lower(),
                'quote_char': request.form.get('quote_char') or default_settings.get('quote_char', '"'),
                'batch_size': request.form.get('batch_size') or str(default_settings.get('batch_size', '')),
                'mode': request.form.get('mode') or default_settings.get('mode', 'append'),
                'sample_size': request.form.get('sample_size') or str(default_settings.get('sample_size', '')),
                'verbose': request.form.get('verbose') or str(default_settings.get('verbose', False)).lower(),
            }
            
            # Handle partitioning
            partition_spec = request.form.getlist('partition_spec')
            
            # If no partitioning specified in form but profile has partitioning enabled,
            # use profile partition specs
            if (not partition_spec or len(partition_spec) == 0) and profile_data:
                if profile_data.get('partitioning', {}).get('enabled', False):
                    partition_spec = profile_data.get('partitioning', {}).get('specs', [])
                    logger.info(f"Using partition specs from profile: {partition_spec}")
            
            params['partition_spec'] = partition_spec
            
            # Create job
            conversion_jobs[job_id] = {
                'file_path': file_path,
                'filename': filename,
                'params': params,
                'status': 'running',
                'stdout': '',
                'stderr': '',
                'error': None,
                'returncode': None
            }
            
            # Start conversion thread
            logger.info(f"Starting conversion thread for job {job_id}")
            thread = threading.Thread(
                target=run_conversion,
                args=(job_id, file_path, params)
            )
            thread.daemon = True
            thread.start()
            
            # Redirect to job status page
            redirect_url = url_for('job_status', job_id=job_id)
            logger.info(f"Redirecting to job status page: {redirect_url}")
            return redirect(redirect_url)
            
        else:
            flash('File type not allowed', 'error')
            return redirect(request.url)

@app.route('/job/<job_id>')
def job_status(job_id):
    """Show the status of a conversion job."""
    logger.info(f"Job status route called for job ID: {job_id}")
    
    # Check if job exists
    if job_id not in conversion_jobs:
        logger.error(f"Job ID {job_id} not found in conversion_jobs dictionary")
        logger.info(f"Available jobs: {list(conversion_jobs.keys())}")
        flash('Job not found', 'error')
        return redirect(url_for('index'))
    
    # Job exists, render the template with job details
    logger.info(f"Rendering job status for job ID: {job_id}, status: {conversion_jobs[job_id]['status']}")
    return render_template('job_status.html', job=conversion_jobs[job_id], job_id=job_id)

@app.route('/jobs')
def jobs():
    """List all conversion jobs."""
    return render_template('jobs.html', jobs=conversion_jobs)

@app.route('/save_profile', methods=['POST'])
def save_profile():
    """Save current form settings as a profile."""
    profile_name = request.form.get('profile_name')
    
    if not profile_name:
        return jsonify({'success': False, 'error': 'Profile name is required'})
    
    # Check if profile already exists
    if config_manager.profile_exists(profile_name):
        return jsonify({'success': False, 'error': f'Profile "{profile_name}" already exists. Please choose a different name.'})
    
    # Prepare connection settings
    connection = {
        'trino_host': request.form.get('trino_host'),
        'trino_port': int(request.form.get('trino_port')) if request.form.get('trino_port') and request.form.get('trino_port').isdigit() else 8080,
        'trino_user': request.form.get('trino_user'),
        'trino_catalog': request.form.get('trino_catalog'),
        'trino_schema': request.form.get('trino_schema'),
        'hive_metastore_uri': request.form.get('hive_metastore_uri')
    }
    
    # Include password only if provided (don't store empty string)
    if request.form.get('trino_password'):
        connection['trino_password'] = request.form.get('trino_password')
    
    # Prepare default settings
    defaults = {
        'delimiter': request.form.get('delimiter', ','),
        'quote_char': request.form.get('quote_char', '"'),
        'has_header': request.form.get('has_header') == 'true',
        'mode': request.form.get('mode', 'append'),
        'verbose': request.form.get('verbose') == 'true'
    }
    
    # Handle numeric values
    batch_size = request.form.get('batch_size')
    if batch_size and batch_size.isdigit():
        defaults['batch_size'] = int(batch_size)
        
    sample_size = request.form.get('sample_size')
    if sample_size and sample_size.isdigit():
        defaults['sample_size'] = int(sample_size)
    
    # Prepare partitioning settings
    has_partitioning = request.form.get('has_partitioning') == 'true'
    partition_specs = request.form.getlist('partition_specs')
    
    partitioning = {
        'enabled': has_partitioning and len(partition_specs) > 0,
        'specs': partition_specs if partition_specs else []
    }
    
    # Create profile settings
    profile_settings = {
        'connection': connection,
        'defaults': defaults,
        'partitioning': partitioning
    }
    
    # Save profile
    try:
        config_manager.create_profile(profile_name, profile_settings)
        config_manager.save()
        logger.info(f"Profile {profile_name} created successfully")
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error creating profile: {str(e)}")
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Run the app
    app.run(host='0.0.0.0', port=5000, debug=True)