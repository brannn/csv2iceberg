import os
import logging
import datetime
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
import tempfile
import subprocess
import threading
from werkzeug.utils import secure_filename

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
        if params.get('http_scheme'):
            cmd.extend(["--http-scheme", params['http_scheme']])
        if params.get('trino_role'):
            cmd.extend(["--trino-role", params['trino_role']])
        
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
            
        # Run the conversion process
        logger.info(f"Starting conversion job {job_id} with command: {' '.join(cmd)}")
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        # Store results
        conversion_jobs[job_id]['status'] = 'completed' if process.returncode == 0 else 'failed'
        conversion_jobs[job_id]['stdout'] = process.stdout
        conversion_jobs[job_id]['stderr'] = process.stderr
        conversion_jobs[job_id]['returncode'] = process.returncode
        # Add completion timestamp
        conversion_jobs[job_id]['completed_at'] = datetime.datetime.now()
        
        logger.info(f"Completed conversion job {job_id} with status: {conversion_jobs[job_id]['status']}")
        
    except Exception as e:
        logger.error(f"Error in conversion job {job_id}: {str(e)}", exc_info=True)
        conversion_jobs[job_id]['status'] = 'failed'
        conversion_jobs[job_id]['error'] = str(e)
        # Add completion timestamp even for failed jobs
        conversion_jobs[job_id]['completed_at'] = datetime.datetime.now()

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
                'delimiter': request.form.get('delimiter'),
                'has_header': request.form.get('has_header', 'true'),
                'quote_char': request.form.get('quote_char'),
                'batch_size': request.form.get('batch_size'),
                'mode': request.form.get('mode', 'append'),
                'sample_size': request.form.get('sample_size'),
                'verbose': request.form.get('verbose', 'false')
            }
            logger.debug(f"Collected parameters: {params}")
            
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

@app.route('/job/<job_id>')
def job_status(job_id):
    """Show the status of a conversion job."""
    if job_id not in conversion_jobs:
        flash('Job not found', 'error')
        return redirect(url_for('index'))
    
    now = datetime.datetime.now()
    return render_template('job_status.html', 
                          job=conversion_jobs[job_id], 
                          job_id=job_id,
                          now=now,
                          format_duration=format_duration)

@app.route('/jobs')
def jobs():
    """List all conversion jobs."""
    now = datetime.datetime.now()
    return render_template('jobs.html', 
                          jobs=conversion_jobs, 
                          now=now, 
                          format_duration=format_duration)

@app.route('/job/<job_id>/progress/<int:percent>', methods=['POST'])
def update_job_progress(job_id, percent):
    """Update the progress of a conversion job."""
    logger.debug(f"Progress update for job {job_id}: {percent}%")
    if job_id in conversion_jobs:
        conversion_jobs[job_id]['progress'] = percent
        return jsonify({"status": "ok"})
    else:
        return jsonify({"status": "error", "message": "Job not found"}), 404

@app.route('/job/<job_id>/progress', methods=['GET'])
def get_job_progress(job_id):
    """Get the current progress of a conversion job."""
    if job_id in conversion_jobs:
        return jsonify({
            "job_id": job_id, 
            "progress": conversion_jobs[job_id].get('progress', 0),
            "status": conversion_jobs[job_id]['status']
        })
    else:
        return jsonify({"status": "error", "message": "Job not found"}), 404

@app.route('/diagnostics')
def diagnostics():
    """Diagnostic endpoint to verify server operation."""
    logger.info("Diagnostics endpoint called")
    try:
        data = {
            "status": "ok",
            "jobs_count": len(conversion_jobs),
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

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Run the app
    app.run(host='0.0.0.0', port=5000, debug=True)