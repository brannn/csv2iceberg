import os
import sys
import datetime
import uuid
from pathlib import Path

# Add the root directory to the path so we can import the package
sys.path.insert(0, os.path.abspath("."))

from csv_to_iceberg.storage.job_manager import JobManager, job_manager

def create_test_job():
    """
    Create a test job to verify that job management is working.
    """
    print("Creating test job...")
    
    # Use the global job manager instance
    # This is already initialized with LMDB if available
    
    # Create a job ID and parameters
    job_id = str(uuid.uuid4())
    
    # Get the file size
    file_path = "samples/sample_data.csv"
    file_size = Path(file_path).stat().st_size
    
    # Job parameters
    params = {
        "csv_file": file_path,
        "table_name": "iceberg.default.test_table",
        "file_size": file_size,
        "original_filename": "sample_data.csv",
        "mode": "APPEND",
        "is_test": True,
        "trino_host": "localhost",
        "trino_port": 8080,
        "trino_catalog": "iceberg",
        "trino_schema": "default",
    }
    
    # Create the job
    job = job_manager.create_job(job_id, params)
    print(f"Test job created with ID: {job_id}")
    
    # Mark it as completed with some test data
    job_manager.update_job_progress(job_id, 100)
    job_manager.mark_job_completed(
        job_id, 
        success=True, 
        stdout="Test job completed successfully", 
        returncode=0,
        error=None
    )
    
    # Update with rows processed
    job_manager.update_job(job_id, {"rows_processed": 1000})
    
    # Verify that the job was added
    job = job_manager.get_job(job_id)
    print(f"Job retrieved: {job['id'] if job else 'Not found'}")
    
    all_jobs = job_manager.get_all_jobs(include_test_jobs=True)
    print(f"Total jobs in store: {len(all_jobs)}")
    
    if job and all_jobs:
        print("Job creation and retrieval successful!")
    else:
        print("Warning: Job creation or retrieval failed!")

if __name__ == "__main__":
    create_test_job()