import os
import sys
import datetime
import uuid
from pathlib import Path

# Add the root directory to the path so we can import the package
sys.path.insert(0, os.path.abspath("."))

from csv_to_iceberg.storage.job_manager import JobManager, job_manager
from csv_to_iceberg.storage.lmdb_job_store import LMDBJobStore

def fix_test_job():
    """
    Fix the existing test job to make it visible in the web interface.
    """
    print("Fixing test job...")
    
    # Use the global job manager instance
    # This is already initialized with LMDB if available
    
    # Get all jobs including test jobs
    all_jobs = job_manager.get_all_jobs(include_test_jobs=True)
    print(f"Found {len(all_jobs)} jobs in total (including test jobs)")
    
    # Display jobs
    for job in all_jobs:
        job_id = job.get('id')
        is_test = job.get('is_test', False)
        print(f"Job ID: {job_id}, Test job: {is_test}")
        
        # If this is a test job, update it to not be a test job
        if is_test:
            print(f"Updating job {job_id} to not be a test job")
            job_manager.update_job(job_id, {"is_test": False})
    
    # Verify that the changes worked
    all_jobs_after = job_manager.get_all_jobs(include_test_jobs=False)
    print(f"After fix, found {len(all_jobs_after)} jobs (excluding test jobs)")

if __name__ == "__main__":
    fix_test_job()