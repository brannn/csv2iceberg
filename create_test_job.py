#!/usr/bin/env python
"""
Script to generate a test job entry in the LMDB job store.
"""

import sys
import datetime
import uuid
import time

# Import from csv_to_iceberg package
from csv_to_iceberg.storage.job_manager import job_manager

def create_test_job():
    """Create a test job entry in the database."""
    job_id = str(uuid.uuid4())
    
    # Create job
    job = {
        'id': job_id,
        'status': 'pending',
        'progress': 0,
        'created_at': datetime.datetime.now(),
        'started_at': None,
        'completed_at': None,
        'duration': None,
        'error': None,
        'params': {
            'trino_host': 'test.example.com',
            'trino_port': 443,
            'trino_user': 'test_user',
            'trino_catalog': 'iceberg',
            'trino_schema': 'test',
            'table_name': 'test_table',
            'mode': 'overwrite',
            'original_filename': 'test_file.csv'
        },
        'rows_processed': 0,
        'file_size': 1024,
    }
    
    # Add job to store
    job_manager.create_job(job_id, job)
    print(f"Created job with ID: {job_id}")
    
    # Update to running
    time.sleep(1)
    job_manager.update_job_progress(job_id, 10)
    job_manager.update_job(job_id, {
        'status': 'running',
        'started_at': datetime.datetime.now()
    })
    print(f"Updated job {job_id} to running status")
    
    # Update progress
    time.sleep(1)
    job_manager.update_job_progress(job_id, 50)
    print(f"Updated job {job_id} progress to 50%")
    
    # Complete job
    time.sleep(1)
    completed_at = datetime.datetime.now()
    duration = (completed_at - job['created_at']).total_seconds()
    
    job_manager.mark_job_completed(
        job_id,
        success=True,
        stdout="Test job completed successfully",
        stderr="",
        returncode=0,
    )
    
    job_manager.update_job(job_id, {
        'rows_processed': 1000,
        'duration': duration
    })
    
    print(f"Marked job {job_id} as completed")
    print("Test job creation completed successfully")
    
    # Verify the job exists in the store
    job = job_manager.get_job(job_id)
    if job:
        print(f"Job verification succeeded. Status: {job.get('status')}")
    else:
        print(f"ERROR: Job {job_id} not found in store after creation!")
    
    return job_id

if __name__ == "__main__":
    job_id = create_test_job()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--list':
        all_jobs = job_manager.get_all_jobs(limit=50, include_test_jobs=True)
        print(f"\nListing all jobs ({len(all_jobs)} found):")
        for job in all_jobs:
            status = job.get('status', 'unknown')
            created = job.get('created_at')
            id = job.get('id', 'no-id')
            print(f"Job {id}: Status={status}, Created={created}")