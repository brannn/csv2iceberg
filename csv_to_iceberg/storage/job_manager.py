"""
Job management module for CSV to Iceberg conversion
"""
import os
import datetime
import logging
import time
from collections import OrderedDict
from typing import Dict, List, Optional, Any, Union

# Try to import LMDB job store
try:
    from csv_to_iceberg.storage.lmdb_job_store import LMDBJobStore, LMDB_IMPORTED
except ImportError:
    LMDB_IMPORTED = False
    logging.warning("LMDB job store not available. Using in-memory storage only.")

# Setup logging
logger = logging.getLogger(__name__)

# Constants for job management
MAX_JOBS_TO_KEEP = 50      # Maximum number of jobs to keep in memory
COMPLETED_JOB_TTL = 1800   # Seconds to keep completed jobs in memory (30 minutes)
TEST_JOB_TTL = 3600        # Seconds to keep test jobs in memory (1 hour)

# Feature flag for LMDB job storage - enable by default for persistence
USE_LMDB_JOBS = os.environ.get("USE_LMDB_JOBS", "true").lower() == "true"

class JobManager:
    """Manager for conversion jobs with support for persistent storage"""
    
    def __init__(self, use_lmdb: bool = USE_LMDB_JOBS):
        """
        Initialize job manager.
        
        Args:
            use_lmdb: Whether to use LMDB for job storage (defaults to environment setting)
        """
        # Store current configuration
        self.use_lmdb = use_lmdb and LMDB_IMPORTED
        
        # Dictionary to track which jobs are being actively viewed
        # This prevents cleanup of jobs that are currently being viewed
        self.active_job_views = {}
        
        # In-memory job storage
        self.memory_jobs = OrderedDict()
        
        # LMDB job storage (if available)
        self.lmdb_store = None
        if self.use_lmdb and LMDB_IMPORTED:
            try:
                self.lmdb_store = LMDBJobStore()
                logger.info("Initialized LMDB job store")
            except Exception as e:
                logger.error(f"Failed to initialize LMDB job store: {str(e)}")
                self.use_lmdb = False
                
        logger.info(f"Job manager initialized with storage type: {'LMDB' if self.use_lmdb else 'Memory'}")
        
    def create_job(self, job_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new job.
        
        Args:
            job_id: Job ID
            params: Job parameters
            
        Returns:
            Job data dictionary
        """
        # Extract and move important fields up to the root level for easier access in templates
        job_data = {
            'id': job_id,
            'params': params,
            'status': 'pending',
            'progress': 0,
            'created_at': datetime.datetime.now(),
            'started_at': None,
            'completed_at': None,
            'stdout': None,
            'stderr': None,
            'returncode': None,
            'error': None,
            'is_test': params.get('is_test', False),
            # Move these fields up to the root level from params
            'table_name': params.get('table_name', ''),
            'file_size': params.get('file_size', 0),
            'original_filename': params.get('original_filename', ''),
            'mode': params.get('mode', '')
        }
        
        # Store in memory first (for immediate access)
        self.memory_jobs[job_id] = job_data
        
        # Store in LMDB if enabled
        if self.use_lmdb and self.lmdb_store:
            self.lmdb_store.add_job(job_id, job_data)
            
        return job_data
        
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a job by ID.
        
        Args:
            job_id: Job ID
            
        Returns:
            Job data dictionary or None if not found
        """
        logger = logging.getLogger(__name__)
        logger.info(f"get_job called for job_id: {job_id}")
        
        # Try memory first (faster)
        if job_id in self.memory_jobs:
            logger.info(f"Job {job_id} found in memory cache")
            return self.memory_jobs[job_id]
            
        # Try LMDB if enabled and not in memory
        if self.use_lmdb and self.lmdb_store:
            logger.info(f"Job {job_id} not in memory, trying LMDB")
            job_data = self.lmdb_store.get_job(job_id)
            if job_data:
                logger.info(f"Job {job_id} found in LMDB")
                # Cache in memory for faster access
                self.memory_jobs[job_id] = job_data
                return job_data
            else:
                logger.info(f"Job {job_id} not found in LMDB")
        
        # List all jobs in memory for debugging
        memory_job_ids = list(self.memory_jobs.keys())
        logger.info(f"All job IDs in memory: {memory_job_ids}")
        
        return None
        
    def update_job(self, job_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Update an existing job.
        
        Args:
            job_id: Job ID
            updates: Dictionary of fields to update
            
        Returns:
            Updated job data or None if job not found
        """
        # Get existing job
        job_data = self.get_job(job_id)
        if not job_data:
            return None
            
        # Update in memory
        if job_id in self.memory_jobs:
            for key, value in updates.items():
                self.memory_jobs[job_id][key] = value
                
        # Update in LMDB if enabled
        if self.use_lmdb and self.lmdb_store:
            self.lmdb_store.update_job(job_id, updates)
            
        return self.get_job(job_id)
        
    def mark_job_as_active(self, job_id: str) -> None:
        """
        Mark a job as being actively viewed to prevent cleanup.
        
        Args:
            job_id: The ID of the job to mark as active
        """
        self.active_job_views[job_id] = datetime.datetime.now()
        
    def get_all_jobs(self, limit: int = 50, include_test_jobs: bool = False) -> List[Dict[str, Any]]:
        """
        Get all jobs.
        
        Args:
            limit: Maximum number of jobs to return
            include_test_jobs: Whether to include test jobs in the results
            
        Returns:
            List of job data dictionaries
        """
        if self.use_lmdb and self.lmdb_store:
            # Get from LMDB (already sorted by newest first)
            return self.lmdb_store.get_all_jobs(limit, include_test_jobs)
        else:
            # Get from memory
            jobs = []
            
            # Filter and limit
            for job_id, job_data in reversed(self.memory_jobs.items()):
                # Skip test jobs if not included
                if not include_test_jobs and job_data.get('is_test', False):
                    continue
                    
                jobs.append(job_data)
                
                if len(jobs) >= limit:
                    break
                    
            return jobs
            
    def update_job_progress(self, job_id: str, percent: int) -> bool:
        """
        Update the progress of a job.
        
        Args:
            job_id: Job ID
            percent: Progress percentage (0-100)
            
        Returns:
            True if successful, False otherwise
        """
        job_data = self.get_job(job_id)
        if not job_data:
            return False
            
        # Normalize percent
        percent = max(0, min(100, percent))
        
        # Only update if the job is not already completed
        if job_data['status'] not in ['completed', 'failed']:
            # Set started_at if this is the first progress update
            updates = {'progress': percent}
            
            if percent > 0 and job_data['status'] == 'pending':
                updates['status'] = 'running'
                updates['started_at'] = datetime.datetime.now()
                
            self.update_job(job_id, updates)
            return True
            
        return False
        
    def mark_job_completed(self, job_id: str, success: bool, stdout: str = None, stderr: str = None, 
                          returncode: int = None, error: str = None) -> bool:
        """
        Mark a job as completed.
        
        Args:
            job_id: Job ID
            success: Whether the job completed successfully
            stdout: Standard output from the job
            stderr: Standard error from the job
            returncode: Return code from the process
            error: Error message if the job failed
            
        Returns:
            True if successful, False otherwise
        """
        job_data = self.get_job(job_id)
        if not job_data:
            return False
            
        # Preserve important values from the job data
        rows_processed = job_data.get('rows_processed')
            
        # Get timestamps
        now = datetime.datetime.now()
        started_at = job_data.get('started_at')
        
        updates = {
            'status': 'completed' if success else 'failed',
            'completed_at': now
        }
        
        # Calculate duration if started_at exists
        if started_at:
            updates['duration'] = (now - started_at).total_seconds()
        
        # Set progress to 100% when completed successfully
        if success:
            updates['progress'] = 100
        
        # Preserve rows_processed if it exists
        if rows_processed is not None:
            updates['rows_processed'] = rows_processed
            
        # Add optional fields if provided
        if stdout is not None:
            updates['stdout'] = stdout
        if stderr is not None:
            updates['stderr'] = stderr
        if returncode is not None:
            updates['returncode'] = returncode
        if error is not None:
            updates['error'] = error
            
        self.update_job(job_id, updates)
        return True
        
    def cleanup_old_jobs(self) -> None:
        """
        Remove old completed jobs from memory to prevent memory leaks.
        
        This function:
        1. Keeps a maximum number of jobs in memory
        2. Removes completed/failed jobs after their TTL expires
        3. Respects active job views to prevent removing jobs that are being viewed
        4. Uses a longer TTL for test jobs
        """
        # The LMDB store handles its own cleanup, so this only applies to memory store
        if len(self.memory_jobs) <= MAX_JOBS_TO_KEEP:
            return
            
        now = datetime.datetime.now()
        jobs_to_remove = []
        
        # Find candidates for removal
        for job_id, job_data in self.memory_jobs.items():
            # Never remove jobs that are currently being viewed
            if job_id in self.active_job_views:
                view_time = self.active_job_views[job_id]
                # Only keep active view status for max 30 minutes
                if (now - view_time).total_seconds() > 1800:
                    del self.active_job_views[job_id]
                else:
                    continue
                    
            # Only consider removing completed or failed jobs
            if job_data['status'] not in ['completed', 'failed']:
                continue
                
            # Check TTL based on completion time
            if job_data['completed_at']:
                is_test_job = job_data.get('is_test', False)
                ttl_seconds = TEST_JOB_TTL if is_test_job else COMPLETED_JOB_TTL
                
                # Remove if older than TTL
                if (now - job_data['completed_at']).total_seconds() > ttl_seconds:
                    jobs_to_remove.append(job_id)
                    
        # If we still have too many jobs, remove oldest completed ones
        if len(self.memory_jobs) - len(jobs_to_remove) > MAX_JOBS_TO_KEEP:
            completed_jobs = [
                (job_id, job_data) 
                for job_id, job_data in self.memory_jobs.items()
                if job_id not in jobs_to_remove and
                   job_data['status'] in ['completed', 'failed'] and
                   job_id not in self.active_job_views
            ]
            
            # Sort by completion time (oldest first)
            completed_jobs.sort(key=lambda x: x[1]['completed_at'] if x[1]['completed_at'] else now)
            
            # Add oldest jobs to removal list
            excess_count = len(self.memory_jobs) - len(jobs_to_remove) - MAX_JOBS_TO_KEEP
            for job_id, _ in completed_jobs[:excess_count]:
                jobs_to_remove.append(job_id)
                
        # Remove jobs
        for job_id in jobs_to_remove:
            del self.memory_jobs[job_id]
            
        if jobs_to_remove:
            logger.info(f"Cleaned up {len(jobs_to_remove)} old jobs from memory")
            
    def close(self) -> None:
        """Close the job manager and any open resources."""
        if self.lmdb_store:
            self.lmdb_store.close()
            
# Create a global job manager instance for easy access
job_manager = JobManager()