"""
LMDB-based job status storage for CSV to Iceberg application
"""
import os
import json
import time
import logging
import datetime
from typing import Dict, List, Optional, Any, Union

try:
    import lmdb
    LMDB_IMPORTED = True
except ImportError:
    LMDB_IMPORTED = False
    logging.warning("LMDB not available. Job persistence will be disabled.")

# Default paths and constants
DEFAULT_LMDB_PATH = os.path.join(os.path.expanduser("~"), ".csv_to_iceberg", "lmdb_jobs")
DEFAULT_MAP_SIZE = 100 * 1024 * 1024  # 100MB default map size
MAX_JOBS_TO_KEEP = 100  # Maximum number of jobs to keep in LMDB
JOB_TTL = 30 * 24 * 60 * 60  # 30 days retention for jobs in seconds

# Setup logging
logger = logging.getLogger(__name__)

class LMDBJobStore:
    """LMDB-based storage for conversion jobs"""

    def __init__(self, path: str = DEFAULT_LMDB_PATH, max_size: int = DEFAULT_MAP_SIZE):
        """Initialize the LMDB environment for job storage.
        
        Args:
            path: Path to the LMDB database directory
            max_size: Maximum size of the database in bytes (default 100MB)
        """
        if not LMDB_IMPORTED:
            raise ImportError("LMDB is not available. Please install the lmdb package.")
        
        # Don't use os.path.expanduser if already an absolute path
        if os.path.isabs(path):
            self.path = path
        else:    
            # Expand path for home directory if needed
            self.path = os.path.expanduser(path)
            
        # Ensure directory exists
        os.makedirs(self.path, exist_ok=True)
        
        logger.info(f"Initializing LMDB job store at {self.path}")
        
        # Create LMDB environment
        self.env = lmdb.open(
            self.path,
            map_size=max_size,
            metasync=True,
            sync=True,
            max_dbs=2  # main db + index db
        )
        
        # Log information about the environment
        with self.env.begin() as txn:
            job_count = txn.stat()['entries']
            
        logger.info(f"LMDB job store initialized with {job_count} jobs")
        
        # Create a separate database for job ID index (sorted by timestamp)
        self.index_db = self.env.open_db(b'job_index')
        
    def serialize_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare job data for serialization.
        
        Args:
            job_data: Job data dictionary
            
        Returns:
            Serializable job data dictionary
        """
        # Create a copy to avoid modifying the original
        serialized = job_data.copy()
        
        # Convert datetime objects to ISO format
        for key in ['created_at', 'started_at', 'completed_at']:
            if key in serialized and isinstance(serialized[key], datetime.datetime):
                serialized[key] = serialized[key].isoformat()
                
        return serialized
        
    def deserialize_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize job data from LMDB.
        
        Args:
            job_data: Serialized job data dictionary
            
        Returns:
            Deserialized job data dictionary with proper types
        """
        # Create a copy to avoid modifying the original
        deserialized = job_data.copy()
        
        # Convert ISO datetime strings back to datetime objects
        for key in ['created_at', 'started_at', 'completed_at']:
            if key in deserialized and isinstance(deserialized[key], str):
                try:
                    deserialized[key] = datetime.datetime.fromisoformat(deserialized[key])
                except (ValueError, TypeError):
                    logger.warning(f"Failed to parse datetime from {key}: {deserialized[key]}")
                    
        return deserialized
    
    def add_job(self, job_id: str, job_data: Dict[str, Any]) -> bool:
        """Add a job to the store.
        
        Args:
            job_id: Job ID string
            job_data: Job data dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare job data for serialization
            serialized = self.serialize_job(job_data)
            
            # Add created_at timestamp if not present
            if 'created_at' not in serialized:
                serialized['created_at'] = datetime.datetime.now().isoformat()
                
            # Ensure job ID is correct in the serialized data
            serialized['id'] = job_id
                
            # Encode job data
            job_key = job_id.encode('utf-8')
            job_value = json.dumps(serialized).encode('utf-8')
            
            # Calculate timestamp for index (latest jobs first)
            # Reverse the timestamp so we can get newest jobs first when iterating normally
            timestamp = int(time.time())
            reverse_timestamp = 2147483647 - timestamp  # Use max 32-bit signed integer
            index_key = f"{reverse_timestamp:012d}:{job_id}".encode('utf-8')
            logger.info(f"Adding job {job_id} to LMDB with index key {index_key.decode('utf-8')}")
            
            # Store job data and update index
            with self.env.begin(write=True) as txn:
                txn.put(job_key, job_value)
                txn.put(index_key, job_key, db=self.index_db)

            # Debug: Check if job was added correctly
            with self.env.begin() as txn:
                value = txn.get(job_key)
                index_value = txn.get(index_key, db=self.index_db)
                
                if value and index_value:
                    logger.info(f"Successfully added job {job_id} to LMDB and indexed it")
                else:
                    logger.error(f"Failed to verify job {job_id} in LMDB. Data exists: {value is not None}, Index exists: {index_value is not None}")
            
            # Run cleanup of old jobs
            self._cleanup_old_jobs()
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding job {job_id} to LMDB: {str(e)}", exc_info=True)
            return False
            
    def update_job(self, job_id: str, job_data: Dict[str, Any]) -> bool:
        """Update an existing job.
        
        Args:
            job_id: Job ID string
            job_data: Updated job data dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.debug(f"LMDB store update_job called for job_id: {job_id}")
            
            # Get existing job to preserve any fields not in the update
            existing_job = self.get_job(job_id)
            if not existing_job:
                logger.warning(f"Job {job_id} not found in update_job, creating new job")
                return self.add_job(job_id, job_data)
                
            # Merge existing data with updates
            merged_data = {**existing_job, **job_data}
            
            # Prepare job data for serialization
            serialized = self.serialize_job(merged_data)
                
            # Encode job data
            job_key = job_id.encode('utf-8')
            job_value = json.dumps(serialized).encode('utf-8')
            
            # We should update the index if:
            # 1. Status is changing from pending to running/completed/failed
            # 2. Status is changing from running to completed/failed
            # 3. Job is being marked as completed or failed regardless of previous state
            update_index = False
            old_status = existing_job.get('status')
            new_status = job_data.get('status')
            
            if new_status in ['completed', 'failed']:
                # Always update index when a job is marked completed or failed
                update_index = True
                logger.info(f"Job {job_id} status changing to {new_status}, updating index")
            elif ('status' in job_data and 
                  old_status in ['pending', 'running'] and 
                  new_status in ['running', 'completed', 'failed']):
                update_index = True
                logger.info(f"Job {job_id} status changing from {old_status} to {new_status}, updating index")
                
            if update_index:
                # First find and remove old index key
                old_index_key = None
                with self.env.begin() as txn:
                    cursor = txn.cursor(db=self.index_db)
                    if cursor.first():
                        while True:
                            current_key = cursor.key()
                            current_value = cursor.value()
                            
                            if current_value == job_key:
                                old_index_key = current_key
                                break
                                
                            if not cursor.next():
                                break
                
                # Calculate new timestamp for index
                timestamp = int(time.time())
                reverse_timestamp = 2147483647 - timestamp
                new_index_key = f"{reverse_timestamp:012d}:{job_id}".encode('utf-8')
                
                # Update both job data and index
                with self.env.begin(write=True) as txn:
                    txn.put(job_key, job_value)
                    if old_index_key:
                        txn.delete(old_index_key, db=self.index_db)
                    txn.put(new_index_key, job_key, db=self.index_db)
                    
                logger.debug(f"Updated job {job_id} in LMDB with new index key")
            else:
                # Just update the job data
                with self.env.begin(write=True) as txn:
                    txn.put(job_key, job_value)
                    
                logger.debug(f"Updated job {job_id} in LMDB (data only)")
                
            return True
            
        except Exception as e:
            logger.error(f"Error updating job {job_id} in LMDB: {str(e)}", exc_info=True)
            return False
            
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get a job by ID.
        
        Args:
            job_id: Job ID string
            
        Returns:
            Job data dictionary or None if not found
        """
        try:
            logger.debug(f"LMDB store get_job called for job_id: {job_id}")
            job_key = job_id.encode('utf-8')
            
            with self.env.begin() as txn:
                job_value = txn.get(job_key)
                
            if job_value:
                logger.debug(f"Found job {job_id} in LMDB, deserializing")
                job_data = json.loads(job_value.decode('utf-8'))
                deserialized = self.deserialize_job(job_data)
                logger.debug(f"Job status: {deserialized.get('status')}, created_at: {deserialized.get('created_at')}")
                return deserialized
            else:
                logger.warning(f"Job {job_id} not found in LMDB")
                # Try to list all jobs to see if we can find it in a different way
                all_job_keys = []
                with self.env.begin() as txn:
                    cursor = txn.cursor()
                    if cursor.first():
                        while True:
                            all_job_keys.append(cursor.key().decode('utf-8'))
                            if not cursor.next():
                                break
                
                logger.debug(f"All job keys in LMDB: {all_job_keys}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting job {job_id} from LMDB: {str(e)}", exc_info=True)
            return None
            
    def get_all_jobs(self, limit: int = 50, include_test_jobs: bool = False) -> List[Dict[str, Any]]:
        """Get all jobs.
        
        Args:
            limit: Maximum number of jobs to return (most recent first)
            include_test_jobs: Whether to include test jobs in the results
            
        Returns:
            List of job data dictionaries
        """
        logger.info(f"LMDB store get_all_jobs called (limit={limit}, include_test_jobs={include_test_jobs})")
        try:
            jobs = []
            count = 0
            
            # First, check how many jobs we have in total
            all_job_keys = []
            with self.env.begin() as txn:
                cursor = txn.cursor()
                if cursor.first():
                    while True:
                        all_job_keys.append(cursor.key().decode('utf-8'))
                        if not cursor.next():
                            break
            
            logger.info(f"LMDB store contains {len(all_job_keys)} total jobs: {all_job_keys}")
            
            # Check index database size
            index_entries = []
            with self.env.begin() as txn:
                cursor = txn.cursor(db=self.index_db)
                if cursor.first():
                    while True:
                        key = cursor.key().decode('utf-8')
                        value = cursor.value().decode('utf-8')
                        index_entries.append(f"{key}->{value}")
                        if not cursor.next():
                            break
            
            logger.info(f"LMDB job index contains {len(index_entries)} entries: {index_entries}")
            
            with self.env.begin() as txn:
                # Use the sorted index to get jobs by timestamp (newest first)
                cursor = txn.cursor(db=self.index_db)
                
                # With our reverse timestamp index (2147483647 - timestamp),
                # the oldest job is now first and newest is last in the natural order
                
                # Start at last record (newest job) and move through them
                if cursor.last():
                    while count < limit:
                        try:
                            # Get the job key from index entry
                            index_key = cursor.key()
                            index_str = index_key.decode('utf-8')
                            job_key = cursor.value()
                            
                            logger.debug(f"Found job index entry: {index_str} -> {job_key.decode('utf-8')}")
                            
                            # Get the actual job data
                            job_value = txn.get(job_key)
                            
                            if job_value:
                                job_data = json.loads(job_value.decode('utf-8'))
                                job_data = self.deserialize_job(job_data)
                                
                                logger.debug(f"Retrieved job from LMDB: {job_data.get('id')} (status: {job_data.get('status')})")
                                
                                # Skip test jobs if not included
                                is_test_job = job_data.get('is_test', False)
                                if not is_test_job or include_test_jobs:
                                    jobs.append(job_data)
                                    count += 1
                            else:
                                logger.warning(f"Index points to missing job data: {job_key.decode('utf-8')}")
                                
                        except Exception as e:
                            logger.error(f"Error processing job during get_all_jobs: {str(e)}")
                            
                        # Move to next job in the index
                        if not cursor.next():
                            break
            
            return jobs
            
        except Exception as e:
            logger.error(f"Error getting all jobs from LMDB: {str(e)}", exc_info=True)
            return []
            
    def delete_job(self, job_id: str) -> bool:
        """Delete a job.
        
        Args:
            job_id: Job ID string
            
        Returns:
            True if successful, False otherwise
        """
        try:
            job_key = job_id.encode('utf-8')
            
            # We need to find and delete the index entry as well
            index_key = None
            
            with self.env.begin() as txn:
                # Find the index key for this job
                cursor = txn.cursor(db=self.index_db)
                if cursor.first():
                    while True:
                        current_key = cursor.key()
                        current_value = cursor.value()
                        
                        if current_value == job_key:
                            index_key = current_key
                            break
                            
                        if not cursor.next():
                            break
            
            # Now delete both entries
            with self.env.begin(write=True) as txn:
                txn.delete(job_key)
                if index_key:
                    txn.delete(index_key, db=self.index_db)
                    
            logger.debug(f"Deleted job {job_id} from LMDB")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting job {job_id} from LMDB: {str(e)}", exc_info=True)
            return False
            
    def _cleanup_old_jobs(self):
        """
        Clean up old jobs from the store.
        
        This method:
        1. Keeps a maximum number of jobs in LMDB
        2. Removes jobs that are older than JOB_TTL
        """
        try:
            now = time.time()
            cutoff_time = now - JOB_TTL
            
            jobs_count = 0
            jobs_to_delete = []
            
            with self.env.begin() as txn:
                # Count total jobs and find old ones
                cursor = txn.cursor(db=self.index_db)
                
                if cursor.first():
                    while True:
                        # Count total
                        jobs_count += 1
                        
                        # Parse timestamp from index key (remember we use reverse timestamps)
                        index_key = cursor.key().decode('utf-8')
                        parts = index_key.split(':', 1)
                        if len(parts) == 2:
                            try:
                                reverse_timestamp = int(parts[0])
                                # Convert back to real timestamp: 2147483647 - reverse_timestamp
                                real_timestamp = 2147483647 - reverse_timestamp
                                # If job is older than cutoff time, mark for deletion
                                if real_timestamp < cutoff_time:
                                    jobs_to_delete.append(index_key)
                            except ValueError:
                                pass
                                
                        if not cursor.next():
                            break
                            
            # If we have more than MAX_JOBS_TO_KEEP, delete oldest ones
            if jobs_count > MAX_JOBS_TO_KEEP:
                # Sort by timestamp (oldest first)
                jobs_to_delete_by_age = sorted(jobs_to_delete)
                
                # Add more jobs if needed to get down to MAX_JOBS_TO_KEEP
                extra_jobs_to_delete = jobs_count - MAX_JOBS_TO_KEEP - len(jobs_to_delete)
                
                if extra_jobs_to_delete > 0:
                    with self.env.begin() as txn:
                        cursor = txn.cursor(db=self.index_db)
                        
                        # Start from oldest job
                        if cursor.first():
                            extra_deleted = 0
                            while extra_deleted < extra_jobs_to_delete:
                                index_key = cursor.key().decode('utf-8')
                                if index_key not in jobs_to_delete:
                                    jobs_to_delete.append(index_key)
                                    extra_deleted += 1
                                    
                                if not cursor.next() or extra_deleted >= extra_jobs_to_delete:
                                    break
                                    
            # Now delete the old/excess jobs
            if jobs_to_delete:
                with self.env.begin(write=True) as txn:
                    for index_key_str in jobs_to_delete:
                        index_key = index_key_str.encode('utf-8')
                        
                        # Get job key from index
                        job_key = txn.get(index_key, db=self.index_db)
                        
                        if job_key:
                            # Delete the job data
                            txn.delete(job_key)
                            
                        # Delete the index entry
                        txn.delete(index_key, db=self.index_db)
                        
                logger.info(f"Cleaned up {len(jobs_to_delete)} old/excess jobs from LMDB")
                
        except Exception as e:
            logger.error(f"Error cleaning up old jobs from LMDB: {str(e)}", exc_info=True)
            
    def close(self):
        """Close the LMDB environment."""
        self.env.close()
        logger.debug("Closed LMDB job store")