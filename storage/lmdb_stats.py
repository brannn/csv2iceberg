"""
LMDB statistics module for CSV to Iceberg conversion tool

This module provides functions to gather statistics and information about
the LMDB databases used by the application.
"""
import os
import shutil
import tempfile
from typing import Dict, Any, Optional

try:
    import lmdb
    LMDB_AVAILABLE = True
except ImportError:
    LMDB_AVAILABLE = False

# Get the paths from the storage modules
from storage.lmdb_job_store import DEFAULT_LMDB_PATH as JOB_STORE_PATH
from storage.lmdb_config_manager import DEFAULT_LMDB_PATH as CONFIG_STORE_PATH


def get_formatted_size(size_bytes: int) -> str:
    """Convert bytes to a human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Human-readable size string (e.g., "4.2 MB")
    """
    if size_bytes == 0:
        return "0 B"
        
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(units) - 1:
        size_bytes /= 1024
        i += 1
        
    return f"{size_bytes:.2f} {units[i]}"


def count_lmdb_entries(env_path: str, db_name: str = None) -> int:
    """Count the number of entries in an LMDB database.
    
    Args:
        env_path: Path to the LMDB environment directory
        db_name: Name of the database (None for the default database)
        
    Returns:
        Number of entries in the database or 0 if database doesn't exist
    """
    if not LMDB_AVAILABLE or not os.path.exists(env_path):
        return 0
        
    try:
        env = lmdb.open(env_path, readonly=True, lock=False)
        with env.begin(db=db_name) as txn:
            stats = txn.stat()
            return stats['entries']
    except Exception:
        return 0
    finally:
        if 'env' in locals():
            env.close()


def get_lmdb_info(env_path: str) -> Dict[str, Any]:
    """Get information about an LMDB environment.
    
    Args:
        env_path: Path to the LMDB environment directory
        
    Returns:
        Dictionary containing LMDB statistics and information
    """
    result = {
        'path': env_path,
        'exists': os.path.exists(env_path),
        'entries': 0,
        'size': 0,
        'size_formatted': '0 B',
        'max_size': 0,
        'max_size_formatted': '0 B',
        'page_size': 0,
        'max_readers': 0,
        'num_readers': 0,
        'disk_free': 0,
        'disk_free_formatted': '0 B',
        'disk_used': 0, 
        'disk_used_formatted': '0 B',
        'disk_total': 0,
        'disk_total_formatted': '0 B'
    }
    
    # Get disk space information for the drive containing the database
    try:
        disk_usage = shutil.disk_usage(os.path.dirname(env_path))
        result['disk_total'] = disk_usage.total
        result['disk_total_formatted'] = get_formatted_size(disk_usage.total)
        result['disk_used'] = disk_usage.used
        result['disk_used_formatted'] = get_formatted_size(disk_usage.used)
        result['disk_free'] = disk_usage.free
        result['disk_free_formatted'] = get_formatted_size(disk_usage.free)
    except Exception:
        pass
    
    if not result['exists'] or not LMDB_AVAILABLE:
        return result
        
    # Get database size on disk
    try:
        size = 0
        for dirpath, dirnames, filenames in os.walk(env_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                size += os.path.getsize(fp)
        result['size'] = size
        result['size_formatted'] = get_formatted_size(size)
    except Exception:
        pass
        
    # Get LMDB environment info
    try:
        env = lmdb.open(env_path, readonly=True, lock=False)
        result['entries'] = count_lmdb_entries(env_path)
        result['max_size'] = env.info()['map_size']
        result['max_size_formatted'] = get_formatted_size(env.info()['map_size'])
        result['page_size'] = env.info()['page_size']
        result['max_readers'] = env.info()['max_readers']
        result['num_readers'] = env.info()['last_txnid'] - env.info()['last_pgno']
    except Exception:
        pass
    finally:
        if 'env' in locals():
            env.close()
            
    return result


def get_system_info() -> Dict[str, Any]:
    """Get system information.
    
    Returns:
        Dictionary containing system information
    """
    return {
        'temp_directory': tempfile.gettempdir(),
        'home_directory': os.path.expanduser('~'),
    }


def get_all_storage_stats() -> Dict[str, Any]:
    """Get all LMDB statistics.
    
    Returns:
        Dictionary containing all statistics
    """
    return {
        'job_store': get_lmdb_info(JOB_STORE_PATH),
        'config_store': get_lmdb_info(CONFIG_STORE_PATH),
        'system': get_system_info(),
        'lmdb_available': LMDB_AVAILABLE
    }