"""
Utility module for getting LMDB storage statistics
"""
import os
import tempfile
import logging
from typing import Dict, Any, Optional

try:
    import lmdb
    LMDB_AVAILABLE = True
except ImportError:
    LMDB_AVAILABLE = False
    logging.warning("LMDB not available for statistics collection")

# Setup logging
logger = logging.getLogger(__name__)

def get_lmdb_stats(env_path: str) -> Dict[str, Any]:
    """
    Get statistics for an LMDB environment
    
    Args:
        env_path: Path to the LMDB environment directory
        
    Returns:
        Dictionary with LMDB statistics
    """
    stats = {
        'path': os.path.abspath(os.path.expanduser(env_path)),
        'exists': False,
        'size_bytes': 0,
        'page_size': 0,
        'max_readers': 0,
        'max_size_bytes': 0,
        'num_readers': 0,
        'entries': 0, 
        'available': LMDB_AVAILABLE
    }
    
    if not LMDB_AVAILABLE:
        return stats
    
    # Check if the directory exists
    db_path = os.path.expanduser(env_path)
    if not os.path.exists(db_path):
        return stats
    
    stats['exists'] = True
    
    # Check if data.mdb exists and get its size
    data_path = os.path.join(db_path, 'data.mdb')
    if os.path.exists(data_path):
        stats['size_bytes'] = os.path.getsize(data_path)
    
    try:
        # Open the environment in read-only mode
        env = lmdb.open(db_path, readonly=True, max_readers=1)
        
        # Get environment info
        info = env.info()
        stats.update({
            'page_size': info['map_size'] // info['last_pgno'] if info['last_pgno'] > 0 else 0,
            'max_readers': info['max_readers'],
            'max_size_bytes': info['map_size'],
            'num_readers': info['num_readers']
        })
        
        # Count entries
        with env.begin() as txn:
            cursor = txn.cursor()
            if cursor.first():
                # Count the number of entries
                stats['entries'] = sum(1 for _ in cursor)
        
        env.close()
    except Exception as e:
        logger.error(f"Error getting LMDB stats for {env_path}: {str(e)}")
    
    return stats

def format_bytes(bytes_value: int) -> str:
    """Format bytes into a human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024
    return f"{bytes_value:.2f} PB"

def get_all_storage_stats() -> Dict[str, Any]:
    """
    Get comprehensive statistics about all storage components
    
    Returns:
        Dictionary with statistics for all storage components
    """
    from csv_to_iceberg.storage.lmdb_job_store import DEFAULT_LMDB_PATH as JOB_LMDB_PATH
    from csv_to_iceberg.storage.lmdb_config_manager import DEFAULT_LMDB_PATH as CONFIG_LMDB_PATH
    
    # Get system paths
    temp_dir = tempfile.gettempdir()
    home_dir = os.path.expanduser("~")
    
    # Calculate absolute paths
    job_lmdb_path = os.path.expanduser(JOB_LMDB_PATH)
    config_lmdb_path = os.path.expanduser(CONFIG_LMDB_PATH)
    
    # Get stats for both LMDB stores
    job_stats = get_lmdb_stats(JOB_LMDB_PATH)
    config_stats = get_lmdb_stats(CONFIG_LMDB_PATH)
    
    # Format human-readable sizes
    job_stats['size_formatted'] = format_bytes(job_stats['size_bytes'])
    job_stats['max_size_formatted'] = format_bytes(job_stats['max_size_bytes'])
    config_stats['size_formatted'] = format_bytes(config_stats['size_bytes'])
    config_stats['max_size_formatted'] = format_bytes(config_stats['max_size_bytes'])
    
    # System disk space info
    system_stats = {
        'temp_directory': temp_dir,
        'home_directory': home_dir,
    }
    
    try:
        # Get disk usage for directory containing LMDB files
        import shutil
        job_disk = os.path.dirname(job_lmdb_path)
        config_disk = os.path.dirname(config_lmdb_path)
        
        # Add disk usage for job LMDB directory
        if os.path.exists(job_disk):
            disk_usage = shutil.disk_usage(job_disk)
            job_stats.update({
                'disk_total': disk_usage.total,
                'disk_used': disk_usage.used,
                'disk_free': disk_usage.free,
                'disk_total_formatted': format_bytes(disk_usage.total),
                'disk_used_formatted': format_bytes(disk_usage.used),
                'disk_free_formatted': format_bytes(disk_usage.free),
            })
        
        # Add disk usage for config LMDB directory  
        if os.path.exists(config_disk) and config_disk != job_disk:
            disk_usage = shutil.disk_usage(config_disk)
            config_stats.update({
                'disk_total': disk_usage.total,
                'disk_used': disk_usage.used,
                'disk_free': disk_usage.free,
                'disk_total_formatted': format_bytes(disk_usage.total),
                'disk_used_formatted': format_bytes(disk_usage.used),
                'disk_free_formatted': format_bytes(disk_usage.free),
            })
        elif config_disk == job_disk:
            # Use the same disk stats if both are on the same disk
            config_stats.update({
                'disk_total': job_stats.get('disk_total', 0),
                'disk_used': job_stats.get('disk_used', 0),
                'disk_free': job_stats.get('disk_free', 0),
                'disk_total_formatted': job_stats.get('disk_total_formatted', '0 B'),
                'disk_used_formatted': job_stats.get('disk_used_formatted', '0 B'),
                'disk_free_formatted': job_stats.get('disk_free_formatted', '0 B'),
            })
    except Exception as e:
        logger.error(f"Error getting disk usage: {str(e)}")
    
    return {
        'system': system_stats,
        'job_store': job_stats,
        'config_store': config_stats,
        'lmdb_available': LMDB_AVAILABLE
    }