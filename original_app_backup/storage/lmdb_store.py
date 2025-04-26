"""
LMDB storage implementation for CSV to Iceberg application
"""
import lmdb
import json
import os
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class LMDBProfileStore:
    """LMDB-based storage for connection profiles"""
    
    def __init__(self, path: str = "~/.csv_to_iceberg/lmdb_profiles", max_size: int = 10_485_760):
        """Initialize the LMDB environment for profiles.
        
        Args:
            path: Path to the LMDB database file
            max_size: Maximum size of the database in bytes (10MB default)
        """
        # Ensure directory exists
        expanded_path = os.path.expanduser(path)
        os.makedirs(os.path.dirname(expanded_path), exist_ok=True)
        
        # Open LMDB environment
        self.env = lmdb.open(expanded_path, max_dbs=2, map_size=max_size)
        self.profiles_db = self.env.open_db(b"profiles")
        
        # Track last used profile
        self.metadata_db = self.env.open_db(b"metadata")
        
        logger.debug(f"Initialized LMDB profile store at {expanded_path}")
    
    def get_profiles(self) -> List[Dict[str, Any]]:
        """Get all profiles.
        
        Returns:
            List of profile dictionaries
        """
        profiles = []
        with self.env.begin(db=self.profiles_db) as txn:
            cursor = txn.cursor()
            for _, value in cursor:
                profiles.append(json.loads(value.decode()))
        return profiles
    
    def get_profile(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a profile by name.
        
        Args:
            name: Profile name
            
        Returns:
            Profile dictionary or None if not found
        """
        with self.env.begin(db=self.profiles_db) as txn:
            value = txn.get(name.encode())
            if value:
                return json.loads(value.decode())
        return None
    
    def get_last_used_profile(self) -> Optional[Dict[str, Any]]:
        """Get the last used profile.
        
        Returns:
            Profile dictionary or None if not found
        """
        with self.env.begin(db=self.metadata_db) as txn:
            last_used = txn.get(b"last_used_profile")
            if not last_used:
                return None
                
            profile_name = last_used.decode()
            return self.get_profile(profile_name)
    
    def add_profile(self, profile: Dict[str, Any]) -> bool:
        """Add a new profile.
        
        Args:
            profile: Profile dictionary
            
        Returns:
            True if successful, False otherwise
        """
        name = profile.get("name")
        if not name:
            logger.error("Cannot add profile without a name")
            return False
            
        with self.env.begin(write=True, db=self.profiles_db) as txn:
            # Check if profile exists
            if txn.get(name.encode()):
                logger.warning(f"Profile with name '{name}' already exists")
                return False
                
            # Add profile
            txn.put(name.encode(), json.dumps(profile).encode())
            logger.debug(f"Added profile: {name}")
        return True
    
    def update_profile(self, name: str, profile: Dict[str, Any]) -> bool:
        """Update an existing profile.
        
        Args:
            name: Name of the profile to update
            profile: Updated profile dictionary
            
        Returns:
            True if successful, False otherwise
        """
        with self.env.begin(write=True, db=self.profiles_db) as txn:
            # Check if profile exists
            if not txn.get(name.encode()):
                logger.warning(f"Cannot update non-existent profile: {name}")
                return False
                
            # Update profile
            txn.put(name.encode(), json.dumps(profile).encode())
            logger.debug(f"Updated profile: {name}")
        return True
    
    def delete_profile(self, name: str) -> bool:
        """Delete a profile.
        
        Args:
            name: Name of the profile to delete
            
        Returns:
            True if successful, False otherwise
        """
        with self.env.begin(write=True, db=self.profiles_db) as txn:
            # Check if profile exists
            if not txn.get(name.encode()):
                logger.warning(f"Cannot delete non-existent profile: {name}")
                return False
                
            # Delete profile
            txn.delete(name.encode())
            logger.debug(f"Deleted profile: {name}")
            
            # If this was the last used profile, clear that reference
            with self.env.begin(write=True, db=self.metadata_db) as meta_txn:
                last_used = meta_txn.get(b"last_used_profile")
                if last_used and last_used.decode() == name:
                    meta_txn.delete(b"last_used_profile")
                    logger.debug(f"Cleared last used profile reference to: {name}")
        
        return True
    
    def set_last_used_profile(self, name: str) -> bool:
        """Set the last used profile.
        
        Args:
            name: Name of the profile
            
        Returns:
            True if successful, False otherwise
        """
        # Verify profile exists
        if not self.get_profile(name):
            logger.warning(f"Cannot set last used profile to non-existent profile: {name}")
            return False
            
        with self.env.begin(write=True, db=self.metadata_db) as txn:
            txn.put(b"last_used_profile", name.encode())
            logger.debug(f"Set last used profile: {name}")
        return True
    
    def close(self):
        """Close the LMDB environment."""
        if hasattr(self, 'env'):
            self.env.close()
            logger.debug("Closed LMDB environment")