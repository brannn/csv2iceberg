"""
LMDB-based configuration management module for CSV to Iceberg conversion
"""
import os
import logging
from typing import Dict, List, Optional, Any
from storage.lmdb_store import LMDBProfileStore
from storage.config_manager import DEFAULT_PROFILE

# Default LMDB directory location
DEFAULT_LMDB_PATH = os.path.expanduser("~/.csv_to_iceberg/lmdb_config")

class LMDBConfigManager:
    """LMDB-based configuration manager with same interface as ConfigManager"""
    
    def __init__(self, path: str = DEFAULT_LMDB_PATH):
        """Initialize LMDB configuration manager.
        
        Args:
            path: Path to LMDB directory
        """
        self.logger = logging.getLogger("csv_to_iceberg")
        self.store = LMDBProfileStore(path=path)
        self.logger.info(f"Initialized LMDB config manager at {path}")
        
        # Initialize with default profile if empty
        if not self.get_profiles():
            self.logger.debug("No profiles found, adding default profile")
            self.add_profile(DEFAULT_PROFILE)
            self.set_last_used_profile(DEFAULT_PROFILE["name"])
    
    def get_profiles(self) -> List[Dict[str, Any]]:
        """Get all profiles.
        
        Returns:
            List of profile dictionaries
        """
        return self.store.get_profiles()
    
    def get_profile(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a profile by name.
        
        Args:
            name: Profile name
            
        Returns:
            Profile dictionary or None if not found
        """
        return self.store.get_profile(name)
    
    def get_last_used_profile(self) -> Optional[Dict[str, Any]]:
        """Get the last used profile.
        
        Returns:
            Profile dictionary or None if not found
        """
        profile = self.store.get_last_used_profile()
        if profile:
            return profile
            
        # If no last used profile is set but profiles exist, return the first one
        profiles = self.get_profiles()
        if profiles:
            return profiles[0]
            
        return None
    
    def add_profile(self, profile: Dict[str, Any]) -> bool:
        """Add a new profile.
        
        Args:
            profile: Profile dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Make sure the profile has a name
            if not profile.get("name"):
                self.logger.error("Profile must have a name")
                return False
                
            return self.store.add_profile(profile)
        except Exception as e:
            self.logger.error(f"Error adding profile: {str(e)}")
            return False
    
    def update_profile(self, name: str, profile: Dict[str, Any]) -> bool:
        """Update an existing profile.
        
        Args:
            name: Name of the profile to update
            profile: Updated profile dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            return self.store.update_profile(name, profile)
        except Exception as e:
            self.logger.error(f"Error updating profile: {str(e)}")
            return False
    
    def delete_profile(self, name: str) -> bool:
        """Delete a profile.
        
        Args:
            name: Name of the profile to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            return self.store.delete_profile(name)
        except Exception as e:
            self.logger.error(f"Error deleting profile: {str(e)}")
            return False
    
    def set_last_used_profile(self, name: str) -> bool:
        """Set the last used profile.
        
        Args:
            name: Name of the profile
            
        Returns:
            True if successful, False otherwise
        """
        try:
            return self.store.set_last_used_profile(name)
        except Exception as e:
            self.logger.error(f"Error setting last used profile: {str(e)}")
            return False