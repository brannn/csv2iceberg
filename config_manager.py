"""
Configuration management module for CSV to Iceberg conversion
"""
import os
import json
import logging
from typing import Dict, List, Optional, Any

# Default config file location
DEFAULT_CONFIG_FILE = os.path.expanduser("~/.csv_to_iceberg_config.json")

# Default profile template
DEFAULT_PROFILE = {
    "name": "Default",
    "trino_host": "sep.sdp-dev.pd.switchnet.nv",
    "trino_port": 8080,
    "trino_user": "",
    "trino_password": "",
    "http_scheme": "http",
    "trino_role": "sysadmin",
    "trino_catalog": "iceberg",
    "trino_schema": "default",
    "hive_metastore_uri": "localhost:9083",
    "use_hive_metastore": True,
    "description": "Default connection profile"
}

class ConfigManager:
    """Manager for configuration profiles"""
    
    def __init__(self, config_file: str = DEFAULT_CONFIG_FILE):
        """
        Initialize configuration manager.
        
        Args:
            config_file: Path to configuration file
        """
        self.config_file = config_file
        self.logger = logging.getLogger("csv_to_iceberg")
        self._config_data = None
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from file."""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    self._config_data = json.load(f)
                self.logger.debug(f"Loaded configuration from {self.config_file}")
            else:
                # Initialize with default configuration
                self._config_data = {
                    "profiles": [DEFAULT_PROFILE],
                    "last_used_profile": "Default"
                }
                self._save_config()
                self.logger.debug(f"Created default configuration in {self.config_file}")
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            # If loading fails, initialize with default configuration
            self._config_data = {
                "profiles": [DEFAULT_PROFILE],
                "last_used_profile": "Default"
            }
            
        # Ensure _config_data is always a dictionary
        if self._config_data is None:
            self._config_data = {
                "profiles": [DEFAULT_PROFILE],
                "last_used_profile": "Default"
            }
    
    def _save_config(self) -> None:
        """Save configuration to file."""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(self.config_file)), exist_ok=True)
            
            with open(self.config_file, 'w') as f:
                json.dump(self._config_data, f, indent=2)
            self.logger.debug(f"Saved configuration to {self.config_file}")
        except Exception as e:
            self.logger.error(f"Error saving configuration: {str(e)}")
    
    def get_profiles(self) -> List[Dict[str, Any]]:
        """
        Get all profiles.
        
        Returns:
            List of profile dictionaries
        """
        return self._config_data.get("profiles", [])
    
    def get_profile(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get a profile by name.
        
        Args:
            name: Profile name
            
        Returns:
            Profile dictionary or None if not found
        """
        for profile in self._config_data.get("profiles", []):
            if profile.get("name") == name:
                return profile
        return None
    
    def get_last_used_profile(self) -> Optional[Dict[str, Any]]:
        """
        Get the last used profile.
        
        Returns:
            Profile dictionary or None if not found
        """
        last_used = self._config_data.get("last_used_profile")
        if last_used:
            return self.get_profile(last_used)
        elif self._config_data.get("profiles"):
            # Return the first profile if last_used is not set
            return self._config_data["profiles"][0]
        return None
    
    def add_profile(self, profile: Dict[str, Any]) -> bool:
        """
        Add a new profile.
        
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
            
            # Check if a profile with the same name already exists
            if self.get_profile(profile["name"]):
                self.logger.error(f"Profile with name '{profile['name']}' already exists")
                return False
            
            # Add the profile
            if "profiles" not in self._config_data:
                self._config_data["profiles"] = []
            
            self._config_data["profiles"].append(profile)
            self._save_config()
            return True
        except Exception as e:
            self.logger.error(f"Error adding profile: {str(e)}")
            return False
    
    def update_profile(self, name: str, profile: Dict[str, Any]) -> bool:
        """
        Update an existing profile.
        
        Args:
            name: Name of the profile to update
            profile: Updated profile dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if the profile exists
            for i, p in enumerate(self._config_data.get("profiles", [])):
                if p.get("name") == name:
                    # Update the profile
                    self._config_data["profiles"][i] = profile
                    self._save_config()
                    return True
            
            self.logger.error(f"Profile with name '{name}' not found")
            return False
        except Exception as e:
            self.logger.error(f"Error updating profile: {str(e)}")
            return False
    
    def delete_profile(self, name: str) -> bool:
        """
        Delete a profile.
        
        Args:
            name: Name of the profile to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if the profile exists
            for i, p in enumerate(self._config_data.get("profiles", [])):
                if p.get("name") == name:
                    # Delete the profile
                    del self._config_data["profiles"][i]
                    
                    # Update last_used_profile if it was this profile
                    if self._config_data.get("last_used_profile") == name:
                        if self._config_data.get("profiles"):
                            self._config_data["last_used_profile"] = self._config_data["profiles"][0]["name"]
                        else:
                            self._config_data["last_used_profile"] = None
                    
                    self._save_config()
                    return True
            
            self.logger.error(f"Profile with name '{name}' not found")
            return False
        except Exception as e:
            self.logger.error(f"Error deleting profile: {str(e)}")
            return False
    
    def set_last_used_profile(self, name: str) -> bool:
        """
        Set the last used profile.
        
        Args:
            name: Name of the profile
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if the profile exists
            if not self.get_profile(name):
                self.logger.error(f"Profile with name '{name}' not found")
                return False
            
            # Set the last used profile
            self._config_data["last_used_profile"] = name
            self._save_config()
            return True
        except Exception as e:
            self.logger.error(f"Error setting last used profile: {str(e)}")
            return False