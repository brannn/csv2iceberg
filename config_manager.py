"""
Configuration manager module for CSV to Iceberg conversion
"""
import os
import json
import logging
from typing import Dict, Any, Optional, List

# Set up logging
logger = logging.getLogger(__name__)

DEFAULT_CONFIG_FILE = "csv_to_iceberg_config.json"
DEFAULT_CONFIG_LOCATIONS = [
    ".",  # Current directory
    os.path.expanduser("~"),  # User's home directory
    os.path.join(os.path.expanduser("~"), ".config"),  # User's config directory
]

class ConfigManager:
    """Manages configuration settings for CSV to Iceberg converter"""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_file: Path to configuration file, or None to search default locations
        """
        self.config_file = config_file
        self.config: Dict[str, Any] = {}
        self.profiles: Dict[str, Dict[str, Any]] = {}
        
        # Load the configuration
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from file"""
        # If a specific config file was provided, try to load it
        if self.config_file:
            if os.path.isfile(self.config_file):
                self._load_from_file(self.config_file)
                return
            else:
                logger.warning(f"Specified config file not found: {self.config_file}")
        
        # Search for config file in default locations
        for location in DEFAULT_CONFIG_LOCATIONS:
            potential_file = os.path.join(location, DEFAULT_CONFIG_FILE)
            if os.path.isfile(potential_file):
                self._load_from_file(potential_file)
                self.config_file = potential_file
                return
        
        # No config file found, create empty configuration
        logger.info("No configuration file found, using defaults")
        self.config = {}
        self.profiles = {}
    
    def _load_from_file(self, file_path: str) -> None:
        """Load configuration from a specific file"""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                
                # Extract main configuration and profiles
                self.config = data.get('config', {})
                self.profiles = data.get('profiles', {})
                
                logger.info(f"Loaded configuration from {file_path}")
                logger.debug(f"Loaded {len(self.profiles)} profile(s)")
        except Exception as e:
            logger.error(f"Error loading configuration from {file_path}: {str(e)}", exc_info=True)
            # Initialize with empty configuration
            self.config = {}
            self.profiles = {}
    
    def save(self, file_path: Optional[str] = None) -> bool:
        """
        Save configuration to file.
        
        Args:
            file_path: Path to save the configuration, or None to use current config file
            
        Returns:
            True if successful, False otherwise
        """
        save_path = file_path or self.config_file
        
        # If no path is specified and no current config file, use default
        if not save_path:
            # Default to user's home directory
            save_path = os.path.join(os.path.expanduser("~"), DEFAULT_CONFIG_FILE)
            self.config_file = save_path
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(save_path)), exist_ok=True)
            
            # Prepare data to save
            data = {
                'config': self.config,
                'profiles': self.profiles
            }
            
            # Write to file
            with open(save_path, 'w') as f:
                json.dump(data, f, indent=2)
                
            logger.info(f"Configuration saved to {save_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving configuration to {save_path}: {str(e)}", exc_info=True)
            return False
    
    def get_profile(self, name: str) -> Dict[str, Any]:
        """
        Get a profile by name.
        
        Args:
            name: Profile name
            
        Returns:
            Profile settings dict, or empty dict if not found
        """
        return self.profiles.get(name, {})
    
    def get_profiles(self) -> List[str]:
        """
        Get a list of available profile names.
        
        Returns:
            List of profile names
        """
        return list(self.profiles.keys())
    
    def create_profile(self, name: str, settings: Dict[str, Any]) -> bool:
        """
        Create a new profile or update an existing one.
        
        Args:
            name: Profile name
            settings: Profile settings
            
        Returns:
            True if successful
        """
        # Validate profile name
        if not name or not isinstance(name, str):
            logger.error("Invalid profile name")
            return False
        
        # Store the profile
        self.profiles[name] = settings
        logger.info(f"Profile '{name}' created/updated")
        return True
    
    def delete_profile(self, name: str) -> bool:
        """
        Delete a profile.
        
        Args:
            name: Profile name
            
        Returns:
            True if successful, False if profile doesn't exist
        """
        if name in self.profiles:
            del self.profiles[name]
            logger.info(f"Profile '{name}' deleted")
            return True
        else:
            logger.warning(f"Profile '{name}' not found")
            return False
    
    def get_setting(self, key: str, default: Any = None) -> Any:
        """
        Get a global configuration setting.
        
        Args:
            key: Setting key
            default: Default value if setting is not found
            
        Returns:
            Setting value or default
        """
        return self.config.get(key, default)
    
    def set_setting(self, key: str, value: Any) -> None:
        """
        Set a global configuration setting.
        
        Args:
            key: Setting key
            value: Setting value
        """
        self.config[key] = value
    
    def profile_exists(self, name: str) -> bool:
        """
        Check if a profile exists.
        
        Args:
            name: Profile name
            
        Returns:
            True if the profile exists
        """
        return name in self.profiles