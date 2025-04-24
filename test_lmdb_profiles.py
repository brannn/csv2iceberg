#!/usr/bin/env python
"""
Test script for LMDB profile store
"""
import os
import json
import shutil
import logging
from lmdb_store import LMDBProfileStore

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Test LMDB profile store functionality"""
    # Path for testing
    test_path = "/tmp/csv_to_iceberg_test_lmdb"
    
    # Remove any existing test database
    if os.path.exists(test_path):
        logger.info(f"Removing existing test database at {test_path}")
        shutil.rmtree(test_path)
    
    # Create store
    logger.info("Creating LMDB profile store")
    store = LMDBProfileStore(path=test_path)
    
    # Test adding profiles
    profile1 = {
        "name": "test_profile_1",
        "trino_host": "localhost",
        "trino_port": 8080,
        "trino_user": "test_user",
        "trino_password": "test_password",
        "trino_catalog": "iceberg",
        "trino_schema": "default"
    }
    
    profile2 = {
        "name": "test_profile_2",
        "trino_host": "remote-host",
        "trino_port": 443,
        "trino_user": "admin",
        "trino_password": "admin_pass",
        "trino_catalog": "hive",
        "trino_schema": "prod"
    }
    
    logger.info("Adding profiles")
    result1 = store.add_profile(profile1)
    result2 = store.add_profile(profile2)
    assert result1, "Failed to add profile1"
    assert result2, "Failed to add profile2"
    logger.info("Successfully added profiles")
    
    # Test getting profiles
    logger.info("Getting profiles")
    profiles = store.get_profiles()
    assert len(profiles) == 2, f"Expected 2 profiles, got {len(profiles)}"
    logger.info(f"Found {len(profiles)} profiles as expected")
    
    # Test getting specific profile
    logger.info("Getting specific profile")
    p1 = store.get_profile("test_profile_1")
    assert p1 and p1["trino_host"] == "localhost", "Failed to get correct profile"
    logger.info(f"Successfully retrieved profile: {json.dumps(p1, indent=2)}")
    
    # Test setting and getting last used profile
    logger.info("Setting last used profile")
    result = store.set_last_used_profile("test_profile_2")
    assert result, "Failed to set last used profile"
    
    last_used = store.get_last_used_profile()
    assert last_used and last_used["name"] == "test_profile_2", "Failed to get correct last used profile"
    logger.info(f"Last used profile: {last_used['name']}")
    
    # Test updating profile
    logger.info("Updating profile")
    p1["trino_port"] = 9090
    result = store.update_profile("test_profile_1", p1)
    assert result, "Failed to update profile"
    
    updated = store.get_profile("test_profile_1")
    assert updated and updated["trino_port"] == 9090, "Failed to update profile correctly"
    logger.info(f"Successfully updated port to: {updated['trino_port']}")
    
    # Test deleting profile
    logger.info("Deleting profile")
    result = store.delete_profile("test_profile_1")
    assert result, "Failed to delete profile"
    
    profiles_after_delete = store.get_profiles()
    assert len(profiles_after_delete) == 1, f"Expected 1 profile after deletion, got {len(profiles_after_delete)}"
    assert not store.get_profile("test_profile_1"), "Profile should have been deleted"
    logger.info("Profile deleted successfully")
    
    # Close the store
    store.close()
    logger.info("All tests passed!")

if __name__ == "__main__":
    main()