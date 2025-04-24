#!/usr/bin/env python
"""
Utility to migrate profiles from JSON config to LMDB
"""
import logging
import argparse
from config_manager import ConfigManager
from lmdb_config_manager import LMDBConfigManager

def setup_logging(verbose=False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def migrate_profiles_to_lmdb(verbose=False, dry_run=False):
    """
    Migrate profiles from JSON to LMDB.
    
    Args:
        verbose: Enable verbose logging
        dry_run: Perform a dry run without actually writing to LMDB
    """
    logger = setup_logging(verbose)
    
    # Initialize managers
    json_manager = ConfigManager()
    lmdb_manager = LMDBConfigManager()
    
    # Get existing profiles
    profiles = json_manager.get_profiles()
    if not profiles:
        logger.info("No profiles found to migrate")
        return
    
    logger.info(f"Found {len(profiles)} profiles to migrate")
    
    # Preview profiles if verbose
    if verbose:
        for profile in profiles:
            logger.debug(f"Profile: {profile['name']}")
    
    if dry_run:
        logger.info("Dry run completed, no changes made")
        return
        
    # Migrate each profile
    success_count = 0
    for profile in profiles:
        name = profile.get("name")
        if not name:
            logger.warning(f"Skipping profile without name: {profile}")
            continue
            
        # Check if profile already exists in LMDB
        if lmdb_manager.get_profile(name):
            logger.warning(f"Profile already exists in LMDB: {name}")
            continue
            
        if lmdb_manager.add_profile(profile):
            logger.info(f"Migrated profile: {name}")
            success_count += 1
        else:
            logger.error(f"Failed to migrate profile: {name}")
    
    # Migrate last used profile
    last_used = json_manager.get_last_used_profile()
    if last_used and "name" in last_used:
        if lmdb_manager.set_last_used_profile(last_used["name"]):
            logger.info(f"Set last used profile: {last_used['name']}")
    
    # Verify migration
    lmdb_profiles = lmdb_manager.get_profiles()
    logger.info(f"LMDB now has {len(lmdb_profiles)} profiles ({success_count} newly migrated)")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Migrate CSV to Iceberg profiles from JSON to LMDB")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--dry-run", "-d", action="store_true", help="Perform a dry run without making changes")
    
    args = parser.parse_args()
    migrate_profiles_to_lmdb(verbose=args.verbose, dry_run=args.dry_run)

if __name__ == "__main__":
    main()