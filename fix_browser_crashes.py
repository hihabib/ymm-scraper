"""
Browser Profile Cleanup and Recovery Script
Fixes the repeated browser crashes by cleaning corrupted profiles and implementing safer launch.
"""

import os
import shutil
import logging
import time
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def cleanup_corrupted_profile(profile_path: str) -> bool:
    """
    Clean up a corrupted browser profile by removing crash data and temporary files.
    
    Args:
        profile_path: Path to the browser profile directory
        
    Returns:
        True if cleanup was successful, False otherwise
    """
    try:
        profile_dir = Path(profile_path)
        
        if not profile_dir.exists():
            logger.info(f"Profile directory does not exist: {profile_path}")
            return True
        
        # Remove crash data
        crashpad_dir = profile_dir / "Crashpad"
        if crashpad_dir.exists():
            reports_dir = crashpad_dir / "reports"
            if reports_dir.exists():
                crash_files = list(reports_dir.glob('*.dmp'))
                logger.info(f"Removing Crashpad directory with {len(crash_files)} crash dumps...")
            else:
                logger.info("Removing Crashpad directory...")
            shutil.rmtree(crashpad_dir)
            logger.info("Crashpad directory removed successfully")
        
        # Remove other problematic directories
        problematic_dirs = [
            "ShaderCache",
            "GrShaderCache", 
            "GraphiteDawnCache",
            "BrowserMetrics",
            "DeferredBrowserMetrics"
        ]
        
        for dir_name in problematic_dirs:
            dir_path = profile_dir / dir_name
            if dir_path.exists():
                logger.info(f"Removing {dir_name} directory...")
                shutil.rmtree(dir_path)
        
        # Remove lock files
        lock_files = [
            "SingletonLock",
            "SingletonSocket", 
            "SingletonCookie"
        ]
        
        for lock_file in lock_files:
            lock_path = profile_dir / lock_file
            if lock_path.exists():
                logger.info(f"Removing lock file: {lock_file}")
                lock_path.unlink()
        
        logger.info(f"Profile cleanup completed: {profile_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning up profile {profile_path}: {e}")
        return False

def backup_and_reset_profile(profile_path: str) -> bool:
    """
    Create a backup of the current profile and reset it.
    
    Args:
        profile_path: Path to the browser profile directory
        
    Returns:
        True if backup and reset was successful, False otherwise
    """
    try:
        profile_dir = Path(profile_path)
        
        if not profile_dir.exists():
            logger.info(f"Profile directory does not exist: {profile_path}")
            return True
        
        # Create backup
        backup_path = f"{profile_path}_backup_{int(time.time())}"
        logger.info(f"Creating backup: {backup_path}")
        shutil.copytree(profile_path, backup_path)
        
        # Remove original profile
        logger.info(f"Removing corrupted profile: {profile_path}")
        shutil.rmtree(profile_path)
        
        logger.info("Profile backup and reset completed")
        return True
        
    except Exception as e:
        logger.error(f"Error backing up and resetting profile {profile_path}: {e}")
        return False

def main():
    """Main function to clean up all worker profiles."""
    base_profile_dir = Path("e:/scraper/src/data")
    
    # Find all worker profiles
    worker_profiles = list(base_profile_dir.glob("chromium_profile_worker_*"))
    
    logger.info(f"Found {len(worker_profiles)} worker profiles to clean")
    
    for profile_path in worker_profiles:
        logger.info(f"Processing profile: {profile_path}")
        
        # Try cleanup first
        if cleanup_corrupted_profile(str(profile_path)):
            logger.info(f"Successfully cleaned profile: {profile_path}")
        else:
            logger.warning(f"Cleanup failed for {profile_path}, attempting full reset...")
            backup_and_reset_profile(str(profile_path))
    
    logger.info("All profiles processed")

if __name__ == "__main__":
    main()