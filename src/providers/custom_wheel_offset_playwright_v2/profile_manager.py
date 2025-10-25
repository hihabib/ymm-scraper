"""
Profile Manager for Custom Wheel Offset Scraper V2.
Handles persistent browser profile creation, validation, and management.
"""

import os
import logging
from pathlib import Path
from typing import Tuple
import importlib.util

# Import local config from the same directory
current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, 'config.py')
spec = importlib.util.spec_from_file_location("local_config", config_path)
local_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local_config)

PROFILE_BASE_DIR = local_config.PROFILE_BASE_DIR
PROFILE_PREFIX = local_config.PROFILE_PREFIX
PROFILE_VALIDATION_FILES = local_config.PROFILE_VALIDATION_FILES

logger = logging.getLogger(__name__)


class ProfileManager:
    """Manages persistent browser profiles for scraper instances."""
    
    def __init__(self, profile_name: str):
        """
        Initialize profile manager for a specific profile.
        
        Args:
            profile_name: Name identifier for the profile
        """
        self.profile_name = profile_name
        self.profile_path = self._get_profile_path()
    
    def _get_profile_path(self) -> Path:
        """Get the full path for the profile directory."""
        return PROFILE_BASE_DIR / f"{PROFILE_PREFIX}{self.profile_name}"
    
    def profile_exists(self) -> bool:
        """Check if profile directory exists."""
        return self.profile_path.exists()
    
    def has_meaningful_data(self) -> Tuple[bool, int]:
        """
        Check if profile contains meaningful browser data.
        
        Returns:
            Tuple of (has_data: bool, file_count: int)
        """
        if not self.profile_exists():
            return False, 0
        
        existing_files = []
        for validation_file in PROFILE_VALIDATION_FILES:
            file_path = self.profile_path / validation_file
            if file_path.exists():
                existing_files.append(validation_file)
        
        file_count = len(existing_files)
        has_data = file_count > 0
        
        logger.info(f"Profile {self.profile_name}: {file_count} data files found")
        return has_data, file_count
    
    def ensure_profile_directory(self) -> None:
        """Ensure profile directory exists, create if necessary."""
        if not self.profile_exists():
            self.profile_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created new profile directory: {self.profile_path}")
        else:
            logger.info(f"Using existing profile directory: {self.profile_path}")
    
    def get_profile_path_str(self) -> str:
        """Get profile path as string for browser configuration."""
        return str(self.profile_path)
    
    def create_fresh_profile(self) -> None:
        """Create a fresh profile by removing problematic files while preserving directory structure."""
        if not self.profile_exists():
            logger.info(f"Profile {self.profile_name} doesn't exist, will create fresh")
            return
        
        logger.info(f"Creating fresh profile for {self.profile_name}")
        
        # Files and directories to remove for a fresh start
        items_to_remove = [
            "Default/LOCK",
            "Default/LOG",
            "Default/LOG.old", 
            "Default/Cache",
            "Default/GPUCache",
            "Default/Code Cache",
            "Default/Service Worker",
            "Default/Session Storage",
            "Default/Local Storage",
            "Default/IndexedDB",
            "Default/WebStorage",
            "Default/blob_storage",
            "Default/DawnGraphiteCache",
            "Default/DawnWebGPUCache",
            "Crashpad",
            "ShaderCache",
            "GrShaderCache",
            "GraphiteDawnCache"
        ]
        
        removed_count = 0
        for item in items_to_remove:
            item_path = self.profile_path / item
            if item_path.exists():
                try:
                    if item_path.is_file():
                        item_path.unlink()
                        removed_count += 1
                    elif item_path.is_dir():
                        import shutil
                        shutil.rmtree(item_path, ignore_errors=True)
                        removed_count += 1
                except Exception as e:
                    logger.warning(f"Failed to remove {item_path}: {e}")
        
        logger.info(f"Fresh profile created: removed {removed_count} items")
    
    def force_cleanup_locks(self) -> None:
        """Force cleanup of lock files and processes that might be blocking profile access."""
        logger.info(f"Force cleaning locks for profile {self.profile_name}")
        
        # Remove lock files
        lock_files = [
            "Default/LOCK",
            "Default/LOG",
            "Default/LOG.old",
            "SingletonLock",
            "lockfile"
        ]
        
        for lock_file in lock_files:
            lock_path = self.profile_path / lock_file
            if lock_path.exists():
                try:
                    lock_path.unlink()
                    logger.info(f"Removed lock file: {lock_file}")
                except Exception as e:
                    logger.warning(f"Failed to remove lock file {lock_file}: {e}")
        
        # Kill any Chrome processes that might be using this profile
        import subprocess
        import psutil
        
        try:
            # Find Chrome processes using this profile directory
            profile_str = str(self.profile_path).lower()
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                        cmdline = ' '.join(proc.info['cmdline'] or []).lower()
                        if profile_str in cmdline:
                            logger.info(f"Terminating Chrome process {proc.info['pid']} using profile")
                            proc.terminate()
                            proc.wait(timeout=5)
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
                    pass
        except Exception as e:
            logger.warning(f"Error during process cleanup: {e}")

    def validate_profile_integrity(self) -> Tuple[bool, str]:
        """
        Validate profile integrity and detect corruption.
        
        Returns:
            Tuple of (is_valid: bool, reason: str)
        """
        if not self.profile_exists():
            return True, "Profile doesn't exist - will create new"
        
        # Check for lock files that indicate corruption
        lock_file = self.profile_path / "Default" / "LOCK"
        if lock_file.exists():
            return False, "Profile has active LOCK file - may be corrupted"
        
        # Check for crash indicators
        crashpad_dir = self.profile_path / "Crashpad"
        if crashpad_dir.exists():
            crash_files = list(crashpad_dir.glob("**/*"))
            if len(crash_files) > 10:  # Too many crash files
                return False, f"Profile has {len(crash_files)} crash files - likely corrupted"
        
        # Check for essential browser files
        essential_files = [
            "Default/Preferences",
            "Local State"
        ]
        
        missing_files = []
        for file_path in essential_files:
            full_path = self.profile_path / file_path
            if not full_path.exists():
                missing_files.append(file_path)
        
        if missing_files:
            return False, f"Missing essential files: {', '.join(missing_files)}"
        
        return True, "Profile appears valid"

    def get_profile_path(self, profile_name: str = None, force_fresh: bool = False) -> Tuple[str, bool]:
        """
        Get profile path and existence status for browser setup.
        
        Args:
            profile_name: Optional profile name (uses instance name if not provided)
            force_fresh: If True, create a fresh profile by removing problematic files
            
        Returns:
            Tuple of (profile_path_str, is_existing_profile)
        """
        # Force cleanup locks before validation to prevent conflicts
        self.force_cleanup_locks()
        
        # Validate profile integrity first
        is_valid, reason = self.validate_profile_integrity()
        if not is_valid:
            logger.warning(f"Profile validation failed: {reason}")
            force_fresh = True
        
        # Create fresh profile if requested or validation failed
        if force_fresh:
            self.create_fresh_profile()
        
        # Ensure profile directory exists
        self.ensure_profile_directory()
        
        # Check if profile has meaningful data
        has_data, file_count = self.has_meaningful_data()
        
        if has_data:
            logger.info(f"Profile '{self.profile_name}' exists with {file_count} data files - existing profile")
        else:
            logger.info(f"Profile '{self.profile_name}' is new or has no meaningful data - new profile")
        
        return self.get_profile_path_str(), has_data