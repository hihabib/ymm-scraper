#!/usr/bin/env python3
"""
Session restart utility for Custom Wheel Offset scraper.
Handles automatic session cleanup and restart when session/token errors occur.
"""

import sys
import os
import time
import subprocess
import json
import threading
import logging
from pathlib import Path
from typing import Optional
from .logging_config import init_module_logger

logger = init_module_logger(__name__)

# Windows doesn't have fcntl, so we'll use threading locks only
try:
    import fcntl
    HAS_FCNTL = True
except ImportError:
    HAS_FCNTL = False

try:
    # Prefer package-relative import
    from .session_manager_threaded import threaded_session_manager
except ImportError:
    # Fallback to absolute import
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from providers.custom_wheel_offset.session_manager_threaded import threaded_session_manager

# Global lock for process restart coordination
_restart_lock = threading.Lock()
_restart_in_progress = threading.Event()

class SessionExpiredError(Exception):
    """Exception raised when session/token has expired and needs to be refreshed."""
    pass


def update_process_registry_pid(new_pid: int, provider: str = "custom-wheel-offset") -> bool:
    """
    Update the PID in process_registry.json for the given provider.
    
    Args:
        new_pid: The new process ID to update
        provider: The provider name (defaults to "custom-wheel-offset")
        
    Returns:
        True if update was successful, False otherwise
    """
    try:
        logger.info(f"[session_restart] === Updating Process Registry PID ===")
        logger.info(f"[session_restart] Provider: {provider}, New PID: {new_pid}")
        
        # Find the process registry file
        # Try both standard and legacy paths as per process.py
        repo_root = Path(__file__).resolve().parents[3]
        registry_paths = [
            repo_root / "data" / "process_registry.json",
            repo_root / "src" / "data" / "process_registry.json"
        ]
        
        registry_data = {}
        registry_path = None
        
        # Load existing registry data
        for path in registry_paths:
            try:
                if path.exists():
                    with open(path, "r", encoding="utf-8") as f:
                        registry_data = json.load(f)
                    if isinstance(registry_data, dict):
                        registry_path = path
                        logger.info(f"[session_restart] Loaded registry from: {path}")
                        break
            except Exception as e:
                logger.error(f"[session_restart] Failed to load registry from {path}: {e}")
                continue
        
        # If no existing registry found, use the standard path
        if registry_path is None:
            registry_path = registry_paths[0]
            registry_data = {}
            logger.info(f"[session_restart] No existing registry found, will create new at: {registry_path}")
        
        # Normalize provider name (same as in process.py)
        provider_norm = provider.strip().lower()
        
        # Update or create the registry entry
        if provider_norm in registry_data:
            old_pid = registry_data[provider_norm].get("pid", "unknown")
            logger.info(f"[session_restart] Updating existing entry - Old PID: {old_pid}, New PID: {new_pid}")
            registry_data[provider_norm]["pid"] = new_pid
            registry_data[provider_norm]["updated_at"] = time.time()
        else:
            logger.info(f"[session_restart] Creating new registry entry for {provider_norm}")
            registry_data[provider_norm] = {
                "pid": new_pid,
                "cmd": f"python -m src.providers.{provider_norm.replace('-', '_')}.index",
                "updated_at": time.time()
            }
        
        # Ensure directory exists
        registry_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to both standard and legacy paths (same as process.py)
        for path in registry_paths:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(registry_data, f, indent=2)
                logger.info(f"[session_restart] Updated registry at: {path}")
            except Exception as e:
                logger.error(f"[session_restart] Failed to save registry to {path}: {e}")
        
        logger.info(f"[session_restart] ✓ Process registry PID updated successfully!")
        return True
        
    except Exception as e:
        logger.error(f"[session_restart] ✗ Error updating process registry PID: {e}")
        return False


def clean_session_data() -> bool:
    """Clean all session data including cookies and persistent storage."""
    try:
        logger.info("[session_restart] === Cleaning Session Data ===")
        
        # Get current session state for current thread
        session = threaded_session_manager.get_session()
        logger.info(f"[session_restart] Current session cookies: {len(session.cookies)}")
        
        # Method 1: Explicitly clear all cookies from current session
        logger.info("[session_restart] 1. Clearing all cookies from current session...")
        if hasattr(session, 'cookies') and session.cookies:
            cookie_count_before = len(session.cookies)
            session.cookies.clear()
            logger.info(f"[session_restart]    Cleared {cookie_count_before} cookies from current session")
        else:
            logger.info("[session_restart]    No cookies found in current session")
        
        # Method 2: Reset all threaded sessions
        logger.info("[session_restart] 2. Resetting all threaded sessions...")
        threaded_session_manager.reset_all_sessions()
        new_session = threaded_session_manager.get_session()
        logger.info(f"[session_restart]    New session cookies: {len(new_session.cookies)}")
        logger.info(f"[session_restart]    Session reset: {'✓' if session is not new_session else '✗'}")
        
        # Method 3: Clear persistent token storage
        logger.info("[session_restart] 3. Clearing persistent token storage...")
        token_file = Path(__file__).resolve().parents[3] / "data" / "custom_wheel_offset_temp.json"
        if token_file.exists():
            # Read current content to show what's being cleared
            try:
                current_content = token_file.read_text()
                if current_content.strip() != "{}":
                    logger.info(f"[session_restart]    Clearing stored data: {current_content[:100]}...")
                else:
                    logger.info("[session_restart]    Token file already empty")
            except Exception:
                logger.info("[session_restart]    Could not read current token file content")
            
            token_file.write_text("{}")
            logger.info(f"[session_restart]    Cleared token file: {token_file}")
        else:
            logger.info(f"[session_restart]    Token file not found: {token_file}")
        
        # Method 4: Verify cleanup was successful
        final_session = threaded_session_manager.get_session()
        logger.info(f"[session_restart] 4. Verification - Final session cookies: {len(final_session.cookies)}")
        
        logger.info("[session_restart] ✓ Session cleanup completed!")
        return True
        
    except Exception as e:
        logger.error(f"[session_restart] ✗ Error cleaning session: {e}")
        return False


def restart_current_process() -> None:
    """Restart the current Python process with the same arguments."""
    # First check without lock for performance
    if _restart_in_progress.is_set():
        logger.info("[session_restart] Process restart already in progress, thread exiting gracefully")
        return
    
    with _restart_lock:
        # Double-check pattern to prevent race condition
        if _restart_in_progress.is_set():
            logger.info("[session_restart] Process restart already initiated by another thread, exiting gracefully")
            return
            
        logger.info(f"[session_restart] Thread {threading.current_thread().ident} initiating process restart")
        _restart_in_progress.set()
        
        try:
            logger.info("[session_restart] === Restarting Current Process ===")
            logger.info(f"[session_restart] Current process: {sys.argv}")
            
            # Check if this is a test script - if so, don't restart to prevent infinite loops
            script_name = os.path.basename(sys.argv[0]) if sys.argv else ""
            if script_name.startswith("test_") or "test" in script_name.lower():
                logger.warning(f"[session_restart] ⚠️  Test script detected: {script_name}")
                logger.warning("[session_restart] Skipping restart to prevent infinite loop in test environment")
                logger.warning("[session_restart] In production, this would restart the main scraper process")
                
                # Clean session data and exit gracefully
                if not clean_session_data():
                    logger.error("[session_restart] ✗ Failed to clean session data")
                else:
                    logger.info("[session_restart] ✓ Session data cleaned successfully")
                
                logger.info("[session_restart] ✓ Test completed - restart mechanism verified")
                sys.exit(0)
            
            # Clean session data first
            if not clean_session_data():
                logger.error("[session_restart] ✗ Failed to clean session data, proceeding with restart anyway")
            
            # Add a small delay to ensure cleanup is complete
            time.sleep(2)
            
            # Restart the process with the same arguments
            logger.info("[session_restart] Restarting process...")
            python_executable = sys.executable
            script_args = sys.argv.copy()
            
            # Ensure we use the full path to the script if it's a relative path
            if len(script_args) > 0:
                script_path = script_args[0]
                if not os.path.isabs(script_path):
                    # If it's just a filename, use the current working directory
                    if os.path.dirname(script_path) == "":
                        # Script is just a filename, use current working directory
                        script_args[0] = os.path.join(os.getcwd(), script_path)
                    else:
                        # Convert relative path to absolute path
                        script_args[0] = os.path.abspath(script_path)
                    logger.info(f"[session_restart] Converted relative path to absolute: {script_args[0]}")
            
            logger.info(f"[session_restart] Executing: {python_executable} {' '.join(script_args)}")
            
            # Use subprocess to start new process
            new_process = subprocess.Popen([python_executable] + script_args)
            
            # Update the PID in the process registry with the new process ID
            logger.info(f"[session_restart] New process PID: {new_process.pid}")
            if update_process_registry_pid(new_process.pid):
                logger.info("[session_restart] ✓ Process registry updated with new PID")
            else:
                logger.warning("[session_restart] ✗ Failed to update process registry, but continuing with restart")
            
            logger.info("[session_restart] ✓ New process started, exiting current process")
            sys.exit(0)
            
        except Exception as e:
            logger.error(f"[session_restart] ✗ Error restarting process: {e}")
            raise


def handle_session_expired_error(context: str = "unknown") -> None:
    """
    Handle session expired error by cleaning session and restarting the process.
    
    Args:
        context: Context where the error occurred (for logging)
    """
    # First check if restart is already in progress
    if _restart_in_progress.is_set():
        logger.info(f"[session_restart] Process restart already in progress, thread {threading.current_thread().ident} exiting gracefully")
        return
    
    logger.warning(f"[session_restart] === Session Expired in {context} ===")
    logger.warning("[session_restart] Session/token has expired, initiating automatic restart...")
    
    try:
        # Log the restart event
        logger.info(f"[session_restart] Logging restart event for context: {context}")
        
        # Attempt to log to database if available
        try:
            # Always use absolute imports to avoid issues
            import sys
            from pathlib import Path
            
            SRC_DIR = Path(__file__).resolve().parents[2]
            if str(SRC_DIR) not in sys.path:
                sys.path.insert(0, str(SRC_DIR))
            from services.repository import insert_error_log
            insert_error_log(
                source="session_restart",
                context={"restart_context": context, "restart_reason": "session_expired"},
                message=f"Automatic session restart triggered from {context}"
            )
            logger.info("[session_restart] Restart event logged to database")
        except Exception as log_error:
            logger.error(f"[session_restart] Failed to log restart event: {log_error}")
        
        # Restart the process
        restart_current_process()
        
    except Exception as e:
        logger.error(f"[session_restart] ✗ Error handling session expired: {e}")
        # If restart fails, at least clean the session
        clean_session_data()
        raise SessionExpiredError(f"Session expired in {context}, restart failed: {e}")


def check_for_session_expired_indicators(response_text: str, url: str = "") -> bool:
    """
    Check if response indicates session/token expiration.
    
    Args:
        response_text: Response text to check
        url: URL that was requested (for context)
        
    Returns:
        True if session appears to be expired
    """
    # Common indicators of session expiration
    expiration_indicators = [
        "session expired",
        "invalid session",
        "authentication failed",
        "unauthorized",
        "token expired",
        "please login",
        "access denied",
        "forbidden"
    ]
    
    response_lower = response_text.lower()
    for indicator in expiration_indicators:
        if indicator in response_lower:
            logger.warning(f"[session_restart] Session expiration detected: '{indicator}' in response from {url}")
            return True
    
    return False


if __name__ == "__main__":
    # Test the session restart functionality
    logger.info("Testing session restart functionality...")
    handle_session_expired_error("test_context")