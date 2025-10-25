#!/usr/bin/env python3
"""
Configuration manager for Custom Wheel Offset scraper.
Centralizes environment variable parsing and configuration setup.
"""

import os
from typing import Dict, Any


def parse_bool_env(env_var: str, default: bool = False) -> bool:
    """Parse boolean environment variable."""
    value = os.getenv(env_var, "")
    if not value:  # If environment variable is not set, use default
        return default
    return value.lower() in ("true", "1", "yes", "on")


def parse_int_env(env_var: str, default: int = 0) -> int:
    """Parse integer environment variable."""
    try:
        return int(os.getenv(env_var, str(default)))
    except ValueError:
        return default


def get_config() -> Dict[str, Any]:
    """Get configuration settings from environment variables."""
    # Import worker config
    try:
        from src.config.worker import CUSTOM_WHEEL_OFFSET_WORKERS
        default_workers = CUSTOM_WHEEL_OFFSET_WORKERS
    except ImportError:
        try:
            # Try adding the parent directory to sys.path
            import sys
            import os
            parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            from src.config.worker import CUSTOM_WHEEL_OFFSET_WORKERS
            default_workers = CUSTOM_WHEEL_OFFSET_WORKERS
        except ImportError:
            default_workers = 200
    
    # Parse basic config
    fast_mode = parse_bool_env("FAST", False)
    
    # In FAST mode, disable vehicle data fetching by default but allow override
    if fast_mode:
        fetch_vehicle_data_default = False
    else:
        fetch_vehicle_data_default = True
    
    return {
        "fast": fast_mode,
        "pref_fetch": parse_bool_env("PREF_FETCH", True),
        "fetch_vehicle_data": parse_bool_env("FETCH_VEHICLE_DATA", True),  # Always default to True
        "multithreading": parse_bool_env("MULTITHREADING", True),
        "realtime_processing": parse_bool_env("REALTIME_PROCESSING", False),
        "use_integrated": parse_bool_env("USE_INTEGRATED", False),
        "workers": parse_int_env("WORKERS", default_workers),
        "skip_existing": parse_bool_env("SKIP_EXISTING", True),
        "restart_on_error": parse_bool_env("RESTART_ON_ERROR", True),
        "max_retries": parse_int_env("MAX_RETRIES", 3),
        "timeout": parse_int_env("TIMEOUT", 3600),  # 1 hour default
    }