"""
Driver Right Data API configuration.
Contains credentials and settings for the Driver Right Data scraper.
"""

# Driver Right Data API credentials
DRIVER_RIGHT_USERNAME: str = "Tire_Wheel_Experts"
DRIVER_RIGHT_SECURITY_TOKEN: str = "0b035d5ccecc43f2a9adce9849c7024e"
DRIVER_RIGHT_BASE_URL: str = "https://api.driverightdata.com/eu/api"

# Default region ID for API calls
DRIVER_RIGHT_DEFAULT_REGION_ID: int = 1

# Request timeout settings (in seconds)
DRIVER_RIGHT_REQUEST_TIMEOUT: int = 30
DRIVER_RIGHT_MAX_RETRIES: int = 3
DRIVER_RIGHT_RETRY_DELAY: float = 1.0

# Aliases for easier import (matching the expected names in utils.py)
USERNAME = DRIVER_RIGHT_USERNAME
SECURITY_TOKEN = DRIVER_RIGHT_SECURITY_TOKEN
BASE_URL = DRIVER_RIGHT_BASE_URL
DEFAULT_REGION_ID = DRIVER_RIGHT_DEFAULT_REGION_ID
REQUEST_TIMEOUT = DRIVER_RIGHT_REQUEST_TIMEOUT
MAX_RETRIES = DRIVER_RIGHT_MAX_RETRIES

__all__ = [
    "DRIVER_RIGHT_USERNAME",
    "DRIVER_RIGHT_SECURITY_TOKEN", 
    "DRIVER_RIGHT_BASE_URL",
    "DRIVER_RIGHT_DEFAULT_REGION_ID",
    "DRIVER_RIGHT_REQUEST_TIMEOUT",
    "DRIVER_RIGHT_MAX_RETRIES",
    "DRIVER_RIGHT_RETRY_DELAY",
    # Aliases
    "USERNAME",
    "SECURITY_TOKEN",
    "BASE_URL",
    "DEFAULT_REGION_ID",
    "REQUEST_TIMEOUT",
    "MAX_RETRIES",
]