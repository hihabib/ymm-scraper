"""
Configuration settings for Custom Wheel Offset Scraper V2.
Independent configuration module with browser settings and constants.
"""

import os
from pathlib import Path

# Base configuration
PROVIDER_NAME = "custom_wheel_offset_v2"
BASE_URL = "https://www.customwheeloffset.com/store/wheels"

# Browser configuration
HEADLESS = True
VIEWPORT = {"width": 1920, "height": 1080}
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Browser arguments for stealth and stability
BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-first-run",
    "--no-default-browser-check",
    "--password-store=basic",
    "--use-mock-keychain",
    "--disable-crash-reporter",
    "--disable-breakpad",
    "--disable-gpu",
    "--disable-background-networking",
    "--disable-default-apps",
    "--disable-sync",
    "--disable-translate",
    "--hide-scrollbars",
    "--mute-audio",
    "--disable-web-security",
    "--disable-features=VizDisplayCompositor",
    "--disable-ipc-flooding-protection",
]

# Extension configuration
CURRENT_DIR = Path(__file__).parent
EXTENSION_PATH = CURRENT_DIR / "extension"

# Profile configuration
PROFILE_BASE_DIR = Path("e:/scraper/src/data")
PROFILE_PREFIX = "chromium_profile_"

# Profile validation files - these indicate a meaningful browser profile
PROFILE_VALIDATION_FILES = [
    "Default/Cookies",
    "Default/History", 
    "Default/Preferences",
    "Default/Web Data"
]

# Timing configuration
DEFAULT_TIMEOUT = 30000  # 30 seconds
NAVIGATION_TIMEOUT = 60000  # 60 seconds
HUMAN_DELAY_RANGE = (1, 3)  # seconds
NETWORK_DELAY_RANGE = (0.5, 2.0)  # seconds

# Human behavior configuration
MOUSE_MOVE_STEPS = 20
SCROLL_DISTANCE_RANGE = (100, 500)
CLICK_SAFE_MARGIN = 50  # pixels from edge

# Logging configuration
LOG_LEVEL = "INFO"