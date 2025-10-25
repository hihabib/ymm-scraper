"""
Custom Wheel Offset Playwright Provider Configuration

This file contains all browser and provider-specific settings for the
Custom Wheel Offset Playwright scraper.
"""

# Browser Configuration
# Change these settings to affect all Custom Wheel Offset browser instances
HEADLESS: bool = False
VIEWPORT = {"width": 1920, "height": 1080}
USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Extension path (if you use one). Keep or change as needed.
EXTENSION_PATH: str = (
    "E:/scraper/src/providers/custom_wheel_offset_playwright/extension/"
    "ifibfemgeogfhoebkmokieepdoobkbpo/3.7.2_0"
)

# Optional extra Chromium args. If empty, sensible defaults are applied.
# If you set this list, it will be used as-is for all workers.
BROWSER_ARGS = []

# Provider-specific settings
PROVIDER_NAME = "custom_wheel_offset_playwright"
BASE_URL = "https://www.customwheeloffset.com"