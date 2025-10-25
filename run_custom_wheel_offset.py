#!/usr/bin/env python3
"""
Wrapper script to run the Custom Wheel Offset Playwright scraper.
Supports concurrent workers with per-worker persistent profiles.
"""

import sys
import os
import asyncio
from pathlib import Path

# Add the src directory to Python path
SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

# Change working directory to project root
os.chdir(SCRIPT_DIR)


async def main():
    """Run the complete workflow with preparation and data distribution."""
    try:
        from config.worker import CUSTOM_WHEEL_OFFSET_WORKERS
    except Exception:
        CUSTOM_WHEEL_OFFSET_WORKERS = 5
    
    n = max(1, int(CUSTOM_WHEEL_OFFSET_WORKERS))
    
    # Import the main function from the provider
    from providers.custom_wheel_offset_playwright.custom_wheel_offset_playwright import main as provider_main
    
    # Run the complete workflow
    await provider_main(workers=n)


if __name__ == "__main__":
    asyncio.run(main())