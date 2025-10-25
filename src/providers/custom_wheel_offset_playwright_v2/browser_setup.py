"""
Browser Setup for Custom Wheel Offset Scraper V2.
Handles browser context creation, extension loading, and stealth configuration.
"""

import logging
import os
import importlib.util
from pathlib import Path
from playwright.async_api import Browser, BrowserContext

# Import local config from the same directory
current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, 'config.py')
spec = importlib.util.spec_from_file_location("local_config", config_path)
local_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local_config)

HEADLESS = local_config.HEADLESS
VIEWPORT = local_config.VIEWPORT
USER_AGENT = local_config.USER_AGENT
BROWSER_ARGS = local_config.BROWSER_ARGS
EXTENSION_PATH = local_config.EXTENSION_PATH
DEFAULT_TIMEOUT = local_config.DEFAULT_TIMEOUT

from profile_manager import ProfileManager

logger = logging.getLogger(__name__)


class BrowserSetup:
    """Handles browser context setup with extensions and stealth configuration."""
    
    def __init__(self, profile_manager: ProfileManager):
        """
        Initialize browser setup with profile manager.
        
        Args:
            profile_manager: ProfileManager instance for handling profiles
        """
        self.profile_manager = profile_manager
        self.context = None
    
    async def create_context(self, browser: Browser) -> BrowserContext:
        """
        Create browser context with extension and stealth configuration.
        
        Args:
            browser: Playwright browser instance
            
        Returns:
            Configured browser context
        """
        # Create context with configuration (no user_data_dir here)
        self.context = await browser.new_context(
            viewport=VIEWPORT,
            user_agent=USER_AGENT,
            ignore_default_args=["--enable-blink-features=IdleDetection"]
        )
        
        # Set default timeout
        self.context.set_default_timeout(DEFAULT_TIMEOUT)
        
        # Apply stealth scripts
        await self._apply_stealth_scripts()
        
        logger.info("Browser context created successfully")
        return self.context
    
    async def _apply_stealth_scripts(self) -> None:
        """Apply stealth scripts to hide automation detection."""
        stealth_script = """
        // Hide webdriver property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
        });
        
        // Override plugins length
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });
        
        // Override languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
        });
        """
        
        await self.context.add_init_script(stealth_script)
        logger.debug("Stealth scripts applied to browser context")
    
    async def close(self) -> None:
        """Close the browser context."""
        if self.context:
            await self.context.close()
            logger.info("Browser context closed")