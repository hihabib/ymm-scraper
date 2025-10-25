"""
Human Behavior Simulator for Custom Wheel Offset Scraper V2.
Orchestrates human-like activities using delay and mouse utilities.
"""

import random
import logging
import os
import importlib.util
from playwright.async_api import Page

# Import local config from the same directory
current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, 'config.py')
spec = importlib.util.spec_from_file_location("local_config", config_path)
local_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local_config)

SCROLL_DISTANCE_RANGE = local_config.SCROLL_DISTANCE_RANGE

from utils.delays import human_delay, network_delay, random_pause
from utils.mouse import click_safe_area, move_mouse_naturally

logger = logging.getLogger(__name__)


class HumanBehavior:
    """Simulates human-like behavior on web pages."""
    
    def __init__(self, page: Page):
        """
        Initialize human behavior simulator.
        
        Args:
            page: Playwright page instance
        """
        self.page = page
    
    async def simulate_reading(self) -> None:
        """Simulate reading behavior with pauses and scrolling."""
        logger.info("Simulating reading behavior")
        
        # Initial pause as if reading the page
        await random_pause()
        
        # Random scrolling to simulate reading
        for _ in range(random.randint(2, 5)):
            scroll_distance = random.randint(*SCROLL_DISTANCE_RANGE)
            await self.page.mouse.wheel(0, scroll_distance)
            await human_delay(0.5, 2.0)
        
        # Final pause
        await random_pause()
    
    async def simulate_exploration(self) -> None:
        """Simulate page exploration with mouse movements and clicks."""
        logger.info("Simulating page exploration")
        
        # Random mouse movements and safe clicks
        for _ in range(random.randint(1, 3)):
            await click_safe_area(self.page)
            await human_delay()
        
        # Network delay to simulate thinking
        await network_delay()
    
    async def simulate_typing_behavior(self, text: str) -> None:
        """
        Simulate human typing with natural delays.
        
        Args:
            text: Text to type
        """
        logger.info(f"Simulating typing: {len(text)} characters")
        
        # Type with human-like delays between characters
        for char in text:
            await self.page.keyboard.type(char)
            # Small random delay between keystrokes
            delay = random.uniform(0.05, 0.2)
            await self.page.wait_for_timeout(int(delay * 1000))
    
    async def perform_full_simulation(self) -> None:
        """Perform a complete human behavior simulation sequence."""
        logger.info("Starting full human behavior simulation")
        
        # Wait for page to load
        await network_delay()
        
        # Simulate reading the page
        await self.simulate_reading()
        
        # Simulate exploration
        await self.simulate_exploration()
        
        # Final pause before any real actions
        await random_pause()
        
        logger.info("Human behavior simulation completed")
    
    async def perform_enhanced_human_simulation(self) -> None:
        """Perform enhanced human behavior simulation with Google search, Wikipedia, and Amazon visits."""
        logger.info("Starting enhanced human behavior simulation")
        
        try:
            # Step 1: Google search for "custom wheel offset"
            logger.info("Performing Google search for 'custom wheel offset'")
            await self.page.goto("https://www.google.com", timeout=30000)
            await human_delay(2, 4)
            
            # Accept cookies if present
            try:
                accept_button = await self.page.wait_for_selector('button:has-text("Accept all"), button:has-text("I agree"), button:has-text("Accept")', timeout=3000)
                if accept_button:
                    await accept_button.click()
                    await human_delay(1, 2)
            except:
                pass  # No cookies dialog or already accepted
            
            # Find search box and search
            search_box = await self.page.wait_for_selector('input[name="q"], textarea[name="q"]', timeout=10000)
            await search_box.click()
            await human_delay(0.5, 1)
            
            # Type search query with human-like typing
            await self.simulate_typing_behavior("custom wheel offset")
            await human_delay(1, 2)
            
            # Press Enter to search
            await self.page.keyboard.press('Enter')
            await self.page.wait_for_load_state('networkidle', timeout=15000)
            await human_delay(2, 4)
            
            # Simulate reading search results
            await self.simulate_reading()
            
            # Step 2: Visit Wikipedia
            logger.info("Visiting Wikipedia")
            await self.page.goto("https://en.wikipedia.org/wiki/Wheel", timeout=30000)
            await human_delay(2, 4)
            
            # Simulate reading Wikipedia page
            await self.simulate_reading()
            
            # Random scroll and exploration
            await self.simulate_exploration()
            
            # Step 3: Visit Amazon
            logger.info("Visiting Amazon")
            await self.page.goto("https://www.amazon.com", timeout=30000)
            await human_delay(2, 4)
            
            # Handle location/zip code popup if present
            try:
                location_button = await self.page.wait_for_selector('a[data-csa-c-content-id="sw_skip_loc"], button:has-text("Not now"), .a-popover-close', timeout=3000)
                if location_button:
                    await location_button.click()
                    await human_delay(1, 2)
            except:
                pass  # No location popup
            
            # Search for wheels on Amazon
            try:
                amazon_search = await self.page.wait_for_selector('input[id="twotabsearchtextbox"]', timeout=10000)
                await amazon_search.click()
                await human_delay(0.5, 1)
                await self.simulate_typing_behavior("car wheels")
                await human_delay(1, 2)
                await self.page.keyboard.press('Enter')
                await self.page.wait_for_load_state('networkidle', timeout=15000)
                await human_delay(2, 4)
                
                # Simulate browsing Amazon results
                await self.simulate_reading()
                await self.simulate_exploration()
            except:
                # If Amazon search fails, just simulate general browsing
                await self.simulate_reading()
                await self.simulate_exploration()
            
            # Step 4: Additional random browsing behavior
            logger.info("Performing additional browsing behavior")
            
            # Random mouse movements and clicks
            for _ in range(random.randint(2, 4)):
                await move_mouse_naturally(self.page)
                await human_delay(0.5, 1.5)
            
            # Random scrolling
            for _ in range(random.randint(3, 6)):
                scroll_distance = random.randint(*SCROLL_DISTANCE_RANGE)
                direction = random.choice([-1, 1])  # Up or down
                await self.page.mouse.wheel(0, scroll_distance * direction)
                await human_delay(0.5, 2.0)
            
            # Final pause before proceeding to main work
            await human_delay(3, 6)
            
            logger.info("Enhanced human behavior simulation completed successfully")
            
        except Exception as e:
            logger.warning(f"Enhanced human simulation encountered error: {e}")
            logger.info("Falling back to basic human simulation")
            # Fallback to basic simulation if enhanced fails
            await self.perform_full_simulation()
    
    async def quick_human_check(self) -> None:
        """Perform a quick human-like check without full simulation."""
        logger.info("Performing quick human check")
        
        # Brief pause and single scroll
        await human_delay(1, 2)
        await self.page.mouse.wheel(0, random.randint(50, 200))
        await human_delay(0.5, 1)
        
        logger.info("Quick human check completed")