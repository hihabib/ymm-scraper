import asyncio
import logging
import random
import time
from playwright.async_api import Page

try:
    from .human_utils import human_delay, human_mouse_movement, human_scroll, network_delay
except ImportError:
    from human_utils import human_delay, human_mouse_movement, human_scroll, network_delay

# Global variable to track last human verification log time
_last_human_verification_log_time = 0
_HUMAN_VERIFICATION_LOG_INTERVAL = 60  # Log once per minute (60 seconds)


async def _wait_until_not_human_verification(page: Page, logger: logging.Logger, max_wait_seconds: int | None = None) -> None:
    """Loop until title is not 'Human Verification'. Optional timeout."""
    global _last_human_verification_log_time
    start = asyncio.get_event_loop().time()
    while True:
        try:
            title = await page.title()
            if title != "Human Verification":
                return
            
            # Rate limit the human verification logging
            current_time = time.time()
            if current_time - _last_human_verification_log_time >= _HUMAN_VERIFICATION_LOG_INTERVAL:
                logger.info("Human Verification detected; waiting for captcha to auto-solve...")
                _last_human_verification_log_time = current_time
            
            # Give the page time to reload after captcha
            try:
                await page.wait_for_load_state('load', timeout=15000)
            except Exception:
                await asyncio.sleep(2)
            try:
                await page.wait_for_load_state('networkidle', timeout=20000)
            except Exception:
                await asyncio.sleep(2)
        except Exception as e:
            logger.debug(f"Title check error: {e}")
            await asyncio.sleep(2)
        if max_wait_seconds is not None and (asyncio.get_event_loop().time() - start) > max_wait_seconds:
            raise TimeoutError("Exceeded max wait while solving Human Verification captcha")


async def _wait_until_not_human_verification_infinite(page: Page, logger: logging.Logger) -> None:
    """
    Loop until title is not 'Human Verification' with infinite retry.
    If captcha is not solved within 4 minutes, reload the page and continue waiting.
    This process continues infinitely until captcha is solved.
    """
    global _last_human_verification_log_time
    cycle_count = 0
    
    while True:
        cycle_count += 1
        logger.info(f"Starting captcha solving cycle #{cycle_count}")
        
        # Wait for 4 minutes (240 seconds) for captcha to be solved
        cycle_start = asyncio.get_event_loop().time()
        captcha_timeout = 240  # 4 minutes
        
        while True:
            try:
                title = await page.title()
                if title != "Human Verification":
                    logger.info(f"Captcha solved successfully in cycle #{cycle_count}!")
                    return
                
                # Rate limit the human verification logging
                current_time = time.time()
                if current_time - _last_human_verification_log_time >= _HUMAN_VERIFICATION_LOG_INTERVAL:
                    elapsed_in_cycle = asyncio.get_event_loop().time() - cycle_start
                    remaining_time = max(0, captcha_timeout - elapsed_in_cycle)
                    logger.info(f"Human Verification detected (cycle #{cycle_count}); waiting for captcha to auto-solve... ({remaining_time:.0f}s remaining)")
                    _last_human_verification_log_time = current_time
                
                # Give the page time to reload after captcha
                try:
                    await page.wait_for_load_state('load', timeout=15000)
                except Exception:
                    await asyncio.sleep(2)
                try:
                    await page.wait_for_load_state('networkidle', timeout=20000)
                except Exception:
                    await asyncio.sleep(2)
            except Exception as e:
                logger.debug(f"Title check error in cycle #{cycle_count}: {e}")
                await asyncio.sleep(2)
            
            # Check if 4 minutes have passed
            if (asyncio.get_event_loop().time() - cycle_start) > captcha_timeout:
                logger.warning(f"Captcha not solved within 4 minutes (cycle #{cycle_count}). Reloading page...")
                break
        
        # Reload the page after 4 minutes timeout
        try:
            current_url = page.url
            logger.info(f"Reloading page: {current_url}")
            await page.reload(wait_until='domcontentloaded')
            
            # Wait for page to stabilize after reload
            try:
                await page.wait_for_load_state('networkidle', timeout=30000)
            except Exception:
                await asyncio.sleep(3)
                
            logger.info(f"Page reloaded successfully. Starting next captcha solving cycle...")
            
        except Exception as e:
            logger.error(f"Error reloading page in cycle #{cycle_count}: {e}")
            # Continue anyway, the next iteration will check the title again
            await asyncio.sleep(5)


async def _wait_for_target_page(page: Page, logger: logging.Logger, target_url: str, max_wait_seconds: int | None = None) -> None:
    """Wait until not Human Verification AND the target page is fully loaded."""
    start = asyncio.get_event_loop().time()
    while True:
        await _wait_until_not_human_verification_infinite(page, logger)
        # Once not HV, ensure we're on target and fully loaded
        current_url = page.url
        if target_url in current_url or current_url.startswith(target_url):
            try:
                await page.wait_for_load_state('networkidle', timeout=30000)
            except Exception:
                # Fallback to a short delay if networkidle not reached
                await asyncio.sleep(2)
            return
        else:
            logger.info(f"Title OK but not on target page (current: {current_url}); navigating to target...")
            try:
                await page.goto(target_url, wait_until='domcontentloaded')
            except Exception as e:
                logger.debug(f"Re-navigation error: {e}")
                await asyncio.sleep(2)
        if max_wait_seconds is not None and (asyncio.get_event_loop().time() - start) > max_wait_seconds:
            raise TimeoutError("Exceeded max wait while loading target page")


async def visit_google_and_search(page: Page, logger: logging.Logger) -> None:
    try:
        logger.info("Starting Google browsing session...")
        await page.goto("https://www.google.com", wait_until='networkidle')
        await human_delay(2, 4)
        await human_mouse_movement(page)
        await human_delay(1, 2)
        search_terms = [
            "custom wheel offset reviews",
            "aftermarket wheels for cars",
            "wheel fitment guide",
            "car wheel sizes explained",
            "best wheel brands 2024",
        ]
        search_term = random.choice(search_terms)
        search_box = await page.wait_for_selector('input[name="q"]', timeout=10000)
        await search_box.click()
        await human_delay(0.5, 1)
        try:
            from .human_utils import human_type as _ht
        except ImportError:
            from human_utils import human_type as _ht
        try:
            await _ht(page, search_box, search_term, "normal")
        except Exception:
            for ch in search_term:
                await search_box.type(ch)
        await human_delay(1, 2)
        await network_delay("request")
        await page.keyboard.press('Enter')
        await page.wait_for_load_state('networkidle')
        await human_delay(2, 4)
        await human_scroll(page)
        await human_delay(2, 3)
    except Exception as e:
        logger.error(f"Error during Google browsing: {e}")


async def visit_wikipedia(page: Page, logger: logging.Logger) -> None:
    try:
        logger.info("Starting Wikipedia browsing session...")
        await page.goto("https://en.wikipedia.org", wait_until='networkidle')
        await human_delay(2, 4)
        await human_mouse_movement(page)
        await human_delay(1, 2)
        terms = ["Alloy wheel", "Tire", "Automotive industry", "Car tuning", "Wheel alignment"]
        term = random.choice(terms)
        try:
            search_box = await page.wait_for_selector('input[name="search"]', timeout=10000)
            await search_box.click()
            await human_delay(0.5, 1)
            for ch in term:
                await search_box.type(ch)
            await human_delay(1, 2)
            await page.keyboard.press('Enter')
            await page.wait_for_load_state('networkidle')
            await human_delay(2, 4)
            await human_scroll(page)
            await human_delay(3, 6)
        except Exception as e:
            logger.debug(f"Wikipedia search error: {e}")
    except Exception as e:
        logger.error(f"Error during Wikipedia browsing: {e}")


async def visit_random_sites(page: Page, logger: logging.Logger) -> None:
    try:
        logger.info("Starting random site browsing...")
        sites = [
            "https://www.reddit.com",
            "https://news.ycombinator.com",
            "https://www.bbc.com/news",
            "https://stackoverflow.com",
        ]
        for site in random.sample(sites, random.randint(1, 2)):
            try:
                logger.info(f"Visiting {site}")
                await page.goto(site, wait_until='networkidle')
                await human_delay(2, 4)
                await human_mouse_movement(page)
                await human_scroll(page)
                await human_delay(3, 6)
            except Exception as e:
                logger.debug(f"Error visiting {site}: {e}")
                continue
    except Exception as e:
        logger.error(f"Error during random site browsing: {e}")


async def navigate_to_dynamic_url(page: Page, logger: logging.Logger, url: str) -> None:
    """Navigate to a dynamically generated URL with proper delays and verification handling."""
    try:
        logger.info(f"Navigating to dynamic URL: {url}")
        await human_delay(2, 4)
        await network_delay("dns")
        await network_delay("connect")
        await network_delay("ssl")
        
        # Initial navigation
        response = await page.goto(url, wait_until='domcontentloaded')
        try:
            await page.wait_for_load_state('networkidle', timeout=30000)
        except Exception:
            await asyncio.sleep(2)
        
        if response:
            logger.info(f"Dynamic URL navigation status: {response.status}")
        else:
            logger.warning("No response received from navigation")
        
        # Wait for human verification to complete if needed (infinite retry)
        await _wait_until_not_human_verification_infinite(page, logger)
        
        logger.info(f"Successfully navigated to dynamic URL: {url}")
    except Exception as e:
        logger.error(f"Error navigating to dynamic URL {url}: {e}")
        raise


async def navigate_to_wheels_page(page: Page, logger: logging.Logger) -> None:
    try:
        logger.info("Preparing to navigate to target site...")
        await human_delay(2, 4)
        target_url = "https://www.customwheeloffset.com/store/wheels"
        logger.info(f"Navigating to: {target_url}")
        await network_delay("dns")
        await network_delay("connect")
        await network_delay("ssl")
        # Initial navigation
        response = await page.goto(target_url, wait_until='domcontentloaded')
        try:
            await page.wait_for_load_state('networkidle', timeout=30000)
        except Exception:
            await asyncio.sleep(2)
        if response:
            logger.info(f"Initial navigation status: {response.status}")
        else:
            logger.warning("No response received from initial navigation")
        # Robust wait until not HV and target page fully loaded, regardless of captcha cycles
        await _wait_for_target_page(page, logger, target_url, max_wait_seconds=None)
        logger.info("Target page loaded successfully and title is not 'Human Verification'")
    except Exception as e:
        logger.error(f"Failed to navigate to wheels page: {e}")
        raise