import asyncio
import logging
from typing import Tuple
from playwright.async_api import async_playwright, Playwright, BrowserContext, Page

try:
    from .profile_utils import check_profile_exists, get_profile_dir
except ImportError:
    from profile_utils import check_profile_exists, get_profile_dir

# Provider-specific browser config (with safe fallbacks when running as a script)
try:
    from .config import (
        HEADLESS,
        VIEWPORT,
        USER_AGENT,
        BROWSER_ARGS,
        EXTENSION_PATH,
    )
except ImportError:
    try:
        from config import (
            HEADLESS,
            VIEWPORT,
            USER_AGENT,
            BROWSER_ARGS,
            EXTENSION_PATH,
        )
    except ImportError:
        # Fallback defaults
        HEADLESS = False
        VIEWPORT = {"width": 1920, "height": 1080}
        USER_AGENT = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        EXTENSION_PATH = (
            "E:/scraper/src/providers/custom_wheel_offset_playwright/extension/"
            "ifibfemgeogfhoebkmokieepdoobkbpo/3.7.2_0"
        )
        BROWSER_ARGS = []


aSYNC_DEFAULT_ARGS = [
    lambda viewport: f'--window-size={viewport["width"]},{viewport["height"]}',
    lambda _: '--start-maximized',
    lambda _: '--disable-blink-features=AutomationControlled',
]


def _build_args(viewport: dict) -> list:
    if BROWSER_ARGS:
        return list(BROWSER_ARGS)
    base = [
        f'--load-extension={EXTENSION_PATH}',
        '--disable-extensions-except=' + EXTENSION_PATH,
    ]
    # Add defaults based on viewport
    for fn in aSYNC_DEFAULT_ARGS:
        base.append(fn(viewport))
    return base


async def setup_browser(profile_name: str, logger: logging.Logger) -> Tuple[Playwright, BrowserContext, Page, bool]:
    """Create persistent context with shared config and stealth; return resources and profile state."""
    is_existing = check_profile_exists(profile_name, logger)
    pw = await async_playwright().start()

    profile_dir = get_profile_dir(profile_name)
    logger.info(f"Using persistent profile directory: {profile_dir}")

    args = _build_args(VIEWPORT)

    context = await pw.chromium.launch_persistent_context(
        user_data_dir=str(profile_dir),
        headless=HEADLESS,
        args=args,
        user_agent=USER_AGENT,
        viewport=VIEWPORT,
        locale='en-US',
        timezone_id='America/New_York',
    )
    page = await context.new_page()

    # Basic stealth script
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
        Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
        window.chrome = { runtime: {} };
    """)

    logger.info(
        f"Browser setup complete; headless={HEADLESS}, "
        f"viewport {VIEWPORT['width']}x{VIEWPORT['height']}"
    )
    return pw, context, page, is_existing